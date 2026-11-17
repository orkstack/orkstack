package engine

import (
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A finished run that was still paused records how its own pause ended: a
// pause that had not yet lapsed ended RUN_ENDED at the run's end; one whose
// expiry the run's end reached or passed had already lapsed, so it ended
// TIMED_OUT at its expiry, whichever the run took first when both were due
// in one workflow task.
func TestLastPauseProto_RunEndedOrLapsed(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	for _, tc := range []struct {
		name     string
		finished time.Time
		wantEnd  orkstackv1.PauseEnd
		wantAt   time.Time
	}{
		{name: "ended before the expiry", finished: expires.Add(-time.Second), wantEnd: orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED, wantAt: expires.Add(-time.Second)},
		{name: "ended at the expiry", finished: expires, wantEnd: orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT, wantAt: expires},
		{name: "ended after the expiry", finished: expires.Add(time.Second), wantEnd: orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT, wantAt: expires},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finished := timestamppb.New(tc.finished)
			s := &runState{status: orkstackv1.Status_STATUS_FAILED, finished: finished,
				pause: &ownPause{by: "alice", at: at, expiresAt: expires}}
			got := s.lastPauseProto()
			if got.GetEnd() != tc.wantEnd || !got.GetEndedAt().AsTime().Equal(tc.wantAt) || got.GetPausedBy() != "alice" {
				t.Fatalf("last pause %v; want alice's pause ended %s at %s", got, tc.wantEnd, tc.wantAt)
			}
			if got.GetEndedAt() == finished {
				t.Fatal("last_pause.ended_at shares the run's finished_at message")
			}
		})
	}
}

// The run's end closes its own pause when another cause ends it (an
// inherited pause's backstop): RUN_ENDED at the end, unless the own pause
// had lapsed by then, which is TIMED_OUT at its expiry.
func TestEndOwnPauseAtRunEnd(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	for _, tc := range []struct {
		name    string
		now     time.Time
		wantEnd pauseEnd
		wantAt  time.Time
	}{
		{name: "before the expiry", now: expires.Add(-time.Second), wantEnd: pauseEndRunEnded, wantAt: expires.Add(-time.Second)},
		{name: "at the expiry", now: expires, wantEnd: pauseEndTimedOut, wantAt: expires},
		{name: "after the expiry", now: expires.Add(time.Minute), wantEnd: pauseEndTimedOut, wantAt: expires},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &runState{pause: &ownPause{at: at, expiresAt: expires}}
			s.endOwnPauseAtRunEnd(tc.now)
			if s.pause != nil || s.lastPause == nil || s.lastPause.end != tc.wantEnd || !s.lastPause.endAt.Equal(tc.wantAt) {
				t.Fatalf("pause %v, last %+v; want ended %d at %s", s.pause, s.lastPause, tc.wantEnd, tc.wantAt)
			}
		})
	}
}

// A run whose status is terminal but whose end is not yet recorded (a
// timed-out run draining its work) still shows its pause as current, so no
// snapshot built in that window loses it.
func TestPauseProtos_TerminalBeforeFinished(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := &runState{status: orkstackv1.Status_STATUS_FAILED,
		pause:     &ownPause{by: "alice", at: at, expiresAt: at.Add(time.Hour)},
		lastPause: &lastPause{pause: ownPause{by: "bob", at: at.Add(-time.Hour), expiresAt: at}, end: pauseEndUnpaused, endAt: at.Add(-time.Minute)}}
	if live := s.livePauseProto(); live.GetPausedBy() != "alice" {
		t.Fatalf("current pause %v; want alice's, still shown until the run's end is recorded", live)
	}
	if last := s.lastPauseProto(); last.GetPausedBy() != "bob" || last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_UNPAUSED {
		t.Fatalf("last pause %v; want bob's, unpaused", last)
	}
}
