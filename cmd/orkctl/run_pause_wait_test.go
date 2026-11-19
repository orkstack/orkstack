package main

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func pauseEvent(typ orkstackv1.EventType, requestID string, transition orkstackv1.PauseTransition) *orkstackv1.Event {
	status := orkstackv1.Status_STATUS_PAUSED
	if typ == orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED {
		status = orkstackv1.Status_STATUS_RUNNING
	}
	return &orkstackv1.Event{Type: typ, RequestId: requestID, PauseTransition: transition, Status: status}
}

// --wait decides by the request's own id: its event, a terminal event
// first, or (for a pause) another request's pause first.
func TestPauseWaiter_Observe(t *testing.T) {
	paused := orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED
	unpaused := orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED
	task := &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, TaskId: "a"}
	ended := &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, Status: orkstackv1.Status_STATUS_FAILED}
	for _, tc := range []struct {
		name   string
		pause  bool
		events []*orkstackv1.Event
		want   pauseOutcome
		exit   int
	}{
		{"own pause applied", true, []*orkstackv1.Event{task, pauseEvent(paused, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED)}, pauseApplied, 0},
		{"own unpause applied", false, []*orkstackv1.Event{pauseEvent(unpaused, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED)}, pauseApplied, 0},
		{"another request paused it first", true, []*orkstackv1.Event{pauseEvent(paused, "other", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED)}, pauseOtherRequest, exitPauseOtherRequest},
		{"an inherited pause first", true, []*orkstackv1.Event{pauseEvent(paused, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED)}, pauseOtherRequest, exitPauseOtherRequest},
		{"a causes change is not another pause", true, []*orkstackv1.Event{
			pauseEvent(paused, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED),
			pauseEvent(paused, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED)}, pauseApplied, 0},
		{"another pause does not decide an unpause", false, []*orkstackv1.Event{
			pauseEvent(paused, "other", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED),
			pauseEvent(unpaused, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED)}, pauseApplied, 0},
		{"an unpause that leaves the parent's pause", false, []*orkstackv1.Event{
			pauseEvent(paused, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED)}, pauseStillPaused, exitPauseOtherRequest},
		{"the run ended first", true, []*orkstackv1.Event{ended}, pauseRunEnded, exitPauseRunEnded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &pauseWaiter{runID: "ork-r", requestID: "req", pause: tc.pause}
			var got pauseOutcome
			decided := false
			for _, ev := range tc.events {
				if got, decided = w.observe(ev); decided {
					break
				}
			}
			if !decided || got != tc.want || got.exitCode() != tc.exit {
				t.Fatalf("outcome %q (decided %t, exit %d), want %q exit %d", got, decided, got.exitCode(), tc.want, tc.exit)
			}
		})
	}
}

// After a STREAM_GAP the run's snapshot decides.
func TestPauseWaiter_FromSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause bool
		run   *orkstackv1.Run
		want  pauseOutcome
		ok    bool
	}{
		{"own pause in place", true, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: &orkstackv1.RunPause{RequestId: "req"}}, pauseApplied, true},
		{"another pause in place", true, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: &orkstackv1.RunPause{RequestId: "x"}}, pauseOtherRequest, true},
		{"not yet applied", true, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING}, "", false},
		{"an unpause is never inferred from a run that reads unpaused", false, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING}, "", false},
		{"still paused", false, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: &orkstackv1.RunPause{}}, "", false},
		{"ended", false, &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED}, pauseRunEnded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &pauseWaiter{runID: "ork-r", requestID: "req", pause: tc.pause}
			got, ok := w.fromSnapshot(tc.run)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("fromSnapshot = %q, %t; want %q, %t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// The human lines of each outcome.
func TestPauseWaiter_Summary(t *testing.T) {
	w := &pauseWaiter{runID: "ork-r", requestID: "req", pause: true,
		event: pauseEvent(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED)}
	if got, want := w.summary(pauseApplied, "2026-11-19T10:00:00Z", true),
		"run ork-r paused until 2026-11-19T10:00:00Z; its running child runs are paused too"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	w.ended = &orkstackv1.Event{Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if got, want := w.summary(pauseApplied, "2026-11-19T10:00:00Z", false),
		"run ork-r paused until 2026-11-19T10:00:00Z; the run then finished (SUCCEEDED)"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	u := &pauseWaiter{runID: "ork-r", requestID: "req",
		event: pauseEvent(orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED, "req", orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED)}
	if got, want := u.summary(pauseApplied, "", false), "run ork-r was not paused"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	e := &pauseWaiter{runID: "ork-r", requestID: "req", endedCode: "RUN_PAUSE_TIMEOUT",
		event: &orkstackv1.Event{Status: orkstackv1.Status_STATUS_FAILED}}
	if got, want := e.summary(pauseRunEnded, "", false), "run ork-r ended (FAILED RUN_PAUSE_TIMEOUT); the request had no effect"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	if got, want := e.summary(pauseTimeout, "", false), "the run has not applied request req yet"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
}
