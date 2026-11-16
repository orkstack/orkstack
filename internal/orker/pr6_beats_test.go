package orker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestHeartbeater_BeatStartedCarriesStarted: once the container is observed
// started the attempt sends one beat at once, by task token, with started
// set, and every later beat carries it (E92 §4.2 R7E92-M1): the engine's
// signal that the attempt did work.
func TestHeartbeater_BeatStartedCarriesStarted(t *testing.T) {
	sent := make(chan HeartbeatProgress, 16)
	h := &Heartbeater{
		ackTimeout: time.Minute,
		interval:   time.Hour, // only the first beat and the started beat go
		send: func(_ context.Context, p HeartbeatProgress) error {
			sent <- p
			return nil
		},
	}
	h.Start(context.Background())
	defer h.Stop()
	if first := <-sent; first.Started {
		t.Fatal("a beat before the container started says started")
	}
	if err := h.beatStarted(); err != nil {
		t.Fatalf("beatStarted = %v", err)
	}
	select {
	case p := <-sent:
		if !p.Started {
			t.Fatal("the started beat does not say started")
		}
	default:
		t.Fatal("beatStarted returned before its beat was sent")
	}
	if !h.progress().Started {
		t.Fatal("a later beat does not carry started")
	}
	failing := &Heartbeater{send: func(context.Context, HeartbeatProgress) error { return errors.New("unreachable") }}
	if err := failing.beatStarted(); err == nil {
		t.Fatal("a started beat the cluster did not take reported success")
	}
}

// TestHeartbeater_BeatReportedStopsTheLoopFirst: before a failure is
// reported the periodic loop is stopped and drained, then one beat with
// phase "reported" goes by task token, so no beat of the loop can land after
// it and the cluster's last beat says the attempt is reporting (E92 §4.2).
func TestHeartbeater_BeatReportedStopsTheLoopFirst(t *testing.T) {
	sent := make(chan HeartbeatProgress, 64)
	h := &Heartbeater{
		ackTimeout: time.Minute,
		interval:   5 * time.Millisecond,
		send: func(_ context.Context, p HeartbeatProgress) error {
			sent <- p
			return nil
		},
	}
	h.Start(context.Background())
	<-sent
	<-sent // the loop is beating
	if err := h.beatReported(); err != nil {
		t.Fatalf("beatReported = %v", err)
	}
	var last HeartbeatProgress
	for drained := false; !drained; {
		select {
		case p := <-sent:
			last = p
		default:
			drained = true
		}
	}
	if last.Phase != PhaseReported {
		t.Fatalf("the last beat says phase %q; want %q", last.Phase, PhaseReported)
	}
	// The loop has stopped: nothing more is sent, whatever the interval.
	select {
	case p := <-sent:
		t.Fatalf("a beat (phase %q) was sent after the reported beat", p.Phase)
	case <-time.After(50 * time.Millisecond):
	}
	h.Stop() // still safe after beatReported stopped the loop
}

// TestStage_StartedSurvivesTheHandBack: a sweep that takes an attempt back
// overwrites its stage, but whether its container started is kept, so a
// hand-back can say whether it did work.
func TestStage_StartedSurvivesTheHandBack(t *testing.T) {
	r := newStopRegistry()
	ran, idle := r.admit(), r.admit()
	ran.markStarted()
	ran.toRevocable()
	if n := r.handBack(); n != 2 {
		t.Fatalf("handBack took %d attempts; want 2", n)
	}
	if !ran.handedBack() || !idle.handedBack() {
		t.Fatal("the sweep did not take both back")
	}
	if !ran.startedContainer() {
		t.Fatal("an attempt whose container started reads as never started after its hand-back")
	}
	if idle.startedContainer() {
		t.Fatal("an attempt that started nothing reads as started")
	}
	var none *attemptStage
	if none.startedContainer() {
		t.Fatal("no stage reads as started")
	}
}
