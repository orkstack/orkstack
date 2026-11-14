package orker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
)

// latchHeartbeater is a fast loop on a cluster whose answer a test sets per
// call, with onLost stopping the attempt as Execute wires it.
func latchHeartbeater(answer func() error, ackTimeout time.Duration, stop context.CancelCauseFunc,
	deliver func(context.Context, HeartbeatProgress)) *Heartbeater {
	return &Heartbeater{
		interval:   2 * time.Millisecond,
		send:       func(context.Context, HeartbeatProgress) error { return answer() },
		deliver:    deliver,
		onLost:     stop,
		ackTimeout: ackTimeout,
	}
}

// TestHeartbeater_TheLatchOpensOnTheFirstAnswer: the first acknowledged
// heartbeat opens the latch, after any number of unanswered ones before it.
func TestHeartbeater_TheLatchOpensOnTheFirstAnswer(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	calls := 0
	h := latchHeartbeater(func() error {
		calls++
		if calls < 4 {
			return context.DeadlineExceeded
		}
		return nil
	}, time.Minute, stop, nil)
	h.Start(ctx)
	defer h.Stop()
	if err := h.AwaitFirstAnswer(ctx); err != nil {
		t.Fatalf("AwaitFirstAnswer = %v, want nil once the cluster answered", err)
	}
	if calls < 4 {
		t.Errorf("the latch opened after %d heartbeats, before any was answered", calls)
	}
}

// TestHeartbeater_TheLatchIsOpenWithNothingToTell: outside an activity
// (orkctl task run, orkctl activity test) nothing is sent and nothing waits.
func TestHeartbeater_TheLatchIsOpenWithNothingToTell(t *testing.T) {
	h := NewHeartbeater(context.Background())
	if h.send != nil {
		t.Fatal("a heartbeater outside an activity has a send")
	}
	h.Start(context.Background())
	defer h.Stop()
	done := make(chan error, 1)
	go func() { done <- h.AwaitFirstAnswer(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AwaitFirstAnswer = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the latch waited with nothing to tell")
	}
}

// TestHeartbeater_TheServersDecisionClosesTheLatchAtOnce: NotFound (a run
// terminated from outside) is handed to the SDK, which cancels the attempt;
// the latch returns that cancel, well inside the heartbeat timeout.
func TestHeartbeater_TheServersDecisionClosesTheLatchAtOnce(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	terminated := serviceerror.NewNotFound("workflow execution already completed")
	var calls atomic.Int32
	h := latchHeartbeater(func() error { calls.Add(1); return terminated }, time.Hour, stop,
		func(context.Context, HeartbeatProgress) { stop(terminated) })
	h.Start(ctx)
	defer h.Stop()
	err := h.AwaitFirstAnswer(ctx)
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("AwaitFirstAnswer = %v, want the server's NotFound", err)
	}
	// At once: the first heartbeat carries the decision, and the latch does
	// not wait for another.
	if n := calls.Load(); n > 2 {
		t.Errorf("the latch saw the server's decision only after %d heartbeats", n)
	}
}

// TestHeartbeater_TheLatchFailsLostAfterTheTimeout: a cluster that answers
// nothing holds the latch until the heartbeat timeout, counted from Start,
// and then the attempt is stopped as lost, never cancelled.
func TestHeartbeater_TheLatchFailsLostAfterTheTimeout(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	// The loop's own clock, advanced a minute per reading: the timeout is
	// counted on it, never on the wall.
	const timeout = 10 * time.Minute
	start := time.Unix(1_700_000_000, 0)
	var readings atomic.Int64
	clock := func() time.Time { return start.Add(time.Duration(readings.Add(1)) * time.Minute) }
	var lostAt atomic.Int64
	h := latchHeartbeater(func() error { return context.DeadlineExceeded }, timeout, func(err error) {
		lostAt.Store(readings.Load())
		stop(err)
	}, nil)
	h.now = clock
	h.Start(ctx)
	defer h.Stop()
	err := h.AwaitFirstAnswer(ctx)
	var lost *heartbeatUnanswered
	if !errors.As(err, &lost) {
		t.Fatalf("AwaitFirstAnswer = %v, want heartbeatUnanswered", err)
	}
	if elapsed := time.Duration(lostAt.Load()-1) * time.Minute; elapsed < timeout {
		t.Errorf("the latch gave up %s after Start on the loop's clock, inside the %s timeout", elapsed, timeout)
	}
	if lostNow, _ := heartbeatLost(ctx); !lostNow {
		t.Error("the attempt's context does not read as lost")
	}
}
