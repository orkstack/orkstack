package orker

import (
	"context"
	"errors"
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
	h := latchHeartbeater(func() error { return terminated }, time.Hour, stop,
		func(context.Context, HeartbeatProgress) { stop(terminated) })
	h.Start(ctx)
	defer h.Stop()
	began := time.Now()
	err := h.AwaitFirstAnswer(ctx)
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("AwaitFirstAnswer = %v, want the server's NotFound", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("the latch took %s to see the server's decision", took)
	}
}

// TestHeartbeater_TheLatchFailsLostAfterTheTimeout: a cluster that answers
// nothing holds the latch until the heartbeat timeout, counted from Start,
// and then the attempt is stopped as lost, never cancelled.
func TestHeartbeater_TheLatchFailsLostAfterTheTimeout(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	const timeout = 60 * time.Millisecond
	h := latchHeartbeater(func() error { return context.DeadlineExceeded }, timeout, stop, nil)
	began := time.Now()
	h.Start(ctx)
	defer h.Stop()
	err := h.AwaitFirstAnswer(ctx)
	var lost *heartbeatUnanswered
	if !errors.As(err, &lost) {
		t.Fatalf("AwaitFirstAnswer = %v, want heartbeatUnanswered", err)
	}
	if took := time.Since(began); took < timeout {
		t.Errorf("the latch gave up after %s, inside the %s timeout", took, timeout)
	}
	if lostNow, _ := heartbeatLost(ctx); !lostNow {
		t.Error("the attempt's context does not read as lost")
	}
}
