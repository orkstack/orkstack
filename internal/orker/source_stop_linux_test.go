package orker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// hostThrough runs the rig's attempt as the interceptor counts it.
func (r *hostRig) hostThrough() *hosting {
	r.a.instances.add()
	g := r.start()
	counted := make(chan instanceEnding, 1)
	go func(done <-chan instanceEnding) {
		e := <-done
		r.a.instances.done()
		counted <- e
	}(g.done)
	g.done = counted
	return g
}

// TestSourceStop_EvictsAtTheFirstSignal: an instance is evicted at the stop's first
// signal, not at its grace's end, and the stop waits for it: it is idle when the
// instance has ended, and stops the source worker then.
func TestSourceStop_EvictsAtTheFirstSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		g := r.hostThrough()
		time.Sleep(3 * time.Second)
		began := time.Now()
		var sources, tasks, idle bool
		out := runStop(r.a, StopSequence{Grace: 24 * time.Hour, StopWorker: func() { tasks = time.Since(began) < 40*time.Second },
			StopSources: func() { sources, idle = time.Since(began) < 40*time.Second, r.a.instances.load() == 0 }})
		if e := g.ends(); e.Reason != "ORKER_STOPPED" {
			t.Errorf("%+v", e)
		}
		if got := <-out; got.Ended != GraceEndedIdle {
			t.Errorf("%+v", got)
		}
		if !sources || !tasks || !idle {
			t.Errorf("sources stopped %v, tasks %v, once its instance had ended %v", sources, tasks, idle)
		}
	})
}

// TestSourceStop_ZeroGrace: with no grace the instance is still stopped by its
// sequence before the source worker is.
func TestSourceStop_ZeroGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		g := r.hostThrough()
		time.Sleep(3 * time.Second)
		ended := false
		out := runStop(r.a, StopSequence{StopSources: func() { ended = r.a.instances.load() == 0 }})
		g.wantEnd("ORKER_STOPPED")
		if got := <-out; got.Ended != GraceEndedTimeout || !ended {
			t.Errorf("%+v, ended before its worker %v", got, ended)
		}
	})
}

// TestSourceStop_OneThatDoesNotEndHoldsNoExit: a container that cannot be stopped is
// left to the next boot past the bound, and said.
func TestSourceStop_OneThatDoesNotEndHoldsNoExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.a.instances.add()
		began := time.Now()
		stopped := false
		out := runStop(r.a, StopSequence{StopSources: func() { stopped = true }})
		<-out
		if took := time.Since(began); took < instanceStopBound || took > instanceStopBound+time.Minute || !stopped {
			t.Errorf("after %s, stopped %v", took, stopped)
		}
		r.a.instances.done()
	})
}

// TestSourceStop_WaitsForAContainersGrace: a container that takes its whole grace is
// stopped before the source worker is. The bound holds the longest a spec may ask
// for after the longest an instance's own end may take, and the stop timeout an
// operator is told of covers it.
func TestSourceStop_WaitsForAContainersGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.termExits = false
		g := r.hostThrough()
		time.Sleep(3 * time.Second)
		left := int32(-1)
		out := runStop(r.a, StopSequence{StopSources: func() { left = r.a.instances.load() }})
		g.wantEnd("ORKER_STOPPED")
		<-out
		if left != 0 {
			t.Errorf("%d instances as the source worker stopped", left)
		}
	})
	if whole := instanceEnds + eventsources.MaxStopSeconds*time.Second; whole >= instanceStopBound || instanceStopBound >= StopTimeoutTail {
		t.Errorf("an end and a grace take %s, a stop waits %s, the tail is %s", whole, instanceStopBound, StopTimeoutTail)
	}
}

// TestAwaitIdle_CountsInstances: an Orker that hosts an instance is not idle.
func TestAwaitIdle_CountsInstances(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	a.instances.add()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := a.AwaitIdle(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("idle while it hosts one: %v", err)
	}
	a.instances.done()
	must.OK(t, a.AwaitIdle(t.Context()))
}

// TestSourceWorkerOptions: the worker of the source channel is activity-only, holds
// one poll, has the cordon's slots and interceptor, and names the Orker.
func TestSourceWorkerOptions(t *testing.T) {
	r := newHostRig(t)
	o, err := r.a.SourceWorkerOptions(worker.Options{})
	must.OK(t, err)
	if !o.DisableWorkflowWorker || !o.DisableRegistrationAliasing || o.MaxConcurrentActivityTaskPollers != 1 || o.Identity != "orker-1" || o.Tuner == nil || len(o.Interceptors) != 1 ||
		o.EnableSessionWorker {
		t.Errorf("%+v", o)
	}
}
