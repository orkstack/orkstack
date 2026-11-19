package engine_test

import (
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// holdWatch records which dispatch holds a run's snapshots published. (A
// snapshot's rows are the run's live rows, so a check of the hold against
// the rows it was published with would race the pass; the reader labels
// only PENDING rows, so a hold beside rows that no longer wait names
// nothing.)
type holdWatch struct {
	mu   sync.Mutex
	seen map[orkstackv1.RunDispatchHold]bool
}

func (w *holdWatch) observe(run *orkstackv1.Run, _ int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seen == nil {
		w.seen = map[orkstackv1.RunDispatchHold]bool{}
	}
	w.seen[run.GetDispatchHold()] = true
	return nil
}

// A run at its in-flight cap publishes the hold while tasks wait behind it,
// and ends with no hold.
func TestDispatchHold_PublishesTheInFlightCap(t *testing.T) {
	h := newHarnessCtx(t, nil, itemBehavior)
	watch := &holdWatch{}
	h.onSnapshot = watch.observe
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-hold-cap", compile(t, flatDocument(30)), payload(t, nil),
		cappedPlan(16, 1900), (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "the run failed")
	if !watch.seen[orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP] {
		t.Fatalf("no snapshot published the in-flight cap; saw %v", watch.seen)
	}
	if final := mustFinalRun(t, h.store, "ork-hold-cap"); final.GetDispatchHold() != orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED {
		t.Fatalf("the finished run publishes %v, want no hold", final.GetDispatchHold())
	}
}

// A run that never reaches its cap never publishes a hold.
func TestDispatchHold_NoneUnderTheCap(t *testing.T) {
	h := newHarnessCtx(t, nil, itemBehavior)
	watch := &holdWatch{}
	h.onSnapshot = watch.observe
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-hold-none", compile(t, flatDocument(5)), payload(t, nil),
		cappedPlan(16, 1900), (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "the run failed")
	if len(watch.seen) != 1 || !watch.seen[orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED] {
		t.Fatalf("a run under its cap published %v", watch.seen)
	}
}
