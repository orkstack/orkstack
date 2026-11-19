package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A run carrying on holds everything back and publishes it while work
// waits; it publishes nothing once nothing waits, once it has finished, and
// while it is paused; carrying on outranks the cap in one pass; and a pass
// that holds nothing clears the hold.
func TestDispatchHold_CarryingOn(t *testing.T) {
	waiting := func() *runState {
		return &runState{
			order: []string{"a", "b"},
			taskRuns: map[string]*orkstackv1.TaskRun{
				"a": {TaskId: "a", Status: orkstackv1.Status_STATUS_RUNNING},
				"b": {TaskId: "b", Status: orkstackv1.Status_STATUS_PENDING},
			},
			status:    orkstackv1.Status_STATUS_RUNNING,
			quiescing: true,
		}
	}
	carrying := orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_CARRYING_ON
	none := orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED

	s := waiting()
	s.beginPass()
	if !s.holdDispatch(nil, "b", nil) || s.dispatchHoldProto() != carrying {
		t.Fatalf("a quiescing run publishes %v, want carrying on", s.dispatchHoldProto())
	}
	s.noteHold(orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP)
	if s.dispatchHoldProto() != carrying {
		t.Fatalf("the cap overwrote carrying on in the same pass: %v", s.dispatchHoldProto())
	}

	s.taskRuns["b"].Status = orkstackv1.Status_STATUS_RUNNING
	if s.dispatchHoldProto() != none {
		t.Fatalf("with nothing waiting the run publishes %v", s.dispatchHoldProto())
	}

	s = waiting()
	s.beginPass()
	s.holdDispatch(nil, "b", nil)
	s.status = orkstackv1.Status_STATUS_SUCCEEDED
	if s.dispatchHoldProto() != none {
		t.Fatalf("a finished run publishes %v", s.dispatchHoldProto())
	}

	s = waiting()
	s.beginPass()
	s.holdDispatch(nil, "b", nil)
	s.failingPhase = true
	if s.dispatchHoldProto() != none {
		t.Fatalf("a run running its failure handlers publishes %v", s.dispatchHoldProto())
	}

	s = waiting()
	s.beginPass()
	s.holdDispatch(nil, "b", nil)
	s.quiescing = false // the carry was given up within the pass
	if s.dispatchHoldProto() != none {
		t.Fatalf("a run no longer carrying on publishes %v", s.dispatchHoldProto())
	}

	// A held fan-out's due items wait though no row is PENDING.
	s = waiting()
	s.taskRuns["b"].Status = orkstackv1.Status_STATUS_RUNNING
	s.eaches = map[string]*eachState{"b": {expanded: true, count: 4, next: 1, inFlight: 1, limit: 2}}
	s.beginPass()
	s.holdDispatch(nil, "b", nil)
	if s.dispatchHoldProto() != carrying {
		t.Fatalf("a held fan-out publishes %v, want carrying on", s.dispatchHoldProto())
	}

	s = waiting()
	s.beginPass()
	s.holdDispatch(nil, "b", nil)
	s.beginPass() // a pass that holds nothing ...
	if s.holdSeen {
		t.Fatal("beginPass kept the last pass's hold")
	}
}
