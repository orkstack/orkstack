package engine

import (
	"testing"

	"go.temporal.io/sdk/workflow"
)

// TestNextDispatchBytes_IsWhatEachDispatchRecords: the lookahead counts, for
// each kind of dispatch, exactly the command bytes that dispatch notes
// (noteCommand): a task's input up to the values bound plus its command, a
// pin's session when it opens it (if larger), a child run's start, one item's
// input plus its command, and nothing for an await or a fan-out, whose items
// are counted one by one. A plan without a history limit counts nothing.
func TestNextDispatchBytes_IsWhatEachDispatchRecords(t *testing.T) {
	s := bareGuardState(t)
	limit := s.historyLimit()
	// A session larger than a task's input, so taking the larger of the two
	// is visible.
	limit.Reserve.SessionBytes = s.valuesBound() + limit.Reserve.CommandBytes + 1<<20
	s.itemInputLimit = 7000
	task := s.valuesBound() + limit.Reserve.CommandBytes

	for id, want := range map[string]int64{
		"a":       task,
		"approve": 0,
		"fan":     0,
		"m1":      limit.Reserve.SessionBytes,
	} {
		if got := s.nextDispatchBytes(id); got != want {
			t.Errorf("nextDispatchBytes(%q) = %d, want %d", id, got, want)
		}
	}
	if got, want := s.nextItemBytes(), int64(7000)+limit.Reserve.CommandBytes; got != want {
		t.Errorf("nextItemBytes = %d, want %d", got, want)
	}

	p, err := s.pinFor("m2")
	if err != nil || p == nil {
		t.Fatalf("pinFor(m2) = %v, %v", p, err)
	}
	p.opened = true
	if got := s.nextDispatchBytes("m2"); got != task {
		t.Errorf("nextDispatchBytes of a member of an opened pin = %d, want a task's %d", got, task)
	}

	if s.children == nil {
		s.children = map[string]*childState{}
	}
	s.children["b"] = &childState{}
	if got := s.nextDispatchBytes("b"); got != limit.Reserve.ChildStartBytes {
		t.Errorf("nextDispatchBytes of a child run = %d, want its start's %d", got, limit.Reserve.ChildStartBytes)
	}

	s.plan.History = nil
	for _, id := range []string{"a", "approve", "m1", "b"} {
		if got := s.nextDispatchBytes(id); got != 0 {
			t.Errorf("without a history limit nextDispatchBytes(%q) = %d, want 0", id, got)
		}
	}
	if got := s.nextItemBytes(); got != 0 {
		t.Errorf("without a history limit nextItemBytes = %d, want 0", got)
	}
}

// TestHoldDispatch_TheHardStopCountsTheBlockingPublish: the hard stop counts
// what the cluster counts as pending — the run's work, its detached item
// publishes, and the one blocking publish an emit may have outstanding — so
// a run holds at hardStop-1 units of work, not one later.
func TestHoldDispatch_TheHardStopCountsTheBlockingPublish(t *testing.T) {
	const hardStop = 4
	runGuardWorkflow(t, nil, func(ctx workflow.Context) error {
		plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
		plan.InFlight = &InFlightLimit{Max: 100, HardStop: hardStop}
		s := newRunState(ctx, "ork-hold", guardCompile(t, guardStateDocument),
			guardInput(t, map[string]any{"regions": itemsArray(3)}), plan)
		inFlight := func(n int) map[string]workflow.Future {
			m := map[string]workflow.Future{}
			for i := 0; i < n; i++ {
				m[string(rune('p'+i))] = nil
			}
			return m
		}
		if s.holdDispatch(ctx, "a", inFlight(hardStop-2)) {
			t.Errorf("held with %d in flight; the hard stop of %d is not reached", hardStop-2, hardStop)
		}
		if !s.holdDispatch(ctx, "a", inFlight(hardStop-1)) {
			t.Errorf("did not hold with %d in flight plus the blocking publish, at the hard stop of %d", hardStop-1, hardStop)
		}
		s.pendingItemPublishes = append(s.pendingItemPublishes, nil)
		if !s.holdDispatch(ctx, "a", inFlight(hardStop-2)) {
			t.Errorf("did not hold with %d in flight, a detached publish and the blocking one", hardStop-2)
		}
		return nil
	})
}

// TestHoldDispatch_AnOpenPinCountsAgainstTheCap: the cap counts every pin
// from its open request to its close (its session-creation activity stays
// pending for the pin's whole life), so with Max-1 activities in flight an
// open pin brings the run to its cap, and a closed one no longer counts.
func TestHoldDispatch_AnOpenPinCountsAgainstTheCap(t *testing.T) {
	const capMax = 3
	runGuardWorkflow(t, nil, func(ctx workflow.Context) error {
		plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
		plan.InFlight = &InFlightLimit{Max: capMax, HardStop: DefaultPendingActivityHardStop}
		s := newRunState(ctx, "ork-pin-cap", guardCompile(t, guardStateDocument),
			guardInput(t, map[string]any{"regions": itemsArray(3)}), plan)
		inFlight := map[string]workflow.Future{"x": nil, "y": nil}
		if s.livePins() != 0 || s.holdDispatch(ctx, "a", inFlight) {
			t.Fatalf("with no pin open: livePins = %d, held = true; want 0 and no hold under the cap", s.livePins())
		}
		p := s.pins["build"]
		p.opened = true
		if got := s.livePins(); got != 1 {
			t.Fatalf("an open pin: livePins = %d, want 1", got)
		}
		if got := s.inFlightWork(inFlight); got != capMax {
			t.Fatalf("inFlightWork = %d, want %d: two activities and the open pin", got, capMax)
		}
		if !s.holdDispatch(ctx, "a", inFlight) {
			t.Fatal("two activities and an open pin did not hold a dispatch at a cap of 3")
		}
		closed, settle := workflow.NewFuture(ctx)
		settle.Set(nil, nil)
		p.closed = closed
		if got := s.livePins(); got != 0 {
			t.Fatalf("a closed pin: livePins = %d, want 0", got)
		}
		if s.holdDispatch(ctx, "a", inFlight) {
			t.Fatal("a closed pin still held a dispatch")
		}
		return nil
	})
}
