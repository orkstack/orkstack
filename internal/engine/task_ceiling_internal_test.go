package engine

import (
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// testPublishReserve is what the publish window every plan built at submit
// pins (DefaultInFlightLimit) can still append, P·e, which the boot's
// flat-stage bound reserves.
var testPublishReserve = DefaultInFlightLimit(DefaultRunMaxInFlight).PublishReserveEvents()

// ceilingDefaults is the history limit and continuation budget
// pinRunTreeLimits builds at the deployment defaults.
func ceilingDefaults() (HistoryLimit, ContinuationBudget) {
	return HistoryLimit{MaxEvents: DefaultHistoryMaxEvents, MaxBytes: DefaultHistoryMaxBytes, Reserve: DefaultHistoryReserve},
		DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
}

// TestShapeMaxTasks_ClosedForm pins 7.4's table, with the detached publish
// path's window reserved and d taken at its worst between
// DetachedDispatchEvents and PlainDispatchEvents: at the defaults 4,389 at
// W = 500 (the flat stage at run_max_in_flight's default), 3,540 at the
// 1,000 the default was before the path, 4,929 at W = 250 (the staged DAG),
// 5,438 at W = 15 (the deep chains) and 5,468 at W = 1;
// continuation.events 12,800 gives 3,988, run_max_in_flight 300 gives 4,821
// and restore_timeout 8 h gives 3,207. X never rises with W, and at W = 500
// the binding d is PlainDispatchEvents. It also computes the bytes form at
// the defaults, for 16 KiB flat and 256 KiB deep dispatches, quotes both,
// and fails if either is below the events form: then bytes, not events,
// would bind.
func TestShapeMaxTasks_ClosedForm(t *testing.T) {
	limit, budget := ceilingDefaults()
	if got := RestoreHistoryBound(budget); got != 1478 {
		t.Fatalf("RestoreHistoryBound at the defaults = %d, want 1,478 (B = 1,502)", got)
	}
	bigEvents := DefaultContinuationBudget(12_800, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
	longRestore := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, 8*time.Hour)
	for _, tc := range []struct {
		name   string
		budget ContinuationBudget
		width  int64
		want   int64
	}{
		{"flat (W = 500, the default)", budget, 500, 4389},
		{"flat (W = 1,000, the default before the detached path)", budget, 1000, 3540},
		{"staged (W = 250)", budget, 250, 4929},
		{"deep (W = 15)", budget, 15, 5438},
		{"one chain (W = 1)", budget, 1, 5468},
		{"continuation.events 12,800", bigEvents, 500, 3988},
		{"run_max_in_flight 300", budget, 300, 4821},
		{"restore_timeout 8h", longRestore, 500, 3207},
	} {
		if got := ShapeMaxTasks(limit, tc.budget, tc.width, testPublishReserve); got != tc.want {
			t.Errorf("%s: ShapeMaxTasks = %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := ShapeMaxTasks(limit, budget, int64(DefaultRunMaxInFlight), testPublishReserve); got < int64(DefaultOrkflowMaxTasks) {
		t.Errorf("at the default run_max_in_flight (%d) X = %d, below the default task ceiling %d",
			DefaultRunMaxInFlight, got, DefaultOrkflowMaxTasks)
	}
	prev := ShapeMaxTasks(limit, budget, 1, testPublishReserve)
	for w := int64(2); w <= 1800; w++ {
		x := ShapeMaxTasks(limit, budget, w, testPublishReserve)
		if x > prev {
			t.Fatalf("X rises from %d to %d at W = %d", prev, x, w)
		}
		prev = x
	}
	r := limit.Reserve
	at14 := shapeMaxTasks(shapeTerms{limit: limit.MaxEvents, base: RestoreHistoryBound(budget) + generation0Start,
		fixed: r.Fixed, pending: r.PerPending, inFlight: r.PerInFlight, budget: budget.Events,
		dispatch: PlainDispatchEvents, complete: PlainCompletionEvents, landed: LandedCompletionEvents,
		width: 500, publishes: testPublishReserve})
	if at14 != 4389 {
		t.Errorf("at W = 500 and d = %d, X = %d, want the binding 4,389", PlainDispatchEvents, at14)
	}

	flatEvents, deepEvents := ShapeMaxTasks(limit, budget, 500, testPublishReserve), ShapeMaxTasks(limit, budget, 15, testPublishReserve)
	flatBytes := shapeMaxTasksBytes(limit, budget, 500, testPublishReserve, 16<<10)
	deepBytes := shapeMaxTasksBytes(limit, budget, 15, testPublishReserve, 256<<10)
	t.Logf("E7 closed form at the defaults: events flat %d, deep %d; bytes flat (16 KiB dispatches) %d, deep (256 KiB) %d",
		flatEvents, deepEvents, flatBytes, deepBytes)
	proof.WriteJSON(t, "task-ceiling", "closed-form.json", map[string]any{
		"events": map[string]int64{"flat": flatEvents, "staged": ShapeMaxTasks(limit, budget, 250, testPublishReserve), "deep": deepEvents, "chain": ShapeMaxTasks(limit, budget, 1, testPublishReserve)},
		"bytes":  map[string]int64{"flat_16KiB": flatBytes, "deep_256KiB": deepBytes},
	})
	if flatBytes < flatEvents || deepBytes < deepEvents {
		t.Fatalf("the bytes form (flat %d, deep %d) is below the events form (flat %d, deep %d): bytes would bind",
			flatBytes, deepBytes, flatEvents, deepEvents)
	}
}

// TestShapePeak_IsTheBoundAtTheMaximum: the predicted peak of a first
// generation at ShapeMaxTasks is at most L − 1, and one task more reaches L;
// a generation with few tasks left peaks at the hold line plus one dispatch,
// its landings and the publish window's reserve (33,402 at the defaults,
// W = 500).
func TestShapePeak_IsTheBoundAtTheMaximum(t *testing.T) {
	limit, budget := ceilingDefaults()
	for _, width := range []int64{500, 1000, 250, 15, 1} {
		n := ShapeMaxTasks(limit, budget, width, testPublishReserve)
		base := RestoreHistoryBound(budget) + generation0Start
		if peak := ShapePeak(limit, budget, width, testPublishReserve, base, n); peak > limit.MaxEvents-1 {
			t.Errorf("W = %d: the peak at %d tasks is %d, over L − 1 = %d", width, n, peak, limit.MaxEvents-1)
		}
		if peak := ShapePeak(limit, budget, width, testPublishReserve, base, n+1); peak < limit.MaxEvents {
			t.Errorf("W = %d: the peak at %d tasks is %d, under L = %d: the bound is not tight", width, n+1, peak, limit.MaxEvents)
		}
	}
	if peak := ShapePeak(limit, budget, 500, testPublishReserve, 4, 100); peak != 33_402 {
		t.Errorf("a generation of 100 tasks below the hold line peaks at %d, want 33,402", peak)
	}
}
