package engine

import (
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// ceilingDefaults is the history limit and continuation budget
// pinRunTreeLimits builds at the deployment defaults.
func ceilingDefaults() (HistoryLimit, ContinuationBudget) {
	return HistoryLimit{MaxEvents: DefaultHistoryMaxEvents, MaxBytes: DefaultHistoryMaxBytes, Reserve: DefaultHistoryReserve},
		DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
}

// TestShapeMaxTasks_ClosedForm pins 7.4's table: at the defaults 4,222 at
// W = 1,000 (the flat stage), 4,949 at W = 250 (the staged DAG), 5,458 at
// W = 15 (the deep chains) and 5,488 at W = 1; continuation.events 12,800
// gives 3,112, run_max_in_flight 300 gives 4,841 and restore_timeout 8 h
// gives 3,041. It also computes the bytes form at the defaults, for 16 KiB
// flat and 256 KiB deep dispatches, quotes both, and fails if either is
// below the events form: then bytes, not events, would bind.
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
		{"flat (W = 1,000)", budget, 1000, 4222},
		{"staged (W = 250)", budget, 250, 4949},
		{"deep (W = 15)", budget, 15, 5458},
		{"one chain (W = 1)", budget, 1, 5488},
		{"continuation.events 12,800", bigEvents, 1000, 3112},
		{"run_max_in_flight 300", budget, 300, 4841},
		{"restore_timeout 8h", longRestore, 1000, 3041},
	} {
		if got := ShapeMaxTasks(limit, tc.budget, tc.width); got != tc.want {
			t.Errorf("%s: ShapeMaxTasks = %d, want %d", tc.name, got, tc.want)
		}
	}
	// Every shape at least ⌈G/d⌉ wide has the flat bound.
	if wide := ShapeMaxTasks(limit, budget, 586); wide != 4222 {
		t.Errorf("W = 586 = ⌈G/d⌉: %d, want the flat 4,222", wide)
	}

	flatEvents, deepEvents := ShapeMaxTasks(limit, budget, 1000), ShapeMaxTasks(limit, budget, 15)
	flatBytes := shapeMaxTasksBytes(limit, budget, 1000, 16<<10)
	deepBytes := shapeMaxTasksBytes(limit, budget, 15, 256<<10)
	t.Logf("E7 closed form at the defaults: events flat %d, deep %d; bytes flat (16 KiB dispatches) %d, deep (256 KiB) %d",
		flatEvents, deepEvents, flatBytes, deepBytes)
	proof.WriteJSON(t, "task-ceiling", "closed-form.json", map[string]any{
		"events": map[string]int64{"flat": flatEvents, "staged": ShapeMaxTasks(limit, budget, 250), "deep": deepEvents, "chain": ShapeMaxTasks(limit, budget, 1)},
		"bytes":  map[string]int64{"flat_16KiB": flatBytes, "deep_256KiB": deepBytes},
	})
	if flatBytes < flatEvents || deepBytes < deepEvents {
		t.Fatalf("the bytes form (flat %d, deep %d) is below the events form (flat %d, deep %d): bytes would bind",
			flatBytes, deepBytes, flatEvents, deepEvents)
	}
}

// TestShapePeak_IsTheBoundAtTheMaximum: the predicted peak of a first
// generation at ShapeMaxTasks is at most L − 1, and one task more reaches L;
// a generation with few tasks left peaks at the hold line plus one dispatch
// and its landings (35,742 at the defaults, W = 1,000).
func TestShapePeak_IsTheBoundAtTheMaximum(t *testing.T) {
	limit, budget := ceilingDefaults()
	for _, width := range []int64{1000, 250, 15, 1} {
		n := ShapeMaxTasks(limit, budget, width)
		base := RestoreHistoryBound(budget) + generation0Start
		if peak := ShapePeak(limit, budget, width, base, n); peak > limit.MaxEvents-1 {
			t.Errorf("W = %d: the peak at %d tasks is %d, over L − 1 = %d", width, n, peak, limit.MaxEvents-1)
		}
		if peak := ShapePeak(limit, budget, width, base, n+1); peak < limit.MaxEvents {
			t.Errorf("W = %d: the peak at %d tasks is %d, under L = %d: the bound is not tight", width, n+1, peak, limit.MaxEvents)
		}
	}
	if peak := ShapePeak(limit, budget, 1000, 4, 100); peak != 35_742 {
		t.Errorf("a generation of 100 tasks below the hold line peaks at %d, want 35,742", peak)
	}
}
