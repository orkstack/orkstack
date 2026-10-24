package engine

import (
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
)

// fanOutDeclaring is one fan-out over the run input declaring max items.
func fanOutDeclaring(t *testing.T, items int, pad int) *orkstackv1.Orkflow {
	t.Helper()
	doc := fmt.Sprintf(`
apiVersion: orkstack/v1
kind: Orkflow
name: bounds
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: %d}
    with: {op: exec, part: "{{ .each.item }}", pad: "%s"}
`, items, strings.Repeat("p", pad))
	wf, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("compile a %d-item fan-out: %v", items, err)
	}
	return wf
}

func TestEachBounds_FloorAppliesOnlyAboveTheBudgetShare(t *testing.T) {
	floor := DefaultItemInputFloorBytes
	for _, tc := range []struct {
		items           int
		floor, wantSize int64
	}{
		{1, floor, task.MaxActivityInputBytes},         // capped by the payload bound, floor or not
		{64, floor, task.MaxActivityInputBytes},        // 16 MiB / 64 = 256 KiB
		{1000, floor, 16777},                           // the budget share, above the floor
		{1024, floor, 16384},                           // exactly the floor: inert
		{1024, 0, 16384},                               //
		{1025, floor, 16384},                           // the share (16,368) is below it: the floor binds
		{1025, 0, 16368},                               // no floor: the share
		{5000, floor, 16384},                           //
		{10000, floor, 16384},                          // 16 KiB, never 1.6 KiB
		{10000, 0, int64(eachItemInputBudget) / 10000}, // 1,677 bytes without the floor
	} {
		wf := fanOutDeclaring(t, tc.items, 1)
		_, inputLimit, _ := eachBounds(wf, DefaultLimits(), tc.floor)
		if inputLimit != tc.wantSize {
			t.Errorf("S=%d floor=%d: input limit %d, want %d", tc.items, tc.floor, inputLimit, tc.wantSize)
		}
	}
	// values_max_bytes still bounds it: a deployment's smaller values bound
	// wins over the floor.
	limits := DefaultLimits()
	limits.ValuesMaxBytes = 8 << 10
	if _, got, _ := eachBounds(fanOutDeclaring(t, 10000, 1), limits, floor); got != 8<<10 {
		t.Errorf("values_max_bytes 8 KiB at S=10000: input limit %d, want 8192", got)
	}
}

func TestEachBounds_KeepTheAggregateArtifactBudget(t *testing.T) {
	for _, items := range []int{1, 64, 1000, 1024, 1025, 5000, 10000} {
		for _, floor := range []int64{0, DefaultItemInputFloorBytes} {
			wf := fanOutDeclaring(t, items, 1)
			share, _, artifactMax := eachBounds(wf, DefaultLimits(), floor)
			if int(share) != items {
				t.Fatalf("share %d, want %d", share, items)
			}
			if artifactMax < 1 {
				t.Errorf("S=%d: %d artifacts per item, want at least 1", items, artifactMax)
			}
			if total := int(artifactMax) * items; total > eachItemArtifactBudget {
				t.Errorf("S=%d floor=%d: %d per item × %d items = %d workload artifacts, over the run's %d",
					items, floor, artifactMax, items, total, eachItemArtifactBudget)
			}
			if want := min(DefaultLimits().Proto().GetArtifactMaxCount(), int32(eachItemArtifactBudget/items)); artifactMax != want {
				t.Errorf("S=%d floor=%d: %d artifacts per item, want today's formula %d", items, floor, artifactMax, want)
			}
		}
	}
}

// TestEachBounds_EveryCallerPassesItsFloor: the three callers of eachBounds
// (spec 4.2) — newRunState with the plan's floor, EachItemBounds with the one
// it is handed (the API's: the default with continuation on, 0 off; its own
// test is TestEachItemBounds_ApplyUsesTheConfiguredFloor), and localItemInput
// with the default always — and at S = 10,000 each gives 16 KiB, never 1.6.
func TestEachBounds_EveryCallerPassesItsFloor(t *testing.T) {
	wf := fanOutDeclaring(t, 10000, 1)
	input := &orkstackv1.Payload{}
	plan := guardPlan(t, 1_000_000, 1<<40)
	plan.Limits = DefaultLimits()

	if s := buildRunState("ork-floor", wf, input, plan); s.itemInputLimit != int64(eachItemInputBudget)/10000 {
		t.Fatalf("newRunState without a pinned floor: %d, want the share %d", s.itemInputLimit, eachItemInputBudget/10000)
	}
	budget := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
	plan.Continuation = &budget
	if s := buildRunState("ork-floor", wf, input, plan); s.itemInputLimit != 16<<10 {
		t.Fatalf("newRunState with the plan's floor: %d, want 16 KiB", s.itemInputLimit)
	}
	budget.ItemInputFloorBytes = 32 << 10
	if s := buildRunState("ork-floor", wf, input, plan); s.itemInputLimit != 32<<10 {
		t.Fatalf("newRunState reads the plan's floor, not a constant: %d, want 32 KiB", s.itemInputLimit)
	}

	if got, _ := EachItemBounds(wf, DefaultLimits(), DefaultItemInputFloorBytes); got != 16<<10 {
		t.Fatalf("EachItemBounds with the default floor: %d, want 16 KiB", got)
	}
	if got, _ := EachItemBounds(wf, DefaultLimits(), 0); got != int64(eachItemInputBudget)/10000 {
		t.Fatalf("EachItemBounds with no floor: %d", got)
	}

	// localItemInput: an item whose own `with:` is 12 KiB composes locally
	// at S = 10,000; without the floor it would be refused at about 1.6 KiB.
	wide := fanOutDeclaring(t, 10000, 12<<10)
	index, count := int32(0), int32(2)
	if _, err := LocalTaskInput(LocalTask{
		Orkflow: wide, TaskID: "spread", RunID: "local-1",
		Input: &orkstackv1.Payload{}, Item: &index, ItemCount: &count,
	}); err != nil {
		t.Fatalf("localItemInput refused a 12 KiB item at S=10,000: %v", err)
	}
}
