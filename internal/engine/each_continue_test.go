package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A fan-out with on_item_failure: continue (docs/DSL.md, "When an item
// fails"): every item runs, and the fan-out then fails with its lowest-index
// failed item's reason; max_failed_items bounds how many may fail.

// continueDocument is a fan-out over an input array, with each appended to
// its each: block, then rest.
func continueDocument(each, rest string) string {
	return `
apiVersion: orkstack/v1
kind: Orkflow
name: continue
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 50` + each + `}
    with: {op: exec, part: "{{ .each.item }}"}
` + rest
}

// itemFailure is an item's failure for good, with code.
func itemFailure(code string) error {
	return temporal.NewNonRetryableApplicationError("item exploded: "+code, task.CategoryActivityFailed, nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: code,
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, Attempt: 1})
}

// failing answers the items in codes with their failure, and the rest with
// success.
func failing(codes map[int32]string) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil {
			if code, ok := codes[in.GetEach().GetIndex()]; ok {
				return nil, itemFailure(code)
			}
		}
		return itemResult(in), nil
	}
}

func dispatchedIndices(h *eachHarness, id string) []int32 {
	var out []int32
	for _, in := range h.itemCalls(id) {
		out = append(out, in.GetEach().GetIndex())
	}
	slices.Sort(out)
	return out
}

// TestEachContinue_RunsEveryItemThenFails: two items fail; every other item
// still runs, and the fan-out then fails with the lowest-index failed item's
// code and a message counting the failures.
func TestEachContinue_RunsEveryItemThenFails(t *testing.T) {
	h := newEachHarness(t, failing(map[int32]string{7: "SEVEN", 3: "THREE"}))
	run := h.run(t, "run-continue", continueDocument(", on_item_failure: continue", ""), map[string]any{"parts": guardItems(10)})
	if got := dispatchedIndices(h, "convert"); len(got) != 10 {
		t.Fatalf("dispatched %v; want every one of the 10 items", got)
	}
	node := taskRunOf(t, run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run %v, node %v; want both FAILED", run.GetStatus(), node.GetStatus())
	}
	if node.GetError().GetCode() != "THREE" || !strings.HasPrefix(node.GetError().GetMessage(), "2 of 10 items failed; item 3: ") {
		t.Fatalf("node error = %v; want item 3's code and \"2 of 10 items failed; item 3: …\"", node.GetError())
	}
	succeeded := 0
	for _, item := range node.GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			succeeded++
		}
	}
	if succeeded != 8 {
		t.Fatalf("%d items SUCCEEDED; want 8", succeeded)
	}
}

// TestEachContinue_SucceedsWhenNoItemFails: continue with every item
// succeeding is an ordinary fan-out.
func TestEachContinue_SucceedsWhenNoItemFails(t *testing.T) {
	h := newEachHarness(t, failing(nil))
	run := h.run(t, "run-continue-ok", continueDocument(", on_item_failure: continue", ""), map[string]any{"parts": guardItems(5)})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || taskRunOf(t, run, "convert").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %v, node %v", run.GetStatus(), taskRunOf(t, run, "convert").GetStatus())
	}
}

// TestEachContinue_TheRunKeepsStartingTasks: while a continue fan-out's items
// still run after one failed, the run is not failing: a task that becomes
// ready then still starts. The last item waits for it, so the node is
// RUNNING when it does.
func TestEachContinue_TheRunKeepsStartingTasks(t *testing.T) {
	itemFailed, lateRan := make(chan struct{}), make(chan struct{})
	var failOnce, lateOnce sync.Once
	wait := func(ctx context.Context, ch chan struct{}, what string) error {
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return errors.New(what + " never happened")
		}
	}
	h := newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch {
		case in.GetEach() != nil && in.GetEach().GetIndex() == 0:
			failOnce.Do(func() { close(itemFailed) })
			return nil, itemFailure("ZERO")
		case in.GetEach() != nil && in.GetEach().GetIndex() == 4:
			if err := wait(ctx, lateRan, "late"); err != nil {
				return nil, err
			}
		case in.GetTaskId() == "slow":
			if err := wait(ctx, itemFailed, "item 0's failure"); err != nil {
				return nil, err
			}
		case in.GetTaskId() == "late":
			lateOnce.Do(func() { close(lateRan) })
		}
		return itemResult(in), nil
	})
	rest := `  - id: slow
    uses: shell
    with: {op: exec}
  - id: late
    uses: shell
    needs: [slow]
    with: {op: exec}
`
	run := h.run(t, "run-continue-late", continueDocument(", on_item_failure: continue", rest), map[string]any{"parts": guardItems(5)})
	if got := taskRunOf(t, run, "late").GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("late = %v; a task ready after a continue item failed must still start", got)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || taskRunOf(t, run, "convert").GetError().GetCode() != "ZERO" {
		t.Fatalf("run %v, convert %v; want the run FAILED by item 0", run.GetStatus(), taskRunOf(t, run, "convert").GetError())
	}
}

// TestEachContinue_MaxFailedItemsFailsFast: with max_failed_items 2 and one
// item at a time, the third failure fails the fan-out fast: no item after it
// is dispatched, those never dispatched end CANCELED, and the reason is the
// lowest-index failed item's.
func TestEachContinue_MaxFailedItemsFailsFast(t *testing.T) {
	h := newEachHarness(t, failing(map[int32]string{1: "ONE", 2: "TWO", 3: "THREE"}))
	run := h.run(t, "run-continue-bound", continueDocument(", max_parallel: 1, on_item_failure: continue, max_failed_items: 2", ""),
		map[string]any{"parts": guardItems(10)})
	if got := dispatchedIndices(h, "convert"); !slices.Equal(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("dispatched %v; want 0 … 3 and nothing after the third failure", got)
	}
	node := taskRunOf(t, run, "convert")
	if node.GetError().GetCode() != "ONE" ||
		!strings.HasPrefix(node.GetError().GetMessage(), "more than max_failed_items 2: 3 of 10 items failed; item 1: ") {
		t.Fatalf("node error = %v; want item 1's code and the bound in the message", node.GetError())
	}
	for _, item := range node.GetItems()[4:] {
		if item.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
			t.Fatalf("item %d = %v; an item never dispatched ends CANCELED", item.GetItem(), item.GetStatus())
		}
	}
}

// TestEachContinue_WithinItsBoundRunsEveryItem: as many failures as the
// bound allows is still continue: every item runs.
func TestEachContinue_WithinItsBoundRunsEveryItem(t *testing.T) {
	h := newEachHarness(t, failing(map[int32]string{1: "ONE", 2: "TWO"}))
	run := h.run(t, "run-continue-within", continueDocument(", max_parallel: 1, on_item_failure: continue, max_failed_items: 2", ""),
		map[string]any{"parts": guardItems(6)})
	if got := dispatchedIndices(h, "convert"); len(got) != 6 {
		t.Fatalf("dispatched %v; want all 6 within the bound", got)
	}
	if msg := taskRunOf(t, run, "convert").GetError().GetMessage(); !strings.HasPrefix(msg, "2 of 6 items failed; item 1: ") {
		t.Fatalf("message = %q", msg)
	}
}

// TestEachContinue_HandlerGetsTheFailedIndices: a failure handler naming a
// failed continue fan-out is handed its count, its commit root and the
// ascending indices of the items that did not succeed; an ordinary
// dependent is skipped.
func TestEachContinue_HandlerGetsTheFailedIndices(t *testing.T) {
	rest := `  - id: report
    uses: shell
    needs: [convert]
    join: any_failed
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`
	h := newEachHarness(t, failing(map[int32]string{7: "SEVEN", 3: "THREE"}))
	run := h.run(t, "run-continue-handler", continueDocument(", on_item_failure: continue", rest), map[string]any{"parts": guardItems(10)})
	if len(h.itemCalls("merge")) != 0 || taskRunOf(t, run, "merge").GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatal("merge ran after a failed fan-out")
	}
	calls := h.itemCalls("report")
	if len(calls) != 1 {
		t.Fatalf("report ran %d times; want once", len(calls))
	}
	up := upstreamRefOf(calls[0], "convert")
	if up.GetEach().GetCount() != 10 || up.GetEach().GetCommitRoot() == "" || !slices.Equal(up.GetEach().GetFailed(), []int32{3, 7}) {
		t.Fatalf("report's convert upstream = %v; want count 10, a commit root and failed [3 7]", up)
	}
}

// TestEachContinue_FailFastHandlerIsUnchanged: a fail_fast fan-out's handler
// still gets no item reference and no failed indices.
func TestEachContinue_FailFastHandlerIsUnchanged(t *testing.T) {
	rest := `  - id: report
    uses: shell
    needs: [convert]
    join: any_failed
    with: {op: exec}
`
	h := newEachHarness(t, failing(map[int32]string{3: "THREE"}))
	h.run(t, "run-failfast-handler", continueDocument(", max_parallel: 1", rest), map[string]any{"parts": guardItems(10)})
	calls := h.itemCalls("report")
	if len(calls) != 1 {
		t.Fatalf("report ran %d times; want once", len(calls))
	}
	if up := upstreamRefOf(calls[0], "convert"); up.GetEach() != nil {
		t.Fatalf("a fail_fast fan-out's handler got %v; want no item reference", up)
	}
}

// TestEachContinue_CrossesASwitch: a continue fan-out whose item failed
// before its run carried on still counts that failure after the switch:
// every item runs once across the generations, and the fan-out fails with
// it.
func TestEachContinue_CrossesASwitch(t *testing.T) {
	doc := continueDocument(", max_parallel: 2, on_item_failure: continue", "")
	g := runGenerations(t, "ork-continue-switch", compile(t, doc), payload(t, map[string]any{"parts": guardItems(10)}), holdPlan(),
		failing(map[int32]string{1: "ONE"}), func(h *harness, generation int) {
			if generation == 0 {
				raiseHistoryWhen(h, 650, func(run *orkstackv1.Run) bool { return itemsSucceeded(run, "convert") >= 2 })
			}
		})
	if len(g.harnesses) < 2 {
		t.Fatalf("%d generations; want the run to carry on", len(g.harnesses))
	}
	seen := map[int32]int{}
	for _, h := range g.harnesses {
		for _, in := range h.invocations() {
			if in.GetEach() != nil {
				seen[in.GetEach().GetIndex()]++
			}
		}
	}
	for i := int32(0); i < 10; i++ {
		if seen[i] != 1 {
			t.Fatalf("items dispatched %v; want each of 0..9 once", seen)
		}
	}
	run := g.final(t)
	node := taskRunOf(t, run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != "ONE" ||
		!strings.HasPrefix(node.GetError().GetMessage(), "1 of 10 items failed; item 1: ") {
		t.Fatalf("run %v, node %v; want item 1's failure counted across the switch", run.GetStatus(), node.GetError())
	}
}

// TestEachContinue_RefusedItemsCountAsFailed: items the engine refuses before
// dispatch (INPUT_TOO_LARGE) are failed items to a continue fan-out: with
// every item refused it still ends, failing with item 0's reason, and with a
// bound the refusal after it fails the fan-out fast.
func TestEachContinue_RefusedItemsCountAsFailed(t *testing.T) {
	big := strings.Repeat("x", 20000)
	for _, tc := range []struct {
		name, each, prefix string
		canceled           int
	}{
		{"every item refused", ", on_item_failure: continue", "5 of 5 items failed; item 0: ", 0},
		{"the bound tripped by a refusal", ", on_item_failure: continue, max_failed_items: 2", "more than max_failed_items 2: 3 of 5 items failed; item 0: ", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEachHarness(t, failing(nil))
			run := h.run(t, "run-continue-refused", fmt.Sprintf(eachInputDocument, 1000, tc.each, `, big: "`+big+`"`),
				map[string]any{"regions": []any{"a", "b", "c", "d", "e"}})
			node := taskRunOf(t, run, "convert")
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != task.ReasonInputTooLarge ||
				!strings.HasPrefix(node.GetError().GetMessage(), tc.prefix) {
				t.Fatalf("run %v, node %v; want %q", run.GetStatus(), node.GetError(), tc.prefix)
			}
			canceled := 0
			for _, item := range node.GetItems() {
				if item.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
					canceled++
				}
			}
			if canceled != tc.canceled || len(h.itemCalls("convert")) != 0 {
				t.Fatalf("%d items CANCELED, %d reached an Orker; want %d and none", canceled, len(h.itemCalls("convert")), tc.canceled)
			}
		})
	}
}

// TestEachContinue_EveryItemFails: a continue fan-out whose every item fails
// ends — once — with item 0's reason.
func TestEachContinue_EveryItemFails(t *testing.T) {
	codes := map[int32]string{}
	for i := int32(0); i < 6; i++ {
		codes[i] = fmt.Sprintf("CODE_%d", i)
	}
	h := newEachHarness(t, failing(codes))
	run := h.run(t, "run-continue-all", continueDocument(", max_parallel: 2, on_item_failure: continue", ""), map[string]any{"parts": guardItems(6)})
	node := taskRunOf(t, run, "convert")
	if node.GetError().GetCode() != "CODE_0" || !strings.HasPrefix(node.GetError().GetMessage(), "6 of 6 items failed; item 0: ") {
		t.Fatalf("node error = %v", node.GetError())
	}
	failed := 0
	for _, ev := range h.eventsFor("run-continue-all", "convert") {
		if ev.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && ev.Item == nil {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("%d node TASK_FAILED events; want exactly one", failed)
	}
}
