package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// A fan-out over an artifact's lines while its line index is in flight: the
// index is one future among the run's others (fanout-artifacts design §5.2),
// counted, guarded and collected like a dispatch, and never sent twice.

// linesSiblingsDocument is a lines fan-out beside siblings that become ready
// with it: every sibling's completion is a pass of the run's loop while the
// index is still in flight.
const linesSiblingsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: lines-siblings
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/files.jsonl, format: jsonl, max_items: 50}
    with: {op: exec}
%s`

func siblings(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: side%d\n    uses: shell\n    needs: [list]\n    with: {op: exec}\n", i)
	}
	return b.String()
}

// sidesThenIndex answers the index only once n siblings have run, so every
// sibling ran — and was collected — while the index was in flight.
type sidesThenIndex struct {
	n    int32
	done atomic.Int32
	all  chan struct{}
	once sync.Once
}

func newSidesThenIndex(n int32) *sidesThenIndex {
	return &sidesThenIndex{n: n, all: make(chan struct{})}
}

func (s *sidesThenIndex) behavior(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if strings.HasPrefix(in.GetTaskId(), "side") {
		if s.done.Add(1) == s.n {
			s.once.Do(func() { close(s.all) })
		}
	}
	return listWrites(linesSource)(ctx, in)
}

func (s *sidesThenIndex) index(ctx context.Context, _ *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
	select {
	case <-s.all:
		return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Second):
		return nil, errors.New("the siblings never ran while the index was in flight")
	}
}

func exploded(what string) error {
	return temporal.NewNonRetryableApplicationError(what+" exploded", task.CategoryActivityFailed, nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: "CONTAINER_EXIT_NONZERO",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, Attempt: 1})
}

// TestEachLines_IndexIsAFutureSiblingsCollect: the expansion schedules the
// index and goes on; siblings dispatch and are collected while it is in
// flight, and its answer then expands the fan-out.
func TestEachLines_IndexIsAFutureSiblingsCollect(t *testing.T) {
	s := newSidesThenIndex(5)
	h := newLinesHarness(t, s.index, s.behavior)
	run := h.runLines(t, "run-lines-future", fmt.Sprintf(linesSiblingsDocument, siblings(5)))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	for i := 0; i < 5; i++ {
		if got := taskRunOf(t, run, fmt.Sprintf("side%d", i)).GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("side%d = %v", i, got)
		}
	}
	if got := len(h.itemCalls("convert")); got != 3 {
		t.Fatalf("%d items, want 3 after the index answered", got)
	}
}

// TestEachLines_NotRedispatchedWhileIndexing: five passes of the loop run
// while the index is in flight, and none of them sends it again.
func TestEachLines_NotRedispatchedWhileIndexing(t *testing.T) {
	s := newSidesThenIndex(5)
	h := newLinesHarness(t, s.index, s.behavior)
	run := h.runLines(t, "run-lines-once", fmt.Sprintf(linesSiblingsDocument, siblings(5)))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || s.done.Load() != 5 {
		t.Fatalf("run = %v %v after %d siblings", run.GetStatus(), run.GetError(), s.done.Load())
	}
	if got := h.indexCalls(); got != 1 {
		t.Fatalf("indexed %d times; the loop's passes while it was in flight sent it again", got)
	}
}

// TestEachLines_FailFastCollectsIndex: a sibling that fails while the index
// is in flight fails the run with its own reason; the index is collected,
// the fan-out never expands and no item starts.
func TestEachLines_FailFastCollectsIndex(t *testing.T) {
	sideRan := make(chan struct{})
	var once sync.Once
	h := newLinesHarness(t, func(ctx context.Context, _ *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		select {
		case <-sideRan:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, errors.New("side0 never ran while the index was in flight")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
		}
	}, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "side0" {
			once.Do(func() { close(sideRan) })
			return nil, exploded("side")
		}
		return listWrites(linesSource)(ctx, in)
	})
	run := h.runLines(t, "run-lines-failfast", fmt.Sprintf(linesSiblingsDocument, siblings(1)))
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetDetails(), `"side0"`) ||
		taskRunOf(t, run, "side0").GetError().GetCode() != "CONTAINER_EXIT_NONZERO" {
		t.Fatalf("run = %v %v, side0 %v; want FAILED by the sibling", run.GetStatus(), run.GetError(), taskRunOf(t, run, "side0").GetError())
	}
	node := taskRunOf(t, run, "convert")
	if node.GetStatus() == orkstackv1.Status_STATUS_RUNNING || node.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED ||
		node.GetEach().GetExpanded() || len(node.GetItems()) != 0 {
		t.Fatalf("convert = %v %v, expanded %v, %d items; want it ended unexpanded", node.GetStatus(), node.GetError(),
			node.GetEach().GetExpanded(), len(node.GetItems()))
	}
	if h.indexCalls() != 1 || len(h.itemCalls("convert")) != 0 {
		t.Fatalf("indexed %d times, %d items; want one index and no item", h.indexCalls(), len(h.itemCalls("convert")))
	}
}

// TestEachLines_IndexHeldByInFlightCap: the index counts toward the run's
// in-flight cap. With a cap of one, the sibling ready beside it waits for its
// answer: never two at once. A cap of two is the positive control: the index
// and the sibling are then in flight together.
func TestEachLines_IndexHeldByInFlightCap(t *testing.T) {
	for _, tc := range []struct {
		limit int32
		peak  int32
	}{{1, 1}, {2, 2}} {
		t.Run(fmt.Sprintf("cap %d", tc.limit), func(t *testing.T) {
			if got := cappedLinesPeak(t, tc.limit); got != tc.peak {
				t.Fatalf("%d activities in flight at once under a cap of %d, want %d", got, tc.limit, tc.peak)
			}
		})
	}
}

// cappedLinesPeak runs a lines fan-out beside one sibling under an in-flight
// cap and reports the most activities in flight at once. The index answers
// only once the sibling ran, or once its own deadline passes when the cap
// keeps the sibling waiting, so both are in flight together whenever the cap
// lets them be.
func cappedLinesPeak(t *testing.T, limit int32) int32 {
	t.Helper()
	var inFlight, peak atomic.Int32
	sideRan := make(chan struct{})
	var once sync.Once
	enter := func() {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				return
			}
		}
	}
	h := newLinesHarness(t, func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		enter()
		defer inFlight.Add(-1)
		select {
		case <-sideRan:
		case <-time.After(2 * time.Second):
		}
		return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
	}, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		enter()
		defer inFlight.Add(-1)
		if in.GetTaskId() == "side0" {
			once.Do(func() { close(sideRan) })
		}
		return listWrites(linesSource)(ctx, in)
	})
	plan := cappedPlan(limit, 1900)
	plan.EachLines = engine.DefaultEachLinesPolicy()
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-lines-capped", compile(t, fmt.Sprintf(linesSiblingsDocument, siblings(1))),
		payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	run, ok := h.store.Get("run-lines-capped")
	if !h.env.IsWorkflowCompleted() || !ok || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.indexCalls() != 1 || len(h.itemCalls("convert")) != 3 || len(h.itemCalls("side0")) != 1 {
		t.Fatalf("indexed %d times, %d items, side0 %d times", h.indexCalls(), len(h.itemCalls("convert")), len(h.itemCalls("side0")))
	}
	return peak.Load()
}

// TestEachLines_CancelDuringIndexCancelsNode: an operator's cancel while the
// index is in flight ends the node CANCELED, in its own TASK_FAILED event
// with that status; an index that answers cancelled while the run was not is
// a failure of the expansion, never the run's cancel.
func TestEachLines_CancelDuringIndexCancelsNode(t *testing.T) {
	t.Run("the run is cancelled", func(t *testing.T) {
		var h *linesHarness
		h = newLinesHarness(t, func(ctx context.Context, _ *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			h.env.CancelWorkflow()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				return nil, errors.New("never canceled")
			}
		}, listWrites(linesSource))
		run := h.runLines(t, "run-lines-cancel-node", fmt.Sprintf(linesDocument, "", ""))
		node := taskRunOf(t, run, "convert")
		if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || node.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
			node.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
			var seen []string
			for _, ev := range h.eventsFor("run-lines-cancel-node", "convert") {
				seen = append(seen, ev.GetType().String()+" "+ev.GetStatus().String()+" "+ev.GetMessage())
			}
			t.Fatalf("run %v, node %v %v; want both CANCELED; convert's events: %q", run.GetStatus(), node.GetStatus(), node.GetError(), seen)
		}
		var ended *orkstackv1.Event
		for _, ev := range h.eventsFor("run-lines-cancel-node", "convert") {
			if ev.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
				ended = ev
			}
		}
		if ended == nil || ended.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
			t.Fatalf("convert's last event = %v; want TASK_FAILED with status CANCELED", ended)
		}
		if len(node.GetItems()) != 0 || len(h.itemCalls("convert")) != 0 ||
			taskRunOf(t, run, "merge").GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("%d items, merge %v; want nothing after the cancel", len(node.GetItems()), taskRunOf(t, run, "merge").GetStatus())
		}
	})
	t.Run("the index alone answers cancelled", func(t *testing.T) {
		h := newLinesHarness(t, func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			return nil, temporal.NewCanceledError("the orker stopped the index")
		}, listWrites(linesSource))
		run := h.runLines(t, "run-lines-index-canceled", fmt.Sprintf(linesDocument, "", ""))
		node := taskRunOf(t, run, "convert")
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != engine.EachExpansionFailedCode {
			t.Fatalf("run %v, node %v %v; want the expansion FAILED %s", run.GetStatus(), node.GetStatus(), node.GetError(),
				engine.EachExpansionFailedCode)
		}
	})
}

// TestEachLines_ProducerIsFailureHandler: a failure handler may write the
// artifact a fan-out reads (a fan-out may not itself be one). When its own
// need succeeded the handler runs, and the fan-out expands from its artifact;
// when its need failed the run is failing, the handler runs alone, and the
// fan-out — no handler — never indexes.
func TestEachLines_ProducerIsFailureHandler(t *testing.T) {
	const doc = `
apiVersion: orkstack/v1
kind: Orkflow
name: lines-handler
tasks:
  - id: prep
    uses: shell
    with: {op: exec}
  - id: list
    uses: shell
    needs: [prep]
    join: all_done
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/files.jsonl, format: jsonl, max_items: 50}
    with: {op: exec}
`
	for _, tc := range []struct {
		name     string
		prepFail bool
		want     orkstackv1.Status
		items    int
	}{
		{"prep succeeds", false, orkstackv1.Status_STATUS_SUCCEEDED, 3},
		{"prep fails", true, orkstackv1.Status_STATUS_FAILED, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLinesHarness(t, indexes(3, 3), func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				if in.GetTaskId() == "prep" && tc.prepFail {
					return nil, exploded("prep")
				}
				return listWrites(linesSource)(ctx, in)
			})
			run := h.runLines(t, "run-lines-handler", doc)
			if run.GetStatus() != tc.want || taskRunOf(t, run, "list").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run = %v %v, list %v; want %v with the handler run", run.GetStatus(), run.GetError(),
					taskRunOf(t, run, "list").GetStatus(), tc.want)
			}
			if got := len(h.itemCalls("convert")); got != tc.items || h.indexCalls() != tc.items/3 {
				t.Fatalf("%d items, %d indexes; want %d items", got, h.indexCalls(), tc.items)
			}
			if tc.prepFail {
				if st := taskRunOf(t, run, "convert").GetStatus(); st == orkstackv1.Status_STATUS_RUNNING || st == orkstackv1.Status_STATUS_SUCCEEDED {
					t.Fatalf("convert = %v in a failing run", st)
				}
				return
			}
			if st := taskRunOf(t, run, "convert").GetStatus(); st != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("convert = %v", st)
			}
		})
	}
}

// registerIndex gives a harness the IndexEachLines activity.
func registerIndex(h *harness, index func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error)) {
	h.env.RegisterActivityWithOptions(index, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
}

// TestEachLines_GuardChargesIndexDispatch: the index is a dispatch to the
// history guard. A run whose history has no room left when its fan-out
// expands stops as the run, RUN_HISTORY_LIMIT, without sending the index;
// with room it sends it.
func TestEachLines_GuardChargesIndexDispatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		length  int
		indexed int32
	}{
		{"no room left", 999, 0},
		{"room", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarnessCtx(t, nil, listWrites(linesSource))
			var indexed atomic.Int32
			registerIndex(h, func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
				indexed.Add(1)
				return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
			})
			if tc.length > 0 {
				h.onSnapshot = onceAt(h.env, tc.length, taskStatusIs("list", orkstackv1.Status_STATUS_SUCCEEDED))
			}
			plan := guardedPlan(1000, engine.DefaultHistoryMaxBytes)
			plan.EachLines = engine.DefaultEachLinesPolicy()
			h.env.ExecuteWorkflow(engine.RunWorkflow, "run-lines-guard", compile(t, fmt.Sprintf(linesDocument, "", "")),
				payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
			if !h.env.IsWorkflowCompleted() {
				t.Fatal("the run did not end")
			}
			run := mustFinalRun(t, h.store, "run-lines-guard")
			if indexed.Load() != tc.indexed {
				t.Fatalf("indexed %d times, want %d", indexed.Load(), tc.indexed)
			}
			if tc.indexed == 0 {
				requireHistoryLimitError(t, h.env)
				if node := taskRunOf(t, run, "convert"); node.GetStatus() == orkstackv1.Status_STATUS_FAILED {
					t.Fatalf("convert = %v %v; the run, not the fan-out, owns the reason", node.GetStatus(), node.GetError())
				}
				return
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
			}
		})
	}
}

// TestEachLines_SwitchWaitsForIndex: a run that reaches its hold line while
// the index is in flight holds new dispatch and switches only once nothing is
// in flight. The order is forced, not raced: once all three siblings have
// started, the index raises the run's history past its hold line, and only
// then do the siblings finish, so each of their completions is a pass of the
// loop at the hold line with the index still in flight — no moment to switch. The index is answered in the
// generation that sent it and sent once in all, and the next generation
// dispatches every item, once, from the line index it carried.
func TestEachLines_SwitchWaitsForIndex(t *testing.T) {
	plan := holdPlan()
	plan.EachLines = engine.DefaultEachLinesPolicy()
	raised, started := make(chan struct{}), make(chan struct{}, 3)
	var raiseOnce sync.Once
	s := newSidesThenIndex(3)
	var indexed atomic.Int32
	behavior := func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if strings.HasPrefix(in.GetTaskId(), "side") {
			started <- struct{}{}
			select {
			case <-raised:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				return nil, errors.New("the index never raised the history")
			}
		}
		return s.behavior(ctx, in)
	}
	g := runGenerations(t, "ork-lines-switch", compile(t, fmt.Sprintf(linesSiblingsDocument, siblings(3))),
		payload(t, nil), plan, behavior, func(h *harness, generation int) {
			registerIndex(h, func(ctx context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
				indexed.Add(1)
				if generation == 0 {
					for range 3 {
						select {
						case <-started:
						case <-time.After(10 * time.Second):
							return nil, errors.New("the siblings never started beside the index")
						}
					}
					raiseOnce.Do(func() {
						h.env.SetCurrentHistoryLength(650)
						close(raised)
					})
				}
				return s.index(ctx, in)
			})
		})
	run := generationsOK(t, g, 2)
	if indexed.Load() != 1 {
		t.Fatalf("indexed %d times across %d generations; a switch abandoned the index in flight", indexed.Load(), len(g.harnesses))
	}
	if got := len(g.harnesses[0].taskCalls("convert")); got != 0 {
		t.Fatalf("generation 0 dispatched %d items past its hold line", got)
	}
	seen := map[int32]int{}
	for _, h := range g.harnesses {
		for _, in := range h.invocations() {
			if in.GetTaskId() == "convert" {
				seen[in.GetEach().GetIndex()]++
				if in.GetEach().GetLines().GetIndex().GetUri() != linesIndex.GetUri() {
					t.Fatalf("item %d lost its index: %v", in.GetEach().GetIndex(), in.GetEach().GetLines())
				}
			}
		}
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 1 || seen[2] != 1 {
		t.Fatalf("items dispatched %v; want each of 0..2 once", seen)
	}
	if got := taskRunOf(t, run, "convert").GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("convert = %v", got)
	}
}
