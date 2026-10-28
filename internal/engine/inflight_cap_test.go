package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// cappedPlan is guardedPlan with an in-flight cap, as pinRunTreeLimits pins
// on every new top-level plan; a test cap may sit below the boot minimum.
func cappedPlan(maxInFlight, hardStop int32) engine.RunPlan {
	plan := guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)
	plan.InFlight = &engine.InFlightLimit{Max: maxInFlight, HardStop: hardStop}
	return plan
}

// runningPeak records, over every snapshot a run writes, the most task rows,
// items and child runs RUNNING at once. A dispatched task stays RUNNING
// until the loop collects it — a completion that landed but was not yet
// collected is still pending to the cluster — so the peak is exactly what
// the cap bounds.
type runningPeak struct {
	mu   sync.Mutex
	peak int
}

func (p *runningPeak) observe(run *orkstackv1.Run, _ int64) error {
	running := 0
	for _, row := range run.GetTaskRuns() {
		if row.GetEach() != nil && row.GetEach().GetExpanded() {
			for _, item := range row.GetItems() {
				if item.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
					running++
				}
			}
			continue
		}
		if row.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			running++
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peak = max(p.peak, running)
	return nil
}

func (p *runningPeak) value() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// flatDocument is n tasks that all start together.
func flatDocument(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: flat\ntasks:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	return b.String()
}

const wideFanOutDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: wide
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 100}
    with: {op: exec, part: "{{ .each.item }}"}
`

func runCapped(t *testing.T, runID, doc string, input *orkstackv1.Payload, plan engine.RunPlan) (*harness, *runningPeak) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, itemBehavior)
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: productionDeadlock()})
	peak := &runningPeak{}
	h.onSnapshot = peak.observe
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc), input, plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	if run := mustFinalRun(t, h.store, runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	return h, peak
}

// TestInFlightCap_HoldsAtTheCap: tasks and items wait PENDING at the cap,
// exactly as on a saturated pool, and the run completes: the peak of work
// pending at once is the cap, never more.
func TestInFlightCap_HoldsAtTheCap(t *testing.T) {
	t.Run("tasks", func(t *testing.T) {
		h, peak := runCapped(t, "ork-cap-tasks", flatDocument(30), payload(t, nil), cappedPlan(16, 1900))
		if peak.value() != 16 {
			t.Fatalf("peak pending = %d, want the cap of 16", peak.value())
		}
		if len(h.invocations()) != 30 {
			t.Fatalf("%d tasks ran, want 30", len(h.invocations()))
		}
	})
	t.Run("items", func(t *testing.T) {
		h, peak := runCapped(t, "ork-cap-items", wideFanOutDocument, spreadInput(t, 40), cappedPlan(16, 1900))
		if peak.value() != 16 {
			t.Fatalf("peak pending items = %d, want the cap of 16", peak.value())
		}
		if len(h.invocations()) != 40 {
			t.Fatalf("%d items ran, want 40", len(h.invocations()))
		}
	})
}

// TestInFlightCap_FanOutWithoutMaxParallelStaysUnderTheCap: a fan-out
// without max_parallel beside a flat stage never takes the run past its cap.
func TestInFlightCap_FanOutWithoutMaxParallelStaysUnderTheCap(t *testing.T) {
	doc := wideFanOutDocument + `  - id: side1
    uses: shell
    needs: []
    with: {op: exec}
  - id: side2
    uses: shell
    needs: []
    with: {op: exec}
`
	_, peak := runCapped(t, "ork-cap-mixed", doc, spreadInput(t, 50), cappedPlan(20, 1900))
	if peak.value() > 20 {
		t.Fatalf("peak pending = %d, over the cap of 20", peak.value())
	}
}

// TestInFlightCap_NeverHoldsWithNothingInFlight: a cap of 1 never holds the
// first dispatch of a pass that starts with nothing in flight (N1): a flat
// stage runs one task at a time and completes.
func TestInFlightCap_NeverHoldsWithNothingInFlight(t *testing.T) {
	h, peak := runCapped(t, "ork-cap-one", flatDocument(3), payload(t, nil), cappedPlan(1, 1900))
	if peak.value() != 1 || len(h.invocations()) != 3 {
		t.Fatalf("peak %d over %d tasks, want one at a time over all 3", peak.value(), len(h.invocations()))
	}
}

// TestInFlightCap_GateIsLazy: a run that never reaches its cap records no
// run-in-flight-cap marker; one that does records exactly one.
func TestInFlightCap_GateIsLazy(t *testing.T) {
	for _, tc := range []struct {
		name string
		cap  int32
		want int
	}{{"below the cap", 16, 0}, {"at the cap", 2, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarnessCtx(t, &suite, itemBehavior)
			r := recordCommands(h)
			h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-cap-lazy", compile(t, flatDocument(4)), payload(t, nil),
				cappedPlan(tc.cap, 1900), (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if got := r.count("version:" + engine.InFlightCapChange); got != tc.want {
				t.Fatalf("%d run-in-flight-cap markers, want %d", got, tc.want)
			}
		})
	}
}

const pinNeedsUnpinnedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: pinned-needs
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    needs: []
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: fetch
    uses: shell
    needs: []
    with: {command: "echo fetch"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep, fetch]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: other1
    uses: shell
    needs: [prep]
    with: {command: "echo other"}
  - id: other2
    uses: shell
    needs: [prep]
    with: {command: "echo other"}
`

// enablePins turns on Temporal's session worker in a harness and registers
// the engine's ReleasePin, as newPinHarness does.
func enablePins(h *harness, options worker.Options) {
	options.EnableSessionWorker = true
	h.env.SetWorkerOptions(testWorkerOptions(options))
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ReleasePinInput) error {
		h.mu.Lock()
		h.released = append(h.released, in)
		h.mu.Unlock()
		return nil
	}, activity.RegisterOptions{Name: task.ActivityReleasePin})
}

// TestInFlightCap_NeverSpins proves the cap never holds with nothing in
// flight and never deadlocks a pin, under the production 1 s deadlock
// detector, with plans built by hand below the boot minimum.
func TestInFlightCap_NeverSpins(t *testing.T) {
	t.Run("a cap of 1 over a flat stage", func(t *testing.T) {
		h, _ := runCapped(t, "ork-spin-a", flatDocument(3), payload(t, nil), cappedPlan(1, 1900))
		if len(h.invocations()) != 3 {
			t.Fatalf("%d tasks ran, want 3", len(h.invocations()))
		}
	})
	for _, tc := range []struct {
		name string
		cap  int32
	}{{"a cap of 2 with an open pin whose member needs an unpinned task", 2}, {"a cap of 1, the pin's closure exempt", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarnessCtx(t, &suite, itemBehavior)
			enablePins(h, worker.Options{DeadlockDetectionTimeout: productionDeadlock()})
			h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-spin-pin", compile(t, pinNeedsUnpinnedDocument), payload(t, nil),
				cappedPlan(tc.cap, 1900), (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("the run failed: %v", err)
			}
			if got := strings.Join(sortedTaskOrder(h), ","); got != "compile,fetch,other1,other2,prep" {
				t.Fatalf("tasks ran %q", got)
			}
		})
	}
	t.Run("two pins open with nothing in flight", func(t *testing.T) {
		const doc = `
apiVersion: orkstack/v1
kind: Orkflow
name: two-pins
pins:
  one: {}
  two: {}
tasks:
  - id: a1
    uses: shell
    pin: one
    needs: []
    run_as_orker_user: true
    with: {command: "echo"}
  - id: b1
    uses: shell
    pin: two
    needs: []
    run_as_orker_user: true
    with: {command: "echo"}
  - id: a2
    uses: shell
    pin: one
    needs: [a1]
    run_as_orker_user: true
    with: {command: "echo"}
  - id: b2
    uses: shell
    pin: two
    needs: [b1]
    run_as_orker_user: true
    with: {command: "echo"}
  - id: tail
    uses: shell
    needs: [a2, b2]
    with: {command: "echo"}
`
		var suite testsuite.WorkflowTestSuite
		h := newHarnessCtx(t, &suite, itemBehavior)
		enablePins(h, worker.Options{DeadlockDetectionTimeout: productionDeadlock()})
		h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-spin-pins", compile(t, doc), payload(t, nil),
			cappedPlan(2, 1900), (*orkstackv1.RunContinuation)(nil))
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("the run failed: %v", err)
		}
		if len(h.invocations()) != 5 {
			t.Fatalf("%d tasks ran, want 5", len(h.invocations()))
		}
	})
}

func sortedTaskOrder(h *harness) []string {
	order := h.taskOrder()
	out := append([]string(nil), order...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// pinnedOverCap is a pin open while unpinned work fills the cap: compile,
// the pin's member, needs fetch and fan, both in the pin's closure.
const pinnedOverCapDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: pin-over-cap
pins:
  build: {}
inputs:
  parts: {type: array}
tasks:
  - id: prep
    uses: shell
    pin: build
    needs: []
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: noise
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 100}
    needs: []
    with: {op: exec}
  - id: fetch
    uses: shell
    needs: [prep]
    with: {command: "echo fetch"}
  - id: compile
    uses: shell
    pin: build
    needs: [fetch]
    run_as_orker_user: true
    with: {command: "echo compile"}
`

// TestInFlightCap_PinClosureIsExemptBelowTheHardStop: at the cap, an open
// pin's needs-closure still dispatches, so the pin closes while unpinned
// work waits, and the run completes.
func TestInFlightCap_PinClosureIsExemptBelowTheHardStop(t *testing.T) {
	pinClosureUnderTheCap(t, 4, 1900)
}

// TestInFlightCap_HardStopHoldsEvenThePin: at the plan's hard stop even an
// open pin's closure waits, so pending work stays below it, and the run
// still completes.
func TestInFlightCap_HardStopHoldsEvenThePin(t *testing.T) {
	pinClosureUnderTheCap(t, 100, 4)
}

func pinClosureUnderTheCap(t *testing.T, maxInFlight, hardStop int32) {
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	var order []string
	// Below the hard stop every fan-out item is held at the gate until
	// compile, the pin's member, has started: the items keep the cap full
	// for as long as the pin needs, so the pin's closure dispatches only if
	// the run exempts it. Without the exemption nothing could free a slot,
	// so the gate's deadline releases the items and the test fails on
	// gateTimedOut — a discriminator that does not depend on the order in
	// which completions are collected. At the hard stop the pin is meant to
	// wait, so nothing is gated.
	gated := hardStop == 1900
	compileStarted := make(chan struct{})
	var startOnce sync.Once
	var gateTimedOut bool
	h := newHarnessCtx(t, &suite, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		if in.GetEach() == nil {
			order = append(order, in.GetTaskId())
		}
		mu.Unlock()
		if in.GetTaskId() == "compile" {
			startOnce.Do(func() { close(compileStarted) })
		}
		if gated && in.GetEach() != nil {
			select {
			case <-compileStarted:
			case <-time.After(10 * time.Second):
				mu.Lock()
				gateTimedOut = true
				mu.Unlock()
				startOnce.Do(func() { close(compileStarted) })
			}
		}
		return itemBehavior(ctx, in)
	})
	enablePins(h, worker.Options{DeadlockDetectionTimeout: productionDeadlock()})
	peak := &runningPeak{}
	h.onSnapshot = peak.observe
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-cap-pin", compile(t, pinnedOverCapDocument),
		spreadInput(t, 30), cappedPlan(maxInFlight, hardStop), (*orkstackv1.RunContinuation)(nil))
	if gateTimedOut {
		// The pin's closure was never dispatched while the capped fan-out
		// held every slot: the pin waited on the cap.
		t.Fatal("compile did not start while the fan-out's items held the cap: the pin's closure was not exempt")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	if run := mustFinalRun(t, h.store, "ork-cap-pin"); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	if len(order) != 3 {
		t.Fatalf("ran %v, want prep, fetch and compile", order)
	}
	if peak.value() >= int(hardStop) {
		// The hard stop counts the one publish an emit may have
		// outstanding too, so the rows pending stay below it.
		t.Fatalf("peak pending %d reached the hard stop %d", peak.value(), hardStop)
	}
}

// TestInFlightCap_ChildRunInheritsTheParentsCap: a composed child's start
// plan carries the parent's cap, copied from the parent's own plan, like
// every run-tree limit.
func TestInFlightCap_ChildRunInheritsTheParentsCap(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes,
		Reserve: engine.DefaultHistoryReserve}
	plan.InFlight = &engine.InFlightLimit{Max: 300, HardStop: 1900}
	budget := engine.DefaultContinuationBudget(500, 1<<20, time.Hour)
	plan.Continuation = &budget
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	})
	h.run("ork-parent-cap", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(h.children) != 1 {
		t.Fatalf("%d children started", len(h.children))
	}
	child := h.children[0].plan
	if child.InFlight == nil || *child.InFlight != *plan.InFlight {
		t.Fatalf("child cap = %v, want the parent's %v", child.InFlight, plan.InFlight)
	}
	if child.Continuation == nil || *child.Continuation != budget {
		t.Fatalf("child budget = %v, want the parent's", child.Continuation)
	}
	if nested := plan.Children["release"].Plan; nested.InFlight != nil || nested.Continuation != nil {
		t.Fatal("the nested child plan the parent carries holds run-tree limits")
	}
}
