package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// guardedPlan is defaultPlan with a history limit at the default reserve, as
// pinRunTreeLimits sets on a top-level plan at submit.
func guardedPlan(maxEvents, maxBytes int64) engine.RunPlan {
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	plan.History = &engine.HistoryLimit{MaxEvents: maxEvents, MaxBytes: maxBytes, Reserve: engine.DefaultHistoryReserve}
	return plan
}

// onceAt returns an onSnapshot hook that moves the run's raw history length to
// length the first time a snapshot satisfies when. It runs in the snapshot
// write's own goroutine while the workflow waits on it, so the next guard
// check reads the moved counter — the test's way of making the cluster's
// history jump at an exact point of the run.
func onceAt(env *testsuite.TestWorkflowEnvironment, length int, when func(*orkstackv1.Run) bool) func(*orkstackv1.Run, int64) error {
	var once sync.Once
	return func(run *orkstackv1.Run, _ int64) error {
		if when(run) {
			once.Do(func() { env.SetCurrentHistoryLength(length) })
		}
		return nil
	}
}

func taskStatusIs(id string, status orkstackv1.Status) func(*orkstackv1.Run) bool {
	return func(run *orkstackv1.Run) bool {
		for _, tr := range run.GetTaskRuns() {
			if tr.GetTaskId() == id {
				return tr.GetStatus() == status
			}
		}
		return false
	}
}

// requireHistoryLimitError asserts the workflow's own error is the guard's.
func requireHistoryLimitError(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()
	err := env.GetWorkflowError()
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != engine.RunHistoryLimitCode || !applicationErr.NonRetryable() {
		t.Fatalf("workflow error = %v, want a non-retryable %s", err, engine.RunHistoryLimitCode)
	}
}

func busEvents(h *harness, runID string) []*orkstackv1.Event { return h.bus.Events(runID, 0) }

const guardFlatDocument = `
name: guard-flat
tasks:
  - id: slow
    uses: shell
    needs: []
    with: {op: exec}
  - id: fast
    uses: shell
    needs: []
    with: {op: exec}
  - id: after
    uses: shell
    needs: [fast]
    with: {op: exec}
`

// TestHistoryGuard_FailsRunHistoryLimitBeforeTheClusterLimit: once the run's
// history plus what its failure path still needs reaches the limit, the run
// stops FAILED, RUN_HISTORY_LIMIT, InfrastructureError, naming how much was
// recorded and what was still running; its workflow error carries the same
// type, so Temporal records it Failed — never terminated.
func TestHistoryGuard_FailsRunHistoryLimitBeforeTheClusterLimit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "slow" {
			return runUntilCanceled(ctx, nil)
		}
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	h.onSnapshot = onceAt(h.env, 900, taskStatusIs("fast", orkstackv1.Status_STATUS_SUCCEEDED))
	asked := 0
	h.env.OnGetVersion(engine.HistoryGuardChange, workflow.DefaultVersion, workflow.Version(1)).
		Run(func(mock.Arguments) { asked++ }).Return(workflow.Version(1)).Once()

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-guarded", compile(t, guardFlatDocument), payload(t, nil),
		guardedPlan(1000, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	requireHistoryLimitError(t, h.env)
	run := mustFinalRun(t, h.store, "ork-guarded")
	failure := run.GetError()
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != engine.RunHistoryLimitCode ||
		failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR || failure.GetRetryable() {
		t.Fatalf("run = %v / %v, want FAILED %s InfrastructureError, not retryable", run.GetStatus(), failure, engine.RunHistoryLimitCode)
	}
	wantMessage := "run ork-guarded stopped before reaching the cluster's history limit: 900 of 1,000 events " +
		"recorded, 100 kept back so it could end cleanly. Still running: slow"
	if failure.GetMessage() != wantMessage {
		t.Fatalf("message = %q\nwant      %q", failure.GetMessage(), wantMessage)
	}
	if failure.GetDetails() != "still running: slow" {
		t.Fatalf("details = %q, want the blockers", failure.GetDetails())
	}
	if got := statusOf(run, "slow"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("slow = %v, want CANCELED: the run stopped it", got)
	}
	if got := statusOf(run, "after"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("after = %v, want CANCELED: it never started", got)
	}
	events := busEvents(h, "ork-guarded")
	last := events[len(events)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetTaskId() != "" {
		t.Fatalf("last event = %v task %q, want RUN_FAILED owned by the run", last.GetType(), last.GetTaskId())
	}
	if asked != 1 {
		t.Fatalf("the guard's version was asked %d times, want once, at the crossing", asked)
	}
}

// TestHistoryGuard_NoMarkerBelowThreshold: a run that never nears its limit
// records no version marker for the guard.
func TestHistoryGuard_NoMarkerBelowThreshold(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	h.env.OnGetVersion(engine.HistoryGuardChange, workflow.DefaultVersion, workflow.Version(1)).
		Return(workflow.Version(1)).Never()
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-small", compile(t, guardFlatDocument), payload(t, nil),
		guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("a small run failed: %v", err)
	}
}

// TestHistoryGuard_PlanWithoutLimitNeverChecks: a plan built before the field
// existed — every recorded history's — never enters the guard, however long
// the history.
func TestHistoryGuard_PlanWithoutLimitNeverChecks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	h.env.SetCurrentHistoryLength(10_000_000)
	h.env.SetCurrentHistorySize(1 << 40)
	h.env.OnGetVersion(engine.HistoryGuardChange, workflow.DefaultVersion, workflow.Version(1)).
		Return(workflow.Version(1)).Never()
	plan := defaultPlan()
	plan.History = nil
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-unguarded", compile(t, guardFlatDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("a run without a limit failed: %v", err)
	}
	if run := mustFinalRun(t, h.store, "ork-unguarded"); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
}

const guardBatchDocument = `
name: guard-batch
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 1000%s}
    with: {op: exec, r: "{{ .each.item }}"}
`

func guardItems(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = fmt.Sprintf("r%d", i)
	}
	return out
}

func runGuardedFanOut(t *testing.T, runID, maxParallel string, length int, maxEvents int64) (*harness, *orkstackv1.Run) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	h.env.SetCurrentHistoryLength(length)
	doc := fmt.Sprintf(guardBatchDocument, maxParallel)
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc),
		payload(t, map[string]any{"regions": guardItems(125)}), guardedPlan(maxEvents, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	requireHistoryLimitError(t, h.env)
	return h, mustFinalRun(t, h.store, runID)
}

// TestHistoryGuard_StopsADispatchBatchMidTask: 125 items of a 1,000-item
// budget are one dispatch batch, issued in one workflow task. The guard counts
// each item's command before the raw counters do, so the crossing falls
// exactly where the plan's reserve puts it — inside the batch — and no item
// past it is scheduled, and none is marked FAILED.
//
// The arithmetic: raw 1,000 events, unmoved; before item i the history is
// 1,000 + i and the reserve Fixed 96 + the expanded fan-out's 8 + 16 per item
// in flight, so 1,104 + 17i reaches a 2,124-event limit at i = 60.
func TestHistoryGuard_StopsADispatchBatchMidTask(t *testing.T) {
	_, run := runGuardedFanOut(t, "ork-batch", "", 1000, 2124)
	node := taskRunOf(t, run, "convert")
	// Scheduled in the same workflow task the guard stopped in, the 60 are
	// cancelled before any reaches an Orker; scheduled_at is what says each
	// was dispatched.
	dispatched := 0
	for _, item := range node.GetItems() {
		if item.GetScheduledAt() != nil {
			dispatched++
		}
	}
	if dispatched != 60 {
		t.Fatalf("%d items were dispatched, want exactly 60: the guard's exact count crosses at item 60", dispatched)
	}
	for _, item := range node.GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_FAILED {
			t.Fatalf("item %d is FAILED: an item the guard stopped is never marked failed", item.GetItem())
		}
		if item.GetItem() >= 60 && item.GetScheduledAt() != nil {
			t.Fatalf("item %d was scheduled past the crossing", item.GetItem())
		}
	}
	if want := "run ork-batch stopped before reaching the cluster's history limit: 1,060 of 2,124 events " +
		"recorded, 1,064 kept back so it could end cleanly. Still running: convert[0], convert[1], convert[2], " +
		"convert[3], convert[4], …"; run.GetError().GetMessage() != want {
		t.Fatalf("message = %q\nwant      %q", run.GetError().GetMessage(), want)
	}
}

// TestHistoryGuard_StopsFanOutDispatchMidBatch: the guard trips inside a
// fan-out's refill (max_parallel), and the run — not the fan-out — owns the
// reason: the node is CANCELED with its unfinished items, never FAILED, and
// RUN_FAILED names no task.
func TestHistoryGuard_StopsFanOutDispatchMidBatch(t *testing.T) {
	h, run := runGuardedFanOut(t, "ork-refill", ", max_parallel: 2", 1000, 1170)
	dispatched := 0
	for _, item := range taskRunOf(t, run, "convert").GetItems() {
		if item.GetScheduledAt() != nil {
			dispatched++
		}
	}
	if dispatched <= 2 || dispatched >= 125 {
		t.Fatalf("%d items dispatched, want the guard to stop the fan-out part-way", dispatched)
	}
	if run.GetError().GetCode() != engine.RunHistoryLimitCode || run.GetError().GetDetails() == "" ||
		strings.Contains(run.GetError().GetDetails(), `task "convert"`) {
		t.Fatalf("run error = %v, want the run's own RUN_HISTORY_LIMIT", run.GetError())
	}
	node := taskRunOf(t, run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_CANCELED || node.GetError().GetCode() == engine.RunHistoryLimitCode {
		t.Fatalf("node = %v / %v, want CANCELED without the run's reason", node.GetStatus(), node.GetError())
	}
	for _, item := range node.GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_FAILED {
			t.Fatalf("item %d is FAILED", item.GetItem())
		}
	}
	events := busEvents(h, "ork-refill")
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetTaskId() != "" {
		t.Fatalf("last event = %v task %q, want RUN_FAILED owned by the run", last.GetType(), last.GetTaskId())
	}
}

const guardHandlersDocument = `
name: guard-handlers
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [b]
    join: all_done
    with: {op: exec}
`

// runGuardWithHandlers trips the guard right after a succeeds, with b still
// to start and its failure handler rollback behind it. 850 + the reserve (96
// + 8 for each of b and rollback + 48 for the handler) = 1,010 crosses a
// 1,000-event limit; the failing phase's own line, 850 + 96 + 8 for the one
// pending handler, does not, so the handler runs.
func runGuardWithHandlers(t *testing.T) (*harness, *orkstackv1.Run) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	h.onSnapshot = onceAt(h.env, 850, taskStatusIs("a", orkstackv1.Status_STATUS_SUCCEEDED))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-handlers", compile(t, guardHandlersDocument), payload(t, nil),
		guardedPlan(1000, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	requireHistoryLimitError(t, h.env)
	return h, mustFinalRun(t, h.store, "ork-handlers")
}

// TestHistoryGuard_WithHandlersRunsHandlersAndFailsRunHistoryLimit: a run
// with failure handlers that reaches its limit fails RUN_HISTORY_LIMIT, owned
// by no task — F4 has no task to record — and its handlers run within the
// history kept back for them.
func TestHistoryGuard_WithHandlersRunsHandlersAndFailsRunHistoryLimit(t *testing.T) {
	_, run := runGuardWithHandlers(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.RunHistoryLimitCode {
		t.Fatalf("run = %v / %v, want FAILED %s", run.GetStatus(), run.GetError(), engine.RunHistoryLimitCode)
	}
	if got := statusOf(run, "a"); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("a = %v, want SUCCEEDED", got)
	}
	if got := statusOf(run, "b"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("b = %v, want CANCELED", got)
	}
	if got := statusOf(run, "rollback"); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("rollback = %v, want SUCCEEDED: failure handlers run on a run that failed", got)
	}
}

// TestHistoryGuard_RunLevelFailureTextsNameNoTask: the not-a-handler reason
// and the failing note take their run-level forms, naming no task.
func TestHistoryGuard_RunLevelFailureTextsNameNoTask(t *testing.T) {
	h, run := runGuardWithHandlers(t)
	if got := taskRunOf(t, run, "b"); got.GetError() != nil && strings.Contains(got.GetError().GetMessage(), `task ""`) {
		t.Fatalf("b's record names an empty task: %v", got.GetError())
	}
	var canceled, failing string
	for _, e := range busEvents(h, "ork-handlers") {
		switch {
		case e.GetTaskId() == "b" && e.GetStatus() == orkstackv1.Status_STATUS_CANCELED:
			canceled = e.GetMessage()
		case e.GetTaskId() == "" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
			failing = e.GetMessage()
		}
	}
	if want := "task canceled: the run is failing (it reached its history limit) and this task is not a failure handler"; canceled != want {
		t.Errorf("b's cancellation = %q, want %q", canceled, want)
	}
	if want := "run failing: it reached its history limit; waiting for its failure handlers: rollback"; failing != want {
		t.Errorf("failing note = %q, want %q", failing, want)
	}
}

const guardFailingPhaseDocument = `
name: guard-failing
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [a]
    join: all_done
    with: {op: exec}
  - id: cleanup
    uses: shell
    needs: [a]
    join: all_done
    with: {op: exec}
`

// TestHistoryGuard_FailingPhaseStopsStartingHandlers: a run already failing
// for its own reason starts a failure handler only while its history holds
// the handler's terminal path (check point 4); past it every pending handler
// is CANCELED with the history sentence, and the run keeps its own reason.
func TestHistoryGuard_FailingPhaseStopsStartingHandlers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "a" {
			return nil, temporal.NewNonRetryableApplicationError("a exploded", "ActivityFailed", nil)
		}
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	// 900 + Fixed 96 + 8 for each of two pending handlers = 1,012.
	h.onSnapshot = onceAt(h.env, 900, taskStatusIs("a", orkstackv1.Status_STATUS_FAILED))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-failing", compile(t, guardFailingPhaseDocument), payload(t, nil),
		guardedPlan(1000, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	run := mustFinalRun(t, h.store, "ork-failing")
	if run.GetError().GetCode() != "RUN_FAILED" || run.GetError().GetDetails() != `task "a"` {
		t.Fatalf("run error = %v, want a's own failure", run.GetError())
	}
	if got := len(h.taskCalls("notify")) + len(h.taskCalls("cleanup")); got != 0 {
		t.Fatalf("%d handlers were dispatched past the line", got)
	}
	for _, id := range []string{"notify", "cleanup"} {
		if got := statusOf(run, id); got != orkstackv1.Status_STATUS_CANCELED {
			t.Errorf("%s = %v, want CANCELED", id, got)
		}
	}
	found := 0
	for _, e := range busEvents(h, "ork-failing") {
		if e.GetMessage() == "task canceled: the run reached its history limit before this failure handler could start" {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("%d handlers carry the history sentence, want 2", found)
	}
}

// taskCalls is every ExecuteTask the harness received for taskID.
func (h *harness) taskCalls(taskID string) []*orkstackv1.ExecuteTaskInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*orkstackv1.ExecuteTaskInput
	for _, in := range h.calls {
		if in.GetTaskId() == taskID {
			out = append(out, in)
		}
	}
	return out
}

const guardWideChildDocument = `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: scan
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: notarize
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: archive
    uses: shell
    with: { op: eval, command: ["true"] }
`

// TestComposedTask_ChildHistoryLimitReachesTheParentWithItsCategory: a child
// run gets its parent's history limit through startChild's copy, reaches it on
// its own, and the parent's composed task records CHILD_RUN_FAILED with
// InfrastructureError and the child's own sentence. Under a 125-event limit
// the parent (three tasks: 96 + 24) never crosses; the child (four: 96 + 32)
// does at its first check.
func TestComposedTask_ChildHistoryLimitReachesTheParentWithItsCategory(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	h.realChild()
	parent, plan, _ := composedFixture(t, guardWideChildDocument)
	plan.History = &engine.HistoryLimit{MaxEvents: 125, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	h.run("ork-parent", parent, payload(t, nil), plan)
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() == nil {
		t.Fatalf("the parent did not fail: %v", h.env.GetWorkflowError())
	}
	childID := plan.Children["release"].RunID
	child, ok := h.store.Get(childID)
	if !ok || child.GetError().GetCode() != engine.RunHistoryLimitCode {
		t.Fatalf("child = %v, want it to have failed %s itself", child.GetError(), engine.RunHistoryLimitCode)
	}
	parentRun, _ := h.store.Get("ork-parent")
	release := taskRunOfRun(parentRun, "release")
	if release.GetStatus() != orkstackv1.Status_STATUS_FAILED || release.GetError().GetCode() != engine.ChildRunFailedCode ||
		release.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Fatalf("release = %v / %v, want FAILED %s InfrastructureError", release.GetStatus(), release.GetError(), engine.ChildRunFailedCode)
	}
	if !strings.Contains(release.GetError().GetMessage(), "stopped before reaching the cluster's history limit") {
		t.Fatalf("release message = %q, want the child's own sentence", release.GetError().GetMessage())
	}
	if parentRun.GetError().GetCode() == engine.RunHistoryLimitCode {
		t.Fatal("the parent reports the child's history limit as its own")
	}
}

const guardMiddleChildDocument = `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.inner.output.value }}'
tasks:
  - id: inner
    uses: orkflow:leaf@1.0.0
    with:
      image: '{{ .inputs.image }}'
`

const guardLeafDocument = `
name: leaf
version: "1.0.0"
inputs:
  image: { type: string, required: true }
outputs:
  value: '{{ .tasks.work.output.value }}'
tasks:
  - id: work
    uses: shell
    with: { op: eval, command: ["true"] }
`

// TestStartChild_CopiesTheRunTreeLimits: the child's START plan carries its
// parent's History, a grandchild inherits it through the child's own copy,
// the nested plans the parent carries never do, and a parent plan without it
// starts its child byte-identically to before.
func TestStartChild_CopiesTheRunTreeLimits(t *testing.T) {
	const leafDigest = "sha256:" + "1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf1eaf"
	tree := func(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
		parent, plan, _ := composedFixture(t, guardMiddleChildDocument)
		middle := plan.Children["release"]
		middleDef, err := middle.Definition()
		if err != nil {
			t.Fatal(err)
		}
		middleDef.Tasks[0].Orkflow.DefinitionDigest = leafDigest
		leaf := compile(t, guardLeafDocument)
		leaf.DefinitionDigest = leafDigest
		leafPlan := defaultPlan()
		leafPlan.CommitRoot = "mem://commits"
		leafChild, err := engine.NewChildRunPlan(leaf, leafPlan, true, true)
		if err != nil {
			t.Fatal(err)
		}
		leafChild.RunID = "ork-1758790021000000000-1"
		middlePlan := middle.Plan
		middlePlan.Children = map[string]*engine.ChildRunPlan{"inner": leafChild}
		rebuilt, err := engine.NewChildRunPlan(middleDef, middlePlan, true, true)
		if err != nil {
			t.Fatal(err)
		}
		rebuilt.RunID = middle.RunID
		plan.Children["release"] = rebuilt
		return parent, plan
	}
	run := func(t *testing.T, plan engine.RunPlan, parent *orkstackv1.Orkflow) []childStart {
		h := newComposeHarness(t, defaultBehavior)
		h.mockChild(func(ctx workflow.Context, start childStart) (*orkstackv1.Run, error) {
			if len(start.plan.Children) > 0 {
				// The middle run: the real interpreter, so its own startChild
				// copies what it was started with.
				return engine.RunWorkflow(ctx, start.runID, start.wf, start.input, start.plan, nil)
			}
			return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED,
				Output: payload(t, map[string]any{"value": "leaf-ok"})}, nil
		})
		h.run("ork-parent", parent, payload(t, nil), plan)
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("the tree failed: %v", err)
		}
		return h.startedChildren()
	}

	t.Run("copied to every start, never to a nested plan", func(t *testing.T) {
		parent, plan := tree(t)
		limit := &engine.HistoryLimit{MaxEvents: 1 << 40, MaxBytes: 1 << 50, Reserve: engine.DefaultHistoryReserve}
		plan.History = limit
		starts := run(t, plan, parent)
		if len(starts) != 2 {
			t.Fatalf("%d child starts, want the child and the grandchild", len(starts))
		}
		for _, start := range starts {
			if start.plan.History == nil || *start.plan.History != *limit {
				t.Fatalf("start of %s carries History %+v, want the parent's %+v", start.runID, start.plan.History, limit)
			}
			for id, nested := range start.plan.Children {
				if nested.Plan.History != nil {
					t.Fatalf("%s's nested plan for %s carries History", start.runID, id)
				}
			}
		}
		if plan.Children["release"].Plan.History != nil {
			t.Fatal("the parent's nested plan was given History")
		}
	})

	t.Run("a parent without it starts its child as before", func(t *testing.T) {
		parent, plan := tree(t)
		starts := run(t, plan, parent)
		want := plan.Children["release"].Plan
		want.Origin = &engine.RunOrigin{Kind: engine.RunOriginKindParentRun, ID: "ork-parent"}
		var got engine.RunPlan
		for _, start := range starts {
			if start.runID == plan.Children["release"].RunID {
				got = start.plan
			}
		}
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if string(wantJSON) != string(gotJSON) || strings.Contains(string(gotJSON), `"history"`) {
			t.Fatalf("child start plan changed:\n got %s\nwant %s", gotJSON, wantJSON)
		}
	})
}

// TestHistoryGuard_NeverIssuesACommandItsFailurePathCannotFollow: the guard
// is asked before each dispatch whether the run could still end cleanly
// AFTER the dispatch's own command is recorded — not only now. A run whose
// history plus its reserve fits, but would not once one more task's input (up
// to the plan's values bound) is recorded, stops RUN_HISTORY_LIMIT before
// that dispatch: dispatching first, and checking after, could leave its
// failure path less room than it keeps back, and the cluster would terminate
// it (continuation chaos C24, a 128 KiB input per dispatch, terminated at
// 50 MiB). The lookahead is asked lazily, so a run that never meets the
// difference records no marker.
func TestHistoryGuard_NeverIssuesACommandItsFailurePathCannotFollow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	const raw = 1 << 20
	h.env.SetCurrentHistorySize(raw)
	// Room for the run's reserve now (about 52 KiB), not for the reserve and
	// one more task's input (the 256 KiB values bound and its command) too.
	plan := guardedPlan(engine.DefaultHistoryMaxEvents, raw+200<<10)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-lookahead", compile(t, guardFlatDocument), payload(t, nil), plan,
		(*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	requireHistoryLimitError(t, h.env)
	run := mustFinalRun(t, h.store, "ork-lookahead")
	for _, id := range []string{"slow", "fast", "after"} {
		if row := taskRunOf(t, run, id); row.GetScheduledAt() != nil {
			t.Fatalf("%s was dispatched; the first dispatch's command alone leaves the failure path no room", id)
		}
	}
	if run.GetError().GetCode() != engine.RunHistoryLimitCode || statusOf(run, "slow") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run %v, slow %v: want RUN_HISTORY_LIMIT with every task canceled before it started", run.GetError(), statusOf(run, "slow"))
	}
}

// TestHistoryGuard_LookaheadAskedOnlyWhereItDiffers: a run whose next
// command always fits never asks the lookahead's version.
func TestHistoryGuard_LookaheadAskedOnlyWhereItDiffers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	})
	h.env.OnGetVersion(engine.HistoryGuardLookaheadChange, workflow.DefaultVersion, workflow.Version(1)).
		Return(workflow.Version(1)).Never()
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-roomy", compile(t, guardFlatDocument), payload(t, nil),
		guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("a small run failed: %v", err)
	}
}
