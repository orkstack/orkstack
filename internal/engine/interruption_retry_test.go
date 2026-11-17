package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The interpreter's interruptions (E92, design §4.7–4.10), driven through
// RunWorkflow with an ExecuteTask that answers as an Orker does: a typed
// interruption (OrkInterruption:<reason>) carrying its ActivityFailureDetail.

// interruptionError is the error an Orker returns for reason, as emission.go
// builds it.
func interruptionError(reason string, detail *orkstackv1.ActivityFailureDetail) error {
	detail.ReasonCode = reason
	return temporal.NewApplicationErrorWithOptions(reason, "OrkInterruption:"+reason,
		temporal.ApplicationErrorOptions{Details: []any{detail}})
}

func stopBounceError() error {
	return interruptionError("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "orker-a"})
}

func handBackError() error {
	return interruptionError("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true, OrkerId: "orker-a"})
}

func storeFaultError() error {
	return interruptionError("ATTEMPT_STORE_UNAVAILABLE",
		&orkstackv1.ActivityFailureDetail{NeverStarted: true, StoreError: "503 Slow Down", OrkerId: "orker-a"})
}

// interruptionPlan is defaultPlan with the deployment's default budgets.
func interruptionPlan(mutate func(*engine.InterruptionBudgets)) engine.RunPlan {
	plan := defaultPlan()
	budgets := engine.DefaultInterruptionBudgets
	if mutate != nil {
		mutate(&budgets)
	}
	plan.Interruptions = &budgets
	return plan
}

// scriptedBehavior answers each unit's calls from its script in order, then
// succeeds; it counts every call.
type scriptedBehavior struct {
	mu     sync.Mutex
	script map[string][]error
	calls  map[string]int
}

func newScript(script map[string][]error) *scriptedBehavior {
	return &scriptedBehavior{script: script, calls: map[string]int{}}
}

func (b *scriptedBehavior) run(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	key := in.GetTaskId()
	if each := in.GetEach(); each != nil {
		key = fmt.Sprintf("%s#%d", key, each.GetIndex())
	}
	b.mu.Lock()
	n := b.calls[key]
	b.calls[key]++
	b.mu.Unlock()
	if steps := b.script[key]; n < len(steps) && steps[n] != nil {
		return nil, steps[n]
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ExitCode: 0}, nil
}

func (b *scriptedBehavior) count(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[key]
}

const oneTaskDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted
tasks:
  - id: work
    uses: shell
    %s
    with: {op: exec}
`

func runInterrupted(t *testing.T, doc string, plan engine.RunPlan, b *scriptedBehavior, input map[string]any) (*harness, *dispatchCapture, error) {
	t.Helper()
	capture := &dispatchCapture{}
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, b.run)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-interrupted", compile(t, doc), payload(t, input), plan, (*orkstackv1.RunContinuation)(nil))
	return h, capture, h.env.GetWorkflowError()
}

// interruptedRow is the row of task id in run-interrupted's last snapshot.
func interruptedRow(t *testing.T, h *harness, id string) *orkstackv1.TaskRun {
	t.Helper()
	return taskRunOf(t, runOf(t, h, "run-interrupted"), id)
}

func interruptionEvents(h *harness) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range h.bus.Events("run-interrupted", 0) {
		if e.GetInterruption() != nil {
			out = append(out, e)
		}
	}
	return out
}

// TestRunWorkflow_AStopBounceRunsTheTaskAgainWithoutSpendingAnAttempt: a
// one-attempt task its Orker handed back before it started runs again and
// succeeds; each execution listed the never-started class; the run emitted
// one interruption notice and asked the gate.
func TestRunWorkflow_AStopBounceRunsTheTaskAgainWithoutSpendingAnAttempt(t *testing.T) {
	b := newScript(map[string][]error{"work": {stopBounceError()}})
	h, capture, err := runInterrupted(t, fmt.Sprintf(oneTaskDocument, ""), interruptionPlan(nil), b, nil)
	must.OKf(t, err, "workflow error")
	if got := b.count("work"); got != 2 {
		t.Fatalf("work ran %d times, want 2", got)
	}
	row := interruptedRow(t, h, "work")
	if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("work ended %v, want SUCCEEDED", row.GetStatus())
	}
	if st := row.GetInterruptions(); st.GetNeverStarted() != 1 || st.GetOwnAttemptsUsed() != 0 {
		t.Fatalf("interruptions = %v, want never_started 1 and no own attempt", st)
	}
	options := capture.of(task.ActivityExecuteTask)
	if len(options) != 2 {
		t.Fatalf("ExecuteTask scheduled %d times, want 2", len(options))
	}
	for i, o := range options {
		types := o.RetryPolicy.NonRetryableErrorTypes
		if o.RetryPolicy.MaximumAttempts != 1 || len(types) == 0 || types[0] != "OrkInterruption:ORKER_STOPPED" {
			t.Fatalf("dispatch %d policy = %+v, want one attempt listing the never-started class", i, o.RetryPolicy)
		}
	}
	if events := interruptionEvents(h); len(events) != 1 || events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_LOG ||
		!strings.Contains(events[0].GetMessage(), "handed back before it started") {
		t.Fatalf("interruption events = %v, want one TASK_LOG naming the hand-back", events)
	}
	if asked := capture.asked[engine.InterruptionRetryChange]; len(asked) == 0 {
		t.Fatalf("the run never asked %s", engine.InterruptionRetryChange)
	}
}

// TestRunWorkflow_HandBacksPastTheirBudgetSpendOwnAttemptsThenFail: with a
// hand-back budget of 2 and two own attempts, a task that is always handed
// back runs four times and fails, its message naming the spent budget.
func TestRunWorkflow_HandBacksPastTheirBudgetSpendOwnAttemptsThenFail(t *testing.T) {
	b := newScript(map[string][]error{"work": {handBackError(), handBackError(), handBackError(), handBackError()}})
	plan := interruptionPlan(func(p *engine.InterruptionBudgets) { p.HandedBack = 2 })
	h, capture, err := runInterrupted(t, fmt.Sprintf(oneTaskDocument, "retry: {maximum_attempts: 2}"), plan, b, nil)
	if err == nil {
		t.Fatal("the run succeeded; want it failed")
	}
	if got := b.count("work"); got != 4 {
		t.Fatalf("work ran %d times, want 4", got)
	}
	row := interruptedRow(t, h, "work")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		!strings.Contains(row.GetError().GetMessage(), "handed back 2/2") {
		t.Fatalf("work = %v %q, want FAILED naming handed back 2/2", row.GetStatus(), row.GetError().GetMessage())
	}
	var attempts []int32
	for _, o := range capture.of(task.ActivityExecuteTask) {
		attempts = append(attempts, o.RetryPolicy.MaximumAttempts)
	}
	if fmt.Sprint(attempts) != "[2 2 2 1]" {
		t.Fatalf("MaximumAttempts per dispatch = %v, want [2 2 2 1]", attempts)
	}
}

// TestRunWorkflow_ATaskThatOptedOutListsNothing: max_interruptions: 0 lists
// nothing, so a stop bounce fails the task at once, as today.
func TestRunWorkflow_ATaskThatOptedOutListsNothing(t *testing.T) {
	b := newScript(map[string][]error{"work": {stopBounceError()}})
	_, capture, err := runInterrupted(t, fmt.Sprintf(oneTaskDocument, "retry: {max_interruptions: 0}"), interruptionPlan(nil), b, nil)
	if err == nil {
		t.Fatal("the run succeeded; want it failed")
	}
	if got := b.count("work"); got != 1 {
		t.Fatalf("work ran %d times, want 1", got)
	}
	for _, o := range capture.of(task.ActivityExecuteTask) {
		if len(o.RetryPolicy.NonRetryableErrorTypes) != 0 {
			t.Fatalf("an opted-out task listed %v", o.RetryPolicy.NonRetryableErrorTypes)
		}
	}
	if asked := capture.asked[engine.InterruptionRetryChange]; len(asked) != 0 {
		t.Fatalf("an opted-out run asked %s", engine.InterruptionRetryChange)
	}
}

// TestRunWorkflow_ALegacyRunListsNothing: on the DefaultVersion branch a run
// with budgets schedules every task as before the gate.
func TestRunWorkflow_ALegacyRunListsNothing(t *testing.T) {
	defer engine.DisableInterruptionRetryGateForTest()()
	b := newScript(map[string][]error{"work": {stopBounceError()}})
	_, capture, err := runInterrupted(t, fmt.Sprintf(oneTaskDocument, ""), interruptionPlan(nil), b, nil)
	if err == nil {
		t.Fatal("the run succeeded; want it failed")
	}
	for _, o := range capture.of(task.ActivityExecuteTask) {
		if len(o.RetryPolicy.NonRetryableErrorTypes) != 0 || o.RetryPolicy.MaximumAttempts != 1 {
			t.Fatalf("a legacy run scheduled %+v", o.RetryPolicy)
		}
	}
}

// TestRunWorkflow_AStoreFaultWaitsItsFloorBeforeTheThirdRun: the first
// never-started store fault runs again at once, the second after the fault
// back-off's floor, and the task then succeeds on its one attempt.
func TestRunWorkflow_AStoreFaultWaitsItsFloorBeforeTheThirdRun(t *testing.T) {
	b := newScript(map[string][]error{"work": {storeFaultError(), storeFaultError()}})
	h, _, err := runInterrupted(t, fmt.Sprintf(oneTaskDocument, ""), interruptionPlan(nil), b, nil)
	must.OKf(t, err, "workflow error")
	if got := b.count("work"); got != 3 {
		t.Fatalf("work ran %d times, want 3", got)
	}
	events := interruptionEvents(h)
	if len(events) != 2 || strings.Contains(events[0].GetMessage(), "runs again at") ||
		!strings.Contains(events[1].GetMessage(), "runs again at") {
		t.Fatalf("notices = %v, want the first immediate and the second waiting", events)
	}
	wait := events[1].GetTimestamp().AsTime()
	row := interruptedRow(t, h, "work")
	if gap := row.GetScheduledAt().AsTime().Sub(wait); gap < 45e9 {
		t.Fatalf("the third run was scheduled %v after the second fault, want at least the 45 s floor", gap)
	}
}

// TestRunWorkflow_AnInterruptedItemRunsAgainInItsFanOut: an item handed back
// runs again; the fan-out succeeds with every item succeeded once.
func TestRunWorkflow_AnInterruptedItemRunsAgainInItsFanOut(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-items
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 10, max_parallel: 1}
    with: {op: exec, part: "{{ .each.item }}"}
`
	b := newScript(map[string][]error{"spread#0": {handBackError()}})
	h, _, err := runInterrupted(t, doc, interruptionPlan(nil), b, map[string]any{"parts": []any{"a", "b", "c"}})
	must.OKf(t, err, "workflow error")
	if b.count("spread#0") != 2 || b.count("spread#1") != 1 || b.count("spread#2") != 1 {
		t.Fatalf("calls = %v, want item 0 twice and the others once", b.calls)
	}
	row := interruptedRow(t, h, "spread")
	if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("spread ended %v, want SUCCEEDED", row.GetStatus())
	}
	if hb := row.GetItems()[0].GetInterruptions().GetHandedBack(); hb != 1 {
		t.Fatalf("item 0 handed back %d times, want 1", hb)
	}
}

// TestRunWorkflow_AUnitWaitingToRunAgainFailsWithItsFailingRun: a task
// waiting out its fault back-off (45 s) when a sibling fails (after a 5 s
// sleep) is not run again: it ends FAILED with its interruption's reason.
func TestRunWorkflow_AUnitWaitingToRunAgainFailsWithItsFailingRun(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-failing
tasks:
  - id: work
    uses: shell
    with: {op: exec}
  - id: pause
    sleep: {seconds: 5}
  - id: broken
    uses: shell
    needs: [pause]
    with: {op: exec}
`
	b := newScript(map[string][]error{
		"work":   {storeFaultError(), storeFaultError()},
		"broken": {temporal.NewNonRetryableApplicationError("workload exited 1", task.CategoryActivityFailed, nil)},
	})
	h, _, err := runInterrupted(t, doc, interruptionPlan(nil), b, nil)
	if err == nil {
		t.Fatal("the run succeeded; want it failed")
	}
	if got := b.count("work"); got != 2 {
		t.Fatalf("work ran %d times, want 2: it must not run again in a failing run", got)
	}
	row := interruptedRow(t, h, "work")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != "ATTEMPT_STORE_UNAVAILABLE" ||
		!strings.Contains(row.GetError().GetMessage(), "not run again") {
		t.Fatalf("work = %v %v %q, want FAILED ATTEMPT_STORE_UNAVAILABLE, not run again",
			row.GetStatus(), row.GetError().GetCode(), row.GetError().GetMessage())
	}
}

// interruptedAcrossDocument: work waits out a fault back-off while a chain of
// containers grows the history past a one-event budget, so the run switches
// with work waiting.
const interruptedAcrossDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-across
tasks:
  - id: work
    uses: shell
    with: {op: exec}
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec}
`

// TestContinuation_AUnitWaitingToRunAgainCrossesASwitch: a task waiting to
// run again is not in flight, so the run switches with it waiting; the next
// generation re-arms its wait toward the original instant, and it runs once
// more and succeeds.
func TestContinuation_AUnitWaitingToRunAgainCrossesASwitch(t *testing.T) {
	b := newScript(map[string][]error{"work": {storeFaultError(), storeFaultError()}})
	plan := continuedPlan(1)
	budgets := engine.DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	g := runGenerations(t, "ork-interrupted-across", compile(t, interruptedAcrossDocument), payload(t, nil), plan, b.run, nil)
	run := generationsOK(t, g, 2)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if got := b.count("work"); got != 3 {
		t.Fatalf("work ran %d times across generations, want 3", got)
	}
	first := mustFinalRun(t, g.harnesses[0].store, g.runID)
	before := taskRunOf(t, first, "work")
	notBefore := before.GetInterruptions().GetNotBefore()
	if before.GetStatus() != orkstackv1.Status_STATUS_PENDING || notBefore == nil {
		t.Fatalf("work was %v (not_before %v) when generation 0 switched, want PENDING and waiting",
			before.GetStatus(), notBefore)
	}
	work := taskRunOf(t, run, "work")
	if work.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || work.GetScheduledAt().AsTime().Before(notBefore.AsTime()) {
		t.Fatalf("work = %v, scheduled %v; want SUCCEEDED and not before %v",
			work.GetStatus(), work.GetScheduledAt().AsTime(), notBefore.AsTime())
	}
	if st := work.GetInterruptions(); st.GetStoreFaults() != 2 || st.GetNotBefore() != nil {
		t.Fatalf("interruptions after the switch = %v, want both faults counted and no wait", st)
	}
}

// TestRunWorkflow_ACancelWhileAnItemWaitsEndsTheRunCanceled: an operator's
// cancel that settles the only remaining item's wait ends the run canceled,
// never succeeded with the item still pending.
func TestRunWorkflow_ACancelWhileAnItemWaitsEndsTheRunCanceled(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-cancel
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
`
	b := newScript(map[string][]error{"spread#0": {storeFaultError(), storeFaultError()}})
	capture := &dispatchCapture{}
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, b.run)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 10*time.Second)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-interrupted", compile(t, doc),
		payload(t, map[string]any{"parts": []any{"a"}}), interruptionPlan(nil), (*orkstackv1.RunContinuation)(nil))
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded; want it canceled")
	}
	run := runOf(t, h, "run-interrupted")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v, want CANCELED", run.GetStatus())
	}
	if b.count("spread#0") != 2 {
		t.Fatalf("item 0 ran %d times, want 2: it must not run after the cancel", b.count("spread#0"))
	}
}

// TestContinuation_AnItemWaitingToRunAgainCrossesASwitch: a fan-out item
// waiting out its fault back-off crosses a switch: the next generation adopts
// the checkpoint, re-arms the wait, and the item runs once more.
func TestContinuation_AnItemWaitingToRunAgainCrossesASwitch(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-items-across
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec}
`
	b := newScript(map[string][]error{"spread#0": {storeFaultError(), storeFaultError()}})
	plan := continuedPlan(1)
	budgets := engine.DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	g := runGenerations(t, "ork-interrupted-items-across", compile(t, doc),
		payload(t, map[string]any{"parts": []any{"x", "y"}}), plan, b.run, nil)
	run := generationsOK(t, g, 2)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if got := b.count("spread#0"); got != 3 {
		t.Fatalf("item 0 ran %d times across generations, want 3", got)
	}
	first := mustFinalRun(t, g.harnesses[0].store, g.runID)
	if item := taskRunOf(t, first, "spread").GetItems()[0]; item.GetStatus() != orkstackv1.Status_STATUS_PENDING ||
		item.GetInterruptions().GetNotBefore() == nil {
		t.Fatalf("item 0 was %v when generation 0 switched, want PENDING and waiting", item.GetStatus())
	}
}

// TestRunWorkflow_ACancelWhileATaskWaitsRecordsItCanceled: an operator's
// cancel that settles a waiting task's wait records the task CANCELED with
// its finished_at and its event, as any task the cancel never let start.
func TestRunWorkflow_ACancelWhileATaskWaitsRecordsItCanceled(t *testing.T) {
	b := newScript(map[string][]error{"work": {storeFaultError(), storeFaultError()}})
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, b.run)
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 10*time.Second)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-interrupted", compile(t, fmt.Sprintf(oneTaskDocument, "")),
		payload(t, nil), interruptionPlan(nil), (*orkstackv1.RunContinuation)(nil))
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded; want it canceled")
	}
	row := interruptedRow(t, h, "work")
	if row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetFinishedAt() == nil {
		t.Fatalf("work = %v finished %v, want CANCELED with its finished_at", row.GetStatus(), row.GetFinishedAt())
	}
}

// TestRunWorkflow_ARunTimeoutTakesEffectWhileATaskWaits: a run whose
// deadline passes while its only task waits to run again ends at the
// deadline, not when the wait ends.
func TestRunWorkflow_ARunTimeoutTakesEffectWhileATaskWaits(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted-timeout
timeout: 10s
tasks:
  - id: work
    uses: shell
    with: {op: exec}
`
	b := newScript(map[string][]error{"work": {storeFaultError(), storeFaultError()}})
	h, _, err := runInterrupted(t, doc, interruptionPlanWithTimeout(), b, nil)
	if err == nil || !strings.Contains(err.Error(), engine.RunTimedOutCode) {
		t.Fatalf("workflow error %v, want %s", err, engine.RunTimedOutCode)
	}
	if got := b.count("work"); got != 2 {
		t.Fatalf("work ran %d times, want 2: the deadline passed during its 45 s wait", got)
	}
	run := runOf(t, h, "run-interrupted")
	if end := run.GetFinishedAt().AsTime().Sub(run.GetCreatedAt().AsTime()); end > 20*time.Second {
		t.Fatalf("the run ended %v after it started, want at its 10 s deadline", end)
	}
}

// interruptionPlanWithTimeout is interruptionPlan for a definition with a
// run timeout, as pinRunTreeLimits pins it.
func interruptionPlanWithTimeout() engine.RunPlan {
	plan := interruptionPlan(nil)
	plan.RunTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 2, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	return plan
}
