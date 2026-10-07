package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The workflow tests of join policies and failure handlers (task-join-policy
// Test plan, internal/engine). Every case drives the real RunWorkflow on the
// test environment; a moment a test must act inside is OBSERVED — an event on
// the bus, a snapshot being written — never waited for with a sleep.

const rejoinDocument = `
name: rejoin
inputs:
  route: {type: string}
tasks:
  - id: decide
    uses: shell
    with: {op: exec}
  - id: canary
    uses: shell
    needs: [decide]
    condition: '{{ eq .inputs.route "canary" }}'
    with: {op: exec}
  - id: full
    uses: shell
    needs: [decide]
    condition: '{{ eq .inputs.route "full" }}'
    with: {op: exec}
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec}
  - id: after
    uses: shell
    needs: [verify]
    with: {op: exec}
`

// joinFailDocument: deploy fails (with an artifact) while smoke runs beside
// it; announce is an ordinary task after deploy; rollback is an any_failed
// handler of deploy; notify an all_done handler of deploy and smoke.
const joinFailDocument = `
name: join-fail
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: deploy
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: smoke
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: announce
    uses: shell
    needs: [deploy]
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [deploy, smoke]
    join: all_done
    with: {op: exec}
  - id: cleanup
    uses: shell
    needs: [notify]
    join: all_done
    with: {op: exec}
`

// joinFailWithoutHandlersDocument is joinFailDocument with every failure
// handler removed: what the same failure records with no handler at all.
const joinFailWithoutHandlersDocument = `
name: join-fail
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: deploy
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: smoke
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: announce
    uses: shell
    needs: [deploy]
    with: {op: exec}
`

// deployFailure is the failure deploy's mocked Orker reports: exit 1, with
// the log it uploaded.
func deployFailure() error {
	return temporal.NewNonRetryableApplicationError("workload exited 1", task.CategoryActivityFailed, nil,
		&orkstackv1.ActivityFailureDetail{
			ReasonCode: "CONTAINER_NONZERO_EXIT",
			Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
			ExitCode:   1,
			Artifacts:  []*orkstackv1.ArtifactRef{deployLog()},
		})
}

func deployLog() *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{
		Name: "deploy.log", Uri: "mem://artifacts/deploy/deploy.log", SizeBytes: 12,
		Sha256: strings.Repeat("a", 64), ContentType: "text/plain",
	}
}

// waitForEvent polls the bus, under a deadline, until an event matching
// match has been published for runID. It heartbeats actCtx while it waits,
// as a real Orker does, and reports whether the event was seen — false when
// actCtx was cancelled first or the deadline passed.
func waitForEvent(actCtx context.Context, h *harness, runID string, match func(*orkstackv1.Event) bool) bool {
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, e := range h.bus.Events(runID, 0) {
			if match(e) {
				return true
			}
		}
		select {
		case <-actCtx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
			activity.RecordHeartbeat(actCtx, "waiting")
		}
	}
}

// runUntilCanceled heartbeats until actCtx is cancelled, closing canceled.
func runUntilCanceled(actCtx context.Context, canceled chan struct{}) (*orkstackv1.ExecuteTaskResult, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-actCtx.Done():
			if canceled != nil {
				close(canceled)
			}
			return &orkstackv1.ExecuteTaskResult{}, actCtx.Err()
		case <-deadline:
			return nil, errors.New("never canceled")
		case <-ticker.C:
			activity.RecordHeartbeat(actCtx, "running")
		}
	}
}

// joinFailBehavior is joinFailDocument's Orker: deploy fails once smoke is
// genuinely running, smoke runs until it is cancelled, every other task
// succeeds. smokeCanceled closes when smoke observes its cancellation.
func joinFailBehavior(smokeCanceled chan struct{}) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	smokeRunning := make(chan struct{})
	var once sync.Once
	return func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "deploy":
			select {
			case <-smokeRunning:
			case <-time.After(30 * time.Second):
				return nil, errors.New("smoke never started")
			}
			return nil, deployFailure()
		case "smoke":
			once.Do(func() { close(smokeRunning) })
			return runUntilCanceled(actCtx, smokeCanceled)
		default:
			return succeeded(in.TaskId, map[string]any{"task": in.TaskId}), nil
		}
	}
}

// failingRun runs doc with joinFailBehavior and returns the run and harness.
func failingRun(t *testing.T, runID, doc string) (*orkstackv1.Run, *harness) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	smokeCanceled := make(chan struct{})
	h := newHarnessCtx(t, &suite, joinFailBehavior(smokeCanceled))
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc), payload(t, nil), defaultPlan())
	if !h.env.IsWorkflowCompleted() {
		t.Fatalf("%s: workflow did not complete", runID)
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatalf("%s: workflow error = nil, want deploy's failure", runID)
	}
	select {
	case <-smokeCanceled:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: smoke never observed its cancellation", runID)
	}
	return runOf(t, h, runID), h
}

func taskSkipReason(h *harness, runID, taskID string) string {
	for _, e := range eventsFor(h, runID, taskID) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
			return e.GetMessage()
		}
	}
	return ""
}

func taskCancelReason(h *harness, runID, taskID string) string {
	for _, e := range eventsFor(h, runID, taskID) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
			return e.GetMessage()
		}
	}
	return ""
}

func dispatched(h *harness, taskID string) int {
	n := 0
	for _, in := range h.invocations() {
		if in.GetTaskId() == taskID {
			n++
		}
	}
	return n
}

func upstreamIDs(in *orkstackv1.ExecuteTaskInput) []string {
	var out []string
	for _, ref := range in.GetUpstream() {
		out = append(out, ref.GetTaskId())
	}
	return out
}

func TestRunWorkflow_NoneFailedRejoinsAfterTheBranchThatRan(t *testing.T) {
	for _, route := range []string{"canary", "full"} {
		t.Run(route, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return succeeded(in.TaskId, map[string]any{"task": in.TaskId}), nil
			})
			runID := "run-rejoin-" + route
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, rejoinDocument),
				payload(t, map[string]any{"route": route}), defaultPlan())
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			run := runOf(t, h, runID)
			other := map[string]string{"canary": "full", "full": "canary"}[route]
			for id, want := range map[string]orkstackv1.Status{
				"decide": orkstackv1.Status_STATUS_SUCCEEDED, route: orkstackv1.Status_STATUS_SUCCEEDED,
				other: orkstackv1.Status_STATUS_SKIPPED, "verify": orkstackv1.Status_STATUS_SUCCEEDED,
				"after": orkstackv1.Status_STATUS_SUCCEEDED,
			} {
				if got := statusOf(run, id); got != want {
					t.Errorf("%s = %v, want %v", id, got, want)
				}
			}
			wantSkip := "task skipped: " + engine.SkipReasonCondition(fmt.Sprintf(`{{ eq .inputs.route %q }}`, other))
			if got := taskSkipReason(h, runID, other); got != wantSkip {
				t.Errorf("%s skip = %q, want %q", other, got, wantSkip)
			}
			in, ok := h.inputFor("verify")
			if !ok {
				t.Fatal("verify was never dispatched")
			}
			if got, want := upstreamIDs(in), []string{route, "decide"}; route == "full" {
				if !reflect.DeepEqual(got, []string{"decide", "full"}) {
					t.Errorf("verify upstream = %v, want [decide full]", got)
				}
			} else if !reflect.DeepEqual(got, []string{"canary", "decide"}) {
				t.Errorf("verify upstream = %v, want %v", got, want)
			}
			if in.GetJoin() != nil {
				t.Errorf("verify (none_failed) carries a JoinContext: %v", in.GetJoin())
			}
			if got := taskRunOf(t, run, "verify").GetJoin(); got != orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED {
				t.Errorf("TaskRun.join = %v, want NONE_FAILED", got)
			}
			if got := taskRunOf(t, run, "after").GetJoin(); got != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
				t.Errorf("after's TaskRun.join = %v, want unset", got)
			}
		})
	}
}

func TestRunWorkflow_NoneFailedIsSkippedWhenEveryNeedWasSkipped(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-rejoin-none", compile(t, rejoinDocument),
		payload(t, map[string]any{"route": "none"}), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := runOf(t, h, "run-rejoin-none")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	if got, want := taskSkipReason(h, "run-rejoin-none", "verify"),
		"task skipped: join none_failed: every need was skipped (canary, full)"; got != want {
		t.Errorf("verify skip = %q, want %q", got, want)
	}
	if got, want := taskSkipReason(h, "run-rejoin-none", "after"),
		`task skipped: upstream task "verify" was skipped`; got != want {
		t.Errorf("after skip = %q, want %q", got, want)
	}
}

const anyFailedSkippedDocument = `
name: any-failed-skipped
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_AnyFailedIsSkippedInARunThatDoesNotFail(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-any-failed-skipped", compile(t, anyFailedSkippedDocument),
		payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := runOf(t, h, "run-any-failed-skipped")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || statusOf(run, "rollback") != orkstackv1.Status_STATUS_SKIPPED {
		t.Fatalf("run = %v, rollback = %v", run.GetStatus(), statusOf(run, "rollback"))
	}
	want := "task skipped: join any_failed: none of its needs failed (deploy); it runs only when one of them fails"
	if got := taskSkipReason(h, "run-any-failed-skipped", "rollback"); got != want {
		t.Errorf("rollback skip = %q, want %q", got, want)
	}
	if n := dispatched(h, "rollback"); n != 0 {
		t.Errorf("rollback dispatched %d times, want never", n)
	}
	// deploy is still a leaf: a handler's needs edge does not remove it.
	if _, ok := run.GetOutput().GetData().GetFields()["deploy"]; !ok {
		t.Errorf("run output %v lacks deploy's values", run.GetOutput())
	}
}

func TestRunWorkflow_AnyFailedRunsAfterItsNeedFailsAndTheRunStillFails(t *testing.T) {
	run, h := failingRun(t, "run-any-failed-runs", joinFailDocument)
	bare, _ := failingRun(t, "run-any-failed-runs", joinFailWithoutHandlersDocument)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED || dispatched(h, "rollback") != 1 {
		t.Fatalf("rollback = %v, dispatched %d", statusOf(run, "rollback"), dispatched(h, "rollback"))
	}
	got, want := run.GetError(), bare.GetError()
	if got.GetCode() != want.GetCode() || got.GetMessage() != want.GetMessage() || got.GetDetails() != want.GetDetails() {
		t.Fatalf("Run.error = %v, want what the same definition without a handler records: %v", got, want)
	}
	if got.GetCode() != "RUN_FAILED" || got.GetDetails() != `task "deploy"` {
		t.Fatalf("Run.error = %v", got)
	}
	if run.GetFinishedAt() == nil {
		t.Fatal("finished_at unset on the terminal snapshot")
	}
}

func TestRunWorkflow_FailureHandlerSeesItsNeedsOutcomesOnItsInput(t *testing.T) {
	_, h := failingRun(t, "run-join-context", joinFailDocument)
	notify, ok := h.inputFor("notify")
	if !ok {
		t.Fatal("notify was never dispatched")
	}
	jc := notify.GetJoin()
	if jc.GetPolicy() != orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE || len(jc.GetNeeds()) != 2 {
		t.Fatalf("notify join = %v", jc)
	}
	deploy, smoke := jc.GetNeeds()[0], jc.GetNeeds()[1]
	if deploy.GetTaskId() != "deploy" || deploy.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		deploy.GetError().GetCode() != "CONTAINER_NONZERO_EXIT" ||
		deploy.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED ||
		!strings.Contains(deploy.GetError().GetMessage(), "workload exited 1") {
		t.Errorf("deploy outcome = %v", deploy)
	}
	if smoke.GetTaskId() != "smoke" || smoke.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		smoke.GetError().GetCode() != "TASK_CANCELED" {
		t.Errorf("smoke outcome = %v, want CANCELED with its TASK_CANCELED error", smoke)
	}
	rollback, _ := h.inputFor("rollback")
	if rollback.GetJoin().GetPolicy() != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED ||
		len(rollback.GetJoin().GetNeeds()) != 1 || rollback.GetJoin().GetNeeds()[0].GetTaskId() != "deploy" {
		t.Errorf("rollback join = %v", rollback.GetJoin())
	}
	build, _ := h.inputFor("build")
	if build.GetJoin() != nil {
		t.Errorf("build (not a handler) carries a JoinContext: %v", build.GetJoin())
	}
}

func TestRunWorkflow_FailureHandlerIsHandedTheFailedNeedsArtifacts(t *testing.T) {
	_, h := failingRun(t, "run-join-artifacts", joinFailDocument)
	rollback, _ := h.inputFor("rollback")
	var deployRef *orkstackv1.UpstreamRef
	for _, ref := range rollback.GetUpstream() {
		if ref.GetTaskId() == "deploy" {
			deployRef = ref
		}
	}
	if deployRef == nil || len(deployRef.GetArtifacts()) != 1 || deployRef.GetArtifacts()[0].GetName() != "deploy.log" ||
		deployRef.GetValues() != nil || deployRef.GetCommitUri() != "" {
		t.Fatalf("rollback's deploy ref = %v, want {task_id, artifacts} only", deployRef)
	}
	if got := upstreamIDs(rollback); !reflect.DeepEqual(got, []string{"build", "deploy"}) {
		t.Errorf("rollback upstream = %v, want [build deploy]", got)
	}
	// cleanup's closure holds deploy, but not as a direct need: no ref.
	cleanup, ok := h.inputFor("cleanup")
	if !ok {
		t.Fatal("cleanup was never dispatched")
	}
	for _, ref := range cleanup.GetUpstream() {
		if ref.GetTaskId() == "deploy" {
			t.Errorf("cleanup was handed deploy's artifacts through its closure: %v", ref)
		}
	}
}

func TestRunWorkflow_FailFastStillCancelsSiblingsAndUnstartedNonHandlers(t *testing.T) {
	run, h := failingRun(t, "run-join-failfast", joinFailDocument)
	if statusOf(run, "smoke") != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("smoke = %v, want CANCELED", statusOf(run, "smoke"))
	}
	announce := taskRunOf(t, run, "announce")
	if announce.GetStatus() != orkstackv1.Status_STATUS_CANCELED || announce.GetStartedAt() != nil ||
		announce.GetScheduledAt() != nil {
		t.Errorf("announce = %v", announce)
	}
	want := `task canceled: the run is failing (task "deploy" failed) and this task is not a failure handler`
	if got := taskCancelReason(h, "run-join-failfast", "announce"); got != want {
		t.Errorf("announce cancel = %q, want %q", got, want)
	}
	if dispatched(h, "announce") != 0 {
		t.Error("announce was dispatched")
	}
}

const allDoneRunningDocument = `
name: all-done-running
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: watcher
    uses: shell
    needs: [a]
    join: all_done
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
`

func TestRunWorkflow_AllDoneHandlerRunningWhenAnotherTaskFailsIsNotCanceled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var h *harness
	watcherRunning := make(chan struct{})
	var watcherCanceled bool
	h = newHarnessCtx(t, &suite, func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "watcher":
			close(watcherRunning)
			// Runs until the run has started failing: b's TASK_FAILED is on
			// the bus. A cancellation would end it first.
			if !waitForEvent(actCtx, h, "run-all-done-running", func(e *orkstackv1.Event) bool {
				return e.GetTaskId() == "b" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED
			}) {
				watcherCanceled = actCtx.Err() != nil
				return nil, errors.New("watcher never saw b fail")
			}
			return succeeded(in.TaskId, map[string]any{}), nil
		case "b":
			select {
			case <-watcherRunning:
			case <-time.After(30 * time.Second):
			}
			return nil, temporal.NewNonRetryableApplicationError("b exploded", task.CategoryActivityFailed, nil)
		default:
			return succeeded(in.TaskId, map[string]any{}), nil
		}
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-all-done-running", compile(t, allDoneRunningDocument),
		payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-all-done-running")
	if watcherCanceled {
		t.Fatal("the all_done handler was cancelled by fail-fast")
	}
	if statusOf(run, "watcher") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("watcher = %v", taskRunOf(t, run, "watcher"))
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "b exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
}

const twoHandlersDocument = `
name: two-handlers
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: page
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_AFailingHandlerStopsNothingAndLeavesTheRunError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "deploy":
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		case "page":
			return nil, temporal.NewNonRetryableApplicationError("pager down", task.CategoryActivityFailed, nil)
		default:
			return okTask(in)
		}
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-two-handlers", compile(t, twoHandlersDocument), payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-two-handlers")
	if statusOf(run, "page") != orkstackv1.Status_STATUS_FAILED || statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("page = %v, rollback = %v", statusOf(run, "page"), statusOf(run, "rollback"))
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") ||
		run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("run error = %v, want deploy's", run.GetError())
	}
}

const escalateDocument = `
name: escalate
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: escalate
    uses: shell
    needs: [rollback]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_AnyFailedOnAHandlerEscalates(t *testing.T) {
	for _, rollbackFails := range []bool{true, false} {
		t.Run(fmt.Sprintf("rollback fails %v", rollbackFails), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				if in.TaskId == "deploy" || (in.TaskId == "rollback" && rollbackFails) {
					return nil, temporal.NewNonRetryableApplicationError(in.TaskId+" exploded", task.CategoryActivityFailed, nil)
				}
				return okTask(in)
			})
			runID := fmt.Sprintf("run-escalate-%v", rollbackFails)
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, escalateDocument), payload(t, nil), defaultPlan())
			run := runOf(t, h, runID)
			if rollbackFails {
				if statusOf(run, "escalate") != orkstackv1.Status_STATUS_SUCCEEDED {
					t.Fatalf("escalate = %v, want it to run after rollback failed", statusOf(run, "escalate"))
				}
			} else {
				want := "task skipped: join any_failed: none of its needs failed (rollback); it runs only when one of them fails"
				if statusOf(run, "escalate") != orkstackv1.Status_STATUS_SKIPPED || taskSkipReason(h, runID, "escalate") != want {
					t.Fatalf("escalate = %v %q", statusOf(run, "escalate"), taskSkipReason(h, runID, "escalate"))
				}
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") {
				t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
			}
		})
	}
}

// operatorCancelDocument: slow runs until cancelled; notify (all_done on
// build) is a handler running beside it; rollback (any_failed on slow) is a
// pending handler; after is an ordinary task after slow.
const operatorCancelDocument = `
name: operator-cancel
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: slow
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [build]
    join: all_done
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [slow]
    join: any_failed
    with: {op: exec}
  - id: after
    uses: shell
    needs: [slow]
    with: {op: exec}
`

func TestRunWorkflow_OperatorCancelStartsNoHandlerAndStopsRunningOnes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var h *harness
	notifyCanceled := make(chan struct{})
	slowRunning, notifyRunning := make(chan struct{}), make(chan struct{})
	var cancelOnce sync.Once
	both := func() {
		select {
		case <-slowRunning:
		default:
			return
		}
		select {
		case <-notifyRunning:
		default:
			return
		}
		cancelOnce.Do(h.env.CancelWorkflow)
	}
	var mu sync.Mutex
	h = newHarnessCtx(t, &suite, func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "slow":
			mu.Lock()
			close(slowRunning)
			both()
			mu.Unlock()
			return runUntilCanceled(actCtx, nil)
		case "notify":
			mu.Lock()
			close(notifyRunning)
			both()
			mu.Unlock()
			return runUntilCanceled(actCtx, notifyCanceled)
		default:
			return okTask(in)
		}
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-operator-cancel", compile(t, operatorCancelDocument), payload(t, nil), defaultPlan())
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	select {
	case <-notifyCanceled:
	case <-time.After(30 * time.Second):
		t.Fatal("the running all_done handler was never cancelled")
	}
	run := runOf(t, h, "run-operator-cancel")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" || run.GetFinishedAt() == nil {
		t.Fatalf("run = %v / %v finished %v", run.GetStatus(), run.GetError(), run.GetFinishedAt())
	}
	if statusOf(run, "notify") != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("notify = %v", statusOf(run, "notify"))
	}
	want := "task canceled: the run was canceled; a canceled run starts no failure handler"
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_CANCELED || taskCancelReason(h, "run-operator-cancel", "rollback") != want {
		t.Errorf("rollback = %v %q", statusOf(run, "rollback"), taskCancelReason(h, "run-operator-cancel", "rollback"))
	}
	if got := taskCancelReason(h, "run-operator-cancel", "after"); got != "task canceled: run ended before this task started" {
		t.Errorf("after = %q", got)
	}
	if dispatched(h, "rollback") != 0 {
		t.Error("a handler was dispatched in a canceled run")
	}
}

// handlerCancelDocument: deploy fails; rollback (a handler) runs until
// cancelled; escalate waits on rollback; notify all_done on deploy succeeds.
const handlerCancelDocument = `
name: handler-cancel
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: escalate
    uses: shell
    needs: [rollback]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_OperatorCancelDuringFailureHandlersKeepsTheRunFailed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var h *harness
	rollbackCanceled := make(chan struct{})
	h = newHarnessCtx(t, &suite, func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "deploy":
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		case "rollback":
			h.env.CancelWorkflow()
			return runUntilCanceled(actCtx, rollbackCanceled)
		default:
			return okTask(in)
		}
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-handler-cancel", compile(t, handlerCancelDocument), payload(t, nil), defaultPlan())
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	select {
	case <-rollbackCanceled:
	case <-time.After(30 * time.Second):
		t.Fatal("the running handler was never cancelled")
	}
	run := runOf(t, h, "run-handler-cancel")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") ||
		run.GetError().GetCode() != "RUN_FAILED" || run.GetFinishedAt() == nil {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("rollback = %v", statusOf(run, "rollback"))
	}
	want := "task canceled: the run was canceled while its failure handlers ran"
	if statusOf(run, "escalate") != orkstackv1.Status_STATUS_CANCELED || taskCancelReason(h, "run-handler-cancel", "escalate") != want {
		t.Errorf("escalate = %v %q", statusOf(run, "escalate"), taskCancelReason(h, "run-handler-cancel", "escalate"))
	}
	events := eventsOf(h, "run-handler-cancel")
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("last event = %v, want RUN_FAILED", last.GetType())
	}
	if !errors.As(h.env.GetWorkflowError(), new(*temporal.ApplicationError)) {
		t.Fatalf("workflow error = %v, want the original cause, not a cancellation", h.env.GetWorkflowError())
	}
}

func TestRunWorkflow_SnapshotsAreRunningWithTheErrorWhileHandlersRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	var snapshots []*orkstackv1.Run
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	})
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		mu.Lock()
		snapshots = append(snapshots, run)
		mu.Unlock()
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-failing-shape", compile(t, escalateDocument), payload(t, nil), defaultPlan())
	mu.Lock()
	defer mu.Unlock()
	failingSeen := false
	for i, run := range snapshots {
		last := i == len(snapshots)-1
		if run.GetError() == nil {
			continue
		}
		if last {
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetFinishedAt() == nil {
				t.Fatalf("terminal snapshot = %v finished %v", run.GetStatus(), run.GetFinishedAt())
			}
			continue
		}
		failingSeen = true
		if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING || run.GetFinishedAt() != nil {
			t.Fatalf("snapshot %d of a failing run = %v finished %v, want RUNNING, unset", i, run.GetStatus(), run.GetFinishedAt())
		}
	}
	if !failingSeen {
		t.Fatal("no snapshot carried the failing shape (RUNNING with an error)")
	}
}

func TestRunWorkflow_EmitsOneFailingNoteNamingTheOutstandingHandlers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-failing-note", compile(t, escalateDocument), payload(t, nil), defaultPlan())
	var notes []*orkstackv1.Event
	for _, e := range eventsOf(h, "run-failing-note") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetTaskId() == "" {
			notes = append(notes, e)
		}
	}
	want := `run failing after task "deploy" failed; waiting for its failure handlers: rollback, escalate`
	if len(notes) != 1 || notes[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING || notes[0].GetMessage() != want {
		t.Fatalf("failing notes = %v, want one RUNNING %q", notes, want)
	}
	var last *orkstackv1.Event
	for _, e := range eventsFor(h, "run-failing-note", "deploy") {
		if !strings.HasPrefix(e.GetType().String(), "EVENT_TYPE_RUN_") {
			last = e
		}
	}
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_FAILED || !strings.Contains(last.GetMessage(), "deploy exploded") {
		t.Fatalf("deploy's last event = %v %q", last.GetType(), last.GetMessage())
	}

	// No note when every handler is already terminal: an any_failed handler
	// of a need that succeeded is skipped before anything fails.
	var suite2 testsuite.WorkflowTestSuite
	h2 := newHarness(t, &suite2, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "b" {
			return nil, temporal.NewNonRetryableApplicationError("b exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	})
	h2.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-note", compile(t, `
name: no-note
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: h
    uses: shell
    needs: [a]
    join: any_failed
    with: {op: exec}
  - id: b
    uses: shell
    needs: [h]
    join: all_done
    with: {op: exec}
`), payload(t, nil), defaultPlan())
	for _, e := range eventsOf(h2, "run-no-note") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetTaskId() == "" {
			t.Fatalf("a failing note was emitted with no handler outstanding: %v", e)
		}
	}
}

const brokenConditionDocument = `
name: broken-condition
inputs:
  a: {type: string}
  b: {type: number}
tasks:
  - id: first
    uses: shell
    with: {op: exec}
  - id: broken
    uses: shell
    needs: [first]
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {op: exec}
  - id: after
    uses: shell
    needs: [broken]
    with: {op: exec}
`

func TestRunWorkflow_AConditionThatCannotBeEvaluatedFailsItsTask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	doc := brokenConditionDocument + `
  - id: rollback
    uses: shell
    needs: [broken]
    join: any_failed
    with: {op: exec}
`
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-condition-failed", compile(t, doc),
		payload(t, map[string]any{"a": "x", "b": 1}), defaultPlan())
	run := runOf(t, h, "run-condition-failed")
	broken := taskRunOf(t, run, "broken")
	if broken.GetStatus() != orkstackv1.Status_STATUS_FAILED || broken.GetError().GetCode() != "CONDITION_FAILED" ||
		broken.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		broken.GetFinishedAt() == nil {
		t.Fatalf("broken = %v", broken)
	}
	failed := 0
	for _, e := range eventsFor(h, "run-condition-failed", "broken") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("broken TASK_FAILED events = %d, want 1", failed)
	}
	if run.GetError().GetCode() != "RUN_FAILED" || run.GetError().GetDetails() != `task "broken"` ||
		!strings.Contains(run.GetError().GetMessage(), "incompatible types for comparison") {
		t.Errorf("Run.error = %v", run.GetError())
	}
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("rollback = %v, want it to run for its broken need", statusOf(run, "rollback"))
	}
	if got := engine.PrimaryFailedTask(run).GetTaskId(); got != "broken" {
		t.Errorf("PrimaryFailedTask = %q, want broken", got)
	}
}

func TestRunWorkflow_ConditionErrorWithoutAHandlerKeepsTodaysRecord(t *testing.T) {
	record := func(doc, runID string) (*orkstackv1.Run, *harness) {
		var suite testsuite.WorkflowTestSuite
		h := newHarness(t, &suite, okTask)
		h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc),
			payload(t, map[string]any{"a": "x", "b": 1}), defaultPlan())
		return runOf(t, h, runID), h
	}
	// A none_failed join is present: the gate is failure handlers, not joins.
	withJoin := brokenConditionDocument + `
  - id: rejoin
    uses: shell
    needs: [after, first]
    join: none_failed
    with: {op: exec}
`
	withoutJoin := brokenConditionDocument + `
  - id: rejoin
    uses: shell
    needs: [after, first]
    with: {op: exec}
`
	joined, hj := record(withJoin, "run-condition-today")
	plain, hp := record(withoutJoin, "run-condition-today")
	broken := taskRunOf(t, joined, "broken")
	if broken.GetStatus() != orkstackv1.Status_STATUS_CANCELED || broken.GetError() != nil {
		t.Fatalf("broken = %v, want today's CANCELED", broken)
	}
	if got := taskCancelReason(hj, "run-condition-today", "broken"); got != "task canceled: run ended before this task started" {
		t.Errorf("broken = %q", got)
	}
	for _, e := range eventsFor(hj, "run-condition-today", "broken") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
			t.Errorf("a TASK_FAILED event for broken in a definition without a handler: %v", e)
		}
	}
	if !proto3Equal(joined.GetError(), plain.GetError()) {
		t.Errorf("Run.error = %v, want %v", joined.GetError(), plain.GetError())
	}
	je, pe := eventsOf(hj, "run-condition-today"), eventsOf(hp, "run-condition-today")
	if len(je) != len(pe) {
		t.Fatalf("events %v vs %v", eventTypes(je), eventTypes(pe))
	}
	for i := range je {
		if je[i].GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
			continue // names the derived version, which differs with the source
		}
		if je[i].GetType() != pe[i].GetType() || je[i].GetTaskId() != pe[i].GetTaskId() ||
			je[i].GetMessage() != pe[i].GetMessage() || je[i].GetStatus() != pe[i].GetStatus() {
			t.Errorf("event %d: %v %s %q vs %v %s %q", i, je[i].GetType(), je[i].GetTaskId(), je[i].GetMessage(),
				pe[i].GetType(), pe[i].GetTaskId(), pe[i].GetMessage())
		}
	}
}

func proto3Equal(a, b *orkstackv1.Error) bool {
	return a.GetCode() == b.GetCode() && a.GetMessage() == b.GetMessage() && a.GetDetails() == b.GetDetails() &&
		a.GetCategory() == b.GetCategory()
}

// bigWith is a with: value large enough that, under a 4 KiB plan bound, the
// task's input is refused INPUT_TOO_LARGE before it is scheduled.
var bigWith = strings.Repeat("x", 6000)

func TestRunWorkflow_RefusedDispatchOfTheCauseIsRecordedFailed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	plan := defaultPlan()
	plan.Limits = engine.WorkspaceLimits{ValuesMaxBytes: 4096}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-refused-cause", compile(t, `
name: refused-cause
tasks:
  - id: big
    uses: shell
    with: {op: exec, blob: "`+bigWith+`"}
  - id: rollback
    uses: shell
    needs: [big]
    join: any_failed
    with: {op: exec}
`), payload(t, nil), plan)
	run := runOf(t, h, "run-refused-cause")
	big := taskRunOf(t, run, "big")
	if big.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(big.GetError().GetMessage(), task.ReasonInputTooLarge) {
		t.Fatalf("big = %v, want FAILED with INPUT_TOO_LARGE (not left RUNNING)", big)
	}
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback = %v", statusOf(run, "rollback"))
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != `task "big"` {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
}

// waitForRelease blocks, under a deadline, until the harness has seen a
// ReleasePin for pin. It is how a handler's mock proves the pin was released
// while it ran, not after.
func waitForRelease(h *harness, pin string) bool {
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, r := range h.releases() {
			if r.GetPin().GetName() == pin {
				return true
			}
		}
		select {
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

func TestRunWorkflow_TerminalTransitionsOfThisSpecReleaseTheirPin(t *testing.T) {
	cases := []struct {
		name  string
		doc   string
		input map[string]any
		limit int64
		cause string
	}{
		{"a broken condition (failCondition)", `
name: pin-condition
inputs:
  a: {type: string}
  b: {type: number}
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    with: {op: exec}
  - id: compile
    uses: shell
    pin: build
    needs: [prep]
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [compile]
    join: any_failed
    with: {op: exec}
`, map[string]any{"a": "x", "b": 1}, 0, "compile"},
		{"a refused cause (recordRefusedDispatch)", `
name: pin-refused
pins:
  build: {}
tasks:
  - id: big
    uses: shell
    pin: build
    with: {op: exec, blob: "` + bigWith + `"}
  - id: rollback
    uses: shell
    needs: [big]
    join: any_failed
    with: {op: exec}
`, nil, 4096, "big"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			var h *harness
			released := false
			h = newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				if in.TaskId == "rollback" {
					// The pin is released while the handler runs — only
					// because both helpers end with closePinFor; without it the
					// pin would be signalled at F11, after this returns.
					released = waitForRelease(h, "build")
				}
				return okTask(in)
			}, nil)
			plan := defaultPlan()
			plan.Limits = engine.WorkspaceLimits{ValuesMaxBytes: tc.limit}
			runID := fmt.Sprintf("run-pin-release-%d", i)
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, tc.doc), payload(t, tc.input), plan)
			if !released {
				t.Fatal("the pin was not released while the failure handler ran")
			}
			run := runOf(t, h, runID)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != fmt.Sprintf("task %q", tc.cause) {
				t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
			}
			if statusOf(run, tc.cause) != orkstackv1.Status_STATUS_FAILED || statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("%s = %v, rollback = %v", tc.cause, statusOf(run, tc.cause), statusOf(run, "rollback"))
			}
		})
	}
}

const handlerRetryDocument = `
name: handler-retry
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
%s    with: {op: exec}
  - id: escalate
    uses: shell
    needs: [rollback]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_AFailureHandlerRetriesPerItsOwnPolicy(t *testing.T) {
	cases := []struct {
		name          string
		retry         string
		calls         int
		rollback      orkstackv1.Status
		escalateState orkstackv1.Status
	}{
		{"with retry", "    retry: {maximum_attempts: 2, initial_interval_seconds: 1}\n", 2,
			orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_SKIPPED},
		{"without retry", "", 1, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			var mu sync.Mutex
			rollbackCalls := 0
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				switch in.TaskId {
				case "deploy":
					return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
				case "rollback":
					mu.Lock()
					rollbackCalls++
					first := rollbackCalls == 1
					mu.Unlock()
					if first {
						return nil, temporal.NewApplicationError("orker lost", task.CategoryInfrastructureError)
					}
				}
				return okTask(in)
			})
			runID := fmt.Sprintf("run-handler-retry-%d", i)
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, fmt.Sprintf(handlerRetryDocument, tc.retry)),
				payload(t, nil), defaultPlan())
			run := runOf(t, h, runID)
			mu.Lock()
			calls := rollbackCalls
			mu.Unlock()
			if calls != tc.calls {
				t.Errorf("rollback invoked %d times, want %d", calls, tc.calls)
			}
			if statusOf(run, "rollback") != tc.rollback || statusOf(run, "escalate") != tc.escalateState {
				t.Errorf("rollback = %v, escalate = %v", statusOf(run, "rollback"), statusOf(run, "escalate"))
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") {
				t.Errorf("run = %v / %v", run.GetStatus(), run.GetError())
			}
		})
	}
}

func TestRunWorkflow_ACancelDuringTheDrainStartsNoHandlerAndEndsFailed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var h *harness
	smokeCanceled := make(chan struct{})
	h = newHarnessCtx(t, &suite, joinFailBehavior(smokeCanceled))
	var once sync.Once
	// The run is cancelled while the snapshot of smoke's fail-fast
	// cancellation is being written (F6's drain).
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		if statusOf(run, "smoke") == orkstackv1.Status_STATUS_CANCELED {
			once.Do(h.env.CancelWorkflow)
		}
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-cancel-drain", compile(t, joinFailDocument), payload(t, nil), defaultPlan())
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-cancel-drain")
	if statusOf(run, "smoke") != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("smoke = %v", statusOf(run, "smoke"))
	}
	for _, id := range []string{"rollback", "notify", "cleanup"} {
		if n := dispatched(h, id); n != 0 {
			t.Errorf("%s dispatched %d times after the cancel", id, n)
		}
		if statusOf(run, id) != orkstackv1.Status_STATUS_CANCELED {
			t.Errorf("%s = %v, want CANCELED", id, statusOf(run, id))
		}
	}
	if got := taskCancelReason(h, "run-cancel-drain", "cleanup"); got != "task canceled: "+"the run was canceled while its failure handlers ran" {
		t.Errorf("cleanup = %q", got)
	}
	// The failing note was written although the cancel had landed: F7 runs on
	// the detached event context.
	notes := 0
	for _, e := range eventsOf(h, "run-cancel-drain") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetTaskId() == "" {
			notes++
		}
	}
	if notes != 1 {
		t.Errorf("failing notes = %d, want 1", notes)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetFinishedAt() == nil ||
		run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if !errors.As(h.env.GetWorkflowError(), new(*temporal.ApplicationError)) {
		t.Fatalf("workflow error = %v, want the cause", h.env.GetWorkflowError())
	}
}

func TestRunWorkflow_ACancelThatLandsMidDispatchRecordsTheHandlerCanceled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var h *harness
	h = newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	})
	var once sync.Once
	// Cancel while rollback's TASK_SCHEDULED snapshot is being written: the
	// write is on handlerCtx, so it fails with the cancellation.
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		if statusOf(run, "rollback") == orkstackv1.Status_STATUS_RUNNING {
			once.Do(h.env.CancelWorkflow)
		}
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-cancel-dispatch", compile(t, anyFailedSkippedDocument), payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-cancel-dispatch")
	rollback := taskRunOf(t, run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil {
		t.Fatalf("rollback = %v, want CANCELED with scheduled_at unset", rollback)
	}
	if got := taskCancelReason(h, "run-cancel-dispatch", "rollback"); got != "task canceled: the run was canceled while its failure handlers ran" {
		t.Errorf("rollback = %q", got)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
}

// childPlanFor plans child as the child run of task id in plan.
func childPlanFor(t *testing.T, plan *engine.RunPlan, id string, child *orkstackv1.Orkflow) {
	t.Helper()
	child.DefinitionDigest = childDigest
	childPlan := defaultPlan()
	cp, err := engine.NewChildRunPlan(child, childPlan, true, true)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = "ork-1758790021000000000-" + id
	if plan.Children == nil {
		plan.Children = map[string]*engine.ChildRunPlan{}
	}
	plan.Children[id] = cp
}

const composedHandlerDocument = `
name: composed-handler
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: compensate
    uses: orkflow:release-block@2.1.0
    needs: [deploy]
    join: any_failed
    with: {image: "ghcr.io/acme/app:1"}
`

func composedHandlerRun(t *testing.T, runID string, onSnapshot func(*harness) func(*orkstackv1.Run, int64) error) (*orkstackv1.Run, *harness) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	var h *harness
	h = newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		}
		// The child's values are really stored, so its outputs render.
		result := succeeded(in.TaskId, map[string]any{"value": "signed"})
		result.ValuesRef = h.values.put("mem://values/"+in.GetRunId()+"/"+in.TaskId, result.GetValuesJson())
		return result, nil
	})
	if onSnapshot != nil {
		h.onSnapshot = onSnapshot(h)
	}
	wf := compile(t, composedHandlerDocument)
	for _, tk := range wf.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = childDigest
		}
	}
	plan := defaultPlan()
	childPlanFor(t, &plan, "compensate", compile(t, composedChildDocument))
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, payload(t, nil), plan)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	return runOf(t, h, runID), h
}

func TestRunWorkflow_ComposedHandlerStartsAChildInAFailingRun(t *testing.T) {
	run, h := composedHandlerRun(t, "run-composed-handler", nil)
	compensate := taskRunOf(t, run, "compensate")
	if compensate.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || compensate.GetChild().GetRunId() == "" {
		t.Fatalf("compensate = %v", compensate)
	}
	// The child was requested after deploy failed.
	order := []string{}
	for _, e := range eventsOf(h, "run-composed-handler") {
		if (e.GetTaskId() == "deploy" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED) ||
			(e.GetTaskId() == "compensate" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED) {
			order = append(order, e.GetTaskId())
		}
	}
	if !reflect.DeepEqual(order, []string{"deploy", "compensate"}) {
		t.Fatalf("event order = %v", order)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
}

const awaitedHandlerDocument = `
name: awaited-handler
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: approve
    needs: [deploy]
    join: any_failed
    await: {timeout_seconds: 3600}
  - id: escalate
    uses: shell
    needs: [approve]
    join: any_failed
    with: {op: exec}
`

func TestRunWorkflow_AwaitedHandlerOpensInAFailingRunAndResolves(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	})
	h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-awaited-handler", compile(t, awaitedHandlerDocument), payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-awaited-handler")
	approve := taskRunOf(t, run, "approve")
	if approve.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || approve.GetAwait().GetResolvedBy() != "alice" {
		t.Fatalf("approve = %v", approve)
	}
	if statusOf(run, "escalate") != orkstackv1.Status_STATUS_SKIPPED {
		t.Fatalf("escalate = %v", statusOf(run, "escalate"))
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "deploy exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	opened := false
	for _, e := range eventsFor(h, "run-awaited-handler", "approve") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING {
			opened = true
		}
	}
	if !opened {
		t.Fatal("the awaited handler never opened")
	}
}

func TestRunWorkflow_RejectedAwaitStartsItsAnyFailedHandler(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	reject := completion("gate", "", "bob")
	reject.Values, reject.ValuesJson, reject.Reject, reject.Reason = nil, nil, true, "not today"
	h.signalAt(time.Minute, reject)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-rejected-gate", compile(t, `
name: rejected-gate
tasks:
  - id: gate
    await: {timeout_seconds: 3600}
  - id: ship
    uses: shell
    needs: [gate]
    with: {op: exec}
  - id: tidy
    uses: shell
    needs: [gate]
    join: any_failed
    with: {op: exec}
`), payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-rejected-gate")
	if statusOf(run, "gate") != orkstackv1.Status_STATUS_FAILED || taskRunOf(t, run, "gate").GetError().GetCode() != engine.AwaitRejectedCode {
		t.Fatalf("gate = %v", taskRunOf(t, run, "gate"))
	}
	if statusOf(run, "tidy") != orkstackv1.Status_STATUS_SUCCEEDED || statusOf(run, "ship") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("tidy = %v, ship = %v", statusOf(run, "tidy"), statusOf(run, "ship"))
	}
	in, _ := h.inputFor("tidy")
	if got := in.GetJoin().GetNeeds()[0].GetError().GetCode(); got != engine.AwaitRejectedCode {
		t.Fatalf("tidy's join error = %q", got)
	}
}

func TestRunWorkflow_AnItemFailureStartsTheFanOutsHandler(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil && in.GetEach().GetIndex() == 1 {
			return nil, temporal.NewNonRetryableApplicationError("item exploded", task.CategoryActivityFailed, nil,
				&orkstackv1.ActivityFailureDetail{ReasonCode: "CONTAINER_EXIT_NONZERO",
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, Attempt: 1,
					Artifacts: []*orkstackv1.ArtifactRef{deployLog()}})
		}
		return okTask(in)
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-item-handler", compile(t, `
name: item-handler
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 5, max_parallel: 1}
    with: {op: exec}
  - id: report
    uses: shell
    needs: [convert]
    join: any_failed
    with: {op: exec}
`), payload(t, map[string]any{"regions": []any{"eu", "us", "ap"}}), defaultPlan())
	run := runOf(t, h, "run-item-handler")
	if statusOf(run, "report") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("report = %v", statusOf(run, "report"))
	}
	in, _ := h.inputFor("report")
	need := in.GetJoin().GetNeeds()[0]
	if need.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.HasPrefix(need.GetError().GetMessage(), "item 1 of 3 failed:") {
		t.Fatalf("report's join = %v", need)
	}
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == "convert" {
			t.Fatalf("a fan-out's item files were staged for its handler: %v", ref)
		}
	}
}

func TestRunWorkflow_ALostPinMemberStartsAnUnpinnedHandler(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, nil)
	h.answerThenLosePinHost(nil, temporal.NewNonRetryableApplicationError(
		"pin host stopped heartbeating", task.CategoryInfrastructureError, nil))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-lost-handler", compile(t, pinnedDocument+`
  - id: recover
    uses: shell
    needs: [prep]
    join: any_failed
    with: {command: "echo recover"}
`), payload(t, nil), defaultPlan())
	run := runOf(t, h, "run-pin-lost-handler")
	if got := taskRunOf(t, run, "prep").GetError().GetCode(); got != "PIN_HOST_LOST" {
		t.Fatalf("prep = %v", taskRunOf(t, run, "prep"))
	}
	if statusOf(run, "recover") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("recover = %v", taskRunOf(t, run, "recover"))
	}
	queue, ok := h.queueFor("recover")
	if !ok || queue != task.DefaultQueue {
		t.Fatalf("recover ran on queue %q, want the pool's own %q, not the lost session's", queue, task.DefaultQueue)
	}
	in, _ := h.inputFor("recover")
	if in.GetPin() != nil {
		t.Fatalf("recover carries a pin: %v", in.GetPin())
	}
}

func TestRunWorkflow_NoneFailedFanOutWhoseSourceWasSkippedFailsEachOverMissing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-each-over-missing", compile(t, `
name: each-over-missing
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: list
    uses: shell
    condition: "false"
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [a, list]
    join: none_failed
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5}
    with: {op: exec}
`), payload(t, nil), plan)
	run := runOf(t, h, "run-each-over-missing")
	convert := taskRunOf(t, run, "convert")
	want := `each.over {{ .tasks.list.output.parts }}: task "list" is SKIPPED, so its values do not exist — a fan-out ` +
		`joined with none_failed reads its array from an input or from a task that always runs`
	if convert.GetStatus() != orkstackv1.Status_STATUS_FAILED || convert.GetError().GetCode() != engine.EachOverMissingCode ||
		convert.GetError().GetMessage() != want ||
		convert.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Fatalf("convert = %v", convert.GetError())
	}
}

func TestRunWorkflow_HandlerEdgesDoNotRemoveLeavesFromTheRunOutput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"by": in.TaskId}), nil
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-handler-leaves", compile(t, `
name: handler-leaves
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [deploy]
    join: all_done
    with: {op: exec}
`), payload(t, nil), defaultPlan())
	run := mustWorkflowResult(t, h.env)
	fields := run.GetOutput().GetData().GetFields()
	if _, ok := fields["deploy"]; !ok {
		t.Errorf("output %v lacks deploy: a handler's needs edge removed it from the leaves", fields)
	}
	if _, ok := fields["notify"]; !ok {
		t.Errorf("output %v lacks notify, an all_done handler nothing needs", fields)
	}
	if _, ok := fields["rollback"]; ok {
		t.Errorf("output %v carries the skipped rollback", fields)
	}
}

// TestRunWorkflow_EveryTaskIsTerminalWhenAFailingRunEnds is the "no third
// state" property (AGENTS.md §11) over the failing shapes above.
func TestRunWorkflow_EveryTaskIsTerminalWhenAFailingRunEnds(t *testing.T) {
	failDeploy := func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" || in.TaskId == "page" {
			return nil, temporal.NewNonRetryableApplicationError(in.TaskId+" exploded", task.CategoryActivityFailed, nil)
		}
		return okTask(in)
	}
	for i, doc := range []string{joinFailWithoutHandlersDocument, twoHandlersDocument, escalateDocument,
		handlerCancelDocument, anyFailedSkippedDocument, awaitedHandlerDocument} {
		var suite testsuite.WorkflowTestSuite
		h := newHarness(t, &suite, failDeploy)
		runID := fmt.Sprintf("run-terminal-%d", i)
		h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc), payload(t, nil), defaultPlan())
		run := runOf(t, h, runID)
		for _, tr := range run.GetTaskRuns() {
			switch tr.GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED,
				orkstackv1.Status_STATUS_SKIPPED, orkstackv1.Status_STATUS_CANCELED:
			default:
				t.Errorf("%s: %s ended %v", runID, tr.GetTaskId(), tr.GetStatus())
			}
		}
		for _, e := range eventsOf(h, runID) {
			if strings.Contains(e.GetMessage(), "the run ended before this task") {
				t.Errorf("%s: F9's defensive sentence appeared: %v", runID, e)
			}
		}
	}
}

// TestRunWorkflow_ADefinitionWithoutJoinSendsTheInputsItAlwaysSent: the
// shipped example with every join removed and its handlers dropped sends no
// join on any input, sets none on any row, and emits no failing note.
func TestRunWorkflow_ADefinitionWithoutJoinSendsTheInputsItAlwaysSent(t *testing.T) {
	wf := compileExample(t, "branch-and-recover.ork.yaml")
	var kept []*orkstackv1.Task
	for _, tk := range wf.GetTasks() {
		switch tk.GetJoin() {
		case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED, orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
			continue
		}
		tk.Join = orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED
		kept = append(kept, tk)
	}
	wf.Tasks = kept
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"value": "canary"}), nil
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-join", wf, payload(t, nil), defaultPlan())
	if len(h.invocations()) == 0 {
		t.Fatal("nothing was dispatched")
	}
	for _, in := range h.invocations() {
		if in.GetJoin() != nil {
			t.Errorf("%s carries a JoinContext", in.GetTaskId())
		}
		for _, ref := range in.GetUpstream() {
			if ref.GetValues() == nil {
				t.Errorf("%s: an upstream ref with no values: %v", in.GetTaskId(), ref)
			}
		}
	}
	run := runOf(t, h, "run-no-join")
	for _, tr := range run.GetTaskRuns() {
		if tr.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			t.Errorf("%s: TaskRun.join = %v", tr.GetTaskId(), tr.GetJoin())
		}
	}
	for _, e := range eventsOf(h, "run-no-join") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetTaskId() == "" {
			t.Errorf("a failing note in a run with no join: %v", e)
		}
	}
}

// failNthSnapshotWhere fails, on every attempt, the snapshot write that comes
// `after` writes after the first snapshot for which match holds.
func failNthSnapshotWhere(match func(*orkstackv1.Run) bool, after int64) func(*harness) func(*orkstackv1.Run, int64) error {
	return func(*harness) func(*orkstackv1.Run, int64) error {
		var mu sync.Mutex
		first := int64(-1)
		return func(run *orkstackv1.Run, seq int64) error {
			mu.Lock()
			defer mu.Unlock()
			if first < 0 && match(run) {
				first = seq
			}
			if first >= 0 && seq == first+after {
				return errors.New("object store unavailable")
			}
			return nil
		}
	}
}

func TestRunWorkflow_AHandlersRefusedOrHalfDoneDispatchIsRecorded(t *testing.T) {
	rollbackRunning := func(run *orkstackv1.Run) bool {
		return statusOf(run, "rollback") == orkstackv1.Status_STATUS_RUNNING
	}
	compensateRunning := func(run *orkstackv1.Run) bool {
		return statusOf(run, "compensate") == orkstackv1.Status_STATUS_RUNNING
	}
	type outcome struct {
		run *orkstackv1.Run
		h   *harness
	}
	containerRun := func(t *testing.T, runID string, with string, limit int64,
		hook func(*harness) func(*orkstackv1.Run, int64) error) outcome {
		var suite testsuite.WorkflowTestSuite
		h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			if in.TaskId == "deploy" {
				return nil, temporal.NewNonRetryableApplicationError("deploy exploded", task.CategoryActivityFailed, nil)
			}
			return okTask(in)
		})
		if hook != nil {
			h.onSnapshot = hook(h)
		}
		plan := defaultPlan()
		plan.Limits = engine.WorkspaceLimits{ValuesMaxBytes: limit}
		h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, `
name: refused-handler
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec, blob: "`+with+`"}
`), payload(t, nil), plan)
		return outcome{runOf(t, h, runID), h}
	}
	cases := []struct {
		name    string
		handler string
		run     func(t *testing.T) outcome
		reason  string
	}{
		{"input over the bound", "rollback", func(t *testing.T) outcome {
			return containerRun(t, "run-refused-handler-0", bigWith, 4096, nil)
		}, task.ReasonInputTooLarge},
		{"TASK_STARTED write fails", "rollback", func(t *testing.T) outcome {
			return containerRun(t, "run-refused-handler-1", "small", 0, failNthSnapshotWhere(rollbackRunning, 1))
		}, "object store unavailable"},
		{"composed TASK_SCHEDULED write fails", "compensate", func(t *testing.T) outcome {
			run, h := composedHandlerRun(t, "run-refused-handler-2", failNthSnapshotWhere(compensateRunning, 0))
			return outcome{run, h}
		}, "object store unavailable"},
		{"composed TASK_STARTED write fails", "compensate", func(t *testing.T) outcome {
			run, h := composedHandlerRun(t, "run-refused-handler-3", failNthSnapshotWhere(func(run *orkstackv1.Run) bool {
				return taskRunOfRun(run, "compensate").GetChild().GetRunId() != ""
			}, 0))
			return outcome{run, h}
		}, "object store unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.run(t)
			handler := taskRunOf(t, o.run, tc.handler)
			if handler.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(handler.GetError().GetMessage(), tc.reason) {
				t.Fatalf("%s = %v, want FAILED with %q, never left RUNNING", tc.handler, handler, tc.reason)
			}
			if o.run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(o.run.GetError().GetMessage(), "deploy exploded") {
				t.Fatalf("run = %v / %v", o.run.GetStatus(), o.run.GetError())
			}
			for _, e := range eventsOf(o.h, o.run.GetId()) {
				if strings.Contains(e.GetMessage(), "the run ended before this task") {
					t.Errorf("F9's defensive sentence appeared: %v", e)
				}
			}
		})
	}
}
