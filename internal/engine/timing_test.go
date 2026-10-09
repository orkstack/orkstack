package engine_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// These tests are the task-timing derivation's engine half (docs/UI_CLI_PARITY.md
// "Task timing"): scheduled_at is the dispatch instant, projected from the same
// workflow.Now the TASK_SCHEDULED event is stamped with, and started_at is
// the Orker's own report of when it began the attempt that decided the task —
// set only when an Orker reported one.

// timingStart is a fixed workflow clock, so a test can name the dispatch
// instant rather than infer it.
var timingStart = time.Date(2026, 9, 22, 9, 14, 2, 0, time.UTC)

// scheduledEventTimes is every TASK_SCHEDULED event's timestamp, by task.
func scheduledEventTimes(t *testing.T, h *harness, runID string) map[string]time.Time {
	t.Helper()
	out := map[string]time.Time{}
	for _, event := range h.bus.Events(runID, 0) {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			out[event.GetTaskId()] = event.GetTimestamp().AsTime()
		}
	}
	return out
}

// TestRunWorkflow_ScheduledAtIsTheTaskScheduledEventsTimestamp: for
// every dispatched task scheduled_at equals the TASK_SCHEDULED event's own
// timestamp — one clock read, two views — and a skipped task has none.
func TestRunWorkflow_ScheduledAtIsTheTaskScheduledEventsTimestamp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"stdout": in.TaskId}), nil
	})
	h.env.SetStartTime(timingStart)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-scheduled-at",
		compile(t, conditionDocument), payload(t, map[string]any{"enabled": false}), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	stored, ok := h.store.Get("run-scheduled-at")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	events := scheduledEventTimes(t, h, "run-scheduled-at")

	always := taskRunOf(t, stored, "always")
	eventAt, ok := events["always"]
	if !ok {
		t.Fatal("no TASK_SCHEDULED event for always")
	}
	if always.GetScheduledAt() == nil {
		t.Fatal("always scheduled_at is unset; it was dispatched")
	}
	if got := always.GetScheduledAt().AsTime(); !got.Equal(eventAt) {
		t.Fatalf("always scheduled_at = %v, TASK_SCHEDULED timestamp = %v; they are one clock read", got, eventAt)
	}
	t.Logf("E1 always: scheduled_at=%s TASK_SCHEDULED.timestamp=%s equal=%v",
		always.GetScheduledAt().AsTime().Format(time.RFC3339Nano), eventAt.Format(time.RFC3339Nano),
		always.GetScheduledAt().AsTime().Equal(eventAt))

	for _, id := range []string{"maybe", "after"} {
		tr := taskRunOf(t, stored, id)
		if tr.GetScheduledAt() != nil {
			t.Errorf("%s scheduled_at = %v, want unset: a skipped task was never scheduled",
				id, tr.GetScheduledAt().AsTime())
		}
		if _, ok := events[id]; ok {
			t.Errorf("%s has a TASK_SCHEDULED event but was skipped", id)
		}
	}
}

// TestRunWorkflow_StartedAtIsTheOrkersStartNotTheDispatch: the terminal
// snapshot's started_at is the ExecuteTaskResult's, not the workflow's clock,
// so started_at − scheduled_at is the wait the Orker saw.
func TestRunWorkflow_StartedAtIsTheOrkersStartNotTheDispatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	reported := map[string]time.Time{}
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := succeeded(in.TaskId, map[string]any{"stdout": in.TaskId})
		// The Orker's clock: seven seconds after the dispatch instant.
		start := timingStart.Add(7 * time.Second)
		mu.Lock()
		reported[in.TaskId] = start
		mu.Unlock()
		result.StartedAt = timestamppb.New(start)
		result.FinishedAt = timestamppb.New(start.Add(time.Second))
		return result, nil
	})
	h.env.SetStartTime(timingStart)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-started-at",
		compile(t, `
name: one
tasks:
  - id: only
    uses: shell
    with: {command: "echo only"}
`), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	stored, _ := h.store.Get("run-started-at")
	only := taskRunOf(t, stored, "only")
	if only.GetStartedAt() == nil || !only.GetStartedAt().AsTime().Equal(reported["only"]) {
		t.Fatalf("started_at = %v, want the Orker's reported start %v", only.GetStartedAt(), reported["only"])
	}
	wait := only.GetStartedAt().AsTime().Sub(only.GetScheduledAt().AsTime())
	if wait != 7*time.Second {
		t.Fatalf("started_at − scheduled_at = %v, want 7s", wait)
	}
	t.Logf("E2 started_at − scheduled_at = %v", wait)
}

// TestRunWorkflow_StartedAtComesFromTheFailureDetail: a failed task's
// started_at comes off ActivityFailureDetail.started_at, and every other
// field the failure path already recorded is unchanged.
func TestRunWorkflow_StartedAtComesFromTheFailureDetail(t *testing.T) {
	start := timingStart.Add(3 * time.Second)
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, temporal.NewNonRetryableApplicationError(
			"workload exited 4", task.CategoryActivityFailed, nil,
			&orkstackv1.ActivityFailureDetail{
				ReasonCode: "CONTAINER_EXIT_NONZERO",
				Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
				ExitCode:   4,
				Attempt:    1,
				OrkerId:    "orker-7",
				StartedAt:  timestamppb.New(start),
				SystemArtifacts: []*orkstackv1.ArtifactRef{
					{Name: "stdout.log", Uri: "mem://logs/stdout"},
				},
			})
	})
	h.env.SetStartTime(timingStart)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-failed-start",
		compile(t, `
name: doomed
tasks:
  - id: doomed
    uses: shell
    with: {command: "exit 4"}
`), payload(t, nil), defaultPlan())
	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the failure")
	}
	stored, _ := h.store.Get("run-failed-start")
	doomed := taskRunOf(t, stored, "doomed")
	if doomed.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("status = %v, want FAILED", doomed.GetStatus())
	}
	if doomed.GetStartedAt() == nil || !doomed.GetStartedAt().AsTime().Equal(start) {
		t.Fatalf("started_at = %v, want the detail's %v", doomed.GetStartedAt(), start)
	}
	if doomed.GetError().GetCode() != "CONTAINER_EXIT_NONZERO" ||
		doomed.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED ||
		doomed.GetExitCode() != 4 || doomed.GetOrkerId() != "orker-7" ||
		len(doomed.GetSystemArtifacts()) != 1 || doomed.GetAttempt() != 1 {
		t.Errorf("the failure path changed: code=%q category=%v exit=%d orker=%q artifacts=%d attempt=%d",
			doomed.GetError().GetCode(), doomed.GetError().GetCategory(), doomed.GetExitCode(),
			doomed.GetOrkerId(), len(doomed.GetSystemArtifacts()), doomed.GetAttempt())
	}
	if doomed.GetScheduledAt() == nil {
		t.Error("scheduled_at unset on a dispatched task")
	}
}

// TestRunWorkflow_LeavesStartedAtUnsetWhenNoOrkerReported: a failure no
// Orker described — a Temporal timeout, a plain error with no detail — leaves
// started_at unset while scheduled_at is set. "Not known" is never invented.
// The pin-loss branch is held in TestRunWorkflow_ClassifiesALostPinHostAsPinHostLost.
func TestRunWorkflow_LeavesStartedAtUnsetWhenNoOrkerReported(t *testing.T) {
	cases := map[string]error{
		"a temporal timeout": temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil),
		"an error with no failure detail": temporal.NewNonRetryableApplicationError(
			"the orker vanished", task.CategoryInfrastructureError, nil),
		"a plain error": errors.New("context canceled"),
	}
	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return nil, cause
			})
			h.env.SetStartTime(timingStart)
			h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-report",
				compile(t, `
name: lost
tasks:
  - id: lost
    uses: shell
    with: {command: "sleep 1"}
`), payload(t, nil), defaultPlan())
			if h.env.GetWorkflowError() == nil {
				t.Fatal("workflow error = nil, want the failure")
			}
			stored, _ := h.store.Get("run-no-report")
			lost := taskRunOf(t, stored, "lost")
			if lost.GetStartedAt() != nil {
				t.Errorf("started_at = %v, want unset: no Orker reported a start", lost.GetStartedAt().AsTime())
			}
			if lost.GetScheduledAt() == nil {
				t.Error("scheduled_at unset; the task was dispatched")
			}
			if lost.GetFinishedAt() == nil {
				t.Error("finished_at unset on a terminal task")
			}
		})
	}
}

// TestRunWorkflow_AdoptedCommitReportsTheCommittingAttemptsStart: the
// retry that adopts a commit reports the COMMITTING attempt's start, on the
// same rule `attempt` already follows.
func TestRunWorkflow_AdoptedCommitReportsTheCommittingAttemptsStart(t *testing.T) {
	t1 := timingStart.Add(2 * time.Second)
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	calls := 0
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			// Attempt 1 committed and then its Orker died before Temporal
			// heard: to the workflow it is a retryable infrastructure failure.
			return nil, temporal.NewApplicationError("orker lost after commit", task.CategoryInfrastructureError)
		}
		result := succeeded(in.TaskId, map[string]any{"ok": true})
		result.CommitAdopted = true
		result.Attempt = 1
		result.StartedAt = timestamppb.New(t1)
		result.FinishedAt = timestamppb.New(t1.Add(time.Second))
		return result, nil
	})
	h.env.SetStartTime(timingStart)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-adopted",
		compile(t, `
name: adopted
tasks:
  - id: committed
    uses: shell
    retry: {maximum_attempts: 2}
    with: {command: "echo ok"}
`), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("ExecuteTask ran %d times, want 2 (fail, then adopt on attempt 2)", calls)
	}
	stored, _ := h.store.Get("run-adopted")
	committed := taskRunOf(t, stored, "committed")
	if committed.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want the committing attempt 1", committed.GetAttempt())
	}
	if committed.GetStartedAt() == nil || !committed.GetStartedAt().AsTime().Equal(t1) {
		t.Errorf("started_at = %v, want the committing attempt's start %v", committed.GetStartedAt(), t1)
	}
}
