package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

const (
	// ScheduleFireWorkflowType is the workflow type of every schedule's action.
	ScheduleFireWorkflowType = "ScheduleFireWorkflow"
	// ActivityPrepareScheduledRun is the local activity a fire runs: the
	// ordinary submit preparation (internal/api).
	ActivityPrepareScheduledRun = "PrepareScheduledRun"

	// Memo keys on the FIRE workflow, upserted as the fire progresses so a
	// DescribeWorkflowExecution answers "which run" and "why not" without a
	// history read. A memo is small structured metadata (AGENTS.md §2.2).
	MemoScheduledRunID      = "orkflow_run_id"
	MemoScheduleFireRefusal = "orkflow_schedule_refusal"

	// ScheduleFireRefusedType is the ApplicationError type of a refused fire.
	// Non-retryable by construction: the ordinary submit refusal is final.
	ScheduleFireRefusedType = "ScheduleFireRefused"

	prepareScheduledRunStartToClose    = 30 * time.Second
	prepareScheduledRunScheduleToClose = 2 * time.Minute
)

// errNotStartedBySchedule is the refusal of a fire workflow that carries no
// nominal fire time: something other than a Temporal Schedule started it.
var errNotStartedBySchedule = errors.New("not started by a schedule")

// ScheduleFireWorkflow is the action every schedule takes. It is registered
// PINNED (register.go): it lives exactly as long as the RunWorkflow it
// starts, which is itself pinned to the same version, so pinning strands
// nothing a run was not already holding open, and no history of it ever
// has to replay against a newer build.
//
// Its history is one local-activity marker, two memo upserts, one child start
// and one child close. Every step is deterministic: it reads its argument,
// one search attribute the cluster stamped at start, and the local
// activity's recorded result.
func ScheduleFireWorkflow(ctx workflow.Context, rec *orkstackv1.ScheduleRecord) error {
	// 1. The nominal fire time, jitter included, stamped by the cluster.
	scheduledAt, ok := workflow.GetTypedSearchAttributes(ctx).GetTime(task.SearchKeyTemporalScheduledStartTime)
	if !ok {
		return refuseFire(ctx, errNotStartedBySchedule.Error(), errNotStartedBySchedule)
	}

	// 2. Prepare, as a LOCAL activity: the run worker is
	// LocalActivityWorkerOnly, and ork-tasks is polled for activities by
	// Orkers, which have never heard of this one.
	localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    prepareScheduledRunStartToClose,
		ScheduleToCloseTimeout: prepareScheduledRunScheduleToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        30 * time.Second,
			NonRetryableErrorTypes: []string{ScheduleFireRefusedType},
		},
	})
	var prep *orkstackv1.ScheduledRunPreparation
	if err := workflow.ExecuteLocalActivity(localCtx, ActivityPrepareScheduledRun, rec, scheduledAt).
		Get(localCtx, &prep); err != nil {
		// 3. A refusal, or a preparation that could not finish inside its
		// budget: either way this fire started no run and says why.
		return refuseFire(ctx, refusalMessage(err), err)
	}
	if prep == nil || prep.GetRun().GetId() == "" {
		err := errors.New("prepare returned no run")
		return refuseFire(ctx, err.Error(), err)
	}
	runID := prep.GetRun().GetId()

	// 4. Record the run id before the child starts, so a describe during the
	// child's own pending window already names the run.
	if err := workflow.UpsertMemo(ctx, map[string]any{MemoScheduledRunID: runID}); err != nil {
		return fmt.Errorf("record scheduled run id: %w", err)
	}

	// 5. Start the run as a child and wait for it, so this fire lives exactly
	// as long as the run and Temporal's overlap policies apply to the run.
	var plan RunPlan
	if err := json.Unmarshal(prep.GetPlan(), &plan); err != nil {
		msg := fmt.Sprintf("decode prepared plan for run %s: %v", runID, err)
		return refuseFire(ctx, msg, err)
	}
	options := workflow.ChildWorkflowOptions{
		WorkflowID: runID,
		// The fire's own channel, which is the engine's run channel the
		// schedule's action names (task.DefaultQueue in `ork`): read from
		// history, so the choice replays.
		TaskQueue:             workflow.GetInfo(ctx).TaskQueueName,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		// ABANDON: a fire terminated by hand, or cancelled by cancel-other,
		// must not take its run with it by the default parent-close policy.
		// Cancellation still reaches the run: the SDK requests cancellation
		// of a child started under a cancelled context.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		// A cancelled fire stays open until its run has actually ended, so a
		// buffered fire starts after the cancelled run is gone.
		WaitForCancellation: true,
		Memo:                RunStartMemo(prep.GetOrkflow(), plan),
	}
	if attrs := RunStartSearchAttributes(prep.GetOrkflow(), plan, prep.GetNamespaceIndexed(), prep.GetOriginIndexed()); len(attrs) > 0 {
		options.TypedSearchAttributes = temporal.NewSearchAttributes(attrs...)
	}
	future := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, options),
		RunWorkflowType, runID, prep.GetOrkflow(), prep.GetInput(), plan)
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		// The one start failure that exists: the id is already an execution.
		// A fresh id is never reissued here — a second collision is a broken
		// generator, and a fire has no caller waiting — so it is a refusal
		// naming the id.
		return refuseFire(ctx, fmt.Sprintf("start run %s: %v", runID, err), err)
	}
	// 6. The fire's outcome is the run's.
	return future.Get(ctx, nil)
}

// refuseFire records why this fire started no run and fails the fire
// non-retryably, which is what pause_on_failure reads.
func refuseFire(ctx workflow.Context, msg string, cause error) error {
	if err := workflow.UpsertMemo(ctx, map[string]any{MemoScheduleFireRefusal: msg}); err != nil {
		return fmt.Errorf("record fire refusal %q: %w", msg, err)
	}
	return temporal.NewNonRetryableApplicationError(msg, ScheduleFireRefusedType, cause)
}

// refusalMessage is the operator-readable reason of a failed preparation: the
// innermost application error's own message (the submit refusal, verbatim),
// or the error's text when there is none.
func refusalMessage(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Message() != "" {
		return app.Message()
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return fmt.Sprintf("prepare did not finish within %s: %v", prepareScheduledRunScheduleToClose, err)
	}
	return err.Error()
}
