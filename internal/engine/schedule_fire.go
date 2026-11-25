package engine

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/stablejson"
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

	// MemoScheduleFireSkipCode and MemoScheduleFireSkipMessage say why a
	// fire that completed started no run, or why the run it started never
	// began: a concurrency limit's (E88 §7.2). They live as long as the
	// fire's own record on the cluster, which is at least the run
	// retention.
	MemoScheduleFireSkipCode    = "orkflow_schedule_skip_code"
	MemoScheduleFireSkipMessage = "orkflow_schedule_skip_message"

	// ScheduleFireRefusedType is the ApplicationError type of a refused fire.
	// Non-retryable by construction: the ordinary submit refusal is final.
	ScheduleFireRefusedType = "ScheduleFireRefused"
	// ScheduleFireNotStartedType is the ApplicationError type of a
	// preparation a concurrency limit kept from starting a run, its details
	// a FireNotStarted. The fire completes, so the schedule never pauses
	// for it.
	ScheduleFireNotStartedType = "ScheduleFireNotStarted"

	prepareScheduledRunStartToClose    = 30 * time.Second
	prepareScheduledRunScheduleToClose = 2 * time.Minute
)

// The codes of a fire a concurrency limit kept from starting a run
// (docs/REASONS.md); none fails the fire.
const (
	FireSkippedConcurrencyCode     = "FIRE_SKIPPED_CONCURRENCY"
	FireNotStartedConcurrencyCode  = "FIRE_NOT_STARTED_CONCURRENCY"
	FireConcurrencyUnavailableCode = "FIRE_CONCURRENCY_UNAVAILABLE"
)

// FireNotStarted is why a fire started no run. RetryFor, above zero, is
// how long the fire asks again before it completes with Code: a check that
// could not be made, which the preparation pins from its own settings.
type FireNotStarted struct {
	Code     string
	Message  string
	RetryFor time.Duration
}

// NewFireNotStarted is the preparation's error for f.
func NewFireNotStarted(f FireNotStarted) error {
	return temporal.NewNonRetryableApplicationError(f.Message, ScheduleFireNotStartedType, nil, f)
}

// The spacing of a fire's asks while a concurrency check cannot be made.
const (
	fireUnavailableBackoffMin = 10 * time.Second
	fireUnavailableBackoffMax = time.Minute
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
	fireScope := runScope(ctx)
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
	var unavailableSince time.Time
	backoff := fireUnavailableBackoffMin
	for {
		err := workflow.ExecuteLocalActivity(localCtx, ActivityPrepareScheduledRun, rec, scheduledAt).Get(localCtx, &prep)
		if err == nil {
			break
		}
		// 3. A concurrency limit kept it from starting a run: the fire
		// completes, asking again first while the limit cannot be checked.
		var notStarted FireNotStarted
		if failure := categoryOf(err); failure.appType() == ScheduleFireNotStartedType && failure.appDetails(&notStarted) {
			if notStarted.RetryFor <= 0 {
				return completeNotStarted(ctx, notStarted.Code, notStarted.Message)
			}
			now := workflow.Now(ctx)
			if unavailableSince.IsZero() {
				unavailableSince = now
			}
			left := notStarted.RetryFor - now.Sub(unavailableSince)
			if left <= 0 {
				return completeNotStarted(ctx, notStarted.Code, fmt.Sprintf(
					"concurrency could not be checked for %s (%s); this fire started no run", notStarted.RetryFor, notStarted.Message))
			}
			if err := workflow.Sleep(ctx, min(backoff, left)); err != nil {
				return err
			}
			backoff = min(2*backoff, fireUnavailableBackoffMax)
			continue
		}
		// 4. A refusal, or a preparation that could not finish inside its
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
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{MemoScheduledRunID: runID})); err != nil {
		return fmt.Errorf("record scheduled run id: %w", err)
	}

	// 5. Start the run as a child and wait for it, so this fire lives exactly
	// as long as the run and Temporal's overlap policies apply to the run.
	// The plan was written by stablejson, its durations as nanoseconds.
	var plan RunPlan
	if err := json.Unmarshal(prep.GetPlan(), &plan, stablejson.DurationAsNano); err != nil {
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
		// Stamped with workflow time, read from history: never later than
		// the run's own start, and the same on every replay.
		Memo: plainMemo(ctx, RunStartMemoAt(prep.GetOrkflow(), plan, workflow.Now(ctx))),
	}
	if attrs := RunStartSearchAttributes(prep.GetOrkflow(), plan, prep.GetNamespaceIndexed(), prep.GetOriginIndexed()); len(attrs) > 0 {
		options.TypedSearchAttributes = temporal.NewSearchAttributes(attrs...)
	}
	future := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, options),
		// The fire's run is started with the arguments its preparation gave:
		// never sealed under the schedule's key, which its run cannot open
		// (a sealed fire hands its run's start on already sealed: P20b).
		RunWorkflowType, payloadcrypt.PlainIn(fireScope, runID), payloadcrypt.PlainIn(fireScope, prep.GetOrkflow()),
		payloadcrypt.PlainIn(fireScope, prep.GetInput()), payloadcrypt.PlainIn(fireScope, plan))
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		// The one start failure that exists: the id is already an execution.
		// A fresh id is never reissued here — a second collision is a broken
		// generator, and a fire has no caller waiting — so it is a refusal
		// naming the id.
		return refuseFire(ctx, fmt.Sprintf("start run %s: %v", runID, err), err)
	}
	// 6. The fire's outcome is the run's, unless a limit ended it.
	return fireOutcome(ctx, plan, future.Get(ctx, nil))
}

// fireOutcome is the fire's ending for its run's (E88 §7.2): a run a limit
// kept from starting completes the fire with the reason, or refuses it for a
// refuse limit's full key; anything else is the run's own.
func fireOutcome(ctx workflow.Context, plan RunPlan, runErr error) error {
	failure := categoryOf(runErr)
	if runErr == nil || plan.Concurrency == nil || plan.Concurrency.runSlot() == nil {
		return runErr
	}
	// Only a run that never started: one that ran and then lost its slot
	// (after a pause, or carrying on) failed as itself.
	var notStarted ConcurrencyRunNotStarted
	if !failure.appDetails(&notStarted) || !notStarted.NotStarted {
		return runErr
	}
	slot := plan.Concurrency.runSlot()
	switch failure.appType() {
	case RunConcurrencyLimitCode:
		if slot.Strategy == ConcurrencyStrategySkip {
			return completeNotStarted(ctx, FireSkippedConcurrencyCode,
				fmt.Sprintf("key %s was full; this fire started no run (%s)", slot.Key, failure.appMessage()))
		}
		return refuseFire(ctx, failure.appMessage(), runErr)
	case RunQueueTimeoutCode, RunConcurrencyFullCode, RunConcurrencyUnavailableCode,
		RunConcurrencyFencedCode, RunConcurrencyUnsupportedCode:
		return completeNotStarted(ctx, FireNotStartedConcurrencyCode, "this fire started no run: "+failure.appMessage())
	}
	return runErr
}

// sealedFireWords is what a fire that is not unsealed records for the reason
// it started no run.
const sealedFireWords = "this fire started no run; why is not shown, because its schedule is encrypted"

// fireWords is msg as a fire's memo may hold it: fixed words, unless the
// fire is unsealed. A memo is never sealed, and msg can quote what is: a
// concurrency key rendered from inputs, a refusal's text, a run's failure. A
// lost key (a nil scope) is not unsealed.
func fireWords(ctx workflow.Context, msg string) string {
	if runScope(ctx) == payloadcrypt.Unsealed() {
		return msg
	}
	return sealedFireWords
}

// completeNotStarted records why this fire started no run, or why its run
// never began, and completes the fire.
func completeNotStarted(ctx workflow.Context, code, msg string) error {
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{
		MemoScheduleFireSkipCode: code, MemoScheduleFireSkipMessage: fireWords(ctx, msg)})); err != nil {
		return fmt.Errorf("record why fire started no run %q: %w", msg, err)
	}
	return nil
}

// refuseFire records why this fire started no run and fails the fire
// non-retryably, which is what pause_on_failure reads.
func refuseFire(ctx workflow.Context, msg string, cause error) error {
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{MemoScheduleFireRefusal: fireWords(ctx, msg)})); err != nil {
		return fmt.Errorf("record fire refusal %q: %w", msg, err)
	}
	return temporal.NewNonRetryableApplicationError(msg, ScheduleFireRefusedType, cause)
}

// refusalMessage is the operator-readable reason of a failed preparation: the
// innermost application error's own message (the submit refusal, verbatim),
// or the error's text when there is none.
func refusalMessage(err error) string {
	failure := categoryOf(err)
	if message := failure.appMessage(); message != "" {
		return message
	}
	if failure.timedOut() {
		return fmt.Sprintf("prepare did not finish within %s: %v", prepareScheduledRunScheduleToClose, err)
	}
	return err.Error()
}
