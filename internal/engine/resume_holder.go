package engine

import (
	"errors"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// A resumed run is a reset of a closed one (E85 §30): the reset's reason
// names an immutable plan record, which the reset run loads at R, its
// reset point, before it decides anything (resumeHolder). A run that was
// never reset, and a reset made by hand, never reach any of it.

// workflowDeps are what RunWorkflow decides from beside its arguments, held
// apart so a test can stand in for what a test environment cannot produce.
type workflowDeps struct {
	// isResetRun says whether this execution is a reset run. A test
	// environment cannot reset a workflow, so its tests stand in for it.
	isResetRun func(info *workflow.Info) bool
	// hasConcurrency is PlanHasConcurrency.
	hasConcurrency func(RunPlan) bool
}

// liveDeps are RunWorkflow's own.
var liveDeps = workflowDeps{isResetRun: isResetRunInfo, hasConcurrency: PlanHasConcurrency}

// isResetRunInfo is the reset-run predicate (§30.9 MEDIUM-1): a fresh
// start, a continue-as-new, a retry and a cron run each set the original
// run id to the run's own, and a reset copies its base's, so only a reset
// run's differs. Deterministic: the same execution answers alike on replay.
func isResetRunInfo(info *workflow.Info) bool {
	return info.OriginalRunID != "" && info.OriginalRunID != info.WorkflowExecution.RunID
}

// RunResumeChange gates what a reset run does at R (AGENTS.md §7.2). It is
// asked by reset runs only, so no other history records it.
const RunResumeChange = "run-resume"

// resumeHolder is the plan a reset run carries out, loaded at R.
type resumeHolder struct {
	key    ResumePlanKey
	record *orkstackv1.ResumePlanRecord
}

// block is the plan's resume block.
func (h *resumeHolder) block() *orkstackv1.ResumeBlock { return h.record.GetBlock() }

// resumeLoadFailure is why a reset run could not load its plan: its code,
// and the key's scope when the key was read (0 when it was not).
type resumeLoadFailure struct {
	code    string
	message string
	scope   uint64
}

func (f *resumeLoadFailure) Error() string { return f.code + ": " + f.message }

// errResumeCanceled: the operator canceled the run while it loaded its plan;
// it ends canceled, as a cancel during a state load does (AGENTS.md §7.3).
var errResumeCanceled = errors.New("the run was canceled while it loaded its resume plan")

// resumeLoadTimeout bounds loading a plan: the run's pinned restore timeout,
// or the AGENTS-fixed resume_plan_load_timeout when its plan pins none.
func resumeLoadTimeout(plan RunPlan) time.Duration {
	if c := plan.Continuation; c != nil && c.Restore.TimeoutSeconds > 0 {
		return time.Duration(c.Restore.TimeoutSeconds) * time.Second
	}
	return ResumePlanLoadTimeout
}

// resumeLoadOptions are one load call's local-activity options: Temporal
// retries it in process until the load's deadline, with a backoff that
// stays below the workflow task timeout, so a failed try costs no event.
func resumeLoadOptions(ctx workflow.Context, deadline time.Time) (workflow.LocalActivityOptions, bool) {
	left := deadline.Sub(workflow.Now(ctx))
	wft := workflow.GetInfo(ctx).WorkflowTaskTimeout
	if left <= 0 || wft <= 0 {
		return workflow.LocalActivityOptions{}, false
	}
	ceiling := min(5*time.Second, wft/2)
	return workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: left,
		StartToCloseTimeout:    min(30*time.Second, left),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    min(time.Second, ceiling),
			BackoffCoefficient: 2,
			MaximumInterval:    ceiling,
		},
	}, true
}

// loadResumeAtR is what a run does at R (§30.2–§30.3): nothing at all
// unless it is a reset run, and for a reset run the plan its reset names,
// or nil for a reset made by hand. A plan that cannot be loaded is a
// *resumeLoadFailure. created is the run's creation, upserted as its memo
// first for a generation-0 run (§30.9 HIGH-3), so even a run whose plan
// cannot be loaded keeps its original created_at.
func loadResumeAtR(ctx workflow.Context, deps workflowDeps, namespace, runID string, plan RunPlan, generation0 bool, created time.Time) (*resumeHolder, error) {
	info := workflow.GetInfo(ctx)
	if !deps.isResetRun(info) {
		return nil, nil
	}
	// Asked only by a reset run, and lazily: a reset run replays its copied
	// prefix with the predicate already true, and a prefix that passed R
	// before this path existed, or a run reset by hand to a later point,
	// holds no marker here and carries on as it always has.
	if workflow.GetVersion(ctx, RunResumeChange, workflow.DefaultVersion, 1) < 1 {
		return nil, nil
	}
	if generation0 {
		if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{
			MemoCreatedAt: created.UTC().Format(time.RFC3339Nano),
		})); err != nil {
			return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "its creation time could not be recorded: " + err.Error()}
		}
	}
	deadline := workflow.Now(ctx).Add(resumeLoadTimeout(plan))

	var facts ResetFacts
	opts, ok := resumeLoadOptions(ctx, deadline)
	if !ok {
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "no time is left to read the reset"}
	}
	err := getLocal(ctx, runScope(ctx), workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, opts), ActivityReadResetFacts,
		ReadResetFactsInput{Namespace: namespace, RunID: runID, ExecutionID: info.WorkflowExecution.RunID}), &facts)
	if ctx.Err() != nil {
		return nil, errResumeCanceled
	}
	if err != nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "the reset that made this run could not be read: " + err.Error()}
	}
	key, isResume, err := planKeyOf(runID, facts.Reason)
	if err != nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanInvalid, message: categoryOf(err).appMessage()}
	}
	if !isResume {
		return nil, nil
	}

	var record orkstackv1.ResumePlanRecord
	if opts, ok = resumeLoadOptions(ctx, deadline); !ok {
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "no time is left to read the plan " + key.String(), scope: key.Scope}
	}
	err = getLocal(ctx, runScope(ctx), workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, opts), ActivityLoadResumePlan,
		LoadResumePlanInput{Namespace: namespace, RunID: runID, Reset: facts}), &record)
	if ctx.Err() != nil {
		return nil, errResumeCanceled
	}
	if err != nil {
		code := ReasonResumePlanUnreadable
		if reading := categoryOf(err); reading.nonRetryable() && reading.appType() == ReasonResumePlanInvalid {
			code = ReasonResumePlanInvalid
		}
		return nil, &resumeLoadFailure{code: code, message: fmt.Sprintf("the plan %s: %v", key, err), scope: key.Scope}
	}
	if record.GetBlock() == nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanInvalid, message: "the plan " + key.String() + " holds no block", scope: key.Scope}
	}
	return &resumeHolder{key: key, record: &record}, nil
}

// PlanHasConcurrency reports whether plan holds the run, or any task of it,
// to a concurrency limit (E88). A resumed run must retake its slots before
// anything reruns (§5.3 step 4); while that step is not built
// (resumeConcurrency), such a run is never resumed: ResumeRun refuses it,
// and a reset run of one ends explicitly. Any limit counts, a task's alone
// included: a limit this build cannot retake is refused, never passed.
func PlanHasConcurrency(plan RunPlan) bool { return plan.Concurrency != nil }

// resumeConcurrencyBuilt is whether resumeConcurrency retakes a resumed
// run's slots; the change that fills it sets this.
const resumeConcurrencyBuilt = false

// resumeConcurrencyUnbuilt is what a person reads of a run that cannot be
// resumed for its concurrency limit.
const resumeConcurrencyUnbuilt = "this run is held to a concurrency limit, and this release cannot resume a run that is; run it again"

// resumeConcurrency is §5.3 step 4 (E88 §18): clear the carried slot flag,
// release every gate of the plan below seq_base, re-upsert the gate memo
// and retake the run-level slot. Until it is filled it does nothing for a
// plan without concurrency and refuses a plan with one, so a resumed run
// never runs past its limit.
func (s *runState) resumeConcurrency(_ workflow.Context, hasConcurrency func(RunPlan) bool) error {
	if hasConcurrency(s.plan) {
		return runLevelError(RunConcurrencyUnsupportedCode, resumeConcurrencyUnbuilt)
	}
	return nil
}
