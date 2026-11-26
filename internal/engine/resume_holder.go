package engine

import (
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
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
	// limitUnbuilt is ResumeLimitUnbuilt.
	limitUnbuilt func(RunPlan) bool
}

// liveDeps are RunWorkflow's own.
var liveDeps = workflowDeps{isResetRun: isResetRunInfo, limitUnbuilt: ResumeLimitUnbuilt}

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

// resumeCanceled: the operator canceled the run before its resume was carried
// out; it ends canceled, as a cancel during a state load does (AGENTS.md §7.3),
// over what it was to set aside, at scope (0 when the key was not read).
func resumeCanceled(scope uint64) *resumeLoadFailure {
	return &resumeLoadFailure{code: runCanceledCode, message: "run canceled", scope: scope}
}

// canceledResetTimeout bounds the one read a canceled run makes of its reset,
// to end at a sequence its base never used.
const canceledResetTimeout = 10 * time.Second

// canceledAtReset is what a run canceled while it read its reset is: nil for
// one reset by hand, which is canceled as any run is. The one read it makes is
// on a context the cancel does not reach.
func canceledAtReset(ctx workflow.Context, runID string, in ReadResetFactsInput) error {
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	var facts ResetFacts
	err := getLocal(dctx, runScope(ctx), workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(dctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: canceledResetTimeout, ScheduleToCloseTimeout: canceledResetTimeout,
	}), ActivityReadResetFacts, in), &facts)
	if err != nil {
		workflow.GetLogger(ctx).Error("a reset run canceled as it read the reset that made it cannot read it; it ends with no event", "run_id", runID, "error", err)
		return resumeCanceled(0)
	}
	key, isResume, err := planKeyOf(runID, facts.Reason)
	if err != nil || isResume {
		return resumeCanceled(key.Scope)
	}
	return nil
}

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
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "no time is left to read how this run was resumed"}
	}
	reading := ReadResetFactsInput{Namespace: namespace, RunID: runID, ExecutionID: info.WorkflowExecution.RunID}
	err := getLocal(ctx, runScope(ctx), workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, opts), ActivityReadResetFacts, reading), &facts)
	if err != nil && ctx.Err() != nil {
		return nil, canceledAtReset(ctx, runID, reading)
	}
	if err != nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "how this run was resumed could not be read: " + resumeSentence(err, "it was not answered in time")}
	}
	key, isResume, err := planKeyOf(runID, facts.Reason)
	if err != nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanInvalid, message: categoryOf(err).appMessage()}
	}
	if !isResume {
		return nil, nil
	}
	if ctx.Err() != nil {
		return nil, resumeCanceled(key.Scope)
	}

	var record orkstackv1.ResumePlanRecord
	if opts, ok = resumeLoadOptions(ctx, deadline); !ok {
		return nil, &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: "no time is left to read the plan " + key.String(), scope: key.Scope}
	}
	err = getLocal(ctx, runScope(ctx), workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, opts), ActivityLoadResumePlan,
		LoadResumePlanInput{Namespace: namespace, RunID: runID, Reset: facts}), &record)
	if ctx.Err() != nil {
		return nil, resumeCanceled(key.Scope)
	}
	if err != nil {
		code := ReasonResumePlanUnreadable
		if reading := categoryOf(err); reading.nonRetryable() && reading.appType() == ReasonResumePlanInvalid {
			code = ReasonResumePlanInvalid
		}
		return nil, &resumeLoadFailure{code: code, message: fmt.Sprintf("the plan %s: %s", key, resumeSentence(err, "it was not read in time")), scope: key.Scope}
	}
	if record.GetBlock() == nil {
		return nil, &resumeLoadFailure{code: ReasonResumePlanInvalid, message: "the plan " + key.String() + " holds no block", scope: key.Scope}
	}
	return &resumeHolder{key: key, record: &record}, nil
}

// PlanHasConcurrency reports whether plan holds the run, or any task of it,
// to a concurrency limit (E88).
func PlanHasConcurrency(plan RunPlan) bool { return plan.Concurrency != nil }

// ResumeLimitUnbuilt reports whether plan holds the run to a limit a resumed
// run cannot retake yet: one of its tasks alone (E88 P7). Such a run is never
// resumed: ResumeRun refuses it, and a reset run of one ends explicitly.
func ResumeLimitUnbuilt(plan RunPlan) bool {
	return plan.Concurrency != nil && plan.Concurrency.Run == nil
}

// resumeConcurrencyUnbuilt is what a person reads of a run that cannot be
// resumed for its concurrency limit.
const resumeConcurrencyUnbuilt = "this run's tasks are held to a concurrency limit, and this release cannot resume a run whose are; run it again"

// resumeConcurrency is §5.3 step 4 (E88 §18): the run's earlier incarnations
// give up every slot and place they may still hold, its records name the
// resume's own, and it asks for the run-level slot anew, waiting in line if it
// must. A run with no run-level limit has none to take.
//
// 4.1: what the replayed prefix held is not held: its gate freed it as the
// base ended. 4.2: release_all below seq_base, to every gate of the plan and
// not only those the prefix asked, which is before the base's task-level
// acquires. 4.3: the memo and attributes. 4.4: the acquire, after a workflow
// task of its own has committed the two before it, so the gate never decides
// it against a record of the run it has already left (CR-H2).
func (s *runState) resumeConcurrency(ctx workflow.Context, seqBase int64) error {
	c := s.conc
	if c == nil {
		return nil
	}
	c.held, c.queued, c.requeue, c.givenBack, c.ordinal = nil, nil, nil, false, 0
	c.seqBase = seqBase
	// On a context the run's own cancel does not reach, as releaseSlots; the
	// wait for them ends with it.
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	scope := runScope(ctx)
	var futures []workflow.Future
	memo := map[string]any{gateMemoSeqKey: seqBase}
	for _, slot := range c.plan.Slots {
		futures = append(futures, workflow.SignalExternalWorkflow(dctx, slot.GateID, "", GateSignalReleaseAll,
			// The gate is no run and holds no key: ids and a sequence only.
			payloadcrypt.PlainIn(scope, GateReleaseAll{Run: s.runID, Below: seqBase})))
		memo[gateMemoKey(slot.GateID, gateRunLevel)] = nil
	}
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, memo)); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the resumed run could not record its concurrency slots: %v", err))
	}
	c.seqWritten = true
	if err := s.syncConcAttributes(ctx); err != nil {
		return err
	}
	for i, f := range futures {
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(f, func(workflow.Future) {})
		sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		sel.Select(ctx)
		if ctx.Err() != nil {
			return temporal.NewCanceledError()
		}
		// A gate that is gone, or that carries its state on, has nothing a
		// later incarnation's acquire does not settle: never a reason to end.
		if err := f.Get(dctx, nil); err != nil && !categoryOf(err).unknownRun {
			workflow.GetLogger(ctx).Warn("a resumed run could not release an earlier concurrency slot; the limit's check pass frees it",
				"run", s.runID, "gate", c.plan.Slots[i].GateID, "error", err)
		}
	}
	err := s.askRunSlot(ctx, orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_RESUME)
	if err != nil && c.queued != nil {
		// It leaves the line without a slot.
		s.noteWaitEnd(ctx, waitOutcomeOf(ctx, err))
		c.queued = nil
		if serr := s.syncConcAttributes(ctx); serr != nil {
			workflow.GetLogger(ctx).Warn("a resumed run that left the line could not update its index", "run_id", s.runID, "error", serr)
		}
	}
	return err
}
