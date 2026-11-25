package engine

import (
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReasonResumeNotReadBack is a kept row's terminal reason when the run ended
// before its result was read back (E85 §4.4): CANCELED, still reading_back,
// and kept by the next resume.
const ReasonResumeNotReadBack = "RESUME_NOT_READ_BACK"

// taskOutputInvalidCode is a kept result whose values do not decode, as
// collect names a fresh one's.
const taskOutputInvalidCode = "TASK_OUTPUT_INVALID"

// adoptResume carries out a loaded plan at R (E85 §5.3), for the parts this
// build has (resume_parts.go): step 1, the dispositions; step 2,
// RUN_RESUMED at seq_base; step 3, the read-back and RUN_RESULTS_KEPT;
// step 4, the run's concurrency slot. The main loop is step 5. A plan that
// needs a part not yet built ends the run as one that cannot be run, with
// nothing run again.
func (s *runState) adoptResume(ctx workflow.Context, deps workflowDeps) error {
	h := s.resume
	b := h.block()
	// Set until the plan is carried out, and left set when it is not, so
	// the ending that follows starts no failure handler.
	s.resumeAdopting = true
	// Before anything is set aside: a run held to a concurrency limit is
	// never resumed while this build cannot retake its slot (§5.3 step 4).
	if deps.hasConcurrency(s.plan) && !resumeConcurrencyBuilt {
		return &resumeLoadFailure{code: RunConcurrencyUnsupportedCode, message: resumeConcurrencyUnbuilt, scope: h.key.Scope}
	}
	units, err := DecodeResumeUnits(s.wf, b, false)
	if err != nil {
		return &resumeLoadFailure{code: ReasonResumePlanInvalid, message: err.Error(), scope: h.key.Scope}
	}
	if part, why, refused := ResumeNeedsUnbuilt(s.wf, resumeUnitsOf(units), false); refused {
		return &resumeLoadFailure{code: ReasonResumePlanInvalid, message: fmt.Sprintf("resume_unsupported:%s: %s", part, why), scope: h.key.Scope}
	}
	if b.GetSeqBase() < 1 || b.GetSeqBase() > maxResumeScope || int64(b.GetSeqBase()) <= int64(s.eventSeq) {
		return &resumeLoadFailure{code: ReasonResumePlanInvalid, message: fmt.Sprintf("its block's seq_base %d is not above this run's events", b.GetSeqBase()), scope: h.key.Scope}
	}
	if err := s.applyDispositions(ctx, b, units); err != nil {
		return &resumeLoadFailure{code: ReasonResumePlanInvalid, message: err.Error(), scope: h.key.Scope}
	}

	// Step 2: the ending the plan sets aside is behind the run.
	seqBase := int64(b.GetSeqBase())
	s.eventSeq = int(seqBase) - 1
	s.runError = nil
	s.finished = nil
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.eventResumed = s.runResumedOf(h, units)
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, "",
		orkstackv1.Status_STATUS_RUNNING, "run resumed"); err != nil {
		return err
	}

	// Step 3.
	started := workflow.Now(ctx)
	kept, err := s.readBack(ctx, b, units, started)
	if err != nil {
		return err
	}
	s.eventResultsKept = &orkstackv1.RunResultsKept{
		KeptCount:       int32(kept),
		ReadBackSeconds: workflow.Now(ctx).Sub(started).Seconds(),
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT, "",
		orkstackv1.Status_STATUS_RUNNING, fmt.Sprintf("%d results kept", kept)); err != nil {
		return err
	}

	// Step 4.
	if err := s.resumeConcurrency(ctx, deps.hasConcurrency); err != nil {
		return err
	}
	s.resumeAdopting = false
	return nil
}

// applyDispositions is step 1: every unit of S waits to run again under
// the resume's scope, every kept unit waits to be read back, and every
// unit kept skipped is skipped now. No task event is emitted.
func (s *runState) applyDispositions(ctx workflow.Context, b *orkstackv1.ResumeBlock, units []ResumeUnitPlan) error {
	now := timestamppb.New(workflow.Now(ctx))
	for _, u := range units {
		row := s.taskRuns[u.Task]
		if row == nil || u.Item >= 0 {
			return fmt.Errorf("its block treats %s, which this build does not carry out", u.unit())
		}
		row.ResumeDisposition = u.Disposition
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			row.ResultEpoch = int64(b.GetSeqBase())
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
			row.ReadingBack = true
			row.ResultEpoch = int64(u.Epoch)
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED:
			row.Status = orkstackv1.Status_STATUS_SKIPPED
			row.FinishedAt = now
		default:
			return fmt.Errorf("its block gives %s the disposition %v, which this build does not carry out", u.unit(), u.Disposition)
		}
	}
	return nil
}

// runResumedOf is RUN_RESUMED's account of h (§5.4).
func (s *runState) runResumedOf(h *resumeHolder, units []ResumeUnitPlan) *orkstackv1.RunResumed {
	b, rec := h.block(), h.record
	var rerun, kept int32
	for _, u := range units {
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			rerun++
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
			kept++
		}
	}
	return &orkstackv1.RunResumed{
		Action:               b.GetAction(),
		FromTaskId:           b.GetFromTaskId(),
		ResumedAfterSequence: rec.GetBaseTerminalSequence(),
		RerunCount:           rerun,
		KeptCount:            kept,
		Principal:            rec.GetPrincipal(),
		PrincipalVerified:    rec.GetPrincipalVerified(),
		Reason:               rec.GetReason(),
		SupersededReason:     rec.GetSupersededReason(),
		ResumeCount:          b.GetResumesBefore() + 1,
	}
}

func (s *runState) takeEventResumed() *orkstackv1.RunResumed {
	r := s.eventResumed
	s.eventResumed = nil
	return r
}

func (s *runState) takeEventResultsKept() *orkstackv1.RunResultsKept {
	r := s.eventResultsKept
	s.eventResultsKept = nil
	return r
}

// readbackTunablesOf are the block's pinned read-back numbers.
func readbackTunablesOf(r *orkstackv1.ResumeReadback) (readbackTunables, error) {
	t := readbackTunables{
		Deadline:         r.GetDeadline().AsDuration(),
		Budget:           r.GetBudget(),
		MaxEntries:       int(r.GetMaxEntries()),
		Concurrency:      int(r.GetConcurrency()),
		GetTimeout:       r.GetGetTimeout().AsDuration(),
		GetTries:         int(r.GetGetTries()),
		CallTimeout:      r.GetCallTimeout().AsDuration(),
		RetryInitial:     r.GetRetryInitial().AsDuration(),
		RetryCoefficient: r.GetRetryCoefficient(),
		CallsPerTask:     int(r.GetCallsPerTask()),
	}
	if t.Deadline <= 0 || t.Budget <= 0 || t.MaxEntries <= 0 || t.Concurrency <= 0 || t.GetTimeout <= 0 ||
		t.GetTries <= 0 || t.CallTimeout <= 0 || t.RetryInitial <= 0 || t.RetryCoefficient < 1 || t.CallsPerTask <= 0 {
		return readbackTunables{}, fmt.Errorf("its block pins no complete read-back bounds")
	}
	return t, nil
}

// readBack is step 3 (§4.3): every kept row's result, read back from its
// commit in calls packed in task order, at most CallsPerTask calls per
// workflow task, each bounded by the phase's one recorded deadline. A
// result read back is applied silently, as an adopted attempt's; a call
// that fails fails the first task it did not read back.
func (s *runState) readBack(ctx workflow.Context, b *orkstackv1.ResumeBlock, units []ResumeUnitPlan, started time.Time) (int, error) {
	var entries []*orkstackv1.AdoptEntry
	var sizes []int64
	for _, u := range units {
		id := u.Task
		if !readBackUnit(s.tasks[id], u) {
			continue
		}
		// The entry the resume was verified with (ResumeVerifier).
		entry, err := adoptEntryOf(s.wf, s.plan.CommitRoot, s.runID, s.tasks[id], int32(u.Item), int64(u.Epoch))
		if err != nil {
			return 0, temporal.NewNonRetryableApplicationError(err.Error(), "INVALID_COMMIT_KEY", err)
		}
		entries = append(entries, entry)
		sizes = append(sizes, u.EncodedSize)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	tun, err := readbackTunablesOf(b.GetReadback())
	if err != nil {
		return 0, s.readBackFailed(ctx, entries[0].GetTaskId(), err)
	}
	calls, err := packReadback(sizes, tun)
	if err != nil {
		return 0, s.readBackFailed(ctx, entries[0].GetTaskId(), err)
	}
	deadline := started.Add(tun.Deadline)
	wft := workflow.GetInfo(ctx).WorkflowTaskTimeout
	kept := 0
	for at, call := range calls {
		if at > 0 && at%tun.CallsPerTask == 0 {
			// A new workflow task for the next calls, so one task's
			// completion carries at most CallsPerTask results.
			if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
				return kept, s.readBackFailed(ctx, entries[call[0]].GetTaskId(), err)
			}
		}
		first := entries[call[0]].GetTaskId()
		timeouts, ok := readbackCallTimeouts(tun, wft, workflow.Now(ctx), deadline)
		if !ok {
			return kept, s.readBackFailed(ctx, first, fmt.Errorf("the read-back's deadline passed"))
		}
		opts := workflow.LocalActivityOptions{
			StartToCloseTimeout:    timeouts.startToClose,
			ScheduleToCloseTimeout: timeouts.scheduleToClose,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    tun.RetryInitial,
				BackoffCoefficient: tun.RetryCoefficient,
				MaximumInterval:    timeouts.maxInterval,
			},
		}
		var result orkstackv1.AdoptRecordedResult
		err := s.getLocal(ctx, workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, opts), ActivityAdoptRecorded,
			&orkstackv1.AdoptRecordedInput{
				Entries:     entries[call[0]:call[1]],
				Concurrency: int32(tun.Concurrency),
				GetTimeout:  durationpb.New(tun.GetTimeout),
				GetTries:    int32(tun.GetTries),
				Budget:      tun.Budget,
			}), &result)
		if err != nil {
			return kept, s.readBackFailed(ctx, first, err)
		}
		if got, sent := len(result.GetEntries()), call[1]-call[0]; got != sent {
			return kept, s.readBackFailed(ctx, first, fmt.Errorf("the read-back answered %d of %d entries", got, sent))
		}
		for _, e := range result.GetEntries() {
			if err := s.applyReadBack(e); err != nil {
				return kept, s.readBackFailed(ctx, e.GetTaskId(), err)
			}
			kept++
		}
	}
	return kept, nil
}

// applyReadBack is one kept row's result, applied as collect applies an
// adopted attempt's, silently: the row reads SUCCEEDED and adopted, with
// its committing attempt's facts.
func (s *runState) applyReadBack(e *orkstackv1.AdoptedEntry) error {
	id, result := e.GetTaskId(), e.GetResult()
	row := s.taskRuns[id]
	if row == nil || !row.GetReadingBack() {
		return fmt.Errorf("the read-back answered %q, which it was not reading back", id)
	}
	values := decodeValues(result.GetValuesJson())
	output, err := payloadOf(values)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("task %q's kept values: %v", id, err), taskOutputInvalidCode, err)
	}
	delete(s.listed, id)
	row.Error = nil
	row.Status = orkstackv1.Status_STATUS_SUCCEEDED
	row.StartedAt = result.GetStartedAt()
	row.FinishedAt = result.GetFinishedAt()
	row.Artifacts = result.GetArtifacts()
	row.SystemArtifacts = result.GetSystemArtifacts()
	row.ValuesRef = result.GetValuesRef()
	row.Commit = result.GetCommit()
	row.ExitCode = result.GetExitCode()
	row.OrkerId = result.GetOrkerId()
	if result.GetAttempt() > 0 {
		row.Attempt = result.GetAttempt()
	}
	row.Cache = result.GetCache()
	row.Output = output
	row.Adopted = true
	row.ReadingBack = false
	s.results[id] = result
	s.outputs[id] = values
	return nil
}

// readBackFailed fails the first task not read back (§4.3): its reason is
// the read-back's, and the run fails as any task failure fails it, with its
// unread rows written CANCELED and RESUME_NOT_READ_BACK (§4.4).
func (s *runState) readBackFailed(ctx workflow.Context, id string, cause error) error {
	row := s.taskRuns[id]
	if row == nil || !row.GetReadingBack() {
		// An answer for a row it was not reading back: the first row still
		// reading back takes the failure.
		cause = fmt.Errorf("an answer for task %q, which it was not reading back: %w", id, cause)
		row, id = nil, ""
		for _, at := range s.order {
			if s.taskRuns[at].GetReadingBack() {
				row, id = s.taskRuns[at], at
				break
			}
		}
		if row == nil {
			return &readBackFailure{cause: cause}
		}
	}
	reading := categoryOf(cause)
	code := reading.appType()
	if code == "" || !reading.isApplication() {
		code = ReasonCommitReadFailed
	}
	row.ReadingBack = false
	row.Status = orkstackv1.Status_STATUS_FAILED
	row.FinishedAt = timestamppb.New(workflow.Now(ctx))
	row.Error = &orkstackv1.Error{Code: code, Message: cause.Error(), Category: readBackCategory(code)}
	return &readBackFailure{task: id, cause: cause}
}

// readBackCategory is the category of a read-back's reason: a result that is
// there and wrong is a contract violation, as its Orker names it, and one
// that is gone or could not be read is the store's.
func readBackCategory(code string) orkstackv1.FailureCategory {
	switch code {
	case ReasonCommitMissing, ReasonCommitReadFailed:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	case ReasonCommitIdentity, ReasonCommitUndecodable, ReasonValuesMissing, taskOutputInvalidCode:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	}
	return orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
}

// readBackFailure is a read-back that failed task.
type readBackFailure struct {
	task  string
	cause error
}

func (f *readBackFailure) Error() string {
	return fmt.Sprintf("task %q: its kept result could not be read back: %v", f.task, f.cause)
}

// endNotReadBack writes every row still reading back CANCELED with
// RESUME_NOT_READ_BACK at a terminal ending (§4.4), still reading_back and
// with its disposition, so the next resume keeps it. No task event.
func (s *runState) endNotReadBack(ctx workflow.Context) {
	now := timestamppb.New(workflow.Now(ctx))
	for _, id := range s.order {
		row := s.taskRuns[id]
		if !row.GetReadingBack() {
			continue
		}
		row.Status = orkstackv1.Status_STATUS_CANCELED
		row.FinishedAt = now
		row.Error = &orkstackv1.Error{Code: ReasonResumeNotReadBack, Message: "the run ended before this kept result was read back; the next resume keeps it",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
	}
}
