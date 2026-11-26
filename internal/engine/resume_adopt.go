package engine

import (
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
	// Before anything is set aside: a run held to a limit this build cannot
	// retake is never resumed (§5.3 step 4).
	if deps.limitUnbuilt(s.plan) {
		return &resumeLoadFailure{code: RunConcurrencyUnsupportedCode, message: resumeConcurrencyUnbuilt, scope: h.key.Scope}
	}
	units, err := DecodeResumeUnits(s.wf, b, s.generation > 0)
	if err != nil {
		return &resumeLoadFailure{code: ReasonResumePlanInvalid, message: err.Error(), scope: h.key.Scope}
	}
	if part, why, refused := ResumeNeedsUnbuilt(s.wf, s.plan); refused {
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
	s.clearRunClock()
	s.eventResumed = s.runResumedOf(h, units)
	s.resumed, s.resumeEntry, s.resumeCount = true, h.entry(), b.GetResumesBefore()+1
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, "",
		orkstackv1.Status_STATUS_RUNNING, "run resumed"); err != nil {
		return err
	}

	// Step 3.
	started := workflow.Now(ctx)
	kept, settled, err := s.readBack(ctx, b, units, started)
	if err != nil {
		return err
	}
	s.eventResultsKept = &orkstackv1.RunResultsKept{
		KeptCount:       int32(kept),
		SettledCount:    int32(settled),
		ReadBackSeconds: workflow.Now(ctx).Sub(started).Seconds(),
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT, "",
		orkstackv1.Status_STATUS_RUNNING, fmt.Sprintf("%d results kept", kept+settled)); err != nil {
		return err
	}

	// Step 4.
	if err := s.resumeConcurrency(ctx, seqBase); err != nil {
		return err
	}
	s.resumeAdopting = false
	// Nothing took an answer while the results were read back, and no await
	// is open yet: one that came is ignored, as one to an await not open is,
	// and never answers the wait it was not sent to.
	s.drainResolutions(ctx)
	return nil
}

// applyDispositions is step 1: every unit of S waits to run again under
// the resume's scope, every kept unit waits to be read back, and every
// unit kept skipped is skipped now. No task event is emitted.
func (s *runState) applyDispositions(ctx workflow.Context, b *orkstackv1.ResumeBlock, units []ResumeUnitPlan) error {
	now := timestamppb.New(workflow.Now(ctx))
	for at := 0; at < len(units); {
		u := units[at]
		items := units[at+1 : at+1]
		for at++; at < len(units) && units[at].Item >= 0; at++ {
			items = items[:len(items)+1]
		}
		row := s.taskRuns[u.Task]
		if row == nil || u.Item >= 0 {
			return fmt.Errorf("its block treats %s, which this build does not carry out", u.unit())
		}
		if u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED {
			continue // it ended before the run carried on (resumeRows)
		}
		row.ResumeDisposition = u.Disposition
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			row.ResultEpoch = int64(b.GetSeqBase())
			if u.Fresh {
				s.readNoMemo(u.unit())
			}
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
			row.ReadingBack = true
			row.ResultEpoch = int64(u.Epoch)
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED:
			row.Status = orkstackv1.Status_STATUS_SKIPPED
			row.FinishedAt = now
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE:
			row.ReadingBack = true
			row.ResultEpoch = int64(u.Epoch)
			s.restoreRecorded(u)
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL:
			// It woke before the run ended: it is done, silently.
			s.restoreRecorded(u)
			row.Status, row.FinishedAt, row.Adopted = orkstackv1.Status_STATUS_SUCCEEDED, u.Row.GetFinishedAt(), true
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT:
			// It waits again, from when its needs are in: PENDING until then,
			// so no door takes an answer meant for the wait it had before. What
			// answers it is of this resume's scope.
			if s.awaits[u.Task] != nil {
				row.ResultEpoch = int64(b.GetSeqBase())
			}
		default:
			return fmt.Errorf("its block gives %s the disposition %v, which this build does not carry out", u.unit(), u.Disposition)
		}
		if s.eaches[u.Task] != nil {
			if err := s.applyFanOut(ctx, b, u, items); err != nil {
				return err
			}
		}
	}
	return nil
}

// restoreRecorded puts u's recorded row back, all but its end, which
// recordedEnd holds until it is settled: an ending before that keeps what the
// row says of it, so the archive lists its artifacts and the next resume
// records it again.
func (s *runState) restoreRecorded(u ResumeUnitPlan) {
	row, was := s.taskRuns[u.Task], u.Row
	row.ScheduledAt, row.StartedAt = was.GetScheduledAt(), was.GetStartedAt()
	row.ValuesRef, row.Artifacts = was.GetValuesRef(), was.GetArtifacts()
	switch {
	case row.GetAwait() != nil:
		row.Await.Deadline = was.GetAwait().GetDeadline()
		row.Await.ResolvedVia = was.GetAwait().GetResolvedVia()
		row.Await.ResolvedBy = was.GetAwait().GetResolvedBy()
		row.Await.ResolvedByVerified = was.GetAwait().GetResolvedByVerified()
	case row.GetChild() != nil:
		row.Child.RunId = was.GetChild().GetRunId()
		row.Child.InputArtifacts = was.GetChild().GetInputArtifacts()
	case row.GetSleep() != nil:
		row.Sleep.WakeAt = was.GetSleep().GetWakeAt()
	}
	if s.recordedEnd == nil {
		s.recordedEnd = map[string]*timestamppb.Timestamp{}
	}
	s.recordedEnd[u.Task] = was.GetFinishedAt()
}

// endOf is when row, a kept one, ended: a settled one when it did, and any
// other now.
func (s *runState) endOf(row *orkstackv1.TaskRun, now *timestamppb.Timestamp) *timestamppb.Timestamp {
	if at := s.recordedEnd[row.GetTaskId()]; at != nil && row.Item == nil {
		return at
	}
	return now
}

// readNoMemo has u, a unit a resume runs again, read no memo.
func (s *runState) readNoMemo(u ResumeUnit) {
	if s.fresh == nil {
		s.fresh = map[ResumeUnit]bool{}
	}
	s.fresh[u] = true
}

// readsNoMemo reports whether task id, or its item, reads no memo.
func (s *runState) readsNoMemo(id string, item int) bool {
	return s.fresh[ResumeUnit{Task: id, Item: item}] || s.fresh[nodeUnit(id)]
}

// runResumedOf is RUN_RESUMED's account of h (§5.4).
func (s *runState) runResumedOf(h *resumeHolder, units []ResumeUnitPlan) *orkstackv1.RunResumed {
	b, rec := h.block(), h.record
	var rerun, kept int32
	for i, u := range units {
		// A fan-out with items is counted by them.
		if u.Item < 0 && i+1 < len(units) && units[i+1].Item >= 0 {
			continue
		}
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			rerun++
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT, orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE:
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

// resumesProto is the newest entry of the run's resumes, the one this
// execution is of or carried on from, from RUN_RESUMED on; nil for every other
// run. Who reads it: a door that needs the scope, and whoever says which
// ending is a resume's. A reader adds the earlier entries (E85 PR6).
func (s *runState) resumesProto() []*orkstackv1.RunResume {
	if !s.resumed {
		return nil
	}
	return []*orkstackv1.RunResume{s.resumeEntry}
}

// entry is the resume as the run's snapshots list it.
func (h *resumeHolder) entry() *orkstackv1.RunResume {
	b, rec := h.block(), h.record
	from := orkstackv1.ResumeFromState_RESUME_FROM_STATE_FAILED
	if rec.GetSupersededReason() == forceCanceledReason {
		from = orkstackv1.ResumeFromState_RESUME_FROM_STATE_FORCE_CANCELED
	}
	return &orkstackv1.RunResume{
		At: rec.GetAt(), Action: b.GetAction(), FromTaskId: b.GetFromTaskId(),
		Principal: rec.GetPrincipal(), PrincipalVerified: rec.GetPrincipalVerified(),
		Reason: rec.GetReason(), SupersededReason: rec.GetSupersededReason(),
		ResumedAfterSequence: rec.GetBaseTerminalSequence(), Sequence: int64(b.GetSeqBase()),
		FromState: from, Scope: b.GetScope(),
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
func (s *runState) readBack(ctx workflow.Context, b *orkstackv1.ResumeBlock, units []ResumeUnitPlan, started time.Time) (kept, settled int, err error) {
	var entries []*orkstackv1.AdoptEntry
	var sizes []int64
	for _, u := range units {
		id := u.Task
		if !readBackUnit(s.tasks[id], u) {
			continue
		}
		if u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE {
			entries = append(entries, settledEntryOf(s.wf, s.runID, s.tasks[id], u.Row.GetValuesRef()))
			sizes = append(sizes, u.EncodedSize)
			continue
		}
		// The entry the resume was verified with (ResumeVerifier).
		entry, err := adoptEntryOf(s.wf, s.plan.CommitRoot, s.runID, s.tasks[id], int32(u.Item), int64(u.Epoch))
		if err != nil {
			return 0, 0, temporal.NewNonRetryableApplicationError(err.Error(), "INVALID_COMMIT_KEY", err)
		}
		if s.plan.Identity != nil {
			// What the engine recorded, and nothing an Orker committed.
			if entry.CommitUri, err = artifacts.AttestedCommitURI(entry.GetCommitUri()); err != nil {
				return 0, 0, s.readBackFailed(ctx, adoptedUnit(entry), err)
			}
		}
		entries = append(entries, entry)
		sizes = append(sizes, u.EncodedSize)
	}
	if len(entries) == 0 {
		return 0, 0, nil
	}
	tun, err := readbackTunablesOf(b.GetReadback())
	if err != nil {
		return 0, 0, s.readBackFailed(ctx, adoptedUnit(entries[0]), err)
	}
	calls, err := packReadback(sizes, tun)
	if err != nil {
		return 0, 0, s.readBackFailed(ctx, adoptedUnit(entries[0]), err)
	}
	deadline := started.Add(tun.Deadline)
	wft := workflow.GetInfo(ctx).WorkflowTaskTimeout
	for at, call := range calls {
		if at > 0 && at%tun.CallsPerTask == 0 {
			// A new workflow task for the next calls, so one task's
			// completion carries at most CallsPerTask results.
			if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
				if ctx.Err() != nil {
					return kept, settled, ctx.Err()
				}
				return kept, settled, s.readBackFailed(ctx, adoptedUnit(entries[call[0]]), err)
			}
		}
		first := adoptedUnit(entries[call[0]])
		timeouts, ok := readbackCallTimeouts(tun, wft, workflow.Now(ctx), deadline)
		if !ok {
			return kept, settled, s.readBackFailed(ctx, first, fmt.Errorf("the read-back's deadline passed"))
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
		if err != nil && ctx.Err() != nil {
			// The operator canceled the run: no result failed to be read.
			return kept, settled, ctx.Err()
		}
		if err != nil {
			return kept, settled, s.readBackFailed(ctx, first, err)
		}
		if got, sent := len(result.GetEntries()), call[1]-call[0]; got != sent {
			return kept, settled, s.readBackFailed(ctx, first, fmt.Errorf("the read-back answered %d of %d entries", got, sent))
		}
		for _, e := range result.GetEntries() {
			if err := s.applyReadBack(e); err != nil {
				return kept, settled, s.readBackFailed(ctx, ResumeUnit{Task: e.GetTaskId(), Item: int(e.GetItem())}, err)
			}
			if s.awaits[e.GetTaskId()] != nil || s.children[e.GetTaskId()] != nil {
				settled++
			} else {
				kept++
			}
		}
	}
	s.settleKeptFanOuts(ctx)
	return kept, settled, nil
}

func adoptedUnit(e *orkstackv1.AdoptEntry) ResumeUnit {
	return ResumeUnit{Task: e.GetTaskId(), Item: int(e.GetItem())}
}

// resumedRow is the row of u, a task's or an item's, nil when there is none.
func (s *runState) resumedRow(u ResumeUnit) *orkstackv1.TaskRun {
	row := s.taskRuns[u.Task]
	if row == nil || u.Item < 0 {
		return row
	}
	if u.Item >= len(row.GetItems()) {
		return nil
	}
	return row.GetItems()[u.Item]
}

// applyReadBack is one kept row's result, applied as collect applies an
// adopted attempt's, silently: the row reads SUCCEEDED and adopted, with
// its committing attempt's facts.
func (s *runState) applyReadBack(e *orkstackv1.AdoptedEntry) error {
	u, result := ResumeUnit{Task: e.GetTaskId(), Item: int(e.GetItem())}, e.GetResult()
	row := s.resumedRow(u)
	if row == nil || !row.GetReadingBack() || u.Item < 0 && s.eaches[u.Task] != nil {
		return fmt.Errorf("the read-back answered %s, which it was not reading back", u)
	}
	if s.awaits[u.Task] != nil || s.children[u.Task] != nil {
		return s.applySettled(u.Task, row, result)
	}
	// Nothing reads an item's values in workflow code (docs/DSL.md rule 24).
	if u.Item < 0 {
		values := decodeValues(result.GetValuesJson())
		output, err := payloadOf(values)
		if err != nil {
			return temporal.NewNonRetryableApplicationError(fmt.Sprintf("task %q's kept values: %v", u.Task, err), taskOutputInvalidCode, err)
		}
		delete(s.listed, u.Task)
		row.Output = output
		s.results[u.Task] = result
		s.outputs[u.Task] = values
	}
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
	row.Adopted = true
	row.ReadingBack = false
	return nil
}

// applySettled is an awaited or composed task's result, applied silently as
// its own resolution applied it (resolve, collectChild): its row reads
// SUCCEEDED with its answer, or its child, and the times it had.
func (s *runState) applySettled(id string, row *orkstackv1.TaskRun, result *orkstackv1.ExecuteTaskResult) error {
	ref, child := row.GetValuesRef(), s.children[id] != nil
	if child && !result.GetValuesInlined() {
		// The run held a composed task's values whole.
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: its child's values were not read back", id), taskOutputInvalidCode, nil)
	}
	values := decodeValues(result.GetValuesJson())
	output, err := payloadOf(values)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("task %q's kept values: %v", id, err), taskOutputInvalidCode, err)
	}
	delete(s.listed, id)
	stored := &orkstackv1.ExecuteTaskResult{TaskId: id, ValuesRef: ref, Artifacts: row.GetArtifacts()}
	if !child || len(result.GetValuesJson()) <= composeInlineValuesLimit {
		stored.ValuesJson, stored.ValuesInlined = result.GetValuesJson(), result.GetValuesInlined()
	}
	s.results[id], s.outputs[id] = stored, values
	row.Error = nil
	row.Status = orkstackv1.Status_STATUS_SUCCEEDED
	row.Output = output
	row.FinishedAt = s.endOf(row, row.GetFinishedAt())
	row.Adopted, row.ReadingBack = true, false
	return nil
}

// readBackFailed fails the unit the read-back failed on, or the first one not
// read back when the failure names none (§4.3): its reason is
// the read-back's, and the run fails as any task failure fails it, with its
// unread rows written CANCELED and RESUME_NOT_READ_BACK (§4.4).
func (s *runState) readBackFailed(ctx workflow.Context, u ResumeUnit, cause error) error {
	if failed := (adoptFailedUnit{}); categoryOf(cause).appDetails(&failed) && failed.Task != "" {
		u = ResumeUnit{Task: failed.Task, Item: int(failed.Item)}
	}
	row := s.resumedRow(u)
	if row == nil || !row.GetReadingBack() {
		// An answer for a row it was not reading back: the first row still
		// reading back takes the failure.
		cause = fmt.Errorf("an answer for %s, which it was not reading back: %w", u, cause)
		if u, row = s.firstReadingBack(); row == nil {
			return &readBackFailure{cause: cause}
		}
	}
	reading := categoryOf(cause)
	code := reading.appType()
	if code == "" || !reading.isApplication() {
		code = ReasonCommitReadFailed
	}
	fail := func(row *orkstackv1.TaskRun, message string) {
		row.ReadingBack = false
		row.Status = orkstackv1.Status_STATUS_FAILED
		row.FinishedAt = s.endOf(row, timestamppb.New(workflow.Now(ctx)))
		row.Error = &orkstackv1.Error{Code: code, Message: message, Category: readBackCategory(code)}
	}
	message := readBackSentence(cause)
	fail(row, message)
	if u.Item >= 0 {
		// An item fails its fan-out, which is then not read back either.
		fail(s.taskRuns[u.Task], fmt.Sprintf("item %d of %d: its kept result could not be read back: %s",
			u.Item, len(s.taskRuns[u.Task].GetItems()), message))
	}
	return &readBackFailure{task: u.Task, cause: cause}
}

// firstReadingBack is the first unit, in unit order, still being read back.
func (s *runState) firstReadingBack() (ResumeUnit, *orkstackv1.TaskRun) {
	for _, id := range s.order {
		row := s.taskRuns[id]
		for i, item := range row.GetItems() {
			if item.GetReadingBack() {
				return ResumeUnit{Task: id, Item: i}, item
			}
		}
		if row.GetReadingBack() {
			return nodeUnit(id), row
		}
	}
	return ResumeUnit{}, nil
}

// readBackSentence is a read-back's failure as a person reads it.
func readBackSentence(cause error) string {
	return resumeSentence(cause, "the saved results could not be read before the read-back's deadline")
}

// resumeSentence is cause as a person reads it: its own sentence, never the
// wording of what carried it, and timedOut when it only ran out of time.
func resumeSentence(cause error, timedOut string) string {
	switch reading := categoryOf(cause); {
	case reading.isApplication():
		return reading.appMessage()
	case reading.timeout != nil:
		return timedOut
	}
	return cause.Error()
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
	end := func(row *orkstackv1.TaskRun) {
		if !row.GetReadingBack() {
			return
		}
		row.Status = orkstackv1.Status_STATUS_CANCELED
		row.FinishedAt = s.endOf(row, now)
		row.Error = &orkstackv1.Error{Code: ReasonResumeNotReadBack, Message: "the run ended before this kept result was read back; the next resume keeps it",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
	}
	for _, id := range s.order {
		end(s.taskRuns[id])
		for _, item := range s.taskRuns[id].GetItems() {
			end(item)
		}
	}
}
