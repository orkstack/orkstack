package engine

import (
	"fmt"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// How a run whose results the engine records takes a resolution
// (docs/TASK_IDENTITY.md, "Resolutions the engine records"): a message only
// names an await, and what the await's record says is what is applied. An
// await is never failed, before its deadline, for a read that was not
// answered.

// ResolutionRecordInvalidCode is an await whose record is there and is not
// its own (InfrastructureError).
const ResolutionRecordInvalidCode = "RESOLUTION_RECORD_INVALID"

const (
	resolutionNotRecorded = "RESOLUTION_NOT_RECORDED: a resolution reached this run and no record of it was found; the task is still awaiting"
	resolutionLate        = "a resolution reached this run whose record was stored after the deadline; the deadline decides"
)

// recordReads is the run loop's working memory of its own reads: a function
// of history, in no snapshot, and fresh in each generation.
type recordReads struct {
	// received counts the resolutions taken off the channel, and staleUpTo is,
	// by await, how far the queue reached when a read of it went unanswered.
	received  int
	staleUpTo map[string]int
	// nones counts, by await, the tellings whose read gave nothing to apply.
	nones map[string]int32
	// confirming is the run's one set of awaits past their deadlines whose
	// record was not read.
	confirming *confirmingSet
	// unanswered says the run's last read or write of a record was not
	// answered, and spent that this turn has had its one such read.
	unanswered, spent bool
}

// resolveOutcome is what became of one telling.
type resolveOutcome int

const (
	resolveDropped resolveOutcome = iota // unread
	resolveIgnored                       // an event says why
	resolveApplied
	resolveOpen // no record; the await stays open
	resolveUnanswered
	resolveFailed
	resolveCanceled
)

// changedState says an outcome changed what a switch would carry.
func (o resolveOutcome) changedState() bool {
	return o == resolveIgnored || o == resolveApplied || o == resolveOpen || o == resolveFailed
}

// dropResolution drops a telling unread: no command, no event.
func (s *runState) dropResolution(ctx workflow.Context, id, why string) resolveOutcome {
	workflow.GetLogger(ctx).Info("a resolution was not read", "run_id", s.runID, "task_id", id, "why", why)
	s.countRecords(ctx, "ork.run.resolutions_coalesced", map[string]string{"why": why})
	return resolveDropped
}

// countRecords counts through the replay-aware handler of workflow code.
func (s *runState) countRecords(ctx workflow.Context, name string, tags map[string]string) {
	with := map[string]string{"ork_namespace": s.wf.GetNamespace()}
	for k, v := range tags {
		with[k] = v
	}
	workflow.GetMetricsHandler(ctx).WithTags(with).Counter(name).Inc(1)
}

// receivedResolution counts a resolution taken off the channel.
func (s *runState) receivedResolution() { s.reads.received++ }

// answered notes that a read or a write of the run's records was answered.
func (s *runState) answered() { s.reads.unanswered = false }

// notAnswered notes one that was not: what the queue holds now, and names id,
// is of before it.
func (s *runState) notAnswered(id string) {
	s.reads.unanswered, s.reads.spent = true, true
	if s.reads.staleUpTo == nil {
		s.reads.staleUpTo = map[string]int{}
	}
	if s.resolutions != nil {
		s.reads.staleUpTo[id] = s.reads.received + s.resolutions.Len()
	}
}

// endingReady says an ending that ends the run is ready: the next pass takes
// it, and nothing is read before it.
func (s *runState) endingReady(ctx workflow.Context, inFlight map[string]workflow.Future) bool {
	if ctx.Err() != nil {
		return true
	}
	if s.failingPhase {
		return s.handlersStopped() || s.hardEndReady() || (s.pauseActsOnHandlers() && s.pauseEndReady())
	}
	due, canceled := s.runTimeoutDue(ctx, inFlight)
	return due || canceled || s.pauseEndReady()
}

func (s *runState) pauseEndReady() bool {
	return (s.pause != nil && s.pause.timer.IsReady()) || (s.pausedByParent != nil && s.pausedByParent.backstop.IsReady())
}

// hardEndReady says the failing phase's hard end is due and not yet taken.
func (s *runState) hardEndReady() bool {
	t := s.runTimeout
	return t != nil && t.hardTimer != nil && t.hardTimer.IsReady() && !t.hardTaken
}

// handlersStopped says the run stopped its failure handlers: an await still
// open then ends with them, and nothing is read for it.
func (s *runState) handlersStopped() bool {
	_, stopped := s.handlersStoppedReason()
	return stopped && s.failingPhase
}

// resolveRecorded takes one telling: it reads the await's record and applies
// what that says. ctx is for events and readCtx for the read; seen are the
// awaits this drain has read. The error is the run's: the task's failure, a
// cancellation, or an applied resolution whose event could not be written.
func (s *runState) resolveRecorded(ctx, readCtx workflow.Context, inFlight map[string]workflow.Future, sig *orkstackv1.AwaitSignal, seen map[string]bool) (resolveOutcome, error) {
	id, position := sig.GetTaskId(), s.reads.received
	a := s.awaits[id]
	if a == nil {
		workflow.GetLogger(ctx).Warn("resolution for a task that is not an await of this run",
			"run_id", s.runID, "task_id", id)
		return resolveDropped, nil
	}
	if s.taskRuns[id].GetStatus() != orkstackv1.Status_STATUS_AWAITING {
		_ = s.ignoreResolution(ctx, sig, s.notAwaiting(id))
		return resolveIgnored, nil
	}
	switch {
	case seen[id]:
		return s.dropResolution(ctx, id, "duplicate"), nil
	case position <= s.reads.staleUpTo[id]:
		return s.dropResolution(ctx, id, "stale"), nil
	case a.confirming:
		return s.dropResolution(ctx, id, "confirming"), nil
	case s.reads.nones[id] >= s.plan.Identity.NonesPerAwait:
		return s.dropResolution(ctx, id, "nones"), nil
	case s.endingReady(readCtx, inFlight):
		return s.dropResolution(ctx, id, "ending"), nil
	case s.historyExhaustedBefore(ctx, inFlight, s.recordReadBytes()) != nil:
		return s.dropResolution(ctx, id, "history"), nil
	}
	if seen != nil {
		seen[id] = true
	}
	return s.takeRead(ctx, id, s.readResolution(readCtx, id))
}

// takeRead applies what a telling's read answered.
func (s *runState) takeRead(ctx workflow.Context, id string, read resolutionAnswer) (resolveOutcome, error) {
	switch {
	case read.canceled:
		return resolveCanceled, read.err
	case read.unanswered != "":
		s.notAnswered(id)
		s.logUnanswered(ctx, id, read.unanswered, read.err)
		return resolveUnanswered, nil
	}
	s.answered()
	switch read.Outcome {
	case ResolutionFound:
		succeeded, err := s.applyResolution(ctx, id, read.applied())
		if succeeded {
			return resolveApplied, err
		}
		return resolveFailed, err
	case ResolutionInvalid:
		return resolveFailed, s.recordInvalid(ctx, id, read.ResolutionRead)
	case ResolutionUnsupported:
		s.logUnsupported(ctx, id, read.ResolutionRead)
		s.foundNothing(id)
		return resolveUnanswered, nil
	}
	message := resolutionNotRecorded
	outcome := resolveOpen
	if read.Outcome == ResolutionLate {
		message, outcome = resolutionLate, resolveIgnored
	} else {
		s.countRecords(ctx, "ork.run.resolutions_unrecorded", nil)
	}
	s.foundNothing(id)
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id, orkstackv1.Status_STATUS_AWAITING, message); err != nil {
		workflow.GetLogger(ctx).Error("emit of a resolution not taken failed", "run_id", s.runID, "task_id", id, "error", err)
	}
	return outcome, nil
}

// foundNothing counts a telling of await id whose read gave nothing to apply.
func (s *runState) foundNothing(id string) {
	if s.reads.nones == nil {
		s.reads.nones = map[string]int32{}
	}
	s.reads.nones[id]++
}

// applied is a resolution read from its record.
func (r resolutionAnswer) applied() appliedResolution {
	out := appliedResolution{reject: r.Rejected, reason: r.Reason, valuesJSON: r.ValuesJSON, via: r.via(), principal: r.Principal, verified: r.PrincipalVerified,
		record: &orkstackv1.RecordRef{Sha256: r.RecordSHA256, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION}}
	if r.Values != nil {
		out.values = r.Values.Proto()
	}
	return out
}

func (s *runState) logUnanswered(ctx workflow.Context, id, class string, err error) {
	workflow.GetLogger(ctx).Warn("an await's record was not read; the await stays open until its deadline",
		"run_id", s.runID, "task_id", id, "kind", "resolution", "deadline", formatInstant(s.awaits[id].deadlineAt), "class", class, "error", err)
	s.countRecords(ctx, "ork.run.record_reads_unanswered", map[string]string{"kind": "resolution", "reason": class})
}

// recordInvalid fails await id, whose record is there and is not its own.
func (s *runState) recordInvalid(ctx workflow.Context, id string, read ResolutionRead) error {
	return s.failAwait(ctx, id, ResolutionRecordInvalidCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		fmt.Sprintf("the record of await %q cannot be used (%s: %s)", id, read.Object, read.Why), "")
}

// failAwait closes await id and fails its task: the error the run fails with.
func (s *runState) failAwait(ctx workflow.Context, id, code string, category orkstackv1.FailureCategory, message, details string) error {
	a, taskRun := s.awaits[id], s.taskRuns[id]
	a.open = false
	a.cancelDeadline()
	if details == "" {
		details = fmt.Sprintf("task %q", id)
	}
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{Code: code, Message: message, Details: details, Category: category}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id, orkstackv1.Status_STATUS_FAILED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed", "run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, code, nil)
}

// takeResolution takes one resolution off the channel without waiting.
func (s *runState) takeResolution(ctx workflow.Context) (sig *orkstackv1.AwaitSignal, received, ok bool) {
	var raw converter.RawValue
	if !s.resolutions.ReceiveAsync(&raw) {
		return nil, false, false
	}
	s.receivedResolution()
	sig, ok = s.openResolution(ctx, raw)
	return sig, true, ok
}

// drainRecorded takes the resolutions queued when it starts, at most the
// plan's number of them, and stops at an ending that is ready or at a read
// that is not answered. changed says one changed the run's state.
func (s *runState) drainRecorded(ctx workflow.Context, inFlight map[string]workflow.Future) (changed bool, taskID string, err error) {
	seen := map[string]bool{}
	for range min(s.resolutions.Len(), int(s.plan.Identity.ResolutionsPerPass)) {
		if s.endingReady(ctx, inFlight) {
			return changed, "", nil
		}
		sig, received, ok := s.takeResolution(ctx)
		if !received {
			return changed, "", nil
		}
		if !ok {
			changed = true // its event is the run's
			continue
		}
		outcome, err := s.resolveRecorded(ctx, ctx, inFlight, sig, seen)
		changed = changed || outcome.changedState()
		if err != nil {
			return true, sig.GetTaskId(), err
		}
		if outcome == resolveUnanswered && s.reads.unanswered {
			return changed, "", nil
		}
	}
	return changed, "", nil
}

// drainRecordedAtPass is the top of a pass's drain, in a run with a timeout:
// nothing while the run's reads go unanswered.
func (s *runState) drainRecordedAtPass(ctx workflow.Context, inFlight map[string]workflow.Future) (taskID string, err error) {
	if s.resolutions == nil || s.reads.unanswered {
		return "", nil
	}
	_, taskID, err = s.drainRecorded(ctx, inFlight)
	return taskID, err
}

// switchRecorded is a switch's check of the queue: what it took changed the
// run's state, or some of it is left. While the run's reads go unanswered what
// is queued is dropped, which changes nothing: each await crosses open, its
// record standing.
func (s *runState) switchRecorded(ctx workflow.Context, inFlight map[string]workflow.Future) (changed bool, taskID string, err error) {
	if s.resolutions == nil {
		return false, "", nil
	}
	if !s.reads.unanswered {
		if changed, taskID, err = s.drainRecorded(ctx, inFlight); err != nil {
			return true, taskID, err
		}
	}
	if s.reads.unanswered {
		for s.resolutions.Len() > 0 {
			sig, received, ok := s.takeResolution(ctx)
			if !received {
				break
			}
			if !ok {
				changed = true
				continue
			}
			s.dropResolution(ctx, sig.GetTaskId(), "stale")
		}
	}
	return changed || s.resolutions.Len() > 0, "", nil
}

// drainRecordedResolutions is the ending run's drain: one event an await, at
// most the plan's number of them and none after one that could not be
// written, and one line for the rest.
func (s *runState) drainRecordedResolutions(ctx workflow.Context) {
	said, left, failed := map[string]bool{}, 0, false
	for queued := s.resolutions.Len(); queued > 0; queued-- {
		sig, received, ok := s.takeResolution(ctx)
		if !received {
			break
		}
		if !ok {
			continue
		}
		id := sig.GetTaskId()
		switch {
		case s.awaits[id] == nil:
			workflow.GetLogger(ctx).Warn("resolution for a task that is not an await of this run", "run_id", s.runID, "task_id", id)
		case said[id]:
			s.dropResolution(ctx, id, "ending")
		case failed || len(said) >= int(s.plan.Identity.ResolutionsPerPass):
			left++
		default:
			said[id] = true
			failed = s.ignoreResolution(ctx, sig, s.notAwaiting(id)) != nil
		}
	}
	if left == 0 {
		return
	}
	message := fmt.Sprintf("%d resolutions queued at the end were not recorded individually; their records stand", left)
	workflow.GetLogger(ctx).Warn(message, "run_id", s.runID)
	s.countRecords(ctx, "ork.run.resolutions_coalesced", map[string]string{"why": "ending"})
	if failed {
		return
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, "", s.status, message); err != nil {
		workflow.GetLogger(ctx).Error("emit of the resolutions left queued failed", "run_id", s.runID, "error", err)
	}
}

// readEvents is what one read or write of a record that is not answered can
// add to the run's history; 0 for a run that makes none.
func (s *runState) readEvents() int64 {
	if !s.recorded() || s.plan.Identity.RecordBudget == nil {
		return 0
	}
	return s.plan.Identity.RecordBudget.ReadEvents
}

// recordReadBytes is readEvents as the history guard charges bytes.
func (s *runState) recordReadBytes() int64 {
	if limit := s.historyLimit(); limit != nil {
		return s.readEvents() * limit.Reserve.BytesPerEvent
	}
	return 0
}

// confirmingTimerEvents is what the confirming set's two timers add: each one
// started and fired, and a workflow task for each firing. The set's, charged
// to every await of it.
const confirmingTimerEvents = 8

// awaitReserve is what an open await's resolution, or its deadline's reads and
// the confirming set's two timers, can still add to the run's history.
func (s *runState) awaitReserve(r HistoryReserve) int64 {
	if n := s.readEvents(); n > 0 {
		return r.PerAwait + 2*n + confirmingTimerEvents
	}
	return r.PerAwait
}
