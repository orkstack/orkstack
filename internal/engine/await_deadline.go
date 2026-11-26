package engine

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What a run whose results the engine records does at an await's deadline: it
// reads the record before it says that nobody answered, and once more, some
// minutes later, when the first read was not answered.

// ResolutionRecordUnavailableCode is an await whose deadline passed and whose
// record could not be read (InfrastructureError).
const ResolutionRecordUnavailableCode = "RESOLUTION_RECORD_UNAVAILABLE"

// awaitCanceledAtHistoryLimit is a failure handler's await whose deadline
// passed with no history left to read its record.
const awaitCanceledAtHistoryLimit = "the run reached its history limit before this task's deadline could be read; its record, if one was stored, was not taken"

// confirmingSet is the awaits whose deadlines passed and whose records were
// not read, and the one timer after which each is read once more.
type confirmingSet struct {
	timer  workflow.Future
	cancel workflow.CancelFunc
	again  bool // armed a second time
}

// expireOutcome is what became of an await at its deadline.
type expireOutcome int

const (
	expireAdopted expireOutcome = iota
	expireConfirming
	expireDeferred // an ending that ends the run is ready
	expireFailed
	expireCanceled
)

// expireRecorded takes await id's settled deadline. The error is the run's for
// failed and canceled, and for an adoption whose events could not be written.
func (s *runState) expireRecorded(ctx, readCtx workflow.Context, inFlight map[string]workflow.Future, id string) (expireOutcome, error) {
	a := s.awaits[id]
	if err := a.deadline.Get(readCtx, nil); err != nil {
		a.open = false
		return expireCanceled, err
	}
	if readCtx.Err() != nil || s.handlersStopped() {
		return expireCanceled, temporal.NewCanceledError()
	}
	if s.endingReady(readCtx, inFlight) {
		a.deferred = true // until the ending is taken, or an unpause wins over it
		return expireDeferred, nil
	}
	if err := s.historyExhausted(ctx, inFlight); err != nil {
		return expireFailed, err
	}
	if s.reads.spent || (len(s.confirmingMembers()) > 0 && s.reads.unanswered) {
		s.confirm(ctx, inFlight, id, "joined")
		return expireConfirming, nil
	}
	read := s.readResolution(readCtx, id)
	if read.canceled {
		return expireCanceled, read.err
	}
	if read.unanswered == "" && read.Outcome != ResolutionUnsupported {
		return s.settleDeadline(ctx, id, read)
	}
	if read.unanswered != "" {
		s.notAnswered(id)
		s.logUnanswered(ctx, id, read.unanswered, read.err)
	} else {
		s.answered()
		s.logUnsupported(ctx, id, read.ResolutionRead)
	}
	s.confirm(ctx, inFlight, id, "")
	return expireConfirming, nil
}

// settleDeadline ends await id, past its deadline, by what its record says.
func (s *runState) settleDeadline(ctx workflow.Context, id string, read resolutionAnswer) (expireOutcome, error) {
	s.answered()
	switch read.Outcome {
	case ResolutionFound:
		succeeded, err := s.applyResolution(ctx, id, read.applied())
		if succeeded {
			return expireAdopted, err
		}
		return expireFailed, err
	case ResolutionInvalid:
		return expireFailed, s.recordInvalid(ctx, id, read.ResolutionRead)
	case ResolutionNone, ResolutionLate:
		s.awaits[id].open = false
		return expireFailed, s.timeOutAwait(ctx, id)
	}
	return expireFailed, s.recordUnavailable(ctx, id, classInternal, true)
}

// confirm puts await id in the run's confirming set, whose first member arms
// its one timer. A run holding new work for a switch gives the hold up: the
// set holds the switch instead.
func (s *runState) confirm(ctx workflow.Context, inFlight map[string]workflow.Future, id, how string) {
	if len(s.confirmingMembers()) == 0 {
		// None, or one a run that began to fail left behind.
		s.closeConfirming()
		s.reads.confirming = &confirmingSet{}
		s.armConfirming(id)
	}
	s.awaits[id].confirming = true
	if how != "" {
		s.countRecords(ctx, "ork.run.deadline_rereads", map[string]string{"outcome": how})
	}
	if s.quiescing {
		s.abandonQuiesce(ctx, inFlight, "an await's record is read once more before the run carries on")
	}
}

// confirmingMembers are the open awaits of the confirming set, in the order of
// their deadlines.
func (s *runState) confirmingMembers() []string {
	var members []string
	for _, id := range s.order {
		if a := s.awaits[id]; a != nil && a.open && a.confirming {
			members = append(members, id)
		}
	}
	slices.SortStableFunc(members, func(x, y string) int { return s.awaits[x].deadlineAt.Compare(s.awaits[y].deadlineAt) })
	return members
}

// closeConfirming ends the confirming set, and its timer with it.
func (s *runState) closeConfirming() {
	if set := s.reads.confirming; set != nil {
		set.cancel()
	}
	s.reads.confirming = nil
}

func (s *runState) armConfirming(id string) {
	set := s.reads.confirming
	wait := time.Duration(s.plan.Identity.RecordBudget.DeadlineRereadSeconds) * time.Second
	timerCtx, cancel := workflow.WithCancel(s.dispatchContext(s.runCtx, id))
	set.timer, set.cancel = workflow.NewTimer(timerCtx, wait), cancel
}

// addDeadlines registers every open await's deadline in declaration order, and
// the confirming set's timer once, where its first member stands.
func (s *runState) addDeadlines(selector workflow.Selector, expired func(id string), confirmed func()) {
	setAdded := false
	for _, id := range s.order {
		a := s.awaits[id]
		if a == nil || !a.open || a.deferred {
			continue
		}
		if a.confirming {
			if !setAdded {
				setAdded = true
				selector.AddFuture(s.reads.confirming.timer, func(workflow.Future) { confirmed() })
			}
			continue
		}
		awaitID := id
		selector.AddFuture(a.deadline, func(workflow.Future) { expired(awaitID) })
	}
}

// undeferAwaits registers again the deadlines an ending was to take: an unpause
// can win over a pause's own expiry, and a failure handler's await whose ending
// was taken ends with it. A ready hard end is taken by its own timer, which the
// failing phase registers after the deadlines: they wait for it.
func (s *runState) undeferAwaits() {
	if !s.recorded() || (s.failingPhase && s.hardEndReady()) {
		return
	}
	for _, id := range s.order {
		if a := s.awaits[id]; a != nil {
			a.deferred = false
		}
	}
}

// settled is one member's end at the set's timer.
type settled struct {
	id      string
	outcome expireOutcome
	err     error
}

// confirmAll reads each member of the confirming set once more, in the order
// of their deadlines, each ended by what its read answers: the first that is
// not answered ends it and every member after it unread. A busy replica ends
// none of them once: the timer is armed again. stop says, of one ended, that
// the rest are the run's end's.
func (s *runState) confirmAll(ctx, readCtx workflow.Context, inFlight map[string]workflow.Future, stop func(settled) bool) {
	set := s.reads.confirming
	canceled := set.timer.Get(readCtx, nil)
	members := s.confirmingMembers()
	lost := ""
	for _, id := range members {
		a := s.awaits[id]
		end := settled{id: id}
		switch {
		case canceled != nil || readCtx.Err() != nil || s.handlersStopped(): // the timer's cancel is the run's
			end.outcome, end.err = expireCanceled, temporal.NewCanceledError()
		case lost != "":
			end.outcome, end.err = expireFailed, s.recordUnavailable(ctx, id, lost, false)
		case s.endingReady(readCtx, inFlight):
			a.deferred, end.outcome = true, expireDeferred
		default:
			if end.err = s.historyExhausted(ctx, inFlight); end.err != nil {
				end.outcome = expireFailed
				break
			}
			read := s.readResolution(readCtx, id)
			switch {
			case read.canceled:
				end.outcome, end.err = expireCanceled, read.err
			case read.unanswered == artifacts.UnansweredBusy && !set.again:
				s.notAnswered(id)
				s.logUnanswered(ctx, id, read.unanswered, read.err)
				set.again = true
				s.armConfirming(id)
				return
			case read.unanswered != "":
				s.notAnswered(id)
				workflow.GetLogger(ctx).Error("an await's record was not read at its deadline, twice",
					"run_id", s.runID, "task_id", id, "class", read.unanswered, "error", read.err)
				s.countRecords(ctx, "ork.run.record_reads_unanswered", map[string]string{"kind": "resolution", "reason": read.unanswered})
				lost = read.unanswered
				end.outcome, end.err = expireFailed, s.recordUnavailable(ctx, id, lost, true)
			case read.Outcome == ResolutionUnsupported:
				s.answered()
				s.logUnsupported(ctx, id, read.ResolutionRead)
				end.outcome, end.err = expireFailed, s.recordUnavailable(ctx, id, "unsupported", true)
			default:
				end.outcome, end.err = s.settleDeadline(ctx, id, read)
			}
		}
		a.confirming = false
		s.countRecords(ctx, "ork.run.deadline_rereads", map[string]string{"outcome": end.outcome.word(s.taskRuns[id])})
		if stop(end) {
			break
		}
	}
	for _, id := range members {
		s.awaits[id].confirming = false
	}
	s.closeConfirming()
}

// word is an outcome as it is counted.
func (o expireOutcome) word(tr *orkstackv1.TaskRun) string {
	switch {
	case o == expireAdopted:
		return "adopted"
	case o == expireCanceled:
		return "canceled"
	case o == expireDeferred:
		return "deferred"
	case tr.GetError().GetCode() == AwaitTimedOutCode:
		return "timed_out"
	case tr.GetError().GetCode() == ResolutionRecordInvalidCode:
		return "invalid"
	}
	return "unanswered"
}

// recordUnavailable ends await id, whose record was not read by its deadline:
// asked says its own read was the one that was not answered.
func (s *runState) recordUnavailable(ctx workflow.Context, id, class string, asked bool) error {
	a := s.awaits[id]
	message := fmt.Sprintf("await %q reached its deadline %s and its record could not be read: a decision may be on record; see this run's records",
		id, formatInstant(a.deadlineAt))
	switch {
	case class == "unsupported":
		message = fmt.Sprintf("await %q reached its deadline %s and its record is in a newer format than this engine reads; see this run's records",
			id, formatInstant(a.deadlineAt))
	case !asked:
		message = fmt.Sprintf("await %q reached its deadline %s, and the store did not answer the run's read of its records then: a decision may be on record; see this run's records",
			id, formatInstant(a.deadlineAt))
	}
	return s.failAwait(ctx, id, ResolutionRecordUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		message, fmt.Sprintf("task %q; class=%s", id, class))
}

func (s *runState) logUnsupported(ctx workflow.Context, id string, read ResolutionRead) {
	workflow.GetLogger(ctx).Error("an await's record is in a newer format than this engine reads",
		"run_id", s.runID, "task_id", id, "object", read.Object, "version", read.Version)
	s.countRecords(ctx, "ork.run.record_reads_unanswered", map[string]string{"kind": "resolution", "reason": "unsupported"})
}

// timeOutAwait fails await id, which nobody resolved by its deadline.
func (s *runState) timeOutAwait(ctx workflow.Context, id string) error {
	a, taskRun := s.awaits[id], s.taskRuns[id]
	message := fmt.Sprintf("await %q was not resolved within %s (deadline %s)",
		id, a.timeout, a.deadlineAt.UTC().Format(time.RFC3339))
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:      AwaitTimedOutCode,
		Message:   message,
		Details:   fmt.Sprintf("task %q", id),
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
		Retryable: false,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, AwaitTimedOutCode, nil)
}

// handlerExpired records what became of a failure handler's await at its
// deadline: none of it stops anything else.
func (s *runState) handlerExpired(ctx workflow.Context, end settled) {
	a, tr := s.awaits[end.id], s.taskRuns[end.id]
	switch {
	case end.err == nil || tr.GetStatus() != orkstackv1.Status_STATUS_AWAITING:
		if end.outcome == expireAdopted && end.err != nil {
			workflow.GetLogger(ctx).Error("emit of an await's resolution failed", "run_id", s.runID, "task_id", end.id, "error", end.err)
		}
		return
	case end.outcome == expireCanceled:
		if err := s.cancelAwaiting(ctx, end.id); err != nil {
			workflow.GetLogger(ctx).Error("mark canceled failed", "run_id", s.runID, "task_id", end.id, "error", err)
		}
		return
	}
	// The history guard: this await ends, the handlers not yet started with
	// it, and those running finish.
	a.open = false
	a.cancelDeadline()
	if err := s.markCanceled(ctx, end.id, awaitCanceledAtHistoryLimit); err != nil {
		workflow.GetLogger(ctx).Error("mark canceled failed", "run_id", s.runID, "task_id", end.id, "error", err)
	}
	s.cancelHandlersAtHistoryLimit(ctx)
}
