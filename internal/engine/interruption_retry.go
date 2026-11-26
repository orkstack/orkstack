package engine

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The interpreter's half of interruptions (E92, design §4.7–4.10): which
// infrastructure facts a unit's next execution lists as its own, what the
// run does when one ends an execution, and the waits between. The
// arithmetic is interruption.go's and pure; this file holds the commands.

// InterruptionRetryChange is the workflow.GetVersion change id of the
// interruption path. Asked lazily, where a run first schedules a unit whose
// budgets are open (unitBudgetsFor, from retryPolicyFor), and only by a run
// whose plan carries budgets: a history recorded before the gate, or by a
// build that ignored the plan field, replays its DefaultVersion branch,
// which lists nothing and so schedules every unit exactly as before.
const InterruptionRetryChange = "interruption-retry"

// interruptionRetryGateDisabledForTest is a TEST-ONLY switch, the precedent
// of retryMaxIntervalGateDisabledForTest: when set, unitBudgetsFor takes the
// DefaultVersion branch WITHOUT asking its version, so a recorder can write a
// history in the shape of the builds before the gate. Never set in
// production code.
var interruptionRetryGateDisabledForTest atomic.Bool

// interruptionWait is one unit waiting out its not_before: a task's own row
// (index -1) or one item of a fan-out.
type interruptionWait struct {
	id     string
	index  int32
	timer  workflow.Future
	cancel workflow.CancelFunc
}

// unitKey is a unit's key: its task id, or its item's in-flight key.
func unitKey(id string, index int32) string {
	if index < 0 {
		return id
	}
	return itemFlightKey(id, index)
}

// unitBudgetsFor is t's budgets for this run, nil when its executions list
// nothing: a plan without budgets, a pinned task (a pinned member's
// interruption spends its own attempt, and a lost pin host is
// PIN_HOST_LOST), a task that opted out, or a run the gate keeps on the
// DefaultVersion branch.
func (s *runState) unitBudgetsFor(ctx workflow.Context, t *orkstackv1.Task) *unitBudgets {
	plan := s.plan.Interruptions
	if plan == nil || t.GetPin() != "" {
		return nil
	}
	var limit *int32
	if r := t.GetRetry(); r != nil && r.MaxInterruptions != nil {
		v := r.GetMaxInterruptions()
		limit = &v
	}
	b := budgetsForUnit(plan, limit, time.Duration(max(plan.NeverStartedFaultFloorMillis, 0))*time.Millisecond)
	if b == nil || interruptionRetryGateDisabledForTest.Load() ||
		s.version(ctx, InterruptionRetryChange, 1) < 1 {
		return nil
	}
	return b
}

// interruptionListing is what the unit's execution about to be scheduled
// lists, remembered until it ends, so the failure is read against what was
// listed then and not against budgets another unit has moved since. Nothing
// in flight crosses a continuation, so the map never needs to.
func (s *runState) interruptionListing(ctx workflow.Context, t *orkstackv1.Task, row *orkstackv1.TaskRun, key string) (*unitBudgets, []string) {
	b := s.unitBudgetsFor(ctx, t)
	if b == nil {
		return nil, nil
	}
	types := interruptionTypes(b, row.GetInterruptions(), s.lossEvents, s.profileContractsPath(t))
	if s.listed == nil {
		s.listed = map[string][]string{}
	}
	s.listed[key] = types
	return b, types
}

// interruptionOf reads an ended execution of the unit (id, index) as an
// interruption: ok is false for anything else — a failure its execution did
// not list, a cancel (which wins), or one met while the run is ending. When
// ok, row.Interruptions has been updated and the decision says what to do.
func (s *runState) interruptionOf(ctx workflow.Context, id string, index int32,
	row *orkstackv1.TaskRun, cause error) (interruptionDecision, bool) {
	key := unitKey(id, index)
	types, listed := s.listed[key]
	delete(s.listed, key)
	// A cancel wins: the event context may be a disconnected one, so the
	// run's own contexts are asked too.
	if !listed || len(types) == 0 || s.ending || ctx.Err() != nil || (s.runCtx != nil && s.runCtx.Err() != nil) ||
		(s.isFailureHandlerID(id) && s.handlerCtx != nil && s.handlerCtx.Err() != nil) {
		return interruptionDecision{}, false
	}
	r := categoryOf(cause)
	if r.canceled {
		return interruptionDecision{}, false
	}
	now := workflow.Now(ctx)
	c := classifyInterruption(r, slices.Contains(types, heartbeatTimeoutType), now)
	if c.kind == notInterruption {
		return interruptionDecision{}, false
	}
	// A typed interruption whose type the execution did not list was retried
	// by the cluster as an ordinary failure and has spent its attempts.
	if c.kind != lost && !slices.Contains(types, interruptionTypePrefix+c.event.GetReason()) {
		return interruptionDecision{}, false
	}
	if c.undecoded {
		workflow.GetLogger(ctx).Warn("interruption details could not be read; budgeted conservatively",
			"run_id", s.runID, "task_id", id, "item", index, "class", c.kind.String())
	}
	t := s.tasks[id]
	b := s.unitBudgetsFor(ctx, t)
	if b == nil {
		return interruptionDecision{}, false
	}
	if row.Interruptions == nil {
		row.Interruptions = &orkstackv1.TaskInterruptions{}
	}
	// The fence is the task's grace plus the heartbeat timeout its activity
	// is scheduled with: that constant predates AGENTS.md §7.2's pinning rule
	// and already decides every activity command a recorded history holds,
	// so changing it is a new command path of its own (taskActivityContext).
	lossWait := time.Duration(graceSecondsFor(t))*time.Second + orkerHeartbeatTimeout
	m := max(t.GetRetry().GetMaximumAttempts(), 1)
	return applyInterruption(row.Interruptions, *b, m, lossWait, s.lossEvents, c, now), true
}

// requeueUnit puts an interrupted unit back to PENDING, emits its notice,
// and then — only then, so the notice precedes the wait in history — waits
// out its not_before on a Timer when it has one; due now otherwise. A due
// item joins its fan-out's queue of items to run again; a due task is ready
// to the next scan, which dispatches it as new dispatch under the hold line.
func (s *runState) requeueUnit(ctx workflow.Context, id string, index int32, row *orkstackv1.TaskRun, d interruptionDecision) {
	// What the interrupted execution reported is its attempt record's
	// (orkctl run attempts), never the PENDING row's.
	row.Status = orkstackv1.Status_STATUS_PENDING
	row.Error = nil
	row.FinishedAt = nil
	row.StartedAt = nil
	row.ExitCode = 0
	row.RetryState = orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED
	if index < 0 && s.failingPhase && s.isFailureHandlerID(id) {
		s.pendingHandlers++
	}
	s.emitInterruption(ctx, id, index, row, d)
	// The wait ends at the instant recorded on the row, however long the
	// notice took: what remains of it, or nothing once it has passed.
	if nb := row.GetInterruptions().GetNotBefore(); d.wait > 0 && nb != nil {
		if remaining := remainingWait(nb.AsTime(), workflow.Now(ctx)); remaining > 0 {
			s.armInterruptionWait(ctx, id, index, remaining)
			return
		}
		row.Interruptions.NotBefore = nil
	}
	if index >= 0 {
		e := s.eaches[id]
		e.again = append(e.again, index)
	}
}

// remainingWait is what is left of a wait that ends at notBefore, read on the
// workflow's clock (now is workflow.Now): 0 once it has passed, so the unit
// is due at once.
func remainingWait(notBefore, now time.Time) time.Duration {
	return max(notBefore.Sub(now), 0)
}

// armInterruptionWait starts the unit's not_before Timer on the context its
// task dispatches on, so fail-fast settles a waiting task's and a failure
// handler's survives it.
func (s *runState) armInterruptionWait(ctx workflow.Context, id string, index int32, wait time.Duration) {
	timerCtx, cancel := workflow.WithCancel(s.dispatchContext(s.runCtx, id))
	timer := workflow.NewTimer(timerCtx, wait)
	if s.waiting == nil {
		s.waiting = map[string]bool{}
	}
	s.waits = append(s.waits, &interruptionWait{id: id, index: index, timer: timer, cancel: cancel})
	s.waiting[unitKey(id, index)] = true
	// The Timer's command now, and what failReserve counts for a wait (its
	// cancel or fire) from this pass on.
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
		s.chargeReserve(limit.Reserve.PerAwait, 0)
	}
}

// addInterruptionWaits registers every wait's Timer, in the order they were
// armed: a fixed position after every other case of the loop's selector.
func (s *runState) addInterruptionWaits(selector workflow.Selector, fired **interruptionWait) {
	for _, w := range s.waits {
		wait := w
		selector.AddFuture(wait.timer, func(workflow.Future) { *fired = wait })
	}
}

// interruptionWaitOver ends w: its unit is due. A due item joins its
// fan-out's queue, and the fan-out refills; a due task is ready to the next
// scan. A Timer the run's end settled — an operator's cancel — returns that
// cancellation, as an await's deadline and a sleep's wake do, so the run
// ends canceled and the ending path records the unit; the failure-handler
// phase discards it, its own stopping check taking the unit.
func (s *runState) interruptionWaitOver(ctx, runCtx workflow.Context, w *interruptionWait,
	inFlight map[string]workflow.Future) error {
	s.dropWait(w.id, w.index)
	if err := w.timer.Get(ctx, nil); err != nil {
		return err
	}
	row := s.unitRow(w.id, w.index)
	if row.GetStatus() != orkstackv1.Status_STATUS_PENDING {
		return nil
	}
	if row.GetInterruptions() != nil {
		row.Interruptions.NotBefore = nil
	}
	if w.index < 0 {
		return nil
	}
	e := s.eaches[w.id]
	e.again = append(e.again, w.index)
	if s.taskRuns[w.id].GetStatus() != orkstackv1.Status_STATUS_RUNNING || s.holdDispatch(ctx, w.id, inFlight) {
		s.heldFanOuts = s.heldFanOuts || s.fanOutCanDispatch(w.id)
		return nil
	}
	return s.dispatchItems(ctx, runCtx, w.id, inFlight)
}

// dropWait forgets the unit's wait, if it has one, and cancels its Timer
// (a no-op once it fired), so a wait given up adds nothing to history.
func (s *runState) dropWait(id string, index int32) {
	key := unitKey(id, index)
	if !s.waiting[key] {
		return
	}
	delete(s.waiting, key)
	s.waits = slices.DeleteFunc(s.waits, func(w *interruptionWait) bool {
		if w.id == id && w.index == index {
			w.cancel()
			return true
		}
		return false
	})
}

// dropHandlerWaits gives up every failure handler's wait: the failure-handler
// phase is stopping, so a waiting handler is never run again and its next
// scan cancels it.
func (s *runState) dropHandlerWaits() {
	for _, w := range slices.Clone(s.waits) {
		if w.index < 0 && s.isFailureHandlerID(w.id) {
			s.dropWait(w.id, w.index)
		}
	}
}

// unitRow is the unit's row: the task's own, or its item's.
func (s *runState) unitRow(id string, index int32) *orkstackv1.TaskRun {
	if index < 0 {
		return s.taskRuns[id]
	}
	return s.taskRuns[id].GetItems()[index]
}

// interruptionNotice is the TASK_LOG an interruption emits: what ended the
// execution and when the unit runs again.
func interruptionNotice(index int32, st *orkstackv1.TaskInterruptions, d interruptionDecision) string {
	subject := "task"
	if index >= 0 {
		subject = fmt.Sprintf("item %d", index)
	}
	what := "interrupted (" + interruptionCause(st.GetLast()) + ")"
	switch {
	case d.wait > 0:
		return fmt.Sprintf("%s %s; runs again at %s", subject, what,
			st.GetNotBefore().AsTime().UTC().Format(time.RFC3339))
	case d.closedBy != "":
		return fmt.Sprintf("%s %s; its budget is spent, so it runs again on its own attempts", subject, what)
	default:
		return fmt.Sprintf("%s %s; runs again", subject, what)
	}
}

// interruptionCause names an interruption's fact in a sentence.
func interruptionCause(last *orkstackv1.TaskInterruption) string {
	switch reason := last.GetReason(); {
	case reason == orkerStoppedReason && last.GetNeverStarted():
		return "handed back before it started by a stopping Orker"
	case reason == orkerStoppedReason:
		return "handed back by a stopping Orker"
	case reason == containerStoppedByRuntimeReason:
		return "its container was stopped by the runtime while its Orker was stopping"
	case reason == orkerHeartbeatLostReason:
		return "its Orker was lost"
	case isNeverStartedFault(reason):
		return strings.ToLower(strings.ReplaceAll(reason, "_", " ")) + " before it started"
	default:
		return reason
	}
}

// emitInterruption publishes the unit's interruption notice with the
// interruption on the event (Event.interruption). An emit error is logged:
// the unit's own state is recorded, and the next event carries it.
func (s *runState) emitInterruption(ctx workflow.Context, id string, index int32, row *orkstackv1.TaskRun, d interruptionDecision) {
	s.eventInterruption = row.GetInterruptions().GetLast()
	defer func() { s.eventInterruption = nil }()
	message := interruptionNotice(index, row.GetInterruptions(), d)
	var err error
	if index < 0 {
		err = s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id, orkstackv1.Status_STATUS_PENDING, message)
	} else {
		err = s.emitItem(ctx, id, index, orkstackv1.Status_STATUS_PENDING, message, false)
	}
	if err != nil {
		workflow.GetLogger(ctx).Error("emit interruption failed", "run_id", s.runID, "task_id", id, "error", err)
	}
}

// takeEventInterruption is the interruption the event being built carries,
// nil for every other event.
func (s *runState) takeEventInterruption() *orkstackv1.TaskInterruption {
	i := s.eventInterruption
	s.eventInterruption = nil
	return i
}

// notRunAgain records a unit waiting to run again when its run fails: it is
// FAILED with its interruption's reason, never run again (design §4.8,
// terminal states). An operator's cancel and a run timeout leave it to the
// ending path's CANCELED instead.
func (s *runState) notRunAgain(ctx workflow.Context, id string, index int32, row *orkstackv1.TaskRun) {
	s.dropWait(id, index)
	row.Status = orkstackv1.Status_STATUS_FAILED
	row.FinishedAt = timestamppb.New(workflow.Now(ctx))
	last := row.GetInterruptions().GetLast()
	row.Error = &orkstackv1.Error{
		Code:     last.GetReason(),
		Message:  "not run again: the run is failing; it was " + interruptionCause(last),
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
	}
	if index >= 0 {
		return
	}
	s.taskEnded(ctx, id)
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, row.Error.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed", "run_id", s.runID, "task_id", id, "error", err)
	}
}

// waitingToRunAgain: the row is a unit an interruption put back to PENDING.
func waitingToRunAgain(row *orkstackv1.TaskRun) bool {
	return row.GetStatus() == orkstackv1.Status_STATUS_PENDING && row.GetInterruptions().GetLast() != nil
}

// restoreInterruptionWaits re-arms, after a continuation, every unit an
// interruption put back to PENDING: a not_before still ahead gets its Timer
// toward the original instant; a due item rejoins its fan-out's queue, in
// index order. A due task needs nothing: the scan finds it PENDING.
func (s *runState) restoreInterruptionWaits(ctx workflow.Context) {
	now := workflow.Now(ctx)
	rearm := func(id string, index int32, row *orkstackv1.TaskRun) bool {
		nb := row.GetInterruptions().GetNotBefore()
		if nb == nil || remainingWait(nb.AsTime(), now) == 0 || ctx.Err() != nil {
			return false
		}
		s.armInterruptionWait(ctx, id, index, remainingWait(nb.AsTime(), now))
		return true
	}
	for _, id := range s.order {
		row := s.taskRuns[id]
		if waitingToRunAgain(row) {
			rearm(id, -1, row)
		}
		e := s.eaches[id]
		if e == nil || !e.expanded {
			continue
		}
		for i, item := range row.GetItems() {
			if !waitingToRunAgain(item) || item.GetScheduledAt() == nil {
				continue
			}
			if !rearm(id, int32(i), item) {
				e.again = append(e.again, int32(i))
			}
		}
	}
}
