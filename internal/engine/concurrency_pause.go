package engine

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// A paused run and its run-level slot (concurrency design §22.5, §22.14.3,
// §22.14.7, §22.15): once paused and quiet it gives the slot back, and on
// unpause it asks for it again, with the slot's next ordinal, at the back of
// the line. Entered only by a plan carrying both Pause and Concurrency.

// requeueWait is a run's wait for its run-level slot again after an
// unpause: the acquire it sent and the gate's answer, its deadline and its
// re-assert timer. The main loop's selector waits on it (addConcWaitCases).
type requeueWait struct {
	req            gateAcquire
	ans            gateAnswer
	deadline       workflow.Future
	cancelDeadline workflow.CancelFunc
	reassert       workflow.Future
	cancelReassert workflow.CancelFunc
	interval       time.Duration
	// rebuilding and unanswered say the wait may end unavailable rather
	// than timed out; lineFull is a line at its bound, asked again with the
	// acquire back-off (§22.14.3, full_bound).
	rebuilding, unanswered, lineFull bool
}

// queuedNoteLineFull is a re-queued run's note while its key's line is at
// its bound (§22.14.3).
const queuedNoteLineFull = "LINE_FULL"

// concWaiting reports whether the run waits for its run-level slot again:
// new dispatch is held, and no switch is made, until it is granted.
func (s *runState) concWaiting() bool {
	return s.conc != nil && s.conc.requeue != nil
}

// concSlotGivenBack reports whether the run gave its run-level slot back
// and does not hold it again.
func (s *runState) concSlotGivenBack() bool {
	return s.conc != nil && s.conc.held == nil && (s.conc.givenBack || s.conc.requeue != nil)
}

// concGivesBack reports whether a paused run gives its run-level slot back
// now: it holds one and is quiet, with no activity, item, child run, pin
// opening, open or closing, or failure handler in flight. Open awaits and
// sleeps do not count.
func (s *runState) concGivesBack(inFlight map[string]workflow.Future) bool {
	return s.conc != nil && s.plan.Pause != nil && s.conc.held != nil && s.paused() &&
		len(inFlight) == 0 && s.openingPins() == 0 && s.openedUnclosedPins() == 0 && s.closingPins() == 0
}

// concReacquires reports whether the run asks for its slot again now: it
// gave it back and nothing pauses it any longer.
func (s *runState) concReacquires() bool {
	return s.conc != nil && s.conc.givenBack && s.conc.requeue == nil && !s.paused()
}

// giveBackRunSlot releases the run-level slot it holds, in one workflow
// task and in §22.14.7's order: the run's own ledger first, so no grant can
// match it any more, then the single-slot release with its ordinal (never
// release_all, whose run tombstone would fence the re-acquire), the memo
// key and the index attribute. It emits no event: the snapshot shows the
// slot released.
func (s *runState) giveBackRunSlot(ctx workflow.Context) error {
	c := s.conc
	held := c.held
	c.held, c.givenBack = nil, true
	return s.releaseRunEntry(ctx, held.gate)
}

// releaseRunEntry sends release{run, seq_base, -1, n} for the run's entry at
// gate, held or queued, and drops its records. A gate that is gone has
// nothing to release; any other failure is logged, and the gate's check pass
// frees the entry.
func (s *runState) releaseRunEntry(ctx workflow.Context, gate string) error {
	c := s.conc
	f := workflow.SignalExternalWorkflow(ctx, gate, "", GateSignalRelease,
		// The gate is no run and holds no key: ids and indices only.
		payloadcrypt.PlainIn(runScope(ctx), GateRelease{Run: s.runID, SeqBase: c.plan.SeqBase, TaskIx: gateRunLevel, N: c.ordinal}))
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{gateMemoKey(gate, gateRunLevel): nil})); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not drop its slot's record: %v", err))
	}
	if err := s.syncConcAttributes(ctx); err != nil {
		return err
	}
	if err := f.Get(ctx, nil); err != nil && !categoryOf(err).unknownRun {
		workflow.GetLogger(ctx).Warn("a paused run could not release its concurrency slot; the limit's check pass frees it",
			"run", s.runID, "gate", gate, "seq_base", c.plan.SeqBase, "n", c.ordinal, "error", err)
	}
	return nil
}

// reacquireRunSlot asks for the run-level slot again after an unpause
// (§22.5): the slot's next ordinal, always strategy queue, marked requeue,
// with an absolute deadline of now plus the slot's queue timeout.
func (s *runState) reacquireRunSlot(ctx workflow.Context) error {
	c := s.conc
	slot := c.plan.runSlot()
	c.ordinal++
	now := workflow.Now(ctx)
	req := gateAcquire{
		Proto: gateProtoOrdinal, Run: s.runID, SeqBase: c.plan.SeqBase, TaskIx: gateRunLevel,
		Limit: slot.Limit, Strategy: gateStrategyQueue, Label: slot.Label,
		WaitUntil: now.Add(time.Duration(slot.QueueTimeoutSeconds) * time.Second), IssuedAt: now,
		N: c.ordinal, Requeue: true,
	}
	c.noteAttempted(slot.GateID)
	res, err := s.sendAcquire(ctx, slot, req)
	if err != nil {
		return err
	}
	return s.takeReacquireAnswer(ctx, req, res)
}

// takeReacquireAnswer acts on a re-acquire's answer (§22.14.3): a grant
// ends the wait, a place in line starts or continues it, a full line is
// asked again until the deadline, and every other answer ends the run.
func (s *runState) takeReacquireAnswer(ctx workflow.Context, req gateAcquire, res AcquireConcurrencyResult) error {
	c := s.conc
	slot := c.plan.runSlot()
	switch {
	case res.Unsupported != "":
		return s.requeueEnding(runLevelError(RunConcurrencyUnsupportedCode,
			fmt.Sprintf("the limit on key %s cannot take this run's request (cause=protocol): %s", slot.Key, res.Unsupported)))
	case res.Unavailable != "":
		// The run has started and done work, so a limit that does not
		// answer keeps it waiting, never ends it at once: counted, so a wait
		// that ends with its limit unreachable ends unavailable, never
		// timed out.
		workflow.GetLogger(ctx).Warn("a re-queued run's acquire was not answered; it keeps waiting",
			"run", s.runID, "gate", slot.GateID, "cause", res.Unavailable)
		return s.waitAgain(ctx, slot, req, gateAnswer{}, waitUnanswered)
	}
	switch res.Answer.Answer {
	case gateAnswerGranted:
		s.noteWaitEnd(ctx, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED)
		s.endRequeue()
		c.givenBack = false
		return s.grantRunSlot(ctx, slot, res.Answer)
	case gateAnswerQueued:
		return s.waitAgain(ctx, slot, req, res.Answer, waitPlaced)
	case gateAnswerFullBound:
		return s.waitAgain(ctx, slot, req, res.Answer, waitLineFull)
	case gateAnswerUnavailable:
		// The limit is rebuilding its record: the run waits, as a run in
		// line does while it rebuilds.
		return s.waitAgain(ctx, slot, req, res.Answer, waitRebuilding)
	case gateAnswerFull:
		// Answered only to refuse and skip, never to a queue re-acquire.
		return s.requeueEnding(runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf(
			"the limit on key %s answered full to this run's request for its slot again after an unpause, which it answers only to refuse and skip", slot.Key)))
	}
	if ending := concEnding(res, slot); ending != nil {
		return s.requeueEnding(ending)
	}
	return s.requeueEnding(runLevelError(RunConcurrencyUnsupportedCode,
		fmt.Sprintf("the limit on key %s answered %q, which this release does not know", slot.Key, res.Answer.Answer)))
}

// waitMode is where a re-queued run stands after the gate's answer.
type waitMode int

const (
	waitPlaced     waitMode = iota // in line, at a known place
	waitLineFull                   // the line is at its bound; the run is not in it
	waitUnanswered                 // the limit did not answer
	waitRebuilding                 // the limit is rebuilding its record
)

// waitAgain starts the re-queued run's wait, or carries it on with the
// gate's newest answer. Outside the line (full, unanswered, rebuilding) the
// run's place is unknown and its note says why; it asks again with the
// acquire back-off while the line is full, the re-queue interval otherwise.
func (s *runState) waitAgain(ctx workflow.Context, slot *ConcurrencySlotPlan, req gateAcquire, ans gateAnswer, mode waitMode) error {
	c := s.conc
	w := c.requeue
	first := w == nil
	if first {
		w = &requeueWait{req: req, interval: c.plan.RequeueInterval}
		c.requeue = w
		dctx, cancel := workflow.WithCancel(ctx)
		w.deadline, w.cancelDeadline = workflow.NewTimer(dctx, max(req.WaitUntil.Sub(workflow.Now(ctx)), time.Millisecond)), cancel
		c.waitStarted = workflow.Now(ctx)
		c.queued = &orkstackv1.ConcurrencyQueued{
			Key: slot.Key, Limit: int32(slot.Limit), Strategy: concStrategyProto(gateStrategyQueue),
			QueuedSince: timestamppb.New(workflow.Now(ctx)), TimesOutAt: timestamppb.New(req.WaitUntil),
			Cause: orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_UNPAUSE,
		}
	}
	until := req.WaitUntil.UTC().Format(time.RFC3339)
	w.lineFull = mode == waitLineFull
	w.unanswered = mode == waitUnanswered
	w.rebuilding = mode == waitRebuilding || ans.Rebuilding
	if mode != waitPlaced {
		c.queued.PositionKnown, c.queued.Position = false, 0
		switch mode {
		case waitLineFull:
			c.queued.NoteCode, c.queued.Note = queuedNoteLineFull, "the key's line is full; this run asks again until "+until
		case waitUnanswered:
			c.queued.NoteCode = QueuedNoteNotAnswering
			c.queued.Note = "the limit is not answering; this run stays queued and asks again by " +
				workflow.Now(ctx).Add(w.interval).UTC().Format(time.RFC3339)
		case waitRebuilding:
			c.queued.NoteCode = QueuedNoteRebuilding
			c.queued.Note = rebuildingNote(slot.Key)
		}
		// Not in line, so no memo entry; the index says the run waits.
		if err := s.syncConcAttributes(ctx); err != nil {
			return err
		}
	} else {
		moved := first || ans.Seq != w.ans.Seq || !ans.ArrivedAt.Equal(w.ans.ArrivedAt)
		w.ans = ans
		c.queued.PositionKnown, c.queued.Position = true, int32(ans.Position)
		c.queued.NoteCode, c.queued.Note = "", ""
		if moved {
			c.queued.PositionAtArrival, c.queued.HoldersCount, c.queued.HoldersAsOfArrival = int32(ans.Position), int32(ans.HoldersAhead), true
			c.queued.QueuedSince = timestamppb.New(ans.ArrivedAt)
			// The run's grant now names this entry.
			if err := s.recordRunSlot(ctx, slot, ans, false); err != nil {
				return err
			}
		}
	}
	if first {
		text := fmt.Sprintf("waiting again for key %s after an unpause (limit %d): ", slot.Key, slot.Limit)
		if mode == waitPlaced {
			text += fmt.Sprintf("position %d behind %d running; times out at %s", ans.Position, ans.HoldersAhead, until)
		} else {
			text += c.queued.GetNote() + "; times out at " + until
		}
		if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "", s.status, text); err != nil {
			return err
		}
	}
	s.armReassert(ctx)
	return nil
}

// armReassert starts the wait's next re-assert, unless one is pending: the
// acquire back-off while the line is full, the re-queue interval otherwise.
func (s *runState) armReassert(ctx workflow.Context) {
	c := s.conc
	w := c.requeue
	if w == nil || w.reassert != nil {
		return
	}
	d := w.interval
	if w.lineFull {
		d = c.plan.AcquireBackoffMax
	}
	tctx, cancel := workflow.WithCancel(ctx)
	w.reassert, w.cancelReassert = workflow.NewTimer(tctx, d), cancel
}

// endRequeue cancels the wait's timers and clears its markers.
func (s *runState) endRequeue() {
	c := s.conc
	if c == nil {
		return
	}
	if w := c.requeue; w != nil {
		w.cancelDeadline()
		if w.cancelReassert != nil {
			w.cancelReassert()
		}
	}
	c.requeue, c.queued = nil, nil
}

// leaveRequeue ends a wait the run's ending cut short (fail): the wait's
// timers and markers go, and so do its memo entry and its index values, so
// the closed run never reads as queued. Best effort: a failed write is
// logged, never put in place of the run's own reason.
func (s *runState) leaveRequeue(ctx workflow.Context) {
	if !s.concWaiting() {
		return
	}
	s.endRequeue()
	gate := s.conc.plan.runSlot().GateID
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{gateMemoKey(gate, gateRunLevel): nil})); err != nil {
		workflow.GetLogger(ctx).Warn("a run that left its re-queue could not drop its slot's record", "run_id", s.runID, "error", err)
	}
	if err := s.syncConcAttributes(ctx); err != nil {
		workflow.GetLogger(ctx).Warn("a run that left its re-queue could not update its index", "run_id", s.runID, "error", err)
	}
}

// abandonRequeue is a pause reaching a run that waits for its slot again: it
// releases its entry in line (ordinal n) and stays given back, so the next
// unpause asks with n+1 (§22.5).
func (s *runState) abandonRequeue(ctx workflow.Context) error {
	s.endRequeue()
	return s.releaseRunEntry(ctx, s.conc.plan.runSlot().GateID)
}

// concWaitCase is what woke the main loop for the re-queued run's wait.
type concWaitCase int

const (
	concWaitNone concWaitCase = iota
	// concWaitIgnored: a grant that is not the wait's entry; the loop
	// goes back to its pass top.
	concWaitIgnored
	concWaitGrant
	concWaitDeadline
	concWaitReassert
)

// addConcWaitCases registers the re-queued run's grant signal, deadline and
// re-assert on the main loop's selector, setting *woke to what fired. A
// grant that matches no live entry is logged and ignored (§22.14.7).
func (s *runState) addConcWaitCases(ctx workflow.Context, selector workflow.Selector, woke *concWaitCase) {
	if !s.concWaiting() {
		return
	}
	c := s.conc
	w := c.requeue
	slot := c.plan.runSlot()
	selector.AddReceive(workflow.GetSignalChannel(ctx, ConcurrencyGrantSignal), func(ch workflow.ReceiveChannel, _ bool) {
		var grant ConcurrencyGrant
		ch.Receive(ctx, &grant)
		mine := grant.Gate == slot.GateID && grant.Run == s.runID && grant.TaskIx == gateRunLevel &&
			grant.SeqBase == w.req.SeqBase && grant.N == w.req.N
		if mine && !w.lineFull && grant.Seq == w.ans.Seq {
			*woke = concWaitGrant
			return
		}
		if mine {
			// This run's slot under another seq (a rebuild renumbered the
			// line): ask the gate at once rather than at the next re-assert.
			*woke = concWaitReassert
			return
		}
		*woke = concWaitIgnored
		workflow.GetLogger(ctx).Warn("a re-queued run ignored a grant that is not its entry's",
			"run", s.runID, "gate", slot.GateID, "grant", grant, "seq", w.ans.Seq, "n", w.req.N)
	})
	selector.AddFuture(w.deadline, func(workflow.Future) { *woke = concWaitDeadline })
	selector.AddFuture(w.reassert, func(workflow.Future) { *woke = concWaitReassert })
}

// takeConcWait acts on what woke the re-queued run's wait: a grant takes the
// slot, the deadline ends the run, and a re-assert asks the gate again at
// the place it gave.
func (s *runState) takeConcWait(ctx workflow.Context, woke concWaitCase) error {
	c := s.conc
	w := c.requeue
	slot := c.plan.runSlot()
	switch woke {
	case concWaitGrant:
		ans := w.ans
		s.noteWaitEnd(ctx, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED)
		s.endRequeue()
		c.givenBack = false
		return s.grantRunSlot(ctx, slot, ans)
	case concWaitDeadline:
		if w.rebuilding || w.unanswered {
			return s.requeueEnding(s.requeueUnavailable(slot))
		}
		return s.requeueEnding(runLevelError(RunQueueTimeoutCode, fmt.Sprintf(
			"waited %s for key %s's slot after an unpause without continuing",
			w.req.WaitUntil.Sub(w.req.IssuedAt).Round(time.Second), slot.Key)))
	case concWaitReassert:
		if w.cancelReassert != nil {
			w.cancelReassert() // a grant may have brought the re-assert forward
		}
		w.reassert = nil
		again := w.req
		again.IssuedAt = workflow.Now(ctx)
		if !w.lineFull {
			again.ArrivedAt, again.Seq = w.ans.ArrivedAt, w.ans.Seq
		}
		res, err := s.sendAcquire(ctx, slot, again)
		if err != nil {
			return err
		}
		if res.Unavailable == "" {
			w.unanswered = false
		}
		if c.requeue != nil && !w.lineFull {
			w.interval = min(time.Duration(c.plan.RequeueBackoffFactor)*w.interval, c.plan.RequeueIntervalMax)
		}
		return s.takeReacquireAnswer(ctx, w.req, res)
	}
	return nil
}

func (s *runState) requeueUnavailable(slot *ConcurrencySlotPlan) error {
	return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf(
		"the limit on key %s did not answer when this run asked for its slot again after an unpause", slot.Key))
}

// requeueEnding is how a re-queued run's wait ends the run. The ending goes
// to fail, which leaves the wait (leaveRequeue).
func (s *runState) requeueEnding(ending error) error {
	return ending
}

// failingRegain asks again, for a failing run that gave its slot back while
// paused, for the slot its failure handlers need, so they run inside the
// limit (§22.14.3, decision D22): the next ordinal, strategy queue, cause
// FAILING. Asked once per run. The wait blocks the failing phase: a pause,
// an await's resolution or deadline met meanwhile is applied after it, and
// only a cancel or the run timeout's hard end cuts it short. It returns "" once the run holds the slot again, or when the run
// is canceled meanwhile (the caller's context decides), and otherwise why its
// failure handlers cannot run: a line that ended, refused or did not answer.
func (s *runState) failingRegain(ctx workflow.Context) string {
	c := s.conc
	slot := c.plan.runSlot()
	c.ordinal++
	now := workflow.Now(ctx)
	timeout := time.Duration(slot.QueueTimeoutSeconds) * time.Second
	req := gateAcquire{
		Proto: gateProtoOrdinal, Run: s.runID, SeqBase: c.plan.SeqBase, TaskIx: gateRunLevel,
		Limit: slot.Limit, Strategy: gateStrategyQueue, Label: slot.Label,
		WaitUntil: now.Add(timeout), IssuedAt: now, N: c.ordinal, Requeue: true,
	}
	c.noteAttempted(slot.GateID)
	res, err := s.sendAcquire(ctx, slot, req)
	if err != nil {
		if ctx.Err() != nil {
			return "" // canceled: the caller's context ends the run
		}
		return s.regainStop(ctx, slot, timeout, runLevelError(RunConcurrencyUnavailableCode,
			fmt.Sprintf("the limit on key %s could not be asked: %v", slot.Key, err)))
	}
	var ending error
	switch {
	case res.Unsupported != "" || res.Unavailable != "":
		ending = concEnding(res, slot)
	case res.Answer.Answer == gateAnswerGranted:
		ending = s.grantRunSlot(ctx, slot, res.Answer)
	case res.Answer.Answer == gateAnswerQueued:
		// The run timeout's hard end bounds the wait as it bounds the
		// handlers: past it, the handlers are stopped (takeHardEnd).
		var hard workflow.Future
		if t := s.runTimeout; t != nil && t.hardTimer != nil && !t.hardTaken {
			hard = t.hardTimer
		}
		ending = s.waitForRunSlot(ctx, slot, req, res.Answer, orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_FAILING, hard)
		if errors.Is(ending, errWaitAborted) {
			s.leaveRegainLine(ctx, slot, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED)
			s.runTimeout.hardTaken = true
			s.takeHardEnd()
			return ""
		}
	}
	switch {
	case c.held != nil:
		// The run holds the slot again. A record it could not write after the
		// grant is logged, never a reason to skip its handlers.
		if ending != nil {
			workflow.GetLogger(ctx).Warn("a failing run regained its concurrency slot but could not record it all",
				"run_id", s.runID, "error", ending)
		}
		c.givenBack, c.queued = false, nil
		return ""
	case ctx.Err() != nil:
		s.leaveRegainLine(ctx, slot, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED)
		return "" // canceled while it waited: the caller's context ends the run
	case ending != nil:
		return s.regainStop(ctx, slot, timeout, ending)
	}
	switch {
	case res.Answer.Answer == gateAnswerFull:
		ending = runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the limit on key %s answered full to a queue request", slot.Key))
	default:
		ending = concEnding(res, slot)
	}
	return s.regainStop(ctx, slot, timeout, ending)
}

// regainStop is why a failing run's handlers do not run: its slot was not
// regained, said with the limit's own reason. The run leaves the line, and
// its records stop saying it waits.
func (s *runState) regainStop(ctx workflow.Context, slot *ConcurrencySlotPlan, timeout time.Duration, ending error) string {
	s.leaveRegainLine(ctx, slot, waitOutcomeOf(ctx, ending))
	failure := concFailure(ending)
	if failure.GetCode() == RunQueueTimeoutCode {
		return fmt.Sprintf("failure handlers did not run: the concurrency slot on key %s was not regained within %s (%s)",
			slot.Key, timeout.Round(time.Second), RunQueueTimeoutCode)
	}
	return fmt.Sprintf("failure handlers did not run: the concurrency slot on key %s was not regained (%s: %s)",
		slot.Key, failure.GetCode(), failure.GetMessage())
}

// leaveRegainLine drops a failing run's wait for its slot: the snapshot,
// memo entry and index stop saying it waits. Best effort: a failed write is
// logged.
func (s *runState) leaveRegainLine(ctx workflow.Context, slot *ConcurrencySlotPlan, outcome orkstackv1.ConcurrencyWaitOutcome) {
	c := s.conc
	if c.queued != nil {
		s.noteWaitEnd(ctx, outcome)
		c.queued = nil
		if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{gateMemoKey(slot.GateID, gateRunLevel): nil})); err != nil {
			workflow.GetLogger(ctx).Warn("a failing run that left the line could not drop its slot's record", "run_id", s.runID, "error", err)
		}
		if err := s.syncConcAttributes(ctx); err != nil {
			workflow.GetLogger(ctx).Warn("a failing run that left the line could not update its index", "run_id", s.runID, "error", err)
		}
	}
}
