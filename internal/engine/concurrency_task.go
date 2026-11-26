package engine

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// A task's own concurrency limit (E88 §5.3): a container task whose plan
// carries a slot asks its gate for it just before it is dispatched, keeps it
// across its retries and interruptions, and releases it when it ends. The
// wait is the main loop's: the acquire, the grant, the deadline and the
// re-assert are cases of its selector, so nothing else of the run waits.
// Entered only by a plan with Concurrency.Tasks, which no history recorded
// before task limits holds (AGENTS.md §7.2, first clause).

// The endings of a task's wait for its slot (§9.8).
const (
	TaskQueueTimeoutCode           = "TASK_QUEUE_TIMEOUT"
	TaskConcurrencyFullCode        = "TASK_CONCURRENCY_FULL"
	TaskConcurrencyUnavailableCode = "TASK_CONCURRENCY_UNAVAILABLE"
	TaskConcurrencyUnsupportedCode = "TASK_CONCURRENCY_UNSUPPORTED"
	TaskConcurrencyFencedCode      = "TASK_CONCURRENCY_FENCED"
)

// taskSlot is one task's slot: the limit, its ordinal n, and where it stands.
type taskSlot struct {
	id   string
	ix   int
	slot *ConcurrencySlotPlan
	// n is the slot's ordinal (§22.14.1): how many times the run gave the
	// slot, or its place in line, back and asked again.
	n int64
	// held: the gate granted it, under ans.
	held bool
	ans  gateAnswer
	// wait is the task's wait for it, nil when it does not wait.
	wait *taskWait
}

// taskWait is one task's wait: the acquire it sent, and the timers.
type taskWait struct {
	req gateAcquire
	// asking is the acquire in flight, nil between two.
	asking    workflow.Future
	cancelAsk workflow.CancelFunc
	// placed: the gate gave the task a place in line, ans. early is a grant
	// that came before the answer it belongs to.
	placed bool
	ans    gateAnswer
	early  *ConcurrencyGrant

	deadline       workflow.Future
	cancelDeadline workflow.CancelFunc
	reassert       workflow.Future
	cancelReassert workflow.CancelFunc
	interval       time.Duration
	// rebuilding and unanswered say the wait may end unavailable rather than
	// timed out.
	rebuilding, unanswered bool
	// lastCause is why the last acquire was not answered, for the ending.
	lastCause string
	// said: TASK_QUEUED was emitted.
	said bool
}

// addTasks builds the slots of wf's tasks that have one, in definition order.
func (c *concRunState) addTasks(wf *orkstackv1.Orkflow) {
	for i, t := range wf.GetTasks() {
		ix, ok := c.plan.Tasks[t.GetId()]
		if !ok {
			continue
		}
		if ix < 0 || ix >= len(c.plan.Slots) {
			// Refused at its ask, never run unlimited.
			c.tasks = append(c.tasks, &taskSlot{id: t.GetId(), ix: i})
			continue
		}
		c.tasks = append(c.tasks, &taskSlot{id: t.GetId(), ix: i, slot: &c.plan.Slots[ix]})
	}
}

// taskSlot is id's slot, nil for a task with no limit.
func (s *runState) taskSlot(id string) *taskSlot {
	if s.conc == nil {
		return nil
	}
	for _, t := range s.conc.tasks {
		if t.id == id {
			return t
		}
	}
	return nil
}

// taskSlotsPending counts the tasks that wait for a slot, or hold one and have
// not been dispatched with it: work the loop waits on, as an activity is.
func (s *runState) taskSlotsPending() int {
	if s.conc == nil {
		return 0
	}
	n := 0
	for _, t := range s.conc.tasks {
		if t.wait != nil || t.held && s.taskRuns[t.id].GetStatus() == orkstackv1.Status_STATUS_PENDING {
			n++
		}
	}
	return n
}

// taskSlotsOut counts the tasks that hold a slot or wait for one, running ones
// included.
func (s *runState) taskSlotsOut() int {
	if s.conc == nil {
		return 0
	}
	n := 0
	for _, t := range s.conc.tasks {
		if t.held || t.wait != nil {
			n++
		}
	}
	return n
}

// taskSlotsWaiting reports whether a task waits for its slot.
func (s *runState) taskSlotsWaiting() bool {
	return s.conc != nil && slices.ContainsFunc(s.conc.tasks, func(t *taskSlot) bool { return t.wait != nil })
}

// askTaskSlot reports whether id holds its slot, and asks for it when it does
// not: its run keeps at most one entry in a key's line, so a task whose key
// another of the run's tasks waits for asks once that one has its slot.
func (s *runState) askTaskSlot(ctx workflow.Context, id string) (bool, error) {
	t := s.taskSlot(id)
	if t == nil || t.held {
		return true, nil
	}
	if t.slot == nil {
		return false, s.endTaskWait(ctx, t, TaskConcurrencyUnsupportedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			"the run's plan names a concurrency slot it does not have")
	}
	for _, other := range s.conc.tasks {
		if other.wait != nil && other.slot.GateID == t.slot.GateID {
			return false, nil // itself, or another of the run's on that key
		}
	}
	now := workflow.Now(ctx)
	req := gateAcquire{
		Proto: gateProtoLegacy, Run: s.runID, SeqBase: s.conc.seqBase, TaskIx: t.ix,
		Limit: t.slot.Limit, Strategy: gateStrategyQueue, Label: t.slot.Label,
		WaitUntil: now.Add(time.Duration(t.slot.QueueTimeoutSeconds) * time.Second), IssuedAt: now,
	}
	if t.n > 0 {
		req.Proto, req.N, req.Requeue = gateProtoOrdinal, t.n, true
	}
	s.conc.noteAttempted(t.slot.GateID)
	t.wait = &taskWait{req: req, interval: s.conc.plan.RequeueInterval}
	// What giving it back appends is reserved from here on (taskSlotsOut).
	s.chargeReserve(s.conc.plan.PerSlotEvents, s.conc.plan.PerSlotBytes)
	s.sendTaskAcquire(ctx, t, req)
	return false, nil
}

// sendTaskAcquire starts the acquire; its answer is a case of the main loop.
func (s *runState) sendTaskAcquire(ctx workflow.Context, t *taskSlot, req gateAcquire) {
	s.noteCommand(ctx, 1, s.conc.plan.PerSlotBytes) // the answer's marker
	actx, cancel := workflow.WithCancel(ctx)
	lctx, in := s.acquireCall(actx, t.slot, req)
	t.wait.asking, t.wait.cancelAsk = workflow.ExecuteLocalActivity(lctx, ActivityAcquireConcurrency, in), cancel
}

// taskWakeKind is what woke the main loop for a task's wait.
type taskWakeKind int

const (
	taskWakeNone taskWakeKind = iota
	taskWakeAnswer
	taskWakeGrant
	taskWakeDeadline
	taskWakeReassert
)

// taskWake names the task a wake is for; slot is nil for a grant that is no
// live wait's.
type taskWake struct {
	kind  taskWakeKind
	slot  *taskSlot
	grant ConcurrencyGrant
}

// addTaskSlotCases registers every waiting task's cases, in definition order,
// and the run's grants after them, so an answer ready beside its own grant is
// taken first. A run that waits for its run-level slot reads the grants itself
// (addConcWaitCases), and then no task waits.
func (s *runState) addTaskSlotCases(ctx workflow.Context, selector workflow.Selector, woke *taskWake) {
	if !s.taskSlotsWaiting() {
		return
	}
	for _, t := range s.conc.tasks {
		w, slot := t.wait, t
		switch {
		case w == nil:
		case w.asking != nil:
			selector.AddFuture(w.asking, func(workflow.Future) { *woke = taskWake{kind: taskWakeAnswer, slot: slot} })
		default:
			selector.AddFuture(w.deadline, func(workflow.Future) { *woke = taskWake{kind: taskWakeDeadline, slot: slot} })
			selector.AddFuture(w.reassert, func(workflow.Future) { *woke = taskWake{kind: taskWakeReassert, slot: slot} })
		}
	}
	if s.concWaiting() {
		return
	}
	selector.AddReceive(workflow.GetSignalChannel(ctx, ConcurrencyGrantSignal), func(ch workflow.ReceiveChannel, _ bool) {
		var grant ConcurrencyGrant
		ch.Receive(ctx, &grant)
		*woke = taskWake{kind: taskWakeGrant, slot: s.grantedTask(grant), grant: grant}
		if woke.slot == nil {
			workflow.GetLogger(ctx).Warn("a run ignored a grant that is no waiting task's", "run", s.runID, "grant", grant)
		}
	})
}

// grantedTask is the waiting task a grant is for, nil for none.
func (s *runState) grantedTask(grant ConcurrencyGrant) *taskSlot {
	for _, t := range s.conc.tasks {
		if w := t.wait; w != nil && grant.Gate == t.slot.GateID && grant.Run == s.runID && grant.TaskIx == t.ix &&
			grant.SeqBase == w.req.SeqBase && grant.N == w.req.N {
			return t
		}
	}
	return nil
}

// takeTaskWake acts on what woke a task's wait. Its error is the task's ending.
func (s *runState) takeTaskWake(ctx workflow.Context, woke taskWake) error {
	t := woke.slot
	if t == nil || t.wait == nil {
		return nil
	}
	if ctx.Err() != nil {
		return temporal.NewCanceledError()
	}
	w := t.wait
	switch woke.kind {
	case taskWakeAnswer:
		var res AcquireConcurrencyResult
		err := getLocal(ctx, runScope(ctx), w.asking, &res)
		w.asking, w.cancelAsk = nil, nil
		if err != nil {
			res = AcquireConcurrencyResult{Unavailable: err.Error()}
		}
		return s.takeTaskAnswer(ctx, t, res)
	case taskWakeGrant:
		switch {
		case w.asking != nil:
			grant := woke.grant
			w.early = &grant
			return nil
		case w.placed && woke.grant.Seq == w.ans.Seq:
			return s.grantTask(ctx, t, w.ans)
		}
		// This task's entry under another seq (a rebuild renumbered the
		// line): ask the gate at once.
		s.reassertTask(ctx, t)
	case taskWakeReassert:
		s.reassertTask(ctx, t)
	case taskWakeDeadline:
		if w.rebuilding || w.unanswered {
			message := fmt.Sprintf("the limit on key %s was not answering or was rebuilding its record when this task's wait ended", t.slot.Key)
			if w.lastCause != "" {
				message += "; its last acquire: " + w.lastCause
			}
			return s.endTaskWait(ctx, t, TaskConcurrencyUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, message)
		}
		return s.endTaskWait(ctx, t, TaskQueueTimeoutCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
			fmt.Sprintf("this task waited %s for key %s and timed out", w.req.WaitUntil.Sub(w.req.IssuedAt).Round(time.Second), t.slot.Key))
	}
	return nil
}

// reassertTask asks the gate again, at the place it gave.
func (s *runState) reassertTask(ctx workflow.Context, t *taskSlot) {
	w := t.wait
	s.stopTaskTimer(&w.reassert, &w.cancelReassert)
	again := w.req
	again.IssuedAt = workflow.Now(ctx)
	if w.placed {
		again.ArrivedAt, again.Seq = w.ans.ArrivedAt, w.ans.Seq
	}
	w.interval = min(time.Duration(s.conc.plan.RequeueBackoffFactor)*w.interval, s.conc.plan.RequeueIntervalMax)
	s.sendTaskAcquire(ctx, t, again)
}

func (s *runState) stopTaskTimer(f *workflow.Future, cancel *workflow.CancelFunc) {
	if *cancel != nil {
		(*cancel)()
	}
	*f, *cancel = nil, nil
}

// takeTaskAnswer acts on an acquire's answer: a grant, a place in line, a
// limit that does not answer or rebuilds, which keeps the task waiting (its
// run has started and done work), or an ending.
func (s *runState) takeTaskAnswer(ctx workflow.Context, t *taskSlot, res AcquireConcurrencyResult) error {
	w := t.wait
	infra := orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	switch {
	case res.Unsupported != "":
		return s.endTaskWait(ctx, t, TaskConcurrencyUnsupportedCode, infra,
			fmt.Sprintf("the limit on key %s cannot take this task's request (cause=protocol): %s", t.slot.Key, res.Unsupported))
	case res.Unavailable != "":
		workflow.GetLogger(ctx).Warn("a task's acquire was not answered; it keeps waiting",
			"run", s.runID, "task_id", t.id, "gate", t.slot.GateID, "cause", res.Unavailable)
		w.unanswered, w.lastCause = true, res.Unavailable
		return s.waitInLine(ctx, t)
	}
	w.unanswered, w.lastCause = false, ""
	ans := res.Answer
	switch ans.Answer {
	case gateAnswerGranted:
		return s.grantTask(ctx, t, ans)
	case gateAnswerQueued:
		moved := !w.placed || ans.Seq != w.ans.Seq || !ans.ArrivedAt.Equal(w.ans.ArrivedAt)
		w.placed, w.ans, w.rebuilding = true, ans, ans.Rebuilding
		if early := w.early; early != nil && early.Seq == ans.Seq {
			return s.grantTask(ctx, t, ans)
		}
		w.early = nil
		if moved {
			if err := s.recordTaskSlot(ctx, t, ans, false); err != nil {
				return err
			}
		}
		return s.waitInLine(ctx, t)
	case gateAnswerUnavailable:
		w.rebuilding = true
		return s.waitInLine(ctx, t)
	case gateAnswerFullBound:
		return s.endTaskWait(ctx, t, TaskConcurrencyFullCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED,
			fmt.Sprintf("the line for key %s is full", t.slot.Key))
	case gateAnswerFenced:
		return s.endTaskWait(ctx, t, TaskConcurrencyFencedCode, infra,
			fmt.Sprintf("the limit on key %s refused a stale or released request from this task", t.slot.Key))
	}
	return s.endTaskWait(ctx, t, TaskConcurrencyUnsupportedCode, infra,
		fmt.Sprintf("the limit on key %s answered %q, which this release does not know", t.slot.Key, ans.Answer))
}

// waitInLine carries a task's wait on: its place for the snapshot, TASK_QUEUED
// once, its deadline and its next re-assert.
func (s *runState) waitInLine(ctx workflow.Context, t *taskSlot) error {
	w, row := t.wait, s.taskRuns[t.id]
	q := row.GetQueued()
	if q == nil {
		q = &orkstackv1.ConcurrencyQueued{
			Key: t.slot.Key, Limit: int32(t.slot.Limit), Strategy: concStrategyProto(gateStrategyQueue),
			QueuedSince: timestamppb.New(w.req.IssuedAt), TimesOutAt: timestamppb.New(w.req.WaitUntil),
			Cause: orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL,
		}
		row.Queued = q
	}
	if w.placed && !w.unanswered {
		if q.GetPositionAtArrival() == 0 {
			q.PositionAtArrival, q.HoldersCount, q.HoldersAsOfArrival = int32(w.ans.Position), int32(w.ans.HoldersAhead), true
			q.QueuedSince = timestamppb.New(w.ans.ArrivedAt)
		}
		q.PositionKnown, q.Position, q.NoteCode, q.Note = true, int32(w.ans.Position), "", ""
	}
	if w.rebuilding {
		setRebuilding(q, true)
	}
	if w.unanswered {
		q.PositionKnown, q.Position, q.NoteCode = false, 0, QueuedNoteNotAnswering
		q.Note = "the limit is not answering; this task stays queued and asks again by " +
			workflow.Now(ctx).Add(w.interval).UTC().Format(time.RFC3339)
	}
	if err := s.syncConcAttributes(ctx); err != nil {
		return err
	}
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 2, 2*limit.Reserve.CommandBytes) // its timers
	}
	if w.deadline == nil {
		dctx, cancel := workflow.WithCancel(ctx)
		w.deadline, w.cancelDeadline = workflow.NewTimer(dctx, max(w.req.WaitUntil.Sub(workflow.Now(ctx)), time.Millisecond)), cancel
	}
	rctx, cancel := workflow.WithCancel(ctx)
	w.reassert, w.cancelReassert = workflow.NewTimer(rctx, w.interval), cancel
	if w.said {
		return nil
	}
	w.said = true
	until := w.req.WaitUntil.UTC().Format(time.RFC3339)
	text := fmt.Sprintf("queued for key %s (limit %d): ", t.slot.Key, t.slot.Limit)
	if q.GetPositionKnown() {
		text += fmt.Sprintf("position %d behind %d running; times out at %s", w.ans.Position, w.ans.HoldersAhead, until)
	} else {
		text += q.GetNote() + "; times out at " + until
	}
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED, t.id, row.GetStatus(), text)
}

// grantTask takes the granted slot: the task is dispatched at the loop's next
// pass, past a quiesce's hold and the cap, which counted its wait.
func (s *runState) grantTask(ctx workflow.Context, t *taskSlot, ans gateAnswer) error {
	s.noteTaskWaitEnd(ctx, t, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED)
	s.stopTaskWait(t)
	t.held, t.ans = true, ans
	s.taskRuns[t.id].Queued = nil
	return s.recordTaskSlot(ctx, t, ans, true)
}

// stopTaskWait ends t's wait in the run's own ledger: no answer, grant or timer
// of it is taken any more.
func (s *runState) stopTaskWait(t *taskSlot) {
	w := t.wait
	if w == nil {
		return
	}
	if w.cancelAsk != nil {
		w.cancelAsk()
	}
	s.stopTaskTimer(&w.deadline, &w.cancelDeadline)
	s.stopTaskTimer(&w.reassert, &w.cancelReassert)
	t.wait = nil
}

// recordTaskSlot issues §5.2.6's commands for the task's entry: its memo entry
// and the index attributes.
func (s *runState) recordTaskSlot(ctx workflow.Context, t *taskSlot, ans gateAnswer, held bool) error {
	c := s.conc
	entry := gateMemoEntry{Proto: gateProtoLegacy, Held: held, Limit: t.slot.Limit, Strategy: gateStrategyQueue, Label: t.slot.Label,
		ArrivedAt: ans.ArrivedAt, Seq: ans.Seq, SeqBase: c.seqBase}
	if t.n > 0 {
		entry.Proto, entry.N = gateProtoOrdinal, t.n
	}
	if !held {
		entry.WaitUntil = t.wait.req.WaitUntil
	}
	memo := map[string]any{gateMemoKey(t.slot.GateID, t.ix): entry}
	if !c.seqWritten {
		memo[gateMemoSeqKey] = c.seqBase
	}
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, memo)); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not record task %s's slot on key %s: %v", t.id, t.slot.Key, err))
	}
	c.seqWritten = true
	s.noteCommand(ctx, 2, c.plan.PerSlotBytes)
	return s.syncConcAttributes(ctx)
}

// releaseTaskSlot gives t's slot, or its place in line, back, in §22.14.7's
// order: the run's own ledger first, so nothing matches the entry any more,
// then the release with its ordinal, which also fences an acquire still on its
// way, the memo key and the attributes. Not waited for, on a context the run's
// own cancel does not reach; the gate's check pass frees what it could not.
func (s *runState) releaseTaskSlot(ctx workflow.Context, t *taskSlot) error {
	s.stopTaskWait(t)
	t.held = false
	s.taskRuns[t.id].Queued = nil
	s.noteCommand(ctx, 4, s.conc.plan.PerSlotBytes) // the signal, its answer, the memo and the attributes
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	workflow.SignalExternalWorkflow(dctx, t.slot.GateID, "", GateSignalRelease,
		// The gate is no run and holds no key: ids and indices only.
		payloadcrypt.PlainIn(runScope(ctx), GateRelease{Run: s.runID, SeqBase: s.conc.seqBase, TaskIx: t.ix, N: t.n}))
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{gateMemoKey(t.slot.GateID, t.ix): nil})); err != nil {
		// The index is still brought up to date: what the run no longer
		// holds is not listed as held.
		if serr := s.syncConcAttributes(ctx); serr != nil {
			workflow.GetLogger(ctx).Warn("a run could not update its concurrency index", "run", s.runID, "error", serr)
		}
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not drop task %s's slot's record: %v", t.id, err))
	}
	return s.syncConcAttributes(ctx)
}

// taskEnded releases a task's slot once its row is terminal; a task that runs
// again keeps it.
func (s *runState) taskEnded(ctx workflow.Context, id string) {
	t := s.taskSlot(id)
	if t == nil || !t.held || !isTerminal(s.taskRuns[id].GetStatus()) {
		return
	}
	if err := s.releaseTaskSlot(ctx, t); err != nil {
		workflow.GetLogger(ctx).Warn("a task's concurrency slot could not be dropped from the run's records; its release was sent",
			"run", s.runID, "task_id", id, "error", err)
	}
}

// dropTaskSlots gives back every slot asked for and not yet used: each place
// in line, each acquire in flight, and each slot granted to a task that has not
// been dispatched with it. A run that fails, is canceled or pauses keeps only
// what its running tasks hold; asked for again, a slot has its next ordinal. A
// task waiting out an interruption keeps its slot: what it ran may still run.
func (s *runState) dropTaskSlots(ctx workflow.Context) {
	if s.conc == nil {
		return
	}
	for _, t := range s.conc.tasks {
		pending := t.held && s.taskRuns[t.id].GetStatus() == orkstackv1.Status_STATUS_PENDING && !s.waiting[unitKey(t.id, -1)]
		if t.wait == nil && !pending {
			continue
		}
		// A grant given back before its dispatch ended no wait an event will
		// tell: the task's next one is of a wait to come.
		delete(s.taskWaitEnds, t.id)
		if err := s.releaseTaskSlot(ctx, t); err != nil {
			workflow.GetLogger(ctx).Warn("a waiting task's concurrency slot could not be dropped from the run's records; its release was sent",
				"run", s.runID, "task_id", t.id, "error", err)
		}
		t.n++
	}
}

// endTaskWait ends t's wait and its task with it: FAILED, with code, and what
// the run then fails with.
func (s *runState) endTaskWait(ctx workflow.Context, t *taskSlot, code string, category orkstackv1.FailureCategory, message string) error {
	s.noteTaskWaitEnd(ctx, t, taskWaitOutcome(code))
	if t.slot != nil {
		// The task ends for its own reason: a record the run could not drop
		// is the gate's check pass's to free, as taskEnded's is.
		if err := s.releaseTaskSlot(ctx, t); err != nil {
			workflow.GetLogger(ctx).Warn("a task's concurrency slot could not be dropped from the run's records; its release was sent",
				"run", s.runID, "task_id", t.id, "error", err)
		}
	}
	return s.failComposed(ctx, t.id, code, category, fmt.Sprintf("task %q: %s", t.id, message), "")
}

// noteTaskWaitEnd records how t's wait in line ended, for the task's next
// event (Event.wait_end, task_level): its TASK_SCHEDULED when granted, its
// TASK_FAILED when the wait failed it. Nothing when t was not in line: a
// slot granted at once, or refused on arrival, ended no wait.
func (s *runState) noteTaskWaitEnd(ctx workflow.Context, t *taskSlot, outcome orkstackv1.ConcurrencyWaitOutcome) {
	q := s.taskRuns[t.id].GetQueued()
	if q == nil || t.slot == nil {
		return
	}
	if s.taskWaitEnds == nil {
		s.taskWaitEnds = map[string]*orkstackv1.ConcurrencyWaitEnd{}
	}
	s.taskWaitEnds[t.id] = &orkstackv1.ConcurrencyWaitEnd{
		Outcome: outcome, Waited: durationpb.New(max(workflow.Now(ctx).Sub(q.GetQueuedSince().AsTime()), 0)),
		Strategy: q.GetStrategy(), LimitLabel: CensusLimitLabel(t.slot.Label), TaskLevel: true,
	}
}

// taskWaitOutcome is how a task's wait ended, by the code that failed it.
func taskWaitOutcome(code string) orkstackv1.ConcurrencyWaitOutcome {
	switch code {
	case TaskQueueTimeoutCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_TIMED_OUT
	case TaskConcurrencyFullCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FULL
	case TaskConcurrencyUnavailableCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNAVAILABLE
	case TaskConcurrencyUnsupportedCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNSUPPORTED
	case TaskConcurrencyFencedCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FENCED
	}
	return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED
}

// dispatchFailed gives back the slot of a task granted it whose dispatch then
// failed: the task never reaches the in-flight set, so nothing else would.
func (s *runState) dispatchFailed(ctx workflow.Context, id string) {
	t := s.taskSlot(id)
	if t == nil || !t.held {
		return
	}
	delete(s.taskWaitEnds, id)
	if err := s.releaseTaskSlot(ctx, t); err != nil {
		workflow.GetLogger(ctx).Warn("a task's concurrency slot could not be dropped from the run's records; its release was sent",
			"run", s.runID, "task_id", id, "error", err)
	}
}
