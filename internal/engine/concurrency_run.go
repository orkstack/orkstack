package engine

import (
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

// The run side of a concurrency limit (E88 §5.2): a run whose plan carries a
// run-level slot asks its gate for it after RUN_CREATED and before it
// starts, waits in line when the key is full, and releases every slot it
// asked for on every exit but a continue-as-new. Every path is entered only
// by a plan with Concurrency set, which no history recorded before limits
// holds, so no GetVersion is needed (§5.2.1; AGENTS.md §7.2, first clause).

// The run-level endings of a concurrency limit (§5.2.2, §5.2.3).
const (
	RunConcurrencyLimitCode       = "RUN_CONCURRENCY_LIMIT"
	RunConcurrencyFullCode        = "RUN_CONCURRENCY_FULL"
	RunConcurrencyUnavailableCode = "RUN_CONCURRENCY_UNAVAILABLE"
	RunConcurrencyFencedCode      = "RUN_CONCURRENCY_FENCED"
	RunConcurrencyUnsupportedCode = "RUN_CONCURRENCY_UNSUPPORTED"
	RunQueueTimeoutCode           = "RUN_QUEUE_TIMEOUT"
	RunSupersededCode             = "RUN_SUPERSEDED"
)

// heldSlot is a run-level slot the run holds: its gate and the seq it was
// granted at, carried across a continue-as-new (§5.2.7).
type heldSlot struct {
	gate string
	seq  int64
}

// concRunState is the run's concurrency state. Nil on a run with no limit.
type concRunState struct {
	plan *ConcurrencyPlan
	// runID is the run's id, for its releases.
	runID string
	// attempted is every gate the run asked, in order: what the release
	// covers.
	attempted []string
	// held is the run-level slot the run holds, nil before a grant.
	held *heldSlot
	// queued is the run's place in line while it waits, for its snapshot;
	// waitStarted is when that wait began, on the run's clock.
	queued      *orkstackv1.ConcurrencyQueued
	waitStarted time.Time
	// attempt numbers the acquires the run sends, for their update ids.
	attempt int
	// seqWritten: orkflow_conc_seq is in the memo.
	seqWritten bool
	// invalid is why the plan's limit cannot be acted on, "" when it can.
	invalid string
	// ordinal is the run-level slot's ordinal n (§22.15): how many times the
	// run gave it back and asked for it again. givenBack: a paused run gave
	// the slot back and has not been granted it again; requeue is its wait
	// for it after an unpause (concurrency_pause.go).
	ordinal   int64
	givenBack bool
	requeue   *requeueWait
	// pausable: the run's plan carries Pause, the only way a slot is given
	// back.
	pausable bool
}

func newConcRunState(plan *ConcurrencyPlan) *concRunState {
	if plan == nil || (plan.Run == nil) {
		// No run-level limit (task-level limits come with E88 P7).
		return nil
	}
	c := &concRunState{plan: plan}
	if plan.runSlot() == nil {
		// A plan that names a run-level slot it does not have: refused at
		// the acquire, never run unlimited.
		c.invalid = fmt.Sprintf("the run's plan names run-level slot %d of %d", *plan.Run, len(plan.Slots))
	}
	return c
}

// acquireRunSlot asks for the run-level slot and, when the key is full,
// waits for it (§5.2.2, §5.2.3). It returns nil once the run holds it, a
// run-level error for every ending the limit decides, and a canceled error
// when the run is canceled while it waits.
func (s *runState) acquireRunSlot(ctx workflow.Context) error {
	c := s.conc
	if c.invalid != "" {
		return runLevelError(RunConcurrencyUnsupportedCode, c.invalid)
	}
	slot := c.plan.runSlot()
	now := workflow.Now(ctx)
	timeout := time.Duration(slot.QueueTimeoutSeconds) * time.Second
	req := gateAcquire{
		Proto: gateProtoLegacy, Run: s.runID, SeqBase: c.plan.SeqBase, TaskIx: gateRunLevel,
		Limit: slot.Limit, Strategy: slot.Strategy, Label: slot.Label,
		WaitUntil: now.Add(timeout), IssuedAt: now,
	}
	c.noteAttempted(slot.GateID)
	res, err := s.sendAcquire(ctx, slot, req)
	if err != nil {
		return err
	}
	if ending := concEnding(res, slot); ending != nil {
		return ending
	}
	switch res.Answer.Answer {
	case gateAnswerGranted:
		return s.recordRunSlot(ctx, slot, res.Answer, true)
	case gateAnswerQueued:
		return s.waitForRunSlot(ctx, slot, req, res.Answer, orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL, nil)
	}
	return runLevelError(RunConcurrencyUnsupportedCode, fmt.Sprintf("the limit on key %s answered %q, which this release does not know", slot.Key, res.Answer.Answer))
}

func (c *concRunState) noteAttempted(gate string) {
	for _, g := range c.attempted {
		if g == gate {
			return
		}
	}
	c.attempted = append(c.attempted, gate)
}

// sendAcquire runs one AcquireConcurrency local activity for req.
func (s *runState) sendAcquire(ctx workflow.Context, slot *ConcurrencySlotPlan, req gateAcquire) (AcquireConcurrencyResult, error) {
	c := s.conc
	c.attempt++
	// Retried within the acquire's time: a worker that stopped mid-acquire
	// cancels the activity, which then runs again rather than failing the
	// run.
	lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: c.plan.AcquireTimeout,
		RetryPolicy:            &temporal.RetryPolicy{InitialInterval: c.plan.AcquireBackoffMin, MaximumInterval: c.plan.AcquireBackoffMax},
	})
	in := AcquireConcurrencyInput{
		Orkflow: c.plan.Orkflow, GateID: slot.GateID, GateQueue: c.plan.GateQueue, Request: req,
		UpdateID:   acquireUpdateID(req.Run, req.SeqBase, req.TaskIx, req.N, c.attempt),
		BackoffMin: c.plan.AcquireBackoffMin, BackoffMax: c.plan.AcquireBackoffMax,
		Start: GateStart{GateID: slot.GateID, Namespace: c.plan.Namespace, Incarnation: c.plan.Incarnation,
			Key: slot.Key, Tunables: c.plan.Gate},
	}
	var res AcquireConcurrencyResult
	err := getLocal(lctx, runScope(ctx), workflow.ExecuteLocalActivity(lctx, ActivityAcquireConcurrency, in), &res)
	if err == nil {
		return res, nil
	}
	if categoryOf(err).canceled || ctx.Err() != nil {
		return res, temporal.NewCanceledError()
	}
	// The activity ran out of its acquire_timeout, or failed past its
	// retries: the limit is unavailable, which the caller decides from.
	return AcquireConcurrencyResult{Unavailable: err.Error()}, nil
}

// concEnding is the run-level ending an acquire's result decides, nil for a
// grant or a place in line (§5.2.2).
func concEnding(res AcquireConcurrencyResult, slot *ConcurrencySlotPlan) error {
	switch {
	case res.Unsupported != "":
		return runLevelError(RunConcurrencyUnsupportedCode, fmt.Sprintf("the limit on key %s cannot take this run's request (cause=protocol): %s", slot.Key, res.Unsupported))
	case res.Unavailable != "":
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the limit on key %s did not answer (%s); submit the run again", slot.Key, res.Unavailable))
	}
	switch res.Answer.Answer {
	case gateAnswerGranted, gateAnswerQueued:
		return nil
	case gateAnswerFull:
		return runLevelError(RunConcurrencyLimitCode, fmt.Sprintf("key %s already has its limit of %d runs", slot.Key, slot.Limit))
	case gateAnswerFullBound:
		return runLevelError(RunConcurrencyFullCode, fmt.Sprintf("the line for key %s is full; submit the run again later", slot.Key))
	case gateAnswerUnavailable:
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the limit on key %s is rebuilding its record; submit the run again", slot.Key))
	case gateAnswerFenced:
		return runLevelError(RunConcurrencyFencedCode, fmt.Sprintf("the limit on key %s refused a stale or released request from this run", slot.Key))
	case gateAnswerSuperseded:
		return runLevelError(RunSupersededCode, fmt.Sprintf("run %s replaced this run on key %s", res.Answer.By, slot.Key))
	}
	return runLevelError(RunConcurrencyUnsupportedCode, fmt.Sprintf("the limit on key %s answered %q, which this release does not know", slot.Key, res.Answer.Answer))
}

// waitForRunSlot is the queued run's wait (§5.2.3): RUN_QUEUED, its records,
// then a grant, a re-assert, its deadline or its cancel, whichever comes
// first.
//
// cause says why the run waits: it arrived (PENDING), or, failing, it asks
// again for the slot it gave back while paused so its failure handlers run
// inside the limit (FAILING, RUNNING; failingRegain). abort, when set, ends
// the wait with errWaitAborted when it is ready: the run timeout's hard end
// of a failing run.
func (s *runState) waitForRunSlot(ctx workflow.Context, slot *ConcurrencySlotPlan, req gateAcquire, ans gateAnswer,
	cause orkstackv1.ConcurrencyQueueCause, abort workflow.Future) error {
	c := s.conc
	c.waitStarted = workflow.Now(ctx)
	c.queued = &orkstackv1.ConcurrencyQueued{
		Key: slot.Key, Limit: int32(slot.Limit), Strategy: concStrategyProto(req.Strategy),
		PositionAtArrival: int32(ans.Position), Position: int32(ans.Position), PositionKnown: true,
		HoldersCount: int32(ans.HoldersAhead), HoldersAsOfArrival: true,
		QueuedSince: timestamppb.New(ans.ArrivedAt), TimesOutAt: timestamppb.New(req.WaitUntil),
		Cause: cause,
	}
	setRebuilding(c.queued, ans.Rebuilding)
	if err := s.recordRunSlot(ctx, slot, ans, false); err != nil {
		return err
	}
	status, text := orkstackv1.Status_STATUS_PENDING, "queued for key %s (limit %d): position %d behind %d running; times out at %s"
	if cause == orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_FAILING {
		status, text = s.status, "waiting again for key %s (limit %d) to run its failure handlers: position %d behind %d running; times out at %s"
	}
	message := fmt.Sprintf(text, slot.Key, slot.Limit, ans.Position, ans.HoldersAhead, req.WaitUntil.UTC().Format(time.RFC3339))
	if ans.Rebuilding {
		// Its place is not known yet: the line is being read back.
		message = c.queued.GetNote() + "; times out at " + req.WaitUntil.UTC().Format(time.RFC3339)
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "", status, message); err != nil {
		return err
	}
	grants := workflow.GetSignalChannel(ctx, ConcurrencyGrantSignal)
	// The deadline and the re-assert timer live on contexts of their own,
	// cancelled at every return, a grant's included, so a run that left the
	// line keeps no timer pending.
	dctx, cancelDeadline := workflow.WithCancel(ctx)
	defer cancelDeadline()
	timerCtx := dctx
	if concDeadlineLeftRunning {
		timerCtx = ctx
	}
	deadline := workflow.NewTimer(timerCtx, max(req.WaitUntil.Sub(workflow.Now(ctx)), time.Millisecond))
	interval := c.plan.RequeueInterval
	rebuilding, unanswered := ans.Rebuilding, false
	var reassert workflow.Future
	var cancelReassert workflow.CancelFunc = func() {}
	defer func() { cancelReassert() }()
	for {
		// One re-assert timer at a time, kept across a grant that is not
		// this run's, so signals never put the re-assert off.
		if reassert == nil {
			var tctx workflow.Context
			tctx, cancelReassert = workflow.WithCancel(ctx)
			reassert = workflow.NewTimer(tctx, interval)
		}
		var (
			grant          ConcurrencyGrant
			granted, timed bool
			tick, canceled bool
		)
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(grants, func(ch workflow.ReceiveChannel, _ bool) {
			ch.Receive(ctx, &grant)
			granted = grant.Gate == slot.GateID && grant.Run == s.runID && grant.TaskIx == gateRunLevel &&
				grant.SeqBase == req.SeqBase && grant.Seq == ans.Seq && grant.N == req.N
			if !granted {
				workflow.GetLogger(ctx).Warn("a queued run ignored a grant that is not its entry's",
					"run", s.runID, "gate", slot.GateID, "grant", grant, "seq", ans.Seq)
			}
		})
		sel.AddFuture(deadline, func(workflow.Future) { timed = true })
		sel.AddFuture(reassert, func(workflow.Future) { tick = true })
		sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { canceled = true })
		aborted := false
		if abort != nil {
			sel.AddFuture(abort, func(workflow.Future) { aborted = true })
		}
		sel.Select(ctx)
		switch {
		case canceled || ctx.Err() != nil:
			return temporal.NewCanceledError()
		case aborted:
			return errWaitAborted
		case granted:
			return s.grantRunSlot(ctx, slot, ans)
		case timed:
			if rebuilding || unanswered {
				return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the limit on key %s was not answering or was rebuilding its record when this run's wait ended", slot.Key))
			}
			return runLevelError(RunQueueTimeoutCode, fmt.Sprintf("this run waited %s for key %s and timed out", req.WaitUntil.Sub(ans.ArrivedAt).Round(time.Second), slot.Key))
		case tick:
			reassert = nil
			// Re-assert at the place the gate gave: it heals a lost grant,
			// puts back a lost entry, or ends the run (§5.2.4).
			again := req
			again.IssuedAt, again.ArrivedAt, again.Seq = workflow.Now(ctx), ans.ArrivedAt, ans.Seq
			res, err := s.sendAcquire(ctx, slot, again)
			if err != nil {
				return err
			}
			if res.Unavailable != "" {
				// Counted, not ignored: a wait that ends with its limit
				// unreachable ends unavailable, never timed out.
				unanswered = true
				workflow.GetLogger(ctx).Warn("a queued run's re-assert was not answered; it keeps waiting",
					"run", s.runID, "gate", slot.GateID, "cause", res.Unavailable)
			} else {
				unanswered = false
				if ending := concEnding(res, slot); ending != nil {
					return ending
				}
				if res.Answer.Answer == gateAnswerGranted {
					return s.grantRunSlot(ctx, slot, res.Answer)
				}
				rebuilding = res.Answer.Rebuilding
				if res.Answer.Seq != ans.Seq || !res.Answer.ArrivedAt.Equal(ans.ArrivedAt) {
					// The gate put the entry back under a new seq: the run's
					// grant now names that one.
					ans.Seq, ans.ArrivedAt = res.Answer.Seq, res.Answer.ArrivedAt
					if err := s.recordRunSlot(ctx, slot, ans, false); err != nil {
						return err
					}
				}
				c.queued.Position = int32(res.Answer.Position)
				setRebuilding(c.queued, rebuilding)
			}
			interval = min(time.Duration(c.plan.RequeueBackoffFactor)*interval, c.plan.RequeueIntervalMax)
		}
	}
}

// setRebuilding marks a queued run's place unknown, with its note, while
// its limit reads which runs hold its key, and known again once it has.
func setRebuilding(q *orkstackv1.ConcurrencyQueued, rebuilding bool) {
	switch {
	case rebuilding:
		q.PositionKnown, q.Position = false, 0
		q.NoteCode, q.Note = QueuedNoteRebuilding, rebuildingNote(q.GetKey())
	case q.GetNoteCode() == QueuedNoteRebuilding:
		q.PositionKnown = true
		q.NoteCode, q.Note = "", ""
	}
}

// errWaitAborted ends a wait for a slot its abort future ended.
var errWaitAborted = errors.New("the wait for the concurrency slot was ended")

// grantRunSlot takes the granted slot: its records move from queued to held.
func (s *runState) grantRunSlot(ctx workflow.Context, slot *ConcurrencySlotPlan, ans gateAnswer) error {
	s.noteWaitEnd(ctx, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED)
	s.conc.queued = nil
	return s.recordRunSlot(ctx, slot, ans, true)
}

// recordRunSlot issues §5.2.6's commands for the run-level slot, in one
// workflow task: its memo entry (and, once, the incarnation), and the index
// attributes that list its gate as held or queued.
func (s *runState) recordRunSlot(ctx workflow.Context, slot *ConcurrencySlotPlan, ans gateAnswer, held bool) error {
	c := s.conc
	entry := gateMemoEntry{Proto: gateProtoLegacy, Held: held, Limit: slot.Limit, Strategy: slot.Strategy, Label: slot.Label,
		ArrivedAt: ans.ArrivedAt, Seq: ans.Seq, SeqBase: c.plan.SeqBase}
	if c.ordinal > 0 {
		// A slot asked for again after a give-back: a rebuilding gate
		// restores the entry with its ordinal (§22.15).
		entry.Proto, entry.N, entry.Strategy = gateProtoOrdinal, c.ordinal, gateStrategyQueue
	}
	if !held && c.queued != nil {
		entry.WaitUntil = c.queued.GetTimesOutAt().AsTime()
	}
	memo := map[string]any{gateMemoKey(slot.GateID, gateRunLevel): entry}
	if !c.seqWritten {
		memo[gateMemoSeqKey] = c.plan.SeqBase
	}
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, memo)); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not record its slot on key %s: %v", slot.Key, err))
	}
	c.seqWritten = true
	if held {
		c.held = &heldSlot{gate: slot.GateID, seq: ans.Seq}
	}
	return s.syncConcAttributes(ctx)
}

// syncConcAttributes upserts the run's concurrency attributes from its
// state: the gates it holds and waits on, and whether it is itself queued.
func (s *runState) syncConcAttributes(ctx workflow.Context) error {
	c := s.conc
	held, queued := []string{}, []string{}
	if c.held != nil {
		held = append(held, c.held.gate)
	}
	if c.queued != nil {
		queued = append(queued, c.plan.runSlot().GateID)
	}
	runQueued := task.SearchKeyOrkflowRunQueued.ValueUnset()
	if c.queued != nil {
		runQueued = task.SearchKeyOrkflowRunQueued.ValueSet("true")
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx,
		task.SearchKeyOrkflowConcurrencyHeld.ValueSet(held),
		task.SearchKeyOrkflowConcurrencyQueued.ValueSet(queued),
		runQueued); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not index its concurrency slots: %v", err))
	}
	return nil
}

// restoreConcRecords re-issues the held slot's records after a carried
// generation adopted its state (§5.2.7): the memo and attributes a new
// execution does not inherit. It emits nothing.
func (s *runState) restoreConcRecords(ctx workflow.Context) error {
	c := s.conc
	if c == nil || c.held == nil {
		return nil
	}
	slot := c.plan.runSlot()
	memo := map[string]any{
		gateMemoKey(c.held.gate, gateRunLevel): c.heldMemoEntry(slot),
		gateMemoSeqKey:                         c.plan.SeqBase,
	}
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, memo)); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not record its carried slot: %v", err))
	}
	c.seqWritten = true
	return s.syncConcAttributes(ctx)
}

// heldMemoEntry is the memo entry of the run-level slot the run holds.
func (c *concRunState) heldMemoEntry(slot *ConcurrencySlotPlan) gateMemoEntry {
	entry := gateMemoEntry{Proto: gateProtoLegacy, Held: true, Limit: slot.Limit,
		Strategy: slot.Strategy, Label: slot.Label, Seq: c.held.seq, SeqBase: c.plan.SeqBase}
	if c.ordinal > 0 {
		entry.Proto, entry.N, entry.Strategy = gateProtoOrdinal, c.ordinal, gateStrategyQueue
	}
	return entry
}

// releaseSlots is the central release (§5.2.5): release_all{run, below
// seq_base+1} to every gate the run asked, on a context the run's own
// cancel does not reach. A gate that is gone is a release done; any other
// failure is logged, and the gate's own check pass frees the slot. It never
// starts a gate, and it is bounded: one signal per gate.
func releaseSlots(ctx workflow.Context, c *concRunState) {
	if c == nil || len(c.attempted) == 0 {
		return
	}
	runID, scope := c.runID, runScope(ctx)
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	futures := make([]workflow.Future, 0, len(c.attempted))
	for _, gate := range c.attempted {
		futures = append(futures, workflow.SignalExternalWorkflow(dctx, gate, "", GateSignalReleaseAll,
			// The gate is no run and holds no key: ids and a sequence only.
			payloadcrypt.PlainIn(scope, GateReleaseAll{Run: runID, Below: c.plan.SeqBase + 1})))
	}
	for i, f := range futures {
		if err := f.Get(dctx, nil); err != nil && !categoryOf(err).unknownRun {
			workflow.GetLogger(ctx).Warn("a run could not release its concurrency slot; the limit's check pass frees it",
				"run", runID, "gate", c.attempted[i], "error", err)
		}
	}
}

// concQueuedProto is the run's place in line for its snapshot, nil when it
// is not waiting.
func (s *runState) concQueuedProto() *orkstackv1.ConcurrencyQueued {
	if s.conc == nil {
		return nil
	}
	return s.conc.queued
}

func concStrategyProto(s string) orkstackv1.ConcurrencyStrategy {
	switch s {
	case gateStrategyQueue:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE
	case gateStrategyRefuse:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE
	case gateStrategySkip:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP
	}
	return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_UNSPECIFIED
}

// concCarry is the run's slots as it carries on: only held run-level slots
// cross, because a queued run never switches (§5.2.7).
func (c *concRunState) carry() *orkstackv1.RunConcurrencyCarry {
	if c == nil {
		return nil
	}
	out := &orkstackv1.RunConcurrencyCarry{Attempted: append([]string(nil), c.attempted...)}
	if c.held != nil {
		out.Held = []*orkstackv1.HeldSlot{{GateId: c.held.gate, Seq: c.held.seq}}
	}
	if c.ordinal > 0 {
		out.Ordinals = []*orkstackv1.SlotOrdinal{{GateId: c.plan.runSlot().GateID, TaskIx: gateRunLevel, N: c.ordinal}}
	}
	return out
}

// seedAttempted takes the gates a predecessor asked, at once, so every exit
// of this generation releases them, a failed restore's included.
func (c *concRunState) seedAttempted(in *orkstackv1.RunConcurrencyCarry) {
	if c == nil || in == nil {
		return
	}
	for _, g := range in.GetAttempted() {
		c.noteAttempted(g)
	}
	for _, h := range in.GetHeld() {
		c.noteAttempted(h.GetGateId())
	}
}

// adoptCarry takes the slots a predecessor carried.
func (c *concRunState) adoptCarry(in *orkstackv1.RunConcurrencyCarry) error {
	if c == nil || in == nil {
		return nil
	}
	for _, g := range in.GetAttempted() {
		c.noteAttempted(g)
	}
	switch ordinals := in.GetOrdinals(); len(ordinals) {
	case 0:
	case 1:
		o := ordinals[0]
		if o.GetGateId() != c.plan.runSlot().GateID || o.GetTaskIx() != gateRunLevel || o.GetN() <= 0 {
			return fmt.Errorf("the carried slot ordinal %v is not this run's run-level slot's", o)
		}
		c.ordinal = o.GetN()
	default:
		return fmt.Errorf("the carry holds %d run-level slot ordinals; a run has one", len(ordinals))
	}
	switch held := in.GetHeld(); len(held) {
	case 0:
		// A run carries on only after it started, which it does holding its
		// slot: one that holds none gave it back while paused (§22.15),
		// which only a run that can be paused does.
		if !c.pausable {
			return errors.New("the carry holds no run-level slot, and a run that cannot be paused never gives one back")
		}
		c.givenBack = true
	case 1:
		if held[0].GetGateId() != c.plan.runSlot().GateID {
			return errors.New("the carried concurrency slot is not this run's run-level gate")
		}
		c.held = &heldSlot{gate: held[0].GetGateId(), seq: held[0].GetSeq()}
	default:
		return fmt.Errorf("the carry holds %d run-level slots; a run holds one", len(held))
	}
	return nil
}

// failInLine ends a run that never held its run-level slot: one whose wait
// in line ended, or that its gate refused. It starts no task, failure
// handlers included, because a handler would run outside the limit the run
// waited for; the run ends FAILED or CANCELED with its own reason, as a run
// without handlers does. Reached only with a plan that carries a limit, so
// no history recorded before one replays it.
func (s *runState) failInLine(ctx workflow.Context, cancelRun workflow.CancelFunc, inFlight map[string]workflow.Future, cause error) (*orkstackv1.Run, error) {
	// It no longer waits: its record and index stop saying so. A failed
	// upsert is logged, never put in place of the run's own reason.
	if c := s.conc; c != nil && c.queued != nil {
		s.noteWaitEnd(ctx, waitOutcomeOf(ctx, cause))
		c.queued = nil
		if err := s.syncConcAttributes(ctx); err != nil {
			workflow.GetLogger(ctx).Warn("a run that left the line could not update its index", "run_id", s.runID, "error", err)
		}
	}
	s.dropHandlersOutsideLimit()
	// It never started: a schedule's fire tells this ending from one a run
	// that ran ended with (fireOutcome). A supersede ends it canceled.
	if code := concCauseCode(cause); code != "" && code != RunSupersededCode {
		read := categoryOf(cause)
		cause = temporal.NewNonRetryableApplicationError(read.appMessage(), code, nil, ConcurrencyRunNotStarted{NotStarted: true})
	}
	return s.fail(ctx, cancelRun, inFlight, "", cause)
}

// noteWaitEnd records how the run's wait in line ended, for the next event
// it emits (Event.wait_end): the one that ends the wait. Nothing when the run
// is not waiting.
func (s *runState) noteWaitEnd(ctx workflow.Context, outcome orkstackv1.ConcurrencyWaitOutcome) {
	c := s.conc
	if c == nil || c.queued == nil {
		return
	}
	slot := c.plan.runSlot()
	if slot == nil {
		return
	}
	s.eventWaitEnd = &orkstackv1.ConcurrencyWaitEnd{
		Outcome: outcome, Waited: durationpb.New(max(workflow.Now(ctx).Sub(c.waitStarted), 0)),
		Strategy: c.queued.GetStrategy(), LimitLabel: CensusLimitLabel(slot.Label),
	}
}

// takeEventWaitEnd hands the noted wait end to the event being built, once:
// never to a task's log line, which an ending emits before its terminal
// event.
func (s *runState) takeEventWaitEnd(eventType orkstackv1.EventType) *orkstackv1.ConcurrencyWaitEnd {
	if eventType == orkstackv1.EventType_EVENT_TYPE_TASK_LOG {
		return nil
	}
	w := s.eventWaitEnd
	s.eventWaitEnd = nil
	return w
}

// waitOutcomeOf is how an ending cut a wait short: the limit's own reason,
// or canceled for any other (the run's cancel, its own failure).
func waitOutcomeOf(ctx workflow.Context, cause error) orkstackv1.ConcurrencyWaitOutcome {
	if ctx.Err() != nil {
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED
	}
	switch concCauseCode(cause) {
	case RunQueueTimeoutCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_TIMED_OUT
	case RunConcurrencyFullCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FULL
	case RunConcurrencyUnavailableCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNAVAILABLE
	case RunConcurrencyUnsupportedCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNSUPPORTED
	case RunConcurrencyFencedCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FENCED
	case RunSupersededCode:
		return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_SUPERSEDED
	}
	return orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED
}

// ConcurrencyRunNotStarted is the detail of a run-level concurrency ending
// a run met before it started: in line for its slot, or refused it.
type ConcurrencyRunNotStarted struct {
	NotStarted bool `json:"not_started"`
}

// dropHandlersOutsideLimit ends a run without its failure handlers: it holds
// no slot of its run-level limit, so a handler would run outside it. The run
// returns from fail at once, so clearing this derived field is never read by
// anything after it.
func (s *runState) dropHandlersOutsideLimit() {
	if s.hasHandlers {
		s.hasHandlers = false
		// The handlers' context issues no command; ending it is tidiness.
		if s.cancelHandlers != nil {
			s.cancelHandlers()
		}
	}
}

// concCauseCode is the code of a run-level ending of a concurrency limit, or
// "": as runTimeoutCauseCode, only an error this workflow's own code raised.
func concCauseCode(cause error) string {
	failure := categoryOf(cause)
	if !failure.isApplication() || failure.viaActivity || failure.viaChild {
		return ""
	}
	switch code := failure.appType(); code {
	case RunConcurrencyLimitCode, RunConcurrencyFullCode, RunConcurrencyUnavailableCode,
		RunConcurrencyFencedCode, RunConcurrencyUnsupportedCode, RunQueueTimeoutCode, RunSupersededCode:
		return code
	}
	return ""
}

// concFailure is the run's error for a concurrency ending: its own code and
// sentence, and the category that says what kind of ending it was.
func concFailure(cause error) *orkstackv1.Error {
	read := categoryOf(cause)
	failure := &orkstackv1.Error{Code: read.appType(), Message: read.appMessage()}
	switch failure.Code {
	case RunQueueTimeoutCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	case RunConcurrencyUnavailableCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	case RunSupersededCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
	case RunConcurrencyFencedCode, RunConcurrencyUnsupportedCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	default:
		// A key at its limit, or a full line, is the limit doing its work,
		// no failure of a kind (docs/REASONS.md).
	}
	return failure
}

// supersededClose reads a canceled close's recorded details where only
// the close is at hand: RUN_SUPERSEDED and its sentence, as categoryOf reads
// them off the error (superseded).
func supersededClose(details *commonpb.Payloads) (string, bool) {
	payloads := details.GetPayloads()
	if len(payloads) < 2 {
		return "", false
	}
	var code, sentence string
	dc := DataConverter()
	if dc.FromPayload(payloads[0], &code) != nil || code != RunSupersededCode || dc.FromPayload(payloads[1], &sentence) != nil {
		return "", false
	}
	return sentence, true
}

// concDeadlineLeftRunning starts a waiting run's deadline timer on the run's
// own context, so a grant leaves it pending, as the run side did before its
// deadline was cancelled at the grant. It is false in every build and set
// only by TestConcurrencyQueuedHistory_CatchesADeadlineLeftRunning, which
// flips it to prove the queued fixture pins the cancel:
// sleepTimerAfterEmit's precedent.
var concDeadlineLeftRunning = false
