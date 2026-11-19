package engine

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
	// queued is the run's place in line while it waits, for its snapshot.
	queued *orkstackv1.ConcurrencyQueued
	// attempt numbers the acquires the run sends, for their update ids.
	attempt int
	// seqWritten: orkflow_conc_seq is in the memo.
	seqWritten bool
	// invalid is why the plan's limit cannot be acted on, "" when it can.
	invalid string
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
		return s.waitForRunSlot(ctx, slot, req, res.Answer)
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
		GateID: slot.GateID, GateQueue: c.plan.GateQueue, Request: req,
		UpdateID:   acquireUpdateID(req.Run, req.SeqBase, req.TaskIx, req.N, c.attempt),
		BackoffMin: c.plan.AcquireBackoffMin, BackoffMax: c.plan.AcquireBackoffMax,
		Start: GateStart{GateID: slot.GateID, Namespace: c.plan.Namespace, Incarnation: c.plan.Incarnation,
			Key: slot.Key, Tunables: c.plan.Gate},
	}
	var res AcquireConcurrencyResult
	err := workflow.ExecuteLocalActivity(lctx, ActivityAcquireConcurrency, in).Get(lctx, &res)
	if err == nil {
		return res, nil
	}
	if temporal.IsCanceledError(err) || ctx.Err() != nil {
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
func (s *runState) waitForRunSlot(ctx workflow.Context, slot *ConcurrencySlotPlan, req gateAcquire, ans gateAnswer) error {
	c := s.conc
	c.queued = &orkstackv1.ConcurrencyQueued{
		Key: slot.Key, Limit: int32(slot.Limit), Strategy: concStrategyProto(slot.Strategy),
		PositionAtArrival: int32(ans.Position), Position: int32(ans.Position), PositionKnown: true,
		HoldersCount: int32(ans.HoldersAhead), HoldersAsOfArrival: true,
		QueuedSince: timestamppb.New(ans.ArrivedAt), TimesOutAt: timestamppb.New(req.WaitUntil),
		Cause: orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL,
	}
	if err := s.recordRunSlot(ctx, slot, ans, false); err != nil {
		return err
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "", orkstackv1.Status_STATUS_PENDING,
		fmt.Sprintf("queued for key %s (limit %d): position %d behind %d running; times out at %s",
			slot.Key, slot.Limit, ans.Position, ans.HoldersAhead, req.WaitUntil.UTC().Format(time.RFC3339))); err != nil {
		return err
	}
	grants := workflow.GetSignalChannel(ctx, ConcurrencyGrantSignal)
	// The deadline and the re-assert timer live on contexts of their own,
	// cancelled at every return, a grant's included, so a run that left the
	// line keeps no timer pending.
	dctx, cancelDeadline := workflow.WithCancel(ctx)
	defer cancelDeadline()
	deadline := workflow.NewTimer(dctx, max(req.WaitUntil.Sub(workflow.Now(ctx)), time.Millisecond))
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
		sel.Select(ctx)
		switch {
		case canceled || ctx.Err() != nil:
			return temporal.NewCanceledError()
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
			}
			interval = min(time.Duration(c.plan.RequeueBackoffFactor)*interval, c.plan.RequeueIntervalMax)
		}
	}
}

// grantRunSlot takes the granted slot: its records move from queued to held.
func (s *runState) grantRunSlot(ctx workflow.Context, slot *ConcurrencySlotPlan, ans gateAnswer) error {
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
	if !held && c.queued != nil {
		entry.WaitUntil = c.queued.GetTimesOutAt().AsTime()
	}
	memo := map[string]any{gateMemoKey(slot.GateID, gateRunLevel): entry}
	if !c.seqWritten {
		memo[gateMemoSeqKey] = c.plan.SeqBase
	}
	if err := workflow.UpsertMemo(ctx, memo); err != nil {
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
		gateMemoKey(c.held.gate, gateRunLevel): gateMemoEntry{Proto: gateProtoLegacy, Held: true, Limit: slot.Limit,
			Strategy: slot.Strategy, Label: slot.Label, Seq: c.held.seq, SeqBase: c.plan.SeqBase},
		gateMemoSeqKey: c.plan.SeqBase,
	}
	if err := workflow.UpsertMemo(ctx, memo); err != nil {
		return runLevelError(RunConcurrencyUnavailableCode, fmt.Sprintf("the run could not record its carried slot: %v", err))
	}
	c.seqWritten = true
	return s.syncConcAttributes(ctx)
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
	runID := c.runID
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	futures := make([]workflow.Future, 0, len(c.attempted))
	for _, gate := range c.attempted {
		futures = append(futures, workflow.SignalExternalWorkflow(dctx, gate, "", GateSignalReleaseAll,
			GateReleaseAll{Run: runID, Below: c.plan.SeqBase + 1}))
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
	switch held := in.GetHeld(); len(held) {
	case 0:
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
