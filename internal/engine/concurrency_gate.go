package engine

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// ConcurrencyGateWorkflow holds one key's slots (E88 §5.1, AGENTS.md §2.1 as
// amended by E88 P1): the runs and tasks holding or queued for them are its
// state, every change is an Update or a Signal Temporal serializes, and it
// exists only while the key has something held or queued. It decides only
// when an accepted run or task may begin. It is a thin shell over
// concurrency_gate_state.go and concurrency_gate_liveness.go, which hold
// every rule; this file turns their answers into signals, timers and local
// activities.

// The gate's workflow type and internal names (§5.1.3). None appears on a
// user surface.
const (
	ConcurrencyGateWorkflowType = "ConcurrencyGateWorkflow"
	GateUpdateAcquire           = "acquire"
	GateSignalRelease           = "release"
	GateSignalReleaseAll        = "release_all"
	GateSignalVerify            = "verify"
	GateQueryState              = "state"
	// ConcurrencyGrantSignal is what a gate sends a run whose queued entry it
	// granted.
	ConcurrencyGrantSignal = "ork-concurrency-grant"
)

// The gate's local activities (§5.1.8). Both run on the engine worker that
// runs the workflow task, never on ork-tasks.
const (
	ActivityDescribeConcurrencyEntries = "DescribeConcurrencyEntries"
	ActivityListConcurrencyEntries     = "ListConcurrencyEntries"
)

// The gate's error types, as an acquiring run reads them.
const (
	// GateErrUnsupported is a protocol rejection: never retried
	// (CONCURRENCY_UNSUPPORTED, cause=protocol).
	GateErrUnsupported = "CONCURRENCY_UNSUPPORTED"
	// GateErrBusy is a draining or frozen rejection, retried with backoff.
	GateErrBusy = "CONCURRENCY_GATE_BUSY"
)

// The start's previous execution, as AcquireConcurrency read it (§5.1.1).
const (
	GatePreviousClean   = "clean"
	GatePreviousAbsent  = "absent"
	GatePreviousUnclean = "unclean"
)

// GateStart is a gate's input: the key it holds, its tunables, how its
// previous execution ended, and, on a continue-as-new, the state it carries.
type GateStart struct {
	GateID      string       `json:"gate_id"`
	Namespace   string       `json:"namespace"`
	Incarnation string       `json:"incarnation"`
	Key         string       `json:"key"`
	Tunables    GateTunables `json:"tunables"`
	Previous    string       `json:"previous,omitempty"`
	Carry       *gateState   `json:"carry,omitempty"`
}

// GateResult is a gate's completion: it closed empty (§5.1.7).
type GateResult struct {
	Closed string `json:"closed"`
	Epoch  int64  `json:"epoch"`
	Seq    int64  `json:"seq"`
}

// GateRelease and GateReleaseAll are the release signals' payloads.
type GateRelease struct {
	Run     string `json:"run"`
	SeqBase int64  `json:"seq_base"`
	TaskIx  int    `json:"task_ix"`
	// N is the slot's ordinal (§22.14.1), sent only above 0, after a
	// protocol 2 acquire.
	N int64 `json:"n,omitempty"`
}

type GateReleaseAll struct {
	Run   string `json:"run"`
	Below int64  `json:"below"`
}

// ConcurrencyGrant is the grant signal's payload.
type ConcurrencyGrant struct {
	Gate    string `json:"gate"`
	Run     string `json:"run"`
	TaskIx  int    `json:"task_ix"`
	SeqBase int64  `json:"seq_base"`
	Seq     int64  `json:"seq"`
	// N is the granted entry's ordinal (§22.15), omitted at 0, so a grant
	// with none matches only a run's n = 0.
	N int64 `json:"n,omitempty"`
}

// DescribeConcurrencyEntriesInput and ListConcurrencyEntriesInput are the
// local activities' requests; ListConcurrencyEntriesResult is one page.
type DescribeConcurrencyEntriesInput struct {
	GateID   string   `json:"gate_id"`
	Runs     []string `json:"runs"`
	Parallel int      `json:"parallel"`
}

type ListConcurrencyEntriesInput struct {
	GateID    string `json:"gate_id"`
	PageToken []byte `json:"page_token,omitempty"`
}

type ListConcurrencyEntriesResult struct {
	Records   []gateRunRecord `json:"records"`
	NextToken []byte          `json:"next_token,omitempty"`
}

// gateRun is one execution of the gate: its state and what its handlers and
// main loop share. Every field is touched only from workflow coroutines.
type gateRun struct {
	in       GateStart
	g        *gateState
	wake     workflow.Channel
	updates  int
	signals  int // grant signal commands awaiting their result
	passWant bool
	passing  bool
	passSeq  int
	// emptySince is when the gate last became empty; zero while it holds
	// or queues anything.
	emptySince time.Time
	// ordinal is the concurrency-gate-ordinal decision, asked the first time
	// an acquire or a release carries a slot ordinal: 0 until then.
	ordinal workflow.Version
}

// GateOrdinalChange marks the first decision a gate makes on a slot
// ordinal (E88 P3, D1). An acquire or release with n above 0 is the only way
// into the ordinal rules; no history recorded before them holds one, so
// every such history replays its DefaultVersion branch, which reads n as 0
// as a gate before them did.
const GateOrdinalChange = "concurrency-gate-ordinal"

// ordinalOf is n as this gate's history decides it: the ordinal itself, or
// 0 on a history whose ordinal decision is DefaultVersion.
func (w *gateRun) ordinalOf(ctx workflow.Context, n int64) int64 {
	if n == 0 {
		return 0
	}
	if w.ordinal == 0 {
		// DefaultVersion is -1, so 0 stays "not asked yet".
		w.ordinal = workflow.GetVersion(ctx, GateOrdinalChange, workflow.DefaultVersion, 1)
	}
	if w.ordinal < 1 {
		return 0
	}
	return n
}

func (w *gateRun) poke() { w.wake.SendAsync(struct{}{}) }

// GateRebuildFirstChange marks a gate that is rebuilding from its first
// instant: an acquire delivered with its start (Update-with-Start) is
// decided after the gate knows it is rebuilding, so it queues instead of
// being granted past a holder the rebuild is about to restore. Asked only on
// a start that rebuilds, before any handler is registered; a history
// recorded before it replays DefaultVersion, where the state was set when
// the rebuild began.
const GateRebuildFirstChange = "concurrency-gate-rebuild-first"

// gateRebuildNotFirst leaves a rebuilding start's state unset until its
// rebuild begins, as a gate did before GateRebuildFirstChange. It is false in
// every build and set only by TestGateRebuildFirstHistory_CatchesALateRebuildState,
// which flips it to prove the rebuild-first fixture pins the change:
// sleepTimerAfterEmit's precedent.
var gateRebuildNotFirst = false

// ConcurrencyGateWorkflow is the gate's body.
func ConcurrencyGateWorkflow(ctx workflow.Context, in GateStart) (GateResult, error) {
	w := &gateRun{in: in, g: in.Carry, wake: workflow.NewBufferedChannel(ctx, 1)}
	// A clean predecessor closed empty, so there is nothing to rebuild. Any
	// other start (absent, unclean, or unstated) rebuilds, and starts a new
	// epoch; only an unclean one also settles (§5.1.6, G8).
	rebuild := false
	if w.g != nil {
		// A carry the gate's own modulus cannot order would be misread, so
		// it fails the gate explicitly; the next start rebuilds.
		if err := w.g.checkOrdinals(); err != nil {
			return GateResult{}, temporal.NewNonRetryableApplicationError(err.Error(), "CONCURRENCY_GATE_CARRY_INVALID", nil)
		}
	}
	if w.g == nil {
		w.g = &gateState{Tunables: in.Tunables, Phase: gatePhaseActive}
		if in.Previous != GatePreviousClean {
			rebuild = true
			w.g.Epoch++
			if workflow.GetVersion(ctx, GateRebuildFirstChange, workflow.DefaultVersion, 1) >= 1 && !gateRebuildNotFirst {
				// Rebuilding before the acquire handler exists, so no acquire
				// is ever decided by a gate that does not yet know it.
				w.g.Rebuild = gateRebuild{State: gateRebuildListing, Since: workflow.Now(ctx)}
			}
		}
	}
	w.g.Draining = false
	if w.g.LastPassAt.IsZero() {
		// The sweep's clock starts with the execution.
		w.g.LastPassAt = workflow.Now(ctx)
	}
	if err := w.register(ctx); err != nil {
		return GateResult{}, err
	}
	if rebuild {
		if err := w.rebuild(ctx, in.Previous != GatePreviousAbsent); err != nil {
			return GateResult{}, err
		}
	}
	if w.g.empty() {
		w.emptySince = workflow.Now(ctx)
	}
	// A grant a predecessor could not deliver before it drained.
	w.resendPending(ctx)
	return w.loop(ctx)
}

func (w *gateRun) register(ctx workflow.Context) error {
	if err := workflow.SetUpdateHandlerWithOptions(ctx, GateUpdateAcquire, w.acquire, workflow.UpdateHandlerOptions{
		Validator: func(ctx workflow.Context, req gateAcquire) error {
			return gateRejectionError(w.g.validateAcquire(req, workflow.Now(ctx)))
		},
	}); err != nil {
		return fmt.Errorf("register %s: %w", GateUpdateAcquire, err)
	}
	if err := workflow.SetQueryHandler(ctx, GateQueryState, func() (GateStateView, error) {
		return GateStateView{Namespace: w.in.Namespace, Key: w.in.Key, State: *w.g}, nil
	}); err != nil {
		return fmt.Errorf("register %s: %w", GateQueryState, err)
	}
	// Each receiver waits until a signal is buffered and then takes it, never
	// blocking in Receive: the SDK hands a signal straight to a blocked
	// Receive, where a drain could not see it. Buffered, every delivered
	// signal is visible to drainSignals in whatever order coroutines run.
	for _, name := range []string{GateSignalRelease, GateSignalReleaseAll, GateSignalVerify} {
		ch := workflow.GetSignalChannel(ctx, name)
		workflow.Go(ctx, func(ctx workflow.Context) {
			for {
				if err := workflow.Await(ctx, func() bool { return ch.Len() > 0 }); err != nil {
					return
				}
				w.applySignal(ctx, name, ch)
			}
		})
	}
	return nil
}

// applySignal takes one buffered signal from ch, if there is one, and
// applies it; it reports whether there was one. A release naming no run,
// or no incarnation, is logged and ignored: it can free nothing and would
// only spend a tombstone.
func (w *gateRun) applySignal(ctx workflow.Context, name string, ch workflow.ReceiveChannel) bool {
	receive := func(v any) bool { return ch.ReceiveAsync(v) }
	now := func() time.Time { return workflow.Now(ctx) }
	switch name {
	case GateSignalRelease:
		var r GateRelease
		if !receive(&r) {
			return false
		}
		if r.Run == "" || r.SeqBase < 1 || r.TaskIx < gateRunLevel || r.N < 0 || (r.N > 0 && r.N >= w.g.Tunables.OrdinalModulus) {
			workflow.GetLogger(ctx).Warn("a concurrency gate ignored a malformed release", "gate", w.in.GateID, "release", r)
			return true
		}
		w.g.release(r.Run, r.SeqBase, r.TaskIx, w.ordinalOf(ctx, r.N), now())
	case GateSignalReleaseAll:
		var r GateReleaseAll
		if !receive(&r) {
			return false
		}
		if r.Run == "" || r.Below < 1 {
			workflow.GetLogger(ctx).Warn("a concurrency gate ignored a malformed release_all", "gate", w.in.GateID, "release_all", r)
			return true
		}
		w.g.releaseAll(r.Run, r.Below, now())
	case GateSignalVerify:
		if !receive(nil) {
			return false
		}
		w.passWant = true
	}
	w.poke()
	return true
}

// drainSignals applies every signal already delivered, so none is left
// behind by a continue-as-new or a close.
func (w *gateRun) drainSignals(ctx workflow.Context) {
	for _, name := range []string{GateSignalRelease, GateSignalReleaseAll, GateSignalVerify} {
		ch := workflow.GetSignalChannel(ctx, name)
		for w.applySignal(ctx, name, ch) {
		}
	}
}

// gateRejectionError turns a validator's refusal into the error type an
// acquiring run tells apart.
func gateRejectionError(err error) error {
	var rej *gateRejection
	if !errors.As(err, &rej) {
		return err
	}
	if rej.Protocol {
		return temporal.NewNonRetryableApplicationError(rej.Reason, GateErrUnsupported, nil)
	}
	return temporal.NewApplicationError(rej.Reason, GateErrBusy)
}

// acquire is the Update handler (§5.1.4). A full key with a check due waits
// for one pass, at most verify_await, and decides again.
func (w *gateRun) acquire(ctx workflow.Context, req gateAcquire) (gateAnswer, error) {
	w.updates++
	req.N = w.ordinalOf(ctx, req.N)
	reassert := !req.ArrivedAt.IsZero() && req.Seq > 0
	d := w.g.decideAcquire(req, workflow.Now(ctx), false)
	// A replacement admitted waiters from the line; they are granted by
	// signal, as the loop grants any head. Once replaced, the acquire is a
	// new arrival: a decision after a pass must not take it back to the
	// place it re-asserts.
	w.grant(ctx, d.Admitted)
	if d.Replaced {
		req.ArrivedAt, req.Seq = time.Time{}, 0
		reassert = false
	}
	if d.WantsPass && d.Answer.Answer == "" {
		mine := w.passSeq
		w.passWant = true
		w.poke()
		if _, err := workflow.AwaitWithTimeout(ctx, w.g.Tunables.VerifyAwait, func() bool { return w.passSeq > mine }); err != nil {
			return gateAnswer{}, err
		}
		d = w.g.decideAcquire(req, workflow.Now(ctx), true)
		w.grant(ctx, d.Admitted)
	} else if d.WantsPass || (reassert && w.passDebounced(workflow.Now(ctx))) {
		w.passWant = true
	}
	w.poke()
	return d.Answer, nil
}

// loop runs housekeeping on every change and timer until the gate carries
// its state on or closes.
func (w *gateRun) loop(ctx workflow.Context) (GateResult, error) {
	var timer workflow.Future
	var timerAt time.Time
	var cancelTimer workflow.CancelFunc
	for {
		if err := ctx.Err(); err != nil {
			// Canceled from outside: an unclean end, which the next start
			// rebuilds from.
			return GateResult{}, temporal.NewCanceledError()
		}
		now := workflow.Now(ctx)
		if w.g.Rebuild.State == gateRebuildSettling && !w.g.settling(now) {
			w.g.Rebuild.State = gateRebuildNone // the settle is over
		}
		w.g.dropExpiredQueued(now)
		w.grant(ctx, w.g.admitHeads(now))
		if err := w.syncPhase(ctx); err != nil {
			return GateResult{}, err
		}
		if w.passDue(now) {
			w.pass(ctx)
			continue
		}
		if w.continueDue(ctx) {
			return GateResult{}, w.continueAsNew(ctx)
		}
		if w.g.empty() {
			if w.emptySince.IsZero() {
				w.emptySince = now
			}
			if !now.Before(w.emptySince.Add(w.g.Tunables.Linger)) {
				result, closed, err := w.close(ctx)
				if err != nil {
					return GateResult{}, err
				}
				if closed {
					return result, nil
				}
				continue
			}
		} else {
			w.emptySince = time.Time{}
		}
		at := w.nextWake(now)
		if timer == nil || !at.Equal(timerAt) {
			if cancelTimer != nil {
				cancelTimer()
			}
			tctx, cancel := workflow.WithCancel(ctx)
			timer, timerAt, cancelTimer = workflow.NewTimer(tctx, at.Sub(now)), at, cancel
		}
		fired := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(w.wake, func(c workflow.ReceiveChannel, _ bool) { c.ReceiveAsync(nil) })
		sel.AddFuture(timer, func(workflow.Future) { fired = true })
		sel.Select(ctx)
		if fired {
			timer, cancelTimer = nil, nil
		}
	}
}

// nextWake is the earliest instant something is due: a sweep, the settle's
// end, a freeze's end, a queued entry's deadline, or the linger's end.
func (w *gateRun) nextWake(now time.Time) time.Time {
	g := w.g
	at := now.Add(g.Tunables.HolderSweepInterval)
	consider := func(t time.Time) {
		if !t.IsZero() && t.After(now) && t.Before(at) {
			at = t
		}
	}
	if len(g.Holders) > 0 {
		last := g.LastPassAt
		if last.IsZero() {
			last = now
		}
		consider(last.Add(g.Tunables.HolderSweepInterval))
	}
	if g.Rebuild.State == gateRebuildSettling {
		consider(g.Rebuild.Since.Add(g.Tunables.RebuildSettle))
	}
	consider(g.FrozenUntil)
	for _, q := range g.Queue {
		consider(q.WaitUntil)
	}
	if g.empty() && !w.emptySince.IsZero() {
		consider(w.emptySince.Add(g.Tunables.Linger))
	}
	return at
}

// passDue reports that a check pass should run now: one was asked for, or
// the sweep interval passed with holders.
func (w *gateRun) passDue(now time.Time) bool {
	if w.g.empty() {
		w.passWant = false
		return false
	}
	if w.passWant {
		return true
	}
	return len(w.g.Holders) > 0 && !w.g.LastPassAt.IsZero() && !now.Before(w.g.LastPassAt.Add(w.g.Tunables.HolderSweepInterval))
}

// passDebounced reports that no pass ran within verify_debounce, so a
// re-assert may ask for one (§5.1.4).
func (w *gateRun) passDebounced(now time.Time) bool {
	return now.Sub(w.g.LastPassAt) >= w.g.Tunables.VerifyDebounce
}

// pass describes every entry's run, verify_batch runs per local activity,
// and applies what it found (§5.1.5). A batch whose describe fails changes
// nothing for its runs; the next pass retries them.
func (w *gateRun) pass(ctx workflow.Context) {
	w.passWant, w.passing = false, true
	// Stamped at the start, so acquires arriving during a long pass are
	// debounced against it rather than starting another; applyFindings
	// stamps it again at the end.
	w.g.LastPassAt = workflow.Now(ctx)
	defer func() { w.passing = false; w.passSeq++ }()
	lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: w.g.Tunables.VerifyTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var findings []gateRunFinding
	failed := 0
	for batch := range slices.Chunk(w.passRuns(), max(1, w.g.Tunables.VerifyBatch)) {
		var got []gateRunFinding
		if err := workflow.ExecuteLocalActivity(lctx, ActivityDescribeConcurrencyEntries, DescribeConcurrencyEntriesInput{
			GateID: w.in.GateID, Runs: batch, Parallel: w.g.Tunables.VerifyParallel,
		}).Get(lctx, &got); err != nil {
			failed += len(batch)
			workflow.GetLogger(ctx).Warn("a concurrency gate could not check a batch of its entries; the next pass retries them",
				"gate", w.in.GateID, "runs", len(batch), "error", err)
			continue
		}
		findings = append(findings, got...)
	}
	now := workflow.Now(ctx)
	out := w.g.applyFindings(findings, now)
	w.g.dropExpiredQueued(now)
	if out.Errors > 0 || failed > 0 {
		first := ""
		for _, f := range findings {
			if f.Err != "" {
				first = f.Err
				break
			}
		}
		workflow.GetLogger(ctx).Warn("a concurrency gate kept entries it could not check",
			"gate", w.in.GateID, "unchecked_runs", failed, "describe_errors", out.Errors, "first_error", first)
	}
	if out.Released > 0 || out.Marked > 0 || out.Stalled > 0 {
		workflow.GetLogger(ctx).Info("a concurrency gate's check pass",
			"gate", w.in.GateID, "released", out.Released, "marked", out.Marked, "stalled", out.Stalled)
	}
}

// passRuns is the runs a pass describes, each once: holders first, then
// the line.
func (w *gateRun) passRuns() []string {
	var runs []string
	seen := map[string]bool{}
	for _, e := range slices.Concat(w.g.Holders, w.g.Queue) {
		if !seen[e.Run] {
			seen[e.Run] = true
			runs = append(runs, e.Run)
		}
	}
	return runs
}

// grant signals each newly granted run (§5.1.4 "Grant"), by workflow id so
// the signal reaches its current execution across continue-as-new (G10).
// The holder stays grant_pending until a signal is delivered. A run the
// cluster does not know is released and fenced; any other failure retries
// with grant_backoff while the entry still holds, until the gate drains:
// then the mark is carried and the next execution sends it again, so a
// drain never waits on a retry.
func (w *gateRun) grant(ctx workflow.Context, granted []gateEntry) {
	for _, h := range granted {
		payload := ConcurrencyGrant{Gate: w.in.GateID, Run: h.Run, TaskIx: h.TaskIx, SeqBase: h.SeqBase, Seq: h.Seq, N: h.N}
		// Counted before the coroutine runs, so a drain in the same
		// workflow task waits for this signal's first attempt.
		w.signals++
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer w.poke()
			backoff := w.g.Tunables.GrantBackoffMin
			for attempt := 0; ; attempt++ {
				if attempt > 0 {
					w.signals++
				}
				err := workflow.SignalExternalWorkflow(ctx, payload.Run, "", ConcurrencyGrantSignal, payload).Get(ctx, nil)
				w.signals--
				i := w.g.holderIx(payload.Run, payload.SeqBase, payload.TaskIx)
				switch {
				case i < 0 || w.g.Holders[i].N != payload.N:
					// Released, or replaced by a newer ordinal whose own
					// grant is its own coroutine's.
					return
				case err == nil:
					w.g.Holders[i].GrantPending = false
					return
				case categoryOf(err).unknownRun:
					w.g.release(payload.Run, payload.SeqBase, payload.TaskIx, payload.N, workflow.Now(ctx))
					return
				}
				workflow.GetLogger(ctx).Warn("a concurrency grant could not be delivered; retrying",
					"gate", w.in.GateID, "run", payload.Run, "error", err)
				drained, waitErr := workflow.AwaitWithTimeout(ctx, backoff, func() bool { return w.g.Draining })
				if waitErr != nil || drained {
					workflow.GetLogger(ctx).Info("a concurrency grant is left pending for the next execution",
						"gate", w.in.GateID, "run", payload.Run)
					return
				}
				backoff = min(2*backoff, w.g.Tunables.GrantBackoffMax)
			}
		})
	}
}

// resendPending sends again every grant still pending: one a predecessor
// could not deliver, or one a drain stopped that a close then lifted.
func (w *gateRun) resendPending(ctx workflow.Context) {
	var pending []gateEntry
	for _, h := range w.g.Holders {
		if h.GrantPending {
			pending = append(pending, h)
		}
	}
	w.grant(ctx, pending)
}

// syncPhase upserts OrkflowGatePhase when the gate's phase changed.
func (w *gateRun) syncPhase(ctx workflow.Context) error {
	phase := w.g.phaseNow()
	if phase == w.g.Phase {
		return nil
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, task.SearchKeyOrkflowGatePhase.ValueSet(phase)); err != nil {
		return fmt.Errorf("upsert the gate's phase: %w", err)
	}
	w.g.Phase = phase
	return nil
}

// continueDue reports the gate should carry its state on (§5.1.7).
func (w *gateRun) continueDue(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() ||
		info.GetCurrentHistoryLength() > w.g.Tunables.ContinueEvents ||
		w.updates > w.g.Tunables.ContinueUpdates
}

// drain stops new acquires, waits for every grant signal and pass, then for
// the handlers already running, at most drainBound (§5.1.7). A handler still
// running then loses its update to the continuation or the close, which the
// acquiring run retries. Only a cancellation is an error.
func (w *gateRun) drain(ctx workflow.Context) error {
	w.g.Draining = true
	if err := workflow.Await(ctx, func() bool { return w.signals == 0 && !w.passing }); err != nil {
		return err
	}
	if _, err := workflow.AwaitWithTimeout(ctx, w.g.Tunables.drainBound(), func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	w.drainSignals(ctx)
	return nil
}

func (w *gateRun) continueAsNew(ctx workflow.Context) error {
	if err := w.drain(ctx); err != nil {
		return err
	}
	carry := *w.g
	carry.Draining = false
	next := w.in
	next.Previous, next.Carry = "", &carry
	return workflow.NewContinueAsNewError(ctx, ConcurrencyGateWorkflowType, next)
}

// close completes the gate if it is still empty after the drain; otherwise
// it lifts the drain and carries on.
func (w *gateRun) close(ctx workflow.Context) (GateResult, bool, error) {
	if err := w.drain(ctx); err != nil {
		return GateResult{}, false, err
	}
	if !w.g.empty() {
		w.g.Draining = false
		w.emptySince = time.Time{}
		w.resendPending(ctx)
		return GateResult{}, false, nil
	}
	return GateResult{Closed: "empty", Epoch: w.g.Epoch, Seq: w.g.Seq}, true, nil
}

// rebuild restores the line from the open runs' records (§5.1.6), retrying
// with backoff while grants stay off; past rebuild_timeout it is failing and
// still retries. settle is whether grants then wait out rebuild_settle.
func (w *gateRun) rebuild(ctx workflow.Context, settle bool) error {
	g := w.g
	g.Rebuild = gateRebuild{State: gateRebuildListing, Since: workflow.Now(ctx)}
	backoff := g.Tunables.RebuildBackoffMin
	for {
		records, err := w.listAll(ctx)
		now := workflow.Now(ctx)
		if err == nil {
			out := g.restore(records, now)
			workflow.GetLogger(ctx).Info("a concurrency gate rebuilt its record",
				"gate", w.in.GateID, "restored", out.Restored, "adopted", out.Adopted,
				"stale_skipped", out.Stale, "released_while_listing", out.Fenced)
			g.Rebuild.Attempts++
			if settle {
				g.Rebuild = gateRebuild{State: gateRebuildSettling, Since: now, Attempts: g.Rebuild.Attempts}
			} else {
				g.Rebuild = gateRebuild{Attempts: g.Rebuild.Attempts}
			}
			return nil
		}
		g.Rebuild.Attempts++
		if now.Sub(g.Rebuild.Since) >= g.Tunables.RebuildTimeout && g.Rebuild.State != gateRebuildFailing {
			g.Rebuild.State = gateRebuildFailing
			workflow.GetLogger(ctx).Error("a concurrency gate is past rebuild_timeout and still cannot rebuild; it grants nothing until it does",
				"gate", w.in.GateID, "attempts", g.Rebuild.Attempts, "error", err)
		}
		workflow.GetLogger(ctx).Warn("a concurrency gate could not rebuild its record; grants stay off until it does",
			"gate", w.in.GateID, "attempts", g.Rebuild.Attempts, "state", g.Rebuild.State, "error", err)
		if err := workflow.Sleep(ctx, backoff); err != nil {
			return err
		}
		backoff = min(2*backoff, g.Tunables.RebuildBackoffMax)
	}
}

// listAll reads every page of the open runs that hold or queue at this
// gate. More entries than max_entries fails the attempt: a truncated
// restore could grant past a holder it never saw (G9).
func (w *gateRun) listAll(ctx workflow.Context) ([]gateRunRecord, error) {
	lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: w.g.Tunables.RebuildPageTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var all []gateRunRecord
	var token []byte
	entries := 0
	for {
		var page ListConcurrencyEntriesResult
		if err := workflow.ExecuteLocalActivity(lctx, ActivityListConcurrencyEntries, ListConcurrencyEntriesInput{
			GateID: w.in.GateID, PageToken: token,
		}).Get(lctx, &page); err != nil {
			return nil, err
		}
		for _, r := range page.Records {
			entries += len(r.Entries)
		}
		if entries > w.g.Tunables.MaxEntries {
			return nil, fmt.Errorf("the open runs hold or queue %d entries at this gate, more than max_entries (%d)", entries, w.g.Tunables.MaxEntries)
		}
		all = append(all, page.Records...)
		if len(page.NextToken) == 0 {
			return all, nil
		}
		token = page.NextToken
	}
}
