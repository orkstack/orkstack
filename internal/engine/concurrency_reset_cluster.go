package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The states of a reset (§5.1.11).
const (
	ResetStateDryRun      = "dry_run"
	ResetStateRunning     = "running"
	ResetStateCompleted   = "completed"
	ResetStateFailed      = "failed"
	ResetStateInterrupted = "interrupted"
	ResetStateNotApplied  = "not_applied"
)

// ConcurrencyReset is a request to reset one key's limit. RequestID and
// IssuedAt together name it: the same request asked again acts once.
type ConcurrencyReset struct {
	Namespace, Incarnation, Key string
	RequestID                   string
	IssuedAt                    time.Time
	ExpectedEpoch               *int64
	DryRun                      bool
	ForceUnanswered             bool
}

func (r ConcurrencyReset) gateID() string { return GateID(r.Namespace, r.Incarnation, r.Key) }

// request is r as the gate decides it: what it asks is in the digest, so
// another request under one id is told apart.
func (r ConcurrencyReset) request() gateReset {
	epoch := "any"
	if r.ExpectedEpoch != nil {
		epoch = strconv.FormatInt(*r.ExpectedEpoch, 10)
	}
	return gateReset{ID: r.RequestID, IssuedAt: r.IssuedAt.UTC(), Body: gateDigest("reset", epoch, strconv.FormatBool(r.ForceUnanswered))[:resetBodyMax]}
}

// ConcurrencyResetResult is what a reset, its repeat or its dry run found.
type ConcurrencyResetResult struct {
	State                    string
	Epoch, NewEpoch          int64
	Holders, Queued          int
	Dropped, Adopted, Seeded int
	ConsequenceCode          string
	Consequence              string
	Code, Reason, Note       string
	// Started: this call started the reset, rather than finding it started.
	Started bool
}

// ConcurrencyResetError is a reset that is refused. Retryable: the same
// request may be asked again as it is.
type ConcurrencyResetError struct {
	Code      string
	Reason    string
	Retryable bool
}

func (e *ConcurrencyResetError) Error() string { return e.Reason }

// ConcurrencyResetter resets a key's limit.
type ConcurrencyResetter interface {
	ResetConcurrencyLimit(ctx context.Context, in ConcurrencyReset) (ConcurrencyResetResult, error)
}

// ConcurrencyResetSettings are what a reset is made with (§9.6).
type ConcurrencyResetSettings struct {
	// Tunables start the gate of a key that has none, or whose gate does not
	// answer; a gate that answers keeps its own.
	Tunables GateTunables
	// FreezeMargin is how long before a freeze ends a reset no longer starts
	// the gate that replaces the frozen one.
	FreezeMargin time.Duration
}

// ResetMemoryLife is how long after a gate closes its memory of the resets it
// applied may still be read: the cluster keeps a closed execution at least
// this long (archive.RetentionController.WithKeyLife), so a reset that could
// still act is never forgotten with the execution that remembers it (CR-1).
func ResetMemoryLife(t GateTunables) time.Duration { return t.ResetWindow + resetClockAhead }

// TemporalConcurrencyResetter is ConcurrencyResetter over the cluster.
type TemporalConcurrencyResetter struct {
	client     client.Client
	activities *GateActivities
	reader     *TemporalConcurrencyReader
	// namespace and identity are the cluster's, for the start this makes
	// itself: the SDK's own sets no request id.
	namespace, identity, queue string
	settings                   ConcurrencyResetSettings
	observer                   ResetObserver
	logger                     *slog.Logger
	now                        func() time.Time
	callTimeout                time.Duration
}

// NewTemporalConcurrencyResetter resets gates that run on queue, in the
// cluster's namespace. observer may be nil.
func NewTemporalConcurrencyResetter(c client.Client, namespace, identity, queue string, settings ConcurrencyResetSettings,
	observer ResetObserver, logger *slog.Logger) *TemporalConcurrencyResetter {
	if logger == nil {
		logger = slog.Default()
	}
	return &TemporalConcurrencyResetter{client: c, activities: NewGateActivities(c), reader: NewTemporalConcurrencyReader(c, 0),
		namespace: namespace, identity: identity, queue: queue, settings: settings, observer: observer, logger: logger,
		now: time.Now, callTimeout: 10 * time.Second}
}

// resetAttempts bounds how often one call looks again at a limit that changed
// under it: one that closed, started or began to answer.
const resetAttempts = 3

// errLookAgain: the limit changed while it was looked at.
var errLookAgain = errors.New("the limit changed while it was read")

// gateLook is one look at a key's gate.
type gateLook struct {
	// state is the state it answered; nil when it is absent or did not.
	state *gateState
	// open: an execution is running.
	open bool
	// runID and memo are its latest execution's, "" and nil when the key
	// never had one the cluster still keeps. Read only when state is nil.
	runID string
	memo  *gateResetMemo
}

// forgets reports that the limit cannot know every reset applied to it.
func (l gateLook) forgets(now time.Time) bool {
	switch {
	case l.state != nil:
		return now.Before(l.state.ResetsUnknownUntil)
	case l.memo != nil:
		return l.memo.Unreadable || now.Before(l.memo.UnknownUntil)
	}
	return false
}

// ResetConcurrencyLimit implements ConcurrencyResetter (§5.1.11).
func (t *TemporalConcurrencyResetter) ResetConcurrencyLimit(ctx context.Context, in ConcurrencyReset) (ConcurrencyResetResult, error) {
	in.Namespace = artifacts.ResolveNamespace(in.Namespace)
	req := in.request()
	for range resetAttempts {
		look, err := t.look(ctx, in.gateID())
		if errors.Is(err, errLookAgain) {
			continue
		}
		if err != nil {
			return ConcurrencyResetResult{}, err
		}
		if !in.DryRun {
			if res, known, err := reportKnown(look, req); known || err != nil {
				return res, err
			}
			if res, done, err := t.tooLate(look, req); done || err != nil {
				return res, err
			}
		}
		var res ConcurrencyResetResult
		var again bool
		switch {
		case look.state != nil:
			res, again, err = t.resetAnswering(ctx, in, req, *look.state)
		case look.open:
			res, again, err = t.resetUnanswered(ctx, in, req, look)
		default:
			res, again, err = t.resetAbsent(ctx, in, req, look)
		}
		if err != nil || !again {
			return res, err
		}
	}
	return ConcurrencyResetResult{}, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true,
		Reason: "the limit changed while it was being reset; ask again"}
}

// look asks the gate for its state, and reads its latest execution when it
// does not answer. A cluster that cannot be read is an error: it says nothing
// of the gate.
func (t *TemporalConcurrencyResetter) look(ctx context.Context, gateID string) (gateLook, error) {
	view, present, queryErr := t.reader.gateState(ctx, gateID)
	if queryErr == nil && present {
		return gateLook{state: &view.State}, nil
	}
	dctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()
	previous, err := t.activities.previousExecution(dctx, gateID)
	if err != nil {
		return gateLook{}, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: "the limit could not be read; ask again"}
	}
	if queryErr == nil && previous.open {
		// One started between the two reads: it was never asked, so it is
		// not one that does not answer.
		return gateLook{}, errLookAgain
	}
	if previous.open {
		t.logger.Warn("reset concurrency limit: the limit did not answer", "limit", gateID, "error", queryErr)
	}
	look := gateLook{open: previous.open, runID: previous.runID, memo: previous.resets}
	if look.memo != nil && look.memo.Unreadable && !previous.closedAt.IsZero() {
		// What it applied cannot be read, and is past its window a fixed time
		// after it closed, whenever it is looked at.
		look.memo = &gateResetMemo{UnknownUntil: previous.closedAt.Add(ResetMemoryLife(t.settings.Tunables))}
	}
	return look, nil
}

// reportKnown is the answer to a request the limit has applied already, and
// whether it has: told from what the gate remembers, or what its latest
// execution's memo says when it does not answer. Another request under an
// applied id is refused.
func reportKnown(look gateLook, req gateReset) (ConcurrencyResetResult, bool, error) {
	var resets []gateReset
	var res ConcurrencyResetResult
	var current, failing, answering bool
	switch {
	case look.state != nil:
		resets, res.Epoch, res.NewEpoch = look.state.Resets, look.state.Epoch, look.state.Epoch
		res.Holders, res.Queued = len(look.state.Holders), len(look.state.Queue)
		current, failing, answering = look.state.ResetID == req.ID, look.state.Rebuild.State == gateRebuildFailing, true
	case look.memo != nil:
		resets, current = look.memo.Resets, look.open && look.memo.Current == req.ID
	}
	i := slices.IndexFunc(resets, func(r gateReset) bool { return r.ID == req.ID })
	switch {
	case i < 0:
		return ConcurrencyResetResult{}, false, nil
	case !resets[i].same(req):
		return ConcurrencyResetResult{}, true, &ConcurrencyResetError{Code: ResetRefusedCode, Reason: "another reset with this request id has already been applied"}
	case resets[i].Done:
		res.State = ResetStateCompleted
	case current && failing:
		res.State, res.Code = ResetStateFailed, ResetFailedCode
		res.Reason = "the limit's record could not be rebuilt: the runs that hold or wait for it could not be read; the limit keeps trying"
	case current:
		res.State = ResetStateRunning
		if !answering {
			res.Note = "the limit is not answering yet"
		}
	default:
		res.State, res.Code = ResetStateInterrupted, ResetInterruptedCode
		res.Reason = "the reset was applied, but the limit was started again before it finished"
	}
	return res, true, nil
}

// tooLate decides by this process's clock alone what the limit need not be
// asked: a request no gate could take, and one whose first action did not come
// in time, which reports that it never acted. It only refuses early; the gate
// decides age by its own clock.
func (t *TemporalConcurrencyResetter) tooLate(look gateLook, req gateReset) (ConcurrencyResetResult, bool, error) {
	g := gateState{Tunables: t.settings.Tunables}
	now := t.now()
	if err := g.checkReset(req, now); err != nil {
		return ConcurrencyResetResult{}, true, &ConcurrencyResetError{Code: ResetRefusedCode, Reason: err.Error()}
	}
	if now.After(req.IssuedAt.Add(t.settings.Tunables.FreezeTimeout + resetClockAhead)) {
		if look.forgets(now) {
			return ConcurrencyResetResult{}, true, &ConcurrencyResetError{Code: ResetRefusedCode,
				Reason: "the limit cannot know whether this reset was applied to its earlier record; start a new one once it can"}
		}
		return notApplied(), true, nil
	}
	return ConcurrencyResetResult{}, false, nil
}

func notApplied() ConcurrencyResetResult {
	return ConcurrencyResetResult{State: ResetStateNotApplied, Code: ResetNotAppliedCode,
		Reason: "the reset never took effect; start a new reset",
		Note:   "its first step came too long after it was issued: if a new reset ends the same way, check the client's clock"}
}

// conflict refuses a reset that expected another epoch.
func conflict(in ConcurrencyReset, epoch int64) error {
	if in.ExpectedEpoch == nil || *in.ExpectedEpoch == epoch {
		return nil
	}
	return &ConcurrencyResetError{Code: ResetConflictCode,
		Reason: fmt.Sprintf("the limit changed since epoch %d was read; read it again", *in.ExpectedEpoch)}
}

// resetAnswering resets a gate that answers: frozen, judged, and started again
// from what it held, in place of the frozen one.
func (t *TemporalConcurrencyResetter) resetAnswering(ctx context.Context, in ConcurrencyReset, req gateReset, state gateState) (ConcurrencyResetResult, bool, error) {
	if err := conflict(in, state.Epoch); err != nil {
		return ConcurrencyResetResult{}, false, err
	}
	if in.DryRun {
		_, res := t.judge(ctx, in, state)
		res.State = ResetStateDryRun
		return res, false, nil
	}
	// Measured from before the gate was asked, on this process's own
	// monotonic clock: the gate froze no earlier, so it stays frozen at least
	// this long whatever the two clocks say.
	began := time.Now()
	frozen, again, err := t.freeze(ctx, in, req)
	if err != nil || again {
		return ConcurrencyResetResult{}, again, err
	}
	if err := conflict(in, frozen.State.Epoch); err != nil {
		t.unfreeze(ctx, in.gateID(), req)
		return ConcurrencyResetResult{}, false, err
	}
	usable := frozen.State.Tunables.FreezeTimeout - t.settings.FreezeMargin
	// A run that could not be checked within half of it is kept.
	jctx, jcancel := context.WithDeadline(ctx, began.Add(usable/2))
	seeds, res := t.judge(jctx, in, frozen.State)
	jcancel()
	if time.Since(began) > usable {
		t.unfreeze(ctx, in.gateID(), req)
		return ConcurrencyResetResult{}, false, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true,
			Reason: "the reset took too long and changed nothing; ask again"}
	}
	start := t.gateStart(in, req, frozen.State.Tunables, seeds)
	start.Reset.FrozenUntil = frozen.Until
	// The start may not reach the cluster once the gate may have unfrozen.
	sctx, cancel := context.WithDeadline(ctx, began.Add(usable+t.settings.FreezeMargin/2))
	defer cancel()
	res.Started, err = t.startGate(sctx, start, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)
	if err != nil {
		failed := t.startFailed(in.gateID(), err)
		if !failed.Retryable {
			// Nothing was started, so nothing needs the key frozen. One whose
			// answer is not known stays frozen: its start may yet arrive.
			t.unfreeze(ctx, in.gateID(), req)
		}
		return ConcurrencyResetResult{}, false, failed
	}
	return t.started(ctx, in, res), false, nil
}

// resetAbsent starts a gate for a key that has none. One an acquire started
// meanwhile is reset as one that answers.
func (t *TemporalConcurrencyResetter) resetAbsent(ctx context.Context, in ConcurrencyReset, req gateReset, look gateLook) (ConcurrencyResetResult, bool, error) {
	if err := conflict(in, 0); err != nil {
		return ConcurrencyResetResult{}, false, err
	}
	res := ConcurrencyResetResult{ConsequenceCode: ResetAbsent, Consequence: ResetConsequence(ResetVerdict{Code: ResetAbsent, Key: in.Key})}
	if in.DryRun {
		res.State = ResetStateDryRun
		return res, false, nil
	}
	return t.startFresh(ctx, in, req, look, res)
}

// resetUnanswered resets a gate that does not answer, only when told to: it is
// ended, by its own execution, and started again from the runs' records.
func (t *TemporalConcurrencyResetter) resetUnanswered(ctx context.Context, in ConcurrencyReset, req gateReset, look gateLook) (ConcurrencyResetResult, bool, error) {
	limit := t.recordedLimit(ctx, in.gateID())
	if !in.ForceUnanswered {
		extra := "as many extra runs as its limit"
		if limit > 0 {
			extra = fmt.Sprintf("up to %d extra runs", limit)
		}
		return ConcurrencyResetResult{}, false, &ConcurrencyResetError{Code: ResetUnansweredCode,
			Reason: "the limit is not answering, so the runs it holds cannot all be known; a forced reset accepts " + extra}
	}
	res := ConcurrencyResetResult{ConsequenceCode: ResetUnansweredForced,
		Consequence: ResetConsequence(ResetVerdict{Code: ResetUnansweredForced, Key: in.Key, Limit: limit})}
	if in.DryRun {
		res.State = ResetStateDryRun
		return res, false, nil
	}
	// Decided before anything is ended. A limit another reset holds is left to
	// it: that one's start would end what this one starts, not knowing of it.
	if look.memo != nil && t.now().Before(look.memo.FrozenUntil.Add(t.settings.FreezeMargin)) {
		return ConcurrencyResetResult{}, false, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: "another reset holds the limit; ask again"}
	}
	if err := t.remembers(look.memo, req); err != nil {
		return ConcurrencyResetResult{}, false, err
	}
	cctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()
	err := t.client.TerminateWorkflow(cctx, in.gateID(), look.runID, "reset by an operator: the limit did not answer")
	var missing *serviceerror.NotFound
	if err != nil && !errors.As(err, &missing) {
		t.logger.Warn("reset concurrency limit: the limit that does not answer could not be ended", "limit", in.gateID(), "error", err)
		return ConcurrencyResetResult{}, false, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: "the limit could not be reset; ask again"}
	}
	return t.startFresh(ctx, in, req, look, res)
}

// remembers refuses a request the key's latest execution may not be followed
// by: what it remembers cannot be known, or is all that can be.
func (t *TemporalConcurrencyResetter) remembers(memo *gateResetMemo, req gateReset) error {
	g := gateState{Tunables: t.settings.Tunables}
	g.inheritResets(memo, t.now())
	if err := g.checkReset(req, t.now()); err != nil {
		return &ConcurrencyResetError{Code: ResetRefusedCode, Reason: err.Error()}
	}
	return nil
}

// startFresh starts a gate from the runs' records alone, remembering what the
// key's latest execution did. A gate started meanwhile is looked at again.
func (t *TemporalConcurrencyResetter) startFresh(ctx context.Context, in ConcurrencyReset, req gateReset, look gateLook, res ConcurrencyResetResult) (ConcurrencyResetResult, bool, error) {
	if err := t.remembers(look.memo, req); err != nil {
		return ConcurrencyResetResult{}, false, err
	}
	var seeds gateSeeds
	if inherited := look.memo.inherited(t.settings.Tunables.ResetWindow, t.now()); inherited != nil {
		seeds.Resets = inherited.Resets
	}
	cctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()
	start := t.gateStart(in, req, t.settings.Tunables, seeds)
	// Until then the gate it starts takes no other reset and does not close,
	// so a repeat of this start finds it.
	start.Reset.StartBy = t.now().Add(start.Tunables.FreezeTimeout)
	started, err := t.startGate(cctx, start, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL)
	var exists *serviceerror.WorkflowExecutionAlreadyStarted
	switch {
	case errors.As(err, &exists):
		return ConcurrencyResetResult{}, true, nil
	case err != nil:
		return ConcurrencyResetResult{}, false, t.startFailed(in.gateID(), err)
	}
	res.Started = started
	res.NewEpoch = 1
	return t.started(ctx, in, res), false, nil
}

func (t *TemporalConcurrencyResetter) started(ctx context.Context, in ConcurrencyReset, res ConcurrencyResetResult) ConcurrencyResetResult {
	res.State = ResetStateRunning
	if res.Started && t.observer != nil {
		t.observer.ObserveConcurrencyReset(ctx, in.Namespace, resetOutcomeStarted, in.ForceUnanswered)
	}
	return res
}

// startFailed is a start the cluster refused, or whose answer is not known:
// the same request asked again finds what that one did.
func (t *TemporalConcurrencyResetter) startFailed(gateID string, err error) *ConcurrencyResetError {
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		t.logger.Error("reset concurrency limit: the cluster refused to start the new limit", "limit", gateID, "error", err)
		return &ConcurrencyResetError{Code: ResetRefusedCode, Reason: "the cluster refused to start the new limit, so nothing was started; the engine's log says why, and the same request can be asked again once that is mended"}
	}
	t.logger.Warn("reset concurrency limit: the new limit's start was not answered", "limit", gateID, "error", err)
	return &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: "the reset was not confirmed; ask again with the same request id"}
}

// judge is the seeds of state's reset, and what it found.
func (t *TemporalConcurrencyResetter) judge(ctx context.Context, in ConcurrencyReset, state gateState) (gateSeeds, ConcurrencyResetResult) {
	var runs []string
	seen := map[string]bool{}
	for _, e := range slices.Concat(state.Holders, state.Queue) {
		if !seen[e.Run] {
			seen[e.Run] = true
			runs = append(runs, e.Run)
		}
	}
	var findings []gateRunFinding
	unchecked := 0
	for batch := range slices.Chunk(runs, max(1, state.Tunables.VerifyBatch)) {
		// A run that could not be checked is kept, so a failure drops nothing.
		got, err := t.activities.DescribeConcurrencyEntries(ctx, DescribeConcurrencyEntriesInput{
			GateID: in.gateID(), Runs: batch, Parallel: state.Tunables.VerifyParallel, Namespace: in.Namespace})
		if err != nil {
			unchecked += len(batch)
			continue
		}
		for _, f := range got {
			if f.Err != "" {
				unchecked++
			}
		}
		findings = append(findings, got...)
	}
	seeds, dropped := state.judgeReset(findings)
	res := ConcurrencyResetResult{Epoch: state.Epoch, NewEpoch: seeds.Epoch + 1, Holders: len(seeds.Holders), Queued: len(seeds.Queue),
		Dropped: dropped, Seeded: len(seeds.Holders) + len(seeds.Queue), ConsequenceCode: ResetAnswering}
	if unchecked > 0 {
		t.logger.Warn("reset concurrency limit: runs could not be checked, so their entries are kept", "limit", in.gateID(), "runs", unchecked)
		res.Note = fmt.Sprintf("%d runs could not be checked, so they are kept; the limit checks them again", unchecked)
	}
	for _, e := range seeds.Holders {
		if e.Adopted {
			res.Adopted++
		}
	}
	res.Consequence = ResetConsequence(ResetVerdict{Code: ResetAnswering, Key: in.Key, Epoch: state.Epoch, Holders: res.Holders,
		Queued: res.Queued, Dropped: dropped, AcquireTimeout: state.Tunables.AcquireTimeout})
	return seeds, res
}

// resetStartID is the request id every attempt of one request's start carries,
// so a repeat is the same start.
func resetStartID(req gateReset) string {
	return "reset-start-" + gateDigest(req.ID, req.Body, req.IssuedAt.Format(time.RFC3339Nano))[:32]
}

// freeze freezes the gate for req and returns its state. again: there is no
// gate to freeze any more, or it applied req meanwhile. Each call is an Update
// of its own, so its answer is never an earlier freeze's.
func (t *TemporalConcurrencyResetter) freeze(ctx context.Context, in ConcurrencyReset, req gateReset) (GateFrozen, bool, error) {
	uctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()
	var frozen GateFrozen
	h, err := t.client.UpdateWorkflow(uctx, client.UpdateWorkflowOptions{
		WorkflowID: in.gateID(), UpdateName: GateUpdateFreeze, Args: []any{GateFreeze{Request: req}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		err = h.Get(uctx, &frozen)
	}
	if err == nil {
		return frozen, false, nil
	}
	var missing *serviceerror.NotFound
	failure := categoryOf(err)
	switch {
	case errors.As(err, &missing), failure.appType() == GateErrResetKnown:
		return frozen, true, nil
	case failure.appType() == GateErrResetRefused:
		return frozen, false, &ConcurrencyResetError{Code: ResetRefusedCode, Reason: failure.appMessage()}
	case failure.appType() == GateErrBusy:
		return frozen, false, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: failure.appMessage()}
	}
	t.logger.Warn("reset concurrency limit: the limit did not freeze", "limit", in.gateID(), "error", err)
	return frozen, false, &ConcurrencyResetError{Code: ResetRefusedCode, Retryable: true, Reason: "the limit could not be frozen for the reset; ask again"}
}

// unfreeze ends req's freeze early. One that is not delivered ends at its own
// time.
func (t *TemporalConcurrencyResetter) unfreeze(ctx context.Context, gateID string, req gateReset) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.callTimeout)
	defer cancel()
	if err := t.client.SignalWorkflow(cctx, gateID, "", GateSignalUnfreeze, req); err != nil {
		t.logger.Warn("reset concurrency limit: the freeze could not be ended early; it ends by itself", "limit", gateID, "error", err)
	}
}

func (t *TemporalConcurrencyResetter) gateStart(in ConcurrencyReset, req gateReset, tun GateTunables, seeds gateSeeds) GateStart {
	return GateStart{GateID: in.gateID(), Namespace: in.Namespace, Incarnation: in.Incarnation, Key: in.Key, Tunables: tun,
		Previous: GatePreviousUnclean, Reset: &GateResetStart{Request: req, Seeds: seeds, Forced: in.ForceUnanswered}}
}

// startGate starts the gate, once for a request: its request id is the
// request's own, so a repeat is answered with the execution it started, and
// never starts, or ends, another. started is false for such a repeat.
func (t *TemporalConcurrencyResetter) startGate(ctx context.Context, start GateStart, conflict enumspb.WorkflowIdConflictPolicy) (bool, error) {
	input, err := DataConverter().ToPayloads(start)
	if err != nil {
		return false, fmt.Errorf("encode the gate's start: %w", err)
	}
	req := start.Reset.Request
	remembered := gateResetMemo{Current: req.ID, Resets: append(slices.Clone(start.Reset.Seeds.Resets), req),
		FrozenUntil: t.now().Add(start.Tunables.FreezeTimeout)}
	memo, err := DataConverter().ToPayload(remembered)
	if err != nil {
		return false, fmt.Errorf("encode the gate's memo: %w", err)
	}
	attributes := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{}}
	for name, value := range map[string]string{
		task.SearchKeyOrkflowNamespace.GetName(): start.Namespace,
		task.SearchKeyOrkflowGatePhase.GetName(): gatePhaseActive,
	} {
		p, err := converter.GetDefaultDataConverter().ToPayload(value)
		if err != nil {
			return false, fmt.Errorf("encode the gate's %s: %w", name, err)
		}
		p.Metadata["type"] = []byte(enumspb.INDEXED_VALUE_TYPE_KEYWORD.String())
		attributes.IndexedFields[name] = p
	}
	resp, err := t.client.WorkflowService().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                t.namespace,
		WorkflowId:               start.GateID,
		WorkflowType:             &commonpb.WorkflowType{Name: ConcurrencyGateWorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: t.queue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    input,
		Identity:                 t.identity,
		RequestId:                resetStartID(req),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: conflict,
		Memo:                     &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateResetsMemoKey: memo}},
		SearchAttributes:         attributes,
	})
	if err != nil {
		return false, err
	}
	return resp.GetStarted(), nil
}

// recordedLimit is the largest limit the runs' records name at the gate, 0
// when none can be read: a gate that does not answer cannot say its own.
func (t *TemporalConcurrencyResetter) recordedLimit(ctx context.Context, gateID string) int {
	cctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()
	page, err := t.activities.listEntries(cctx, ListConcurrencyEntriesInput{GateID: gateID})
	if err != nil {
		t.logger.Warn("reset concurrency limit: the runs' records could not be read", "limit", gateID, "error", err)
		return 0
	}
	limit := 0
	for _, r := range page.Records {
		for _, raw := range r.Entries {
			var m gateMemoEntry
			if decodeGateMemoEntry(raw, &m) == nil {
				limit = max(limit, m.Limit)
			}
		}
	}
	return limit
}
