package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The submit path after prepare: claim a key, start a run, and decide what a
// start whose answer was lost means (docs/API.md, "Idempotency keys").
//
// It rests on one fact the cluster guarantees: a run id is started at most
// once (StartRun's REJECT_DUPLICATE and CONFLICT_POLICY_FAIL), and one the
// archive adds: a run the cluster has let go is still in the archive, whose
// bodies are never removed. An idempotency key names one run id. So whoever
// acts for a key reads that id in both, and starts it only when both say it
// never was; the cluster lets exactly one start in.

// submitWorkBound is how long the engine spends, on its own clock, on a
// submission's claim and start once it has a candidate. The caller's
// deadline decides only whether it hears the answer: a caller that left
// learns the outcome by submitting again under its key.
const submitWorkBound = 30 * time.Second

// clusterRetentionMaxAge is how old the cluster's reported retention may be
// when a claim's age is compared with it.
const clusterRetentionMaxAge = time.Minute

// claimRetryFirst and claimRetryMax space the claim's repeats after an error.
const (
	claimRetryFirst = 200 * time.Millisecond
	claimRetryMax   = 2 * time.Second
)

// idState is what the two reads of a key's run id say of it.
type idState int

const (
	// idOwn: the cluster holds the id and its first start is this key's.
	idOwn idState = iota
	// idForeign: the cluster holds the id, and its first start was read and
	// is not this key's.
	idForeign
	// idArchived: the cluster does not hold it; the archive does.
	idArchived
	// idAbsent: the cluster has no such execution and the archive no such
	// run. Only this starts anything.
	idAbsent
)

// idRead is one read of a run id: its state, and the archived run for
// idArchived.
type idRead struct {
	state    idState
	archived *orkstackv1.Run
}

// firstStartReader is the cluster read a key's run id needs, which
// engine.TemporalRunReader provides.
type firstStartReader interface {
	FirstStart(ctx context.Context, runID string) (engine.FirstStartRead, error)
}

// The production reader is the one that answers it.
var _ firstStartReader = (*engine.TemporalRunReader)(nil)

// firstStartOf is the first-start reader of a binding: its reader's, else
// its starter's (a unit test's stand-in cluster), else none.
func firstStartOf(binding engine.Binding) firstStartReader {
	if reader, ok := binding.Reader.(firstStartReader); ok {
		return reader
	}
	if starter, ok := binding.Starter.(firstStartReader); ok {
		return starter
	}
	return nil
}

// processStarts is the stand-in cluster of a Service with no reader wired (a
// unit test): the ids this process started, with the fingerprint each start
// carried. It is never consulted behind a wired reader.
type processStarts struct {
	mu  sync.Mutex
	ids map[string]string
}

func (p *processStarts) record(runID, fingerprint string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ids == nil {
		p.ids = map[string]string{}
	}
	p.ids[runID] = fingerprint
}

func (p *processStarts) read(runID string) engine.FirstStartRead {
	p.mu.Lock()
	defer p.mu.Unlock()
	fp, ok := p.ids[runID]
	return engine.FirstStartRead{Held: ok, Fingerprint: fp}
}

// retentionReading caches the cluster's reported retention for at most
// clusterRetentionMaxAge.
type retentionReading struct {
	mu    sync.Mutex
	value time.Duration
	at    time.Time
}

// submitUnkeyed starts a submission that carries no idempotency key.
func (s *Service) submitUnkeyed(
	ctx context.Context,
	sub submission,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), submitWorkBound)
	defer cancel()
	run := s.newPendingRun(s.newRunID(), sub.wf, sub.input, sub.origin)
	s.runs.Save(run)
	return s.startRun(work, run, sub, false, true)
}

// submitAgain is what an unavailable answer asks of the caller.
func submitAgain(sub submission) string {
	if sub.key == "" {
		return "submit again"
	}
	return "submit again under the same key"
}

// submitKeyed runs a keyed submission: look the key up, claim it, and read
// what it names.
func (s *Service) submitKeyed(
	ctx context.Context,
	sub submission,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	// Ask what the key names before minting anything. Any error is no
	// answer, and the claim, which is the authority, decides instead.
	binding, bound, lookupErr := s.idempotency.Lookup(ctx, sub.claimKey())
	if lookupErr != nil {
		s.logger.Debug("idempotency lookup gave no answer; the claim decides",
			"key", sub.key, "err", lookupErr)
	}

	// From here the engine finishes what it began, on its own clock.
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), submitWorkBound)
	defer cancel()
	if lookupErr == nil && bound {
		return s.answerBinding(work, sub, binding)
	}

	candidate := s.newPendingRun(s.newRunID(), sub.wf, sub.input, sub.origin)
	claim, err := newClaim(candidate, sub.plan.IdempotencyFingerprint)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	winner, err := s.claimWithRetry(work, sub.claimKey(), claim)
	if err != nil && permanentClaimError(err) {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("claim idempotency key %q: %w", sub.key, err))
	}
	if err != nil {
		// The candidate may hold the key: the answer never came. Nothing was
		// started, and the next submission under the key reads what it names.
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("claim idempotency key %q: %w; submit again under the same key", sub.key, err))
	}
	// Whether the candidate won is judged by the run id alone: a v0.3.0
	// worker answering for the claim drops the fingerprint.
	if winner.RunID == candidate.GetId() {
		s.runs.Save(candidate)
		return s.startRun(work, candidate, sub, false, true)
	}
	return s.answerBinding(work, sub, winner)
}

// claimWithRetry offers claim until the claimer answers or ctx ends. Asking
// again with the same candidate is safe: a candidate that already won is
// told itself.
func (s *Service) claimWithRetry(ctx context.Context, key string, claim engine.Claim) (engine.Claim, error) {
	wait := claimRetryFirst
	for {
		winner, err := s.idempotency.Claim(ctx, key, claim)
		if err == nil {
			return winner, nil
		}
		if permanentClaimError(err) {
			return engine.Claim{}, err
		}
		s.logger.Warn("idempotency claim gave no answer; asking again", "err", err)
		select {
		case <-ctx.Done():
			return engine.Claim{}, err
		case <-time.After(wait):
		}
		wait = min(2*wait, claimRetryMax)
	}
}

// permanentClaimError is a claim error asking again cannot change: the
// claim refused the request itself.
func permanentClaimError(err error) bool {
	var invalid *serviceerror.InvalidArgument
	return errors.Is(err, engine.ErrInvalidClaim) || errors.As(err, &invalid)
}

// answerBinding answers a submission under a key that already names a run
// id: the fingerprint first, then the id's two reads.
func (s *Service) answerBinding(
	ctx context.Context,
	sub submission,
	binding engine.Claim,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	if binding.Fingerprint == "" {
		return s.answerLegacyBinding(ctx, sub.key, binding)
	}
	if binding.Fingerprint != sub.plan.IdempotencyFingerprint {
		return nil, errKeyUsedElsewhere(sub.key)
	}
	read, err := s.readRunID(ctx, sub.binding, binding.RunID, sub.plan.IdempotencyFingerprint)
	if err != nil {
		return nil, unavailableRead(sub.key, binding.RunID, err)
	}
	switch read.state {
	case idOwn:
		snapshot, _ := claimedSnapshot(binding)
		return s.answerKeyedRun(ctx, sub.key, binding.RunID, snapshot)
	case idArchived:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: read.archived}), nil
	case idForeign:
		return s.replaceForeignID(ctx, sub, binding.RunID)
	default: // idAbsent
		return s.startClaimedID(ctx, sub, binding)
	}
}

// errKeyUsedElsewhere is the refusal of a key reused for another submission.
func errKeyUsedElsewhere(key string) error {
	return connect.NewError(connect.CodeAlreadyExists,
		fmt.Errorf("idempotency key %q was used for another submission "+
			"(another Orkflow, version as written, source run or input); use one key for one submission", key))
}

// unavailableRead answers a key whose run id could not be read.
func unavailableRead(key, runID string, err error) error {
	return connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("idempotency key %q names run %q, which could not be read: %w; submit again under the same key",
			key, runID, err))
}

// answerKeyedRun answers with the run a key's own start made, as any read
// reads it. No callback token is disclosed: only the submission that
// started a run discloses them. The cluster has just said it holds the run,
// so a read that finds nothing yet is answered with pending, the binding's
// own snapshot (fallback); any other failure is unavailable.
func (s *Service) answerKeyedRun(
	ctx context.Context,
	key, runID string,
	fallback *orkstackv1.Run,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	run, err := s.LookupRun(ctx, runID)
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
	case connect.CodeOf(err) == connect.CodeNotFound && fallback != nil:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: fallback}), nil
	default:
		return nil, unavailableRead(key, runID, err)
	}
}

// answerLegacyBinding answers a binding with no fingerprint as v0.3.0
// answered it: the run if it is found, the claim's pending snapshot if not.
// It never starts anything and never moves the key.
func (s *Service) answerLegacyBinding(
	ctx context.Context,
	key string,
	binding engine.Claim,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	switch run, err := s.LookupRun(ctx, binding.RunID); {
	case err == nil:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
	case connect.CodeOf(err) != connect.CodeNotFound:
		return nil, err
	}
	run, err := claimedSnapshot(binding)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("idempotency key %q refers to run %q: %w", key, binding.RunID, err))
	}
	if run == nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("idempotency key %q refers to missing run %q", key, binding.RunID))
	}
	// A run that may never start, answered PENDING: said, and counted, so it
	// is seen while a rollout is still in hand.
	s.logger.Warn("idempotency key claimed by an earlier release answered with a pending run that was not found",
		"key", key, "claim", engine.ClaimWorkflowID(key), "run_id", binding.RunID)
	if s.onLegacyPending != nil {
		s.onLegacyPending(ctx)
	}
	return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
}

// readRunID reads a key's run id: the cluster first, then the archive. The
// other order could miss a run archived and retired between the two reads.
// Every failure is an error: only two definite "no"s are idAbsent.
func (s *Service) readRunID(
	ctx context.Context,
	binding engine.Binding,
	runID, fingerprint string,
) (idRead, error) {
	// The cluster's own read, through the binding's reader; a test's
	// stand-in cluster may be its starter; with neither, the starts this
	// process made.
	var first engine.FirstStartRead
	if reader := firstStartOf(binding); reader != nil {
		var err error
		if first, err = reader.FirstStart(ctx, runID); err != nil {
			return idRead{}, err
		}
	} else {
		first = s.starts.read(runID)
	}
	if first.Held {
		if first.Fingerprint == fingerprint {
			return idRead{state: idOwn}, nil
		}
		return idRead{state: idForeign}, nil
	}
	if s.archive == nil {
		return idRead{state: idAbsent}, nil
	}
	run, err := s.archive.LoadRun(ctx, runID)
	switch {
	case err == nil:
		return idRead{state: idArchived, archived: run}, nil
	case errors.Is(err, artifacts.ErrNotFound):
		return idRead{state: idAbsent}, nil
	default:
		return idRead{}, fmt.Errorf("read run %q from the archive: %w", runID, err)
	}
}

// startClaimedID starts the id a key names when neither the cluster nor the
// archive holds it: the submission that claimed it never started it, or its
// start never landed. It is started only under a claim younger than the
// retention the cluster reports; past that, a run that finished and was
// never archived cannot be told from one that never started.
func (s *Service) startClaimedID(
	ctx context.Context,
	sub submission,
	binding engine.Claim,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	if err := s.checkClaimAge(ctx, sub.claimKey()); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("idempotency key %q: %w", sub.key, err))
	}
	run := s.newPendingRun(binding.RunID, sub.wf, sub.input, sub.origin)
	// The run was asked for when the key was claimed; who started it is
	// this caller, so the origin is its own.
	if claimed, err := claimedSnapshot(binding); err == nil && claimed.GetCreatedAt() != nil {
		run.CreatedAt = claimed.GetCreatedAt()
	}
	if _, held := s.runs.Get(run.GetId()); !held {
		s.runs.Save(run)
	}
	return s.startRun(ctx, run, sub, false, false)
}

// checkClaimAge refuses a start under a claim at least as old as the
// retention the cluster reports, or when either cannot be read.
func (s *Service) checkClaimAge(ctx context.Context, claimKey string) error {
	if s.retention == nil {
		return nil // no cluster retention is managed here: nothing deletes a run under a key
	}
	retention, err := s.clusterRetention(ctx)
	if err != nil {
		return fmt.Errorf("its run cannot be told from one that never started (%w); submit again, or use a new key", err)
	}
	claimedAt, err := s.idempotency.ClaimedAt(ctx, claimKey)
	if err != nil {
		return fmt.Errorf("its run cannot be told from one that never started (%w); submit again, or use a new key", err)
	}
	if s.now().Sub(claimedAt) >= retention {
		return fmt.Errorf("this key's run can no longer be told from one that never started; use a new key")
	}
	return nil
}

// clusterRetention is the cluster's own reported retention, read at most
// clusterRetentionMaxAge ago.
func (s *Service) clusterRetention(ctx context.Context) (time.Duration, error) {
	now := s.now()
	s.retentionRead.mu.Lock()
	cached, at := s.retentionRead.value, s.retentionRead.at
	s.retentionRead.mu.Unlock()
	if !at.IsZero() && now.Sub(at) < clusterRetentionMaxAge {
		return cached, nil
	}
	d, err := s.retention.ClusterRetention(ctx)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("the cluster reported no retention")
	}
	s.retentionRead.mu.Lock()
	s.retentionRead.value, s.retentionRead.at = d, now
	s.retentionRead.mu.Unlock()
	return d, nil
}

// startOutcome classifies a start's answer.
type startOutcome int

const (
	startAccepted startOutcome = iota
	// startTaken: the id is already an execution.
	startTaken
	// startRefused: the cluster said no, and nothing started.
	startRefused
	// startUnknown: it may have started, or may yet.
	startUnknown
)

// classifyStart sorts a start's answer. Only the answers in which the
// cluster definitely refused are refusals; everything else, a transport
// error and the engine's own bound included, is unknown.
func classifyStart(err error) startOutcome {
	if err == nil {
		return startAccepted
	}
	if errors.Is(err, engine.ErrRunIDTaken) {
		return startTaken
	}
	var (
		invalid      *serviceerror.InvalidArgument
		notFound     *serviceerror.NotFound
		nsNotFound   *serviceerror.NamespaceNotFound
		denied       *serviceerror.PermissionDenied
		precondition *serviceerror.FailedPrecondition
		unimpl       *serviceerror.Unimplemented
	)
	if errors.As(err, &invalid) || errors.As(err, &notFound) || errors.As(err, &nsNotFound) ||
		errors.As(err, &denied) || errors.As(err, &precondition) || errors.As(err, &unimpl) {
		return startRefused
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
			codes.FailedPrecondition, codes.Unimplemented:
			return startRefused
		}
	}
	return startUnknown
}

// unknownStartCode is the code a start whose outcome is unknown answers
// with: ResourceExhausted when the cluster said, on the start or the read
// after it, that it was rate-limiting, and Unavailable otherwise. Both ask
// the caller to submit again, and neither records anything. A deadline here
// is the engine's own bound (submitWorkBound), never the caller's, so it is
// Unavailable too; a caller that left is told so by callerGaveUp.
func unknownStartCode(errs ...error) connect.Code {
	var exhausted *serviceerror.ResourceExhausted
	for _, err := range errs {
		if errors.As(err, &exhausted) {
			return connect.CodeResourceExhausted
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			return connect.CodeResourceExhausted
		}
	}
	return connect.CodeUnavailable
}

// callerGaveUp answers a caller whose own context ended before its
// submission was answered: an answer that asks it to ask again (Unavailable,
// ResourceExhausted) becomes Canceled or DeadlineExceeded, as CancelRun
// answers, with the same message. The engine finished the submission on its
// own clock either way; a refusal, or a run, is answered as it is.
func callerGaveUp(ctx context.Context, err error) error {
	if ctx.Err() == nil {
		return err
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	if code := ce.Code(); code != connect.CodeUnavailable && code != connect.CodeResourceExhausted {
		return err
	}
	gone := connect.CodeCanceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		gone = connect.CodeDeadlineExceeded
	}
	return connect.NewError(gone, ce.Unwrap())
}

// startRun writes the run's definition when it is passed by reference, then
// starts it, and decides what each answer means. reissued is true for a
// fresh id that already replaced a refused one: a second refusal ends the
// submission.
func (s *Service) startRun(
	ctx context.Context,
	run *orkstackv1.Run,
	sub submission,
	reissued, fresh bool,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	runID := run.GetId()
	fingerprint := sub.plan.IdempotencyFingerprint
	// Only an id this submission minted is known to nobody else in this
	// process; a key's existing id may be another submission's live run
	// here, so it is never forgotten on a path that cannot tell.
	forgetFresh := func() {
		if fresh {
			s.forget(runID)
		}
	}
	// A definition passed by reference is written under THIS id before the
	// start that uses it, and a run whose definition could not be stored is
	// never started. Nothing is recorded: the store is what failed, so a
	// record would not be durable either. A key keeps naming the id, which
	// the next submission under it starts.
	if err := s.writeDefinitionObject(ctx, runID, sub); err != nil {
		forgetFresh()
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; submit again", err))
	}
	err := sub.binding.Starter.StartRun(ctx, runID, sub.startDefinition(), sub.input, sub.plan)
	switch classifyStart(err) {
	case startAccepted:
		if firstStartOf(sub.binding) == nil {
			s.starts.record(runID, fingerprint)
		}
		return s.startedResponse(run, sub), nil
	case startRefused:
		return nil, s.recordFailedStart(ctx, run, err)
	case startTaken:
		if sub.key == "" {
			// A fresh id nobody else knew: a collision. One fresh id is
			// tried; a second collision is a broken generator, not bad luck.
			s.forget(runID)
			if reissued {
				return nil, connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("start run: two run ids in a row were already taken: %w", err))
			}
			next := s.newPendingRun(s.newRunID(), sub.wf, sub.input, sub.origin)
			s.runs.Save(next)
			return s.startRun(ctx, next, sub, true, true)
		}
	}
	// Taken under a key, or unknown: read the id. Never record a failed
	// start beside a run that may have landed, and never start the id again
	// from here: this submission has just tried.
	read, readErr := s.readRunID(ctx, sub.binding, runID, fingerprint)
	if readErr != nil {
		forgetFresh()
		return nil, connect.NewError(unknownStartCode(err, readErr),
			fmt.Errorf("start run %s: %w; whether it started could not be read (%w); %s",
				runID, err, readErr, submitAgain(sub)))
	}
	switch read.state {
	case idOwn:
		if sub.key == "" {
			// An unkeyed id was known to this submission alone, so the
			// start that landed is its own, and so are the tokens.
			return s.startedResponse(run, sub), nil
		}
		// The key's run exists, but another submission under the key may
		// have made the start, with callback tokens of its own: answer it as
		// a read, and disclose none. The run is real, so this process keeps
		// what it holds of it.
		return s.answerKeyedRun(ctx, sub.key, runID, run)
	case idArchived:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: read.archived}), nil
	case idForeign:
		s.forget(runID)
		if sub.key == "" || reissued {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("start run %s: the id is held by a run this submission did not start; submit again", runID))
		}
		return s.replaceForeignID(ctx, sub, runID)
	default: // idAbsent
		forgetFresh()
		return nil, connect.NewError(unknownStartCode(err),
			fmt.Errorf("start run %s: %w; it may still start; %s", runID, err, submitAgain(sub)))
	}
}

// startedResponse is the answer of the submission that started a run: the
// run it created, never a re-read (which would race the run's first events),
// and its callback tokens, disclosed here and nowhere else.
func (s *Service) startedResponse(run *orkstackv1.Run, sub submission) *connect.Response[orkstackv1.SubmitRunResponse] {
	return connect.NewResponse(&orkstackv1.SubmitRunResponse{
		Run:            run,
		AwaitCallbacks: awaitCallbacksFor(run.GetId(), sub.callbacks),
	})
}

// forget drops a candidate this process holds in memory and will not answer
// for: one whose start was refused as someone else's, or a fresh id whose
// start may never land. A key's existing id, which another submission in
// this process may have started, is never forgotten on a path that cannot
// tell. The run store is write-side
// state only (AGENTS.md §7).
func (s *Service) forget(runID string) {
	s.runs.Delete(runID)
	s.bus.Discard(runID)
}

// replaceForeignID replaces a key's id that is held by something other than
// the key's own run (two ids collided, or something started a workflow under
// the id first). It is replaced once: a fresh id, the definition written
// under it, then the key moved by naming the id that was refused, then the
// start. A failure before the move leaves the key where it was and records
// nothing under an id this submission does not own.
func (s *Service) replaceForeignID(
	ctx context.Context,
	sub submission,
	refused string,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	next := s.newPendingRun(s.newRunID(), sub.wf, sub.input, sub.origin)
	if err := s.writeDefinitionObject(ctx, next.GetId(), sub); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; submit again under the same key", err))
	}
	claim, err := newClaim(next, sub.plan.IdempotencyFingerprint)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.idempotency.Rebind(ctx, sub.claimKey(), claim, refused); err != nil {
		// Another submission may have moved the key first: go on with the id
		// it moved it to, read as any key's id is read. Its definition object
		// written above is left for the sweep, as any unread one is.
		return s.followMovedKey(ctx, sub, refused, err)
	}
	s.runs.Save(next)
	return s.startRun(ctx, next, sub, true, true)
}

// followMovedKey goes on with the id a key was moved to by another
// submission while this one tried to move it off refused. It does not move
// the key again: an id that is foreign too, or anything unread, is
// unavailable.
func (s *Service) followMovedKey(
	ctx context.Context,
	sub submission,
	refused string,
	rebindErr error,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	unavailable := connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("move idempotency key %q from %s: %w; submit again under the same key", sub.key, refused, rebindErr))
	binding, bound, err := s.idempotency.Lookup(ctx, sub.claimKey())
	if err != nil || !bound || binding.RunID == refused {
		return nil, unavailable
	}
	if binding.Fingerprint != "" && binding.Fingerprint != sub.plan.IdempotencyFingerprint {
		return nil, errKeyUsedElsewhere(sub.key)
	}
	read, err := s.readRunID(ctx, sub.binding, binding.RunID, sub.plan.IdempotencyFingerprint)
	if err != nil {
		return nil, unavailableRead(sub.key, binding.RunID, err)
	}
	switch read.state {
	case idOwn:
		snapshot, _ := claimedSnapshot(binding)
		return s.answerKeyedRun(ctx, sub.key, binding.RunID, snapshot)
	case idArchived:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: read.archived}), nil
	case idAbsent:
		return s.startClaimedID(ctx, sub, binding)
	default:
		return nil, unavailable
	}
}

// newClaim packs a pending run and the submission's fingerprint into the
// claim that binds a key to it. The snapshot travels so a replica can answer
// a legacy binding with the winner's run.
func newClaim(run *orkstackv1.Run, fingerprint string) (engine.Claim, error) {
	snapshot, err := proto.Marshal(run)
	if err != nil {
		return engine.Claim{}, fmt.Errorf("encode run %s: %w", run.GetId(), err)
	}
	return engine.Claim{RunID: run.GetId(), Snapshot: snapshot, Fingerprint: fingerprint}, nil
}
