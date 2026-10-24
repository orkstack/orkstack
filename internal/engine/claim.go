package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Names and defaults for the idempotency claim. The workflow is engine
// internal: nothing outside this package should need to spell these.
const (
	// ClaimWorkflowType is the registered type of IdempotencyClaimWorkflow.
	ClaimWorkflowType = "IdempotencyClaimWorkflow"

	// ClaimUpdateName is the update that binds a key to a run.
	ClaimUpdateName = "claim"

	// ClaimWorkflowIDPrefix marks a workflow id as a claim rather than a run.
	// A run id is `ork-{timestamp}-{sequence}` (AGENTS.md §7) and can never
	// collide with this.
	ClaimWorkflowIDPrefix = "ork-idem-"

	// claimWorkflowIDHexLen is how much of the key's digest the id carries.
	// 128 bits is far beyond what an accidental collision could reach, and it
	// keeps the id short enough to read in `temporal workflow list`.
	claimWorkflowIDHexLen = 32

	// DefaultClaimTTL is how long a key stays bound to its run. A claim is
	// never released early — see the compare-and-swap in claimState.apply —
	// so this is the only thing that ever frees a key.
	DefaultClaimTTL = 24 * time.Hour
)

// ErrInvalidClaim reports a claim that names no run. It is a programming
// error in the caller, not a race, so the update rejects it rather than
// storing a binding that resolves to nothing.
var ErrInvalidClaim = errors.New("claim names no run")

// Claim is a binding of an idempotency key to one run.
//
// Snapshot carries the winner's pending run, marshalled by the replica that
// created it. It exists because the replica that loses a claim has to answer
// its caller with a run it never started and, while the run is still
// executing, has not archived either. Carrying it in the claim keeps the
// answer the *winner's* run rather than a plausible reconstruction from the
// loser's own request, which could differ in orkflow version or input.
type Claim struct {
	RunID    string `json:"run_id"`
	Snapshot []byte `json:"snapshot,omitempty"`
}

// ClaimRequest is one candidate offered for a key.
//
// Supersedes, when set, asks to replace an existing binding, and is honoured
// only when it names the current holder. That is the reissue path: Temporal
// refused the winner's run id (ErrRunIDTaken) and the winner is moving its own
// claim onto the fresh id. A caller that never held the key cannot use it to
// steal one, because it does not know the current holder's id until the claim
// tells it — at which point it is the loser and has nothing to move.
type ClaimRequest struct {
	Candidate  Claim  `json:"candidate"`
	Supersedes string `json:"supersedes,omitempty"`
}

// ClaimWorkflowInput starts a claim. The key is carried for operators reading
// history — the workflow id is a digest and cannot be read back.
type ClaimWorkflowInput struct {
	Key string        `json:"key"`
	TTL time.Duration `json:"ttl"`
}

// ClaimWorkflowID is the workflow id that holds a key's binding.
//
// The key is hashed rather than used directly for three reasons: a key is
// caller-supplied and need not satisfy Temporal's workflow-id rules; it may
// carry information a workflow listing should not show; and a digest is fixed
// length, so no key can be long enough to be truncated into another key's id.
func ClaimWorkflowID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return ClaimWorkflowIDPrefix + hex.EncodeToString(sum[:])[:claimWorkflowIDHexLen]
}

// IdempotencyClaimWorkflow holds one idempotency key's binding to a run, and
// is the authority every engine replica defers to.
//
// It exists because dedupe cannot live in a replica. Two processes hold two
// maps, so the same key reaches each of them unseen and each starts its own
// run. Temporal already serializes concurrent writes to a single workflow
// execution, so a workflow whose id is derived from the key turns "who saw it
// first" into a question the server answers once for the whole fleet.
//
// The cost of holding it server-side, stated plainly: the binding outlives the
// replica that won it. If a replica wins a claim and dies before its StartRun
// reaches the cluster, the key stays bound to a run that will never execute,
// and every later submission of that key is answered with that PENDING run
// (internal/api/service.go, claimedRun) until the TTL expires. The
// process-local map this replaced had no such window — a crash cleared it —
// and could not have the guarantee either: it let two replicas start two runs
// for one key. Never two runs is the property that was bought; the window is
// the price, ORK_IDEMPOTENCY_TTL bounds it for every key, and terminating the
// claim execution (its id is ClaimWorkflowID(key)) frees one by hand. See
// docs/DEPLOY.md §5.
//
// Determinism (AGENTS.md §2.1): the body sets an update handler, sleeps, and
// returns. No clock is read, no IO is done, nothing iterates a map, and the
// key's digest is computed by the caller and arrives as the workflow id.
func IdempotencyClaimWorkflow(ctx workflow.Context, in ClaimWorkflowInput) (Claim, error) {
	state := &claimState{}
	if err := workflow.SetUpdateHandler(ctx, ClaimUpdateName, state.apply); err != nil {
		return Claim{}, fmt.Errorf("register %s handler: %w", ClaimUpdateName, err)
	}

	ttl := in.TTL
	if ttl <= 0 {
		ttl = DefaultClaimTTL
	}
	// The claim is held for its whole life and then the workflow completes,
	// which frees the key: a later submission finds no running execution and
	// starts a fresh claim. Sleeping is what holds it — there is no release
	// path, because a released key is a key that can start a second run.
	if err := workflow.Sleep(ctx, ttl); err != nil {
		// Cancellation is the operator terminating a claim by hand. Report the
		// binding as it stands so history records who held the key.
		return state.winner, err
	}
	return state.winner, nil
}

// claimState is the binding, and the compare-and-swap that maintains it.
type claimState struct {
	winner Claim
}

// apply is the claim update handler: first candidate wins, everyone else is
// told the winner.
//
// Update handlers run one at a time on the workflow goroutine, so this is a
// serialized compare-and-swap without a mutex — and the serialization point is
// the server, not any replica, which is the entire reason for the workflow.
func (s *claimState) apply(_ workflow.Context, req ClaimRequest) (Claim, error) {
	if req.Candidate.RunID == "" {
		return Claim{}, ErrInvalidClaim
	}
	switch {
	case s.winner.RunID == "":
		// Unbound: this candidate is the winner, for good.
		s.winner = req.Candidate
	case req.Supersedes != "" && req.Supersedes == s.winner.RunID:
		// The holder's run id was refused by Temporal and it reissued. Moving
		// the binding is the only way the key keeps naming a run that exists.
		s.winner = req.Candidate
	}
	return s.winner, nil
}
