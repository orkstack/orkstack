package engine

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/task"
)

// claimGrace is how much longer than its TTL a claim execution may live before
// the server closes it. The workflow body owns the lifetime; this bound only
// catches a claim no worker ever picked up, so it never becomes an execution
// that outlives the fleet that created it.
const claimGrace = 5 * time.Minute

// claimClient is the slice of client.Client the claimer uses. Narrowing it
// keeps the start options testable without a cluster — the options are the
// whole mechanism, so they are what a test has to be able to read.
type claimClient interface {
	NewWithStartWorkflowOperation(
		options client.StartWorkflowOptions,
		workflow any,
		args ...any,
	) client.WithStartWorkflowOperation
	UpdateWithStartWorkflow(
		ctx context.Context,
		options client.UpdateWithStartWorkflowOptions,
	) (client.WorkflowUpdateHandle, error)
}

// TemporalIdempotency makes Temporal the authority on which run an idempotency
// key names. It is the production implementation of the API's claimer seam.
//
// The mechanism is update-with-start against IdempotencyClaimWorkflow, whose
// id is derived from the key:
//
//   - WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING joins the execution that already
//     holds the key instead of starting a second one, and is required by
//     UpdateWithStartWorkflow (v1.47.0 client/client.go:1580-1591);
//   - the update in the same request reads back the winner.
//
// It is the only GA call in SDK v1.47 that both creates-or-joins an execution
// and returns application state in one round trip, so the decision of who wins
// is taken once, by the server, and never by a replica. The documented
// fallback — start with USE_EXISTING, then QueryWorkflow for the winner — needs
// two round trips with a gap in between, and atomicity in one call is the point.
//
// Both replicas of a race issue this call; the server serializes them onto one
// execution and both are told the same run id.
type TemporalIdempotency struct {
	client claimClient
	queue  string
	ttl    time.Duration
}

// NewTemporalIdempotency returns a claimer that hosts claims on queue, which
// must be a queue the engine's own workflow worker polls: a claim is engine
// state and no Orker should ever see it. An empty queue means
// task.DefaultQueue; a non-positive ttl means DefaultClaimTTL.
func NewTemporalIdempotency(c client.Client, queue string, ttl time.Duration) *TemporalIdempotency {
	if queue == "" {
		queue = task.DefaultQueue
	}
	if ttl <= 0 {
		ttl = DefaultClaimTTL
	}
	return &TemporalIdempotency{client: c, queue: queue, ttl: ttl}
}

// Claim binds key to a run and returns the binding, which is the winner's, not
// necessarily this caller's.
//
// propose is called only when a candidate is actually needed. The seam is a
// callback rather than a value because the two authorities need the candidate
// at different moments: this one has to offer a candidate before the server can
// choose, while an in-process map can answer a known key without minting a run
// id at all.
func (t *TemporalIdempotency) Claim(
	ctx context.Context,
	key string,
	propose func() (Claim, error),
) (Claim, error) {
	candidate, err := propose()
	if err != nil {
		return Claim{}, err
	}
	return t.update(ctx, key, ClaimRequest{Candidate: candidate})
}

// Rebind moves key from a run id Temporal refused (ErrRunIDTaken) onto the
// fresh one its holder started under. It is not a release: the key is bound
// throughout, first to the discarded id and then to the real one, so no
// concurrent submission can slip between the two and start a second run.
func (t *TemporalIdempotency) Rebind(
	ctx context.Context,
	key string,
	next Claim,
	supersedes string,
) error {
	winner, err := t.update(ctx, key, ClaimRequest{Candidate: next, Supersedes: supersedes})
	if err != nil {
		return err
	}
	if winner.RunID != next.RunID {
		return fmt.Errorf("rebind idempotency key to run %s: the key names run %s",
			next.RunID, winner.RunID)
	}
	return nil
}

// update issues one update-with-start and returns the binding it reports.
//
// Retrying it is safe and needs no request id of its own: a candidate that
// already won is told itself, and one that lost is told the winner, so a
// resent request cannot produce a second binding.
func (t *TemporalIdempotency) update(
	ctx context.Context,
	key string,
	request ClaimRequest,
) (Claim, error) {
	claimID := ClaimWorkflowID(key)
	start := t.client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:        claimID,
		TaskQueue: t.queue,
		// Join the execution holding the key rather than starting a rival.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		// Deliberately not RunWorkflow's REJECT_DUPLICATE (temporal.go): a run
		// id is single-use for all time, but a key must be reusable once its
		// claim has expired, and a closed claim's id is exactly that case.
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionTimeout: t.ttl + claimGrace,
	}, ClaimWorkflowType, ClaimWorkflowInput{Key: key, TTL: t.ttl})

	handle, err := t.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName: ClaimUpdateName,
			Args:       []any{request},
			// Completed, not Accepted: an accepted update has not run the
			// handler yet, so there is no winner to read.
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return Claim{}, fmt.Errorf("claim idempotency key at %s: %w", claimID, err)
	}
	var winner Claim
	if err := handle.Get(ctx, &winner); err != nil {
		return Claim{}, fmt.Errorf("read idempotency claim at %s: %w", claimID, err)
	}
	if winner.RunID == "" {
		return Claim{}, fmt.Errorf("idempotency claim at %s named no run", claimID)
	}
	return winner, nil
}
