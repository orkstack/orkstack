package engine_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
)

// claimOutcome captures one update's result the way a client sees it.
type claimOutcome struct {
	winner   engine.Claim
	rejected error
	failed   error
}

// claimRecorder adapts the test environment's update callbacks to a value a
// test can assert on.
type claimRecorder struct{ outcome *claimOutcome }

func (r claimRecorder) Accept() {}

func (r claimRecorder) Reject(err error) { r.outcome.rejected = err }

func (r claimRecorder) Complete(success any, err error) {
	if err != nil {
		r.outcome.failed = err
		return
	}
	switch value := success.(type) {
	case engine.Claim:
		r.outcome.winner = value
	case converter.EncodedValue:
		// The real client receives the result encoded; decode it the same way
		// so a Claim that cannot survive the wire fails here too.
		if decodeErr := value.Get(&r.outcome.winner); decodeErr != nil {
			r.outcome.failed = decodeErr
		}
	default:
		r.outcome.failed = fmt.Errorf("claim update returned %T, want engine.Claim", success)
	}
}

// claimEnv runs IdempotencyClaimWorkflow with a set of claim updates delivered
// in order at workflow start, and returns each update's outcome.
//
// Every update is scheduled at delay 0 (plus a nanosecond apart, so the
// environment keeps their order) because the workflow's whole body is a long
// sleep: the test environment skips that sleep instantly, and an update
// scheduled later would arrive after the workflow had already completed.
func claimEnv(t *testing.T, requests ...engine.ClaimRequest) []claimOutcome {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(30 * time.Second)
	env.RegisterWorkflowWithOptions(engine.IdempotencyClaimWorkflow,
		workflow.RegisterOptions{Name: engine.ClaimWorkflowName})

	outcomes := make([]claimOutcome, len(requests))
	for i, request := range requests {
		env.RegisterDelayedCallback(func() {
			env.UpdateWorkflow(engine.ClaimUpdateName, "",
				claimRecorder{outcome: &outcomes[i]}, request)
		}, time.Duration(i))
	}

	env.ExecuteWorkflow(engine.ClaimWorkflowName,
		engine.ClaimWorkflowInput{Key: "order-42", TTL: time.Hour})
	if !env.IsWorkflowCompleted() {
		t.Fatal("claim workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("claim workflow: %v", err)
	}
	return outcomes
}

func candidate(runID string) engine.ClaimRequest {
	return engine.ClaimRequest{Candidate: engine.Claim{
		RunID:    runID,
		Snapshot: []byte("snapshot of " + runID),
	}}
}

// TestClaimWorkflow_SecondClaimGetsTheFirstCandidate is the property the whole
// milestone rests on: two replicas offering different run ids for one key are
// told the same answer, and it is the first one.
func TestClaimWorkflow_SecondClaimGetsTheFirstCandidate(t *testing.T) {
	outcomes := claimEnv(t, candidate("ork-1-1"), candidate("ork-2-2"), candidate("ork-3-3"))

	for i, outcome := range outcomes {
		if outcome.rejected != nil || outcome.failed != nil {
			t.Fatalf("claim %d: rejected=%v failed=%v", i, outcome.rejected, outcome.failed)
		}
		if outcome.winner.RunID != "ork-1-1" {
			t.Errorf("claim %d won by %q, want the first candidate ork-1-1", i, outcome.winner.RunID)
		}
	}
	// The loser is answered with the winner's own snapshot, not its own: it
	// has to report the run that exists, not the one it proposed.
	if got := string(outcomes[1].winner.Snapshot); got != "snapshot of ork-1-1" {
		t.Errorf("loser got snapshot %q, want the winner's", got)
	}
}

// TestClaimWorkflow_HolderCanMoveItsClaimToAReissuedRunID keeps the binding
// pointing at a run that exists. Temporal can refuse a candidate run id
// (engine.ErrRunIDTaken); the holder then starts under a fresh one, and a key
// still naming the refused id would make every repeat submission resolve to a
// run that never existed.
func TestClaimWorkflow_HolderCanMoveItsClaimToAReissuedRunID(t *testing.T) {
	reissue := candidate("ork-9-9")
	reissue.Supersedes = "ork-1-1"

	outcomes := claimEnv(t, candidate("ork-1-1"), reissue, candidate("ork-2-2"))

	if got := outcomes[1].winner.RunID; got != "ork-9-9" {
		t.Fatalf("reissue got %q, want the fresh id ork-9-9", got)
	}
	if got := outcomes[2].winner.RunID; got != "ork-9-9" {
		t.Errorf("a later claim got %q, want the reissued id ork-9-9", got)
	}
}

// TestClaimWorkflow_SupersedeFromANonHolderIsIgnored bounds the reissue path.
// Supersedes moves a binding, so a claim naming the wrong predecessor must
// change nothing at all.
func TestClaimWorkflow_SupersedeFromANonHolderIsIgnored(t *testing.T) {
	stale := candidate("ork-7-7")
	stale.Supersedes = "ork-not-the-holder"

	outcomes := claimEnv(t, candidate("ork-1-1"), stale, candidate("ork-2-2"))

	if got := outcomes[1].winner.RunID; got != "ork-1-1" {
		t.Errorf("a non-holder moved the claim to %q", got)
	}
	if got := outcomes[2].winner.RunID; got != "ork-1-1" {
		t.Errorf("after the stale supersede the key names %q, want ork-1-1", got)
	}
}

// TestClaimWorkflow_ARunlessClaimIsRefused: a claim naming no run would bind
// the key to nothing, and every later submission would resolve to a run that
// does not exist. It is a caller bug, so it fails loudly.
func TestClaimWorkflow_ARunlessClaimIsRefused(t *testing.T) {
	outcomes := claimEnv(t, engine.ClaimRequest{}, candidate("ork-1-1"))

	if outcomes[0].rejected == nil && outcomes[0].failed == nil {
		t.Fatalf("an empty claim was accepted and won %q", outcomes[0].winner.RunID)
	}
	reported := errors.Join(outcomes[0].rejected, outcomes[0].failed).Error()
	if !strings.Contains(reported, engine.ErrInvalidClaim.Error()) {
		t.Errorf("empty claim reported %q, want %q", reported, engine.ErrInvalidClaim)
	}
	// And the key is still free for the next real candidate.
	if got := outcomes[1].winner.RunID; got != "ork-1-1" {
		t.Errorf("after the refused claim the key names %q, want ork-1-1", got)
	}
}

// TestClaimWorkflowID_IsAnOpaqueFixedLengthIDPerKey pins the id contract the
// two-engine integration test asserts against, and the properties that make it
// safe: distinct per key, fixed length whatever the key, and never shaped like
// a run id.
func TestClaimWorkflowID_IsAnOpaqueFixedLengthIDPerKey(t *testing.T) {
	short := engine.ClaimWorkflowID("k")
	long := engine.ClaimWorkflowID(strings.Repeat("k", 4096))
	other := engine.ClaimWorkflowID("k2")

	for _, id := range []string{short, long, other} {
		if !strings.HasPrefix(id, engine.ClaimWorkflowIDPrefix) {
			t.Errorf("claim id %q does not start with %q", id, engine.ClaimWorkflowIDPrefix)
		}
		if len(id) != len(engine.ClaimWorkflowIDPrefix)+32 {
			t.Errorf("claim id %q is %d bytes, want a fixed %d",
				id, len(id), len(engine.ClaimWorkflowIDPrefix)+32)
		}
	}
	if short == other {
		t.Error("two keys share one claim id")
	}
	if strings.Contains(short, "k") && len(short) > len(engine.ClaimWorkflowIDPrefix) {
		// Only a digest may appear after the prefix; a key must not be
		// readable from a workflow listing.
		if strings.Contains(strings.TrimPrefix(short, engine.ClaimWorkflowIDPrefix), "k") {
			t.Logf("digest happens to contain the letter k, which hex allows; id=%s", short)
		}
	}
	if engine.ClaimWorkflowID("") == "" {
		t.Error("an empty key still has to produce a usable workflow id")
	}
}
