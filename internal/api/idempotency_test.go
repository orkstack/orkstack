package api_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// sharedClaimer stands in for the cluster: one binding per key, visible to
// every Service wired to it. Two Services sharing one of these are two engine
// replicas sharing one Temporal namespace.
type sharedClaimer struct {
	mu       sync.Mutex
	claims   map[string]engine.Claim
	proposed int
	rebinds  []string
	err      error
}

func newSharedClaimer() *sharedClaimer {
	return &sharedClaimer{claims: map[string]engine.Claim{}}
}

func (c *sharedClaimer) Claim(
	_ context.Context,
	key string,
	propose func() (engine.Claim, error),
) (engine.Claim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return engine.Claim{}, c.err
	}
	candidate, err := propose()
	if err != nil {
		return engine.Claim{}, err
	}
	c.proposed++
	if existing, seen := c.claims[key]; seen {
		return existing, nil
	}
	c.claims[key] = candidate
	return candidate, nil
}

func (c *sharedClaimer) Rebind(
	_ context.Context,
	key string,
	next engine.Claim,
	supersedes string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebinds = append(c.rebinds, fmt.Sprintf("%s:%s->%s", key, supersedes, next.RunID))
	if current, seen := c.claims[key]; seen && current.RunID != supersedes {
		return nil
	}
	c.claims[key] = next
	return nil
}

// bind writes a claim directly, standing for a binding another replica made.
func (c *sharedClaimer) bind(key string, run *orkstackv1.Run) {
	snapshot, err := proto.Marshal(run)
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.claims[key] = engine.Claim{RunID: run.GetId(), Snapshot: snapshot}
}

func (c *sharedClaimer) counts() (proposed int, rebinds []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proposed, append([]string(nil), c.rebinds...)
}

// replica builds one engine replica: its own read model and starter, sharing
// whatever claimer and archive it is given.
type replica struct {
	svc     *api.Service
	runs    *engine.RunStore
	starter *fakeStarter
}

func newReplica(
	t *testing.T,
	claimer api.IdempotencyClaimer,
	archive api.RunArchive,
	starter *fakeStarter,
) *replica {
	t.Helper()
	runs := engine.NewRunStore()
	svc := api.NewService(api.Config{
		Workflows:   engine.NewWorkflowStore(),
		Runs:        runs,
		Bus:         engine.NewEventBus(),
		Starter:     starter,
		Archive:     archive,
		Idempotency: claimer,
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return &replica{svc: svc, runs: runs, starter: starter}
}

func (r *replica) submit(t *testing.T, key string) (*orkstackv1.Run, error) {
	t.Helper()
	resp, err := r.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{
			WorkflowName:   "hello-ork",
			IdempotencyKey: key,
		}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

// TestSubmitRun_TwoReplicasSharingAClaimerConverseOnOneRun is acceptance 1 at
// the unit level, and it is the shape the two-engine integration test proves
// against a real cluster: two Services with nothing in common but the claimer
// — separate run stores, separate buses, separate starters — must still answer
// one key with one run.
func TestSubmitRun_TwoReplicasSharingAClaimerConverseOnOneRun(t *testing.T) {
	claimer := newSharedClaimer()
	a := newReplica(t, claimer, nil, &fakeStarter{})
	b := newReplica(t, claimer, nil, &fakeStarter{})

	first, err := a.submit(t, "order-42")
	if err != nil {
		t.Fatalf("submit on A: %v", err)
	}
	second, err := b.submit(t, "order-42")
	if err != nil {
		t.Fatalf("submit on B: %v", err)
	}

	if first.GetId() != second.GetId() {
		t.Fatalf("one key produced two runs: A %q, B %q", first.GetId(), second.GetId())
	}
	if started := b.starter.startedRuns(); len(started) != 0 {
		t.Errorf("replica B started %v; the key was already claimed", started)
	}
	if started := a.starter.startedRuns(); len(started) != 1 {
		t.Errorf("replica A started %v, want exactly one run", started)
	}
	// B never saw the run, so its answer can only have come from the claim.
	if _, local := b.runs.Get(first.GetId()); local {
		t.Fatal("replica B's read model holds the run; the test proves nothing")
	}
}

// TestSubmitRun_LosingReplicaReportsTheWinnersRunNotItsOwnRequest: the loser
// discards its candidate, so everything it reports has to describe the run
// that exists. Rebuilding a plausible run from its own request would be a
// different orkflow version under the winner's id.
func TestSubmitRun_LosingReplicaReportsTheWinnersRunNotItsOwnRequest(t *testing.T) {
	claimer := newSharedClaimer()
	winner := &orkstackv1.Run{
		Id:              "ork-1-1",
		WorkflowName:    "hello-ork",
		WorkflowVersion: "9.9.9-claimed-elsewhere",
		Status:          orkstackv1.Status_STATUS_PENDING,
	}
	claimer.bind("order-42", winner)

	loser := newReplica(t, claimer, nil, &fakeStarter{})
	got, err := loser.submit(t, "order-42")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if got.GetId() != winner.GetId() {
		t.Errorf("run id = %q, want the claimed %q", got.GetId(), winner.GetId())
	}
	if got.GetWorkflowVersion() != winner.GetWorkflowVersion() {
		t.Errorf("run version = %q, want the winner's %q: the loser described its own request",
			got.GetWorkflowVersion(), winner.GetWorkflowVersion())
	}
	if started := loser.starter.startedRuns(); len(started) != 0 {
		t.Errorf("the loser started %v", started)
	}
}

// TestSubmitRun_FailedStartStaysDeduplicatedToItsFailedRun is acceptance 3.
// The claim is never released, so a repeat submission is told the run that
// failed rather than being allowed to start a second one — the behaviour the
// in-process path documented, now on a binding two replicas share.
func TestSubmitRun_FailedStartStaysDeduplicatedToItsFailedRun(t *testing.T) {
	claimer := newSharedClaimer()
	starter := &fakeStarter{err: errors.New("temporal is down")}
	engineA := newReplica(t, claimer, nil, starter)

	if _, err := engineA.submit(t, "order-42"); err == nil {
		t.Fatal("submit succeeded, want the start failure reported")
	}

	repeat, err := engineA.submit(t, "order-42")
	if err != nil {
		t.Fatalf("repeat submit: %v", err)
	}
	if repeat.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("repeat submission reports %s, want the recorded FAILED run", repeat.GetStatus())
	}
	if got := repeat.GetError().GetCode(); got != "RUN_START_FAILED" {
		t.Errorf("repeat submission error code = %q, want RUN_START_FAILED", got)
	}
	if proposed, _ := claimer.counts(); proposed != 2 {
		t.Errorf("the claimer saw %d candidates, want 2 (one per submission)", proposed)
	}
}

// TestSubmitRun_FailedStartStaysDeduplicatedOnAnotherReplica is acceptance 3
// where the process-local map could never reach: the replica that failed the
// start is not the one that receives the repeat. The claim names the run and
// the shared archive holds it, so the second replica reports the same failure
// instead of starting a run the caller already has.
func TestSubmitRun_FailedStartStaysDeduplicatedOnAnotherReplica(t *testing.T) {
	claimer := newSharedClaimer()
	archive := newArchiver(t)
	a := newReplica(t, claimer, archive, &fakeStarter{err: errors.New("temporal is down")})
	b := newReplica(t, claimer, archive, &fakeStarter{})

	if _, err := a.submit(t, "order-42"); err == nil {
		t.Fatal("submit on A succeeded, want the start failure reported")
	}

	got, err := b.submit(t, "order-42")
	if err != nil {
		t.Fatalf("submit on B: %v", err)
	}
	if got.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("replica B reports %s, want the FAILED run replica A recorded", got.GetStatus())
	}
	if started := b.starter.startedRuns(); len(started) != 0 {
		t.Errorf("replica B started %v after a failed start elsewhere", started)
	}
}

// TestSubmitRun_AClaimWhoseWinnerDiedHoldsTheKeyUntilItsTTL pins the ONE case
// acceptance 3 does not cover, and the liveness price the durable claim
// charges for "never two runs for one key".
//
// A replica can win a claim and die before its StartRun reaches the cluster.
// The claim survives the replica, so the key stays bound to a run that nobody
// will ever execute, and every later submission is answered from the claim's
// own PENDING snapshot — no error, no second run — until the claim workflow
// completes at ORK_IDEMPOTENCY_TTL or an operator terminates it by hand
// (internal/engine/claim.go, docs/DEPLOY.md §5). The in-process map this
// replaced had no such window; it also let two replicas start two runs.
//
// It is written down as a test so the shape of the window cannot drift
// unnoticed: if some later change decided to start a fresh run here, that is
// two runs for one key and the milestone's headline property is gone; if it
// decided to fail the submission instead, the runbook's advice would be
// wrong. Both are visible from here.
func TestSubmitRun_AClaimWhoseWinnerDiedHoldsTheKeyUntilItsTTL(t *testing.T) {
	claimer := newSharedClaimer()
	// The binding a dead replica left behind: a PENDING run, in no read model
	// and in no archive, because it never reached Temporal.
	stranded := &orkstackv1.Run{
		Id:           "ork-20260817000000-0001",
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_PENDING,
	}
	claimer.bind("order-42", stranded)

	survivor := newReplica(t, claimer, newArchiver(t), &fakeStarter{})
	got, err := survivor.submit(t, "order-42")
	if err != nil {
		t.Fatalf("submit against a stranded claim: %v", err)
	}

	if got.GetId() != stranded.GetId() {
		t.Errorf("submission answered with run %q, want the claim's stranded run %q: "+
			"a different id means a second run was started for one key",
			got.GetId(), stranded.GetId())
	}
	if got.GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Errorf("stranded claim answered %s, want PENDING — the snapshot the winner recorded",
			got.GetStatus())
	}
	if started := survivor.starter.startedRuns(); len(started) != 0 {
		t.Errorf("started %v for a key already bound; the dedupe guarantee is the point of the claim",
			started)
	}

	// The TTL is the only thing that ends the window, so the engine must never
	// hold a claim forever by default. DefaultClaimTTL is what an unset
	// ORK_IDEMPOTENCY_TTL resolves to (cmd/ork, loadConfig).
	if engine.DefaultClaimTTL <= 0 {
		t.Fatalf("DefaultClaimTTL = %v: a non-positive default would strand a crashed "+
			"replica's key for the life of the cluster", engine.DefaultClaimTTL)
	}
}

// TestSubmitRun_AnUnreachableClaimerFailsTheSubmission: without the authority
// there is no way to know whether the key is already bound, and guessing is
// the duplicate this milestone removes. The submission fails and nothing
// starts.
func TestSubmitRun_AnUnreachableClaimerFailsTheSubmission(t *testing.T) {
	claimer := newSharedClaimer()
	claimer.err = errors.New("temporal frontend unreachable")
	r := newReplica(t, claimer, nil, &fakeStarter{})

	_, err := r.submit(t, "order-42")
	if err == nil {
		t.Fatal("submit succeeded with no reachable claim authority")
	}
	if code := connect.CodeOf(err); code != connect.CodeUnavailable {
		t.Errorf("code = %v, want Unavailable", code)
	}
	if started := r.starter.startedRuns(); len(started) != 0 {
		t.Errorf("started %v without a claim", started)
	}
}

// TestSubmitRun_ReissueMovesTheClaimToTheRunThatStarted keeps the binding
// honest across the M8.1 retry: when Temporal refuses the claimed run id, the
// key has to follow the id that actually started, and it has to move by
// naming the id it replaces so no other caller can take the binding over.
func TestSubmitRun_ReissueMovesTheClaimToTheRunThatStarted(t *testing.T) {
	claimer := newSharedClaimer()
	starter := &takenIDStarter{rejections: 1}

	// newReplica takes a *fakeStarter; this case needs the rejecting starter,
	// so the Service is built directly.
	runs := engine.NewRunStore()
	svc := api.NewService(api.Config{
		Workflows:   engine.NewWorkflowStore(),
		Runs:        runs,
		Bus:         engine.NewEventBus(),
		Starter:     starter,
		Idempotency: claimer,
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	submit := func() (*orkstackv1.Run, error) {
		resp, err := svc.SubmitRun(context.Background(),
			connect.NewRequest(&orkstackv1.SubmitRunRequest{
				WorkflowName:   "hello-ork",
				IdempotencyKey: "order-42",
			}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetRun(), nil
	}

	first, err := submit()
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rejected, started := starter.attempts()
	if len(rejected) != 1 || len(started) != 1 {
		t.Fatalf("attempts: rejected %v started %v, want one of each", rejected, started)
	}
	if first.GetId() != started[0] {
		t.Errorf("submit returned %q, want the id that started, %q", first.GetId(), started[0])
	}

	_, rebinds := claimer.counts()
	want := fmt.Sprintf("order-42:%s->%s", rejected[0], started[0])
	if len(rebinds) != 1 || rebinds[0] != want {
		t.Errorf("claim moves = %v, want exactly [%s]", rebinds, want)
	}

	repeat, err := submit()
	if err != nil {
		t.Fatalf("repeat submit: %v", err)
	}
	if repeat.GetId() != started[0] {
		t.Errorf("repeat submission returned %q, want the started run %q", repeat.GetId(), started[0])
	}
}

// TestSubmitRun_AnUnkeyedSubmissionNeverTouchesTheClaimer: dedupe is opt-in.
// A submission with no key must not create a claim, or every submission would
// pay a round trip to the authority.
func TestSubmitRun_AnUnkeyedSubmissionNeverTouchesTheClaimer(t *testing.T) {
	claimer := newSharedClaimer()
	r := newReplica(t, claimer, nil, &fakeStarter{})

	if _, err := r.submit(t, ""); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if proposed, rebinds := claimer.counts(); proposed != 0 || len(rebinds) != 0 {
		t.Errorf("an unkeyed submission reached the claimer: %d candidates, %v moves",
			proposed, rebinds)
	}
}
