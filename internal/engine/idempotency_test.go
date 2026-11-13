package engine_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordingClaimClient captures the update-with-start call TemporalIdempotency
// issues. The options are the whole mechanism — they are what makes the server
// rather than a replica decide the winner — so they are what a test has to be
// able to read.
type recordingClaimClient struct {
	client.Client

	startOptions client.StartWorkflowOptions
	workflowType any
	workflowArgs []any
	updateInput  client.UpdateWithStartWorkflowOptions

	winner   engine.Claim
	startErr error
	getErr   error

	// describe and query answer Lookup and ClaimedAt.
	describe    *workflowservice.DescribeWorkflowExecutionResponse
	describeErr error
	queried     engine.Claim
	queryErr    error
	queries     int
}

func (c *recordingClaimClient) DescribeWorkflowExecution(
	context.Context, string, string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if c.describeErr != nil {
		return nil, c.describeErr
	}
	if c.describe == nil {
		return nil, serviceerror.NewNotFound("no claim")
	}
	return c.describe, nil
}

func (c *recordingClaimClient) QueryWorkflow(
	context.Context, string, string, string, ...any,
) (converter.EncodedValue, error) {
	c.queries++
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(c.queried)
	if err != nil {
		return nil, err
	}
	return encodedClaim{payloads: payloads}, nil
}

type encodedClaim struct{ payloads *commonpb.Payloads }

func (e encodedClaim) HasValue() bool { return true }
func (e encodedClaim) Get(valuePtr any) error {
	return converter.GetDefaultDataConverter().FromPayloads(e.payloads, valuePtr)
}

func (c *recordingClaimClient) NewWithStartWorkflowOperation(
	options client.StartWorkflowOptions,
	workflowType any,
	args ...any,
) client.WithStartWorkflowOperation {
	c.startOptions = options
	c.workflowType = workflowType
	c.workflowArgs = args
	return stubStartOperation{}
}

func (c *recordingClaimClient) UpdateWithStartWorkflow(
	_ context.Context,
	options client.UpdateWithStartWorkflowOptions,
) (client.WorkflowUpdateHandle, error) {
	c.updateInput = options
	if c.startErr != nil {
		return nil, c.startErr
	}
	return stubUpdateHandle{winner: c.winner, err: c.getErr}, nil
}

// request returns the ClaimRequest the caller sent, decoded the way the
// workflow's update handler receives it.
func (c *recordingClaimClient) request(t *testing.T) engine.ClaimRequest {
	t.Helper()
	args := c.updateInput.UpdateOptions.Args
	if len(args) != 1 {
		t.Fatalf("update carried %d args, want one ClaimRequest", len(args))
	}
	request, ok := args[0].(engine.ClaimRequest)
	if !ok {
		t.Fatalf("update arg is %T, want engine.ClaimRequest", args[0])
	}
	return request
}

type stubStartOperation struct{}

func (stubStartOperation) Get(context.Context) (client.WorkflowRun, error) { return nil, nil }

type stubUpdateHandle struct {
	winner engine.Claim
	err    error
}

func (stubUpdateHandle) WorkflowID() string { return "" }
func (stubUpdateHandle) RunID() string      { return "" }
func (stubUpdateHandle) UpdateID() string   { return "" }

func (h stubUpdateHandle) Get(_ context.Context, valuePtr any) error {
	if h.err != nil {
		return h.err
	}
	if valuePtr == nil {
		return nil
	}
	claim, ok := valuePtr.(*engine.Claim)
	if !ok {
		return errors.New("update result must decode into an engine.Claim")
	}
	*claim = h.winner
	return nil
}

func claimOf(runID string) engine.Claim {
	return engine.Claim{RunID: runID, Snapshot: []byte(runID)}
}

// TestTemporalIdempotency_JoinsTheKeysClaimInOneRoundTrip pins the claim
// round trip: the dedupe decision is taken by the server, in one call, using GA v1.50
// APIs. USE_EXISTING is what makes a second replica join the claim already
// holding the key instead of starting a rival, and it is required by
// UpdateWithStartWorkflow; waiting for Completed is what makes that same call
// return the winner.
func TestTemporalIdempotency_JoinsTheKeysClaimInOneRoundTrip(t *testing.T) {
	c := &recordingClaimClient{winner: claimOf("ork-1-1")}
	idempotency := engine.NewTemporalIdempotency(c, "", time.Hour)

	winner, err := idempotency.Claim(context.Background(), "order-42", claimOf("ork-1-1"))
	must.OKf(t, err, "Claim")
	if winner.RunID != "ork-1-1" {
		t.Errorf("winner = %q, want ork-1-1", winner.RunID)
	}

	if got := c.startOptions.ID; got != engine.ClaimWorkflowID("order-42") {
		t.Errorf("claim workflow id = %q, want the key's derived id %q",
			got, engine.ClaimWorkflowID("order-42"))
	}
	if got := c.startOptions.WorkflowIDConflictPolicy; got != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		t.Errorf("conflict policy = %v, want USE_EXISTING: a second replica must join the "+
			"claim, not start a rival", got)
	}
	// A run id is single-use for all time (temporal.go); a key is not, once
	// its claim has expired, so it must not inherit REJECT_DUPLICATE.
	if got := c.startOptions.WorkflowIDReusePolicy; got != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		t.Errorf("reuse policy = %v, want ALLOW_DUPLICATE: an expired key has to be reusable", got)
	}
	if got := c.startOptions.TaskQueue; got != task.DefaultQueue {
		t.Errorf("claim task queue = %q, want the engine's own %q", got, task.DefaultQueue)
	}
	if got := c.workflowType; got != engine.ClaimWorkflowType {
		t.Errorf("started workflow %v, want %q", got, engine.ClaimWorkflowType)
	}
	if got := c.updateInput.UpdateOptions.UpdateName; got != engine.ClaimUpdateName {
		t.Errorf("update name = %q, want %q", got, engine.ClaimUpdateName)
	}
	if got := c.updateInput.UpdateOptions.WaitForStage; got != client.WorkflowUpdateStageCompleted {
		t.Errorf("wait stage = %v, want Completed: an accepted update has not chosen a winner yet", got)
	}
	if c.updateInput.StartWorkflowOperation == nil {
		t.Error("update was not attached to a start operation, so it cannot create the claim")
	}
	if got := c.request(t); got.Candidate.RunID != "ork-1-1" || got.Supersedes != "" {
		t.Errorf("claim request = %+v, want candidate ork-1-1 superseding nothing", got)
	}
}

// TestTemporalIdempotency_TheClaimOutlivesItsTTLByAGraceOnly bounds a claim no
// worker ever picked up, without letting the server close one the workflow is
// still holding.
func TestTemporalIdempotency_TheClaimOutlivesItsTTLByAGraceOnly(t *testing.T) {
	c := &recordingClaimClient{winner: claimOf("ork-1-1")}
	idempotency := engine.NewTemporalIdempotency(c, "engine-queue", 2*time.Hour)

	if _, err := idempotency.Claim(context.Background(), "order-42", claimOf("ork-1-1")); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if got := c.startOptions.WorkflowExecutionTimeout; got <= 2*time.Hour {
		t.Errorf("execution timeout = %v, want longer than the %v TTL the workflow sleeps",
			got, 2*time.Hour)
	}
	// The TTL reaches the workflow as an argument, never as an environment
	// read inside workflow code (AGENTS.md §2.1).
	if len(c.workflowArgs) != 1 {
		t.Fatalf("claim workflow got %d args, want one input", len(c.workflowArgs))
	}
	in, ok := c.workflowArgs[0].(engine.ClaimWorkflowInput)
	if !ok {
		t.Fatalf("claim workflow arg is %T, want engine.ClaimWorkflowInput", c.workflowArgs[0])
	}
	if in.TTL != 2*time.Hour || in.Key != "order-42" {
		t.Errorf("claim workflow input = %+v, want key order-42 and a 2h ttl", in)
	}
	if got := c.startOptions.TaskQueue; got != "engine-queue" {
		t.Errorf("claim task queue = %q, want engine-queue", got)
	}
}

// TestTemporalIdempotency_ReportsALostCandidate: the caller has to be able to
// tell that it lost, because that is what stops it starting a run.
func TestTemporalIdempotency_ReportsALostCandidate(t *testing.T) {
	c := &recordingClaimClient{winner: claimOf("ork-9-9")}
	idempotency := engine.NewTemporalIdempotency(c, "", 0)

	winner, err := idempotency.Claim(context.Background(), "order-42", claimOf("ork-1-1"))
	must.OKf(t, err, "Claim")
	if winner.RunID != "ork-9-9" || string(winner.Snapshot) != "ork-9-9" {
		t.Errorf("winner = %+v, want the claim holder ork-9-9 with its own snapshot", winner)
	}
	if got := c.startOptions.WorkflowExecutionTimeout; got <= engine.DefaultClaimTTL {
		t.Errorf("a zero ttl produced timeout %v, want more than the %v default",
			got, engine.DefaultClaimTTL)
	}
}

// TestTemporalIdempotency_RebindNamesTheIDItReplaces: the move is a
// compare-and-swap, so the request has to carry the predecessor. Without it
// the claim workflow cannot tell the holder from any other caller.
func TestTemporalIdempotency_RebindNamesTheIDItReplaces(t *testing.T) {
	c := &recordingClaimClient{winner: claimOf("ork-2-2")}
	idempotency := engine.NewTemporalIdempotency(c, "", time.Hour)

	must.OKf(t, idempotency.Rebind(context.Background(), "order-42", claimOf("ork-2-2"), "ork-1-1"), "Rebind")
	got := c.request(t)
	if got.Candidate.RunID != "ork-2-2" || got.Supersedes != "ork-1-1" {
		t.Errorf("rebind request = %+v, want ork-2-2 superseding ork-1-1", got)
	}
}

// TestTemporalIdempotency_RebindFailsWhenTheKeyMovedElsewhere: a move the
// server refused must not be reported as done, or the key would be believed to
// name a run it does not.
func TestTemporalIdempotency_RebindFailsWhenTheKeyMovedElsewhere(t *testing.T) {
	c := &recordingClaimClient{winner: claimOf("ork-7-7")}
	idempotency := engine.NewTemporalIdempotency(c, "", time.Hour)

	err := idempotency.Rebind(context.Background(), "order-42", claimOf("ork-2-2"), "ork-1-1")
	if err == nil {
		t.Fatal("Rebind reported success while the key names another run")
	}
}

// TestTemporalIdempotency_ReportsAnUnreachableAuthority: the API turns this
// into a failed submission, so it must not be silently swallowed here.
func TestTemporalIdempotency_ReportsAnUnreachableAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		client *recordingClaimClient
	}{
		{"start", &recordingClaimClient{startErr: errors.New("frontend unreachable")}},
		{"read", &recordingClaimClient{getErr: errors.New("update lost")}},
		{"empty", &recordingClaimClient{winner: engine.Claim{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idempotency := engine.NewTemporalIdempotency(tc.client, "", time.Hour)
			if _, err := idempotency.Claim(context.Background(), "k", claimOf("ork-1-1")); err == nil {
				t.Fatal("Claim succeeded with no usable answer from the server")
			}
		})
	}
}

// TestClaimRequest_SurvivesTheDefaultDataConverter: the request crosses a
// process boundary as the update's argument, so a field the JSON converter
// drops would silently disable the reissue path or lose the winner's snapshot.
func TestClaimRequest_SurvivesTheDefaultDataConverter(t *testing.T) {
	request := engine.ClaimRequest{
		Candidate:  engine.Claim{RunID: "ork-1-1", Snapshot: []byte{0x00, 0x01, 0xff}, Fingerprint: "fp"},
		Supersedes: "ork-0-0",
	}
	encoded, err := stablejson.Marshal(request)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded engine.ClaimRequest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Candidate.RunID != request.Candidate.RunID ||
		decoded.Supersedes != request.Supersedes ||
		string(decoded.Candidate.Snapshot) != string(request.Candidate.Snapshot) {
		t.Errorf("round trip lost detail: %+v -> %+v", request, decoded)
	}
}

func claimDescribe(status enumspb.WorkflowExecutionStatus, started time.Time) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, StartTime: timestamppb.New(started)},
	}
}

// Lookup is the read a submission makes before it mints anything: only a
// running claim is a binding, and every failure is "no answer", never
// "unbound".
func TestTemporalIdempotency_Lookup(t *testing.T) {
	ctx := context.Background()
	started := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	held := engine.Claim{RunID: "ork-1-1", Fingerprint: "fp"}

	cases := []struct {
		name      string
		client    *recordingClaimClient
		bound     bool
		wantErr   bool
		wantQuery bool
	}{
		{"no claim execution", &recordingClaimClient{}, false, false, false},
		{"a claim that ended", &recordingClaimClient{describe: claimDescribe(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, started), queried: held}, false, false, false},
		{"a running claim", &recordingClaimClient{describe: claimDescribe(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, started), queried: held}, true, false, true},
		{"a running claim with no winner yet", &recordingClaimClient{describe: claimDescribe(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, started)}, false, false, true},
		{"the describe fails", &recordingClaimClient{describeErr: serviceerror.NewUnavailable("down")}, false, true, false},
		{"the query fails", &recordingClaimClient{describe: claimDescribe(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, started), queryErr: errors.New("unknown query")}, false, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, bound, err := engine.NewTemporalIdempotency(tc.client, "", time.Hour).Lookup(ctx, "k")
			if (err != nil) != tc.wantErr || bound != tc.bound {
				t.Fatalf("Lookup = %+v, %v, %v; want bound=%v err=%v", got, bound, err, tc.bound, tc.wantErr)
			}
			if bound && (got.RunID != held.RunID || got.Fingerprint != held.Fingerprint) {
				t.Fatalf("binding = %+v, want %+v", got, held)
			}
			if (tc.client.queries > 0) != tc.wantQuery {
				t.Fatalf("queried %d times", tc.client.queries)
			}
		})
	}

	at, err := engine.NewTemporalIdempotency(&recordingClaimClient{
		describe: claimDescribe(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, started)}, "", time.Hour).ClaimedAt(ctx, "k")
	if err != nil || !at.Equal(started) {
		t.Fatalf("ClaimedAt = %v, %v; want %v", at, err, started)
	}
	if _, err := engine.NewTemporalIdempotency(&recordingClaimClient{}, "", time.Hour).ClaimedAt(ctx, "k"); !errors.Is(err, engine.ErrClaimEnded) {
		t.Fatalf("ClaimedAt of no claim = %v, want ErrClaimEnded", err)
	}
}
