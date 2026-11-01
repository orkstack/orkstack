package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// cancelClient records the two ways CancelRun asks the cluster for a
// cancellation: the SDK's CancelWorkflow, and the service's own request,
// which answers with each of refusals in turn and then success.
type cancelClient struct {
	client.Client
	service  *cancelService
	canceled []string
}

func (c *cancelClient) CancelWorkflow(_ context.Context, workflowID, _ string) error {
	c.canceled = append(c.canceled, workflowID)
	return nil
}

func (c *cancelClient) WorkflowService() workflowservice.WorkflowServiceClient { return c.service }

type cancelService struct {
	workflowservice.WorkflowServiceClient
	refusals []error
	// endless, when set, is every answer once refusals are spent.
	endless  error
	onCall   func()
	requests []*workflowservice.RequestCancelWorkflowExecutionRequest
}

func (s *cancelService) RequestCancelWorkflowExecution(
	_ context.Context,
	in *workflowservice.RequestCancelWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	s.requests = append(s.requests, in)
	if s.onCall != nil {
		s.onCall()
	}
	if len(s.refusals) > 0 {
		err := s.refusals[0]
		s.refusals = s.refusals[1:]
		return nil, err
	}
	if s.endless != nil {
		return nil, s.endless
	}
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// newCancelStarter is a starter over a fake cluster answering refusals in
// turn, asking again 1 ms apart for at most 200 ms.
func newCancelStarter(refusals ...error) (*engine.TemporalStarter, *cancelClient) {
	fake := &cancelClient{service: &cancelService{refusals: refusals}}
	return engine.NewTemporalStarter(fake, "").WithClusterIdentity("ork-main", "engine-a").
		WithCancelRetryForTest(time.Millisecond, 200*time.Millisecond), fake
}

// A principal travels in the cancellation request itself, on the namespace
// and identity the client was dialed with, as a record the reader decodes.
func TestCancelRun_CarriesThePrincipalInTheRequest(t *testing.T) {
	starter, fake := newCancelStarter()
	if err := starter.CancelRun(context.Background(), "ork-1",
		engine.CancelRequest{Reason: "stuck", Principal: "dana@acme"}); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if len(fake.canceled) != 0 || len(fake.service.requests) != 1 {
		t.Fatalf("CancelWorkflow %d times, the service %d times; want the service once",
			len(fake.canceled), len(fake.service.requests))
	}
	sent := fake.service.requests[0]
	if sent.GetNamespace() != "ork-main" || sent.GetIdentity() != "engine-a" ||
		sent.GetWorkflowExecution().GetWorkflowId() != "ork-1" || sent.GetWorkflowExecution().GetRunId() != "" ||
		sent.GetRequestId() == "" {
		t.Fatalf("request = %v, want ork-1 on ork-main as engine-a, any execution, a request id", sent)
	}
	var record orkstackv1.RunCancelRequest
	if err := protojson.Unmarshal([]byte(sent.GetReason()), &record); err != nil {
		t.Fatalf("reason %q is not a RunCancelRequest: %v", sent.GetReason(), err)
	}
	if record.GetFormat() != 1 || record.GetPrincipal() != "dana@acme" {
		t.Fatalf("record = %v, want format 1 naming dana@acme", &record)
	}
}

// With no principal there is nothing to record: the cancellation is the
// SDK's, exactly as before.
func TestCancelRun_WithNoPrincipalIsTheSDKCancel(t *testing.T) {
	starter, fake := newCancelStarter()
	if err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Reason: "stuck"}); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if len(fake.canceled) != 1 || fake.canceled[0] != "ork-1" || len(fake.service.requests) != 0 {
		t.Fatalf("CancelWorkflow %v, the service %d times; want CancelWorkflow(ork-1) alone",
			fake.canceled, len(fake.service.requests))
	}
}

// A starter that was never told its namespace refuses rather than cancel
// without the name: the run would otherwise say nobody canceled it.
func TestCancelRun_RefusesAPrincipalItCannotRecord(t *testing.T) {
	fake := &cancelClient{service: &cancelService{}}
	starter := engine.NewTemporalStarter(fake, "")
	err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
	if !errors.Is(err, engine.ErrCancelNotWired) {
		t.Fatalf("CancelRun = %v, want ErrCancelNotWired", err)
	}
	if len(fake.canceled) != 0 || len(fake.service.requests) != 0 {
		t.Fatal("a refused cancellation reached the cluster")
	}
}

// A cluster that refuses for a moment — unavailable, rate-limiting, aborting
// the call — is asked again under the one request id it dedupes on, until it
// accepts or the budget is spent; any other refusal is the answer at once.
func TestCancelRun_AsksAgainOnlyWhileTheRefusalIsTransient(t *testing.T) {
	unavailable := serviceerror.NewUnavailable("frontend restarting")
	tests := []struct {
		name     string
		refusals []error
		endless  error
		wantSent int // 0: more than one, however many the budget held
		wantErr  bool
	}{
		{"unavailable twice, then accepted", []error{unavailable, unavailable}, nil, 3, false},
		{"rate-limited, then accepted", []error{serviceerror.NewResourceExhausted(0, "busy")}, nil, 2, false},
		{"aborted, then accepted", []error{serviceerror.NewAborted("try again")}, nil, 2, false},
		{"unavailable until the budget is spent", nil, unavailable, 0, true},
		{"not found is final", []error{serviceerror.NewNotFound("workflow execution already completed")}, nil, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			starter, fake := newCancelStarter(tt.refusals...)
			fake.service.endless = tt.endless
			err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("CancelRun = %v, want error %t", err, tt.wantErr)
			}
			sent := len(fake.service.requests)
			if (tt.wantSent > 0 && sent != tt.wantSent) || (tt.wantSent == 0 && sent < 2) {
				t.Fatalf("sent %d requests, want %d", sent, tt.wantSent)
			}
			for _, request := range fake.service.requests[1:] {
				if request.GetRequestId() != fake.service.requests[0].GetRequestId() {
					t.Fatalf("request ids %q and %q differ; the cluster could not dedupe them",
						fake.service.requests[0].GetRequestId(), request.GetRequestId())
				}
			}
			if tt.endless != nil {
				var refused *serviceerror.Unavailable
				if !errors.As(err, &refused) {
					t.Fatalf("a spent budget answered %v, want the cluster's last refusal", err)
				}
			}
		})
	}
}

// A caller that gives up while the starter waits to ask again is answered
// that it gave up — not that the cluster was unavailable, which would tell
// it to ask again.
func TestCancelRun_ACallerThatGivesUpIsToldSo(t *testing.T) {
	fake := &cancelClient{service: &cancelService{endless: serviceerror.NewUnavailable("frontend restarting")}}
	starter := engine.NewTemporalStarter(fake, "").WithClusterIdentity("ork-main", "engine-a").
		WithCancelRetryForTest(time.Hour, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	fake.service.onCall = cancel
	err := starter.CancelRun(ctx, "ork-1", engine.CancelRequest{Principal: "dana@acme"})
	var refused *serviceerror.Unavailable
	if !errors.Is(err, context.Canceled) || errors.As(err, &refused) {
		t.Fatalf("CancelRun = %v, want the caller's cancellation and not the cluster's refusal", err)
	}
	if len(fake.service.requests) != 1 {
		t.Fatalf("sent %d requests after the caller gave up, want 1", len(fake.service.requests))
	}
}
