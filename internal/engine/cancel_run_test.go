package engine_test

import (
	"context"
	"errors"
	"testing"

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
	requests []*workflowservice.RequestCancelWorkflowExecutionRequest
}

func (s *cancelService) RequestCancelWorkflowExecution(
	_ context.Context,
	in *workflowservice.RequestCancelWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	s.requests = append(s.requests, in)
	if len(s.refusals) > 0 {
		err := s.refusals[0]
		s.refusals = s.refusals[1:]
		return nil, err
	}
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

func newCancelStarter(refusals ...error) (*engine.TemporalStarter, *cancelClient) {
	fake := &cancelClient{service: &cancelService{refusals: refusals}}
	return engine.NewTemporalStarter(fake, "").WithClusterIdentity("ork-main", "engine-a"), fake
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

// A cluster that answers Unavailable is asked again, a bounded number of
// times, under the one request id it dedupes on; any other refusal is the
// answer at once.
func TestCancelRun_AsksAgainOnlyWhileTheClusterIsUnavailable(t *testing.T) {
	unavailable := serviceerror.NewUnavailable("frontend restarting")
	tests := []struct {
		name     string
		refusals []error
		wantSent int
		wantErr  bool
	}{
		{"unavailable twice, then accepted", []error{unavailable, unavailable}, 3, false},
		{"unavailable every time", []error{unavailable, unavailable, unavailable, unavailable}, 3, true},
		{"not found is final", []error{serviceerror.NewNotFound("workflow execution already completed")}, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			starter, fake := newCancelStarter(tt.refusals...)
			err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("CancelRun = %v, want error %t", err, tt.wantErr)
			}
			if got := len(fake.service.requests); got != tt.wantSent {
				t.Fatalf("sent %d requests, want %d", got, tt.wantSent)
			}
			for _, sent := range fake.service.requests[1:] {
				if sent.GetRequestId() != fake.service.requests[0].GetRequestId() {
					t.Fatalf("request ids %q and %q differ; the cluster could not dedupe them",
						fake.service.requests[0].GetRequestId(), sent.GetRequestId())
				}
			}
		})
	}
}
