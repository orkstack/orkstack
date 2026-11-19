package engine_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// pauseService records the signals SignalPause sends and answers each with
// the next refusal, then success. A test double for the refusals a cluster
// cannot be made to give on demand (Unavailable, then success).
type pauseService struct {
	workflowservice.WorkflowServiceClient
	refusals []error
	requests []*workflowservice.SignalWorkflowExecutionRequest
}

func (s *pauseService) SignalWorkflowExecution(
	_ context.Context, in *workflowservice.SignalWorkflowExecutionRequest, _ ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	s.requests = append(s.requests, in)
	if len(s.requests) > runaway {
		return nil, errors.New("the retry went on past any budget")
	}
	if len(s.refusals) > 0 {
		err := s.refusals[0]
		s.refusals = s.refusals[1:]
		return nil, err
	}
	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}

type pauseClient struct {
	client.Client
	service *pauseService
}

func (c *pauseClient) WorkflowService() workflowservice.WorkflowServiceClient { return c.service }

func pauseStarter(service *pauseService) *engine.TemporalStarter {
	clock := &cancelClock{}
	return engine.NewTemporalStarter(&pauseClient{service: service}, "").
		WithClusterIdentity("ork-main", "engine-a").WithCancelClockForTest(clock.bound, clock.wait)
}

func pauseSignal() *orkstackv1.RunPauseSignal {
	return &orkstackv1.RunPauseSignal{Format: 1, Pause: true, Principal: "alice", Reason: "db",
		TimeoutSeconds: 3600, RequestId: "req-1"}
}

// One signal under the request's own id, asked again under that id on a
// transient refusal, and carrying the request in the shape the run decodes.
func TestSignalPause_SendsOneRequestUnderItsID(t *testing.T) {
	service := &pauseService{refusals: []error{serviceerror.NewUnavailable("frontend restarting")}}
	must.OKf(t, pauseStarter(service).SignalPause(context.Background(), "ork-r", pauseSignal()), "SignalPause")
	if len(service.requests) != 2 {
		t.Fatalf("sent %d requests, want 2 (one refusal, asked again)", len(service.requests))
	}
	for _, request := range service.requests {
		if request.GetRequestId() != "req-1" || request.GetSignalName() != engine.PauseSignal ||
			request.GetWorkflowExecution().GetWorkflowId() != "ork-r" || request.GetNamespace() != "ork-main" ||
			request.GetIdentity() != "engine-a" {
			t.Fatalf("request = %v", request)
		}
	}
	payloads := service.requests[0].GetInput().GetPayloads()
	if len(payloads) != 1 {
		t.Fatalf("input has %d payloads, want 1", len(payloads))
	}
	var decoded map[string]any
	must.OKf(t, json.Unmarshal(payloads[0].GetData(), &decoded), "decode the payload")
	if decoded["format"] != float64(1) || decoded["requestId"] != "req-1" || decoded["timeoutSeconds"] != "3600" ||
		decoded["principal"] != "alice" || decoded["pause"] != true {
		t.Fatalf("payload = %s, want the run's protojson names", payloads[0].GetData())
	}
}

// A closed run is ErrRunEnded; a request with no id and an unwired starter
// send nothing.
func TestSignalPause_Refusals(t *testing.T) {
	service := &pauseService{refusals: []error{serviceerror.NewNotFound("workflow execution already completed")}}
	err := pauseStarter(service).SignalPause(context.Background(), "ork-r", pauseSignal())
	if !errors.Is(err, engine.ErrRunEnded) {
		t.Fatalf("closed run: %v, want ErrRunEnded", err)
	}
	if len(service.requests) != 1 {
		t.Fatalf("asked a closed run %d times, want once", len(service.requests))
	}

	service = &pauseService{}
	noID := pauseSignal()
	noID.RequestId = ""
	if err := pauseStarter(service).SignalPause(context.Background(), "ork-r", noID); err == nil {
		t.Fatal("sent a request with no id")
	}
	unwired := engine.NewTemporalStarter(&pauseClient{service: service}, "")
	if err := unwired.SignalPause(context.Background(), "ork-r", pauseSignal()); !errors.Is(err, engine.ErrPauseNotWired) {
		t.Fatalf("unwired: %v, want ErrPauseNotWired", err)
	}
	if len(service.requests) != 0 {
		t.Fatalf("sent %d requests, want none", len(service.requests))
	}
}
