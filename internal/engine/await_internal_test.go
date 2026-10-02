package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func awaitPlanForTest() RunPlan {
	return RunPlan{
		Namespace: "default",
		Routes:    map[string]string{"main": "ork-tasks"},
		Awaits: map[string]AwaitCredential{
			"gate": {Salt: "00112233445566778899aabbccddeeff", Digest: strings.Repeat("ab", 32)},
		},
	}
}

func TestTemporalRunReader_RunStartDecodesDefinitionAndPlan(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "awaited", Version: "1.0.0", Tasks: []*orkstackv1.Task{
		{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60, Callback: true}},
	}}
	plan := awaitPlanForTest()
	started := &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(readerEpoch),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input:        payloadsOf(t, "ork-await-1", wf, &orkstackv1.Payload{}, plan),
			},
		},
	}
	reader, _ := readerOver([]*historypb.HistoryEvent{started}, "ork-await-1")

	record, err := reader.RunStart(context.Background(), "ork-await-1")
	if err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if record.Orkflow.GetName() != "awaited" || !record.Orkflow.GetTasks()[0].GetAwait().GetCallback() {
		t.Fatalf("definition = %v", record.Orkflow)
	}
	if got := record.Plan.Awaits["gate"]; got != plan.Awaits["gate"] {
		t.Fatalf("plan credential = %+v, want %+v", got, plan.Awaits["gate"])
	}

	t.Run("an unknown run is ErrRunNotFound", func(t *testing.T) {
		_, err := reader.RunStart(context.Background(), "ork-absent-1")
		if !errors.Is(err, ErrRunNotFound) {
			t.Fatalf("error = %v, want ErrRunNotFound", err)
		}
	})
	t.Run("an undecodable plan is an error, not an empty record", func(t *testing.T) {
		bad := &historypb.HistoryEvent{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: payloadsOf(t, "ork-bad-1", wf, &orkstackv1.Payload{}, "not a plan"),
				},
			},
		}
		badReader, _ := readerOver([]*historypb.HistoryEvent{bad}, "ork-bad-1")
		record, err := badReader.RunStart(context.Background(), "ork-bad-1")
		if err == nil || record != nil {
			t.Fatalf("RunStart = %v, %v; want an error and no record", record, err)
		}
		if !strings.Contains(err.Error(), "ork-bad-1") {
			t.Fatalf("error %q does not name the run", err)
		}
	})
}

// signalClient is the client.Client seam SignalAwait uses; every other method
// is the embedded nil interface and panics if reached.
type signalClient struct {
	client.Client
	err      error
	name     string
	workflow string
	arg      any
}

func (c *signalClient) SignalWorkflow(_ context.Context, workflowID, _ string, signalName string, arg any) error {
	c.workflow, c.name, c.arg = workflowID, signalName, arg
	return c.err
}

func TestTemporalStarter_SignalAwaitMapsAClosedRunToErrRunEnded(t *testing.T) {
	sig := &orkstackv1.AwaitSignal{TaskId: "gate"}

	ok := &signalClient{}
	if err := (&TemporalStarter{client: ok}).SignalAwait(context.Background(), "ork-1", sig); err != nil {
		t.Fatalf("SignalAwait: %v", err)
	}
	if ok.workflow != "ork-1" || ok.name != ResolveAwaitSignal || ok.arg != sig {
		t.Fatalf("signalled %q/%q with %v", ok.workflow, ok.name, ok.arg)
	}

	closed := &signalClient{err: serviceerror.NewNotFound("workflow execution already completed")}
	err := (&TemporalStarter{client: closed}).SignalAwait(context.Background(), "ork-1", sig)
	if !errors.Is(err, ErrRunEnded) {
		t.Fatalf("closed run: error = %v, want ErrRunEnded", err)
	}
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("closed run: error = %v, want the server's NotFound joined", err)
	}

	down := &signalClient{err: serviceerror.NewUnavailable("cluster is down")}
	err = (&TemporalStarter{client: down}).SignalAwait(context.Background(), "ork-1", sig)
	if err == nil || errors.Is(err, ErrRunEnded) {
		t.Fatalf("unavailable cluster: error = %v, want a wrapped error that is not ErrRunEnded", err)
	}
}

// drainTestWorkflow queues resolutions for two awaits that never open, then
// drains them, and reports whether the channel was left empty.
func drainTestWorkflow(ctx workflow.Context) (bool, error) {
	wf := &orkstackv1.Orkflow{Name: "drain", Tasks: []*orkstackv1.Task{
		{Id: "first", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
		{Id: "second", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
	}}
	state := newRunState(ctx, "run-drain", wf, nil, RunPlan{Namespace: "default"})
	// Both signals were delivered before this wakes.
	if err := workflow.Sleep(ctx, time.Minute); err != nil {
		return false, err
	}
	state.drainResolutions(ctx)
	var leftover *orkstackv1.AwaitSignal
	return !state.resolutions.ReceiveAsync(&leftover), nil
}

func TestDrainResolutions_RecordsEveryQueuedSignalAsALog(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(drainTestWorkflow)
	var (
		mu     sync.Mutex
		events []*orkstackv1.Event
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, _ int64) (string, error) { return "memory://x", nil },
		activity.RegisterOptions{Name: ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, event *orkstackv1.Event, _ string, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
			return nil
		},
		activity.RegisterOptions{Name: ActivityPublishEvent},
	)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ResolveAwaitSignal, &orkstackv1.AwaitSignal{
			TaskId: "first", Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice",
		})
		env.SignalWorkflow(ResolveAwaitSignal, &orkstackv1.AwaitSignal{
			TaskId: "second", Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK,
		})
	}, time.Second)
	env.ExecuteWorkflow(drainTestWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var empty bool
	if err := env.GetWorkflowResult(&empty); err != nil || !empty {
		t.Fatalf("channel left empty = %v (%v), want true", empty, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("%d events, want two ignored-resolution logs: %v", len(events), events)
	}
	want := []string{
		"resolution ignored: task is PENDING (via api by alice)",
		"resolution ignored: task is PENDING (via callback)",
	}
	for i, e := range events {
		if e.GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_LOG || e.GetMessage() != want[i] ||
			e.GetStatus() != orkstackv1.Status_STATUS_PENDING {
			t.Fatalf("event %d = %v %v %q, want TASK_LOG PENDING %q", i, e.GetType(), e.GetStatus(), e.GetMessage(), want[i])
		}
	}
}
