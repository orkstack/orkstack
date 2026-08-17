//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// privateOrkerQueue names a task queue no other process on this cluster polls.
func privateOrkerQueue() string {
	return fmt.Sprintf("ork-tasks-engine-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// TestIntegration_TasksRunOnTheQueueTheEnginesOrkersPoll is the end of the
// ORK_ORKER_QUEUE story: an engine configured for a private orker queue must
// dispatch an ordinary orkflow's tasks there. The orkflow (examples/hello.ork.yaml)
// names no queue, and nothing in this test polls the shared default queue for
// activities — so if dispatch ignored the engine's configuration, every task
// would sit unassigned and the run would never leave RUNNING.
func TestIntegration_TasksRunOnTheQueueTheEnginesOrkersPoll(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()

	orkerQueue := privateOrkerQueue()
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	var observedMu sync.Mutex
	observedQueues := map[string]string{}

	// The run worker polls the engine's own queue and handles workflow tasks
	// only, exactly as `ork` does.
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)

	// The Orker stub polls the private queue and nothing else. Its record of
	// the queue each activity arrived on is the evidence: it comes from
	// Temporal's own activity info, not from the engine.
	orkerWorker := worker.New(temporalClient, orkerQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	orkerWorker.RegisterActivityWithOptions(
		func(actCtx context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			observedMu.Lock()
			observedQueues[in.TaskID] = activity.GetInfo(actCtx).TaskQueue
			observedMu.Unlock()
			return task.ExecuteTaskResult{
				TaskID: in.TaskID,
				Output: plugin.Payload{"echoed_task": in.TaskID},
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "orker": orkerWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	svc := api.NewService(api.Config{
		Workflows: engine.NewWorkflowStore(),
		Runs:      runs,
		Bus:       bus,
		Starter: engine.NewTemporalStarterWithOrkerQueue(
			temporalClient, task.DefaultQueue, orkerQueue),
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	httpClient := &http.Client{Transport: transport}
	baseURL := "http://" + listener.Addr().String()
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	if strings.Contains(string(source), "queue:") {
		t.Fatalf("the example now pins a queue; this test needs an orkflow that names none")
	}

	workflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)

	if _, err := workflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()

	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID,
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var scheduled []string
	var last *orkstackv1.Event
	for stream.Receive() {
		event := stream.Msg()
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			scheduled = append(scheduled, event.GetMessage())
		}
		last = event
	}
	if err := stream.Err(); err != nil {
		// A deadline here is the symptom this test exists for: the tasks were
		// scheduled onto a queue no worker polls, so the run never terminated
		// and the stream stayed open until the context expired.
		observedMu.Lock()
		reached := fmt.Sprintf("%v", observedQueues)
		observedMu.Unlock()
		t.Fatalf("event stream error: %v. Run %s never reached a terminal state; "+
			"the orker on %q executed %s, so check that dispatch follows the engine's orker queue",
			err, runID, orkerQueue, reached)
	}

	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("run %s ended on %v (%s), want RUN_SUCCEEDED. Tasks reached the queues %v; "+
			"an unassigned run means dispatch ignored the engine's orker queue %q",
			runID, last.GetType(), last.GetMessage(), observedQueues, orkerQueue)
	}

	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observedQueues) != 3 {
		t.Fatalf("orker executed %d tasks (%v), want the example's 3", len(observedQueues), observedQueues)
	}
	for taskID, queue := range observedQueues {
		if queue != orkerQueue {
			t.Errorf("task %q ran on queue %q, want the engine's orker queue %q", taskID, queue, orkerQueue)
		}
	}

	// The run's own event stream names the queue too, so an operator can see
	// where a task went without reading Temporal.
	for _, message := range scheduled {
		if !strings.Contains(message, orkerQueue) {
			t.Errorf("TASK_SCHEDULED event says %q, want it to name queue %q", message, orkerQueue)
		}
	}
}
