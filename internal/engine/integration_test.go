//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// temporalAddress is where the integration test expects a real Temporal
// frontend. Override with ORK_TEMPORAL_ADDRESS.
func temporalAddress() string {
	if addr := os.Getenv("ORK_TEMPORAL_ADDRESS"); addr != "" {
		return addr
	}
	return client.DefaultHostPort
}

// TestIntegration_HelloOrkRunSucceedsOnRealTemporal drives the whole M2 engine
// against a real Temporal cluster: the API compiles and stores the example
// workflow, submits a run, a real worker executes RunWorkflow with a stub
// ExecuteTask activity, and the run reaches STATUS_SUCCEEDED with observed
// events.
func TestIntegration_HelloOrkRunSucceedsOnRealTemporal(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Skipf("temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		t.Skipf("cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	var observedMu sync.Mutex
	observedRunInputs := map[string]plugin.Payload{}

	// Worker 1 matches production: it handles workflow and local-activity tasks
	// only, so it cannot steal ExecuteTask activities from an Orker sharing the
	// queue.
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	runWorker.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.WorkflowName})

	// Worker 2 is the activity-only Orker stub on the same shared queue.
	taskWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(
		func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			observedMu.Lock()
			observedRunInputs[in.TaskID] = in.RunInput
			observedMu.Unlock()
			upstream := make([]string, 0, len(in.Upstream))
			for id := range in.Upstream {
				upstream = append(upstream, id)
			}
			return task.ExecuteTaskResult{
				TaskID: in.TaskID,
				Output: plugin.Payload{
					"echoed_task":     in.TaskID,
					"echoed_plugin":   in.Plugin,
					"upstream_count":  float64(len(upstream)),
					"workflow":        in.WorkflowName,
					"workflow_versio": in.WorkflowVersion,
				},
				Artifacts: []plugin.ArtifactRef{{
					Name: "echo",
					URI:  fmt.Sprintf("file:///tmp/%s/%s.txt", in.RunID, in.TaskID),
				}},
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	// Worker 3 is activity-only for engine projections.
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	if err := runWorker.Start(); err != nil {
		t.Fatalf("start run worker: %v", err)
	}
	defer runWorker.Stop()
	if err := taskWorker.Start(); err != nil {
		t.Fatalf("start task worker: %v", err)
	}
	defer taskWorker.Stop()
	if err := eventWorker.Start(); err != nil {
		t.Fatalf("start event worker: %v", err)
	}
	defer eventWorker.Stop()

	svc := api.NewService(api.Config{
		Workflows: engine.NewWorkflowStore(),
		Runs:      runs,
		Bus:       bus,
		Starter:   engine.NewTemporalStarter(temporalClient, task.DefaultQueue),
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

	workflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)

	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	applied, err := workflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if applied.Msg.GetOrkflow().GetName() != "hello-ork" {
		t.Fatalf("applied workflow = %q, want hello-ork", applied.Msg.GetOrkflow().GetName())
	}

	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("submitted run %s", runID)

	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID,
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var observed []*orkstackv1.Event
	for stream.Receive() {
		observed = append(observed, stream.Msg())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream error: %v", err)
	}

	if len(observed) == 0 {
		t.Fatal("no events observed for the run")
	}
	var previous int64
	for _, event := range observed {
		if event.GetSequence() <= previous {
			t.Fatalf("event sequences are not strictly increasing: %d after %d", event.GetSequence(), previous)
		}
		previous = event.GetSequence()
	}
	last := observed[len(observed)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("last event = %v (%s), want RUN_SUCCEEDED", last.GetType(), last.GetMessage())
	}

	got, err := runClient.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	run := got.Msg.GetRun()
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run status = %v, want SUCCEEDED (error: %v)", run.GetStatus(), run.GetError())
	}
	if len(run.GetTaskRuns()) != 3 {
		t.Fatalf("task runs = %d, want 3", len(run.GetTaskRuns()))
	}
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Errorf("task %q status = %v, want SUCCEEDED", taskRun.GetTaskId(), taskRun.GetStatus())
		}
		if len(taskRun.GetArtifacts()) != 1 {
			t.Errorf("task %q artifacts = %v, want the stub's single ref", taskRun.GetTaskId(), taskRun.GetArtifacts())
		}
	}
	observedMu.Lock()
	observedCount := len(observedRunInputs)
	observedURL := observedRunInputs["fetch"]["url"]
	observedMu.Unlock()
	if observedCount != 3 {
		t.Errorf("stub ExecuteTask observed %d task inputs, want 3", observedCount)
	}
	if observedURL != "https://httpbin.org/json" {
		t.Errorf("stub ExecuteTask RunInput.url = %v, want declared hello-ork default", observedURL)
	}

	assertDeterministicReplay(t, ctx, temporalClient, runID)

	writeEventProof(t, runID, observed)
}

// assertDeterministicReplay replays the run's real Temporal history through
// RunWorkflow. A non-deterministic workflow body fails here, which is the
// direct check of the determinism boundary in AGENTS.md §1.3.
func assertDeterministicReplay(t *testing.T, ctx context.Context, c client.Client, runID string) {
	t.Helper()
	history := &history.History{}
	iter := c.GetWorkflowHistory(ctx, runID, "", false,
		enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read workflow history: %v", err)
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		t.Fatal("workflow history is empty")
	}

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.WorkflowName})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatalf("replaying the run's history failed (non-deterministic workflow): %v", err)
	}
	t.Logf("replayed %d history events deterministically", len(history.Events))
}

// writeEventProof records the observed event stream when ORK_PROOF_EVENT_FILE
// is set, producing the committed proof artifact.
func writeEventProof(t *testing.T, runID string, events []*orkstackv1.Event) {
	t.Helper()
	path := os.Getenv("ORK_PROOF_EVENT_FILE")
	if path == "" {
		return
	}
	marshaler := protojson.MarshalOptions{Multiline: false}
	raw := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		encoded, err := marshaler.Marshal(event)
		if err != nil {
			t.Fatalf("encode event: %v", err)
		}
		raw = append(raw, encoded)
	}
	document, err := json.MarshalIndent(map[string]any{
		"run_id":   runID,
		"captured": time.Now().UTC().Format(time.RFC3339),
		"events":   raw,
	}, "", "  ")
	if err != nil {
		t.Fatalf("encode proof: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	if err := os.WriteFile(path, append(document, '\n'), 0o644); err != nil {
		t.Fatalf("write proof: %v", err)
	}
	t.Logf("wrote event stream proof to %s", path)
}
