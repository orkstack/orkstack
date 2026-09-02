//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
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
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// temporalAddress is where the integration test expects a real Temporal
// frontend. Override with ORK_TEMPORAL_ADDRESS.
func temporalAddress() string {
	if addr := os.Getenv("ORK_TEMPORAL_ADDRESS"); addr != "" {
		return addr
	}
	return client.DefaultHostPort
}

// TestIntegration_HelloOrkRunSucceedsOnRealTemporal drives the whole engine
// against a real Temporal cluster and the real orkflow store: the API compiles
// the example orkflow into S3, submits a run, a real worker executes
// RunWorkflow with a stub ExecuteTask activity, and the run reaches
// STATUS_SUCCEEDED with observed events.
func TestIntegration_HelloOrkRunSucceedsOnRealTemporal(t *testing.T) {
	orkflows := newDurableOrkflowStore(t)
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

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	var observedMu sync.Mutex
	observedRunInputs := map[string]map[string]any{}

	// Worker 1 matches production: it handles workflow and local-activity tasks
	// only, so it cannot steal ExecuteTask activities from an Orker sharing the
	// queue.
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	// Worker 2 is the activity-only Orker stub on the same shared queue.
	taskWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			observedMu.Lock()
			observedRunInputs[in.TaskId] = in.GetRunInput().GetData().AsMap()
			observedMu.Unlock()
			upstream := make([]string, 0, len(in.GetUpstream()))
			for _, ref := range in.GetUpstream() {
				upstream = append(upstream, ref.GetTaskId())
			}
			return &orkstackv1.ExecuteTaskResult{
				TaskId: in.TaskId,
				ValuesJson: valuesJSON(map[string]any{
					"echoed_task":     in.TaskId,
					"echoed_image":    in.Image.GetReference(),
					"upstream_count":  float64(len(upstream)),
					"workflow":        in.OrkflowName,
					"workflow_versio": in.OrkflowVersion,
				}),
				Artifacts: []*orkstackv1.ArtifactRef{{
					Name: "echo",
					Uri:  fmt.Sprintf("file:///tmp/%s/%s.txt", in.RunId, in.TaskId),
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
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
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
		Orkflows: orkflows,
		Runs:     runs,
		Bus:      bus,
		Starter:  engine.NewTemporalStarter(temporalClient, task.DefaultQueue),
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

	orkflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)

	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	applied, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if applied.Msg.GetOrkflow().GetName() != "hello-ork" {
		t.Fatalf("applied orkflow = %q, want hello-ork", applied.Msg.GetOrkflow().GetName())
	}

	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "hello-ork",
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
// direct check of the determinism boundary in AGENTS.md §2.1.
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
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatalf("replaying the run's history failed (non-deterministic workflow): %v", err)
	}
	t.Logf("replayed %d history events deterministically", len(history.Events))
}

// writeEventProof records the observed event stream as evidence.
func writeEventProof(t *testing.T, runID string, events []*orkstackv1.Event) {
	t.Helper()
	marshaler := protojson.MarshalOptions{Multiline: false}
	raw := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		encoded, err := marshaler.Marshal(event)
		if err != nil {
			t.Fatalf("encode event: %v", err)
		}
		raw = append(raw, encoded)
	}
	proof.WriteJSON(t, "engine", "run-event-stream.json", map[string]any{
		"run_id":   runID,
		"captured": time.Now().UTC().Format(time.RFC3339),
		"events":   raw,
	})
}
