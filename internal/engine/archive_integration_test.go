//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// requireMinIO skips when the dev object store is not up, matching the
// artifact suite's behaviour: integration tests skip, they never fail, when the
// infrastructure is absent.
func requireMinIO(t *testing.T, cfg artifacts.Config) {
	t.Helper()
	base, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		t.Skipf("MinIO endpoint %q is unusable: %v", cfg.S3Endpoint, err)
	}
	health := base.JoinPath("/minio/health/live").String()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, health, nil)
	if err != nil {
		t.Skipf("MinIO health request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("MinIO not reachable at %s (%v); run `make infra-up`", health, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Skipf("MinIO health %s returned %d; run `make infra-up`", health, resp.StatusCode)
	}
	t.Logf("MinIO health %s -> %d", health, resp.StatusCode)
}

// enginePlant is one `ork` process: its Connect surface plus the in-process
// state that dies with it. The archive is deliberately not part of it.
type enginePlant struct {
	runs   *engine.RunStore
	bus    *engine.EventBus
	client orkstackv1connect.RunServiceClient
	flows  orkstackv1connect.OrkflowServiceClient
	stop   func()
}

// startEngine boots an engine over an existing archive. withWorkers starts the
// Temporal workers; a read-only restart does not need them, which is exactly
// how it proves the archive — not Temporal — answers the query.
func startEngine(
	t *testing.T,
	temporalClient client.Client,
	archive *engine.RunArchiver,
	withWorkers bool,
) *enginePlant {
	t.Helper()
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	var stops []func()

	if withWorkers {
		runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
			DisableRegistrationAliasing: true,
			LocalActivityWorkerOnly:     true,
		})
		runWorker.RegisterWorkflowWithOptions(engine.RunWorkflow,
			workflow.RegisterOptions{Name: engine.WorkflowName})

		// Activity-only stub Orker on the shared queue.
		taskWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
		})
		taskWorker.RegisterActivityWithOptions(
			func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
				return task.ExecuteTaskResult{
					TaskID: in.TaskID,
					Output: plugin.Payload{"echoed_task": in.TaskID},
					Artifacts: []plugin.ArtifactRef{{
						Name: "echo",
						URI: fmt.Sprintf("s3://orkstack-artifacts/orkflows/%s/%s/%s/echo",
							in.WorkflowName, in.TaskID, in.RunID),
					}},
				}, nil
			},
			activity.RegisterOptions{Name: task.ActivityExecuteTask},
		)

		eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
		})
		eventWorker.RegisterActivityWithOptions(
			engine.NewArchivingEventActivities(bus, runs, archive).PublishEvent,
			activity.RegisterOptions{Name: engine.ActivityPublishEvent},
		)

		for _, w := range []worker.Worker{runWorker, taskWorker, eventWorker} {
			if err := w.Start(); err != nil {
				t.Fatalf("start worker: %v", err)
			}
			stops = append(stops, w.Stop)
		}
	}

	svc := api.NewService(api.Config{
		Workflows: engine.NewWorkflowStore(),
		Runs:      runs,
		Bus:       bus,
		Starter:   engine.NewTemporalStarter(temporalClient, task.DefaultQueue),
		Archive:   archive,
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

	stops = append(stops, func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	return &enginePlant{
		runs:   runs,
		bus:    bus,
		client: orkstackv1connect.NewRunServiceClient(httpClient, baseURL),
		flows:  orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL),
		stop: func() {
			for i := len(stops) - 1; i >= 0; i-- {
				stops[i]()
			}
		},
	}
}

// TestIntegration_RunArchivesToS3AndSurvivesEngineRestart is the whole M7.6
// claim on real infrastructure: a run executed on real Temporal is written to
// real S3 as JSONL, and a second engine with an empty read model still answers
// `run get` and `run list --status SUCCEEDED` for it.
func TestIntegration_RunArchivesToS3AndSurvivesEngineRestart(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Skipf("temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireMinIO(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		t.Skipf("cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()

	store, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	archive := engine.NewRunArchiver(store)

	// ---- engine process 1: execute a real run --------------------------------
	first := startEngine(t, temporalClient, archive, true)

	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	if _, err := first.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	})); err != nil {
		first.stop()
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	submitted, err := first.client.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		first.stop()
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("submitted run %s", runID)

	stream, err := first.client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID,
	}))
	if err != nil {
		first.stop()
		t.Fatalf("StreamEvents: %v", err)
	}
	var streamed []*orkstackv1.Event
	for stream.Receive() {
		streamed = append(streamed, stream.Msg())
	}
	if err := stream.Err(); err != nil {
		stream.Close()
		first.stop()
		t.Fatalf("event stream: %v", err)
	}
	stream.Close()
	if len(streamed) == 0 {
		first.stop()
		t.Fatal("no events observed")
	}
	if last := streamed[len(streamed)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		first.stop()
		t.Fatalf("last event = %s (%s), want RUN_SUCCEEDED", last.GetType(), last.GetMessage())
	}

	// ---- acceptance 1: the run is JSONL in S3, at the contract path ----------
	uri, err := store.URI(runID)
	if err != nil {
		first.stop()
		t.Fatalf("archive URI: %v", err)
	}
	wantURI := "s3://" + cfg.S3Bucket + "/metadata/runs/" + runID + ".jsonl"
	if uri != wantURI {
		first.stop()
		t.Fatalf("archive URI = %q, want %q", uri, wantURI)
	}

	body := awaitArchivedBody(t, ctx, store, runID)
	archivedRun, archivedEvents, err := engine.UnmarshalRunArchive(body)
	if err != nil {
		first.stop()
		t.Fatalf("decode archived body: %v", err)
	}
	if archivedRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		first.stop()
		t.Fatalf("archived status = %s, want SUCCEEDED", archivedRun.GetStatus())
	}
	if len(archivedRun.GetTaskRuns()) != 3 {
		first.stop()
		t.Fatalf("archived run has %d task runs, want 3", len(archivedRun.GetTaskRuns()))
	}

	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 1+len(archivedEvents) {
		first.stop()
		t.Fatalf("archive has %d lines for %d events, want one event per line",
			len(lines), len(archivedEvents))
	}
	// Every event the client streamed must be in the archive, in order. The
	// archive additionally holds RUN_CREATED, which predates the subscription.
	assertContainsStreamedEvents(t, archivedEvents, streamed)

	// ---- the engine process dies --------------------------------------------
	first.stop()

	// ---- acceptance 3 + 4: a fresh engine still answers for that run ---------
	second := startEngine(t, temporalClient, archive, false)
	defer second.stop()
	if _, live := second.runs.Get(runID); live {
		t.Fatal("the restarted engine started with a populated read model; the test proves nothing")
	}

	got, err := second.client.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun after restart: %v", err)
	}
	recovered := got.Msg.GetRun()
	if recovered.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("recovered status = %s, want SUCCEEDED", recovered.GetStatus())
	}
	if recovered.GetWorkflowName() != "hello-ork" || len(recovered.GetTaskRuns()) != 3 {
		t.Fatalf("recovered run lost detail: %d task runs of %q",
			len(recovered.GetTaskRuns()), recovered.GetWorkflowName())
	}
	if len(recovered.GetTaskRuns()[0].GetArtifacts()) != 1 {
		t.Fatal("recovered run lost its artifact references")
	}

	// ---- acceptance 2: `run list --status SUCCEEDED` is answered from S3 -----
	listed, err := second.client.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_SUCCEEDED,
	}))
	if err != nil {
		t.Fatalf("ListRuns after restart: %v", err)
	}
	if !containsRun(listed.Msg.GetRuns(), runID) {
		t.Fatalf("`run list --orkflow hello-ork --status SUCCEEDED` did not return %s (got %v)",
			runID, runIDsOf(listed.Msg.GetRuns()))
	}
	for _, run := range listed.Msg.GetRuns() {
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("status filter leaked %s (%s)", run.GetId(), run.GetStatus())
		}
		if run.GetWorkflowName() != "hello-ork" {
			t.Fatalf("orkflow filter leaked %s (%s)", run.GetId(), run.GetWorkflowName())
		}
	}

	failedOnly, err := second.client.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_FAILED,
	}))
	if err != nil {
		t.Fatalf("ListRuns(FAILED): %v", err)
	}
	if containsRun(failedOnly.Msg.GetRuns(), runID) {
		t.Fatalf("a succeeded run was returned by --status FAILED")
	}

	writeArchiveProof(t, runID, uri, body, recovered, listed.Msg.GetRuns())
}

// awaitArchivedBody polls for the archived object: the terminal projection
// activity completes just after the workflow returns, so the body can land a
// moment after the event stream closes.
func awaitArchivedBody(
	t *testing.T,
	ctx context.Context,
	store *artifacts.S3RunArchive,
	runID string,
) []byte {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		body, err := store.GetRun(ctx, runID)
		if err == nil {
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s was never archived: %v", runID, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s was never archived: %v", runID, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// assertContainsStreamedEvents checks the archive is a superset of what a
// client observed live, in the same order.
func assertContainsStreamedEvents(t *testing.T, archived, streamed []*orkstackv1.Event) {
	t.Helper()
	bySequence := make(map[int64]*orkstackv1.Event, len(archived))
	for _, event := range archived {
		bySequence[event.GetSequence()] = event
	}
	previous := int64(0)
	for _, want := range streamed {
		got, ok := bySequence[want.GetSequence()]
		if !ok {
			t.Fatalf("event %d (%s) is missing from the archive", want.GetSequence(), want.GetType())
		}
		if got.GetType() != want.GetType() || got.GetTaskId() != want.GetTaskId() {
			t.Fatalf("archived event %d = %s/%s, streamed %s/%s", want.GetSequence(),
				got.GetType(), got.GetTaskId(), want.GetType(), want.GetTaskId())
		}
		if want.GetSequence() <= previous {
			t.Fatalf("streamed sequences are not increasing: %d after %d", want.GetSequence(), previous)
		}
		previous = want.GetSequence()
	}
}

func containsRun(runs []*orkstackv1.Run, runID string) bool {
	for _, run := range runs {
		if run.GetId() == runID {
			return true
		}
	}
	return false
}

// writeArchiveProof records the M7.6 artifacts when ORK_PROOF_M76_DIR is set:
// the run's raw JSONL, the recovered run, and the list query's answer.
func writeArchiveProof(
	t *testing.T,
	runID, uri string,
	body []byte,
	recovered *orkstackv1.Run,
	listed []*orkstackv1.Run,
) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_M76_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}

	write := func(name string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("run-archive.jsonl", body)
	write("run-archive-uri.txt", []byte(uri+"\n"))

	recoveredJSON, err := api.MarshalJSON(recovered)
	if err != nil {
		t.Fatalf("encode recovered run: %v", err)
	}
	write("run-get-after-restart.json", indentJSON(t, recoveredJSON))

	listJSON, err := api.MarshalJSON(&orkstackv1.ListRunsResponse{Runs: listed})
	if err != nil {
		t.Fatalf("encode run list: %v", err)
	}
	write("run-list-succeeded.json", indentJSON(t, listJSON))
	t.Logf("wrote M7.6 proof artifacts for run %s to %s", runID, dir)
}

func indentJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		t.Fatalf("indent json: %v", err)
	}
	pretty.WriteByte('\n')
	return pretty.Bytes()
}
