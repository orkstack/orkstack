//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// servedBody is what the hermetic stand-in for the public fetch URL returns.
// It is deliberately JSON with a trailing newline so the shell task's `tr -d`
// step observably changes the bytes.
const servedBody = "{\"slideshow\": {\"title\": \"Sample Slide Show\", \"slides\": 2}}\n"

func temporalAddress() string {
	if addr := os.Getenv("ORK_TEMPORAL_ADDRESS"); addr != "" {
		return addr
	}
	return client.DefaultHostPort
}

// TestIntegration_OrkerRunsHelloOrkOnRealTemporalAndMinIO drives the M3 Orker
// end to end: a real Orker worker on task.DefaultQueue executes the http, shell
// and file plugins for examples/hello.ork.yaml while the engine's RunWorkflow
// orchestrates, with artifacts stored in MinIO.
func TestIntegration_OrkerRunsHelloOrkOnRealTemporalAndMinIO(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Skipf("temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	storeConfig := artifacts.ConfigFromEnv()
	storeConfig.Backend = artifacts.BackendS3
	requireMinIO(t, storeConfig.S3Endpoint)
	store, err := artifacts.New(storeConfig)
	if err != nil {
		t.Skipf("cannot open the s3 artifact store at %s: %v", storeConfig.S3Endpoint, err)
	}

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

	// The fetch step targets a local httptest server so the run is hermetic.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, servedBody)
	}))
	defer origin.Close()

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	// Worker 1: the engine's workflow worker (local activities only).
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	runWorker.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name: engine.WorkflowName,
	})

	// Worker 2: the real Orker on the same shared queue.
	orkerWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	orkerWorker.RegisterActivityWithOptions(
		orker.NewActivities(orker.BuiltinRegistry(), store).ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	// Worker 3: the engine's event projection worker.
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
		t.Fatalf("read example workflow: %v", err)
	}
	if _, err := workflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	input, err := structpb.NewStruct(map[string]any{"url": origin.URL})
	if err != nil {
		t.Fatalf("build run input: %v", err)
	}
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
		Input:        &orkstackv1.Payload{Data: input},
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("submitted run %s against origin %s", runID, origin.URL)

	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID,
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() {
		event := stream.Msg()
		t.Logf("event %d %s %s %s", event.GetSequence(), event.GetType(), event.GetTaskId(), event.GetMessage())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream error: %v", err)
	}
	_ = stream.Close()

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

	taskRuns := map[string]*orkstackv1.TaskRun{}
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Errorf("task %q status = %v (error: %v), want SUCCEEDED",
				taskRun.GetTaskId(), taskRun.GetStatus(), taskRun.GetError())
		}
		taskRuns[taskRun.GetTaskId()] = taskRun
	}
	for _, id := range []string{"fetch", "transform", "save"} {
		if taskRuns[id] == nil {
			t.Fatalf("run has no task %q", id)
		}
	}

	// 1. fetch produced the response body as an artifact holding exact bytes.
	fetchArtifact := artifactNamed(t, taskRuns["fetch"], "body")
	fetched := readArtifact(t, ctx, store, fetchArtifact.GetUri())
	if fetched != servedBody {
		t.Fatalf("fetch artifact = %q, want the served bytes %q", fetched, servedBody)
	}
	if status := taskRuns["fetch"].GetOutput().GetData().GetFields()["status"].GetNumberValue(); status != 200 {
		t.Errorf("fetch output status = %v, want 200", status)
	}

	// 2. transform consumed the materialized artifact through the shell.
	stdout := taskRuns["transform"].GetOutput().GetData().GetFields()["stdout"].GetStringValue()
	wantStdout := strings.ReplaceAll(servedBody, "\n", "")
	if stdout != wantStdout {
		t.Fatalf("transform stdout = %q, want the materialized artifact contents %q", stdout, wantStdout)
	}
	titleArtifact := artifactNamed(t, taskRuns["transform"], "title")
	if title := readArtifact(t, ctx, store, titleArtifact.GetUri()); title != wantStdout {
		t.Fatalf("transform artifact = %q, want %q", title, wantStdout)
	}

	// 3. save persisted the shell output through the file plugin.
	savedArtifact := artifactNamed(t, taskRuns["save"], "result.txt")
	if saved := readArtifact(t, ctx, store, savedArtifact.GetUri()); saved != wantStdout {
		t.Fatalf("saved artifact = %q, want %q", saved, wantStdout)
	}
	if path := taskRuns["save"].GetOutput().GetData().GetFields()["path"].GetStringValue(); path != "result.txt" {
		t.Errorf("save output path = %q, want result.txt", path)
	}

	// 4. M7.4: every artifact this run produced is addressed by the run that
	// produced it, not by a content hash.
	assertReadableArtifactURIs(t, storeConfig.S3Bucket, "hello-ork", runID, taskRuns)

	writeHandoffProof(t, runID, origin.URL, taskRuns)
	writeArtifactURIProof(t, runID, got.Msg, taskRuns)
}

// assertReadableArtifactURIs pins the M7.4 layout on a real run:
//
//	s3://{bucket}/orkflows/{orkflow-name}/{task-id}/{run-id}/{artifact-name}
func assertReadableArtifactURIs(
	t *testing.T,
	bucket, orkflow, runID string,
	taskRuns map[string]*orkstackv1.TaskRun,
) {
	t.Helper()
	seen := 0
	for _, id := range []string{"fetch", "transform", "save"} {
		for _, ref := range taskRuns[id].GetArtifacts() {
			want := fmt.Sprintf("s3://%s/orkflows/%s/%s/%s/%s", bucket, orkflow, id, runID, ref.GetName())
			if ref.GetUri() != want {
				t.Errorf("task %q artifact %q uri = %q, want %q", id, ref.GetName(), ref.GetUri(), want)
			}
			if strings.Contains(ref.GetUri(), ref.GetSha256()) {
				t.Errorf("task %q artifact %q uri %q still carries the content hash", id, ref.GetName(), ref.GetUri())
			}
			seen++
		}
	}
	if seen != 3 {
		t.Errorf("checked %d artifact URIs, want 3 (fetch/body, transform/title, save/result.txt)", seen)
	}
}

// requireMinIO skips the test unless the MinIO endpoint answers its health
// probe, matching the engine test's reachability convention.
func requireMinIO(t *testing.T, endpoint string) {
	t.Helper()
	probe := strings.TrimSuffix(endpoint, "/") + "/minio/health/live"
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(probe)
	if err != nil {
		t.Skipf("minio is not reachable at %s: %v", endpoint, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("minio health probe %s returned %d", probe, resp.StatusCode)
	}
}

func artifactNamed(t *testing.T, taskRun *orkstackv1.TaskRun, name string) *orkstackv1.ArtifactRef {
	t.Helper()
	for _, ref := range taskRun.GetArtifacts() {
		if ref.GetName() == name {
			return ref
		}
	}
	t.Fatalf("task %q has no artifact named %q (has %v)", taskRun.GetTaskId(), name, taskRun.GetArtifacts())
	return nil
}

func readArtifact(t *testing.T, ctx context.Context, store artifacts.Store, uri string) string {
	t.Helper()
	reader, err := store.Get(ctx, uri)
	if err != nil {
		t.Fatalf("read artifact %s: %v", uri, err)
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read artifact %s: %v", uri, err)
	}
	return string(raw)
}

// writeHandoffProof records the artifact hand-off transcript when
// ORK_PROOF_M3_DIR is set, producing the committed proof artifacts.
func writeHandoffProof(t *testing.T, runID, originURL string, taskRuns map[string]*orkstackv1.TaskRun) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_M3_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}

	var transcript strings.Builder
	var uris strings.Builder
	fmt.Fprintf(&transcript, "run_id:   %s\n", runID)
	fmt.Fprintf(&transcript, "captured: %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "origin:   %s (hermetic httptest server)\n", originURL)
	fmt.Fprintf(&transcript, "served:   %q\n\n", servedBody)

	for _, id := range []string{"fetch", "transform", "save"} {
		taskRun := taskRuns[id]
		fmt.Fprintf(&transcript, "task %s (%s)\n", id, taskRun.GetStatus())
		fmt.Fprintf(&transcript, "  output: %s\n", taskRun.GetOutput().GetData())
		for _, ref := range taskRun.GetArtifacts() {
			fmt.Fprintf(&transcript, "  artifact %q -> %s (%d bytes, sha256 %s, %s)\n",
				ref.GetName(), ref.GetUri(), ref.GetSizeBytes(), ref.GetSha256(), ref.GetContentType())
			fmt.Fprintf(&uris, "%s\t%s\t%s\n", id, ref.GetName(), ref.GetUri())
		}
		fmt.Fprintln(&transcript)
	}
	fmt.Fprintf(&transcript,
		"hand-off: fetch captured the response bytes as artifact %q; the orker materialized it\n"+
			"into the transform workdir, where the shell command read it (stdout above); the file\n"+
			"plugin then wrote that stdout to result.txt and uploaded it as artifact %q.\n",
		"body", "result.txt")

	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("artifact-handoff.txt", transcript.String())
	write("artifact-uris.txt", uris.String())
	t.Logf("wrote proof transcripts to %s", dir)
}

// writeArtifactURIProof checks the M7.4 acceptance the CLI is responsible for —
// `orkctl run get --json` shows clean URIs — and records the evidence.
//
// The rendering is the CLI's own: cmd/orkctl/output.go prints GetRunResponse
// through api.MarshalJSON plus json.Indent, which is exactly what happens here.
func writeArtifactURIProof(
	t *testing.T,
	runID string,
	msg *orkstackv1.GetRunResponse,
	taskRuns map[string]*orkstackv1.TaskRun,
) {
	t.Helper()
	compact, err := api.MarshalJSON(msg)
	if err != nil {
		t.Fatalf("marshal run as orkctl does: %v", err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, compact, "", "  "); err != nil {
		t.Fatalf("indent run json: %v", err)
	}
	rendered := indented.String()

	var uris strings.Builder
	fmt.Fprintf(&uris, "run_id:   %s\n", runID)
	fmt.Fprintf(&uris, "captured: %s\n\n", time.Now().UTC().Format(time.RFC3339))
	for _, id := range []string{"fetch", "transform", "save"} {
		for _, ref := range taskRuns[id].GetArtifacts() {
			if !strings.Contains(rendered, fmt.Sprintf("%q", ref.GetUri())) {
				t.Errorf("run get --json does not show artifact uri %q", ref.GetUri())
			}
			fmt.Fprintf(&uris, "%-10s %-12s %s\n", id, ref.GetName(), ref.GetUri())
		}
	}
	if strings.Contains(rendered, "/objects/") {
		t.Errorf("run get --json still shows a pre-M7.4 content-hash URI:\n%s", rendered)
	}

	dir := os.Getenv("ORK_PROOF_M74_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	for name, content := range map[string]string{
		"run-artifact-uris.txt": uris.String(),
		"run-get.json":          rendered + "\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	t.Logf("wrote M7.4 artifact URI proof to %s", dir)
}
