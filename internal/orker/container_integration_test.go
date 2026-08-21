//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ContainerExampleRunsOnTheLiveEngine is M7.7's end-to-end
// acceptance: examples/container-example.ork.yaml submitted to a real engine on
// real Temporal, executed by a real Orker against a real Docker daemon, with
// artifacts landing in MinIO.
//
// It proves, in one run, that a containerized task receives an upstream
// artifact read-only, gets an ephemeral mount that is created and cleaned,
// expands its environment, and hands both output streams back as artifacts a
// downstream task can read.
func TestIntegration_ContainerExampleRunsOnTheLiveEngine(t *testing.T) {
	requireDockerDaemon(t)

	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	storeConfig := artifacts.ConfigFromEnv()
	storeConfig.Backend = artifacts.BackendS3
	requireMinIO(t, storeConfig.S3Endpoint)
	store, err := artifacts.New(storeConfig)
	if err != nil {
		infraAbsent(t, "cannot open the s3 artifact store at %s: %v", storeConfig.S3Endpoint, err)
	}

	// The Orker's workdir holds both the materialized artifact and the
	// ephemeral mount, so the daemon has to be able to bind-mount it.
	workRoot := bindableWorkRoot(t)
	t.Logf("orker work root: %s", workRoot)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
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

	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)

	orkerWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	orkerWorker.RegisterActivityWithOptions(
		orker.NewActivitiesWithOptions(orker.BuiltinRegistry(), store, orker.Options{
			WorkRoot: workRoot,
		}).ExecuteTask,
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
		Orkflows: engine.NewMemoryOrkflowStore(),
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

	source, err := os.ReadFile("../../examples/container-example.ork.yaml")
	if err != nil {
		t.Fatalf("read example orkflow: %v", err)
	}
	if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: string(source),
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "container-example",
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
	var eventLog []string
	for stream.Receive() {
		event := stream.Msg()
		line := fmt.Sprintf("event %d %s %s %s",
			event.GetSequence(), event.GetType(), event.GetTaskId(), event.GetMessage())
		eventLog = append(eventLog, line)
		t.Log(line)
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

	taskRuns := map[string]*orkstackv1.TaskRun{}
	for _, taskRun := range run.GetTaskRuns() {
		taskRuns[taskRun.GetTaskId()] = taskRun
		if taskRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Errorf("task %q status = %v (error: %v), want SUCCEEDED",
				taskRun.GetTaskId(), taskRun.GetStatus(), taskRun.GetError())
		}
	}
	for _, id := range []string{"seed", "build", "record"} {
		if taskRuns[id] == nil {
			t.Fatalf("run has no task %q (has %v)", id, run.GetTaskRuns())
		}
	}

	// 1. The container ran to a clean exit, on the declared image.
	build := taskRuns["build"].GetOutput().GetData().GetFields()
	if code := build["exit_code"].GetNumberValue(); code != 0 {
		t.Errorf("build exit_code = %v, want 0", code)
	}
	if image := build["image"].GetStringValue(); image != "alpine:3.20" {
		t.Errorf("build image = %q, want alpine:3.20", image)
	}
	if build["timed_out"].GetBoolValue() {
		t.Errorf("build reported a timeout it did not have")
	}

	// The container's own output is read from its CAPTURE ARTIFACT, because
	// that is where it is: the payload above holds what only the plugin knows
	// and none of what the container wrote (AGENTS.md §1.2). The artifact is
	// also the stronger observation — it is the whole stream rather than a
	// truncated head.
	stdout := readArtifact(t, ctx, store,
		artifactNamed(t, taskRuns["build"], "build-log").GetUri())
	// 2. `env` was templated by the engine and $-expanded by the plugin.
	if !strings.Contains(stdout, "release=v1.4.2 archive=orkstack-v1.4.2.tar.gz") {
		t.Errorf("container stdout %q does not show the expanded environment", stdout)
	}
	// 3. The upstream artifact was mounted and readable inside the container.
	if !strings.Contains(stdout, "built-by=orkstack") {
		t.Errorf("container stdout %q does not show the mounted upstream artifact", stdout)
	}
	// 4. The ephemeral mount was writable: the container copied into it and
	//    read the copy back.
	if !strings.Contains(stdout, "lines=2") {
		t.Errorf("container stdout %q does not show the file written to the ephemeral mount", stdout)
	}

	// 5. And the payload carries NO part of it, however short.
	for _, field := range []string{"stdout", "stderr"} {
		if _, ok := build[field]; ok {
			t.Errorf("the build task's stored output carries %q; a task's output is "+
				"bytes and travels as artifacts (AGENTS.md §1.2)", field)
		}
	}
	diagnostics := readArtifact(t, ctx, store,
		artifactNamed(t, taskRuns["build"], "build-diagnostics").GetUri())
	if !strings.Contains(diagnostics, "scratch contains: manifest.copy.txt") {
		t.Errorf("build-diagnostics artifact = %q, want the container's stderr", diagnostics)
	}

	// 6. The container's output crossed the task boundary like any other:
	//    the payload fields by template, the bytes by artifact reference.
	report := readArtifact(t, ctx, store,
		artifactNamed(t, taskRuns["record"], "container-report.txt").GetUri())
	for _, want := range []string{"image: alpine:3.20", "exit_code: 0", "build log: "} {
		if !strings.Contains(report, want) {
			t.Errorf("container-report.txt = %q, want it to contain %q", report, want)
		}
	}

	// 7. Nothing the run created is still on the host.
	assertNoOrkContainers(t, "build")

	writeContainerProof(t, runID, eventLog, taskRuns, stdout, diagnostics, report)
}

// requireDockerDaemon skips unless a Docker daemon answers (fails instead
// under ORK_REQUIRE_INFRA; see infraAbsent).
func requireDockerDaemon(t *testing.T) {
	t.Helper()
	code, out := dockerCLI(t, 15*time.Second, "version", "--format", "{{.Server.Version}}")
	if code != 0 {
		infraAbsent(t, "docker daemon is not reachable: %s", strings.TrimSpace(out))
	}
	t.Logf("docker server %s", strings.TrimSpace(out))
}

func dockerCLI(t *testing.T, timeout time.Duration, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var combined bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, combined.String()
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), combined.String()
	default:
		infraAbsent(t, "cannot run docker %s: %v", strings.Join(args, " "), err)
		return -1, ""
	}
}

// bindableWorkRoot returns a work root the Docker daemon can bind-mount, which
// is what an operator configures through ORK_ORKER_WORKDIR. A daemon inside a
// VM sees only the paths that VM shares — on macOS the default share is the
// user's home, not the OS temp directory.
func bindableWorkRoot(t *testing.T) string {
	t.Helper()
	if explicit := strings.TrimSpace(os.Getenv(orker.WorkRootVar)); explicit != "" {
		return explicit
	}
	home, _ := os.UserHomeDir()
	candidates := []string{os.TempDir()}
	if home != "" {
		candidates = append(candidates, filepath.Join(home, ".ork-test-workdirs"))
	}
	for _, candidate := range candidates {
		if err := os.MkdirAll(candidate, 0o755); err != nil {
			continue
		}
		probe, err := os.MkdirTemp(candidate, "ork-bind-probe-*")
		if err != nil {
			continue
		}
		code, out := dockerCLI(t, 2*time.Minute, "run", "--rm",
			"--mount", "type=bind,source="+probe+",target=/ork-probe,readonly",
			"alpine:3.20", "true")
		_ = os.RemoveAll(probe)
		if code == 0 {
			return candidate
		}
		t.Logf("the docker daemon cannot bind-mount %s: %s", candidate, strings.TrimSpace(out))
	}
	// infraAbsent, not t.Skip: under ORK_REQUIRE_INFRA a proof has already
	// probed this host and found a bind-mountable directory, so reaching here
	// is the test opting out of a measurement the proof was told it could
	// make (AGENTS.md §6).
	infraAbsent(t, "no work root on this host can be bind-mounted by the docker daemon; set %s",
		orker.WorkRootVar)
	return ""
}

// assertNoOrkContainers proves the plugin's cleanup on a real host: the label
// every container carries must match nothing once the run is over.
func assertNoOrkContainers(t *testing.T, taskID string) {
	t.Helper()
	code, out := dockerCLI(t, 30*time.Second,
		"ps", "--all", "--quiet", "--filter", "label=ork.task="+taskID)
	if code != 0 {
		t.Fatalf("docker ps: %s", out)
	}
	if ids := strings.Fields(out); len(ids) != 0 {
		t.Errorf("the run left %d container(s) behind: %v", len(ids), ids)
	}
}

// writeContainerProof records the M7.7 evidence when ORK_PROOF_M77_DIR is set.
func writeContainerProof(
	t *testing.T,
	runID string,
	eventLog []string,
	taskRuns map[string]*orkstackv1.TaskRun,
	stdout, diagnostics, report string,
) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_M77_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "run_id:   %s\n", runID)
	fmt.Fprintf(&transcript, "captured: %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "orkflow:  examples/container-example.ork.yaml\n\n")
	fmt.Fprintf(&transcript, "events\n")
	for _, line := range eventLog {
		fmt.Fprintf(&transcript, "  %s\n", line)
	}
	fmt.Fprintf(&transcript, "\ntasks\n")
	for _, id := range []string{"seed", "build", "record"} {
		taskRun := taskRuns[id]
		fmt.Fprintf(&transcript, "  %s (%s) plugin=%s\n", id, taskRun.GetStatus(), taskRun.GetPlugin())
		fmt.Fprintf(&transcript, "    output: %s\n", taskRun.GetOutput().GetData())
		for _, ref := range taskRun.GetArtifacts() {
			fmt.Fprintf(&transcript, "    artifact %q -> %s (%d bytes, sha256 %s)\n",
				ref.GetName(), ref.GetUri(), ref.GetSizeBytes(), ref.GetSha256())
		}
	}
	fmt.Fprintf(&transcript, "\ncontainer stdout (artifact build-log)\n%s\n", indent(stdout))
	fmt.Fprintf(&transcript, "container stderr (artifact build-diagnostics)\n%s\n", indent(diagnostics))
	fmt.Fprintf(&transcript, "downstream file task (artifact container-report.txt)\n%s\n", indent(report))

	path := filepath.Join(dir, "container-example-run.txt")
	if err := os.WriteFile(path, []byte(transcript.String()), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote proof transcript to %s", path)
}

func indent(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		lines[i] = "    " + line
	}
	return strings.Join(lines, "\n")
}
