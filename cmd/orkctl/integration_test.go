//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"

	"github.com/orkstack/orkstack/internal/task"
)

const (
	// successOrkflowName is the example orkflow, pinned to this test's
	// private task queue so only this test's Orker can execute it.
	successOrkflowName = "hello-ork-m4"

	// failingOrkflowName is the same orkflow with its tasks replaced by one
	// shell command that exits non-zero, proving the failure exit code.
	failingOrkflowName = "hello-ork-m4-failing"
)

// servedBody is what the hermetic stand-in for the example's public fetch URL
// returns. The trailing newline makes the shell task's `tr -d` step observable.
const servedBody = "{\"slideshow\": {\"title\": \"Sample Slide Show\", \"slides\": 2}}\n"

// TestIntegration_CLIDrivesARealEngineToTerminalRuns builds the real orkctl,
// ork and orker binaries, runs them against a real Temporal cluster, and
// drives apply -> submit --watch -> run get through the CLI process, asserting
// both the terminal run status and the matching CLI exit code.
//
// Isolation: the engine's workflow worker must poll the shared queue, but every
// task the runs schedule is pinned to a private task queue that only this
// test's Orker polls, so a stray Orker elsewhere can never steal a task.
func TestIntegration_CLIDrivesARealEngineToTerminalRuns(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	temporalClient.Close()

	// The example's fetch step targets a local server so the run is hermetic.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, servedBody)
	}))
	defer origin.Close()

	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}
	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	orkctlPath := buildBinary(t, repoRoot, binDir, "orkctl")
	enginePath := buildBinary(t, repoRoot, binDir, "ork")
	orkerPath := buildBinary(t, repoRoot, binDir, "orker")

	startOrker(ctx, t, orkerPath, address, taskQueue)
	engineAddr := startEngine(ctx, t, enginePath, address, taskQueue)

	session := &sessionRecorder{}
	cli := func(args ...string) (string, string, int) {
		full := append(args, "--server", "http://"+engineAddr)
		stdout, stderr, code := runBinary(ctx, t, orkctlPath, repoRoot, full...)
		session.record(full, stdout, stderr, code)
		return stdout, stderr, code
	}
	waitForEngine(t, func(args ...string) (string, string, int) {
		return runBinary(ctx, t, orkctlPath, repoRoot,
			append(args, "--server", "http://"+engineAddr)...)
	})
	t.Cleanup(func() { session.writeProof(t) })

	if _, stderr, code := cli("version"); code != 0 {
		t.Fatalf("version exit = %d, want 0\nstderr:\n%s", code, stderr)
	}

	// apply the repository's example orkflow exactly as a user would
	stdout, stderr, code := cli("apply", filepath.Join(repoRoot, "examples", "hello.ork.yaml"))
	if code != 0 {
		t.Fatalf("apply exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "Applied orkflow hello-ork") {
		t.Fatalf("apply stdout:\n%s", stdout)
	}

	// Runs use a copy of that example pinned to this test's private task queue.
	successPath := writePoolPinnedOrkflow(t, repoRoot, successOrkflowName, pool)
	if _, stderr, code = cli("apply", successPath); code != 0 {
		t.Fatalf("apply of the pinned orkflow exit = %d, want 0\nstderr:\n%s", code, stderr)
	}

	// submit --watch: the real Orker runs http -> shell -> file for real.
	stdout, stderr, code = cli("run", "submit", successOrkflowName,
		"--input", "url="+origin.URL+"/json", "--watch")
	if code != 0 {
		t.Fatalf("submit --watch exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"RUN_CREATED", "RUN_STARTED", "TASK_SUCCEEDED", "RUN_SUCCEEDED",
		"finished with status SUCCEEDED"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("submit --watch stdout is missing %q:\n%s", want, stdout)
		}
	}
	runID := parseRunID(t, stdout)

	// run get agrees with the watched outcome
	stdout, stderr, code = cli("run", "get", runID)
	if code != 0 {
		t.Fatalf("run get exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "Status:    SUCCEEDED") {
		t.Fatalf("run get stdout does not report SUCCEEDED:\n%s", stdout)
	}

	// The same path for a run whose task exits non-zero: the CLI must too.
	failingPath := writePoolPinnedOrkflow(t, repoRoot, failingOrkflowName, pool)
	if _, stderr, code = cli("apply", failingPath); code != 0 {
		t.Fatalf("apply of the failing orkflow exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	stdout, stderr, code = cli("run", "submit", failingOrkflowName, "--watch")
	if code != exitRunFailed {
		t.Fatalf("submit --watch of a failing run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitRunFailed, stdout, stderr)
	}
	if !strings.Contains(stdout, "RUN_FAILED") || !strings.Contains(stdout, "finished with status FAILED") {
		t.Fatalf("failing submit --watch stdout:\n%s", stdout)
	}
	failedRunID := parseRunID(t, stdout)

	stdout, stderr, code = cli("run", "get", failedRunID)
	if code != 0 {
		t.Fatalf("run get exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "Status:    FAILED") {
		t.Fatalf("run get stdout does not report FAILED:\n%s", stdout)
	}

	// run list and plugin list work against the same live engine.
	stdout, stderr, code = cli("run", "list")
	if code != 0 {
		t.Fatalf("run list exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, runID) || !strings.Contains(stdout, failedRunID) {
		t.Fatalf("run list is missing a run:\n%s", stdout)
	}
	if _, stderr, code = cli("plugin", "list"); code != 0 {
		t.Fatalf("plugin list exit = %d, want 0\nstderr:\n%s", code, stderr)
	}

	// A machine-readable sample of the same successful run: statuses are named
	// without their enum prefix, against the real engine.
	stdout, stderr, code = cli("run", "get", runID, "--json")
	if code != 0 {
		t.Fatalf("run get --json exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	var decoded struct {
		Run struct {
			Status   string `json:"status"`
			TaskRuns []struct {
				TaskID string `json:"taskId"`
				Status string `json:"status"`
			} `json:"taskRuns"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("run get --json is not valid JSON: %v\nstdout:\n%s", err, stdout)
	}
	if decoded.Run.Status != "SUCCEEDED" {
		t.Fatalf("run get --json status = %q, want SUCCEEDED\nstdout:\n%s", decoded.Run.Status, stdout)
	}
	for _, taskRun := range decoded.Run.TaskRuns {
		if taskRun.Status != "SUCCEEDED" {
			t.Fatalf("task %s status = %q, want SUCCEEDED", taskRun.TaskID, taskRun.Status)
		}
	}
	if strings.Contains(stdout, "STATUS_") || strings.Contains(stdout, "EVENT_TYPE_") {
		t.Fatalf("run get --json still carries an enum prefix:\n%s", stdout)
	}
	session.jsonSample = stdout
}

// sessionRecorder captures the real CLI session for proof/M4. Set
// ORK_PROOF_SESSION_FILE and ORK_PROOF_JSON_FILE to write the artifacts.
type sessionRecorder struct {
	transcript strings.Builder
	jsonSample string
}

func (s *sessionRecorder) record(args []string, stdout, stderr string, code int) {
	fmt.Fprintf(&s.transcript, "$ orkctl %s\n", strings.Join(args, " "))
	if stdout != "" {
		s.transcript.WriteString(stdout)
	}
	if stderr != "" {
		for _, line := range strings.Split(strings.TrimRight(stderr, "\n"), "\n") {
			fmt.Fprintf(&s.transcript, "(stderr) %s\n", line)
		}
	}
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
}

func (s *sessionRecorder) writeProof(t *testing.T) {
	t.Helper()
	if path := os.Getenv("ORK_PROOF_SESSION_FILE"); path != "" {
		header := fmt.Sprintf(
			"# orkctl against a real ork + real orker + real Temporal (%s)\n"+
				"# The runs execute the example orkflow's http -> shell -> file tasks\n"+
				"# for real; tasks are pinned to a private task queue this test's Orker\n"+
				"# alone polls, and the fetch URL is a local hermetic server.\n"+
				"# generated by: go test -tags=integration -p 1 ./cmd/orkctl/ -run Integration\n\n",
			time.Now().UTC().Format(time.RFC3339))
		if err := os.WriteFile(path, []byte(header+s.transcript.String()), 0o644); err != nil {
			t.Errorf("write session proof: %v", err)
		}
	}
	if path := os.Getenv("ORK_PROOF_JSON_FILE"); path != "" && s.jsonSample != "" {
		if err := os.WriteFile(path, []byte(s.jsonSample), 0o644); err != nil {
			t.Errorf("write json proof: %v", err)
		}
	}
}

// temporalAddress is where the integration test expects a real Temporal
// frontend. Override with ORK_TEMPORAL_ADDRESS.
func temporalAddress() string {
	if addr := os.Getenv("ORK_TEMPORAL_ADDRESS"); addr != "" {
		return addr
	}
	return client.DefaultHostPort
}

// privateTaskPool names a pool unique to this test process. Its queue is
// derived by internal/task.QueueForPool; the test never writes an engine term
// into the DSL.
func privateTaskPool() string {
	return fmt.Sprintf("orkctl-m4-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// startOrker runs the real orker binary against the private task queue, with a
// local-filesystem artifact store so the test needs no object storage.
func startOrker(ctx context.Context, t *testing.T, orkerPath, temporalAddr, queue string) {
	t.Helper()
	root := t.TempDir()
	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ORKER_WORKDIR="+filepath.Join(root, "work"),
		"ORK_ARTIFACT_BACKEND=local",
		"ORK_ARTIFACT_DIR="+filepath.Join(root, "artifacts"),
	)
}

// startEngine runs the real ork binary on a free loopback port. The
// orkerQueue is passed through so plugin discovery targets the same private
// queue the test's Orker polls.
func startEngine(ctx context.Context, t *testing.T, enginePath, temporalAddr, orkerQueue string) string {
	t.Helper()
	addr := freeAddress(t)
	startProcess(ctx, t, "ork", enginePath,
		"ORK_HTTP_ADDR="+addr,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_ORKER_QUEUE="+orkerQueue,
	)
	return addr
}

// startProcess starts a built binary with extra environment and guarantees it
// is stopped when the test ends, so no worker outlives the test.
func startProcess(ctx context.Context, t *testing.T, name, path string, env ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, path)
	cmd.Env = append(os.Environ(), env...)
	var log strings.Builder
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		t.Logf("%s log:\n%s", name, log.String())
	})
}

// waitForEngine polls the engine through the CLI until it answers.
func waitForEngine(t *testing.T, cli func(args ...string) (string, string, int)) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, code := cli("orkflow", "list"); code == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("ork did not become ready within 60s")
}

// buildBinary compiles one command into dir and returns its path.
func buildBinary(t *testing.T, repoRoot, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", path, "./cmd/"+name)
	cmd.Dir = repoRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/%s: %v\n%s", name, err, output)
	}
	return path
}

// runBinary executes a built binary and returns stdout, stderr, and its exit
// code.
func runBinary(ctx context.Context, t *testing.T, path, dir string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = dir
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return stdout.String(), stderr.String(), exitErr.ExitCode()
	}
	t.Fatalf("run %s %v: %v", path, args, err)
	return "", "", -1
}

// writePoolPinnedOrkflow rewrites examples/hello.ork.yaml under a new name
// with every task assigned to pool, and returns the file's path. The failing
// variant keeps the same shape but runs one command that exits non-zero.
func writePoolPinnedOrkflow(t *testing.T, repoRoot, name, pool string) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(repoRoot, "examples", "hello.ork.yaml"))
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(source, &doc); err != nil {
		t.Fatalf("parse example: %v", err)
	}
	doc["name"] = name

	if name == failingOrkflowName {
		doc["tasks"] = []any{map[string]any{
			"id":     "fail",
			"plugin": "shell",
			"with":   map[string]any{"command": "echo boom >&2; exit 7"},
		}}
	}
	tasks, ok := doc["tasks"].([]any)
	if !ok || len(tasks) == 0 {
		t.Fatalf("example has no tasks: %#v", doc["tasks"])
	}
	for _, raw := range tasks {
		task, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("unexpected task shape: %#v", raw)
		}
		task["pool"] = pool
	}

	encoded, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatalf("encode orkflow: %v", err)
	}
	path := filepath.Join(t.TempDir(), name+".ork.yaml")
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatalf("write orkflow: %v", err)
	}
	return path
}

var runIDPattern = regexp.MustCompile(`Submitted run (\S+)`)

func parseRunID(t *testing.T, stdout string) string {
	t.Helper()
	match := runIDPattern.FindStringSubmatch(stdout)
	if match == nil {
		t.Fatalf("no run id in CLI output:\n%s", stdout)
	}
	return match[1]
}

// repoRoot resolves the module root from cmd/orkctl.
func repoRoot(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return path
}

// freeAddress returns a loopback address that is free right now.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return address
}
