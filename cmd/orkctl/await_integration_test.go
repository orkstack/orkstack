//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// awaitCLI is one test's real binaries: the CLI, one or two engines, and an
// Orker on a private pool, in a private Temporal namespace, sharing one local
// artifact directory.
type awaitCLI struct {
	t         *testing.T
	ctx       context.Context
	orkctl    string
	repoRoot  string
	engines   []string
	namespace string
	session   *sessionRecorder
}

func newAwaitCLI(t *testing.T, engines int, withOrker bool) *awaitCLI {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}
	root := repoRoot(t)
	c := &awaitCLI{
		t: t, ctx: ctx, repoRoot: root,
		orkctl:  testbin.Path(t, "./cmd/orkctl"),
		session: &sessionRecorder{},
	}
	enginePath := testbin.Path(t, "./cmd/ork")
	temporalNamespace := privateTemporalNamespace()
	if withOrker {
		startOrker(ctx, t, testbin.Path(t, "./cmd/orker"), address, temporalNamespace, taskQueue)
	}
	var exits []<-chan struct{}
	for i := 0; i < engines; i++ {
		addr, exited := startEngine(ctx, t, enginePath, address, temporalNamespace, taskQueue)
		c.engines = append(c.engines, "http://"+addr)
		exits = append(exits, exited)
	}
	for i := range c.engines {
		waitForEngine(t, exits[i], func(args ...string) (string, string, int) { return c.on(i, args...) })
	}
	t.Cleanup(func() { c.session.writeProof(t) })
	namespace := fmt.Sprintf("awaits-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, stderr, code := c.on(0, "namespace", "create", namespace); code != 0 {
		t.Fatalf("namespace create: %d %s", code, stderr)
	}
	// From here on every command acts in the test's own namespace.
	c.namespace = namespace
	if withOrker {
		waitForPoolLive(t, func(args ...string) (string, string, int) { return c.on(0, args...) }, c.namespace, pool)
	}
	return c
}

// on runs orkctl against engine i, in this test's namespace.
func (c *awaitCLI) on(i int, args ...string) (string, string, int) {
	full := append(append([]string(nil), args...), "--server", c.engines[i])
	if c.namespace != "" && !containsArg(args, "-n") {
		full = append(full, "-n", c.namespace)
	}
	stdout, stderr, code := runBinary(c.ctx, c.t, c.orkctl, c.repoRoot, full...)
	c.session.record(full, stdout, stderr, code)
	return stdout, stderr, code
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func (c *awaitCLI) must(i int, args ...string) string {
	c.t.Helper()
	stdout, stderr, code := c.on(i, args...)
	if code != 0 {
		c.t.Fatalf("orkctl %v exit %d\nstdout:\n%s\nstderr:\n%s", args, code, stdout, stderr)
	}
	return stdout
}

type submitJSON struct {
	Run struct {
		ID string `json:"id"`
	} `json:"run"`
	AwaitCallbacks []struct {
		TaskID string `json:"taskId"`
		Path   string `json:"path"`
		Token  string `json:"token"`
	} `json:"awaitCallbacks"`
}

type runJSON struct {
	Run struct {
		Status   string `json:"status"`
		TaskRuns []struct {
			TaskID string `json:"taskId"`
			Status string `json:"status"`
			Await  *struct {
				ResolvedVia string `json:"resolvedVia"`
				ResolvedBy  string `json:"resolvedBy"`
			} `json:"await"`
		} `json:"taskRuns"`
	} `json:"run"`
}

func (c *awaitCLI) get(i int, runID string) runJSON {
	c.t.Helper()
	var doc runJSON
	if err := json.Unmarshal([]byte(c.must(i, "run", "get", runID, "--json")), &doc); err != nil {
		c.t.Fatalf("decode run get: %v", err)
	}
	return doc
}

func (doc runJSON) task(id string) (string, string) {
	for _, tr := range doc.Run.TaskRuns {
		if tr.TaskID == id {
			via := ""
			if tr.Await != nil {
				via = tr.Await.ResolvedVia
			}
			return tr.Status, via
		}
	}
	return "", ""
}

// waitAwaiting polls `run get --json` until taskID is AWAITING.
func (c *awaitCLI) waitAwaiting(i int, runID, taskID string) {
	c.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		if status, _ := c.get(i, runID).task(taskID); status == "AWAITING" {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("task %s of run %s never reached AWAITING", taskID, runID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *awaitCLI) post(i int, path, token, body string) (int, string) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.engines[i]+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

// Apply the example, submit, POST the callback over real HTTP, watch the
// run finish, read the values back byte for byte, and repeat.
func TestIntegration_CLISubmitCallbackAndWatchAnAwaitedRun(t *testing.T) {
	c := newAwaitCLI(t, 1, true)
	example := filepath.Join(c.repoRoot, "examples", "awaited-approval.ork.yaml")
	c.must(0, "apply", example)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "awaited-approval", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	runID := submitted.Run.ID
	if len(submitted.AwaitCallbacks) != 1 || submitted.AwaitCallbacks[0].TaskID != "approval" {
		t.Fatalf("callbacks = %+v", submitted.AwaitCallbacks)
	}
	callback := submitted.AwaitCallbacks[0]
	c.waitAwaiting(0, runID, "approval")

	body := "{\"approved\": true, \"approver\": \"ci-bot\"}\n"
	if status, out := c.post(0, callback.Path, callback.Token, body); status != http.StatusAccepted {
		t.Fatalf("callback answered %d: %s", status, out)
	}
	if _, stderr, code := c.on(0, "run", "watch", runID); code != 0 {
		t.Fatalf("run watch exit %d: %s", code, stderr)
	}
	doc := c.get(0, runID)
	if status, via := doc.task("approval"); status != "SUCCEEDED" || via != "CALLBACK" {
		t.Fatalf("approval = %s via %s", status, via)
	}
	if status, _ := doc.task("apply"); status != "SUCCEEDED" {
		t.Fatalf("apply = %s, want the approved branch to run", status)
	}
	out := filepath.Join(t.TempDir(), "values.json")
	c.must(0, "run", "artifact", "download", runID, "--task", "approval", "--kind", "values",
		"--name", "values.json", "--output", out)
	downloaded, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(downloaded, []byte(body)) {
		t.Fatalf("downloaded values %q, want the posted bytes %q", downloaded, body)
	}
	repeat := c.must(0, "run", "resolve", runID, "--task", "approval", "--values", body)
	if !strings.Contains(repeat, "was already resolved with these values; nothing was sent.") {
		t.Fatalf("repeat printed %q", repeat)
	}
}

const twoAwaitsYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: two-awaits
version: "1.0.0"
tasks:
  - id: callback-gate
    await: {timeout_seconds: 600, callback: true}
  - id: operator-gate
    await: {timeout_seconds: 600}
`

// Submit on engine A; the callback and `run resolve` both go to engine B,
// which started nothing.
func TestIntegration_AwaitResolvedOnASecondEngineReplica(t *testing.T) {
	c := newAwaitCLI(t, 2, false)
	path := filepath.Join(t.TempDir(), "two-awaits.ork.yaml")
	if err := os.WriteFile(path, []byte(twoAwaitsYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "apply", path)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "two-awaits", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	runID := submitted.Run.ID
	c.waitAwaiting(1, runID, "callback-gate")
	c.waitAwaiting(1, runID, "operator-gate")
	callback := submitted.AwaitCallbacks[0]
	if status, out := c.post(1, callback.Path, callback.Token, `{"from":"replica-b"}`); status != http.StatusAccepted {
		t.Fatalf("callback on B answered %d: %s", status, out)
	}
	c.must(1, "run", "resolve", runID, "--task", "operator-gate", "--values", `{"ok":true}`)
	if _, stderr, code := c.on(1, "run", "watch", runID); code != 0 {
		t.Fatalf("run watch exit %d: %s", code, stderr)
	}
	doc := c.get(1, runID)
	if doc.Run.Status != "SUCCEEDED" {
		t.Fatalf("run = %s", doc.Run.Status)
	}
	if _, via := doc.task("callback-gate"); via != "CALLBACK" {
		t.Fatalf("callback-gate via %s", via)
	}
	if _, via := doc.task("operator-gate"); via != "API" {
		t.Fatalf("operator-gate via %s", via)
	}
}

// Every refusal leaves the run AWAITING, and its event stream — read to
// the end once the run is cancelled — holds no TASK_RESOLVED.
func TestIntegration_CallbackRefusalsLeaveTheRunAwaiting(t *testing.T) {
	c := newAwaitCLI(t, 1, false)
	path := filepath.Join(t.TempDir(), "refusals.ork.yaml")
	if err := os.WriteFile(path, []byte(`apiVersion: orkstack/v1
kind: Orkflow
name: refusals
version: "1.0.0"
tasks:
  - id: gate
    await:
      timeout_seconds: 600
      callback: true
      schema:
        approved: {type: boolean, required: true}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "apply", path)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "refusals", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	runID := submitted.Run.ID
	callback := submitted.AwaitCallbacks[0]
	c.waitAwaiting(0, runID, "gate")

	for _, refusal := range []struct {
		name, path, token, body string
		want                    int
	}{
		{"wrong token", callback.Path, strings.Repeat("0", 64), `{"approved":true}`, 401},
		{"unknown task", strings.TrimSuffix(callback.Path, "gate") + "nobody", callback.Token, `{"approved":true}`, 404},
		{"schema violation", callback.Path, callback.Token, `{"approved":"yes"}`, 400},
		{"oversize", callback.Path, callback.Token, `{"approved":true,"pad":"` + strings.Repeat("x", 300*1024) + `"}`, 413},
	} {
		if status, out := c.post(0, refusal.path, refusal.token, refusal.body); status != refusal.want {
			t.Fatalf("%s: %d, want %d: %s", refusal.name, status, refusal.want, out)
		}
	}
	if status, _ := c.get(0, runID).task("gate"); status != "AWAITING" {
		t.Fatalf("after the refusals the await is %s, want AWAITING", status)
	}
	c.must(0, "run", "cancel", runID)
	events, _, _ := c.on(0, "run", "watch", runID, "--json")
	if strings.Contains(events, "TASK_RESOLVED") {
		t.Fatalf("a refused callback reached the run's events:\n%s", events)
	}
	if !strings.Contains(events, "TASK_AWAITING") || !strings.Contains(events, "RUN_CANCELED") {
		t.Fatalf("the event stream was not read to its end:\n%s", events)
	}
}
