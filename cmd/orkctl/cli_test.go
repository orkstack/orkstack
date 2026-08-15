package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ---------------------------------------------------------------------------
// apply
// ---------------------------------------------------------------------------

func TestApply_RegistersExampleOrkflowAgainstTheRealServer(t *testing.T) {
	f := newFixture(t)

	applied := f.applyExample().requireExit(t, exitOK)
	requireGolden(t, "apply.golden", applied.stdout)

	listed := f.run("orkflow", "list").requireExit(t, exitOK)
	listed.requireContains(t, "hello-ork", "0.1.0")
}

func TestApply_MissingFileExitsNonZero(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("apply", filepath.Join(t.TempDir(), "absent.ork.yaml"))
	if outcome.code == exitOK {
		t.Fatalf("expected a non-zero exit for a missing file, stdout:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "read orkflow file") {
		t.Fatalf("stderr does not explain the failure:\n%s", outcome.stderr)
	}
}

func TestApply_InvalidOrkflowExitsNonZero(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(t.TempDir(), "broken.ork.yaml")
	if err := os.WriteFile(path, []byte("name: \"\"\ntasks: []\n"), 0o644); err != nil {
		t.Fatalf("write broken orkflow: %v", err)
	}

	outcome := f.run("apply", path)
	if outcome.code == exitOK {
		t.Fatalf("expected a non-zero exit for an invalid orkflow, stdout:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "apply:") {
		t.Fatalf("stderr does not name the failing operation:\n%s", outcome.stderr)
	}
}

// ---------------------------------------------------------------------------
// orkflow
// ---------------------------------------------------------------------------

func TestOrkflowGet_ReturnsTheAppliedOrkflow(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	f.run("orkflow", "get", "hello-ork").
		requireExit(t, exitOK).
		requireContains(t, "hello-ork", "0.1.0", "fetch", "transform", "save", "url")
}

func TestOrkflowGet_UnknownOrkflowExitsNonZero(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("orkflow", "get", "does-not-exist")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstderr:\n%s", outcome.code, exitError, outcome.stderr)
	}
	if !strings.Contains(outcome.stderr, "not_found") {
		t.Fatalf("stderr does not report not_found:\n%s", outcome.stderr)
	}
}

func TestOrkflowList_ReportsEmptyRegistry(t *testing.T) {
	f := newFixture(t)

	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "No orkflows.")
}

// ---------------------------------------------------------------------------
// run submit / get / list / cancel
// ---------------------------------------------------------------------------

func TestRunSubmit_CreatesRunAndPrintsItsID(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	f.run("run", "submit", "hello-ork").
		requireExit(t, exitOK).
		requireContains(t, "Submitted run run-001", "hello-ork", "PENDING")

	if started := f.starter.startedRuns(); len(started) != 1 || started[0] != "run-001" {
		t.Fatalf("starter observed %v, want [run-001]", started)
	}
}

func TestRunSubmit_TypesInputValuesAndKeepsPlainStringsVerbatim(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	observed := make(chan map[string]any, 1)
	f.starter.onStart = func(_ string, _ *orkstackv1.Orkflow, input *orkstackv1.Payload) {
		observed <- input.GetData().AsMap()
	}

	f.run("run", "submit", "hello-ork",
		"--input", "url=https://example.test/data",
		"--input", "retries=3",
		"--input", "verbose=true",
		"--input", "tags=[\"a\",\"b\"]",
	).requireExit(t, exitOK)

	got := <-observed
	if got["url"] != "https://example.test/data" {
		t.Fatalf("url = %#v, want the verbatim string", got["url"])
	}
	if got["retries"] != float64(3) {
		t.Fatalf("retries = %#v, want the number 3", got["retries"])
	}
	if got["verbose"] != true {
		t.Fatalf("verbose = %#v, want the boolean true", got["verbose"])
	}
	tags, ok := got["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Fatalf("tags = %#v, want the list [a b]", got["tags"])
	}
}

func TestRunSubmit_RejectsMalformedInputPair(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	outcome := f.run("run", "submit", "hello-ork", "--input", "novalue")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d", outcome.code, exitError)
	}
	if !strings.Contains(outcome.stderr, "expected key=value") {
		t.Fatalf("stderr does not explain the input syntax:\n%s", outcome.stderr)
	}
}

func TestRunSubmit_UnknownOrkflowExitsNonZero(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("run", "submit", "no-such-orkflow")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstderr:\n%s", outcome.code, exitError, outcome.stderr)
	}
}

func TestRunGet_ReturnsTheSubmittedRun(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)
	f.completeRunSnapshot("run-001")

	got := f.run("run", "get", "run-001").requireExit(t, exitOK)
	requireGolden(t, "run-get.golden", got.stdout)
}

func TestRunGet_UnknownRunExitsNonZero(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("run", "get", "run-missing")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstderr:\n%s", outcome.code, exitError, outcome.stderr)
	}
}

func TestRunList_ReturnsTheSubmittedRun(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)
	f.completeRunSnapshot("run-001")

	got := f.run("run", "list").requireExit(t, exitOK)
	requireGolden(t, "run-list.golden", got.stdout)
}

func TestRunList_FiltersByStatusAndRejectsUnknownStatus(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.run("run", "list", "--status", "succeeded").
		requireExit(t, exitOK).
		requireContains(t, "No runs.")
	f.run("run", "list", "--status", "pending").
		requireExit(t, exitOK).
		requireContains(t, "run-001")

	outcome := f.run("run", "list", "--status", "nonsense")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d", outcome.code, exitError)
	}
	if !strings.Contains(outcome.stderr, "invalid --status") {
		t.Fatalf("stderr does not reject the status:\n%s", outcome.stderr)
	}
}

func TestRunCancel_IsAcceptedByTheServer(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.run("run", "cancel", "run-001", "--reason", "operator stopped it").
		requireExit(t, exitOK).
		requireContains(t, "Cancellation requested for run run-001")

	if canceled := f.starter.canceledRuns(); len(canceled) != 1 || canceled[0] != "run-001" {
		t.Fatalf("starter observed cancellations %v, want [run-001]", canceled)
	}
}

// ---------------------------------------------------------------------------
// plugin + version
// ---------------------------------------------------------------------------

func TestPluginList_ReturnsTheEnginesPlugins(t *testing.T) {
	f := newFixture(t,
		&orkstackv1.PluginInfo{Name: "http", Version: "0.1.0", Description: "HTTP requests"},
		&orkstackv1.PluginInfo{Name: "shell", Version: "0.1.0", Description: "Shell commands"},
	)

	f.run("plugin", "list").
		requireExit(t, exitOK).
		requireContains(t, "NAME", "http", "shell", "HTTP requests")
}

func TestPluginList_ReportsAnEmptyRegistry(t *testing.T) {
	f := newFixture(t)

	f.run("plugin", "list").requireExit(t, exitOK).requireContains(t, "No plugins.")
}

func TestVersion_PrintsTheBuildVersion(t *testing.T) {
	f := newFixture(t)

	f.run("version").requireExit(t, exitOK).requireContains(t, "orkctl:", version)
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

func TestRunWatch_StreamsEventsUntilTerminalAndExitsZero(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		orkstackv1.Status_STATUS_RUNNING, "run started")
	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.Status_STATUS_SUCCEEDED, "task fetch succeeded")
	f.finishRun("run-001", orkstackv1.Status_STATUS_SUCCEEDED, nil)
	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")

	f.run("run", "watch", "run-001").
		requireExit(t, exitOK).
		requireContains(t, "RUN_CREATED", "RUN_STARTED", "TASK_SUCCEEDED", "RUN_SUCCEEDED",
			"Run run-001 finished with status SUCCEEDED")
}

func TestRunWatch_ExitsNonZeroWhenTheRunFails(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		orkstackv1.Status_STATUS_RUNNING, "run started")
	f.finishRun("run-001", orkstackv1.Status_STATUS_FAILED,
		&orkstackv1.Error{Code: "TASK_FAILED", Message: "fetch exploded"})
	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.Status_STATUS_FAILED, "fetch exploded")

	outcome := f.run("run", "watch", "run-001").requireExit(t, exitRunFailed)
	outcome.requireContains(t, "RUN_FAILED", "Run run-001 finished with status FAILED")
	if !strings.Contains(outcome.stderr, "TASK_FAILED") {
		t.Fatalf("stderr does not carry the run error:\n%s", outcome.stderr)
	}
}

func TestRunSubmitWatch_ExitsNonZeroWhenTheRunFails(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	// The stub Temporal starter drives the run to a terminal state exactly as
	// the engine's projection activity would.
	f.starter.onStart = func(runID string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload) {
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
			orkstackv1.Status_STATUS_RUNNING, "run started")
		f.finishRun(runID, orkstackv1.Status_STATUS_FAILED,
			&orkstackv1.Error{Code: "TASK_FAILED", Message: "fetch exploded"})
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
			orkstackv1.Status_STATUS_FAILED, "fetch exploded")
	}

	f.run("run", "submit", "hello-ork", "--watch").
		requireExit(t, exitRunFailed).
		requireContains(t, "Submitted run run-001", "RUN_FAILED",
			"Run run-001 finished with status FAILED")
}

func TestRunSubmitWatch_ExitsZeroWhenTheRunSucceeds(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)

	f.starter.onStart = func(runID string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload) {
		f.finishRun(runID, orkstackv1.Status_STATUS_SUCCEEDED, nil)
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	}

	f.run("run", "submit", "hello-ork", "--watch").
		requireExit(t, exitOK).
		requireContains(t, "Run run-001 finished with status SUCCEEDED")
}

func TestRunWatch_ExitsNonZeroWhenTheRunIsCanceled(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.finishRun("run-001", orkstackv1.Status_STATUS_CANCELED, nil)
	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED,
		orkstackv1.Status_STATUS_CANCELED, "canceled by operator")

	f.run("run", "watch", "run-001").
		requireExit(t, exitRunCanceled).
		requireContains(t, "RUN_CANCELED", "Run run-001 finished with status CANCELED")
}

// liveWriter publishes the next run event the moment the CLI writes the
// previous one, so the test deadlocks unless output really is streamed live.
type liveWriter struct {
	buf      bytes.Buffer
	triggers map[string]func()
}

func (w *liveWriter) Write(p []byte) (int, error) {
	n, err := w.buf.Write(p)
	for marker, fire := range w.triggers {
		if strings.Contains(string(p), marker) {
			delete(w.triggers, marker)
			fire()
		}
	}
	return n, err
}

func TestRunWatch_PrintsEachEventBeforeTheNextOneIsPublished(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	out := &liveWriter{triggers: map[string]func(){}}
	out.triggers["RUN_CREATED"] = func() {
		f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
			orkstackv1.Status_STATUS_RUNNING, "run started")
	}
	out.triggers["RUN_STARTED"] = func() {
		f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "task fetch succeeded")
	}
	out.triggers["TASK_SUCCEEDED"] = func() {
		f.finishRun("run-001", orkstackv1.Status_STATUS_SUCCEEDED, nil)
		f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	}

	done := make(chan result, 1)
	go func() { done <- f.runTo(out, "run", "watch", "run-001") }()

	select {
	case outcome := <-done:
		outcome.requireExit(t, exitOK)
	case <-time.After(15 * time.Second):
		t.Fatalf("run watch did not stream events live; captured output:\n%s", out.buf.String())
	}
	for _, marker := range []string{"RUN_CREATED", "RUN_STARTED", "TASK_SUCCEEDED", "RUN_SUCCEEDED"} {
		if !strings.Contains(out.buf.String(), marker) {
			t.Fatalf("streamed output is missing %s:\n%s", marker, out.buf.String())
		}
	}
}

// ---------------------------------------------------------------------------
// global flags and transport
// ---------------------------------------------------------------------------

func TestServerFlag_FallsBackToTheOrkServerEnvironmentVariable(t *testing.T) {
	f := newFixture(t)
	t.Setenv(serverEnvVar, f.baseURL)

	var stdout, stderr bytes.Buffer
	code := Execute([]string{"orkflow", "list"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("exit code = %d, want %d\nstderr:\n%s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "No orkflows.") {
		t.Fatalf("unexpected stdout:\n%s", stdout.String())
	}
}

func TestServerFlag_OverridesTheEnvironmentVariable(t *testing.T) {
	f := newFixture(t)
	t.Setenv(serverEnvVar, "http://127.0.0.1:1")

	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "No orkflows.")
}

func TestUnreachableServer_ExitsNonZero(t *testing.T) {
	address := closedAddress(t)

	var stdout, stderr bytes.Buffer
	code := Execute([]string{"orkflow", "list", "--server", "http://" + address}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("exit code = %d, want %d\nstderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "list orkflows") {
		t.Fatalf("stderr does not name the failing operation:\n%s", stderr.String())
	}
}

func TestServerFlag_RejectsAnUnusableURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"orkflow", "list", "--server", "ftp://example.test"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("exit code = %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr.String(), "scheme must be http or https") {
		t.Fatalf("stderr does not reject the scheme:\n%s", stderr.String())
	}
}

func TestTimeoutFlag_BoundsTheRequest(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("orkflow", "list", "--timeout", "1ns")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s", outcome.code, exitError, outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "list orkflows") {
		t.Fatalf("stderr does not name the failing operation:\n%s", outcome.stderr)
	}
}

// TestCLITransport_SpeaksUnencryptedHTTP2ToTheEngine proves the client the
// commands use negotiates h2c against the real server.
func TestCLITransport_SpeaksUnencryptedHTTP2ToTheEngine(t *testing.T) {
	f := newFixture(t)
	g := &globalFlags{server: f.baseURL}

	request, err := http.NewRequest(http.MethodPost,
		f.baseURL+"/orkstack.v1.OrkflowService/ListOrkflows", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := g.httpClient().Do(request)
	if err != nil {
		t.Fatalf("call engine: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if response.Proto != "HTTP/2.0" {
		t.Fatalf("protocol = %s, want HTTP/2.0", response.Proto)
	}
}

// ---------------------------------------------------------------------------
// --json
// ---------------------------------------------------------------------------

func TestJSONFlag_EmitsParseableJSONForEveryDataCommand(t *testing.T) {
	f := newFixture(t, &orkstackv1.PluginInfo{Name: "http", Version: "0.1.0"})
	f.applyExample().requireExit(t, exitOK)

	f.starter.onStart = func(runID string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload) {
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
			orkstackv1.Status_STATUS_RUNNING, "run started")
		f.finishRun(runID, orkstackv1.Status_STATUS_SUCCEEDED, nil)
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	}
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	commands := map[string][]string{
		"apply":        {"apply", examplePath(t)},
		"orkflow list": {"orkflow", "list"},
		"orkflow get":  {"orkflow", "get", "hello-ork"},
		"run submit":   {"run", "submit", "hello-ork"},
		"run get":      {"run", "get", "run-001"},
		"run list":     {"run", "list"},
		"run watch":    {"run", "watch", "run-001"},
		"run cancel":   {"run", "cancel", "run-001"},
		"plugin list":  {"plugin", "list"},
		"version":      {"version"},
	}

	for name, args := range commands {
		t.Run(name, func(t *testing.T) {
			outcome := f.run(append(args, "--json")...).requireExit(t, exitOK)
			var decoded any
			if err := json.Unmarshal([]byte(outcome.stdout), &decoded); err != nil {
				t.Fatalf("stdout is not valid JSON: %v\nstdout:\n%s", err, outcome.stdout)
			}
			// Machine output names enum values without their type prefix.
			for _, prefix := range []string{"STATUS_", "EVENT_TYPE_"} {
				if strings.Contains(outcome.stdout, prefix) {
					t.Errorf("stdout carries the %s enum prefix:\n%s", prefix, outcome.stdout)
				}
			}
		})
	}
}

func TestJSONFlag_WatchDocumentCarriesEventsAndFinalRun(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	f.finishRun("run-001", orkstackv1.Status_STATUS_FAILED,
		&orkstackv1.Error{Code: "TASK_FAILED", Message: "fetch exploded"})
	f.publish("run-001", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.Status_STATUS_FAILED, "fetch exploded")

	outcome := f.run("run", "watch", "run-001", "--json").requireExit(t, exitRunFailed)

	var document struct {
		Events []struct {
			RunID    string `json:"runId"`
			Sequence string `json:"sequence"`
			Type     string `json:"type"`
		} `json:"events"`
		Run struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(outcome.stdout), &document); err != nil {
		t.Fatalf("watch --json is not valid JSON: %v\nstdout:\n%s", err, outcome.stdout)
	}
	if len(document.Events) != 2 {
		t.Fatalf("events = %d, want 2\nstdout:\n%s", len(document.Events), outcome.stdout)
	}
	if document.Events[1].Type != "RUN_FAILED" {
		t.Fatalf("last event type = %s, want RUN_FAILED", document.Events[1].Type)
	}
	if document.Run.ID != "run-001" || document.Run.Status != "FAILED" {
		t.Fatalf("final run = %+v, want run-001 / FAILED", document.Run)
	}
	if strings.Contains(outcome.stdout, "STATUS_") || strings.Contains(outcome.stdout, "EVENT_TYPE_") {
		t.Fatalf("watch --json still carries an enum prefix:\n%s", outcome.stdout)
	}
}

// The headline of M7.2: what a machine reads from `run get` / `run list` is
// the bare enum value, at every depth of the document.
func TestJSONFlag_RunGetAndListNameStatusesWithoutTheEnumPrefix(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)
	f.completeRunSnapshot("run-001")

	got := f.run("run", "get", "run-001", "--json").requireExit(t, exitOK)
	var single struct {
		Run struct {
			Status   string `json:"status"`
			TaskRuns []struct {
				TaskID string `json:"taskId"`
				Status string `json:"status"`
			} `json:"taskRuns"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &single); err != nil {
		t.Fatalf("run get --json is not valid JSON: %v\nstdout:\n%s", err, got.stdout)
	}
	if single.Run.Status != "SUCCEEDED" {
		t.Errorf("run get status = %q, want SUCCEEDED\nstdout:\n%s", single.Run.Status, got.stdout)
	}
	if len(single.Run.TaskRuns) != 3 {
		t.Fatalf("task runs = %d, want 3\nstdout:\n%s", len(single.Run.TaskRuns), got.stdout)
	}
	for _, taskRun := range single.Run.TaskRuns {
		if taskRun.Status != "SUCCEEDED" {
			t.Errorf("task %s status = %q, want SUCCEEDED", taskRun.TaskID, taskRun.Status)
		}
	}
	if !strings.Contains(got.stdout, `"status": "SUCCEEDED"`) {
		t.Errorf("run get --json lacks a literal \"status\": \"SUCCEEDED\"\nstdout:\n%s", got.stdout)
	}

	listed := f.run("run", "list", "--json").requireExit(t, exitOK)
	var many struct {
		Runs []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"runs"`
	}
	if err := json.Unmarshal([]byte(listed.stdout), &many); err != nil {
		t.Fatalf("run list --json is not valid JSON: %v\nstdout:\n%s", err, listed.stdout)
	}
	if len(many.Runs) != 1 || many.Runs[0].Status != "SUCCEEDED" {
		t.Fatalf("run list runs = %+v, want one SUCCEEDED run\nstdout:\n%s", many.Runs, listed.stdout)
	}
}

func TestJSONFlag_ApplyCarriesTheCompiledOrkflow(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("apply", examplePath(t), "--json").requireExit(t, exitOK)

	var document struct {
		Orkflow struct {
			Name    string `json:"name"`
			Version string `json:"version"`
			Tasks   []struct {
				ID     string `json:"id"`
				Plugin string `json:"plugin"`
			} `json:"tasks"`
		} `json:"orkflow"`
	}
	if err := json.Unmarshal([]byte(outcome.stdout), &document); err != nil {
		t.Fatalf("apply --json is not valid JSON: %v\nstdout:\n%s", err, outcome.stdout)
	}
	if document.Orkflow.Name != "hello-ork" || document.Orkflow.Version != "0.1.0" {
		t.Fatalf("orkflow = %s@%s, want hello-ork@0.1.0",
			document.Orkflow.Name, document.Orkflow.Version)
	}
	if len(document.Orkflow.Tasks) != 3 {
		t.Fatalf("tasks = %d, want 3", len(document.Orkflow.Tasks))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// completeRunSnapshot projects a finished run with task runs into the read
// model, standing in for the engine projection the absent Orker would drive.
func (f *fixture) completeRunSnapshot(runID string) {
	f.t.Helper()
	run, ok := f.runs.Get(runID)
	if !ok {
		f.t.Fatalf("run %s is not in the store", runID)
	}
	run.Status = orkstackv1.Status_STATUS_SUCCEEDED
	run.StartedAt = timestamppb.New(fixedNow)
	run.FinishedAt = timestamppb.New(fixedNow)
	run.Output = &orkstackv1.Payload{Data: mustStruct(f.t, map[string]any{"saved": "result.txt"})}
	run.TaskRuns = []*orkstackv1.TaskRun{
		newTaskRun("fetch", "http"),
		newTaskRun("transform", "shell"),
		newTaskRun("save", "file"),
	}
	f.runs.Apply(run)
}

func newTaskRun(id, pluginName string) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{
		TaskId:     id,
		Plugin:     pluginName,
		Status:     orkstackv1.Status_STATUS_SUCCEEDED,
		Attempt:    1,
		StartedAt:  timestamppb.New(fixedNow),
		FinishedAt: timestamppb.New(fixedNow),
	}
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	data, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}
	return data
}

// closedAddress returns a loopback address with nothing listening on it.
func closedAddress(t *testing.T) string {
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
