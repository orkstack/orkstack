package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

const awaitedCLIYAML = `name: awaited-cli
version: "1.0.0"
tasks:
  - id: plan
    uses: shell
    with: {op: exec, command: ["echo", "plan"]}
  - id: training
    needs: [plan]
    await:
      timeout_seconds: 21600
      callback: true
      schema:
        model_uri: {type: string, required: true}
  - id: approval
    needs: [plan]
    await: {timeout_seconds: 86400}
  - id: evaluate
    uses: shell
    needs: [training, approval]
    with: {op: exec, command: ["echo", "evaluate"]}
`

// awaitReader is the run record a CLI fixture resolves against: the run and
// what it was started with, held in maps the test fills.
type awaitReader struct {
	mu     sync.Mutex
	runs   map[string]*orkstackv1.Run
	starts map[string]*engine.RunStartRecord
}

func (r *awaitReader) GetRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run, ok := r.runs[runID]; ok {
		return run, nil
	}
	return nil, engine.ErrRunNotFound
}

func (r *awaitReader) ListRuns(context.Context, engine.RunQuery) ([]*orkstackv1.Run, error) {
	return nil, nil
}

func (r *awaitReader) StreamEvents(context.Context, string, int64, func(*orkstackv1.Event) error) error {
	return engine.ErrRunNotFound
}

func (r *awaitReader) Executions(context.Context, engine.StatsWindow) (engine.Executions, error) {
	return engine.Executions{}, nil
}

func (r *awaitReader) RunningCount(context.Context, string, int64) (int64, error) { return 0, nil }

func (r *awaitReader) RunStart(_ context.Context, runID string) (*engine.RunStartRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if start, ok := r.starts[runID]; ok {
		return start, nil
	}
	return nil, engine.ErrRunNotFound
}

// awaitCLIFixture is the CLI fixture with a run record and a local artifact
// store, and one awaiting run seeded: training AWAITING, approval PENDING.
func awaitCLIFixture(t *testing.T, gate orkstackv1.Status) (*fixture, *awaitReader, *orkstackv1.Run) {
	t.Helper()
	reader := &awaitReader{runs: map[string]*orkstackv1.Run{}, starts: map[string]*engine.RunStartRecord{}}
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local store: %v", err)
	}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = reader
		cfg.ArtifactStore = store
	})
	wf, err := dsl.Compile([]byte(awaitedCLIYAML))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opened := fixedNow.Add(-19 * time.Minute)
	run := &orkstackv1.Run{
		Id: "ork-cli-1", OrkflowName: "awaited-cli", OrkflowVersion: "1.0.0",
		Status:    orkstackv1.Status_STATUS_RUNNING,
		CreatedAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "plan", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1", Attempt: 1,
				ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened), FinishedAt: timestamppb.New(opened)},
			{TaskId: "training", Status: gate,
				ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
				Await: &orkstackv1.TaskAwait{Callback: true, Deadline: timestamppb.New(opened.Add(6 * time.Hour))}},
			{TaskId: "approval", Status: orkstackv1.Status_STATUS_PENDING, Await: &orkstackv1.TaskAwait{}},
			{TaskId: "evaluate", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
	salt := []byte("0123456789abcdef")
	reader.runs[run.GetId()] = run
	reader.starts[run.GetId()] = &engine.RunStartRecord{
		Orkflow: wf,
		Plan: engine.RunPlan{Namespace: "default", Awaits: map[string]engine.AwaitCredential{
			"training": {Salt: hex.EncodeToString(salt), Digest: hex.EncodeToString(triggers.HashSecret(salt, "tok"))},
		}},
	}
	return f, reader, run
}

func TestRunResolveGolden(t *testing.T) {
	f, _, _ := awaitCLIFixture(t, orkstackv1.Status_STATUS_AWAITING)
	out := f.run("run", "resolve", "ork-cli-1", "--task", "training", "--values", `{"model_uri":"s3://models/88421"}`).
		requireExit(t, exitOK)
	requireGolden(t, "run-resolve.golden", out.stdout)

	f, _, _ = awaitCLIFixture(t, orkstackv1.Status_STATUS_AWAITING)
	machine := f.run("run", "resolve", "ork-cli-1", "--task", "training", "--values", `{"model_uri":"s3://m"}`, "--json").
		requireExit(t, exitOK)
	requireGolden(t, "run-resolve.json.golden", machine.stdout)

	f, _, _ = awaitCLIFixture(t, orkstackv1.Status_STATUS_AWAITING)
	reject := f.run("run", "resolve", "ork-cli-1", "--task", "training", "--reject", "--reason", "databricks job cancelled").
		requireExit(t, exitOK)
	requireGolden(t, "run-resolve-reject.golden", reject.stdout)
	if sig := f.starter.signals; len(sig) != 1 || !sig[0].GetReject() || sig[0].GetReason() != "databricks job cancelled" {
		t.Fatalf("delivered %v", sig)
	}
}

func TestRunResolveRepeatGolden(t *testing.T) {
	f, reader, run := awaitCLIFixture(t, orkstackv1.Status_STATUS_SUCCEEDED)
	body := `{"model_uri":"s3://models/88421"}`
	sum := sha256Hex([]byte(body))
	reader.mu.Lock()
	run.TaskRuns[1].ValuesRef = &orkstackv1.ArtifactRef{Sha256: sum}
	reader.mu.Unlock()
	out := f.run("run", "resolve", "ork-cli-1", "--task", "training", "--values", body).requireExit(t, exitOK)
	requireGolden(t, "run-resolve-repeat.golden", out.stdout)
	if len(f.starter.signals) != 0 {
		t.Fatalf("a repeat delivered %v", f.starter.signals)
	}
}

func TestRunResolve_FlagRules(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"run", "resolve", "ork-cli-1"}, "--task is required"},
		{[]string{"run", "resolve", "ork-cli-1", "--task", "training", "--values", "{}", "--values-file", "x"}, "--values and --values-file are exclusive"},
		{[]string{"run", "resolve", "ork-cli-1", "--task", "training", "--reject", "--reason", "r", "--values", "{}"}, "values_json and reject are exclusive"},
		{[]string{"run", "resolve", "ork-cli-1", "--task", "training", "--reject", "--reason", "r", "--values-file", "x"}, "values_json and reject are exclusive"},
		{[]string{"run", "resolve", "ork-cli-1", "--task", "training", "--reason", "r"}, "reason is only for a rejection"},
		{[]string{"run", "resolve", "ork-cli-1", "--task", "training", "--reject"}, "reason is required with a rejection"},
	}
	for _, tc := range cases {
		f := newFixture(t)
		out := f.run(tc.args...).requireExit(t, exitError)
		if !strings.Contains(out.stderr, tc.want) {
			t.Errorf("%v: stderr %q, want %q", tc.args, out.stderr, tc.want)
		}
		if requests := f.requests(); len(requests) != 0 {
			t.Errorf("%v: refused locally but sent %v", tc.args, requests)
		}
	}
}

func TestRunResolve_ValuesFileIsSentVerbatim(t *testing.T) {
	f, _, _ := awaitCLIFixture(t, orkstackv1.Status_STATUS_AWAITING)
	body := "{\r\n    \"model_uri\": \"s3://m\",\r\n    \"n\": 12345678901234567890\r\n}\r\n"
	path := filepath.Join(t.TempDir(), "values.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	f.run("run", "resolve", "ork-cli-1", "--task", "training", "--values-file", path).requireExit(t, exitOK)
	if len(f.starter.signals) != 1 || string(f.starter.signals[0].GetValuesJson()) != body {
		t.Fatalf("delivered %q, want the file's exact bytes %q", f.starter.signals, body)
	}
}

var (
	tokenPattern = regexp.MustCompile(`[0-9a-f]{64}`)
	hostPattern  = regexp.MustCompile(`http://127\.0\.0\.1:\d+`)
)

func TestRunSubmitCallbacksGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "awaited.ork.yaml", awaitedCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	out := f.run("run", "submit", "awaited-cli").requireExit(t, exitOK)
	if tokens := tokenPattern.FindAllString(out.stdout, -1); len(tokens) != 1 {
		t.Fatalf("submit printed %d tokens, want exactly one (training): %s", len(tokens), out.stdout)
	}
	normalized := hostPattern.ReplaceAllString(tokenPattern.ReplaceAllString(out.stdout, "<token>"), "http://<server>")
	requireGolden(t, "run-submit-callbacks.golden", normalized)
}

func TestRunSubmit_CallbackCommandIsShellSafe(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh to parse the command with: %v", err)
	}
	baseURL := "http://example.test"
	path := "/v1/runs/ork-1'; rm -rf / #/awaits/gate"
	token := "tok'en$(touch /tmp/pwned)`id`"
	rendered := awaitCurlExample(baseURL, path, token)
	// The URL and header arguments are the two quoted words of the first and
	// second lines; the shell must hand them back unchanged.
	lines := strings.Split(rendered, "\n")
	url := strings.TrimSuffix(strings.TrimPrefix(lines[0], "  curl -X POST "), " \\")
	header := strings.TrimSuffix(strings.TrimPrefix(lines[1], "    -H "), " \\")
	got, err := exec.Command(sh, "-c", "printf '%s\\n' "+url+" "+header).Output()
	if err != nil {
		t.Fatalf("sh: %v", err)
	}
	want := baseURL + path + "\nAuthorization: Bearer " + token + "\n"
	if string(got) != want {
		t.Fatalf("the shell read\n%q\nwant\n%q", got, want)
	}
}

func TestRunGetAwaitGolden(t *testing.T) {
	saved := nowFunc
	nowFunc = func() time.Time { return fixedNow }
	defer func() { nowFunc = saved }()
	f, reader, run := awaitCLIFixture(t, orkstackv1.Status_STATUS_AWAITING)
	reader.mu.Lock()
	run.TaskRuns[2].Status = orkstackv1.Status_STATUS_SUCCEEDED
	run.TaskRuns[2].Await = &orkstackv1.TaskAwait{
		Deadline:    timestamppb.New(fixedNow.Add(23 * time.Hour)),
		ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		ResolvedBy:  "alice",
	}
	reader.mu.Unlock()
	out := f.run("run", "get", "ork-cli-1").requireExit(t, exitOK)
	requireGolden(t, "run-get-await.golden", out.stdout)
}

func TestRunPlanAwaitGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "awaited.ork.yaml", awaitedCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	out := f.run("run", "plan", "awaited-cli").requireExit(t, exitOK)
	requireGolden(t, "run-plan-await.golden", out.stdout)
}

func TestOrkflowGetAwaitGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "awaited.ork.yaml", awaitedCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	out := f.run("orkflow", "get", "awaited-cli").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-await.golden", out.stdout)
}

func TestRunPlan_AssertRunsAcceptsAnAwait(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "awaited.ork.yaml", awaitedCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	f.run("run", "plan", "awaited-cli", "--assert-runs", "training", "--assert-runs", "evaluate").requireExit(t, exitOK)
	f.run("run", "plan", "awaited-cli", "--assert-skipped", "training").requireExit(t, exitPlanAssertionFailed)
}

func TestRunTiming_AnAwaitedTaskIsAwaitedNotQueued(t *testing.T) {
	opened := fixedNow
	run := &orkstackv1.Run{Id: "ork-t", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "gate", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
		FinishedAt: timestamppb.New(opened.Add(90 * time.Minute)),
		Await:      &orkstackv1.TaskAwait{},
	}}}
	timing := computeRunTiming(run, nil, timingOptions{AttemptStore: true, Now: fixedNow})
	task := timing.Tasks[0]
	if !task.Awaited || task.Wait.applies || task.QueueWait.applies || !task.Run.known || task.Run.value != 90*time.Minute {
		t.Fatalf("timing = %+v", task)
	}
	var b strings.Builder
	if err := renderRunTiming(&b, timing, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "awaited 1h30m0s") || strings.Contains(b.String(), "attempts:") {
		t.Fatalf("rendered:\n%s", b.String())
	}
}

func sha256Hex(b []byte) string {
	sum := sha256Sum(b)
	return hex.EncodeToString(sum[:])
}

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }

// TestRunSubmitWatchJSON_PrintsTheCallbackOnStderr: under --watch --json the
// one-time await callback is still shown once — on stderr, so stdout stays
// the single JSON document the watch prints.
func TestRunSubmitWatchJSON_PrintsTheCallbackOnStderr(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "awaited.ork.yaml", awaitedCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	f.starter.onStart = func(runID string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload) {
		f.finishRun(runID, orkstackv1.Status_STATUS_SUCCEEDED, nil)
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	}
	out := f.run("run", "submit", "awaited-cli", "--watch", "--json").requireExit(t, exitOK)
	if tokens := tokenPattern.FindAllString(out.stderr, -1); len(tokens) != 1 {
		t.Fatalf("stderr carries %d tokens, want exactly one (training):\n%s", len(tokens), out.stderr)
	}
	for _, want := range []string{"Callback for await training", "curl -X POST", "/awaits/training"} {
		if !strings.Contains(out.stderr, want) {
			t.Fatalf("stderr does not contain %q:\n%s", want, out.stderr)
		}
	}
	if tokenPattern.MatchString(out.stdout) {
		t.Fatalf("stdout carries the token; it belongs on stderr only:\n%s", out.stdout)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out.stdout), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, out.stdout)
	}
}

// TestRunSubmitWatchJSON_NoCallbacksLeavesStderrEmpty: a run with nothing to
// disclose writes nothing to stderr.
func TestRunSubmitWatchJSON_NoCallbacksLeavesStderrEmpty(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.starter.onStart = func(runID string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload) {
		f.finishRun(runID, orkstackv1.Status_STATUS_SUCCEEDED, nil)
		f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	}
	out := f.run("run", "submit", "hello-ork", "--watch", "--json").requireExit(t, exitOK)
	if out.stderr != "" {
		t.Fatalf("stderr = %q, want empty", out.stderr)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out.stdout), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, out.stdout)
	}
}
