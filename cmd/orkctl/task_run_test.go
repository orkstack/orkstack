package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ---------------------------------------------------------------- fixture

const (
	taskRunShellRef    = fakeRegistry + "/test/fake:dev"
	taskRunShellDigest = "sha256:" + "5eed0000000000000000000000000000000000000000000000000000000000aa"
	taskRunPinned      = fakeRegistry + "/test/fake@" + taskRunShellDigest
	taskRunSourceRun   = "ork-1790000000000000000-4294967297"
	taskRunPayload     = "payload marker 6c1b3f2e9a\n"
	taskRunSideBytes   = "side output\n"
)

// taskRunFixtureYAML is the CLI suite's task-run-fixture: produce -> consume,
// plus the members C10 needs (a skipped task and a second producer), a task
// declaring a secret, one whose with: reads an output key that does not
// exist, and a pinned task.
const taskRunFixtureYAML = `name: task-run-fixture
version: "1"
inputs:
  target:
    type: string
  expect:
    type: string
    default: prod
pins:
  p: {}
tasks:
  - id: produce
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo produce"]
  - id: skipme
    uses: shell
    condition: '{{ eq .inputs.target "never" }}'
    with:
      op: exec
      command: ["sh", "-c", "echo skipped"]
  - id: side
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo side"]
  - id: consume
    uses: shell
    needs: [produce]
    with:
      op: exec
      command: ["sh", "-c", "test \"$TARGET\" = \"$EXPECT\""]
      target: '{{ .tasks.produce.output.target }}'
      expect: '{{ .inputs.expect }}'
      payload: '{{ artifact "produce/payload.txt" }}'
  - id: gather
    uses: shell
    needs: [produce, skipme, side]
    with:
      op: exec
      command: ["sh", "-c", "echo gather"]
  - id: secretive
    uses: shell
    needs: [produce]
    secrets:
      - name: tok
        source: "env:NOT_ON_THIS_ORKER"
    with:
      op: exec
      command: ["sh", "-c", "sha256sum /orkstack/input/secrets/tok"]
  - id: broken
    uses: shell
    needs: [produce]
    with:
      op: exec
      command: ["sh", "-c", "echo {{ .tasks.produce.output.missing }}"]
  - id: pinned
    uses: shell
    pin: p
    run_as_orker_user: true
    with:
      op: exec
      command: ["sh", "-c", "ls /orkstack/shared"]
`

type taskRunFixture struct {
	*fixture
	store    *artifacts.LocalStore
	orkflows *engine.MemoryOrkflowStore
	wf       *orkstackv1.Orkflow
	workdir  string
	file     string
	values   *orkstackv1.ArtifactRef
	payload  *orkstackv1.ArtifactRef
	side     *orkstackv1.ArtifactRef
	sideOut  *orkstackv1.ArtifactRef
}

func taskRunResolution() *dsl.Resolution {
	return &dsl.Resolution{Image: &orkstackv1.ImageRef{
		Registry: fakeRegistry, Repository: "test/fake", IndexDigest: taskRunShellDigest,
		Pinned: taskRunPinned, MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"},
	}}
}

// newTaskRunFixture serves the real API with a local artifact store, a
// catalog naming `shell`, and a stub resolver pinning it; applies the fixture
// orkflow through `orkctl apply`; and seeds the source run.
func newTaskRunFixture(t *testing.T) *taskRunFixture {
	t.Helper()
	workdir := t.TempDir()
	t.Setenv(orker.WorkRootVar, workdir)
	t.Setenv(orker.EnvAllowlistVar, "")
	for _, name := range []string{orker.MaxCPUVar, orker.MaxMemoryVar, orker.DefaultCPUVar, orker.DefaultMemoryVar,
		orker.PidsLimitVar, orker.TmpfsBytesVar, orker.ForceUserVar, orker.SecretSchemesVar, orker.SecretAllowVar} {
		t.Setenv(name, "")
	}
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	orkflows := engine.NewMemoryOrkflowStore()
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.ArtifactStore = store
		cfg.Orkflows = orkflows
		cfg.Catalog = dsl.Catalog{"shell": taskRunShellRef}
		cfg.Resolver = stubResolver{taskRunShellRef: taskRunResolution()}
	})
	fx := &taskRunFixture{fixture: f, store: store, orkflows: orkflows, workdir: workdir}
	fx.file = fx.writeDoc(t, "task-run-fixture.ork.yaml", taskRunFixtureYAML)
	f.run("apply", fx.file).requireExit(t, exitOK)
	wf, err := orkflows.Get(context.Background(), "default", "task-run-fixture", "1")
	if err != nil {
		t.Fatalf("stored fixture: %v", err)
	}
	fx.wf = wf
	fx.values = fx.put(t, "values.json", `{"target":"staging"}`)
	fx.payload = fx.put(t, "payload.txt", taskRunPayload)
	fx.side = fx.put(t, "values.json", `{"side":true}`)
	fx.sideOut = fx.put(t, "side.txt", taskRunSideBytes)
	fx.seedRun(t, taskRunSourceRun, nil)
	return fx
}

func (fx *taskRunFixture) writeDoc(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func (fx *taskRunFixture) put(t *testing.T, name, body string) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := fx.store.Put(context.Background(), name, strings.NewReader(body), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return protoRef(ref)
}

// seedRun records a finished source run carrying the stored version's name,
// version and digest. edit, when non-nil, adjusts it before it is applied.
func (fx *taskRunFixture) seedRun(t *testing.T, id string, edit func(*orkstackv1.Run)) {
	t.Helper()
	succeeded := func(task string, values *orkstackv1.ArtifactRef, outputs ...*orkstackv1.ArtifactRef) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{
			TaskId: task, Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1,
			ValuesRef: values, Artifacts: outputs,
			Commit: &orkstackv1.CommitRef{Uri: "s3://fake/commits/" + task},
		}
	}
	run := &orkstackv1.Run{
		Id: id, OrkflowName: fx.wf.GetName(), OrkflowVersion: fx.wf.GetVersion(),
		OrkflowDefinitionDigest: fx.wf.GetDefinitionDigest(),
		Status:                  orkstackv1.Status_STATUS_FAILED,
		Input:                   payloadOf(t, map[string]any{"target": "staging"}),
		CreatedAt:               timestamppb.New(fixedNow),
		FinishedAt:              timestamppb.New(fixedNow),
		TaskRuns: []*orkstackv1.TaskRun{
			succeeded("produce", fx.values, fx.payload),
			{TaskId: "skipme", Status: orkstackv1.Status_STATUS_SKIPPED},
			succeeded("side", fx.side, fx.sideOut),
			{TaskId: "consume", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1,
				ScheduledAt: timestamppb.New(fixedNow), StartedAt: timestamppb.New(fixedNow),
				Error: &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO",
					Message: `CONTAINER_EXIT_NONZERO: task "consume": the container exited 1`}},
			{TaskId: "gather", Status: orkstackv1.Status_STATUS_CANCELED},
			{TaskId: "secretive", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1,
				ScheduledAt: timestamppb.New(fixedNow), StartedAt: timestamppb.New(fixedNow),
				Error: &orkstackv1.Error{Code: "SECRET_UNRESOLVED",
					Message: `SECRET_UNRESOLVED: secret "tok": environment variable "NOT_ON_THIS_ORKER" is not set on this orker`}},
			{TaskId: "broken", Status: orkstackv1.Status_STATUS_CANCELED},
			{TaskId: "pinned", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1},
		},
	}
	if edit != nil {
		edit(run)
	}
	fx.runs.Apply(run)
}

func payloadOf(t *testing.T, values map[string]any) *orkstackv1.Payload {
	t.Helper()
	data, err := structFromMap(values)
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: data}
}

// runTaskRun mirrors runTo, but builds the command tree with newRootCommand
// so the container runtime seam can be set, and maps the returned error
// through exitCodeFor exactly as Execute does.
func (f *fixture) runTaskRun(rt orker.ContainerRuntime, args ...string) result {
	f.t.Helper()
	return f.runTaskRunContext(context.Background(), nil, rt, args...)
}

func (f *fixture) runTaskRunContext(ctx context.Context, watch io.Writer, rt orker.ContainerRuntime, args ...string) result {
	f.t.Helper()
	root, flags := newRootCommand()
	defer flags.closeIdleConnections()
	flags.containerRuntime = rt
	var stdout, stderr bytes.Buffer
	out := io.Writer(&stdout)
	if watch != nil {
		out = io.MultiWriter(&stdout, watch)
	}
	root.SetArgs(append(append([]string{"task", "run"}, args...), "--server", f.baseURL))
	root.SetOut(out)
	root.SetErr(&stderr)
	err := root.ExecuteContext(ctx)
	if err != nil {
		fmt.Fprintf(&stderr, "Error: %v\n", err)
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: exitCodeFor(err)}
}

// fakeTaskRuntime implements orker.ContainerRuntime for the CLI suite. Start
// runs a scripted func that reads the input mount's host source and writes
// into the output mount's, and writes scripted lines to the two writers.
type fakeTaskRuntime struct {
	mu          sync.Mutex
	describeErr error
	pulls       int
	specs       []orker.ContainerSpec
	starts      int
	signals     []orker.Signal
	removes     int
	exit        int32
	block       bool
	term        chan struct{}
	termOnce    sync.Once
	onCreate    func(spec orker.ContainerSpec)
	onStart     func(spec orker.ContainerSpec)
	script      func(spec orker.ContainerSpec, stdout, stderr io.Writer) int32
}

func (r *fakeTaskRuntime) Pull(context.Context, string) (orker.PullInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pulls++
	return orker.PullInfo{PlatformDigest: "sha256:" + strings.Repeat("f", 64), Platform: "linux/amd64", Delivery: orker.DeliveryRegistry}, nil
}

func (r *fakeTaskRuntime) LoadLayout(context.Context, string, string) (orker.PullInfo, error) {
	return orker.PullInfo{}, orker.ErrNotInLayout
}

func (r *fakeTaskRuntime) Create(_ context.Context, spec orker.ContainerSpec) (orker.ContainerID, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	onCreate := r.onCreate
	r.mu.Unlock()
	if onCreate != nil {
		onCreate(spec)
	}
	return "fake-container", nil
}

func (r *fakeTaskRuntime) Start(_ context.Context, _ orker.ContainerID, stdout, stderr io.Writer) error {
	r.mu.Lock()
	r.starts++
	spec := r.specs[len(r.specs)-1]
	onStart, script := r.onStart, r.script
	r.mu.Unlock()
	if onStart != nil {
		onStart(spec)
	}
	exit := int32(0)
	if script != nil {
		exit = script(spec, stdout, stderr)
	}
	r.mu.Lock()
	r.exit = exit
	r.mu.Unlock()
	return nil
}

func (r *fakeTaskRuntime) Signal(_ context.Context, _ orker.ContainerID, sig orker.Signal) error {
	r.mu.Lock()
	r.signals = append(r.signals, sig)
	term := r.term
	r.mu.Unlock()
	if sig == orker.SIGTERM && term != nil {
		r.termOnce.Do(func() { close(term) })
	}
	return nil
}

func (r *fakeTaskRuntime) Wait(context.Context, orker.ContainerID) (orker.ExitStatus, error) {
	r.mu.Lock()
	block, term := r.block, r.term
	r.mu.Unlock()
	if block {
		<-term
		return orker.ExitStatus{Code: 143}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return orker.ExitStatus{Code: r.exit}, nil
}

func (r *fakeTaskRuntime) Remove(context.Context, orker.ContainerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removes++
	return nil
}

func (r *fakeTaskRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (r *fakeTaskRuntime) Describe(context.Context) (orker.RuntimeInfo, error) {
	if r.describeErr != nil {
		return orker.RuntimeInfo{}, r.describeErr
	}
	return orker.RuntimeInfo{Name: "fake", Version: "1"}, nil
}

func (r *fakeTaskRuntime) started() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts
}

func (r *fakeTaskRuntime) created() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.specs)
}

func mountSource(spec orker.ContainerSpec, destination string) string {
	for _, mount := range spec.Mounts {
		if mount.Destination == destination {
			return mount.Source
		}
	}
	return ""
}

// exitWith is a script that writes one stderr line and exits code.
func exitWith(code int32, stderrLine string) func(orker.ContainerSpec, io.Writer, io.Writer) int32 {
	return func(_ orker.ContainerSpec, _, stderr io.Writer) int32 {
		if stderrLine != "" {
			fmt.Fprintln(stderr, stderrLine)
		}
		return code
	}
}

// succeedWith is a script that writes values.json and one artifact.
func succeedWith(values, artifact string) func(orker.ContainerSpec, io.Writer, io.Writer) int32 {
	return func(spec orker.ContainerSpec, stdout, _ io.Writer) int32 {
		out := mountSource(spec, orker.ContainerOutput)
		_ = os.WriteFile(filepath.Join(out, "values.json"), []byte(values), 0o644)
		if artifact != "" {
			_ = os.WriteFile(filepath.Join(out, "artifacts", "result.txt"), []byte(artifact), 0o644)
		}
		fmt.Fprintln(stdout, "done")
		return 0
	}
}

var (
	taskRunLocalID  = regexp.MustCompile(`local-[0-9a-f]{16}`)
	taskRunDuration = regexp.MustCompile(`\b\d+(\.\d+)?(ms|s)\b`)
	taskRunMillis   = regexp.MustCompile(`"(totalMs|containerMs)": \d+`)
)

// normalizeTaskRun replaces the three nondeterministic values of a transcript
// before a golden comparison: the local run id, durations, and the session
// path.
func normalizeTaskRun(out, workdir string) string {
	out = regexp.MustCompile(regexp.QuoteMeta(workdir)+`/ork-task-run-[0-9a-f]{16}`).ReplaceAllString(out, "<session>")
	out = taskRunLocalID.ReplaceAllString(out, "local-<id>")
	out = taskRunDuration.ReplaceAllString(out, "<d>")
	return taskRunMillis.ReplaceAllString(out, `"$1": 0`)
}

func sessions(t *testing.T, workdir string) []string {
	t.Helper()
	entries, err := os.ReadDir(workdir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "ork-task-run-") {
			out = append(out, filepath.Join(workdir, entry.Name()))
		}
	}
	return out
}

func requireNoSession(t *testing.T, workdir string) {
	t.Helper()
	if left := sessions(t, workdir); len(left) > 0 {
		t.Fatalf("session directories were left behind: %v", left)
	}
}

func requestsSince(f *fixture, before int) []string {
	return f.requests()[before:]
}

func countRequests(paths []string, method string) int {
	n := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "/"+method) {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------- C1

func TestTaskRun_RefusesMalformedFlagsWithoutAnyRequest(t *testing.T) {
	fx := newTaskRunFixture(t)
	dir := t.TempDir()
	regular := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(regular, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	withLink := filepath.Join(dir, "seed")
	if err := os.MkdirAll(withLink, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(regular, filepath.Join(withLink, "link")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORK_TEST_TASK_RUN_UNSET_VARIABLE", "")
	_ = os.Unsetenv("ORK_TEST_TASK_RUN_UNSET_VARIABLE")
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"no task", []string{"--from-run", taskRunSourceRun}, nil, "--task is required"},
		{"no source", []string{"--task", "consume"}, nil, "one of --from-run or --file is required"},
		{"secret with render-only", []string{"--task", "consume", "--from-run", "r", "--render-only", "--secret", "tok=file:" + regular}, nil,
			"--secret has no effect with --render-only: no container starts, so nothing is staged"},
		{"omit-secret with render-only", []string{"--task", "consume", "--from-run", "r", "--render-only", "--omit-secret", "tok"}, nil,
			"--omit-secret has no effect with --render-only"},
		{"shared with render-only", []string{"--task", "consume", "--from-run", "r", "--render-only", "--shared", dir}, nil,
			"--shared has no effect with --render-only"},
		{"malformed output", []string{"--task", "consume", "--from-run", "r", "--output", "nope"}, nil,
			`invalid --output "nope": expected TASK=JSON`},
		{"malformed artifact", []string{"--task", "consume", "--from-run", "r", "--artifact", "nope"}, nil,
			`invalid --artifact "nope": want TASK/NAME=PATH`},
		{"artifact name", []string{"--task", "consume", "--from-run", "r", "--artifact", "produce/..=" + regular}, nil,
			`NAME ".." is not a file name an Orker can produce`},
		{"artifact path", []string{"--task", "consume", "--from-run", "r", "--artifact", "produce/a.txt=" + dir}, nil,
			"is not a regular file"},
		{"malformed secret", []string{"--task", "consume", "--from-run", "r", "--secret", "tok"}, nil,
			`invalid --secret "tok": want NAME=file:PATH or NAME=env:VAR`},
		{"missing secret file", []string{"--task", "consume", "--from-run", "r", "--secret", "tok=file:/nonexistent/tok"}, nil,
			"--secret tok: /nonexistent/tok: no such file"},
		{"unset secret variable", []string{"--task", "consume", "--from-run", "r", "--secret", "tok=env:ORK_TEST_TASK_RUN_UNSET_VARIABLE"}, nil,
			"--secret tok: environment variable ORK_TEST_TASK_RUN_UNSET_VARIABLE is not set"},
		{"secret scheme", []string{"--task", "consume", "--from-run", "r", "--secret", "tok=vault:x"}, nil,
			`--secret tok: scheme "vault" is not one this machine resolves (allowed: env, file)`},
		{"shared not a directory", []string{"--task", "consume", "--from-run", "r", "--shared", regular}, nil,
			"is not a directory"},
		{"shared with a symlink", []string{"--task", "consume", "--from-run", "r", "--shared", withLink}, nil,
			"is a symlink; only regular files and directories are copied"},
		{"supplied and omitted", []string{"--task", "consume", "--from-run", "r", "--secret", "tok=file:" + regular, "--omit-secret", "tok"}, nil,
			`secret "tok" is both supplied and omitted`},
		{"invalid policy", []string{"--task", "consume", "--from-run", "r"}, map[string]string{orker.MaxCPUVar: "lots"},
			orker.MaxCPUVar + `="lots" is not a valid cpu quantity`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			before := len(fx.requests())
			rt := &fakeTaskRuntime{}
			got := fx.runTaskRun(rt, c.args...).requireExit(t, exitError).requireStderrContains(t, c.want)
			if sent := requestsSince(fx.fixture, before); len(sent) > 0 {
				t.Fatalf("a refused flag sent %v", sent)
			}
			_ = got
			requireNoSession(t, fx.workdir)
		})
	}
}

// ---------------------------------------------------------------- C2, C3, C4, C5

func TestTaskRun_ReplaysAgainstTheRunsRecordedBytes(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{script: exitWith(1, "mismatch: staging != prod")}
	var config map[string]any
	rt.onCreate = func(spec orker.ContainerSpec) {
		input := mountSource(spec, orker.ContainerInput)
		requireFileBytes(t, filepath.Join(input, "upstream", "produce", "values.json"), `{"target":"staging"}`)
		requireFileBytes(t, filepath.Join(input, "artifacts", "produce", "payload.txt"), taskRunPayload)
		raw, err := os.ReadFile(filepath.Join(input, "config.json"))
		if err != nil {
			t.Errorf("config.json: %v", err)
			return
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			t.Errorf("config.json: %v", err)
		}
	}
	got := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitRunFailed)
	if config["target"] != "staging" || config["payload"] != "/orkstack/input/artifacts/produce/payload.txt" || config["expect"] != "prod" {
		t.Errorf("config.json = %v, want the run's upstream value, the staged container path and the defaulted input", config)
	}
	requireGolden(t, "task-run-failed.golden", normalizeTaskRun(got.stdout, fx.workdir))
	requireNoSession(t, fx.workdir)
}

func requireFileBytes(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("read %s: %v", path, err)
		return
	}
	if string(raw) != want {
		t.Errorf("%s = %q, want %q", path, raw, want)
	}
}

func TestTaskRun_SucceedsAndReportsValuesAndArtifacts(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{script: succeedWith(`{"ok":true,"target":"staging"}`, "result bytes\n")}
	human := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "consume", "--input", "expect=staging").
		requireExit(t, exitOK)
	requireGolden(t, "task-run-succeeded.golden", normalizeTaskRun(human.stdout, fx.workdir))

	rt = &fakeTaskRuntime{script: succeedWith(`{"ok":true,"target":"staging"}`, "result bytes\n")}
	machine := fx.runTaskRun(rt, "--json", "--from-run", taskRunSourceRun, "--task", "consume", "--input", "expect=staging").
		requireExit(t, exitOK)
	requireGolden(t, "task-run-succeeded.json.golden", normalizeTaskRun(machine.stdout, fx.workdir))
	var report taskRunReport
	if err := json.Unmarshal([]byte(machine.stdout), &report); err != nil {
		t.Fatalf("--json stdout is not only the report: %v\n%s", err, machine.stdout)
	}
	if !strings.Contains(machine.stderr, "› running") || !strings.Contains(machine.stderr, "  │ done") {
		t.Errorf("--json progress and streams did not go to stderr:\n%s", machine.stderr)
	}
	if strings.Contains(machine.stderr, taskRunSideEffects) {
		t.Error("the side-effect sentence is printed in human mode only")
	}
}

func TestTaskRun_RenderOnlyStartsNoContainer(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{}
	got := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "consume", "--render-only").requireExit(t, exitOK)
	if rt.started() != 0 || rt.created() != 0 {
		t.Fatalf("--render-only created %d and started %d containers", rt.created(), rt.started())
	}
	if !strings.Contains(got.stdout, "RENDERED  no container was started") {
		t.Fatalf("no RENDERED outcome:\n%s", got.stdout)
	}
	requireGolden(t, "task-run-render-only.golden", normalizeTaskRun(got.stdout, fx.workdir))
}

func TestTaskRun_RenderFailureIsATaskFailure(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{}
	got := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "broken").requireExit(t, exitRunFailed)
	if !strings.Contains(got.stdout, "FAILED  RENDER_FAILED (ContractViolation)") {
		t.Fatalf("not a RENDER_FAILED task failure:\n%s", got.stdout)
	}
	if rt.created() != 0 {
		t.Fatal("a render failure created a container")
	}
}

// ---------------------------------------------------------------- C6, C7

func TestTaskRun_WritesNothingToTheDeployment(t *testing.T) {
	fx := newTaskRunFixture(t)
	ctx := context.Background()
	storeBefore, err := fx.store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	runsBefore := len(fx.runs.List(engine.ListFilter{}))
	before := len(fx.requests())

	fx.runTaskRun(&fakeTaskRuntime{script: exitWith(1, "nope")}, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitRunFailed)
	fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "x")}, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitOK)
	fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "")}, "--file", fx.file, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitOK)

	allowed := map[string]bool{
		"/orkstack.v1.RunService/GetRun": true, "/orkstack.v1.OrkflowService/GetOrkflow": true,
		"/orkstack.v1.OrkflowService/ValidateOrkflow": true, "/orkstack.v1.RunService/ReadArtifact": true,
	}
	for _, path := range requestsSince(fx.fixture, before) {
		if !allowed[path] {
			t.Errorf("task run called %s; it may call only GetRun, GetOrkflow, ValidateOrkflow and ReadArtifact", path)
		}
	}
	storeAfter, err := fx.store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(storeAfter) != fmt.Sprint(storeBefore) {
		t.Errorf("the deployment's artifact store changed:\nbefore %v\nafter  %v", storeBefore, storeAfter)
	}
	if after := len(fx.runs.List(engine.ListFilter{})); after != runsBefore {
		t.Errorf("runs: %d before, %d after", runsBefore, after)
	}
	if started := fx.starter.startedRuns(); len(started) > 0 {
		t.Errorf("task run started runs %v", started)
	}
}

func TestTaskRun_NeverReusesTheSourceRunsIdentity(t *testing.T) {
	fx := newTaskRunFixture(t)
	var keys []string
	for range 2 {
		rt := &fakeTaskRuntime{script: exitWith(1, "")}
		rt.onCreate = func(spec orker.ContainerSpec) {
			raw, err := os.ReadFile(filepath.Join(mountSource(spec, orker.ContainerInput), "task.json"))
			if err != nil {
				t.Errorf("task.json: %v", err)
				return
			}
			var doc struct {
				IdempotencyKey string `json:"idempotencyKey"`
				Run            struct {
					ID string `json:"id"`
				} `json:"run"`
			}
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Errorf("task.json: %v", err)
			}
			if !regexp.MustCompile(`^local-[0-9a-f]{16}$`).MatchString(doc.Run.ID) {
				t.Errorf("task.json run.id = %q, want a fresh local-<16 hex> id", doc.Run.ID)
			}
			if doc.IdempotencyKey == "default/"+taskRunSourceRun+"/consume" || !strings.HasPrefix(doc.IdempotencyKey, "default/local-") {
				t.Errorf("task.json idempotencyKey = %q reuses or does not name the local run", doc.IdempotencyKey)
			}
			keys = append(keys, doc.IdempotencyKey)
			for label, value := range spec.Labels {
				if strings.Contains(value, taskRunSourceRun) {
					t.Errorf("container label %s=%s carries the source run id", label, value)
				}
			}
		}
		fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitRunFailed)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("two invocations produced keys %v, want two different ones", keys)
	}
}

// ---------------------------------------------------------------- C8, C9

func TestTaskRun_RefusesAnUnaccountedSecretBeforeFetching(t *testing.T) {
	fx := newTaskRunFixture(t)
	before := len(fx.requests())
	got := fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", taskRunSourceRun, "--task", "secretive").requireExit(t, exitError)
	got.requireStderrContains(t, `task "secretive" declares secret "tok"; supply it with --secret tok=file:PATH or --secret tok=env:VAR, or run without it with --omit-secret tok`)
	if n := countRequests(requestsSince(fx.fixture, before), "ReadArtifact"); n != 0 {
		t.Fatalf("%d ReadArtifact requests before the secret was accounted for", n)
	}
	requireNoSession(t, fx.workdir)
	// A secret the task does not declare is named too.
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", taskRunSourceRun, "--task", "consume", "--omit-secret", "tok").
		requireExit(t, exitError).
		requireStderrContains(t, `--omit-secret tok: task "consume" declares no secret named "tok" (declared: none)`)
}

func TestTaskRun_StagesASuppliedSecretAndKeepsNoByteOfIt(t *testing.T) {
	fx := newTaskRunFixture(t)
	marker := "secret-marker-" + strings.Repeat("9", 24)
	secretFile := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(secretFile, []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	rt := &fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "")}
	staged := ""
	rt.onCreate = func(spec orker.ContainerSpec) {
		raw, _ := os.ReadFile(filepath.Join(mountSource(spec, orker.ContainerSecrets), "tok"))
		staged = string(raw)
	}
	got := fx.runTaskRun(rt, "--json", "--keep", "--from-run", taskRunSourceRun, "--task", "secretive",
		"--secret", "tok=file:"+secretFile).requireExit(t, exitOK)
	if staged != marker {
		t.Fatalf("the secrets mount held %q at Create, want the supplied value", staged)
	}
	var report taskRunReport
	if err := json.Unmarshal([]byte(got.stdout), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Secrets) != 1 || report.Secrets[0] != (taskRunSecret{Name: "tok", Staged: true, Scheme: "file"}) {
		t.Errorf("report secrets = %+v, want tok staged from file", report.Secrets)
	}
	if strings.Contains(got.stdout, marker) || strings.Contains(got.stderr, marker) || strings.Contains(got.stdout, secretFile) {
		t.Error("the secret's value or source reached the transcript or report")
	}
	kept := sessions(t, fx.workdir)
	if len(kept) != 1 {
		t.Fatalf("--keep left %v", kept)
	}
	_ = filepath.WalkDir(kept[0], func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr == nil && bytes.Contains(raw, []byte(marker)) {
			t.Errorf("kept file %s contains the secret", path)
		}
		return nil
	})

	// --omit-secret stages nothing and task.json lists no secret.
	rt = &fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "")}
	rt.onCreate = func(spec orker.ContainerSpec) {
		entries, _ := os.ReadDir(mountSource(spec, orker.ContainerSecrets))
		if len(entries) != 0 {
			t.Errorf("--omit-secret staged %d secrets", len(entries))
		}
		raw, _ := os.ReadFile(filepath.Join(mountSource(spec, orker.ContainerInput), "task.json"))
		var doc struct {
			Secrets []any `json:"secrets"`
		}
		_ = json.Unmarshal(raw, &doc)
		if len(doc.Secrets) != 0 {
			t.Errorf("task.json lists %v", doc.Secrets)
		}
	}
	omitted := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "secretive", "--omit-secret", "tok").requireExit(t, exitOK)
	omitted.requireContains(t, "tok  omitted (--omit-secret)")
}

// ---------------------------------------------------------------- C10, C11, C12

func TestTaskRun_UpstreamSourcesAreExplicit(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "")}
	rt.onCreate = func(spec orker.ContainerSpec) {
		input := mountSource(spec, orker.ContainerInput)
		requireFileBytes(t, filepath.Join(input, "upstream", "produce", "values.json"), `{"target":"staging"}`)
		requireFileBytes(t, filepath.Join(input, "upstream", "side", "values.json"), `{"overridden":1}`)
		if _, err := os.Stat(filepath.Join(input, "artifacts", "side")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("an overridden producer's run artifacts were staged: %v", err)
		}
		if _, err := os.Stat(filepath.Join(input, "upstream", "skipme")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("a skipped member was staged: %v", err)
		}
	}
	got := fx.runTaskRun(rt, "--json", "--from-run", taskRunSourceRun, "--task", "gather", "--output", `side={"overridden":1}`).
		requireExit(t, exitOK)
	var report taskRunReport
	if err := json.Unmarshal([]byte(got.stdout), &report); err != nil {
		t.Fatal(err)
	}
	from := map[string]string{}
	for _, up := range report.Upstream {
		from[up.TaskID] = up.From
	}
	if from["produce"] != "run" || from["skipme"] != "none" || from["side"] != "supplied" {
		t.Errorf("upstream sources = %v, want produce=run skipme=none side=supplied", from)
	}

	// A producer outside the closure, and a member the run has not finished.
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", taskRunSourceRun, "--task", "consume", "--output", `side={}`).
		requireExit(t, exitError).
		requireStderrContains(t, `--output side: "side" is not upstream of "consume" (upstream: produce)`)
	fx.seedRun(t, "ork-1790000000000000001-1", func(run *orkstackv1.Run) {
		run.TaskRuns[0] = &orkstackv1.TaskRun{TaskId: "produce", Status: orkstackv1.Status_STATUS_RUNNING}
	})
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", "ork-1790000000000000001-1", "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, `upstream task "produce" is RUNNING in run ork-1790000000000000001-1; its output does not exist yet — wait for it, or supply it with --output/--artifact`)
}

func TestTaskRun_DefinitionMustBeTheRunsOrkflow(t *testing.T) {
	fx := newTaskRunFixture(t)
	other := fx.writeDoc(t, "other.ork.yaml", strings.Replace(taskRunFixtureYAML, "name: task-run-fixture", "name: other-fixture", 1))
	fx.runTaskRun(&fakeTaskRuntime{}, "--file", other, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, `--file defines orkflow "other-fixture" but run `+taskRunSourceRun+` ran "task-run-fixture"`)

	fx.seedRun(t, "ork-1790000000000000002-2", func(run *orkstackv1.Run) {
		run.OrkflowDefinitionDigest = "sha256:" + strings.Repeat("0", 64)
	})
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", "ork-1790000000000000002-2", "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, "run ork-1790000000000000002-2 pinned sha256:"+strings.Repeat("0", 64)+
			" but the stored task-run-fixture@1 is "+fx.wf.GetDefinitionDigest()+"; nothing was run")

	fx.seedRun(t, "ork-1790000000000000003-3", func(run *orkstackv1.Run) { run.OrkflowDefinitionDigest = "" })
	fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{}`, "")}, "--from-run", "ork-1790000000000000003-3", "--task", "consume").
		requireExit(t, exitOK).
		requireContains(t, "the run recorded no definition digest")
}

func TestTaskRun_ValidationErrorsRefuseBeforeFetching(t *testing.T) {
	fx := newTaskRunFixture(t)
	invalid := fx.writeDoc(t, "invalid.ork.yaml", strings.Replace(taskRunFixtureYAML, "  - id: consume\n    uses: shell", "  - id: consume\n    uses: nosuch", 1))
	validated := fx.run("orkflow", "validate", invalid)
	before := len(fx.requests())
	got := fx.runTaskRun(&fakeTaskRuntime{}, "--file", invalid, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, "the document does not validate; nothing was run")
	issue := strings.SplitN(validated.stdout, "\n", 2)[0]
	if !strings.HasPrefix(issue, "error") || !strings.Contains(got.stdout, issue) {
		t.Fatalf("the ERROR issue does not render as `orkflow validate` renders it:\nvalidate: %q\ntask run:\n%s", issue, got.stdout)
	}
	if n := countRequests(requestsSince(fx.fixture, before), "ReadArtifact"); n != 0 {
		t.Fatalf("%d ReadArtifact requests for an invalid document", n)
	}

	warned := fx.writeDoc(t, "warned.ork.yaml", strings.Replace(taskRunFixtureYAML, "    pin: p\n    run_as_orker_user: true\n", "    pin: p\n", 1))
	got = fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{}`, "")}, "--file", warned, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitOK)
	if !regexp.MustCompile(`(?m)^warning: `).MatchString(got.stdout) {
		t.Fatalf("the WARNING was not printed:\n%s", got.stdout)
	}
}

// ---------------------------------------------------------------- C13

func TestTaskRunExitCode_FollowsTheCategory(t *testing.T) {
	cases := []struct {
		outcome, category string
		want              int
	}{
		{taskRunSucceeded, "", exitOK},
		{taskRunRendered, "", exitOK},
		{taskRunCanceled, "", exitRunCanceled},
		{taskRunFailed, orker.CategoryActivityFailed, exitRunFailed},
		{taskRunFailed, orker.CategoryContractViolation, exitRunFailed},
		{taskRunFailed, orker.CategoryTimedOut, exitRunFailed},
		{taskRunFailed, orker.CategoryInfrastructureError, exitError},
	}
	for _, c := range cases {
		if got := taskRunExitCode(c.outcome, c.category); got != c.want {
			t.Errorf("taskRunExitCode(%s, %s) = %d, want %d", c.outcome, c.category, got, c.want)
		}
	}
	// Every reason code maps by its category alone.
	for _, reason := range []string{orker.ReasonImagePullFailed, orker.ReasonRuntimeRefused, orker.ReasonWorkspaceNotBindable,
		orker.ReasonInputUnavailable, orker.ReasonArtifactUploadFailed, orker.ReasonSecretUnresolved} {
		if got := taskRunExitCode(taskRunFailed, orker.CategoryFor(reason)); got != exitError {
			t.Errorf("%s exits %d, want %d", reason, got, exitError)
		}
	}
	for _, reason := range []string{orker.ReasonRenderFailed, orker.ReasonInputTooLarge, orker.ReasonImageNotPinned,
		orker.ReasonResourceRefused, orker.ReasonNetworkModeRefused, orker.ReasonContainerExitNonzero, orker.ReasonContainerTimeout} {
		if got := taskRunExitCode(taskRunFailed, orker.CategoryFor(reason)); got != exitRunFailed {
			t.Errorf("%s exits %d, want %d", reason, got, exitRunFailed)
		}
	}
}

// ---------------------------------------------------------------- C14

// readyWatcher cancels once the transcript shows the container's ready line.
type readyWatcher struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	cancel context.CancelFunc
	fired  bool
}

func (w *readyWatcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	if !w.fired && strings.Contains(w.buf.String(), "│ ready") {
		w.fired = true
		w.cancel()
	}
	return len(p), nil
}

func TestTaskRun_InterruptRunsTheCancellationSequence(t *testing.T) {
	fx := newTaskRunFixture(t)
	rt := &fakeTaskRuntime{block: true, term: make(chan struct{})}
	rt.script = func(_ orker.ContainerSpec, stdout, _ io.Writer) int32 {
		fmt.Fprintln(stdout, "ready")
		return 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := fx.runTaskRunContext(ctx, &readyWatcher{cancel: cancel}, rt, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitRunCanceled)
	if !strings.Contains(got.stdout, "\nCANCELED\n") {
		t.Fatalf("no CANCELED outcome:\n%s", got.stdout)
	}
	if !strings.Contains(got.stdout, "› interrupted: SIGTERM, then SIGKILL after the grace period") {
		t.Errorf("the cancellation sequence was not announced:\n%s", got.stdout)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.signals) == 0 || rt.signals[0] != orker.SIGTERM {
		t.Fatalf("signals = %v, want SIGTERM first", rt.signals)
	}
	if rt.removes != 1 {
		t.Fatalf("the container was removed %d times, want once", rt.removes)
	}
	requireNoSession(t, fx.workdir)
}

// ---------------------------------------------------------------- C15, C16

func TestTaskRun_PinnedTaskGetsAFreshOrSeededSharedDirectory(t *testing.T) {
	fx := newTaskRunFixture(t)
	listing := func(dir string) []string {
		var out []string
		_ = filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
			if err == nil && path != dir {
				rel, _ := filepath.Rel(dir, path)
				out = append(out, filepath.ToSlash(rel))
			}
			return nil
		})
		sort.Strings(out)
		return out
	}
	var atStart []string
	rt := &fakeTaskRuntime{script: succeedWith(`{}`, "")}
	rt.onStart = func(spec orker.ContainerSpec) { atStart = listing(mountSource(spec, orker.ContainerShared)) }
	got := fx.runTaskRun(rt, "--keep", "--from-run", taskRunSourceRun, "--task", "pinned").requireExit(t, exitOK)
	if len(atStart) != 0 {
		t.Errorf("the shared directory held %v at Start, want it empty", atStart)
	}
	got.requireContains(t, "pin          p — /orkstack/shared starts EMPTY")
	requireNoPinDirectory(t, fx.workdir)

	seed := t.TempDir()
	if err := os.MkdirAll(filepath.Join(seed, "state"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(seed, "state", "cache.txt"), []byte("warm"), 0o644); err != nil {
		t.Fatal(err)
	}
	rt = &fakeTaskRuntime{script: succeedWith(`{}`, "")}
	rt.onStart = func(spec orker.ContainerSpec) { atStart = listing(mountSource(spec, orker.ContainerShared)) }
	got = fx.runTaskRun(rt, "--keep", "--from-run", taskRunSourceRun, "--task", "pinned", "--shared", seed).requireExit(t, exitOK)
	if strings.Join(atStart, ",") != "state,state/cache.txt" {
		t.Errorf("the shared directory held %v at Start, want the seeded tree", atStart)
	}
	got.requireContains(t, "starts with the contents of "+seed+" (--shared)")
	requireNoPinDirectory(t, fx.workdir)
}

func requireNoPinDirectory(t *testing.T, workdir string) {
	t.Helper()
	for _, session := range sessions(t, workdir) {
		entries, _ := os.ReadDir(filepath.Join(session, ".work"))
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "ork-pin-") {
				t.Errorf("pin directory %s remains under %s", entry.Name(), session)
			}
		}
	}
}

func TestTaskRun_SessionDirectoryIsRemovedUnlessKept(t *testing.T) {
	fx := newTaskRunFixture(t)
	fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{}`, "")}, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitOK)
	requireNoSession(t, fx.workdir)
	fx.runTaskRun(&fakeTaskRuntime{script: exitWith(1, "")}, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitRunFailed)
	requireNoSession(t, fx.workdir)
	// A refusal after step 8: the upstream bytes are gone.
	fx.seedRun(t, "ork-1790000000000000004-4", func(run *orkstackv1.Run) {
		run.TaskRuns[0].Artifacts = []*orkstackv1.ArtifactRef{{Name: "gone.txt", Uri: fx.payload.GetUri() + "-gone",
			Sha256: strings.Repeat("1", 64), SizeBytes: 3}}
	})
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", "ork-1790000000000000004-4", "--task", "consume").requireExit(t, exitError)
	requireNoSession(t, fx.workdir)

	got := fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{"ok":true}`, "kept\n")}, "--keep", "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitOK)
	kept := sessions(t, fx.workdir)
	if len(kept) != 1 {
		t.Fatalf("--keep left %v", kept)
	}
	got.requireContains(t, "workspace kept at "+kept[0])
	for _, rel := range []string{"input/config.json", "input/task.json", "input/run.json", "input/upstream/produce/values.json",
		"input/artifacts/produce/payload.txt", "output/values.json", "output/artifacts/result.txt",
		"logs/stream.log", "logs/stdout.log", "logs/stderr.log", "report.json"} {
		if _, err := os.Stat(filepath.Join(kept[0], rel)); err != nil {
			t.Errorf("--keep layout lacks %s: %v", rel, err)
		}
	}
	if info, err := os.Stat(filepath.Join(kept[0], "input", "secrets")); err != nil || !info.IsDir() {
		t.Errorf("input/secrets is not the empty mountpoint: %v", err)
	}
	stream, _ := os.ReadFile(filepath.Join(kept[0], "logs", "stream.log"))
	if string(stream) != "done\n" {
		t.Errorf("logs/stream.log = %q, want every framed line the transcript showed", stream)
	}
}

// ---------------------------------------------------------------- C17, C18

func TestTaskRun_UnreadableUpstreamBytesRefuse(t *testing.T) {
	fx := newTaskRunFixture(t)
	fx.seedRun(t, "ork-1790000000000000005-5", func(run *orkstackv1.Run) {
		run.TaskRuns[0].Artifacts = []*orkstackv1.ArtifactRef{{Name: "gone.txt", Uri: fx.payload.GetUri() + "-gone",
			Sha256: strings.Repeat("1", 64), SizeBytes: 3}}
	})
	rt := &fakeTaskRuntime{}
	fx.runTaskRun(rt, "--from-run", "ork-1790000000000000005-5", "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, "fetch produce/gone.txt from run ork-1790000000000000005-5:",
			"the bytes may have been reclaimed; supply them with --artifact produce/gone.txt=PATH")
	if rt.pulls != 0 {
		t.Fatal("an unreadable upstream still pulled the image")
	}
}

func TestTaskRun_NoRuntimeRefusesBeforeAnyFetch(t *testing.T) {
	fx := newTaskRunFixture(t)
	before := len(fx.requests())
	fx.runTaskRun(&fakeTaskRuntime{describeErr: errors.New("daemon down")}, "--from-run", taskRunSourceRun, "--task", "consume").
		requireExit(t, exitError).
		requireStderrContains(t, "no container runtime answered (daemon down); orkctl task run executes the task on this machine")
	if sent := requestsSince(fx.fixture, before); len(sent) > 0 {
		t.Fatalf("a missing runtime still sent %v", sent)
	}
}

// ---------------------------------------------------------------- C19 pure helpers

func TestLocalSecrets(t *testing.T) {
	declared := []*orkstackv1.SecretRef{{Name: "a", Source: "env:A"}, {Name: "b", Source: "env:B"}, {Name: "c", Source: "env:C"}}
	got := localSecrets(declared, map[string]string{"c": "file:/x/c", "a": "env:LOCAL_A"}, map[string]bool{"b": true}, false)
	if len(got) != 2 || got[0].GetName() != "a" || got[0].GetSource() != "env:LOCAL_A" || got[1].GetName() != "c" || got[1].GetSource() != "file:/x/c" {
		t.Errorf("localSecrets = %v, want a and c, in declaration order, with the supplied sources", got)
	}
	if got := localSecrets(declared, map[string]string{"a": "env:X"}, nil, true); len(got) != 0 {
		t.Errorf("--render-only staged %v", got)
	}
}

func TestParseTaskArtifactFlag(t *testing.T) {
	file := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseTaskArtifactFlag("build/out=put.tar=" + file)
	if err == nil {
		t.Fatalf("a PATH containing '=' after the first split parsed as %+v", got)
	}
	got, err = parseTaskArtifactFlag("build/app.tar=" + file)
	if err != nil || got.producer != "build" || got.name != "app.tar" || got.path != file {
		t.Fatalf("parse = %+v, %v", got, err)
	}
	for _, bad := range []string{"", "x", "build=" + file, "/a=" + file, "build/=" + file, "build/a/b=" + file,
		"build/.=" + file, "build/..=" + file, "build/a\x00b=" + file, "build/\xff=" + file, "build/a=" + filepath.Dir(file)} {
		if _, err := parseTaskArtifactFlag(bad); err == nil {
			t.Errorf("parseTaskArtifactFlag(%q) succeeded", bad)
		}
	}
}

func TestReplayStore_ServesFetchedBytesByRecordedURIAndWritesLocally(t *testing.T) {
	ctx := context.Background()
	store, err := newReplayStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fetched := filepath.Join(t.TempDir(), "fetched")
	if err := os.WriteFile(fetched, []byte("recorded bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.serve("s3://bucket/objects/x", fetched)
	reader, err := store.Get(ctx, "s3://bucket/objects/x")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(raw) != "recorded bytes" {
		t.Errorf("Get(recorded) = %q", raw)
	}
	ref, err := store.Put(ctx, "out.txt", strings.NewReader("local"), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ref.URI, "file://") {
		t.Errorf("Put wrote %s, want the session's local store", ref.URI)
	}
	reader, err = store.Get(ctx, ref.URI)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(reader)
	_ = reader.Close()
	if string(raw) != "local" {
		t.Errorf("Get(local) = %q", raw)
	}
	if _, err := store.Get(ctx, "s3://bucket/objects/unfetched"); err == nil {
		t.Error("an unfetched recorded URI was served")
	}
	if _, err := store.Presign(ctx, ref.URI, 0); !errors.Is(err, artifacts.ErrUnsupported) {
		t.Errorf("Presign err = %v, want ErrUnsupported", err)
	}
}

func TestDecodeDispatchFailure(t *testing.T) {
	failure := orker.Fail(orker.ReasonContainerExitNonzero, "task %q: the container exited 1", "x").
		WithDetail(&orkstackv1.ActivityFailureDetail{ExitCode: 1})
	detail, category, ok := decodeDispatchFailure(failure.TemporalError())
	if !ok || category != orker.CategoryActivityFailed || detail.GetReasonCode() != orker.ReasonContainerExitNonzero || detail.GetExitCode() != 1 {
		t.Errorf("ApplicationError decoded to (%v, %q, %v)", detail, category, ok)
	}
	if msg := dispatchMessage(failure.TemporalError(), orker.ReasonContainerExitNonzero); msg != `task "x": the container exited 1` {
		t.Errorf("dispatchMessage = %q", msg)
	}
	raw := orker.Fail(orker.ReasonImageNotPinned, "not pinned")
	detail, category, ok = decodeDispatchFailure(raw)
	if !ok || category != orker.CategoryContractViolation || detail.GetReasonCode() != orker.ReasonImageNotPinned {
		t.Errorf("*orker.Failure decoded to (%v, %q, %v)", detail, category, ok)
	}
	if _, _, ok := decodeDispatchFailure(context.Canceled); ok {
		t.Error("a cancellation decoded as a dispatch failure")
	}
}

func TestLocalToolRoot_PrefersTheOrkerWorkRootThenHome(t *testing.T) {
	work := filepath.Join(t.TempDir(), "work")
	t.Setenv(orker.WorkRootVar, work)
	if got := localToolRoot("task-run"); got != work {
		t.Errorf("localToolRoot = %q, want ORK_ORKER_WORKDIR %q", got, work)
	}
	home := t.TempDir()
	t.Setenv(orker.WorkRootVar, "")
	t.Setenv("HOME", home)
	if got := localToolRoot("task-run"); got != filepath.Join(home, ".orkstack", "task-run") {
		t.Errorf("localToolRoot = %q, want ~/.orkstack/task-run", got)
	}
	if got := localToolRoot("activity-test"); got != filepath.Join(home, ".orkstack", "activity-test") {
		t.Errorf("localToolRoot(activity-test) = %q", got)
	}
}

// chunkServer is a RunService whose ReadArtifact streams one chunk, with or
// without the final `last`.
type chunkServer struct {
	orkstackv1connect.UnimplementedRunServiceHandler
	last bool
}

func (s chunkServer) ReadArtifact(_ context.Context, _ *connect.Request[orkstackv1.ReadArtifactRequest], stream *connect.ServerStream[orkstackv1.ArtifactChunk]) error {
	if err := stream.Send(&orkstackv1.ArtifactChunk{Data: []byte("bytes"), Artifact: &orkstackv1.ArtifactRef{Name: "a", Uri: "s3://b/a"}}); err != nil {
		return err
	}
	if s.last {
		return stream.Send(&orkstackv1.ArtifactChunk{Last: true})
	}
	return nil
}

func TestReadRecordedArtifact_RequiresIntegrityConfirmation(t *testing.T) {
	for _, last := range []bool{false, true} {
		mux := http.NewServeMux()
		mux.Handle(orkstackv1connect.NewRunServiceHandler(chunkServer{last: last}))
		server := httptest.NewServer(mux)
		client := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
		var buf bytes.Buffer
		ref, err := readRecordedArtifact(context.Background(), client, &orkstackv1.ReadArtifactRequest{RunId: "r", TaskId: "t", Name: "a",
			Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT}, &buf)
		server.Close()
		if !last {
			if err == nil || !strings.Contains(err.Error(), "ended without integrity confirmation") {
				t.Errorf("a stream without last returned (%v, %v)", ref, err)
			}
			continue
		}
		if err != nil || ref.GetUri() != "s3://b/a" || buf.String() != "bytes" {
			t.Errorf("a confirmed stream returned (%v, %v, %q)", ref, err, buf.String())
		}
	}
}

func TestRuneCut_NeverSplitsAUTF8Sequence(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		limit int
		want  int
	}{
		{"shorter than the limit", "abc", 10, 3},
		{"ascii at the limit", "abcdef", 3, 3},
		{"cut inside a two-byte rune backs off", "aé", 2, 1},
		{"cut after a whole two-byte rune", "aéb", 3, 3},
		{"cut inside a four-byte rune backs off", "x🙂y", 3, 1},
		{"zero limit", "é", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runeCut([]byte(tc.in), tc.limit)
			if got != tc.want {
				t.Fatalf("runeCut(%q, %d) = %d, want %d", tc.in, tc.limit, got, tc.want)
			}
			if !utf8.ValidString(tc.in[:got]) {
				t.Fatalf("runeCut(%q, %d) left an invalid prefix %q", tc.in, tc.limit, tc.in[:got])
			}
		})
	}
}
