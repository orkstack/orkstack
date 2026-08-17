package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// stubPlugin records what the Orker handed it and returns a scripted result.
type stubPlugin struct {
	name string
	run  func(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error)

	gotConfig  map[string]any
	gotInputs  plugin.Payload
	gotTaskID  string
	gotWorkDir string
}

func (s *stubPlugin) Name() string { return s.name }

func (s *stubPlugin) Describe() plugin.Descriptor {
	return plugin.Descriptor{Name: s.name, Version: "test"}
}

func (s *stubPlugin) Execute(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	s.gotConfig = ec.Config
	s.gotInputs = ec.Inputs
	s.gotTaskID = ec.TaskID
	s.gotWorkDir = ec.WorkDir
	if ec.Log != nil {
		ec.Log("stub plugin %s running", s.name)
	}
	if s.run != nil {
		return s.run(ctx, ec)
	}
	return &plugin.Result{Output: plugin.Payload{"ok": true}}, nil
}

// runActivity executes ExecuteTask through Temporal's activity test
// environment, so the input crosses the real data converter.
func runActivity(t *testing.T, a *Activities, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(a.ExecuteTask, activity.RegisterOptions{
		Name: task.ActivityExecuteTask,
	})
	encoded, err := env.ExecuteActivity(task.ActivityExecuteTask, in)
	if err != nil {
		return task.ExecuteTaskResult{}, err
	}
	var result task.ExecuteTaskResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode activity result: %v", err)
	}
	return result, nil
}

func registryWith(plugins ...plugin.Plugin) *plugin.Registry {
	registry := plugin.NewRegistry()
	for _, p := range plugins {
		registry.Register(p)
	}
	return registry
}

func TestExecuteTask_DispatchesByPluginNameAndUploadsProducedArtifacts(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("captured bytes\x00\xff")
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			path := filepath.Join(ec.WorkDir, "out.bin")
			if err := os.WriteFile(path, payload, 0o644); err != nil {
				return nil, err
			}
			return &plugin.Result{
				Output: plugin.Payload{"greeting": "hi"},
				Artifacts: []plugin.ProducedArtifact{{
					Name:        "body",
					LocalPath:   "out.bin",
					ContentType: "application/octet-stream",
					Metadata:    map[string]string{"origin": "stub"},
				}},
			}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub, &stubPlugin{name: "other"}), store, Options{})

	result, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID:       "run-1",
		OrkflowName: "hello-ork",
		TaskID:      "produce",
		Plugin:      "demo",
	})
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.TaskID != "produce" {
		t.Errorf("result.TaskID = %q, want produce", result.TaskID)
	}
	if result.Output["greeting"] != "hi" {
		t.Errorf("result.Output = %#v, want the plugin payload", result.Output)
	}
	if len(result.Artifacts) != 1 {
		t.Fatalf("result.Artifacts = %#v, want one ref", result.Artifacts)
	}
	ref := result.Artifacts[0]
	if ref.Name != "body" {
		t.Errorf("ref.Name = %q, want body", ref.Name)
	}
	if ref.URI == "" {
		t.Error("ref.URI is empty, want the stored artifact URI")
	}
	if ref.SizeBytes != int64(len(payload)) {
		t.Errorf("ref.SizeBytes = %d, want %d", ref.SizeBytes, len(payload))
	}

	reader, err := store.Get(context.Background(), ref.URI)
	if err != nil {
		t.Fatalf("get uploaded artifact: %v", err)
	}
	defer func() { _ = reader.Close() }()
	stored, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read uploaded artifact: %v", err)
	}
	if string(stored) != string(payload) {
		t.Errorf("stored artifact = %q, want %q", stored, payload)
	}

	stat, err := store.Stat(context.Background(), ref.URI)
	if err != nil {
		t.Fatalf("stat uploaded artifact: %v", err)
	}
	if stat.Metadata[artifacts.MetaOrkflow] != "hello-ork" ||
		stat.Metadata[artifacts.MetaTaskID] != "produce" ||
		stat.Metadata[artifacts.MetaRunID] != "run-1" ||
		stat.Metadata["origin"] != "stub" {
		t.Errorf("artifact metadata = %#v, want provenance plus plugin metadata", stat.Metadata)
	}
	// The store addresses an artifact by those coordinates, so the Orker must
	// hand them over in the form ProvenanceFromMetadata reads back.
	want := artifacts.Provenance{Orkflow: "hello-ork", TaskID: "produce", RunID: "run-1"}
	if got := artifacts.ProvenanceFromMetadata(stat.Metadata); got != want {
		t.Errorf("provenance = %+v, want %+v", got, want)
	}
}

func TestExecuteTask_UnknownPluginFailsWithClearNonRetryableError(t *testing.T) {
	activities := NewActivitiesWithOptions(registryWith(&stubPlugin{name: "http"}), newTestStore(t), Options{})

	_, err := runActivity(t, activities, task.ExecuteTaskInput{TaskID: "boom", Plugin: "nope"})
	if err == nil {
		t.Fatal("ExecuteTask succeeded for an unknown plugin, want an error")
	}
	assertNonRetryable(t, err, ErrTypeUnknownPlugin)
	for _, want := range []string{"boom", "nope", "http"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestExecuteTask_RendersConfigAndInputsBeforeExecutingThePlugin(t *testing.T) {
	store := newTestStore(t)
	ref := putArtifact(t, store, "body", []byte("upstream bytes"))
	var materialized string
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			materialized, _ = ec.Config["file"].(string)
			contents, err := os.ReadFile(materialized)
			if err != nil {
				return nil, err
			}
			return &plugin.Result{Output: plugin.Payload{"read": string(contents)}}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), store, Options{})

	result, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID:    "run-2",
		TaskID:   "transform",
		Plugin:   "demo",
		RunInput: plugin.Payload{"url": "http://example.test"},
		Upstream: map[string]task.UpstreamResult{
			"fetch": {
				Output:    plugin.Payload{"status": float64(200)},
				Artifacts: []plugin.ArtifactRef{ref},
			},
		},
		With: map[string]any{
			"url":    "{{ .inputs.url }}",
			"status": "status={{ .tasks.fetch.output.status }}",
			"file":   "{{ artifact \"fetch/body\" }}",
			"raw":    "no templates here",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if stub.gotConfig["url"] != "http://example.test" {
		t.Errorf("config url = %v, want the rendered run input", stub.gotConfig["url"])
	}
	if stub.gotConfig["status"] != "status=200" {
		t.Errorf("config status = %v, want the rendered upstream output", stub.gotConfig["status"])
	}
	if stub.gotConfig["raw"] != "no templates here" {
		t.Errorf("config raw = %v, want it preserved", stub.gotConfig["raw"])
	}
	if result.Output["read"] != "upstream bytes" {
		t.Errorf("plugin read %v from the materialized artifact, want the stored bytes", result.Output["read"])
	}
	if stub.gotTaskID != "transform" {
		t.Errorf("ExecContext.TaskID = %q, want transform", stub.gotTaskID)
	}
	if stub.gotInputs["url"] != "http://example.test" {
		t.Errorf("ExecContext.Inputs = %#v, want the run input", stub.gotInputs)
	}
	if !strings.HasPrefix(materialized, stub.gotWorkDir) {
		t.Errorf("materialized path %q is not inside the workdir %q", materialized, stub.gotWorkDir)
	}
}

func TestExecuteTask_UnknownTemplateReferenceFailsTheTask(t *testing.T) {
	stub := &stubPlugin{name: "demo"}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{})

	_, err := runActivity(t, activities, task.ExecuteTaskInput{
		TaskID: "transform",
		Plugin: "demo",
		With:   map[string]any{"url": "{{ .inputs.missing }}"},
	})
	if err == nil {
		t.Fatal("ExecuteTask succeeded with an unknown reference, want an error")
	}
	assertNonRetryable(t, err, ErrTypeRender)
	if !strings.Contains(err.Error(), "missing") || !strings.Contains(err.Error(), "with.url") {
		t.Errorf("error %q does not identify the failing reference", err)
	}
	if stub.gotConfig != nil {
		t.Error("the plugin ran despite a render failure")
	}
}

func TestExecuteTask_RemovesTheWorkDirAfterTheAttempt(t *testing.T) {
	cases := []struct {
		name string
		run  func(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error)
	}{
		{
			name: "success",
			run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
				return &plugin.Result{Output: plugin.Payload{"dir": ec.WorkDir}}, nil
			},
		},
		{
			name: "plugin failure",
			run: func(_ context.Context, _ *plugin.ExecContext) (*plugin.Result, error) {
				return nil, errors.New("boom")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workRoot := t.TempDir()
			stub := &stubPlugin{name: "demo", run: tc.run}
			activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
				WorkRoot: workRoot,
			})

			_, _ = runActivity(t, activities, task.ExecuteTaskInput{
				RunID:  "run-3",
				TaskID: "scratch",
				Plugin: "demo",
			})

			if stub.gotWorkDir == "" {
				t.Fatal("the plugin never saw a workdir")
			}
			if !strings.HasPrefix(stub.gotWorkDir, workRoot) {
				t.Errorf("workdir %q is not under the configured root %q", stub.gotWorkDir, workRoot)
			}
			if _, err := os.Stat(stub.gotWorkDir); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("workdir %q still exists after the attempt (stat error: %v)", stub.gotWorkDir, err)
			}
			entries, err := os.ReadDir(workRoot)
			if err != nil {
				t.Fatalf("read work root: %v", err)
			}
			if len(entries) != 0 {
				t.Errorf("work root still holds %d entries after the attempt", len(entries))
			}
		})
	}
}

func TestExecuteTask_PerAttemptWorkDirsAreDistinct(t *testing.T) {
	workRoot := t.TempDir()
	seen := map[string]struct{}{}
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			seen[ec.WorkDir] = struct{}{}
			return &plugin.Result{}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{WorkRoot: workRoot})

	for range 3 {
		if _, err := runActivity(t, activities, task.ExecuteTaskInput{
			RunID: "run-4", TaskID: "scratch", Plugin: "demo",
		}); err != nil {
			t.Fatalf("ExecuteTask: %v", err)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("saw %d distinct workdirs across 3 attempts, want 3", len(seen))
	}
}

func TestExecuteTask_ClassifiesPluginErrors(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{name: "plain error stays retryable", err: errors.New("transient"), wantPermanent: false},
		{name: "explicit retryable stays retryable", err: plugin.Retryable(errors.New("later")), wantPermanent: false},
		{name: "permanent is non-retryable", err: plugin.Permanent(errors.New("bad config")), wantPermanent: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubPlugin{
				name: "demo",
				run: func(_ context.Context, _ *plugin.ExecContext) (*plugin.Result, error) {
					return nil, tc.err
				},
			}
			activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{})

			_, err := runActivity(t, activities, task.ExecuteTaskInput{TaskID: "t", Plugin: "demo"})
			if err == nil {
				t.Fatal("ExecuteTask succeeded, want the plugin error")
			}
			var appErr *temporal.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("error %v is not an ApplicationError", err)
			}
			if appErr.NonRetryable() != tc.wantPermanent {
				t.Fatalf("NonRetryable() = %v, want %v (error: %v)", appErr.NonRetryable(), tc.wantPermanent, err)
			}
			if !strings.Contains(err.Error(), "demo") || !strings.Contains(err.Error(), "\"t\"") {
				t.Errorf("error %q does not name the task and plugin", err)
			}
		})
	}
}

func TestExecuteTask_MissingProducedFileFailsNonRetryably(t *testing.T) {
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, _ *plugin.ExecContext) (*plugin.Result, error) {
			return &plugin.Result{Artifacts: []plugin.ProducedArtifact{{Name: "ghost", LocalPath: "nope.txt"}}}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{})

	_, err := runActivity(t, activities, task.ExecuteTaskInput{TaskID: "t", Plugin: "demo"})
	if err == nil {
		t.Fatal("ExecuteTask succeeded with a missing produced file, want an error")
	}
	assertNonRetryable(t, err, ErrTypeArtifact)
}

func TestExecuteTask_StreamsPluginLogs(t *testing.T) {
	var lines []string
	stub := &stubPlugin{name: "demo"}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
		LogSink: func(_ task.ExecuteTaskInput, line string) { lines = append(lines, line) },
	})

	if _, err := runActivity(t, activities, task.ExecuteTaskInput{TaskID: "t", Plugin: "demo"}); err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "stub plugin demo running") {
		t.Fatalf("plugin log lines = %q, want the plugin's own line", joined)
	}
	if !strings.Contains(joined, "workdir ") {
		t.Fatalf("orker log lines = %q, want the workdir line", joined)
	}
}

func TestBuiltinRegistry_RegistersTheFourBuiltinPlugins(t *testing.T) {
	registry := BuiltinRegistry()
	for _, name := range []string{"http", "shell", "file", "container"} {
		if _, ok := registry.Get(name); !ok {
			t.Errorf("builtin registry is missing plugin %q", name)
		}
	}
	if len(registry.List()) != 4 {
		t.Errorf("builtin registry holds %d plugins, want 4", len(registry.List()))
	}
}

// TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector holds every
// builtin to the shared `op` contract documented in docs/PLUGIN_DESIGN.md:
// the selector appears in the discovery schema with its enumerated values, and
// an unrecognized op fails the task permanently instead of being ignored.
func TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector(t *testing.T) {
	// http checks `url` before `op`, so the config carries one; the address is
	// unroutable to prove no request is attempted.
	extraConfig := map[string]map[string]any{
		"http": {"url": "http://127.0.0.1:1/never"},
	}

	for _, name := range []string{"http", "shell", "file", "container"} {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinRegistry().Get(name)
			if !ok {
				t.Fatalf("builtin registry is missing plugin %q", name)
			}

			properties, _ := p.Describe().InputSchema["properties"].(map[string]any)
			opSchema, _ := properties["op"].(map[string]any)
			if opSchema == nil {
				t.Fatalf("%s does not document an `op` field in its input schema", name)
			}
			enum, _ := opSchema["enum"].([]any)
			if len(enum) == 0 {
				t.Errorf("%s.op has no enumerated ops in its input schema", name)
			}
			// Logged so the milestone's documentation review can diff the
			// advertised ops against the prose (scripts/m7-3-doc-review.py).
			ops := make([]string, 0, len(enum))
			for _, value := range enum {
				ops = append(ops, fmt.Sprintf("%v", value))
			}
			t.Logf("advertised ops: %s=%s", name, strings.Join(ops, ","))

			config := map[string]any{"op": "frobnicate"}
			for key, value := range extraConfig[name] {
				config[key] = value
			}
			_, err := p.Execute(context.Background(), &plugin.ExecContext{
				TaskID:  "t",
				Config:  config,
				WorkDir: t.TempDir(),
				Log:     func(string, ...any) {},
			})
			if err == nil {
				t.Fatalf("%s accepted op=frobnicate, want a permanent error", name)
			}
			if !strings.Contains(err.Error(), `unknown op "frobnicate"`) {
				t.Errorf("%s error %q does not report the unknown op", name, err)
			}
			var retryable *plugin.RetryableError
			if !errors.As(err, &retryable) || retryable.Retryable {
				t.Errorf("%s error %v is not marked permanent", name, err)
			}
		})
	}
}

// assertNonRetryable checks the error is a non-retryable application error of
// the expected type.
func assertNonRetryable(t *testing.T, err error, wantType string) {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error %v is not an ApplicationError", err)
	}
	if !appErr.NonRetryable() {
		t.Fatalf("error %v is retryable, want non-retryable", err)
	}
	if appErr.Type() != wantType {
		t.Fatalf("error type = %q, want %q", appErr.Type(), wantType)
	}
}
