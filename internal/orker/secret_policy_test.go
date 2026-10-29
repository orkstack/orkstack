package orker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// capturingRuntime records every question the dispatcher asks the runtime
// and, at Create, what the workspace holds under /orkstack/input and its
// secrets mount — the only moment the staged secret files exist.
type capturingRuntime struct {
	mu       sync.Mutex
	pulls    int
	creates  int
	secrets  map[string][]byte
	modes    map[string]os.FileMode
	taskJSON []byte
}

func (r *capturingRuntime) Pull(context.Context, string) (PullInfo, error) {
	r.mu.Lock()
	r.pulls++
	r.mu.Unlock()
	return PullInfo{Platform: "linux/amd64", PlatformDigest: "sha256:" + repeat64('b'), Delivery: DeliveryRegistry}, nil
}

func (r *capturingRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (r *capturingRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.creates++
	r.secrets, r.modes = map[string][]byte{}, map[string]os.FileMode{}
	for _, m := range spec.Mounts {
		switch m.Destination {
		case ContainerSecrets:
			entries, _ := os.ReadDir(m.Source)
			for _, e := range entries {
				path := filepath.Join(m.Source, e.Name())
				raw, err := os.ReadFile(path)
				if err != nil {
					return "", err
				}
				info, err := os.Stat(path)
				if err != nil {
					return "", err
				}
				r.secrets[e.Name()] = raw
				r.modes[e.Name()] = info.Mode().Perm()
			}
		case ContainerInput:
			r.taskJSON, _ = os.ReadFile(filepath.Join(m.Source, "task.json"))
		case ContainerOutput:
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "fake-container", nil
}

func (r *capturingRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return nil
}
func (r *capturingRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }
func (r *capturingRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 0}, nil
}
func (r *capturingRuntime) Remove(context.Context, ContainerID) error     { return nil }
func (r *capturingRuntime) Reap(context.Context, string) (int, error)     { return 0, nil }
func (r *capturingRuntime) Describe(context.Context) (RuntimeInfo, error) { return RuntimeInfo{}, nil }

func mintSecret(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("mint secret: %v", err)
	}
	return hex.EncodeToString(raw)
}

// runOnce executes the dispatcher once inside Temporal's test environment,
// with the retry policy attemptProbeWorkflow uses, and returns the workflow
// error.
func runOnce(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) error {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
	env.RegisterWorkflow(attemptProbeWorkflow)
	env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
	env.ExecuteWorkflow(attemptProbeWorkflow, in)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	return env.GetWorkflowError()
}

func TestDispatcher_RefusesASourceOutsideTheFloorBeforeAnythingRuns(t *testing.T) {
	value := mintSecret(t)
	t.Setenv("PROD_DB_PASSWORD", value)
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := &capturingRuntime{}
	d := newAttemptDispatcher(t, runtime, attempts)
	d.Policy.SecretAllow = []string{"env:SANDBOX_*"}
	in := baseExportInput("grab")
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:PROD_DB_PASSWORD"}}

	failure := d.Validate(in)
	if failure == nil || failure.Reason != ReasonSecretNotPermitted {
		t.Fatalf("Validate = %v, want %s", failure, ReasonSecretNotPermitted)
	}
	want := `secret "db": "env:PROD_DB_PASSWORD" matches none of env:SANDBOX_* (this orker's ORK_ORKER_SECRET_ALLOW); a source outside that floor is refused before anything runs`
	if failure.Message != want {
		t.Fatalf("Validate message =\n%s\nwant\n%s", failure.Message, want)
	}

	err := runOnce(t, d, in)
	if err == nil {
		t.Fatal("a source outside the floor ran")
	}
	if runtime.pulls != 0 || runtime.creates != 0 {
		t.Fatalf("runtime asked: %d pulls, %d creates; want none", runtime.pulls, runtime.creates)
	}
	entries, _ := os.ReadDir(d.WorkRoot)
	for _, e := range entries {
		if e.IsDir() {
			t.Fatalf("a workspace directory %q exists after a refusal", e.Name())
		}
	}
	records, lerr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if lerr != nil {
		t.Fatalf("ListAttempts: %v", lerr)
	}
	if len(records) != 1 {
		t.Fatalf("attempt records = %d, want exactly one (non-retryable)", len(records))
	}
	rec := records[0]
	if rec.GetError().GetCode() != ReasonSecretNotPermitted ||
		rec.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		rec.GetError().GetRetryable() {
		t.Fatalf("attempt error = %v, want %s / CONTRACT_VIOLATION / not retryable", rec.GetError(), ReasonSecretNotPermitted)
	}

	// The value appears nowhere the fakes captured.
	recordJSON, _ := protojson.Marshal(rec)
	haystack := []string{err.Error(), failure.Message, string(recordJSON)}
	logs := d.Logs.(*recordingLogStore)
	logs.mu.Lock()
	for _, chunk := range logs.chunks {
		haystack = append(haystack, string(chunk.Data))
	}
	logs.mu.Unlock()
	for _, h := range haystack {
		if strings.Contains(h, value) {
			t.Fatalf("the secret value leaked into %q", h)
		}
	}
}

func TestDispatcher_ResolvesASourceInsideTheFloor(t *testing.T) {
	value := mintSecret(t)
	t.Setenv("SANDBOX_TOKEN", value)
	runtime := &capturingRuntime{}
	d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	d.Policy.SecretAllow = []string{"env:SANDBOX_*"}
	in := baseExportInput("fetch")
	in.Secrets = []*orkstackv1.SecretRef{{Name: "token", Source: "env:SANDBOX_TOKEN"}}

	if err := runOnce(t, d, in); err != nil {
		t.Fatalf("a source inside the floor failed: %v", err)
	}
	if got := string(runtime.secrets["token"]); got != value {
		t.Fatalf("staged secret = %q, want the minted value", got)
	}
	// Staged 0600, then sealed with every other input to ModeInputFile
	// (read-only, readable by the image's own USER) before the container
	// exists — which is what the runtime is handed.
	if mode := runtime.modes["token"]; mode != ModeInputFile {
		t.Fatalf("staged secret mode = %o, want the sealed input mode %o", mode, ModeInputFile)
	}
	task := string(runtime.taskJSON)
	if !strings.Contains(task, `"token"`) || !strings.Contains(task, ContainerSecrets+"/token") {
		t.Fatalf("task.json = %s, want the secret's name and container path", task)
	}
	if strings.Contains(task, value) || strings.Contains(task, "SANDBOX_TOKEN") {
		t.Fatalf("task.json carries the value or the source: %s", task)
	}
}

func TestDispatcher_AnEmptyFloorChangesNothing(t *testing.T) {
	value := mintSecret(t)
	t.Setenv("PROD_DB_PASSWORD", value)
	runtime := &capturingRuntime{}
	d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	in := baseExportInput("grab")
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:PROD_DB_PASSWORD"}}

	if err := runOnce(t, d, in); err != nil {
		t.Fatalf("an empty floor refused: %v", err)
	}
	if got := string(runtime.secrets["db"]); got != value {
		t.Fatalf("staged secret = %q, want the value", got)
	}

	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "vault:db"}}
	if failure := d.Validate(in); failure == nil || failure.Reason != ReasonSecretSchemeForbid {
		t.Fatalf("a forbidden scheme under an empty floor = %v, want %s", failure, ReasonSecretSchemeForbid)
	}
	d.Policy.SecretAllow = []string{"env:SANDBOX_*"}
	if failure := d.Validate(in); failure == nil || failure.Reason != ReasonSecretSchemeForbid {
		t.Fatalf("a forbidden scheme under a floor = %v, want %s still", failure, ReasonSecretSchemeForbid)
	}
}

// TestResolveSecret_RepeatsTheFloorBesideTheRead: the check that protects
// the value lives beside the read, whatever Validate did or did not do.
func TestResolveSecret_RepeatsTheFloorBesideTheRead(t *testing.T) {
	t.Setenv("PROD_DB_PASSWORD", mintSecret(t))
	d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
	d.Policy.SecretAllow = []string{"env:SANDBOX_*"}
	value, failure := d.resolveSecret(&orkstackv1.SecretRef{Name: "db", Source: "env:PROD_DB_PASSWORD"})
	if value != nil || failure == nil || failure.Reason != ReasonSecretNotPermitted {
		t.Fatalf("resolveSecret = %q, %v; want no value and %s", value, failure, ReasonSecretNotPermitted)
	}
}
