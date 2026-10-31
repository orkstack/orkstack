package orker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// TestDispatcher_ASecretRefusalSaysWhichRuleRefusedIt: a source is refused by
// one of three rules, and the refusal says which, whatever the floor holds. A
// source outside the floor names the floor; one of the Orker's own says no
// floor admits it; a file named by anything but a plain absolute path says
// that rule holds under every floor. Neither of the last two speaks of the
// floor as what refused it, so neither misleads under an empty one.
func TestDispatcher_ASecretRefusalSaysWhichRuleRefusedIt(t *testing.T) {
	const (
		own = `secret "s": "%s" is this orker's own (%s), which no secret floor admits; it is refused ` +
			`before anything runs, whatever this orker's ORK_ORKER_SECRET_ALLOW says`
		path = `secret "s": "%s" has a ".." segment, which is refused rather than normalized; a file source ` +
			`is read by a plain absolute name of at most 4096 bytes, under every floor, and is refused before anything runs`
	)
	for _, tc := range []struct {
		floor  []string
		source string
		want   string
	}{
		{[]string{"env:SANDBOX_*"}, "env:PROD_DB", `secret "s": "env:PROD_DB" matches none of env:SANDBOX_* ` +
			`(this orker's ORK_ORKER_SECRET_ALLOW); a source outside that floor is refused before anything runs`},
		{nil, "env:ORK_S3_SECRET_KEY", fmt.Sprintf(own, "env:ORK_S3_SECRET_KEY", "one of its variables")},
		{[]string{"env:*"}, "env:ORK_S3_SECRET_KEY", fmt.Sprintf(own, "env:ORK_S3_SECRET_KEY", "one of its variables")},
		{nil, "file:/proc/self/environ", fmt.Sprintf(own, "file:/proc/self/environ", "one of its files")},
		{nil, "file:/etc/orkstack/../db", fmt.Sprintf(path, "file:/etc/orkstack/../db")},
		{[]string{"file:/etc/*/*"}, "file:/etc/orkstack/../db", fmt.Sprintf(path, "file:/etc/orkstack/../db")},
	} {
		d := newAttemptDispatcher(t, &capturingRuntime{}, artifacts.NewMemoryAttemptStore())
		d.Policy.SecretAllow = tc.floor
		in := baseExportInput("grab")
		in.Secrets = []*orkstackv1.SecretRef{{Name: "s", Source: tc.source}}
		failure := d.Validate(in)
		if failure == nil || failure.Reason != ReasonSecretNotPermitted || failure.Message != tc.want {
			t.Errorf("floor %q, source %q: Validate = %v, want %s:\n%s", tc.floor, tc.source, failure, ReasonSecretNotPermitted, tc.want)
		}
		if _, beside := d.resolveSecret(in.Secrets[0]); beside == nil || beside.Message != tc.want {
			t.Errorf("floor %q, source %q: resolveSecret = %v, want the same refusal beside the read", tc.floor, tc.source, beside)
		}
	}
}

// TestDispatcher_ASecretRefusalQuotesALongValueByItsHead: a secret's name, its
// scheme and the variable it names are what a task wrote, and none has a bound
// the dispatcher can rely on, so every refusal that names one quotes it by its
// head and its length rather than in full.
func TestDispatcher_ASecretRefusalQuotesALongValueByItsHead(t *testing.T) {
	long := strings.Repeat("A", 200<<10)
	length := fmt.Sprintf("(%d bytes)", len(long))
	d := newAttemptDispatcher(t, &capturingRuntime{}, artifacts.NewMemoryAttemptStore())
	for _, tc := range []struct {
		what   string
		secret *orkstackv1.SecretRef
		reason string
		// read is whether the read beside the value is asked too, and admit
		// whether Validate lets the secret through to it.
		read, admit bool
	}{
		{"a name", &orkstackv1.SecretRef{Name: long, Source: "env:SANDBOX_TOKEN"}, ReasonSecretSchemeForbid, false, false},
		{"a scheme", &orkstackv1.SecretRef{Name: "s", Source: long + ":x"}, ReasonSecretSchemeForbid, true, false},
		{"a variable", &orkstackv1.SecretRef{Name: "s", Source: "env:" + long}, ReasonSecretUnresolved, true, true},
	} {
		in := baseExportInput("grab")
		in.Secrets = []*orkstackv1.SecretRef{tc.secret}
		failures := map[string]*Failure{}
		if failure := d.Validate(in); tc.admit {
			if failure != nil {
				t.Fatalf("%s: Validate = %v, want it admitted", tc.what, failure)
			}
		} else {
			failures["Validate"] = failure
		}
		if tc.read {
			_, failures["resolveSecret"] = d.resolveSecret(tc.secret)
		}
		for step, failure := range failures {
			if failure == nil || failure.Reason != tc.reason {
				t.Fatalf("%s: %s = %v, want %s", tc.what, step, failure, tc.reason)
			}
			if len(failure.Message) > 512 || !strings.Contains(failure.Message, length) {
				t.Errorf("%s: %s's message is %d bytes, want at most 512 that end the value with %s:\n%.300s",
					tc.what, step, len(failure.Message), length, failure.Message)
			}
		}
	}
}

// TestDispatcher_TheDefaultFloorRefusesTheOrkersOwnSourcesBeforeAnythingRuns:
// with no floor set, a task that names the Orker's own environment or process
// files is refused SECRET_NOT_PERMITTED exactly as a source outside a floor
// is — before a pull, a workspace or a container, with one non-retryable
// attempt record, and with the value nowhere.
func TestDispatcher_TheDefaultFloorRefusesTheOrkersOwnSourcesBeforeAnythingRuns(t *testing.T) {
	value := mintSecret(t)
	t.Setenv("ORK_S3_SECRET_KEY", value)
	for _, tc := range []struct{ source, want string }{
		{"env:ORK_S3_SECRET_KEY", `secret "s3": "env:ORK_S3_SECRET_KEY" is this orker's own (one of its variables), which no secret floor admits`},
		{"file:/proc/self/environ", `secret "s3": "file:/proc/self/environ" is this orker's own (one of its files), which no secret floor admits`},
	} {
		t.Run(tc.source, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			runtime := &capturingRuntime{}
			d := newAttemptDispatcher(t, runtime, attempts)
			in := baseExportInput("grab")
			in.Secrets = []*orkstackv1.SecretRef{{Name: "s3", Source: tc.source}}

			failure := d.Validate(in)
			if failure == nil || failure.Reason != ReasonSecretNotPermitted {
				t.Fatalf("Validate = %v, want %s", failure, ReasonSecretNotPermitted)
			}
			if !strings.HasPrefix(failure.Message, tc.want) {
				t.Fatalf("Validate message =\n%s\nwant it to begin\n%s", failure.Message, tc.want)
			}
			if _, failure := d.resolveSecret(in.Secrets[0]); failure == nil || failure.Reason != ReasonSecretNotPermitted {
				t.Fatalf("resolveSecret = %v, want %s beside the read", failure, ReasonSecretNotPermitted)
			}

			err := runOnce(t, d, in)
			if err == nil {
				t.Fatal("a task naming the orker's own source ran")
			}
			if runtime.pulls != 0 || runtime.creates != 0 {
				t.Fatalf("runtime asked: %d pulls, %d creates; want none", runtime.pulls, runtime.creates)
			}
			records, lerr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if lerr != nil {
				t.Fatalf("ListAttempts: %v", lerr)
			}
			if len(records) != 1 || records[0].GetError().GetCode() != ReasonSecretNotPermitted ||
				records[0].GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
				records[0].GetError().GetRetryable() {
				t.Fatalf("attempt records = %v, want one %s / CONTRACT_VIOLATION / not retryable", records, ReasonSecretNotPermitted)
			}
			recordJSON, _ := protojson.Marshal(records[0])
			for _, h := range []string{err.Error(), failure.Message, string(recordJSON)} {
				if strings.Contains(h, value) {
					t.Fatalf("the secret value leaked into %q", h)
				}
			}
		})
	}
}

// TestNewActivities_TheWorkRootIsTheOrkersOwn: every attempt's workspace —
// another task's staged secrets among it — lives under the work root, so the
// constructor every Orker and `orkctl task run` is built with adds it to the
// exclusion, whether the policy came from the environment or the caller.
func TestNewActivities_TheWorkRootIsTheOrkersOwn(t *testing.T) {
	t.Setenv(WorkRootVar, "")
	root := t.TempDir()
	staged := "file:" + filepath.Join(root, "ork-run-task-1-x", "input", "secrets", "db")
	explicit := DefaultSecurityPolicy()
	for _, opts := range []Options{
		{WorkRoot: root},
		{WorkRoot: root, Policy: &explicit},
	} {
		activities := NewActivitiesWithOptions(nil, opts)
		assertOwn(t, activities.Dispatcher().Policy, staged)
	}
	if len(explicit.own) != 0 {
		t.Fatalf("the caller's policy was changed in place: %v", explicit.own)
	}
	// No work root is the temp dir, where every attempt then lives.
	activities := NewActivitiesWithOptions(nil, Options{})
	assertOwn(t, activities.Dispatcher().Policy, "file:"+filepath.Join(os.TempDir(), "ork-run-task-1-x", "input", "secrets", "db"))
}

// TestBoot_WarnsThatAnEmptySecretFloorAdmitsEverySourceButTheOrkersOwn: an
// Orker with no floor says so at boot, naming the setting, and one with a
// floor does not.
func TestBoot_WarnsThatAnEmptySecretFloorAdmitsEverySourceButTheOrkersOwn(t *testing.T) {
	for _, tc := range []struct {
		allow []string
		warn  bool
	}{{nil, true}, {[]string{"env:SANDBOX_*"}, false}} {
		fake := newCountingDocker(t)
		fake.set("present", "")
		var logs lockedBuffer
		policy := DefaultSecurityPolicy()
		policy.SecretAllow = tc.allow
		activities := NewActivitiesWithOptions(nil, Options{
			OrkerID:  fmt.Sprintf("orker-floor-warn-%d-%t", os.Getpid(), tc.warn),
			WorkRoot: t.TempDir(),
			Runtime:  &DockerRuntime{Bin: fake.bin},
			Policy:   &policy,
			Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
		})
		t.Cleanup(activities.Close)
		if err := activities.Boot(context.Background()); err != nil {
			t.Fatalf("Boot = %v", err)
		}
		warned := strings.Contains(logs.String(), "level=WARN msg=\"secret floor is empty") &&
			strings.Contains(logs.String(), SecretAllowVar)
		if warned != tc.warn {
			t.Errorf("floor %q: warned = %t, want %t; logs:\n%s", tc.allow, warned, tc.warn, logs.String())
		}
	}
}
