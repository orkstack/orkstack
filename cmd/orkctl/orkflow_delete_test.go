package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	deleteTestName       = "hello-ork"
	deleteTestNamespace  = "payments"
	deleteNamespaceFlag  = "--namespace"
	deleteForceFlag      = "--force"
	noDeleteCallsMessage = "Delete calls = %v, want none"
)

type recordingOrkflowStore struct {
	inner *engine.MemoryOrkflowStore
	mu    sync.Mutex
	calls [][2]string
}

func newRecordingOrkflowStore() *recordingOrkflowStore {
	return &recordingOrkflowStore{inner: engine.NewMemoryOrkflowStore()}
}

func (s *recordingOrkflowStore) Put(ctx context.Context, wf *orkstackv1.Orkflow, expected artifacts.HeadExpectation) (*orkstackv1.Orkflow, bool, error) {
	return s.inner.Put(ctx, wf, expected)
}

func (s *recordingOrkflowStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	return s.inner.Get(ctx, namespace, name, version)
}

func (s *recordingOrkflowStore) List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	return s.inner.List(ctx, namespace)
}

func (s *recordingOrkflowStore) Versions(ctx context.Context, namespace, name string) (artifacts.OrkflowVersions, error) {
	return s.inner.Versions(ctx, namespace, name)
}

func (s *recordingOrkflowStore) SetCurrentVersion(ctx context.Context, namespace, name, version string) (string, error) {
	return s.inner.SetCurrentVersion(ctx, namespace, name, version)
}

func (s *recordingOrkflowStore) Delete(ctx context.Context, namespace, name string) error {
	s.mu.Lock()
	s.calls = append(s.calls, [2]string{namespace, name})
	s.mu.Unlock()
	return s.inner.Delete(ctx, namespace, name)
}

func (s *recordingOrkflowStore) deleteCalls() [][2]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]string(nil), s.calls...)
}

func runCLIWithInput(t *testing.T, baseURL string, in io.Reader, args ...string) result {
	t.Helper()
	root, flags := newRootCommand()
	defer flags.closeIdleConnections()
	root.SetArgs(append(append([]string(nil), args...), "--server", baseURL))
	root.SetIn(in)
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	code := exitOK
	if err := root.ExecuteContext(context.Background()); err != nil {
		stderr.WriteString("Error: " + err.Error() + "\n")
		code = exitCodeFor(err)
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func deleteFixture(t *testing.T) (*fixture, *recordingOrkflowStore) {
	t.Helper()
	store := newRecordingOrkflowStore()
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Orkflows = store })
	f.run("namespace", "create", deleteTestNamespace).requireExit(t, exitOK)
	f.run("apply", examplePath(t), deleteNamespaceFlag, deleteTestNamespace).requireExit(t, exitOK)
	return f, store
}

func TestOrkflowDelete_ConfirmedWarningNamesTheTargetAndDeletesExactlyOnce(t *testing.T) {
	f, store := deleteFixture(t)

	got := runCLIWithInput(t, f.baseURL, strings.NewReader("yes\n"),
		"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace).requireExit(t, exitOK)

	requireGolden(t, "orkflow-delete-warning.golden", got.stderr)
	requireGolden(t, "orkflow-delete-success.golden", got.stdout)
	calls := store.deleteCalls()
	if len(calls) != 1 || calls[0] != [2]string{deleteTestNamespace, deleteTestName} {
		t.Fatalf("Delete calls = %v, want exactly [(payments hello-ork)]", calls)
	}
}

func TestOrkflowDelete_DecliningEmptyAndClosedInputNeverDelete(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "no", input: "no\n"},
		{name: "bare Enter", input: "\n"},
		{name: "closed stdin", input: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, store := deleteFixture(t)
			got := runCLIWithInput(t, f.baseURL, strings.NewReader(tc.input),
				"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace).requireExit(t, exitOK)
			if calls := store.deleteCalls(); len(calls) != 0 {
				t.Fatalf(noDeleteCallsMessage, calls)
			}
			if !strings.Contains(got.stderr, "Cancelled.") {
				t.Fatalf("stderr does not report cancellation:\n%s", got.stderr)
			}
		})
	}
}

func TestOrkflowDelete_JSONCancellationWritesNoDocumentAndDoesNotDelete(t *testing.T) {
	f, store := deleteFixture(t)
	got := runCLIWithInput(t, f.baseURL, strings.NewReader("no\n"),
		"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace, "--json").requireExit(t, exitOK)

	if got.stdout != "" {
		t.Fatalf("stdout = %q, want no JSON document for a cancelled operation", got.stdout)
	}
	if !strings.Contains(got.stderr, "Cancelled.") {
		t.Fatalf("stderr does not report cancellation:\n%s", got.stderr)
	}
	if calls := store.deleteCalls(); len(calls) != 0 {
		t.Fatalf(noDeleteCallsMessage, calls)
	}
}

type failOnRead struct{ reads int }

func (r *failOnRead) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("stdin must not be read")
}

func TestOrkflowDelete_ForceSkipsOnlyTheLocalPromptAndDoesNotReadStdin(t *testing.T) {
	f, store := deleteFixture(t)
	in := &failOnRead{}

	got := runCLIWithInput(t, f.baseURL, in,
		"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace, deleteForceFlag).requireExit(t, exitOK)

	if in.reads != 0 {
		t.Fatalf("stdin reads = %d, want 0", in.reads)
	}
	if strings.Contains(got.stderr, "cannot be reversed") || strings.Contains(got.stderr, "confirm") {
		t.Fatalf("forced deletion printed an interactive prompt:\n%s", got.stderr)
	}
	if calls := store.deleteCalls(); len(calls) != 1 {
		t.Fatalf("Delete calls = %v, want exactly one", calls)
	}
}

func TestOrkflowDelete_ServerRunRefusalPreservesTheReasonAndExitsNonZero(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", deleteTestName).requireExit(t, exitOK)

	got := runCLIWithInput(t, f.baseURL, strings.NewReader(""),
		"orkflow", "delete", deleteTestName, deleteForceFlag)
	if got.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", got.code, exitError, got.stdout, got.stderr)
	}
	const reason = "cannot delete orkflow \"hello-ork\" in namespace \"default\": 1 run(s) block deletion (1 active, 0 finished within 24h0m0s)"
	if !strings.Contains(got.stderr, reason) {
		t.Fatalf("stderr does not preserve server reason %q:\n%s", reason, got.stderr)
	}
}

func TestOrkflowDelete_JSONEmitsTheDeleteResponseMessage(t *testing.T) {
	f, _ := deleteFixture(t)
	got := runCLIWithInput(t, f.baseURL, strings.NewReader(""),
		"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace, deleteForceFlag, "--json").requireExit(t, exitOK)

	var doc map[string]any
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, got.stdout)
	}
	if doc["name"] != deleteTestName || doc["namespace"] != deleteTestNamespace || doc["deletedAt"] != fixedNow.Format("2006-01-02T15:04:05Z") {
		t.Fatalf("JSON output = %#v", doc)
	}
}

func TestOrkflowDelete_WrongArgumentCountIsAUsageErrorWithoutDeleting(t *testing.T) {
	for _, args := range [][]string{
		{"orkflow", "delete", deleteForceFlag},
		{"orkflow", "delete", "one", "two", deleteForceFlag},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			f, store := deleteFixture(t)
			got := runCLIWithInput(t, f.baseURL, strings.NewReader(""), args...)
			if got.code != exitError {
				t.Fatalf("exit code = %d, want %d", got.code, exitError)
			}
			if !strings.Contains(got.stderr, "accepts 1 arg(s)") {
				t.Fatalf("stderr does not explain argument count:\n%s", got.stderr)
			}
			if calls := store.deleteCalls(); len(calls) != 0 {
				t.Fatalf(noDeleteCallsMessage, calls)
			}
		})
	}
}

func TestOrkflowDelete_ForceHelpSaysItCannotBypassServerRunSafety(t *testing.T) {
	f := newFixture(t)
	got := runCLIWithInput(t, f.baseURL, strings.NewReader(""),
		"orkflow", "delete", "--help").requireExit(t, exitOK)
	const help = "skip only the local confirmation prompt; server run-safety checks still apply"
	if !strings.Contains(got.stdout, help) {
		t.Fatalf("help does not contain %q:\n%s", help, got.stdout)
	}
}
