package engine_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests cover the API's use of the orkflow store: apply writes through to
// it, and a store that cannot be reached is reported rather than papered over.
// They live in internal/engine because M7.5's scope covers this package and
// internal/api/service.go.

// recordingStore captures what the API writes.
type recordingStore struct {
	inner *engine.MemoryOrkflowStore
	puts  []*orkstackv1.Orkflow
}

func newRecordingStore() *recordingStore {
	return &recordingStore{inner: engine.NewMemoryOrkflowStore()}
}

func (r *recordingStore) Put(ctx context.Context, wf *orkstackv1.Orkflow) error {
	r.puts = append(r.puts, wf)
	return r.inner.Put(ctx, wf)
}

func (r *recordingStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	return r.inner.Get(ctx, namespace, name, version)
}

func (r *recordingStore) List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	return r.inner.List(ctx, namespace)
}

// unreachableStore is a durable store that is down.
type unreachableStore struct{ err error }

func (u unreachableStore) Put(context.Context, *orkstackv1.Orkflow) error { return u.err }

func (u unreachableStore) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, u.err
}

func (u unreachableStore) List(context.Context, string) ([]*orkstackv1.Orkflow, error) {
	return nil, u.err
}

func exampleYAML(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	return string(source)
}

func TestApplyOrkflow_WritesTheCompiledOrkflowThroughToTheStore(t *testing.T) {
	store := newRecordingStore()
	svc := api.NewService(api.Config{Orkflows: store})
	ctx := context.Background()

	applied, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: exampleYAML(t),
	}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	if len(store.puts) != 1 {
		t.Fatalf("store received %d writes, want 1", len(store.puts))
	}
	stored := store.puts[0]
	if stored.GetName() != "hello-ork" || stored.GetVersion() != "0.1.0" {
		t.Errorf("stored %s@%s, want hello-ork@0.1.0", stored.GetName(), stored.GetVersion())
	}
	if len(stored.GetTasks()) != 3 {
		t.Errorf("stored orkflow has %d tasks, want 3", len(stored.GetTasks()))
	}
	if stored.GetSourceYaml() == "" {
		t.Error("stored orkflow lost its source document")
	}
	if stored.GetCreatedAt() == nil {
		t.Error("stored orkflow has no created_at, so 'latest' cannot be resolved")
	}
	if applied.Msg.GetOrkflow().GetName() != stored.GetName() {
		t.Error("the response and the stored definition disagree")
	}
}

// TestOrkflowPaths_ReportAnUnreachableStoreInsteadOfAnEmptyCatalog is the
// "no in-memory fallback" claim: with the durable store down, every orkflow
// path fails loudly. An empty list or a stale answer would be a lie.
func TestOrkflowPaths_ReportAnUnreachableStoreInsteadOfAnEmptyCatalog(t *testing.T) {
	down := unreachableStore{err: errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")}
	svc := api.NewService(api.Config{Orkflows: down, Starter: noopStarter{}})
	ctx := context.Background()

	listed, err := svc.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err == nil {
		t.Fatalf("ListOrkflows = %v, want an error", listed.Msg.GetOrkflows())
	}
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("ListOrkflows code = %v, want Unavailable", connect.CodeOf(err))
	}

	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: exampleYAML(t),
	})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("ApplyOrkflow code = %v, want Unavailable", connect.CodeOf(err))
	}

	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
		Name: "hello-ork",
	})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("GetOrkflow code = %v, want Unavailable", connect.CodeOf(err))
	}

	if _, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "hello-ork",
	})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("SubmitRun code = %v, want Unavailable", connect.CodeOf(err))
	}
}

// A store that is reachable but has no such orkflow is a different answer.
func TestOrkflowPaths_ReportAMissingOrkflowAsNotFound(t *testing.T) {
	missing := unreachableStore{err: artifacts.ErrNotFound}
	svc := api.NewService(api.Config{Orkflows: missing, Starter: noopStarter{}})
	ctx := context.Background()

	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
		Name: "ghost",
	})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetOrkflow code = %v, want NotFound", connect.CodeOf(err))
	}
	if _, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "ghost",
	})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("SubmitRun code = %v, want NotFound", connect.CodeOf(err))
	}
}

// TestApplyOrkflow_RejectsNonDefaultNamespaceUntilM103 closes the black hole
// M10.1 would otherwise open.
//
// The DSL, the store, the archive index and the registry are namespace-aware
// in this slice; the API's read paths are not — GetOrkflow, ListOrkflows and
// SubmitRun all ask the store for the default namespace until M10.3. So an
// accepted `namespace: payments` would write an object under a key nothing can
// read and no run can execute: a success that silently does nothing, which is
// worse than a refusal. Documenting a field a user cannot use is the same
// defect stated in prose.
//
// The refusal is Unimplemented, not InvalidArgument: the document is correct
// and will be accepted verbatim one slice later. Deleting the guard is what
// M10.3 does, and this test is what makes that a deliberate act rather than an
// accident — it fails the moment the guard goes, and its replacement is a test
// that applies and then READS BACK a non-default namespace.
func TestApplyOrkflow_RejectsNonDefaultNamespaceUntilM103(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    with: {command: "echo settle"}
`
	store := newRecordingStore()
	svc := api.NewService(api.Config{Orkflows: store, Starter: noopStarter{}})
	ctx := context.Background()

	applied, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: document,
	}))
	if err == nil {
		t.Fatalf("ApplyOrkflow(namespace: payments) stored %s@%s and returned no error; "+
			"nothing can read or run it until M10.3",
			applied.Msg.GetOrkflow().GetName(), applied.Msg.GetOrkflow().GetVersion())
	}
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("ApplyOrkflow(namespace: payments) code = %v, want Unimplemented", connect.CodeOf(err))
	}
	// The message has to tell the author which slice lifts the restriction;
	// "not implemented" alone leaves them guessing whether it is their document.
	for _, want := range []string{"payments", "M10.3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ApplyOrkflow(namespace: payments) error = %q, want it to name %q", err, want)
		}
	}
	if len(store.puts) != 0 {
		t.Errorf("the store received %d write(s); a refused apply must not leave an unreadable object",
			len(store.puts))
	}

	// The default namespace, named explicitly or left out, still applies.
	for _, accepted := range []string{
		"namespace: " + artifacts.DefaultNamespace + "\nname: nightly-close\ntasks:\n  - id: settle\n    plugin: shell\n",
		"name: nightly-close\ntasks:\n  - id: settle\n    plugin: shell\n",
	} {
		if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: accepted,
		})); err != nil {
			t.Errorf("ApplyOrkflow(%q) = %v, want the default namespace to still apply", accepted, err)
		}
	}
	if len(store.puts) != 2 {
		t.Errorf("the store received %d write(s) for the default namespace, want 2", len(store.puts))
	}
}

// noopStarter satisfies the API's Temporal seam; these tests never reach it.
type noopStarter struct{}

func (noopStarter) StartRun(context.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
	return nil
}

func (noopStarter) CancelRun(context.Context, string, string) error { return nil }
