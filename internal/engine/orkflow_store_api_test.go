package engine_test

import (
	"context"
	"errors"
	"os"
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

func (r *recordingStore) Put(
	ctx context.Context,
	wf *orkstackv1.Orkflow,
	expected artifacts.HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	r.puts = append(r.puts, wf)
	return r.inner.Put(ctx, wf, expected)
}

func (r *recordingStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	return r.inner.Get(ctx, namespace, name, version)
}

func (r *recordingStore) List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	return r.inner.List(ctx, namespace)
}

func (r *recordingStore) Versions(
	ctx context.Context,
	namespace, name string,
) (artifacts.OrkflowVersions, error) {
	return r.inner.Versions(ctx, namespace, name)
}

func (r *recordingStore) SetCurrentVersion(
	ctx context.Context,
	namespace, name, version string,
) (string, error) {
	return r.inner.SetCurrentVersion(ctx, namespace, name, version)
}

// unreachableStore is a durable store that is down.
type unreachableStore struct{ err error }

func (u unreachableStore) Put(
	context.Context,
	*orkstackv1.Orkflow,
	artifacts.HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	return nil, false, u.err
}

func (u unreachableStore) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, u.err
}

func (u unreachableStore) List(context.Context, string) ([]*orkstackv1.Orkflow, error) {
	return nil, u.err
}

func (u unreachableStore) Versions(
	context.Context,
	string, string,
) (artifacts.OrkflowVersions, error) {
	return artifacts.OrkflowVersions{}, u.err
}

func (u unreachableStore) SetCurrentVersion(context.Context, string, string, string) (string, error) {
	return "", u.err
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

// TestApplyOrkflow_StoresTheNamespaceTheDocumentNames replaces M10.1's interim
// guard.
//
// That guard refused every namespace but the default, because the API's read
// paths could not honour one and a stored `namespace: payments` would have
// been an object nothing could read. M10.3 made the reads namespace-aware and
// deleted the guard; what has to hold now is that a document naming a
// namespace is written INTO it, which is what this asserts at the store seam.
//
// The end-to-end half — apply, list, get and submit in a non-default namespace,
// invisible from the default — is
// TestTwoNamespaces_SameOrkflowName_ListsAndRunsStayDisjoint and
// TestApplyOrkflow_AppliesIntoANonDefaultNamespaceAndReadsItBack, both in
// internal/api, and against real Temporal and MinIO in
// TestIntegration_TwoNamespacesStayDisjointThroughTheCLI.
func TestApplyOrkflow_StoresTheNamespaceTheDocumentNames(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    uses: shell
    with: {command: "echo settle"}
`
	store := newRecordingStore()
	svc := api.NewService(api.Config{
		Orkflows:   store,
		Namespaces: everyNamespace{},
		Starter:    noopStarter{},
	})
	ctx := context.Background()

	applied, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: document,
	}))
	if err != nil {
		t.Fatalf("ApplyOrkflow(namespace: payments): %v", err)
	}
	if got := applied.Msg.GetOrkflow().GetNamespace(); got != "payments" {
		t.Errorf("applied namespace = %q, want payments", got)
	}
	if len(store.puts) != 1 {
		t.Fatalf("the store received %d write(s), want 1", len(store.puts))
	}
	if got := store.puts[0].GetNamespace(); got != "payments" {
		t.Errorf("stored namespace = %q, want payments: the document would be "+
			"written where its author cannot find it", got)
	}

	// The default namespace, named explicitly or left out, still applies.
	for _, accepted := range []string{
		"namespace: " + artifacts.DefaultNamespace + "\nname: nightly-close\ntasks:\n  - id: settle\n    uses: shell\n",
		"name: nightly-close\ntasks:\n  - id: settle\n    uses: shell\n",
	} {
		if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: accepted,
		})); err != nil {
			t.Errorf("ApplyOrkflow(%q) = %v, want the default namespace to still apply", accepted, err)
		}
	}
	if len(store.puts) != 3 {
		t.Errorf("the store received %d write(s), want 3", len(store.puts))
	}
	for _, stored := range store.puts[1:] {
		if got := stored.GetNamespace(); got != artifacts.DefaultNamespace {
			t.Errorf("stored namespace = %q, want %q", got, artifacts.DefaultNamespace)
		}
	}
}

// everyNamespace is a registry in which every name exists with no allow-list.
// These tests are about the orkflow store seam, not about the registry, and a
// nil registry would refuse a non-default name before the store was reached.
type everyNamespace struct{}

func (everyNamespace) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	return &orkstackv1.Namespace{Name: artifacts.ResolveNamespace(name)}, nil
}

func (everyNamespace) List(context.Context) ([]*orkstackv1.Namespace, error) {
	return []*orkstackv1.Namespace{artifacts.DefaultNamespaceDocument()}, nil
}

func (everyNamespace) Put(
	context.Context,
	*orkstackv1.Namespace,
	artifacts.PutConditions,
) (*orkstackv1.Namespace, error) {
	return nil, errors.New("everyNamespace: this double answers reads only")
}

func (everyNamespace) Delete(context.Context, string) error {
	return errors.New("everyNamespace: this double answers reads only")
}

// noopStarter satisfies the API's Temporal seam; these tests never reach it.
type noopStarter struct{}

func (noopStarter) StartRun(context.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
	return nil
}

func (noopStarter) CancelRun(context.Context, string, string) error { return nil }
