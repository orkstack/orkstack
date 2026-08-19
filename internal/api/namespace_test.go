package api_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeRegistry is the namespace registry in memory, with the durable store's
// conditional-write semantics: one etag per document, moved on every write,
// and IfAbsent/IfMatch honoured exactly as a conditional PUT would.
//
// The conditions are implemented rather than accepted, because the API layer's
// whole create-and-update contract is expressed in them: a double that stored
// whatever it was handed would let an Update that never sent an etag pass a
// test named for compare-and-swap. What is NOT proved here is that S3 and
// MinIO honour the same conditions — that is a fact about the backend and is
// measured against a real one in
// TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters.
type fakeRegistry struct {
	mu   sync.Mutex
	docs map[string]*orkstackv1.Namespace
	seq  int
}

func newFakeRegistry(names ...*orkstackv1.Namespace) *fakeRegistry {
	r := &fakeRegistry{docs: map[string]*orkstackv1.Namespace{}}
	for _, ns := range names {
		if _, err := r.Put(context.Background(), ns, artifacts.PutConditions{}); err != nil {
			panic(err)
		}
	}
	return r
}

func (r *fakeRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name = artifacts.ResolveNamespace(name)
	if ns, ok := r.docs[name]; ok {
		return proto.Clone(ns).(*orkstackv1.Namespace), nil
	}
	if name == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
}

func (r *fakeRegistry) List(_ context.Context) ([]*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byName := map[string]*orkstackv1.Namespace{
		artifacts.DefaultNamespace: artifacts.DefaultNamespaceDocument(),
	}
	for name, ns := range r.docs {
		byName[name] = proto.Clone(ns).(*orkstackv1.Namespace)
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out, nil
}

func (r *fakeRegistry) Put(
	_ context.Context,
	ns *orkstackv1.Namespace,
	cond artifacts.PutConditions,
) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := artifacts.ResolveNamespace(ns.GetName())
	existing, exists := r.docs[name]
	if cond.IfAbsent && exists {
		return nil, fmt.Errorf("put %q: %w", name, artifacts.ErrPreconditionFailed)
	}
	if cond.IfMatch != "" && (!exists || existing.GetEtag() != cond.IfMatch) {
		return nil, fmt.Errorf("put %q: %w", name, artifacts.ErrPreconditionFailed)
	}
	r.seq++
	stored := proto.Clone(ns).(*orkstackv1.Namespace)
	stored.Name = name
	stored.Etag = fmt.Sprintf("etag-%d", r.seq)
	r.docs[name] = stored
	return proto.Clone(stored).(*orkstackv1.Namespace), nil
}

// Delete mirrors the durable store: the default namespace is refused, and a
// name nobody registered is NotFound. It does NOT count orkflows — the
// registry is a different prefix from the catalog and cannot see one — which
// is exactly why the API performs that check itself, and why a test of the
// refusal here measures the API's check rather than the double's.
func (r *fakeRegistry) Delete(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name = artifacts.ResolveNamespace(name)
	if name == artifacts.DefaultNamespace {
		return fmt.Errorf("namespace %q holds every orkflow that names none: %w",
			name, artifacts.ErrNamespaceNotEmpty)
	}
	if _, ok := r.docs[name]; !ok {
		return fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
	}
	delete(r.docs, name)
	return nil
}

// namespacedService wires a Service over an in-memory catalog and registry.
func namespacedService(t *testing.T, reg *fakeRegistry) (*api.Service, *engine.RunStore) {
	t.Helper()
	runs := engine.NewRunStore()
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: reg,
		Runs:       runs,
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	return svc, runs
}

func mustApply(t *testing.T, svc *api.Service, document, namespace string) *orkstackv1.Orkflow {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: document, Namespace: namespace}))
	if err != nil {
		t.Fatalf("ApplyOrkflow(namespace %q): %v", namespace, err)
	}
	return resp.Msg.GetOrkflow()
}

// TestTwoNamespaces_SameOrkflowName_ListsAndRunsStayDisjoint is the acceptance
// this slice exists for.
//
// An orkflow name is unique WITHIN a namespace, not across the deployment. So
// two tenants may each own a "deploy", and every read has to answer for the
// one that asked: a list that leaked the other's definition would show a
// document its reader cannot run, and a run list that leaked the other's runs
// would report work this tenant never submitted.
//
// The two definitions differ in their task sets on purpose. Identical
// documents would pass this test even if both namespaces resolved to one
// stored object, which is precisely the bug it is here to catch.
func TestTwoNamespaces_SameOrkflowName_ListsAndRunsStayDisjoint(t *testing.T) {
	const deployInA = `
namespace: alpha
name: deploy
version: 1.0.0
tasks:
  - id: build
    plugin: shell
    with: {command: "echo build"}
`
	const deployInB = `
namespace: bravo
name: deploy
version: 1.0.0
tasks:
  - id: ship
    plugin: shell
    with: {command: "echo ship"}
  - id: verify
    plugin: shell
    needs: [ship]
    with: {command: "echo verify"}
`
	reg := newFakeRegistry(
		&orkstackv1.Namespace{Name: "alpha"},
		&orkstackv1.Namespace{Name: "bravo"},
	)
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	mustApply(t, svc, deployInA, "")
	mustApply(t, svc, deployInB, "")

	// Lists are disjoint, and each names its own.
	for _, tc := range []struct {
		namespace string
		tasks     int
	}{
		{"alpha", 1},
		{"bravo", 2},
	} {
		listed, err := svc.ListOrkflows(ctx,
			connect.NewRequest(&orkstackv1.ListOrkflowsRequest{Namespace: tc.namespace}))
		if err != nil {
			t.Fatalf("ListOrkflows(%q): %v", tc.namespace, err)
		}
		orkflows := listed.Msg.GetOrkflows()
		if len(orkflows) != 1 {
			t.Fatalf("ListOrkflows(%q) returned %d orkflows, want exactly its own one",
				tc.namespace, len(orkflows))
		}
		if got := orkflows[0].GetNamespace(); got != tc.namespace {
			t.Errorf("ListOrkflows(%q) returned an orkflow of namespace %q", tc.namespace, got)
		}
		if got := len(orkflows[0].GetTasks()); got != tc.tasks {
			t.Errorf("ListOrkflows(%q) returned %d tasks, want %d: the other namespace's "+
				"document is being served", tc.namespace, got, tc.tasks)
		}
	}

	// The default namespace holds neither.
	listed, err := svc.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err != nil {
		t.Fatalf("ListOrkflows(default): %v", err)
	}
	if got := len(listed.Msg.GetOrkflows()); got != 0 {
		t.Errorf("ListOrkflows(default) returned %d orkflows, want 0", got)
	}

	// Two gets of one name answer with two different definitions.
	getA, err := svc.GetOrkflow(ctx,
		connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "deploy", Namespace: "alpha"}))
	if err != nil {
		t.Fatalf("GetOrkflow(alpha, deploy): %v", err)
	}
	getB, err := svc.GetOrkflow(ctx,
		connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "deploy", Namespace: "bravo"}))
	if err != nil {
		t.Fatalf("GetOrkflow(bravo, deploy): %v", err)
	}
	if proto.Equal(getA.Msg.GetOrkflow(), getB.Msg.GetOrkflow()) {
		t.Fatal("GetOrkflow answered with the same document in both namespaces")
	}
	if taskA, taskB := getA.Msg.GetOrkflow().GetTasks()[0].GetId(),
		getB.Msg.GetOrkflow().GetTasks()[0].GetId(); taskA != "build" || taskB != "ship" {
		t.Errorf("GetOrkflow tasks = %q / %q, want build / ship", taskA, taskB)
	}

	// One run in each namespace.
	runIn := map[string]string{}
	for _, namespace := range []string{"alpha", "bravo"} {
		submitted, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "deploy",
			Namespace:   namespace,
		}))
		if err != nil {
			t.Fatalf("SubmitRun(%q, deploy): %v", namespace, err)
		}
		run := submitted.Msg.GetRun()
		if run.GetNamespace() != namespace {
			t.Errorf("submitted run namespace = %q, want %q", run.GetNamespace(), namespace)
		}
		runIn[namespace] = run.GetId()
	}
	if runIn["alpha"] == runIn["bravo"] {
		t.Fatal("both submissions produced one run")
	}

	// Run lists are disjoint too.
	for _, namespace := range []string{"alpha", "bravo"} {
		listedRuns, err := svc.ListRuns(ctx,
			connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: namespace}))
		if err != nil {
			t.Fatalf("ListRuns(%q): %v", namespace, err)
		}
		runs := listedRuns.Msg.GetRuns()
		if len(runs) != 1 || runs[0].GetId() != runIn[namespace] {
			t.Fatalf("ListRuns(%q) = %v, want exactly [%s]",
				namespace, runIDs(runs), runIn[namespace])
		}
	}
	listedDefault, err := svc.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns(default): %v", err)
	}
	if got := len(listedDefault.Msg.GetRuns()); got != 0 {
		t.Errorf("ListRuns(default) = %v, want none: neither run is in it",
			runIDs(listedDefault.Msg.GetRuns()))
	}
}

func runIDs(runs []*orkstackv1.Run) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.GetId())
	}
	return ids
}

// TestApplyOrkflow_AppliesIntoANonDefaultNamespaceAndReadsItBack is the
// replacement for M10.1's interim guard.
//
// That guard refused every namespace but the default because the read paths
// could not honour one, and its test said its replacement would be "a test
// that applies and then READS BACK a non-default namespace". This is it: the
// same document the guard refused, now applied, listed, read and submitted.
func TestApplyOrkflow_AppliesIntoANonDefaultNamespaceAndReadsItBack(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    with: {command: "echo settle"}
`
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	applied := mustApply(t, svc, document, "")
	if applied.GetNamespace() != "payments" {
		t.Fatalf("applied namespace = %q, want payments", applied.GetNamespace())
	}

	got, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
		Name: "nightly-close", Namespace: "payments",
	}))
	if err != nil {
		t.Fatalf("GetOrkflow(payments, nightly-close): %v", err)
	}
	if got.Msg.GetOrkflow().GetVersion() != applied.GetVersion() {
		t.Errorf("read back version %q, want %q",
			got.Msg.GetOrkflow().GetVersion(), applied.GetVersion())
	}

	submitted, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "nightly-close", Namespace: "payments",
	}))
	if err != nil {
		t.Fatalf("SubmitRun(payments, nightly-close): %v", err)
	}
	if ns := submitted.Msg.GetRun().GetNamespace(); ns != "payments" {
		t.Errorf("run namespace = %q, want payments", ns)
	}

	// And the default namespace still cannot see it.
	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
		Name: "nightly-close",
	})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetOrkflow(default, nightly-close) code = %v, want NotFound",
			connect.CodeOf(err))
	}
}

// TestApplyOrkflow_RefusesASelectorThatDisagreesWithTheDocument keeps the two
// ways of naming a namespace from silently diverging.
//
// `orkctl -n staging apply payments.ork.yaml` is a mistake somebody made:
// either the flag or the file is wrong, and guessing which would store the
// definition where its author will not look for it.
func TestApplyOrkflow_RefusesASelectorThatDisagreesWithTheDocument(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    with: {command: "echo settle"}
`
	reg := newFakeRegistry(
		&orkstackv1.Namespace{Name: "payments"},
		&orkstackv1.Namespace{Name: "staging"},
	)
	svc, _ := namespacedService(t, reg)

	_, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: document, Namespace: "staging"}))
	if err == nil {
		t.Fatal("applying a payments document into staging succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
	for _, want := range []string{"payments", "staging"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; both are needed to fix it", err, want)
		}
	}
}

// TestApplyOrkflow_SelectorAdoptsADocumentThatNamesNoNamespace is the other
// half: a portable document plus a selector is how one file is applied into
// several namespaces.
func TestApplyOrkflow_SelectorAdoptsADocumentThatNamesNoNamespace(t *testing.T) {
	const document = `
name: deploy
tasks:
  - id: build
    plugin: shell
    with: {command: "echo build"}
`
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "staging"})
	svc, _ := namespacedService(t, reg)

	applied := mustApply(t, svc, document, "staging")
	if applied.GetNamespace() != "staging" {
		t.Fatalf("applied namespace = %q, want staging", applied.GetNamespace())
	}
}

// TestSubmitRun_IntoAMissingNamespaceNamesItAndListsTheOnesThatExist is
// acceptance 4.
//
// "namespace not found" is true and useless. A caller who mistyped needs the
// name they typed and the names they could have typed, in the answer they
// already have — otherwise the next step is a second command they have to know
// exists.
func TestSubmitRun_IntoAMissingNamespaceNamesItAndListsTheOnesThatExist(t *testing.T) {
	reg := newFakeRegistry(
		&orkstackv1.Namespace{Name: "payments"},
		&orkstackv1.Namespace{Name: "staging"},
	)
	svc, _ := namespacedService(t, reg)

	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy",
		Namespace:   "paymnets",
	}))
	if err == nil {
		t.Fatal("submitting into a namespace nobody registered succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", code)
	}
	for _, want := range []string{"paymnets", "default", "payments", "staging"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// TestCreateNamespace_IsIdempotentForAnIdenticalSpec lets a provisioning
// script run twice.
func TestCreateNamespace_IsIdempotentForAnIdenticalSpec(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	spec := &orkstackv1.Namespace{
		Name:         "payments",
		Description:  "the payments tenant",
		Labels:       map[string]string{"team": "money"},
		DefaultPool:  "batch",
		AllowedPools: []string{"batch", "main"},
	}

	first, err := svc.CreateNamespace(ctx,
		connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: spec}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if !first.Msg.GetCreated() {
		t.Error("the first create reports created=false")
	}

	// Re-sent with the allow-list in another order: the same namespace, said
	// differently. An order-sensitive comparison would refuse a script that
	// only reformatted its input.
	repeat := proto.Clone(spec).(*orkstackv1.Namespace)
	repeat.AllowedPools = []string{"main", "batch"}
	second, err := svc.CreateNamespace(ctx,
		connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: repeat}))
	if err != nil {
		t.Fatalf("CreateNamespace repeated: %v", err)
	}
	if second.Msg.GetCreated() {
		t.Error("the repeated create reports created=true; nothing was written")
	}
	if got := second.Msg.GetNamespace().GetEtag(); got != first.Msg.GetNamespace().GetEtag() {
		t.Errorf("etag moved from %q to %q on an idempotent repeat",
			first.Msg.GetNamespace().GetEtag(), got)
	}
}

// TestCreateNamespace_RefusesADifferentSpecUnderAnExistingName separates a
// repeat from a collision.
func TestCreateNamespace_RefusesADifferentSpecUnderAnExistingName(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	if _, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments", DefaultPool: "batch"},
	})); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	_, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments", DefaultPool: "gpu"},
	}))
	if err == nil {
		t.Fatal("re-creating payments with another default pool succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeAlreadyExists {
		t.Errorf("code = %v, want AlreadyExists", code)
	}
	if !strings.Contains(err.Error(), "default_pool") {
		t.Errorf("error %q does not name the field that differs", err)
	}
}

// TestUpdateNamespace_RefusesAStaleEtag is the compare-and-swap.
func TestUpdateNamespace_RefusesAStaleEtag(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	created, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments", Description: "first"},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	stale := created.Msg.GetNamespace().GetEtag()
	if stale == "" {
		t.Fatal("create returned no etag, so nothing can compare-and-swap on it")
	}

	winner := proto.Clone(created.Msg.GetNamespace()).(*orkstackv1.Namespace)
	winner.Description = "the winner"
	updated, err := svc.UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: winner}))
	if err != nil {
		t.Fatalf("UpdateNamespace: %v", err)
	}
	if updated.Msg.GetNamespace().GetEtag() == stale {
		t.Error("the etag did not move on a successful update")
	}

	loser := proto.Clone(created.Msg.GetNamespace()).(*orkstackv1.Namespace)
	loser.Description = "the loser"
	_, err = svc.UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: loser}))
	if err == nil {
		t.Fatal("an update carrying a stale etag overwrote the winner")
	}
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", code)
	}

	current, err := svc.GetNamespace(ctx,
		connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "payments"}))
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if got := current.Msg.GetNamespace().GetDescription(); got != "the winner" {
		t.Errorf("description = %q, want %q: the loser's write landed", got, "the winner")
	}
}

// TestDeleteNamespace_RefusesWhileItHoldsOrkflows keeps definitions reachable.
func TestDeleteNamespace_RefusesWhileItHoldsOrkflows(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    with: {command: "echo settle"}
`
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()
	mustApply(t, svc, document, "")

	_, err := svc.DeleteNamespace(ctx,
		connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "payments"}))
	if err == nil {
		t.Fatal("deleting a namespace that still holds an orkflow succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", code)
	}
	for _, want := range []string{"payments", "nightly-close"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; the refusal has to say what is in the way",
				err, want)
		}
	}

	// The namespace is still there and still readable.
	if _, err := svc.GetNamespace(ctx,
		connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "payments"})); err != nil {
		t.Fatalf("GetNamespace after a refused delete: %v", err)
	}
}

// TestDeleteNamespace_RefusesTheDefault: every orkflow that names none is in
// it, so it is not a tenant that can be removed.
func TestDeleteNamespace_RefusesTheDefault(t *testing.T) {
	svc, _ := namespacedService(t, newFakeRegistry())

	_, err := svc.DeleteNamespace(context.Background(),
		connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: ""}))
	if err == nil {
		t.Fatal("deleting the default namespace succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", code)
	}
}

// TestDeleteNamespace_RemovesAnEmptyOneAndItStopsBeingListed.
func TestDeleteNamespace_RemovesAnEmptyOneAndItStopsBeingListed(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "scratch"})
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	if _, err := svc.DeleteNamespace(ctx,
		connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "scratch"})); err != nil {
		t.Fatalf("DeleteNamespace(scratch): %v", err)
	}
	listed, err := svc.ListNamespaces(ctx, connect.NewRequest(&orkstackv1.ListNamespacesRequest{}))
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	for _, ns := range listed.Msg.GetNamespaces() {
		if ns.GetName() == "scratch" {
			t.Fatal("the deleted namespace is still listed")
		}
	}
	if _, err := svc.GetNamespace(ctx,
		connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "scratch"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetNamespace(scratch) code = %v, want NotFound", connect.CodeOf(err))
	}
}

// TestListNamespaces_AlwaysIncludesTheDefault: a deployment that registered
// nothing still has one namespace, and a client listing them must see it.
func TestListNamespaces_AlwaysIncludesTheDefault(t *testing.T) {
	svc, _ := namespacedService(t, newFakeRegistry())

	listed, err := svc.ListNamespaces(context.Background(),
		connect.NewRequest(&orkstackv1.ListNamespacesRequest{}))
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(listed.Msg.GetNamespaces()) != 1 ||
		listed.Msg.GetNamespaces()[0].GetName() != artifacts.DefaultNamespace {
		t.Fatalf("ListNamespaces on an empty registry = %v, want [default]",
			namespaceNames(listed.Msg.GetNamespaces()))
	}
}

// TestNamespaceWrites_WithoutARegistryAreRefusedNotFaked: a process with no
// registry cannot register anything, and pretending it did would lose the
// document at the first read.
func TestNamespaceWrites_WithoutARegistryAreRefusedNotFaked(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}})

	_, err := svc.CreateNamespace(context.Background(),
		connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
			Namespace: &orkstackv1.Namespace{Name: "payments"},
		}))
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("CreateNamespace with no registry: code = %v, want FailedPrecondition", code)
	}
}

// TestCreateNamespace_RefusesANameTheCompilerWouldNotAccept keeps the registry
// and the DSL in agreement: a namespace nothing can name in YAML is a document
// nobody can put an orkflow into.
func TestCreateNamespace_RefusesANameTheCompilerWouldNotAccept(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)

	for _, name := range []string{"", "Payments", "pay ments", "1payments", "pay/ments"} {
		_, err := svc.CreateNamespace(context.Background(),
			connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: name},
			}))
		if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
			t.Errorf("CreateNamespace(%q) code = %v, want InvalidArgument", name, code)
		}
	}
}

func namespaceNames(namespaces []*orkstackv1.Namespace) []string {
	names := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		names = append(names, ns.GetName())
	}
	return names
}
