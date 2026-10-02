package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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
	// A hold with no reason is the store's refusal (validNamespaceHold); the
	// double mirrors only this one case, so the audit trail's record of a
	// refused namespace write can be observed through the Connect surface.
	// The full rule is proved against the store itself.
	if hold := ns.GetHold(); hold != nil && strings.TrimSpace(hold.GetReason()) == "" {
		return nil, fmt.Errorf("put %q: hold needs a reason: %w", name, artifacts.ErrInvalidNamespace)
	}
	// allowed_secrets is held to the store's own rule, by the store's own
	// function, so this double refuses exactly what the store refuses.
	if err := artifacts.ValidNamespaceSecrets(ns); err != nil {
		return nil, err
	}
	if err := artifacts.ValidNamespaceSigner(ns); err != nil {
		return nil, err
	}
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
	// The hold's since is the registry's, by the one rule the durable store
	// applies, so this double cannot drift from it.
	artifacts.StampNamespaceHold(stored, existing, time.Now())
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

// TestTwoNamespaces_SameOrkflowName_ListsAndRunsStayDisjoint is the namespace
// contract's central acceptance.
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
    uses: shell
    with: {command: "echo build"}
`
	const deployInB = `
namespace: bravo
name: deploy
version: 1.0.0
tasks:
  - id: ship
    uses: shell
    with: {command: "echo ship"}
  - id: verify
    uses: shell
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

// TestSubmitRun_OneIdempotencyKeyInTwoNamespacesStartsTwoRuns covers the last
// coordinate on the submit path that was still global.
//
// An idempotency key is the caller's own word for "this submission", and two
// tenants pick the same words: nightly, deploy-1, retry. The claim authority
// binds one key to one run for the whole deployment — engine.ClaimWorkflowID
// hashes it into a single Temporal workflow id — so a key that is not scoped
// by namespace answers the second tenant with the FIRST tenant's run: a run id
// from a namespace they cannot read, reported as success, while the work they
// asked for is never started.
//
// The repeat at the end is the other half of the property. Scoping the key
// must not turn dedupe off: one key, twice, in one namespace is still one run.
func TestSubmitRun_OneIdempotencyKeyInTwoNamespacesStartsTwoRuns(t *testing.T) {
	// One portable document, applied into both namespaces — the case a
	// shared provisioning script produces, and the one where a shared key is
	// most likely.
	const document = `
name: deploy
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`
	claimer := newSharedClaimer()
	starter := &fakeStarter{}
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Namespaces: newFakeRegistry(
			&orkstackv1.Namespace{Name: "alpha"},
			&orkstackv1.Namespace{Name: "bravo"},
		),
		Runs:        engine.NewRunStore(),
		Bus:         engine.NewEventBus(),
		Starter:     starter,
		Idempotency: claimer,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	ctx := context.Background()
	mustApply(t, svc, document, "alpha")
	mustApply(t, svc, document, "bravo")

	submit := func(namespace string) *orkstackv1.Run {
		t.Helper()
		resp, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName:    "deploy",
			Namespace:      namespace,
			IdempotencyKey: "nightly",
		}))
		if err != nil {
			t.Fatalf("SubmitRun(%q, key nightly): %v", namespace, err)
		}
		return resp.Msg.GetRun()
	}

	alpha := submit("alpha")
	bravo := submit("bravo")

	if alpha.GetId() == bravo.GetId() {
		t.Fatalf("one key in two namespaces produced one run (%s): the second tenant was "+
			"answered with the first tenant's run and its own was never started", alpha.GetId())
	}
	if ns := bravo.GetNamespace(); ns != "bravo" {
		t.Errorf("the bravo submission was answered with a run in namespace %q", ns)
	}
	if started := starter.startedRuns(); len(started) != 2 {
		t.Errorf("started %v, want one run per namespace", started)
	}

	repeat := submit("alpha")
	if repeat.GetId() != alpha.GetId() {
		t.Errorf("repeating the key in alpha answered with %q, want its own run %q: "+
			"scoping a key must not stop it deduplicating", repeat.GetId(), alpha.GetId())
	}
	if started := starter.startedRuns(); len(started) != 2 {
		t.Errorf("started %v after the repeat, want the same two runs", started)
	}
}

// TestApplyOrkflow_AppliesIntoANonDefaultNamespaceAndReadsItBack applies into
// a non-default namespace and then lists, reads and submits the same document
// there, which is the contract every read path must honour.
func TestApplyOrkflow_AppliesIntoANonDefaultNamespaceAndReadsItBack(t *testing.T) {
	const document = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    uses: shell
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
    uses: shell
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
    uses: shell
    with: {command: "echo build"}
`
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "staging"})
	svc, _ := namespacedService(t, reg)

	applied := mustApply(t, svc, document, "staging")
	if applied.GetNamespace() != "staging" {
		t.Fatalf("applied namespace = %q, want staging", applied.GetNamespace())
	}
}

// TestSubmitRun_IntoAMissingNamespaceNamesItAndListsTheOnesThatExist pins the
// shape of the refusal.
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

	// An omitted default_pool and an explicit "main" are the same namespace
	// said differently, and the system says so itself: "" MEANS main
	// (artifacts.DefaultPool), `orkctl namespace list` prints main for a
	// stored "", and `--default-pool` advertises main as its own default. So
	// a user who creates a namespace, reads what the CLI tells them and
	// re-runs the create with it is re-sending an identical spec. Comparing
	// the raw strings refused that with `default_pool is "main", not "main"`
	// — a collision whose two sides print identically.
	for _, tc := range []struct {
		name    string
		created string
		repeat  string
	}{
		{name: "omitted-then-main", created: "", repeat: artifacts.DefaultPool},
		{name: "main-then-omitted", created: artifacts.DefaultPool, repeat: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := namespacedService(t, newFakeRegistry())
			created, err := svc.CreateNamespace(ctx,
				connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
					Namespace: &orkstackv1.Namespace{Name: "billing", DefaultPool: tc.created},
				}))
			if err != nil {
				t.Fatalf("CreateNamespace(default_pool %q): %v", tc.created, err)
			}
			again, err := svc.CreateNamespace(ctx,
				connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
					Namespace: &orkstackv1.Namespace{Name: "billing", DefaultPool: tc.repeat},
				}))
			if err != nil {
				t.Fatalf("re-creating with default_pool %q after %q: %v",
					tc.repeat, tc.created, err)
			}
			if again.Msg.GetCreated() {
				t.Error("the repeated create reports created=true; nothing was written")
			}
			if got := again.Msg.GetNamespace().GetEtag(); got != created.Msg.GetNamespace().GetEtag() {
				t.Errorf("etag moved from %q to %q on an idempotent repeat",
					created.Msg.GetNamespace().GetEtag(), got)
			}
		})
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
    uses: shell
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

// TestRunID_IsAnAddressInsideANamespaceNotACapabilityOverTheDeployment is the
// tenancy boundary for every request that carries a run id.
//
// A run id is unguessable, and unguessable was doing all the work: GetRun,
// CancelRun and StreamEvents took an id and nothing else, so any caller
// holding one read and CANCELLED a run of a namespace they could not list.
// An id is not a secret — it is printed by submit, quoted in tickets, and
// echoed into every CI transcript — and cancel is a write, which makes the
// gap a way to stop another tenant's work rather than merely to watch it.
//
// So the namespace scopes the id the same way it scopes an orkflow name, and
// the refusal is NotFound rather than PermissionDenied: a caller who cannot
// name the namespace must not be able to tell "that id is not yours" from
// "there is no such run".
func TestRunID_IsAnAddressInsideANamespaceNotACapabilityOverTheDeployment(t *testing.T) {
	const document = `
name: deploy
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`
	starter := &fakeStarter{}
	bus := engine.NewEventBus()
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Namespaces: newFakeRegistry(
			&orkstackv1.Namespace{Name: "alpha"},
			&orkstackv1.Namespace{Name: "bravo"},
		),
		Runs:    engine.NewRunStore(),
		Bus:     bus,
		Starter: starter,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	ctx := context.Background()
	mustApply(t, svc, document, "alpha")

	submitted, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy",
		Namespace:   "alpha",
	}))
	if err != nil {
		t.Fatalf("SubmitRun(alpha, deploy): %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()

	// The run's stream is finished before anything asks for it, so a stream
	// that is NOT refused ends immediately and reports "succeeded" instead of
	// blocking for a run that will never terminate. A test whose refusal is
	// the only reason it returns cannot tell a working scope from a hang, and
	// the mutation that removes the scope would time out rather than fail.
	bus.Publish(&orkstackv1.Event{
		RunId: runID,
		Type:  orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
	}, runID+"#done")

	// Its own namespace reads it.
	if _, err := svc.GetRun(ctx, connect.NewRequest(
		&orkstackv1.GetRunRequest{RunId: runID, Namespace: "alpha"})); err != nil {
		t.Fatalf("GetRun(alpha, %s): %v", runID, err)
	}

	// Another namespace does not — and neither does the default, which is what
	// a request that names no namespace at all resolves to.
	for _, namespace := range []string{"bravo", ""} {
		label := namespace
		if label == "" {
			label = "(unset, so the default namespace)"
		}
		t.Run("GetRun from "+label, func(t *testing.T) {
			_, err := svc.GetRun(ctx, connect.NewRequest(
				&orkstackv1.GetRunRequest{RunId: runID, Namespace: namespace}))
			if err == nil {
				t.Fatalf("GetRun(%s, %s) succeeded: alpha's run is readable from another namespace",
					label, runID)
			}
			if code := connect.CodeOf(err); code != connect.CodeNotFound {
				t.Errorf("code = %v, want NotFound", code)
			}
		})
		t.Run("StreamEvents from "+label, func(t *testing.T) {
			err := svc.StreamRunEvents(ctx, runID, namespace, 0,
				func(*orkstackv1.Event) error { return nil })
			if err == nil {
				t.Fatalf("StreamRunEvents(%s, %s) succeeded: alpha's events stream to another namespace",
					label, runID)
			}
			if code := connect.CodeOf(err); code != connect.CodeNotFound {
				t.Errorf("code = %v, want NotFound", code)
			}
		})
		t.Run("CancelRun from "+label, func(t *testing.T) {
			_, err := svc.CancelRun(ctx, connect.NewRequest(
				&orkstackv1.CancelRunRequest{RunId: runID, Namespace: namespace}))
			if err == nil {
				t.Fatalf("CancelRun(%s, %s) succeeded: another namespace stopped alpha's work",
					label, runID)
			}
			if code := connect.CodeOf(err); code != connect.CodeNotFound {
				t.Errorf("code = %v, want NotFound", code)
			}
		})
	}

	// The refusal must be a refusal, not a slow yes: nothing reached the
	// starter.
	if canceled := starter.canceledRuns(); len(canceled) != 0 {
		t.Errorf("the starter was asked to cancel %v", canceled)
	}

	// …and the run's own namespace still cancels it, so the scope is a scope
	// and not a wall.
	if _, err := svc.CancelRun(ctx, connect.NewRequest(
		&orkstackv1.CancelRunRequest{RunId: runID, Namespace: "alpha"})); err != nil {
		t.Fatalf("CancelRun(alpha, %s): %v", runID, err)
	}
	if canceled := starter.canceledRuns(); len(canceled) != 1 || canceled[0] != runID {
		t.Errorf("canceled = %v, want exactly [%s]", canceled, runID)
	}
}

// TestWebSocketBridge_RefusesARunOfAnotherNamespace holds the second entry
// point to the same rule.
//
// The bridge is the UI's read path and takes the run id in the URL, so it is
// exactly where a scope gets forgotten: it shares Service.StreamRunEvents with
// the RPC, but it resolves the run itself before upgrading, and a check
// written only in the RPC handler would leave this door open. The namespace
// travels as a query parameter — a socket has no request body — and a run of
// another namespace is 404, before any upgrade.
func TestWebSocketBridge_RefusesARunOfAnotherNamespace(t *testing.T) {
	const document = `
name: deploy
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Namespaces: newFakeRegistry(
			&orkstackv1.Namespace{Name: "alpha"},
			&orkstackv1.Namespace{Name: "bravo"},
		),
		Runs:    engine.NewRunStore(),
		Bus:     engine.NewEventBus(),
		Starter: &fakeStarter{},
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	ctx := context.Background()
	mustApply(t, svc, document, "alpha")
	submitted, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy",
		Namespace:   "alpha",
	}))
	if err != nil {
		t.Fatalf("SubmitRun(alpha, deploy): %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()

	mux := http.NewServeMux()
	mux.Handle("/ws/runs/{id}", api.NewWebSocketBridge(svc, nil))
	server := httptest.NewServer(mux)
	defer server.Close()

	// A plain GET never upgrades, so what is being read here is the decision
	// the bridge makes BEFORE the upgrade: 404 for a run this namespace
	// cannot address, and anything else for one it can.
	get := func(namespace string) int {
		t.Helper()
		resp, err := server.Client().Get(
			server.URL + "/ws/runs/" + runID + "?namespace=" + namespace)
		if err != nil {
			t.Fatalf("GET /ws/runs/%s?namespace=%s: %v", runID, namespace, err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	for _, namespace := range []string{"bravo", ""} {
		if code := get(namespace); code != http.StatusNotFound {
			t.Errorf("GET /ws/runs/%s?namespace=%q = %d, want 404: alpha's run is watchable "+
				"from another namespace", runID, namespace, code)
		}
	}
	if code := get("alpha"); code == http.StatusNotFound {
		t.Errorf("GET /ws/runs/%s?namespace=alpha = 404: its own namespace cannot watch it", runID)
	}
}

// TestUpdateNamespace_TwoFirstEditsOfTheDefaultCannotClobberEachOther is the
// compare-and-swap on the one document nobody created.
//
// Namespace.etag promises that "two operators editing the same namespace at
// once cannot silently clobber each other". For `default` that promise had a
// hole with nothing in the tree to catch it: until somebody writes its
// document, a read hands back a SYNTHESIZED one whose etag is empty, and an
// empty etag means "no such requirement" to the store. So both of two
// concurrent first edits were unconditional writes, and one of them vanished
// with no error — the caller never asked for an unconditional write, the
// server handed them the token that means one.
//
// TestUpdateNamespace_RefusesAStaleEtag proves the ordinary case and cannot
// see this one: it creates its namespace first, so it never holds a document
// with no stored bytes.
func TestUpdateNamespace_TwoFirstEditsOfTheDefaultCannotClobberEachOther(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	// Both operators read `default` before either writes. Neither read has an
	// etag to carry, because nothing has ever been stored under that name.
	read := func() *orkstackv1.Namespace {
		t.Helper()
		resp, err := svc.GetNamespace(ctx,
			connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "default"}))
		if err != nil {
			t.Fatalf("GetNamespace(default): %v", err)
		}
		return resp.Msg.GetNamespace()
	}
	first, second := read(), read()
	if first.GetEtag() != "" || second.GetEtag() != "" {
		t.Fatalf("the synthesized default carries etags %q/%q; this test is about the case "+
			"where it carries none", first.GetEtag(), second.GetEtag())
	}

	first.Description = "the winner"
	if _, err := svc.UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: first})); err != nil {
		t.Fatalf("the first edit of default: %v", err)
	}

	second.Description = "the loser"
	_, err := svc.UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: second}))
	if err == nil {
		t.Fatal("the second first-time edit of default overwrote the first with no error")
	}
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", code)
	}
	if !strings.Contains(err.Error(), "read it again") {
		t.Errorf("error %q does not tell the loser what to do", err)
	}

	if got := read().GetDescription(); got != "the winner" {
		t.Errorf("description = %q, want %q: the loser's write landed", got, "the winner")
	}

	// Once the document exists, the ordinary rule takes over: the etag the
	// read handed back is the token, and the escape hatch is still an escape
	// hatch rather than a permanent one.
	stored := read()
	if stored.GetEtag() == "" {
		t.Fatal("the stored default still has no etag, so nothing can compare-and-swap on it")
	}
	stored.Description = "edited again"
	if _, err := svc.UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: stored})); err != nil {
		t.Fatalf("editing the stored default with the etag it handed back: %v", err)
	}
}
