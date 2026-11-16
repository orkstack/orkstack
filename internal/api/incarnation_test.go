package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// incarnationRegistry is a namespace registry whose documents carry the
// created_at the durable store stamps, so a namespace made again under its
// name is told apart from the one that was deleted.
type incarnationRegistry struct {
	mu   sync.Mutex
	docs map[string]*orkstackv1.Namespace
	now  time.Time
}

func newIncarnationRegistry(names ...string) *incarnationRegistry {
	r := &incarnationRegistry{docs: map[string]*orkstackv1.Namespace{}, now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	for _, n := range names {
		r.make(n)
	}
	return r
}

// make stores the namespace anew, a second later than anything before.
func (r *incarnationRegistry) make(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.now = r.now.Add(time.Second)
	r.docs[name] = &orkstackv1.Namespace{Name: name, CreatedAt: timestamppb.New(r.now)}
}

func (r *incarnationRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name = artifacts.ResolveNamespace(name)
	if ns, ok := r.docs[name]; ok {
		return proto.Clone(ns).(*orkstackv1.Namespace), nil
	}
	if name == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, artifacts.ErrNotFound
}
func (r *incarnationRegistry) List(context.Context) ([]*orkstackv1.Namespace, error) { return nil, nil }
func (r *incarnationRegistry) ListNames(context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := []string{artifacts.DefaultNamespace}
	for n := range r.docs {
		names = append(names, n)
	}
	return names, nil
}
func (r *incarnationRegistry) Put(_ context.Context, ns *orkstackv1.Namespace, cond artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	if _, ok := r.docs[ns.GetName()]; ok && cond.IfAbsent {
		r.mu.Unlock()
		return nil, artifacts.ErrPreconditionFailed
	}
	r.mu.Unlock()
	r.make(ns.GetName())
	return r.Get(context.Background(), ns.GetName())
}
func (r *incarnationRegistry) Delete(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.docs, name)
	return nil
}

// failingIncarnations is an id store that cannot be reached.
type failingIncarnations struct{ IncarnationStore }

var errIDStoreDown = errors.New("the id store is down")

func (failingIncarnations) NamespaceID(context.Context, string, time.Time) (string, bool, error) {
	return "", false, errIDStoreDown
}
func (failingIncarnations) EnsureNamespaceID(context.Context, string, time.Time) (string, error) {
	return "", errIDStoreDown
}
func (failingIncarnations) OrkflowID(context.Context, string, string, time.Time) (string, bool, error) {
	return "", false, errIDStoreDown
}
func (failingIncarnations) EnsureOrkflowID(context.Context, string, string, time.Time) (string, error) {
	return "", errIDStoreDown
}

// pinnedService is an enforcing service with an id store, whose document
// makes alice admin at the deployment.
func pinnedService(t *testing.T) (*Service, accessHandler, *incarnationRegistry, *artifacts.IncarnationStore) {
	t.Helper()
	ids, err := artifacts.NewIncarnationStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	reg := newIncarnationRegistry("payments")
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore(), Namespaces: reg, Incarnations: ids})
	a, _ := accessFor(t, authn.ModeEnforce, nil,
		&orkstackv1.Binding{Name: "admins", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:alice"}})
	s.access = a
	return s, accessHandler{svc: s}, reg, ids
}

var bob = &authz.Subject{ID: "user:corp:bob", Kind: "user", Issuer: "corp"}

// bobMay decides run.read for bob on res, with the ids the interceptor reads.
func bobMay(t *testing.T, s *Service, res authz.Resource) bool {
	t.Helper()
	ctx := asAlice(authn.ModeEnforce)
	policy, _ := s.access.policy(ctx)
	res, err := s.withIDs(ctx, policy, res)
	if err != nil {
		t.Fatalf("withIDs: %v", err)
	}
	_, err = authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunRead), Subject: bob, Resource: res, Policy: policy})
	return err == nil
}

func bindingNamed(t *testing.T, h accessHandler, name string) *orkstackv1.Binding {
	t.Helper()
	resp, err := h.GetAccess(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.GetAccessRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range resp.Msg.GetDocument().GetBindings() {
		if b.GetName() == name {
			return b
		}
	}
	t.Fatalf("no binding %q", name)
	return nil
}

// expireIDs makes the next decision read every id again, as one replica's
// cache does once incarnationCacheTTL has passed.
func expireIDs(s *Service) {
	at := time.Now().Add(incarnationCacheTTL)
	s.ids.now = func() time.Time { return at }
}

func mustCreatedAt(t *testing.T, reg *incarnationRegistry, name string) time.Time {
	t.Helper()
	ns, err := reg.Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return ns.GetCreatedAt().AsTime()
}

// etagFromStore reads the document's etag from the store's head, for a
// service whose GetAccess cannot answer.
func etagFromStore(t *testing.T, h accessHandler) string {
	t.Helper()
	head, err := h.svc.access.Policies.store.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return etagOfHead(head)
}

// TestBinding_PinnedToTheIncarnationItWasSavedFor: a binding saved at a
// namespace is pinned to that incarnation; after the name is made again
// around the API it matches nothing, is listed as stale, and ExplainAccess
// says why; a repin pins it to what exists now.
func TestBinding_PinnedToTheIncarnationItWasSavedFor(t *testing.T) {
	s, h, reg, ids := pinnedService(t)
	ctx := asAlice(authn.ModeEnforce)
	etag, _ := etagOf(t, h)
	created, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag,
		Binding: &orkstackv1.Binding{Name: "viewers", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:bob"}}}))
	if err != nil {
		t.Fatal(err)
	}
	first := created.Msg.GetBinding()
	if first.GetPin() != orkstackv1.BindingPin_BINDING_PIN_PINNED || !strings.HasPrefix(first.GetScopeId(), artifacts.NamespaceIDPrefix) {
		t.Fatalf("created binding = pin %v, scope_id %q", first.GetPin(), first.GetScopeId())
	}
	payments := authz.Resource{Namespace: "payments"}
	if !bobMay(t, s, payments) {
		t.Fatal("the pinned binding does not grant its own incarnation")
	}

	// Deleted and made again by something that went round the API: an
	// engine of an earlier release, or a writer of the bucket.
	reg.make("payments")
	if _, err := ids.EnsureNamespaceID(context.Background(), "payments", mustCreatedAt(t, reg, "payments")); err != nil {
		t.Fatal(err)
	}
	expireIDs(s)
	if bobMay(t, s, payments) {
		t.Fatal("a binding pinned to the deleted namespace grants the one made again under its name")
	}
	if got := bindingNamed(t, h, "viewers"); got.GetPin() != orkstackv1.BindingPin_BINDING_PIN_STALE || got.GetScopeId() != first.GetScopeId() {
		t.Fatalf("after the re-create the binding is %v pinned to %q", got.GetPin(), got.GetScopeId())
	}
	ex, err := h.ExplainAccess(ctx, connect.NewRequest(&orkstackv1.ExplainAccessRequest{PrincipalId: bob.ID, Permission: string(authz.PermRunRead), Namespace: "payments"}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ex.Msg.GetExplanation(), `"viewers" would have matched by name`) || !strings.Contains(ex.Msg.GetExplanation(), "--repin") {
		t.Fatalf("ExplainAccess said %q", ex.Msg.GetExplanation())
	}

	// A save that changes nothing else never moves a pin.
	etag, _ = etagOf(t, h)
	same := &orkstackv1.Binding{Name: "viewers", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:bob"}}
	if _, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag, Binding: same})); err != nil {
		t.Fatal(err)
	}
	if got := bindingNamed(t, h, "viewers"); got.GetScopeId() != first.GetScopeId() {
		t.Fatalf("an unchanged save moved the pin to %q", got.GetScopeId())
	}
	etag, _ = etagOf(t, h)
	repinned, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag, Binding: same, Repin: true}))
	if err != nil {
		t.Fatal(err)
	}
	if b := repinned.Msg.GetBinding(); b.GetPin() != orkstackv1.BindingPin_BINDING_PIN_PINNED || b.GetScopeId() == first.GetScopeId() {
		t.Fatalf("repinned = %v %q (was %q)", b.GetPin(), b.GetScopeId(), first.GetScopeId())
	}
	if !bobMay(t, s, payments) {
		t.Fatal("a repinned binding does not grant the namespace it is pinned to")
	}
}

// TestBinding_ByNameUntilItsScopeExists: a binding made before its Orkflow
// exists matches by name, and is pinned by its next save once it does; a
// repin with nothing to pin to is refused, and so is one of a scope that is
// never pinned.
func TestBinding_ByNameUntilItsScopeExists(t *testing.T) {
	s, h, _, _ := pinnedService(t)
	ctx := asAlice(authn.ModeEnforce)
	early := &orkstackv1.Binding{Name: "report-viewers", Role: authz.RoleViewer, Scope: "orkflow:payments/report", Subjects: []string{"user:corp:bob"}}
	etag, _ := etagOf(t, h)
	created, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag, Binding: early}))
	if err != nil {
		t.Fatal(err)
	}
	if b := created.Msg.GetBinding(); b.GetPin() != orkstackv1.BindingPin_BINDING_PIN_BY_NAME || b.GetScopeId() != "" {
		t.Fatalf("a binding of an Orkflow that does not exist = %v %q", b.GetPin(), b.GetScopeId())
	}
	report := authz.Resource{Namespace: "payments", Orkflow: "report"}
	if !bobMay(t, s, report) {
		t.Fatal("a binding by name does not match by name")
	}
	etag, _ = etagOf(t, h)
	if _, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag, Binding: early, Repin: true})); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "nothing exists") {
		t.Fatalf("a repin with nothing to pin to = %v", err)
	}
	deployment := &orkstackv1.Binding{Name: "everyone", Role: authz.RoleViewer, Scope: "deployment", Subjects: []string{"user:corp:bob"}}
	if _, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag, Binding: deployment, Repin: true})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a repin at the deployment = %v", err)
	}

	if _, _, err := s.orkflows.Put(context.Background(), &orkstackv1.Orkflow{Namespace: "payments", Name: "report", Version: "1",
		Tasks: []*orkstackv1.Task{{Id: "a", Uses: "shell"}}}, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	s.mintOrkflowID(context.Background(), "payments", "report")
	if b := bindingNamed(t, h, "report-viewers"); b.GetPin() != orkstackv1.BindingPin_BINDING_PIN_BY_NAME {
		t.Fatalf("the Orkflow's create pinned the binding itself: %v", b.GetPin())
	}
	etag, _ = etagOf(t, h)
	saved, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag, Binding: early}))
	if err != nil {
		t.Fatal(err)
	}
	if b := saved.Msg.GetBinding(); b.GetPin() != orkstackv1.BindingPin_BINDING_PIN_PINNED || !strings.HasPrefix(b.GetScopeId(), artifacts.OrkflowIDPrefix) {
		t.Fatalf("the next save = %v %q", b.GetPin(), b.GetScopeId())
	}
	if !bobMay(t, s, report) {
		t.Fatal("the binding pinned at its save does not grant its Orkflow")
	}
}

// TestBinding_AnIDStoreThatCannotBeReachedIsUnavailable: a save that must
// pin, and a decision a pinned binding could take part in, are unavailable
// while the ids cannot be read; neither is decided as if there were none.
func TestBinding_AnIDStoreThatCannotBeReachedIsUnavailable(t *testing.T) {
	s, h, _, _ := pinnedService(t)
	ctx := asAlice(authn.ModeEnforce)
	etag, _ := etagOf(t, h)
	if _, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag,
		Binding: &orkstackv1.Binding{Name: "viewers", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:bob"}}})); err != nil {
		t.Fatal(err)
	}
	s.incarnations = failingIncarnations{}
	expireIDs(s)
	policy, _ := s.access.policy(ctx)
	if _, err := s.withIDs(ctx, policy, authz.Resource{Namespace: "payments"}); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("a decision with the id store down = %v; want unavailable", err)
	}
	if _, err := s.withIDs(ctx, policy, authz.Resource{Namespace: "billing"}); err != nil {
		t.Fatalf("a namespace no binding pins read its ids: %v", err)
	}
	if got := bindingNamed(t, h, "viewers"); got.GetPin() != orkstackv1.BindingPin_BINDING_PIN_PINNED {
		t.Fatalf("with the id store down GetAccess shows the binding %v; want it answered, pinned", got.GetPin())
	}
	if _, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etagFromStore(t, h),
		Binding: &orkstackv1.Binding{Name: "more", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:eve"}}})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("a save with the id store down = %v; want unavailable", err)
	}
}

// TestNamespaceIDs_FollowCreateAndDelete: a namespace is given its id when
// it is created and loses the record when it is deleted; a re-created
// namespace gets another id. `default`'s id is the constant.
func TestNamespaceIDs_FollowCreateAndDelete(t *testing.T) {
	s, _, _, ids := pinnedService(t)
	ctx := context.Background()
	if _, err := s.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{Name: "orders"}})); err != nil {
		t.Fatal(err)
	}
	first, err := s.namespaceID(ctx, "orders", false)
	if err != nil || !strings.HasPrefix(first, artifacts.NamespaceIDPrefix) {
		t.Fatalf("after create the id is %q, %v", first, err)
	}
	if _, err := s.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "orders"})); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := ids.NamespaceID(ctx, "orders", time.Time{}); err != nil || ok || id != "" {
		t.Fatalf("after delete the record reads %q, %v, %v", id, ok, err)
	}
	if _, err := s.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{Name: "orders"}})); err != nil {
		t.Fatal(err)
	}
	if again, _ := s.namespaceID(ctx, "orders", false); again == "" || again == first {
		t.Fatalf("the re-created namespace's id is %q (was %q)", again, first)
	}
	if id, _ := s.namespaceID(ctx, "default", false); id != artifacts.DefaultNamespaceID {
		t.Fatalf("default's id is %q", id)
	}
}

// TestGetShowsTheIDs: GetNamespace and GetOrkflow answer the id of the
// incarnation they read, and neither makes one.
func TestGetShowsTheIDs(t *testing.T) {
	s, _, _, _ := pinnedService(t)
	ctx := context.Background()
	got, err := s.GetNamespace(ctx, connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "payments"}))
	if err != nil || got.Msg.GetNamespace().GetIdentity().GetNamespaceId() != "" {
		t.Fatalf("a namespace with no id yet answered %q, %v", got.Msg.GetNamespace().GetIdentity().GetNamespaceId(), err)
	}
	id, err := s.namespaceID(ctx, "payments", true)
	if err != nil {
		t.Fatal(err)
	}
	got, err = s.GetNamespace(ctx, connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "payments"}))
	if err != nil || got.Msg.GetNamespace().GetIdentity().GetNamespaceId() != id {
		t.Fatalf("GetNamespace = %q, %v; want %q", got.Msg.GetNamespace().GetIdentity().GetNamespaceId(), err, id)
	}
	if _, _, err := s.orkflows.Put(ctx, &orkstackv1.Orkflow{Namespace: "payments", Name: "report", Version: "1",
		Tasks: []*orkstackv1.Task{{Id: "a", Uses: "shell"}}}, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	wf, err := s.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Namespace: "payments", Name: "report"}))
	if err != nil || wf.Msg.GetOrkflowId() != "" {
		t.Fatalf("an Orkflow with no id yet answered %q, %v", wf.Msg.GetOrkflowId(), err)
	}
	s.mintOrkflowID(ctx, "payments", "report")
	wf, err = s.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Namespace: "payments", Name: "report"}))
	if err != nil || !strings.HasPrefix(wf.Msg.GetOrkflowId(), artifacts.OrkflowIDPrefix) {
		t.Fatalf("GetOrkflow = %q, %v", wf.Msg.GetOrkflowId(), err)
	}
}

// TestIncarnationCache_ARaceWithAForgetIsNotKept: a read that began before a
// create or delete through this replica forgot the entry is not kept, so it
// cannot leave the old answer cached past the change.
func TestIncarnationCache_ARaceWithAForgetIsNotKept(t *testing.T) {
	c := newIncarnationCache()
	key := incarnationKey("", "report")
	if key != "default/report" {
		t.Fatalf("key = %q; a namespace spelled empty is default's", key)
	}
	_, ok, gen := c.get(key)
	if ok {
		t.Fatal("an empty cache answered")
	}
	c.forget("default", "report")
	c.fill(key, "", gen)
	if _, ok, _ := c.get(key); ok {
		t.Fatal("a read that raced a forget was kept")
	}
	_, _, gen = c.get(key)
	c.fill(key, "of_1", gen)
	if id, ok, _ := c.get(key); !ok || id != "of_1" {
		t.Fatalf("an unraced read was not kept: %q, %v", id, ok)
	}
	c.forget("default", "")
	if _, ok, _ := c.get(key); ok {
		t.Fatal("forgetting a namespace kept one of its Orkflows")
	}
}

// TestMayAnswer_ReadsTheIDsAPinnedBindingNeeds: a handler's own check, past
// the interceptor, grants what a pinned binding grants.
func TestMayAnswer_ReadsTheIDsAPinnedBindingNeeds(t *testing.T) {
	s, h, _, _ := pinnedService(t)
	etag, _ := etagOf(t, h)
	if _, err := h.CreateBinding(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag,
		Binding: &orkstackv1.Binding{Name: "viewers", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:bob"}}})); err != nil {
		t.Fatal(err)
	}
	asBob := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce),
		&authn.Principal{Kind: authn.KindUser, ID: bob.ID, Issuer: "corp", Method: authn.MethodJWT})
	if may, err := s.mayAnswer(asBob, authz.PermRunRead, authz.Resource{Namespace: "payments"}); err != nil || !may {
		t.Fatalf("a pinned binding does not grant a handler's own check: %v, %v", may, err)
	}
	// A list filtered by row fails whole: it never answers with rows left
	// out that it could not decide. Admitted while the store answers, the
	// rows are filtered after it stops.
	req := &orkstackv1.ListOrkflowsRequest{Namespace: "payments"}
	ctx, adm, err := s.admitCall(asBob, orkstackv1connect.OrkflowServiceListOrkflowsProcedure, req, "")
	if err != nil {
		t.Fatalf("bob's list was not admitted: %v", err)
	}
	s.incarnations = failingIncarnations{}
	expireIDs(s)
	if may, err := s.mayAnswer(asBob, authz.PermRunRead, authz.Resource{Namespace: "payments"}); may || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("a handler's check with the ids unreadable = %v, %v; want unavailable", may, err)
	}

	resp := &orkstackv1.ListOrkflowsResponse{Orkflows: []*orkstackv1.Orkflow{{Name: "report"}}}
	if err := s.filterRowsOf(ctx, adm, req, resp); connect.CodeOf(err) != connect.CodeUnavailable || len(resp.GetOrkflows()) != 1 {
		t.Fatalf("a filtered list with the ids unreadable = %v, rows %d; want unavailable, untouched", err, len(resp.GetOrkflows()))
	}
}

// TestNamespaceID_IsNeverStoredOrComparedAsPolicy: a document read with its
// id and sent back is no change of policy, and the id is never written into
// the namespace document.
func TestNamespaceID_IsNeverStoredOrComparedAsPolicy(t *testing.T) {
	stored := &orkstackv1.Namespace{Name: "payments"}
	echoed := &orkstackv1.Namespace{Name: "payments", Identity: &orkstackv1.NamespaceIdentity{NamespaceId: "ns_1"}}
	if namespacePolicyDiffers(echoed, stored) {
		t.Fatal("an echoed id reads as a change of policy")
	}
	if got := withoutNamespaceID(echoed); got.GetIdentity() != nil || echoed.GetIdentity().GetNamespaceId() != "ns_1" {
		t.Fatalf("withoutNamespaceID = %v, and the input became %v", got, echoed)
	}
}

// TestIncarnationErrors_SayWhatTheyAre: an unreadable record is an internal
// fault that says how to clear it, a name that addresses nothing is the
// caller's, a lost race is aborted, and anything else is unavailable.
func TestIncarnationErrors_SayWhatTheyAre(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code connect.Code
		says string
	}{
		{artifacts.ErrIncarnationUnreadable, connect.CodeInternal, "operator removes"},
		{artifacts.ErrInvalidNamespace, connect.CodeInvalidArgument, ""},
		{artifacts.ErrInvalidOrkflow, connect.CodeInvalidArgument, ""},
		{artifacts.ErrIncarnationContended, connect.CodeAborted, ""},
		{context.Canceled, connect.CodeCanceled, ""},
		{errIDStoreDown, connect.CodeUnavailable, ""},
	} {
		s := &Service{logger: slog.New(slog.DiscardHandler)}
		leak := fmt.Errorf("artifacts: read s3://secret-bucket/metadata/identity/namespaces/payments.json: %w", tc.err)
		got := s.idError("the id of namespace \"payments\"", leak)
		if connect.CodeOf(got) != tc.code || !strings.Contains(got.Error(), tc.says) {
			t.Errorf("%v = %v; want %v saying %q", tc.err, got, tc.code, tc.says)
		}
		if tc.code != connect.CodeInvalidArgument && strings.Contains(got.Error(), "secret-bucket") {
			t.Errorf("%v told the caller where the store is: %v", tc.err, got)
		}
	}
}
