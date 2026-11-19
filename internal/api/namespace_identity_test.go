package api

import (
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// policyService is a service whose namespaces' ids and identity policies
// are kept by one local store, with `payments` registered.
func policyService(t *testing.T) (*Service, *policyRegistry, *artifacts.IncarnationStore) {
	t.Helper()
	ids, err := artifacts.NewIncarnationStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	reg := &policyRegistry{incarnationRegistry: newIncarnationRegistry("payments")}
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore(), Namespaces: reg, Incarnations: ids, NamespacePolicies: ids, IdentityAttemptsMaxExtra: 4})
	return s, reg, ids
}

// policyRegistry is incarnationRegistry whose Put updates a document in
// place, keeping its created_at and moving its etag, as the durable store
// does for an update; make still makes a namespace anew.
type policyRegistry struct {
	*incarnationRegistry
	writes       int
	refuseDelete bool
}

func (r *policyRegistry) Delete(ctx context.Context, name string) error {
	if r.refuseDelete {
		return artifacts.ErrNamespaceNotEmpty
	}
	return r.incarnationRegistry.Delete(ctx, name)
}

func (r *policyRegistry) Put(ctx context.Context, ns *orkstackv1.Namespace, cond artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	stored, ok := r.docs[ns.GetName()]
	switch {
	case !ok:
		r.mu.Unlock()
		return r.incarnationRegistry.Put(ctx, ns, cond)
	case cond.IfAbsent && stored.GetEtag() != "", cond.IfMatch != "" && cond.IfMatch != stored.GetEtag():
		r.mu.Unlock()
		return nil, artifacts.ErrPreconditionFailed
	}
	r.writes++
	next := proto.Clone(ns).(*orkstackv1.Namespace)
	next.CreatedAt = stored.GetCreatedAt()
	next.Etag = fmt.Sprintf("e%d", r.writes)
	r.docs[ns.GetName()] = next
	r.mu.Unlock()
	return r.Get(ctx, ns.GetName())
}

func getNS(t *testing.T, s *Service, name string) *orkstackv1.Namespace {
	t.Helper()
	got, err := s.GetNamespace(context.Background(), connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: name}))
	if err != nil {
		t.Fatal(err)
	}
	return got.Msg.GetNamespace()
}

func withPolicy(ns *orkstackv1.Namespace, edit func(*orkstackv1.NamespaceIdentity)) *orkstackv1.Namespace {
	out := proto.Clone(ns).(*orkstackv1.Namespace)
	if out.Identity == nil {
		out.Identity = &orkstackv1.NamespaceIdentity{}
	}
	edit(out.Identity)
	return out
}

func deployerPolicy(id *orkstackv1.NamespaceIdentity) {
	id.Audiences = []string{"https://vault.example"}
	id.Grants = map[string]*orkstackv1.Grant{"deployer": {Provider: "aws", Principal: "arn:aws:iam::1:role/deploy", Orkflows: []string{"deploy"}, Origins: []string{"submit"}}}
}

// TestUpdateNamespace_IdentityWritesOneObject: an update that changes the
// identity policy writes the policy alone, against its own etag, and leaves
// the namespace document as it was; one that also changes the document is
// refused; a stale policy etag is refused; a read shows the policy.
func TestUpdateNamespace_IdentityWritesOneObject(t *testing.T) {
	ctx := context.Background()
	s, reg, _ := policyService(t)
	before := getNS(t, s, "payments")
	if p := before.GetIdentity(); len(p.GetAudiences())+len(p.GetGrants()) != 0 || p.GetEtag() != "" {
		t.Fatalf("a namespace with no policy shows %v", p)
	}
	docBefore, _ := reg.Get(ctx, "payments")

	got, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: withPolicy(before, deployerPolicy)}))
	if err != nil {
		t.Fatal(err)
	}
	if id := got.Msg.GetNamespace().GetIdentity(); id.GetRevision() != 1 || id.GetEtag() == "" || id.GetGrants()["deployer"].GetPrincipal() != "arn:aws:iam::1:role/deploy" {
		t.Fatalf("the update answered %v", id)
	}
	docAfter, _ := reg.Get(ctx, "payments")
	if !proto.Equal(docBefore, docAfter) {
		t.Fatalf("an identity change rewrote the namespace document: %v then %v", docBefore, docAfter)
	}
	read := getNS(t, s, "payments")
	if read.GetIdentity().GetRevision() != 1 || read.GetIdentity().GetAudiences()[0] != "https://vault.example" || read.GetIdentity().GetNamespaceId() == "" {
		t.Fatalf("a read shows %v", read.GetIdentity())
	}

	// The first read's policy etag is stale now.
	stale := withPolicy(before, func(id *orkstackv1.NamespaceIdentity) { id.Audiences = []string{"https://other.example"} })
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: stale})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a stale policy etag = %v", err)
	}
	both := withPolicy(read, func(id *orkstackv1.NamespaceIdentity) { id.IdentitiesCurrentOnly = true })
	both.Description = "changed too"
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: both})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a policy and a document change in one update = %v", err)
	}
	// A document-only update carrying the policy as read leaves the policy.
	doc := proto.Clone(read).(*orkstackv1.Namespace)
	doc.Description = "described"
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: doc})); err != nil {
		t.Fatal(err)
	}
	if again := getNS(t, s, "payments"); again.GetDescription() != "described" || again.GetIdentity().GetRevision() != 1 {
		t.Fatalf("after a document update: %v", again)
	}
	stored, _ := reg.Get(ctx, "payments")
	if len(stored.GetIdentity().GetGrants())+len(stored.GetIdentity().GetAudiences()) != 0 {
		t.Fatalf("the policy was stored in the namespace document: %v", stored.GetIdentity())
	}
}

// TestUpdateNamespace_AttemptsExtraAboveMax: a policy whose attempts_extra
// exceeds identity.attempts.max_extra is refused, naming the limit, and an
// invalid grant is refused as invalid.
func TestUpdateNamespace_AttemptsExtraAboveMax(t *testing.T) {
	ctx := context.Background()
	s, _, _ := policyService(t)
	ns := getNS(t, s, "payments")
	five := int32(5)
	over := withPolicy(ns, func(id *orkstackv1.NamespaceIdentity) { id.AttemptsExtra = &five })
	_, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: over}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !contains(err.Error(), "identity.attempts.max_extra") {
		t.Fatalf("attempts_extra over the deployment's max = %v", err)
	}
	bad := withPolicy(ns, func(id *orkstackv1.NamespaceIdentity) {
		id.Grants = map[string]*orkstackv1.Grant{"Bad": {Provider: "aws", Principal: "p", Orkflows: []string{"*"}}}
	})
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: bad})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("an invalid grant name = %v", err)
	}
}

// TestUpdateNamespace_IdentityNeedsPolicyUpdate: under enforce, a caller
// who may update the namespace but not its policy is refused an identity
// change.
func TestUpdateNamespace_IdentityNeedsPolicyUpdate(t *testing.T) {
	s, _, _ := policyService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil,
		&orkstackv1.Binding{Name: "ns-admins", Role: authz.RoleNamespaceAdmin, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx := asAlice(authn.ModeEnforce)
	ns := getNS(t, s, "payments")
	_, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: withPolicy(ns, deployerPolicy)}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("an identity change without namespace.policy.update = %v", err)
	}
}

// TestDeleteNamespace_PolicyBeforeDocIdAfter: a delete removes the policy
// before the namespace document and the id after it, so the name made again
// starts with no policy.
func TestDeleteNamespace_PolicyBeforeDocIdAfter(t *testing.T) {
	ctx := context.Background()
	s, reg, ids := policyService(t)
	ns := getNS(t, s, "payments")
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: withPolicy(ns, deployerPolicy)})); err != nil {
		t.Fatal(err)
	}
	created := ns.GetCreatedAt().AsTime()
	if _, err := s.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "payments"})); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ids.NamespacePolicy(ctx, "payments", created); err != nil || ok {
		t.Fatalf("the deleted namespace's policy = %v, %v", ok, err)
	}
	if _, ok, _ := ids.NamespaceID(ctx, "payments", created); ok {
		t.Fatal("the deleted namespace's id is still recorded")
	}
	reg.make("payments")
	if again := getNS(t, s, "payments"); len(again.GetIdentity().GetGrants()) != 0 {
		t.Fatalf("the name made again inherits %v", again.GetIdentity())
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestCreateNamespace_RefusesAnIdentityPolicy: a namespace is created with
// no identity policy, so a create that carries one is refused and nothing is
// stored, in the document or beside it.
func TestCreateNamespace_RefusesAnIdentityPolicy(t *testing.T) {
	ctx := context.Background()
	s, reg, _ := policyService(t)
	ns := withPolicy(&orkstackv1.Namespace{Name: "billing"}, deployerPolicy)
	if _, err := s.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: ns})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a create with a policy = %v", err)
	}
	if _, err := reg.Get(ctx, "billing"); err == nil {
		t.Fatal("a refused create stored the namespace")
	}
}

// TestDeleteNamespace_ARefusedDeleteKeepsThePolicy: a delete the registry
// refuses after the policy was cleared puts the policy back.
func TestDeleteNamespace_ARefusedDeleteKeepsThePolicy(t *testing.T) {
	ctx := context.Background()
	s, reg, ids := policyService(t)
	ns := getNS(t, s, "payments")
	if _, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: withPolicy(ns, deployerPolicy)})); err != nil {
		t.Fatal(err)
	}
	reg.refuseDelete = true
	if _, err := s.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "payments"})); err == nil {
		t.Fatal("a refused delete succeeded")
	}
	p, ok, err := ids.NamespacePolicy(ctx, "payments", ns.GetCreatedAt().AsTime())
	if err != nil || !ok || len(p.Grants) != 1 || p.Generation != 3 {
		t.Fatalf("the policy after a refused delete = %+v, %v, %v", p, ok, err)
	}
}
