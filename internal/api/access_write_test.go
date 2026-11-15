package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var nina = &authn.Principal{Kind: authn.KindUser, ID: "user:corp:nina", Issuer: "corp", Method: authn.MethodJWT}

func asNina() context.Context {
	return authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), nina)
}

// knownNamespaces is a registry holding exactly the names it lists.
type knownNamespaces []string

func (k knownNamespaces) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	if name == "" || name == artifacts.DefaultNamespace || slices.Contains(k, name) {
		return &orkstackv1.Namespace{Name: artifacts.ResolveNamespace(name)}, nil
	}
	return nil, artifacts.ErrNotFound
}
func (k knownNamespaces) List(context.Context) ([]*orkstackv1.Namespace, error) { return nil, nil }
func (k knownNamespaces) ListNames(context.Context) ([]string, error)           { return k, nil }
func (k knownNamespaces) Put(_ context.Context, ns *orkstackv1.Namespace, _ artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	return ns, nil
}
func (k knownNamespaces) Delete(context.Context, string) error { return nil }

// accessWriter is an enforcing service whose document makes alice admin at
// the deployment and nina namespace-admin of payments.
func accessWriter(t *testing.T) (*Service, accessHandler) {
	t.Helper()
	s, h, _ := accessWriterOn(t)
	return s, h
}

// accessWriterOn is accessWriter with the bucket under its store.
func accessWriterOn(t *testing.T) (*Service, accessHandler, *artifacts.MemoryCAS) {
	t.Helper()
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore(), Namespaces: knownNamespaces{"payments", "billing"}})
	a, cas := accessFor(t, authn.ModeEnforce, nil,
		&orkstackv1.Binding{Name: "admins", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:alice"}},
		&orkstackv1.Binding{Name: "payments-admins", Role: authz.RoleNamespaceAdmin, Scope: "namespace:payments", Subjects: []string{"user:corp:nina"}},
	)
	s.access = a
	return s, accessHandler{svc: s}, cas
}

func etagOf(t *testing.T, h accessHandler) (string, int64) {
	t.Helper()
	resp, err := h.GetAccess(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.GetAccessRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg.GetDocument().GetEtag(), resp.Msg.GetDocument().GetRevision()
}

func TestAccessWrite_RoleLifecycle(t *testing.T) {
	s, h := accessWriter(t)
	ctx := asAlice(authn.ModeEnforce)
	etag, rev := etagOf(t, h)
	role := &orkstackv1.Role{Name: "release-manager", Permissions: []string{"orkflow.read", "orkflow.promote"}}
	created, err := h.CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Role: role, Etag: etag}))
	if err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	next, nextRev := etagOf(t, h)
	if created.Msg.GetEtag() != next || next == etag || nextRev != rev+1 {
		t.Fatalf("after a create: etag %q (answered %q, was %q), revision %d (was %d)", next, created.Msg.GetEtag(), etag, nextRev, rev)
	}
	// The replica that wrote it decides from it at once.
	if got := s.access.Policies.Last(); got == nil || got.Revision != nextRev {
		t.Fatalf("the writing replica decides from revision %v, want %d", got, nextRev)
	}
	if _, ok := s.access.Policies.Last().RolePermissions("release-manager"); !ok {
		t.Fatal("the writing replica does not know the role it wrote")
	}
	// A write against an etag the document has moved past changes nothing.
	if _, err := h.UpdateRole(ctx, connect.NewRequest(&orkstackv1.UpdateRoleRequest{Role: role, Etag: etag})); connect.CodeOf(err) != connect.CodeAborted {
		t.Fatalf("a stale etag = %v; want aborted", err)
	}
	// Creating it twice, or a role of a built-in name, is refused.
	if _, err := h.CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Role: role, Etag: next})); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("a second create = %v; want already_exists", err)
	}
	// An unknown permission is refused and writes nothing.
	bad := &orkstackv1.Role{Name: "release-manager", Permissions: []string{"orkflow.teleport"}}
	if _, err := h.UpdateRole(ctx, connect.NewRequest(&orkstackv1.UpdateRoleRequest{Role: bad, Etag: next})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("an unknown permission = %v; want invalid_argument", err)
	}
	if e, _ := etagOf(t, h); e != next {
		t.Fatal("a refused write changed the document")
	}
	// A role a binding names cannot be deleted; unbound, it can.
	if _, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: next,
		Binding: &orkstackv1.Binding{Name: "rm", Role: "release-manager", Scope: "namespace:payments", Subjects: []string{"user:corp:bob"}}})); err != nil {
		t.Fatalf("CreateBinding: %v", err)
	}
	cur, _ := etagOf(t, h)
	if _, err := h.DeleteRole(ctx, connect.NewRequest(&orkstackv1.DeleteRoleRequest{Name: "release-manager", Etag: cur})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("deleting a bound role = %v; want invalid_argument", err)
	}
	if _, err := h.DeleteBinding(ctx, connect.NewRequest(&orkstackv1.DeleteBindingRequest{Name: "rm", Etag: cur})); err != nil {
		t.Fatalf("DeleteBinding: %v", err)
	}
	cur, _ = etagOf(t, h)
	if _, err := h.DeleteRole(ctx, connect.NewRequest(&orkstackv1.DeleteRoleRequest{Name: "release-manager", Etag: cur})); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
	cur, _ = etagOf(t, h)
	if _, err := h.DeleteRole(ctx, connect.NewRequest(&orkstackv1.DeleteRoleRequest{Name: "release-manager", Etag: cur})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("deleting a role that is gone = %v; want not_found", err)
	}
}

// TestAccessWrite_NobodyIsLockedOut: a change that leaves nobody able to
// change access again, with auth.admins empty, is refused.
func TestAccessWrite_NobodyIsLockedOut(t *testing.T) {
	_, h := accessWriter(t)
	etag, _ := etagOf(t, h)
	if _, err := h.DeleteBinding(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.DeleteBindingRequest{Name: "admins", Etag: etag})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("deleting the last admin binding = %v; want invalid_argument", err)
	}
}

// TestAccessWrite_Delegation: access.bind at a namespace creates and
// deletes bindings there of a role its holder holds, and nothing else.
func TestAccessWrite_Delegation(t *testing.T) {
	_, h := accessWriter(t)
	ctx := asNina()
	bindingIn := func(name, role, scope string) *orkstackv1.Binding {
		return &orkstackv1.Binding{Name: name, Role: role, Scope: scope, Subjects: []string{"group:corp:payments-eng"}}
	}
	etag, _ := etagOf(t, h)
	if _, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag,
		Binding: bindingIn("payments-devs", authz.RoleDeveloper, "namespace:payments")})); err != nil {
		t.Fatalf("a namespace admin binding developer in its namespace: %v", err)
	}
	etag, _ = etagOf(t, h)
	for name, b := range map[string]*orkstackv1.Binding{
		"admin":             bindingIn("x", authz.RoleAdmin, "namespace:payments"),
		"another namespace": bindingIn("x", authz.RoleDeveloper, "namespace:billing"),
		"the deployment":    bindingIn("x", authz.RoleViewer, "deployment"),
	} {
		if _, err := h.CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag, Binding: b})); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("a namespace admin binding %s = %v; want permission_denied", name, err)
		}
	}
	// Only access.write replaces a binding, and a delegate deletes only what
	// it could have made.
	if _, err := h.UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Etag: etag,
		Binding: bindingIn("payments-devs", authz.RoleViewer, "namespace:payments")})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("a delegate updating a binding = %v; want permission_denied", err)
	}
	if _, err := h.DeleteBinding(ctx, connect.NewRequest(&orkstackv1.DeleteBindingRequest{Name: "admins", Etag: etag})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("a delegate deleting the deployment's admins = %v; want permission_denied", err)
	}
	if _, err := h.DeleteBinding(ctx, connect.NewRequest(&orkstackv1.DeleteBindingRequest{Name: "payments-devs", Etag: etag})); err != nil {
		t.Errorf("a delegate deleting the binding it made: %v", err)
	}
	// Nobody else writes the document at all.
	if _, err := h.CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Role: &orkstackv1.Role{Name: "r", Permissions: []string{"run.read"}}})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("a delegate creating a role = %v; want permission_denied", err)
	}
}

func TestAccessWrite_ServiceAccountsAndDisabled(t *testing.T) {
	_, h := accessWriter(t)
	ctx := asAlice(authn.ModeEnforce)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	etag, _ := etagOf(t, h)
	sa := &orkstackv1.ServiceAccount{Name: "backup-bot", Keys: []*orkstackv1.ServiceAccountKey{{PublicKeyPem: pemKey}}}
	created, err := h.CreateServiceAccount(ctx, connect.NewRequest(&orkstackv1.CreateServiceAccountRequest{ServiceAccount: sa, Etag: etag}))
	if err != nil {
		t.Fatalf("CreateServiceAccount: %v", err)
	}
	if kid := created.Msg.GetServiceAccount().GetKeys()[0].GetKid(); kid == "" {
		t.Fatal("a stored key has no kid: the thumbprint is the key's name")
	}
	if created.Msg.GetServiceAccount().GetKeys()[0].GetAddedAt() == nil {
		t.Fatal("a stored key records no added_at")
	}
	if _, err := h.CreateServiceAccount(ctx, connect.NewRequest(&orkstackv1.CreateServiceAccountRequest{
		ServiceAccount: &orkstackv1.ServiceAccount{Name: authz.BreakGlassAccount}, Etag: created.Msg.GetEtag()})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("the break-glass name = %v; want invalid_argument", err)
	}
	etag, _ = etagOf(t, h)
	if _, err := h.DeleteServiceAccount(ctx, connect.NewRequest(&orkstackv1.DeleteServiceAccountRequest{Name: "backup-bot", Etag: etag})); err != nil {
		t.Fatalf("DeleteServiceAccount: %v", err)
	}
	etag, _ = etagOf(t, h)
	if _, err := h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: "user:corp:mallory", Disabled: true, Etag: etag})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	etag, _ = etagOf(t, h)
	// Disabling again is a no-op answered with the same etag; enabling
	// removes it.
	again, err := h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: "user:corp:mallory", Disabled: true, Etag: etag}))
	if err != nil || again.Msg.GetEtag() != etag {
		t.Fatalf("disabling twice = %v, etag %q; want a no-op at %q", err, again.Msg.GetEtag(), etag)
	}
	if _, err := h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: "user:corp:mallory", Etag: etag})); err != nil {
		t.Fatalf("enable: %v", err)
	}
}

// TestAccessWrite_TheInterceptorAdmitsADelegate: access.bind admits a
// binding's create and delete in its own namespace at the interceptor, read
// for a delete from the stored binding, and nothing else.
func TestAccessWrite_TheInterceptorAdmitsADelegate(t *testing.T) {
	s, h := accessWriter(t)
	etag, _ := etagOf(t, h)
	if _, err := h.CreateBinding(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.CreateBindingRequest{Etag: etag,
		Binding: &orkstackv1.Binding{Name: "payments-devs", Role: authz.RoleDeveloper, Scope: "namespace:payments", Subjects: []string{"group:corp:eng"}}})); err != nil {
		t.Fatal(err)
	}
	a := s.access
	admitted := func(procedure string, msg proto.Message) bool {
		_, err := a.decide(asNina(), s, procedure, msg)
		return err == nil
	}
	if !admitted(orkstackv1connect.AccessServiceDeleteBindingProcedure, &orkstackv1.DeleteBindingRequest{Name: "payments-devs"}) {
		t.Error("a delegate deleting a binding of its namespace was refused at the interceptor")
	}
	if admitted(orkstackv1connect.AccessServiceDeleteBindingProcedure, &orkstackv1.DeleteBindingRequest{Name: "admins"}) {
		t.Error("a delegate deleting the deployment's admins was admitted")
	}
	if admitted(orkstackv1connect.AccessServiceDeleteBindingProcedure, &orkstackv1.DeleteBindingRequest{Name: "no-such"}) {
		t.Error("a delegate deleting a binding that does not exist was admitted")
	}
	if admitted(orkstackv1connect.AccessServiceCreateBindingProcedure, &orkstackv1.CreateBindingRequest{
		Binding: &orkstackv1.Binding{Name: "x", Role: authz.RoleViewer, Scope: "namespace:billing"}}) {
		t.Error("a delegate binding in another namespace was admitted")
	}
	if admitted(orkstackv1connect.AccessServiceUpdateBindingProcedure, &orkstackv1.UpdateBindingRequest{
		Binding: &orkstackv1.Binding{Name: "payments-devs", Role: authz.RoleViewer, Scope: "namespace:payments"}}) {
		t.Error("a delegate updating a binding was admitted")
	}
}

// TestAccessWrite_IsAuditedWithItsRevision: every write's record names the
// revision it produced.
func TestAccessWrite_IsAuditedWithItsRevision(t *testing.T) {
	s, h := accessWriter(t)
	ctx := asAlice(authn.ModeEnforce)
	etag, rev := etagOf(t, h)
	req := connect.NewRequest(&orkstackv1.CreateRoleRequest{Etag: etag, Role: &orkstackv1.Role{Name: "r", Permissions: []string{"run.read"}}})
	next := func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
		return h.CreateRole(ctx, r.(*connect.Request[orkstackv1.CreateRoleRequest]))
	}
	if _, err := s.auditUnary(ctx, orkstackv1connect.AccessServiceCreateRoleProcedure, req, next); err != nil {
		t.Fatal(err)
	}
	page, err := s.audit.store.ListAuditRecords(context.Background(), artifacts.AuditQuery{
		Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("revision=%d", rev+1)
	if len(page.Records) != 1 || page.Records[0].GetAction() != "access.role.create" || page.Records[0].GetTarget() != "r" ||
		page.Records[0].GetDetail() != want {
		t.Fatalf("records = %v; want one access.role.create of r with detail %q", page.Records, want)
	}
}

// TestAccessWrite_RecoveryCannotBeDisabled: the principals configuration
// makes admin (auth.admins, break-glass) and the caller itself are
// never disabled through the API, so no write ends the way back in
// (security review M1).
func TestAccessWrite_RecoveryCannotBeDisabled(t *testing.T) {
	s, h := accessWriter(t)
	configured, err := authz.CompileConfiguration(authz.Configuration{Admins: []string{"user:corp:root"}, BreakGlass: true,
		Orkers: []authz.OrkerEntry{{Subject: "serviceaccount:orker-*", Pool: "main"}}})
	if err != nil {
		t.Fatal(err)
	}
	s.access.Configured = configured
	s.access.Policies.configured = configured
	ctx := asAlice(authn.ModeEnforce)
	for _, id := range []string{"serviceaccount:" + authz.BreakGlassAccount, "user:corp:root", "user:corp:alice"} {
		etag, _ := etagOf(t, h)
		if _, err := h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: id, Disabled: true, Etag: etag})); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("disabling %s = %v; want invalid_argument", id, err)
		}
	}
	// An Orker bound by auth.orkers is disabled: the first step of the
	// stolen-Orker procedure works however the Orker was bound.
	for _, id := range []string{"serviceaccount:orker-1", "user:corp:mallory"} {
		etag, _ := etagOf(t, h)
		if _, err := h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: id, Disabled: true, Etag: etag})); err != nil {
			t.Fatalf("disabling %s: %v", id, err)
		}
	}
}
