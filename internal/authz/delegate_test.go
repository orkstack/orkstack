package authz_test

import (
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

// TestMayDelegate holds access.bind to authn-authz.md §5.4: its holder binds,
// at its namespace or one of its Orkflows, a role whose every permission
// that binding would grant it holds there itself; never admin, never a
// deployment or a pool, never a permission it lacks.
func TestMayDelegate(t *testing.T) {
	doc := &orkstackv1.AccessDocument{SchemaVersion: 1,
		Roles: []*orkstackv1.Role{
			{Name: "deleter", Permissions: []string{"orkflow.read", "orkflow.delete"}},
			{Name: "reader", Permissions: []string{"orkflow.read"}},
			{Name: "watcher", Permissions: []string{"orkflow.read", "system.read"}},
		},
		Bindings: []*orkstackv1.Binding{
			{Name: "nsadmin", Role: authz.RoleNamespaceAdmin, Scope: "namespace:payments", Subjects: []string{"user:corp:nina"}},
			{Name: "dev", Role: authz.RoleDeveloper, Scope: "namespace:payments", Subjects: []string{"user:corp:dev"}},
			{Name: "devbind", Role: "binder", Scope: "namespace:payments", Subjects: []string{"user:corp:dev"}},
		},
	}
	doc.Roles = append(doc.Roles, &orkstackv1.Role{Name: "binder", Permissions: []string{"access.bind"}})
	p := mustCompile(t, doc, authz.Configuration{})
	nina := &authz.Subject{ID: "user:corp:nina", Kind: "user", Issuer: "corp"}
	dev := &authz.Subject{ID: "user:corp:dev", Kind: "user", Issuer: "corp"}
	scope := func(s string) authz.Scope {
		sc, err := authz.ParseScope(s)
		if err != nil {
			t.Fatal(err)
		}
		return sc
	}
	tests := []struct {
		name  string
		sub   *authz.Subject
		role  string
		scope string
		ok    bool
	}{
		{name: "a built-in role it holds, at its namespace", sub: nina, role: authz.RoleDeveloper, scope: "namespace:payments", ok: true},
		{name: "its own role, at an Orkflow of its namespace", sub: nina, role: authz.RoleNamespaceAdmin, scope: "orkflow:payments/deploy", ok: true},
		{name: "a deployment-only permission is inert at a namespace", sub: nina, role: "watcher", scope: "namespace:payments", ok: true},
		{name: "viewer, whose deployment permissions are inert there", sub: nina, role: authz.RoleViewer, scope: "namespace:payments", ok: true},
		{name: "admin", sub: nina, role: authz.RoleAdmin, scope: "namespace:payments"},
		{name: "the deployment", sub: nina, role: "reader", scope: "deployment"},
		{name: "a pool", sub: nina, role: "reader", scope: "pool:gpu"},
		{name: "another namespace", sub: nina, role: "reader", scope: "namespace:billing"},
		{name: "the orker role", sub: nina, role: authz.RoleOrker, scope: "namespace:payments"},
		{name: "a permission the holder lacks", sub: dev, role: "deleter", scope: "namespace:payments"},
		{name: "what the holder holds, with access.bind", sub: dev, role: "reader", scope: "namespace:payments", ok: true},
		{name: "a holder with no access.bind", sub: &authz.Subject{ID: "user:corp:nobody", Kind: "user", Issuer: "corp"}, role: "reader", scope: "namespace:payments"},
		{name: "an unknown role", sub: nina, role: "ghost", scope: "namespace:payments"},
		{name: "nobody", role: "reader", scope: "namespace:payments"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := authz.MayDelegate(p, tt.sub, tt.role, scope(tt.scope))
			if tt.ok && err != nil {
				t.Fatalf("MayDelegate = %v; want allowed", err)
			}
			var denied *authz.DeniedError
			if !tt.ok && !errors.As(err, &denied) {
				t.Fatalf("MayDelegate = %v; want a DeniedError", err)
			}
		})
	}
	// A disabled holder delegates nothing.
	disabled := mustCompile(t, &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: doc.Bindings[:1], Disabled: []string{"user:corp:nina"}}, authz.Configuration{})
	if err := authz.MayDelegate(disabled, nina, authz.RoleViewer, scope("namespace:payments")); err == nil {
		t.Fatal("a disabled holder delegated")
	}
}
