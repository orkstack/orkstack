package authz_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

func TestSubjectFromID(t *testing.T) {
	for id, want := range map[string]authz.Subject{
		"user:corp:ada":            {ID: "user:corp:ada", Kind: "user", Issuer: "corp"},
		"service:github:repo:x":    {ID: "service:github:repo:x", Kind: "service", Issuer: "github"},
		"serviceaccount:backup":    {ID: "serviceaccount:backup", Kind: "service", Issuer: "serviceaccount"},
		"cert:spiffe://acme/orker": {ID: "cert:spiffe://acme/orker", Kind: "service", Issuer: "cert"},
	} {
		got, err := authz.SubjectFromID(id)
		if err != nil || got.ID != want.ID || got.Kind != want.Kind || got.Issuer != want.Issuer {
			t.Errorf("SubjectFromID(%q) = %+v, %v; want %+v", id, got, err, want)
		}
	}
	for _, bad := range []string{"", "ada", "user:corp", "robot:x:y", "user::ada"} {
		if _, err := authz.SubjectFromID(bad); err == nil {
			t.Errorf("SubjectFromID(%q) accepted", bad)
		}
	}
}

// TestExplain says, of every binding that names the principal or covers the
// resource, why it did or did not grant, and names the one that did.
func TestExplain(t *testing.T) {
	p := mustCompile(t, &orkstackv1.AccessDocument{SchemaVersion: 1,
		Roles: []*orkstackv1.Role{{Name: "promoter", Permissions: []string{"orkflow.promote"}}},
		Bindings: []*orkstackv1.Binding{
			bind("devs", authz.RoleDeveloper, "namespace:payments", "user:corp:ada"),
			bind("readers", authz.RoleViewer, "namespace:payments", "user:corp:bob"),
			bind("elsewhere", authz.RoleAdmin, "namespace:billing", "user:corp:ada"),
			bind("viewers", authz.RoleViewer, "orkflow:payments/deploy", "user:corp:ada"),
		}}, authz.Configuration{})
	ada, _ := authz.SubjectFromID("user:corp:ada")
	res := authz.Resource{Namespace: "payments", Orkflow: "deploy"}
	ex := authz.Explain(p, ada, authz.PermOrkflowPromote, res)
	if ex.Err != nil || ex.GrantedBy == nil || ex.GrantedBy.Binding != "devs" {
		t.Fatalf("Explain = %+v; want granted by devs", ex)
	}
	want := map[string]authz.Match{
		"devs":      authz.MatchGranted,
		"readers":   authz.MatchSubjectDiffers,
		"elsewhere": authz.MatchScopeDiffers,
		"viewers":   authz.MatchRoleLacksPermission,
	}
	if len(ex.Considered) != len(want) {
		t.Fatalf("considered %d bindings, want %d: %+v", len(ex.Considered), len(want), ex.Considered)
	}
	for _, c := range ex.Considered {
		if c.Match != want[c.Binding] {
			t.Errorf("binding %s: %v; want %v", c.Binding, c.Match, want[c.Binding])
		}
	}
	// A denial names no binding that granted, and its reason.
	bob, _ := authz.SubjectFromID("user:corp:bob")
	if ex := authz.Explain(p, bob, authz.PermOrkflowPromote, res); ex.Err == nil || ex.GrantedBy != nil {
		t.Fatalf("Explain for bob = %+v; want denied", ex)
	}
}

// TestHoldings: what a principal holds is each binding one of whose
// subjects matches it, with the permissions that binding grants at its
// scope; a disabled principal holds nothing.
func TestHoldings(t *testing.T) {
	p := mustCompile(t, &orkstackv1.AccessDocument{SchemaVersion: 1,
		Bindings: []*orkstackv1.Binding{
			bind("devs", authz.RoleDeveloper, "namespace:payments", "group:corp:eng"),
			bind("ops", authz.RoleViewer, "deployment", "user:corp:ada"),
			bind("other", authz.RoleAdmin, "deployment", "user:corp:bob"),
		}, Disabled: []string{"user:corp:mallory"}}, authz.Configuration{Admins: []string{"user:corp:root"}})
	ada := &authz.Subject{ID: "user:corp:ada", Kind: "user", Issuer: "corp", Groups: []string{"eng"}}
	got := p.Holdings(ada)
	if len(got) != 2 || got[0].Binding.Binding != "devs" || got[1].Binding.Binding != "ops" {
		t.Fatalf("Holdings(ada) = %+v; want devs and ops", got)
	}
	if got[0].Binding.Subject != "group:corp:eng" || len(got[0].Permissions) == 0 {
		t.Fatalf("devs holding = %+v", got[0])
	}
	for _, perm := range got[0].Permissions {
		if perm == authz.PermSystemRead {
			t.Fatal("a namespace binding lists a deployment permission it cannot grant")
		}
	}
	root := &authz.Subject{ID: "user:corp:root", Kind: "user", Issuer: "corp"}
	if h := p.Holdings(root); len(h) != 1 || h[0].Binding.Binding != authz.ConfigAdminsRule {
		t.Fatalf("Holdings(root) = %+v; want configuration's admins", h)
	}
	if h := p.Holdings(&authz.Subject{ID: "user:corp:mallory", Kind: "user", Issuer: "corp"}); len(h) != 0 {
		t.Fatalf("a disabled principal holds %+v", h)
	}
}
