package authz_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

func binderDoc(bindings ...*orkstackv1.Binding) *orkstackv1.AccessDocument {
	return &orkstackv1.AccessDocument{SchemaVersion: 1,
		Roles:    []*orkstackv1.Role{{Name: "submitter", Permissions: []string{"run.submit"}}, {Name: "reader", Permissions: []string{"run.read"}}},
		Bindings: bindings}
}

// TestGrantingBinding: the binding that let a principal submit is its own
// binding first, then the first by name; the selector that matched, the
// role and the scope are recorded; nothing grants is none.
func TestGrantingBinding(t *testing.T) {
	alice := &authz.Subject{ID: "user:corp:alice", Kind: "user", Issuer: "corp", Groups: []string{"deployers"}}
	deploy := authz.Resource{Namespace: "payments", Orkflow: "deploy"}
	p := mustCompile(t, binderDoc(
		&orkstackv1.Binding{Name: "a-group", Role: "submitter", Scope: "namespace:payments", Subjects: []string{"group:corp:deployers"}},
		&orkstackv1.Binding{Name: "z-own", Role: "submitter", Scope: "orkflow:payments/deploy", Subjects: []string{"user:corp:alice"}},
	), authz.Configuration{})
	got, ok := p.GrantingBinding(alice, authz.PermRunSubmit, deploy)
	if !ok || got != (authz.BinderBinding{Name: "z-own", Selector: "user:corp:alice", Role: "submitter", Scope: "orkflow:payments/deploy"}) {
		t.Fatalf("own binding: %+v %v", got, ok)
	}
	p = mustCompile(t, binderDoc(
		&orkstackv1.Binding{Name: "b-group", Role: "submitter", Scope: "namespace:payments", Subjects: []string{"group:corp:deployers"}},
		&orkstackv1.Binding{Name: "a-reader", Role: "reader", Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}},
	), authz.Configuration{})
	got, ok = p.GrantingBinding(alice, authz.PermRunSubmit, deploy)
	if !ok || got.Name != "b-group" || got.Selector != "group:corp:deployers" {
		t.Fatalf("by group: %+v %v", got, ok)
	}
	if _, ok := p.GrantingBinding(&authz.Subject{ID: "user:corp:bob", Kind: "user", Issuer: "corp"}, authz.PermRunSubmit, deploy); ok {
		t.Fatal("a principal nothing grants has a binding")
	}
}

// TestStillGrants: a recorded binding still grants while it exists, still
// lists the selector and still grants the permission at a covering scope,
// and the binder is not disabled; no group is read again.
func TestStillGrants(t *testing.T) {
	deploy := authz.Resource{Namespace: "payments", Orkflow: "deploy"}
	rec := authz.BinderBinding{Name: "g", Selector: "group:corp:deployers", Role: "submitter", Scope: "namespace:payments"}
	keep := &orkstackv1.Binding{Name: "g", Role: "submitter", Scope: "namespace:payments", Subjects: []string{"group:corp:deployers"}}
	if ok, why := mustCompile(t, binderDoc(keep), authz.Configuration{}).StillGrants(rec, "user:corp:alice", authz.PermRunSubmit, deploy); !ok {
		t.Fatalf("an unchanged binding: %s", why)
	}
	for name, doc := range map[string]*orkstackv1.AccessDocument{
		"removed":          binderDoc(),
		"selector dropped": binderDoc(&orkstackv1.Binding{Name: "g", Role: "submitter", Scope: "namespace:payments", Subjects: []string{"group:corp:others"}}),
		"role reads only":  binderDoc(&orkstackv1.Binding{Name: "g", Role: "reader", Scope: "namespace:payments", Subjects: []string{"group:corp:deployers"}}),
		"scope moved":      binderDoc(&orkstackv1.Binding{Name: "g", Role: "submitter", Scope: "namespace:other", Subjects: []string{"group:corp:deployers"}}),
		"binder disabled": func() *orkstackv1.AccessDocument {
			d := binderDoc(keep)
			d.Disabled = []string{"user:corp:alice"}
			return d
		}(),
	} {
		if ok, why := mustCompile(t, doc, authz.Configuration{}).StillGrants(rec, "user:corp:alice", authz.PermRunSubmit, deploy); ok || why == "" {
			t.Errorf("%s: still grants (%q)", name, why)
		}
	}
}
