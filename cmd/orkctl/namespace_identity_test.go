package main

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// policyFixture is a fixture whose service keeps identity policies.
func policyFixture(t *testing.T) *fixture {
	t.Helper()
	ids, err := artifacts.NewIncarnationStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return newFixtureWith(t, func(c *api.Config) {
		c.NamespacePolicies, c.IdentityAttemptsMaxExtra = ids, 4
	})
}

// TestNamespaceUpdate_IdentityFlags: the identity flags write a namespace's
// identity policy in an update of its own, and `namespace get` shows it;
// mixing them with a document flag is refused before any request.
func TestNamespaceUpdate_IdentityFlags(t *testing.T) {
	f := policyFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("namespace", "update", "payments",
		"--audience", "https://vault.example", "--audience", "//iam.googleapis.com/projects/1/*",
		"--grant", "deployer=aws:arn:aws:iam::1:role/deploy", "--grant-orkflow", "deployer=deploy",
		"--grant-origin", "deployer=submit", "--grant-current-only", "deployer", "--attempts-extra", "1",
	).requireExit(t, exitOK)
	ns := f.getNamespace(t, "payments")
	id := ns.GetIdentity()
	g := id.GetGrants()["deployer"]
	if len(id.GetAudiences()) != 2 || id.GetAttemptsExtra() != 1 || id.GetRevision() != 1 ||
		g.GetProvider() != "aws" || g.GetPrincipal() != "arn:aws:iam::1:role/deploy" || !g.GetCurrentOnly() ||
		len(g.GetOrkflows()) != 1 || g.GetOrigins()[0] != "submit" {
		t.Fatalf("the policy = %v", id)
	}
	f.run("namespace", "get", "payments").requireContains(t, "deployer", "aws:arn:aws:iam::1:role/deploy", "https://vault.example")

	f.run("namespace", "update", "payments", "--remove-grant", "deployer", "--clear-audiences", "--clear-attempts-extra").requireExit(t, exitOK)
	if id := f.getNamespace(t, "payments").GetIdentity(); len(id.GetGrants())+len(id.GetAudiences()) != 0 || id.AttemptsExtra != nil || id.GetRevision() != 2 {
		t.Fatalf("after removing it all = %v", id)
	}

	before := len(f.requests())
	mixed := f.run("namespace", "update", "payments", "--audience", "a", "--description", "x")
	if mixed.code == exitOK {
		t.Fatal("an identity flag beside a document flag succeeded")
	}
	mixed.requireStderrContains(t, "--description", "update of its own")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a mixed update reached the server: %v", after[before:])
	}
	for _, bad := range [][]string{
		{"--grant", "deployer"},
		{"--grant-orkflow", "nobody=deploy"},
		{"--audience", "a", "--clear-audiences"},
		{"--attempts-extra", "1", "--clear-attempts-extra"},
	} {
		if r := f.run(append([]string{"namespace", "update", "payments"}, bad...)...); r.code == exitOK {
			t.Errorf("%v succeeded", bad)
		}
	}
}

// getNamespace reads a namespace through the fixture's server.
func (f *fixture) getNamespace(t *testing.T, name string) *orkstackv1.Namespace {
	t.Helper()
	r := f.run("--json", "namespace", "get", name)
	r.requireExit(t, exitOK)
	var got orkstackv1.GetNamespaceResponse
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(r.stdout), &got); err != nil {
		t.Fatalf("decode %q: %v", r.stdout, err)
	}
	return got.GetNamespace()
}

var _ = connect.CodeOf
