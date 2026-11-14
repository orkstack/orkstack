package authz_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

var update = flag.Bool("update", false, "rewrite testdata golden files")

// renderCatalogue is what TestCatalogue_Golden holds: every permission with
// its scope kinds and the built-in roles that hold it, every built-in role's
// exact set, and the catalogue's count, which docs/ACCESS.md quotes.
func renderCatalogue() string {
	var b strings.Builder
	perms := authz.Permissions()
	fmt.Fprintf(&b, "permissions: %d\n\n", len(perms))
	for _, p := range perms {
		kinds := make([]string, 0, 2)
		for _, k := range authz.ScopeKinds(p) {
			kinds = append(kinds, k.String())
		}
		fmt.Fprintf(&b, "%s [%s] roles=%s\n", p, strings.Join(kinds, ","), strings.Join(authz.RolesHolding(p), ","))
	}
	b.WriteString("\n")
	for _, r := range authz.BuiltInRoles() {
		ps := authz.BuiltInRolePermissions(r)
		names := make([]string, len(ps))
		for i, p := range ps {
			names[i] = string(p)
		}
		fmt.Fprintf(&b, "%s (%d): %s\n", r, len(ps), strings.Join(names, " "))
	}
	return b.String()
}

// TestCatalogue_Golden holds the catalogue and the built-in roles to a
// golden file, so a permission added, moved or granted to a role is seen
// in review. Regenerate with go test ./internal/authz -run Golden -update.
func TestCatalogue_Golden(t *testing.T) {
	path := filepath.Join("testdata", "catalogue.golden")
	got := renderCatalogue()
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to write it)", err)
	}
	if got != string(want) {
		t.Errorf("the catalogue differs from %s; run go test ./internal/authz -run Golden -update and review the diff\n%s", path, got)
	}
}

func TestCatalogue_RolesFollowTheirRules(t *testing.T) {
	admin := authz.BuiltInRolePermissions(authz.RoleAdmin)
	if len(admin) != len(authz.Permissions())-3 {
		t.Errorf("admin holds %d permissions; it holds every one but the Orker's three (%d)", len(admin), len(authz.Permissions())-3)
	}
	for _, p := range []authz.Permission{authz.PermIdentityTokenMint, authz.PermTaskResultCommit, authz.PermPayloadKeyUse} {
		if got := authz.RolesHolding(p); len(got) != 1 || got[0] != authz.RoleOrker {
			t.Errorf("%s is held by %v; only %s holds it", p, got, authz.RoleOrker)
		}
	}
	if got := authz.RolesHolding(authz.PermRunHistoryDecode); len(got) != 1 || got[0] != authz.RoleAdmin {
		t.Errorf("run.history.decode is held by %v; only admin holds it", got)
	}
	if got := authz.RolesHolding(authz.PermNamespacePolicyUpdate); len(got) != 1 || got[0] != authz.RoleAdmin {
		t.Errorf("namespace.policy.update is held by %v; a tenant's admin must not widen its namespace", got)
	}
	auditor := map[authz.Permission]bool{}
	for _, p := range authz.BuiltInRolePermissions(authz.RoleAuditor) {
		auditor[p] = true
	}
	for _, p := range []authz.Permission{authz.PermRunLogsRead, authz.PermRunArtifactsRead} {
		if auditor[p] {
			t.Errorf("auditor holds %s: logs and artifact bytes are above it", p)
		}
	}
	// Every role a role is built on is a subset of it.
	chain := []string{authz.RoleViewer, authz.RoleOperator, authz.RoleDeveloper, authz.RoleNamespaceAdmin}
	for i := 1; i < len(chain); i++ {
		upper := map[authz.Permission]bool{}
		for _, p := range authz.BuiltInRolePermissions(chain[i]) {
			upper[p] = true
		}
		for _, p := range authz.BuiltInRolePermissions(chain[i-1]) {
			if !upper[p] {
				t.Errorf("%s lacks %s, which %s holds", chain[i], p, chain[i-1])
			}
		}
	}
}

func user(issuer, name string, groups ...string) *authz.Subject {
	return &authz.Subject{ID: "user:" + issuer + ":" + name, Kind: "user", Issuer: issuer, Groups: groups}
}

func service(issuer, sub string, claims map[string]string) *authz.Subject {
	return &authz.Subject{ID: "service:" + issuer + ":" + sub, Kind: "service", Issuer: issuer, Claims: claims}
}

func doc(roles []*orkstackv1.Role, bindings ...*orkstackv1.Binding) *orkstackv1.AccessDocument {
	return &orkstackv1.AccessDocument{SchemaVersion: authz.AccessSchemaVersion, Revision: 7, Roles: roles, Bindings: bindings}
}

func bind(name, role, scope string, subjects ...string) *orkstackv1.Binding {
	return &orkstackv1.Binding{Name: name, Role: role, Scope: scope, Subjects: subjects}
}

func mustCompile(t testing.TB, d *orkstackv1.AccessDocument, config authz.Configuration) *authz.Policy {
	t.Helper()
	configured, err := authz.CompileConfiguration(config)
	if err != nil {
		t.Fatal(err)
	}
	p, err := authz.Compile(d, configured, time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAuthorize_Permission(t *testing.T) {
	releaseManager := &orkstackv1.Role{Name: "release-manager", Permissions: []string{"orkflow.read", "orkflow.promote", "namespace.create"}}
	d := doc([]*orkstackv1.Role{releaseManager},
		bind("payments-devs", authz.RoleDeveloper, "namespace:payments", "group:corp:payments-eng"),
		bind("deploy-ci", authz.RoleOperator, "orkflow:payments/deploy", "claims:github:repository=acme/payments,ref=refs/heads/main"),
		bind("everyone-views", authz.RoleViewer, "deployment", "group:corp:staff"),
		bind("alice-deploy", authz.RoleOperator, "orkflow:payments/deploy", "user:corp:alice@acme.dev"),
		bind("releasers", "release-manager", "namespace:payments", "user:corp:rel-*"),
		bind("gpu-orkers", authz.RoleOrker, "pool:gpu", "cert:spiffe://acme.dev/orker/gpu-*"),
		bind("by-sub", authz.RoleViewer, "namespace:search", "claims:cluster:sub=system:serviceaccount:search:indexer"),
	)
	d.Disabled = []string{"user:corp:mallory@acme.dev"}
	policy := mustCompile(t, d, authz.Configuration{Admins: []string{"group:corp:sre"}})

	payments := authz.Resource{Namespace: "payments"}
	deploy := authz.Resource{Namespace: "payments", Orkflow: "deploy"}
	ci := service("github", "repo:acme/payments", map[string]string{"repository": "acme/payments", "ref": "refs/heads/main"})
	cases := []struct {
		name     string
		subject  *authz.Subject
		perm     authz.Permission
		resource authz.Resource
		rule     string // "" means refused
		reason   string
	}{
		{"a group binding at a namespace covers its Orkflows", user("corp", "bob", "payments-eng"), authz.PermOrkflowWrite, deploy, "binding:payments-devs", ""},
		{"and the namespace itself", user("corp", "bob", "payments-eng"), authz.PermRunRead, payments, "binding:payments-devs", ""},
		{"but not another namespace", user("corp", "bob", "payments-eng"), authz.PermRunRead, authz.Resource{Namespace: "search"}, "", authz.ReasonPermissionDenied},
		{"nor the deployment", user("corp", "bob", "payments-eng"), authz.PermNamespaceCreate, authz.Resource{}, "", authz.ReasonPermissionDenied},
		{"a role grants only what it holds", user("corp", "bob", "payments-eng"), authz.PermOrkflowDelete, deploy, "", authz.ReasonPermissionDenied},
		{"the narrowest binding is the rule", user("corp", "alice@acme.dev", "payments-eng"), authz.PermRunSubmit, deploy, "binding:alice-deploy", ""},
		{"an Orkflow binding does not cover its namespace", user("corp", "alice@acme.dev"), authz.PermRunRead, payments, "", authz.ReasonPermissionDenied},
		{"nor another Orkflow", user("corp", "alice@acme.dev"), authz.PermRunRead, authz.Resource{Namespace: "payments", Orkflow: "nightly"}, "", authz.ReasonPermissionDenied},
		{"claims bind a service", ci, authz.PermRunSubmit, deploy, "binding:deploy-ci", ""},
		{"every claim must hold", service("github", "x", map[string]string{"repository": "acme/payments", "ref": "refs/heads/dev"}), authz.PermRunSubmit, deploy, "", authz.ReasonPermissionDenied},
		{"claims bind only their issuer", service("gitlab", "x", ci.Claims), authz.PermRunSubmit, deploy, "", authz.ReasonPermissionDenied},
		{"a claims subject never matches a person", &authz.Subject{ID: "user:github:x", Kind: "user", Issuer: "github", Claims: ci.Claims}, authz.PermRunSubmit, deploy, "", authz.ReasonPermissionDenied},
		{"sub is selectable", service("cluster", "system:serviceaccount:search:indexer", nil), authz.PermRunRead, authz.Resource{Namespace: "search"}, "binding:by-sub", ""},
		{"a deployment binding covers everything it holds", user("corp", "carol", "staff"), authz.PermOrkerRead, authz.Resource{}, "binding:everyone-views", ""},
		{"a viewer reads no logs", user("corp", "carol", "staff"), authz.PermRunLogsRead, deploy, "", authz.ReasonPermissionDenied},
		{"a custom role at a namespace grants no deployment permission", user("corp", "rel-1"), authz.PermNamespaceCreate, authz.Resource{}, "", authz.ReasonPermissionDenied},
		{"a custom role's own permission", user("corp", "rel-1"), authz.PermOrkflowPromote, deploy, "binding:releasers", ""},
		{"a wildcard is a prefix", user("corp", "rel"), authz.PermOrkflowPromote, deploy, "", authz.ReasonPermissionDenied},
		{"a pool binding covers its pool", &authz.Subject{ID: "cert:spiffe://acme.dev/orker/gpu-1", Kind: "service", Issuer: "cert"}, authz.PermIdentityTokenMint, authz.Resource{Pool: "gpu"}, "binding:gpu-orkers", ""},
		{"and no other pool", &authz.Subject{ID: "cert:spiffe://acme.dev/orker/gpu-1", Kind: "service", Issuer: "cert"}, authz.PermIdentityTokenMint, authz.Resource{Pool: "cpu"}, "", authz.ReasonPermissionDenied},
		{"configuration's admins hold admin", user("corp", "dana", "sre"), authz.PermNamespaceDelete, authz.Resource{}, authz.ConfigAdminsRule, ""},
		{"a document binding is narrower than configuration", user("corp", "dana", "sre", "payments-eng"), authz.PermRunRead, payments, "binding:payments-devs", ""},
		{"admin holds no Orker permission", user("corp", "dana", "sre"), authz.PermIdentityTokenMint, authz.Resource{Pool: "gpu"}, "", authz.ReasonPermissionDenied},
		{"a disabled principal is refused whatever it holds", user("corp", "mallory@acme.dev", "sre"), authz.PermRunRead, payments, "", authz.ReasonPrincipalDisabled},
		{"nobody authenticated", nil, authz.PermRunRead, payments, "", authz.ReasonUnauthenticated},
		{"catalog.read is every authenticated principal's", user("corp", "nobody"), authz.PermCatalogRead, authz.Resource{}, authz.RuleAuthenticated, ""},
		{"but not an unauthenticated caller's", nil, authz.PermCatalogRead, authz.Resource{}, "", authz.ReasonUnauthenticated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := authz.Authorize(authz.Request{Action: authz.Action(tc.perm), Subject: tc.subject, Resource: tc.resource, Policy: policy})
			if tc.rule == "" {
				var denied *authz.DeniedError
				if !errors.As(err, &denied) {
					t.Fatalf("Authorize = %+v, %v; want a denial", got, err)
				}
				if denied.Reason != tc.reason || denied.Permission != tc.perm || denied.Scope != tc.resource.String() {
					t.Errorf("denial = %+v; want reason %s for %s at %s", denied, tc.reason, tc.perm, tc.resource)
				}
				if strings.Contains(err.Error(), "binding") {
					t.Errorf("a denial names a binding: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			if got.Rule != tc.rule || got.Scope != tc.resource.String() || got.Generation != 7 || got.Subject != tc.subject.ID {
				t.Errorf("decision = %+v; want rule %s at %s, generation 7", got, tc.rule, tc.resource)
			}
			if got.DocumentReadAt.IsZero() {
				t.Error("a decision does not say when its document was read")
			}
		})
	}
}

func TestAuthorize_NoPolicyIsAnError(t *testing.T) {
	_, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunRead), Subject: user("corp", "a"), Resource: authz.Resource{Namespace: "x"}})
	if !errors.Is(err, authz.ErrNoPolicy) {
		t.Fatalf("Authorize with no policy = %v; want ErrNoPolicy", err)
	}
	var unknown *authz.UnknownActionError
	if _, err := authz.Authorize(authz.Request{Action: "run.everything", Policy: mustCompile(t, nil, authz.Configuration{})}); !errors.As(err, &unknown) {
		t.Fatalf("an unknown permission = %v; want UnknownActionError", err)
	}
}

func TestAuthorize_PinnedBindingMatchesItsIncarnation(t *testing.T) {
	b := bind("pinned", authz.RoleViewer, "namespace:payments", "user:corp:a")
	b.ScopeId = "ns-1"
	policy := mustCompile(t, doc(nil, b), authz.Configuration{})
	for _, tc := range []struct {
		id      string
		allowed bool
	}{{"ns-1", true}, {"", false}, {"ns-2", false}} {
		_, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunRead), Subject: user("corp", "a"),
			Resource: authz.Resource{Namespace: "payments", NamespaceID: tc.id}, Policy: policy})
		if (err == nil) != tc.allowed {
			t.Errorf("namespace id %q: err = %v; want allowed=%v", tc.id, err, tc.allowed)
		}
	}
}

func TestAuthorize_BreakGlassHoldsAdmin(t *testing.T) {
	glass := &authz.Subject{ID: "serviceaccount:" + authz.BreakGlassAccount, Kind: "service", Issuer: "config"}
	off := mustCompile(t, nil, authz.Configuration{})
	if _, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermAccessWrite), Subject: glass, Policy: off}); err == nil {
		t.Fatal("break-glass holds admin with no key configured")
	}
	on := mustCompile(t, nil, authz.Configuration{BreakGlass: true})
	got, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermAccessWrite), Subject: glass, Policy: on})
	if err != nil || got.Rule != authz.BreakGlassRule {
		t.Fatalf("break-glass = %+v, %v; want %s", got, err, authz.BreakGlassRule)
	}
}

func TestByBinding(t *testing.T) {
	b := bind("payments-devs", authz.RoleDeveloper, "namespace:payments", "group:corp:payments-eng", "user:corp:x")
	policy := mustCompile(t, doc(nil, b), authz.Configuration{})
	deploy := authz.Resource{Namespace: "payments", Orkflow: "deploy"}
	first, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunSubmit), Subject: user("corp", "bob", "payments-eng"), Resource: deploy, Policy: policy})
	if err != nil || first.Granted == nil || first.Granted.Subject != "group:corp:payments-eng" {
		t.Fatalf("first decision = %+v, %v", first, err)
	}
	// Later, with no groups read again: the binding still stands.
	if _, err := authz.ByBinding(policy, "user:corp:bob", *first.Granted, authz.PermRunSubmit, deploy); err != nil {
		t.Errorf("ByBinding on a standing binding: %v", err)
	}
	// The group removed from the binding: refused.
	b2 := bind("payments-devs", authz.RoleDeveloper, "namespace:payments", "user:corp:x")
	if _, err := authz.ByBinding(mustCompile(t, doc(nil, b2), authz.Configuration{}), "user:corp:bob", *first.Granted, authz.PermRunSubmit, deploy); err == nil {
		t.Error("ByBinding allowed after the selector was removed")
	}
	// The binding's role lowered to viewer: refused.
	b3 := bind("payments-devs", authz.RoleViewer, "namespace:payments", "group:corp:payments-eng")
	if _, err := authz.ByBinding(mustCompile(t, doc(nil, b3), authz.Configuration{}), "user:corp:bob", *first.Granted, authz.PermRunSubmit, deploy); err == nil {
		t.Error("ByBinding allowed after the role lost the permission")
	}
	// Disabled: refused.
	d := doc(nil, b)
	d.Disabled = []string{"user:corp:bob"}
	_, err = authz.ByBinding(mustCompile(t, d, authz.Configuration{}), "user:corp:bob", *first.Granted, authz.PermRunSubmit, deploy)
	var denied *authz.DeniedError
	if !errors.As(err, &denied) || denied.Reason != authz.ReasonPrincipalDisabled {
		t.Errorf("ByBinding for a disabled principal = %v; want %s", err, authz.ReasonPrincipalDisabled)
	}
	// Gone: refused.
	if _, err := authz.ByBinding(mustCompile(t, nil, authz.Configuration{}), "user:corp:bob", *first.Granted, authz.PermRunSubmit, deploy); err == nil {
		t.Error("ByBinding allowed with the binding gone")
	}
}

func TestParseSubject(t *testing.T) {
	ok := []string{
		"user:corp:alice@acme.dev", "user:corp:rel-*", "group:corp:payments-eng",
		"claims:github:repository=acme/payments,ref=refs/heads/main", "claims:github:repository=acme/*,ref=refs/heads/main",
		"claims:cluster:sub=system:serviceaccount:x:y", "serviceaccount:backup-bot", "cert:spiffe://acme.dev/orker/gpu-*",
		"cert:dns:orker-1.acme.dev",
	}
	for _, s := range ok {
		if _, err := authz.ParseSubject(s); err != nil {
			t.Errorf("ParseSubject(%q) = %v", s, err)
		}
	}
	bad := []string{
		"", "alice", "user:corp", "user:corp:", "user:corp:*", "user:corp:a*b", "group:Corp:x", "claims:github:",
		"claims:github:repository=*", "claims:github:repository=acme/*,ref=refs/*", "claims:github:repository",
		"claims:github:repository=a,repository=b", "serviceaccount:*", "cert:", "role:admin", "user:corp:a\nb",
		"user:" + strings.Repeat("a", 40) + ":x",
	}
	for _, s := range bad {
		if _, err := authz.ParseSubject(s); err == nil {
			t.Errorf("ParseSubject(%q) accepted a subject it must refuse", s)
		}
	}
}

func TestParseScope(t *testing.T) {
	for s, want := range map[string]authz.ScopeKind{
		"deployment": authz.ScopeDeployment, "namespace:payments": authz.ScopeNamespace,
		"orkflow:payments/deploy": authz.ScopeOrkflow, "pool:gpu": authz.ScopePool,
	} {
		got, err := authz.ParseScope(s)
		if err != nil || got.Kind != want || got.String() != s {
			t.Errorf("ParseScope(%q) = %+v, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "Deployment", "namespace:", "namespace:Pay", "orkflow:payments", "orkflow:/x", "pool:", "cluster:x", "namespace:a/b"} {
		if _, err := authz.ParseScope(s); err == nil {
			t.Errorf("ParseScope(%q) accepted a scope it must refuse", s)
		}
	}
}

func publicKeyPEM(t testing.TB) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func TestCompile_Refuses(t *testing.T) {
	admins := bind("admins", authz.RoleAdmin, "deployment", "group:corp:sre")
	key := publicKeyPEM(t)
	cases := map[string]*orkstackv1.AccessDocument{
		"an unknown schema version": {SchemaVersion: 2},
		"an unknown permission":     doc([]*orkstackv1.Role{{Name: "x", Permissions: []string{"run.everything"}}}, admins),
		"a built-in role's name":    doc([]*orkstackv1.Role{{Name: "viewer"}}, admins),
		"a role declared twice":     doc([]*orkstackv1.Role{{Name: "x"}, {Name: "x"}}, admins),
		"an unknown role":           doc(nil, bind("b", "nobody", "deployment", "user:corp:a")),
		"a scope that is not one":   doc(nil, bind("b", authz.RoleViewer, "cluster", "user:corp:a")),
		"a binding with no subject": doc(nil, bind("b", authz.RoleViewer, "deployment")),
		"a subject too wide":        doc(nil, bind("b", authz.RoleViewer, "deployment", "user:corp:*")),
		"a binding declared twice":  doc(nil, admins, admins),
		"orker_ids_any off the orker role": doc(nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "pool:gpu",
			Subjects: []string{"cert:x"}, OrkerIdsAny: true}),
		"a pinned deployment binding": doc(nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "deployment",
			Subjects: []string{"cert:x"}, ScopeId: "x"}),
		"a stored binding from configuration": doc(nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "deployment",
			Subjects: []string{"cert:x"}, FromConfiguration: true}),
		"the break-glass account": {SchemaVersion: 1, ServiceAccounts: []*orkstackv1.ServiceAccount{{Name: authz.BreakGlassAccount}}},
		"a key that does not parse": {SchemaVersion: 1, ServiceAccounts: []*orkstackv1.ServiceAccount{{Name: "bot",
			Keys: []*orkstackv1.ServiceAccountKey{{PublicKeyPem: "nope"}}}}},
		"a kid that is not the thumbprint": {SchemaVersion: 1, ServiceAccounts: []*orkstackv1.ServiceAccount{{Name: "bot",
			Keys: []*orkstackv1.ServiceAccountKey{{PublicKeyPem: key, Kid: "wrong"}}}}},
		"one key twice": {SchemaVersion: 1, ServiceAccounts: []*orkstackv1.ServiceAccount{{Name: "bot",
			Keys: []*orkstackv1.ServiceAccountKey{{PublicKeyPem: key}, {PublicKeyPem: key}}}}},
		"a disabled id that is empty": {SchemaVersion: 1, Disabled: []string{""}},
	}
	for name, d := range cases {
		if _, err := authz.Compile(d, nil, time.Time{}); !errors.Is(err, authz.ErrInvalidDocument) {
			t.Errorf("%s: Compile = %v; want ErrInvalidDocument", name, err)
		}
	}
}

func TestValidate(t *testing.T) {
	issuers := func(name string) (authz.IssuerFacts, bool) {
		if name == "github" {
			return authz.IssuerFacts{Claims: []string{"repository", "ref"}}, true
		}
		return authz.IssuerFacts{}, name == "corp"
	}
	exists := func(ns string) bool { return ns == "payments" }
	admins := bind("admins", authz.RoleAdmin, "deployment", "group:corp:sre")
	opts := authz.ValidateOptions{Issuer: issuers, NamespaceExists: exists}

	if _, err := authz.Validate(doc(nil, admins), opts); err != nil {
		t.Fatalf("a valid document: %v", err)
	}
	if _, err := authz.Validate(doc(nil, bind("v", authz.RoleViewer, "deployment", "user:corp:a")), opts); err == nil {
		t.Error("a document nobody could change access through, with auth.admins empty, was accepted")
	}
	withAdmins := opts
	withAdmins.AdminsConfigured = true
	if _, err := authz.Validate(doc(nil, bind("v", authz.RoleViewer, "deployment", "user:corp:a")), withAdmins); err != nil {
		t.Errorf("the same with auth.admins set: %v", err)
	}
	if _, err := authz.Validate(doc(nil, admins, bind("n", authz.RoleViewer, "namespace:nope", "user:corp:a")), opts); err == nil {
		t.Error("a binding at a namespace that does not exist was accepted")
	}
	warnings, err := authz.Validate(doc(nil, admins, bind("o", authz.RoleViewer, "orkflow:later/x", "user:corp:a"), bind("i", authz.RoleViewer, "deployment", "user:other:a")), opts)
	if err != nil || len(warnings) != 2 {
		t.Errorf("an Orkflow scope in a namespace yet to exist and an untrusted issuer = %v, %v; want two warnings", warnings, err)
	}
	if _, err := authz.Validate(doc(nil, admins, bind("c", authz.RoleViewer, "deployment", "claims:github:environment=prod")), opts); err == nil {
		t.Error("a selector on a claim the issuer does not keep was accepted")
	}
	if _, err := authz.Validate(doc(nil, admins, bind("c", authz.RoleViewer, "deployment", "claims:github:sub=repo:acme/x:ref:main")), opts); err != nil {
		t.Errorf("a selector on sub: %v", err)
	}
}

func FuzzSubject(f *testing.F) {
	for _, s := range []string{"user:corp:alice", "user:corp:a*", "claims:github:repository=acme/*,ref=main", "cert:spiffe://x/*", "serviceaccount:bot", "group:corp:*", "user:corp:a*b"} {
		f.Add(s, "alice", "main")
	}
	f.Fuzz(func(t *testing.T, s, value, claim string) {
		sel, err := authz.ParseSubject(s)
		if err != nil {
			return
		}
		if sel.String() != s {
			t.Fatalf("a selector does not render as written: %q != %q", sel.String(), s)
		}
		// The literal-or-prefix rule, checked against the subject string
		// itself for the kinds whose subject is the principal's id.
		kind, rest, _ := strings.Cut(s, ":")
		if kind == "serviceaccount" || kind == "cert" {
			sub := &authz.Subject{ID: kind + ":" + value, Kind: "service", Issuer: kind}
			want := rest == value || (strings.HasSuffix(rest, "*") && strings.HasPrefix(value, strings.TrimSuffix(rest, "*")))
			if sel.Matches(sub) != want {
				t.Fatalf("%q matches %q = %v; the literal-or-prefix rule says %v", s, sub.ID, !want, want)
			}
		}
		if sel.Matches(nil) {
			t.Fatal("a selector matches no caller")
		}
	})
}

func FuzzScope(f *testing.F) {
	for _, s := range []string{"deployment", "namespace:payments", "orkflow:payments/deploy", "pool:gpu", "orkflow:a/b/c"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		scope, err := authz.ParseScope(s)
		if err != nil {
			return
		}
		if scope.String() != s {
			t.Fatalf("a scope does not render as written: %q != %q", scope.String(), s)
		}
	})
}

// FuzzAuthorize: no policy never allows; adding a binding never turns an
// allow into a denial; a denial names no binding.
func FuzzAuthorize(f *testing.F) {
	f.Add("viewer", "namespace:payments", "group:corp:eng", "eng", "payments", "", "run.read")
	f.Add("admin", "deployment", "user:corp:a", "", "", "", "namespace.create")
	f.Add("orker", "pool:gpu", "cert:x*", "", "", "gpu", "identity.token.mint")
	f.Fuzz(func(t *testing.T, role, scope, subject, group, namespace, pool, perm string) {
		if !authz.Known(authz.Permission(perm)) || strings.Contains(namespace+pool, "zqbind") {
			return
		}
		sub := &authz.Subject{ID: "user:corp:a", Kind: "user", Issuer: "corp", Groups: []string{group}}
		res := authz.Resource{Namespace: namespace, Pool: pool}
		if _, err := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: sub, Resource: res}); err == nil {
			t.Fatal("a permission was allowed with no policy")
		}
		base := doc(nil, bind("zqbind-other", authz.RoleViewer, "namespace:payments", "group:corp:eng"))
		basePolicy, err := authz.Compile(base, nil, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		before, beforeErr := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: sub, Resource: res, Policy: basePolicy})
		grown := doc(nil, base.Bindings[0], bind("zqbind-added", role, scope, subject))
		grownPolicy, err := authz.Compile(grown, nil, time.Time{})
		if err != nil {
			return
		}
		after, afterErr := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: sub, Resource: res, Policy: grownPolicy})
		if beforeErr == nil && afterErr != nil {
			t.Fatalf("adding a binding turned an allow (%+v) into %v", before, afterErr)
		}
		if afterErr != nil && strings.Contains(afterErr.Error(), "zqbind") {
			t.Fatalf("a denial names a binding: %v", afterErr)
		}
		_ = after
	})
}

// TestAuthorize_ThePrincipalsOwnBindingIsGrantedFirst: among bindings of
// one rank, the one that names the principal itself is the granting one,
// then the first by name (authn-authz.md §7).
func TestAuthorize_ThePrincipalsOwnBindingIsGrantedFirst(t *testing.T) {
	policy := mustCompile(t, doc(nil,
		bind("zz-group", authz.RoleViewer, "namespace:payments", "group:corp:eng"),
		bind("mm-group", authz.RoleViewer, "namespace:payments", "group:corp:eng"),
		bind("yy-own", authz.RoleViewer, "namespace:payments", "user:corp:bob"),
	), authz.Configuration{})
	res := authz.Resource{Namespace: "payments"}
	got, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunRead), Subject: user("corp", "bob", "eng"), Resource: res, Policy: policy})
	if err != nil || got.Granted.Name != "yy-own" {
		t.Fatalf("granted = %+v, %v; want the principal's own binding", got.Granted, err)
	}
	got, err = authz.Authorize(authz.Request{Action: authz.Action(authz.PermRunRead), Subject: user("corp", "carol", "eng"), Resource: res, Policy: policy})
	if err != nil || got.Granted.Name != "mm-group" {
		t.Fatalf("granted = %+v, %v; want the first by name", got.Granted, err)
	}
}

func TestCompile_RefusesAStoredPin(t *testing.T) {
	b := bind("b", authz.RoleViewer, "deployment", "user:corp:a")
	b.Pin = orkstackv1.BindingPin_BINDING_PIN_PINNED
	if _, err := authz.Compile(doc(nil, b), nil, time.Time{}); !errors.Is(err, authz.ErrInvalidDocument) {
		t.Fatalf("a stored pin = %v; want refused: pin is output only", err)
	}
}

func TestAuthorize_ConfigurationOnlyGrantsNoCatalogue(t *testing.T) {
	p := mustCompile(t, nil, authz.Configuration{}).ConfigurationOnly()
	if _, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermCatalogRead), Subject: user("corp", "a"), Policy: p}); err == nil {
		t.Fatal("past the stale limit, catalog.read was granted to a principal configuration does not name")
	}
}
