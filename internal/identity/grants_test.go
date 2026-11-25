package identity_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// parseOnly is a declared provider as a submit meets one: it answers its
// name and checks a reference, and is never logged in to. It stands in for
// a provider type, which this build has none of.
type parseOnly struct {
	provider.Provider
	name   string
	refuse string
}

func (p parseOnly) Name() string        { return p.name }
func (p parseOnly) Audience() string    { return "aud-" + p.name }
func (p parseOnly) LoginScheme() string { return "sts" }
func (p parseOnly) Parse(ref provider.Ref) (provider.Parsed, error) {
	if p.refuse != "" && strings.HasPrefix(ref.Locator, p.refuse) {
		return provider.Parsed{}, errors.New("not a path of this type")
	}
	return provider.Parsed{Ref: ref}, nil
}

// declared answers the providers of a deployment that declares aws and vault.
func declared(name string) (provider.Provider, bool) {
	switch name {
	case "aws", "vault":
		return parseOnly{name: name, refuse: "nope/"}, true
	}
	return nil, false
}

func grantAsk() identity.GrantAsk {
	return identity.GrantAsk{
		Namespace: "payments", Owner: "payments", Orkflow: "deploy", Origin: "submit", Providers: declared,
		Current: func() (bool, error) { return true, nil },
		Grants: map[string]artifacts.Grant{
			"deployer": {Provider: "aws", Principal: "arn:aws:iam::1:role/deploy", Orkflows: []string{"deploy"}},
			"reader":   {Provider: "vault", Principal: "payments-app", Orkflows: []string{"*"}},
			"gone":     {Provider: "gcp", Principal: "x", Orkflows: []string{"*"}},
			"people":   {Provider: "aws", Principal: "p", Orkflows: []string{"*"}, Origins: []string{"submit"}},
			"reviewed": {Provider: "aws", Principal: "p", Orkflows: []string{"*"}, CurrentOnly: true},
			"library":  {Provider: "aws", Principal: "p", Orkflows: []string{"*", "platform/deploy"}},
			"climbs":   {Provider: "aws", Principal: "role/../admin", Orkflows: []string{"*"}},
		},
	}
}

func holding(creds []*orkstackv1.CredentialRef, secrets ...*orkstackv1.SecretRef) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "build", Secrets: []*orkstackv1.SecretRef{{Name: "own", Source: "env:OWN"}}},
		{Id: "apply", Credentials: creds, Secrets: secrets}}}
}

func cred(grant, path string) []*orkstackv1.CredentialRef {
	return []*orkstackv1.CredentialRef{{Name: "c", Grant: grant, Path: path}}
}

// TestCheckGrants: each rule of a grant a task names, with its reason, its
// field and what it selects.
func TestCheckGrants(t *testing.T) {
	secret := func(source, grant string) *orkstackv1.SecretRef {
		return &orkstackv1.SecretRef{Name: "s", Source: source, Grant: grant}
	}
	for _, tc := range []struct {
		name   string
		ask    func(*identity.GrantAsk)
		wf     *orkstackv1.Orkflow
		reason string
		field  string
	}{
		{"a login", nil, holding(cred("deployer", "")), "", ""},
		{"a path and a secret of one grant", nil, holding(cred("reader", "database/creds/rw"), secret("vault:secret/data/db#url", "reader")), "", ""},
		{"a grant that is not there", nil, holding(cred("nobody", "")), provider.ReasonGrantNotDefined, "tasks[1].credentials[0].grant"},
		{"a secret's grant that is not there", nil, holding(nil, secret("vault:a/b", "nobody")), provider.ReasonGrantNotDefined, "tasks[1].secrets[0].grant"},
		{"a provider nobody declares", nil, holding(cred("gone", "")), provider.ReasonProviderUnknown, "tasks[1].credentials[0].grant"},
		{"a source of another provider than its grant's", nil, holding(nil, secret("vault:a/b", "deployer")), provider.ReasonGrantNotPermitted, "tasks[1].secrets[0].source"},
		{"a source of no declared provider", nil, holding(nil, secret("gcp:a/b", "reader")), provider.ReasonProviderUnknown, "tasks[1].secrets[0].source"},
		{"an Orkflow the grant does not list", func(a *identity.GrantAsk) { a.Orkflow = "report" }, holding(cred("deployer", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"a shared Orkflow is not under *", func(a *identity.GrantAsk) { a.Owner = "platform" }, holding(cred("people", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"a shared Orkflow listed by its owner", func(a *identity.GrantAsk) { a.Owner = "platform" }, holding(cred("library", "")), "", ""},
		{"the owner's other Orkflow", func(a *identity.GrantAsk) { a.Owner, a.Orkflow = "platform", "other" }, holding(cred("library", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"an origin the grant does not serve", func(a *identity.GrantAsk) { a.Origin = "trigger" }, holding(cred("people", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"a run whose start is not known", func(a *identity.GrantAsk) { a.Origin = "" }, holding(cred("people", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"no origin yet, at create", func(a *identity.GrantAsk) { a.Origin, a.Current = "", nil }, holding(cred("people", "")), "", ""},
		{"a version that is not current", func(a *identity.GrantAsk) { a.Current = func() (bool, error) { return false, nil } }, holding(cred("reviewed", "")), provider.ReasonGrantNotPermitted, "tasks[1].credentials[0].grant"},
		{"no version yet, at create", func(a *identity.GrantAsk) { a.Current = nil }, holding(cred("reviewed", "")), "", ""},
		{"a path its provider refuses", nil, holding(cred("reader", "nope/x")), provider.ReasonProviderRequestInvalid, "tasks[1].credentials[0].path"},
		{"a secret's locator its provider refuses", nil, holding(nil, secret("vault:nope/x", "reader")), provider.ReasonProviderRequestInvalid, "tasks[1].secrets[0].source"},
		{"a principal no policy string can hold", nil, holding(cred("climbs", "")), provider.ReasonProviderRequestInvalid, "tasks[1].credentials[0].grant"},
		{"a login the list admits", func(a *identity.GrantAsk) { a.Allowed = []string{"aws:sts:arn:aws:iam::1:role/*"} }, holding(cred("deployer", "")), "", ""},
		{"a login the list does not", func(a *identity.GrantAsk) { a.Allowed = []string{"env:*"} }, holding(cred("deployer", "")), identity.ReasonReferenceNotAllowed, "tasks[1].credentials[0].grant"},
		{"a secret's field is not judged", func(a *identity.GrantAsk) { a.Allowed = []string{"vault:secret/data/db"} }, holding(nil, secret("vault:secret/data/db#url", "reader")), "", ""},
		{"a path the list does not admit", func(a *identity.GrantAsk) { a.Allowed = []string{"vault:secret/**"} }, holding(cred("reader", "database/creds/rw")), identity.ReasonReferenceNotAllowed, "tasks[1].credentials[0].path"},
	} {
		ask := grantAsk()
		if tc.ask != nil {
			tc.ask(&ask)
		}
		used, err := identity.CheckGrants(ask, tc.wf)
		var refusal *identity.GrantRefusedError
		switch {
		case tc.reason == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.reason == "":
			for name, got := range used {
				g := ask.Grants[name]
				if !reflect.DeepEqual(got, artifacts.GrantSelection{Provider: g.Provider, Principal: g.Principal, Audience: "aud-" + g.Provider}) {
					t.Errorf("%s: grant %s selected %+v", tc.name, name, got)
				}
			}
			if len(used) != 1 {
				t.Errorf("%s: used %v; want the one grant it names", tc.name, used)
			}
		case !errors.As(err, &refusal) || refusal.Reason != tc.reason || refusal.Field() != tc.field || !strings.Contains(err.Error(), tc.reason):
			t.Errorf("%s: %v; want %s at %s", tc.name, err, tc.reason, tc.field)
		case used != nil:
			t.Errorf("%s: a refusal selected %v", tc.name, used)
		}
	}
}

// TestCheckGrants_UsesNone: a definition that names no grant is judged by
// nothing, and a fact that could not be read is never a refusal.
func TestCheckGrants_UsesNone(t *testing.T) {
	if used, err := identity.CheckGrants(identity.GrantAsk{}, holding(nil)); used != nil || err != nil {
		t.Fatalf("no grant: %v, %v", used, err)
	}
	ask, down := grantAsk(), errors.New("the store did not answer")
	ask.Current = func() (bool, error) { return false, down }
	var refusal *identity.GrantRefusedError
	if _, err := identity.CheckGrants(ask, holding(cred("reviewed", ""))); !errors.Is(err, down) || errors.As(err, &refusal) {
		t.Fatalf("an unread current version: %v", err)
	}
	if _, err := identity.CheckGrants(ask, holding(cred("deployer", ""))); err != nil {
		t.Fatalf("a grant that does not ask which version is current: %v", err)
	}
}
