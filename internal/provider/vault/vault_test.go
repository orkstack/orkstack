package vault

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
)

func declare(t testing.TB, body string) (provider.Provider, error) {
	t.Helper()
	var b strings.Builder
	b.WriteString("providers:\n  vault:\n    type: vault\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		b.WriteString("    " + line + "\n")
	}
	set, err := provider.ParseWith([]byte(b.String()), map[string]provider.Type{"vault": Type{}})
	if err != nil {
		return nil, err
	}
	p, _ := set.ByName("vault")
	return p, nil
}

const declaration = `address: https://vault.internal:8200
audience: vault.internal
kv2_mounts:
  - secret
  - team/kv
`

func testProvider(t testing.TB) provider.Provider {
	t.Helper()
	p, err := declare(t, declaration)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestNew_ReadsADeclaration: what is declared is what an operator is shown,
// and what is left out has its default.
func TestNew_ReadsADeclaration(t *testing.T) {
	p := testProvider(t)
	if p.Name() != "vault" || p.Type() != "vault" || p.Audience() != "vault.internal" || p.LoginScheme() != "auth" {
		t.Fatalf("provider = %q %q %q %q", p.Name(), p.Type(), p.Audience(), p.LoginScheme())
	}
	d := p.Describe()
	if d.Fields["address"] != "https://vault.internal:8200" || d.Fields["auth_mount"] != "jwt" || d.Fields["max_login_ttl"] != "1h0m0s" {
		t.Fatalf("described as %+v", d)
	}
}

func TestNew_RefusesADeclaration(t *testing.T) {
	const ok = "address: https://vault.internal:8200\naudience: vault.internal\n"
	for name, c := range map[string]struct{ body, want string }{
		"no address":              {"audience: a\n", "address"},
		"no audience":             {"address: https://v:8200\n", "audience"},
		"plain http":              {"address: http://v:8200\naudience: a\n", "https"},
		"a path":                  {"address: https://v:8200/v1\naudience: a\n", "address"},
		"a query":                 {"address: https://v:8200?x=1\naudience: a\n", "address"},
		"user information":        {"address: https://u:p@v:8200\naudience: a\n", "address"},
		"no host":                 {"address: https://:8200\naudience: a\n", "address"},
		"a relative CA":           {ok + "ca_file: ca.pem\n", "ca_file"},
		"a mount with a slash":    {ok + "auth_mount: /jwt\n", "auth_mount"},
		"a mount that climbs":     {ok + "auth_mount: a/../b\n", "auth_mount"},
		"an empty mount":          {ok + "auth_mount: \"\"\n", "auth_mount"},
		"a KV mount with a hash":  {ok + "kv2_mounts:\n  - a#b\n", "kv2_mounts"},
		"a KV mount twice":        {ok + "kv2_mounts:\n  - a\n  - a\n", "kv2_mounts"},
		"a KV mount in another":   {ok + "kv2_mounts:\n  - a\n  - a/b\n", "kv2_mounts"},
		"a bound too long":        {ok + "max_login_ttl: 800h\n", "max_login_ttl"},
		"a bound too short":       {ok + "max_login_ttl: 5s\n", "max_login_ttl"},
		"a namespace that climbs": {ok + "vault_namespace: ../x\n", "vault_namespace"},
		"no verification":         {ok + "insecure_skip_verify: true\n", "insecure_skip_verify"},
		"a write mount":           {ok + "write_mounts:\n  pki: pki\n", "write_mounts"},
	} {
		if _, err := declare(t, c.body); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v; want a refusal naming %s", name, err, c.want)
		}
	}
}

func TestParse(t *testing.T) {
	p := testProvider(t)
	secret, credential := provider.KindSecret, provider.KindCredential
	for _, c := range []struct {
		name string
		ref  provider.Ref
		ok   bool
	}{
		{"a KV v1 secret", provider.Ref{Kind: secret, Locator: "kv/payments/db#password"}, true},
		{"a KV v2 secret", provider.Ref{Kind: secret, Locator: "secret/data/payments/db#password"}, true},
		{"the whole of one", provider.Ref{Kind: secret, Locator: "team/kv/data/app"}, true},
		{"a KV v2 path that is not data", provider.Ref{Kind: secret, Locator: "secret/metadata/payments"}, false},
		{"a KV v2 mount itself", provider.Ref{Kind: secret, Locator: "secret"}, false},
		{"a KV v2 data path with no name", provider.Ref{Kind: secret, Locator: "secret/data"}, false},
		{"a mount that only begins alike", provider.Ref{Kind: secret, Locator: "secrets/x"}, true},
		{"no path", provider.Ref{Kind: secret}, false},
		{"a path that climbs", provider.Ref{Kind: secret, Locator: "secret/data/../x"}, false},
		{"a ciphertext", provider.Ref{Kind: secret, Locator: "kv/x", Ciphertext: []byte("x")}, false},
		{"params on a secret", provider.Ref{Kind: secret, Locator: "kv/x", Params: map[string]string{"ttl": "1h"}}, false},
		{"the login itself", provider.Ref{Kind: credential}, true},
		{"a dynamic secret", provider.Ref{Kind: credential, Locator: "database/creds/app"}, true},
		{"a credential that names a field", provider.Ref{Kind: credential, Locator: "database/creds/app#password"}, false},
		{"a credential that is a KV secret", provider.Ref{Kind: credential, Locator: "secret/data/app"}, false},
		{"params on a path that is read", provider.Ref{Kind: credential, Locator: "database/creds/app", Params: map[string]string{"ttl": "1h"}}, false},
		{"params on a login", provider.Ref{Kind: credential, Params: map[string]string{"ttl": "1h"}}, false},
		{"a login spelled as a path", provider.Ref{Kind: secret, Locator: "auth:admin"}, false},
		{"no kind", provider.Ref{Locator: "kv/x"}, false},
	} {
		if _, err := p.Parse(c.ref); (err == nil) != c.ok {
			t.Errorf("%s: Parse = %v, want accepted %v", c.name, err, c.ok)
		}
	}
}

// FuzzVaultLocator: a locator Parse accepts is one the framework's rule
// accepts, its policy string is the provider's name and the path without the
// field, and the path the provider will ask for is under /v1/ exactly as
// written.
func FuzzVaultLocator(f *testing.F) {
	for _, s := range []string{"secret/data/a#b", "database/creds/app", "kv/a", "secret", "a#", "a/../b", "auth:x", "a%2fb", "a?b", ""} {
		f.Add(s, true)
		f.Add(s, false)
	}
	p := testProvider(f)
	f.Fuzz(func(t *testing.T, locator string, asSecret bool) {
		ref := provider.Ref{Kind: provider.KindCredential, Locator: locator}
		if asSecret {
			ref.Kind = provider.KindSecret
		}
		parsed, err := p.Parse(ref)
		if err != nil || locator == "" {
			return
		}
		if err := provider.ValidLocator(locator); err != nil {
			t.Fatalf("Parse accepted %q, which is no locator: %v", locator, err)
		}
		d := parsed.Detail.(detail)
		path, field, _ := strings.Cut(locator, "#")
		if d.path != path || d.field != field {
			t.Fatalf("Parse(%q) = path %q field %q", locator, d.path, d.field)
		}
		policy, err := provider.Policy(p, ref, "app")
		if err != nil || policy != "vault:"+path {
			t.Fatalf("policy of %q = %q, %v; want vault:%s", locator, policy, err, path)
		}
		if u := apiURL("https://v:8200", path); !strings.HasPrefix(u, "https://v:8200/v1/") || strings.ContainsAny(strings.TrimPrefix(u, "https://v:8200"), "?#%") {
			t.Fatalf("the path %q is asked for as %q", path, u)
		}
	})
}
