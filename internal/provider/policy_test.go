package provider_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/internal/scripted"
)

// policyCases are the policy string's table and its fuzz target's seeds.
var policyCases = []struct {
	name      string
	ref       provider.Ref
	principal string
	want      string // "" is a refusal
}{
	{"a secret", provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/payments/db"}, "app", "vault:secret/data/payments/db"},
	{"a secret's field is dropped", provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/payments/db#url"}, "app", "vault:secret/data/payments/db"},
	{"a credential's path", provider.Ref{Kind: provider.KindCredential, Locator: "database/creds/rw"}, "app", "vault:database/creds/rw"},
	{"a login names its principal", provider.Ref{Kind: provider.KindCredential}, "payments-app", "vault:login:payments-app"},
	{"a login as an ARN", provider.Ref{Kind: provider.KindCredential}, "arn:aws:iam::123456789012:role/deploy", "vault:login:arn:aws:iam::123456789012:role/deploy"},
	{"a secret names something", provider.Ref{Kind: provider.KindSecret}, "app", ""},
	{"a login needs a principal", provider.Ref{Kind: provider.KindCredential}, "", ""},
	{"a principal that climbs", provider.Ref{Kind: provider.KindCredential}, "role/../admin", ""},
	{"a principal with a field", provider.Ref{Kind: provider.KindCredential}, "role#x", ""},
	{"a principal that ends a list entry", provider.Ref{Kind: provider.KindCredential}, "role\nenv:X", ""},
	{"a path is not a login", provider.Ref{Kind: provider.KindCredential, Locator: "login:admin"}, "app", ""},
	{"nor is a secret", provider.Ref{Kind: provider.KindSecret, Locator: "login:admin"}, "app", ""},
	{"the login's word elsewhere is a path", provider.Ref{Kind: provider.KindSecret, Locator: "login/admin"}, "app", "vault:login/admin"},
	{"a credential's path has no field", provider.Ref{Kind: provider.KindCredential, Locator: "database/creds/rw#x"}, "app", ""},
	{"a locator that climbs", provider.Ref{Kind: provider.KindSecret, Locator: "a/../b"}, "app", ""},
	{"no kind", provider.Ref{Locator: "a"}, "app", ""},
}

// TestPolicy: one policy string per reference, computed here from the
// reference and the grant's principal, whatever the provider's Parse says.
func TestPolicy(t *testing.T) {
	p := scripted.New("vault", "vault.internal")
	for _, tc := range policyCases {
		got, err := provider.Policy(p, tc.ref, tc.principal)
		if (err == nil) != (tc.want != "") || got != tc.want {
			t.Errorf("%s: Policy = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

// schemed is a provider whose login scheme is another type's.
type schemed struct {
	provider.Provider
	scheme string
}

func (p schemed) LoginScheme() string { return p.scheme }

// FuzzPolicy, for several login schemes: a login's string is exactly
// "<name>:<scheme>:<principal>" and no other reference's begins that way;
// every other string is the provider's name and the locator without its
// field. So two references share a string only where they read one thing.
// Each string climbs nowhere, holds nothing an allow-list gives a meaning,
// and, where a list entry can be that long, admits itself as an entry.
func FuzzPolicy(f *testing.F) {
	for _, tc := range policyCases {
		f.Add(int(tc.ref.Kind), tc.ref.Locator, tc.principal)
	}
	f.Add(int(provider.KindSecret), "sts:arn:aws:iam::1:role/x", "app")
	f.Add(int(provider.KindSecret), "ssm:/payments/license", "app")
	f.Add(int(provider.KindCredential), "auth/token/create", "app")
	f.Fuzz(func(t *testing.T, kind int, locator, principal string) {
		for _, scheme := range []string{"login", "sts", "auth"} {
			p := schemed{scripted.New("vault", "vault.internal"), scheme}
			got, err := provider.Policy(p, provider.Ref{Kind: provider.Kind(kind), Locator: locator}, principal)
			if err != nil {
				if got != "" {
					t.Fatalf("a refusal answered %q", got)
				}
				continue
			}
			prefix := "vault:" + scheme + ":"
			want, login := "vault:"+strings.SplitN(locator, "#", 2)[0], provider.Kind(kind) == provider.KindCredential && locator == ""
			if login {
				want = prefix + principal
			}
			if got != want || strings.HasPrefix(got, prefix) != login {
				t.Fatalf("Policy of kind %d, locator %q, principal %q = %q; want %q", kind, locator, principal, got, want)
			}
			if strings.ContainsAny(got, "#%?*, \t\n\\") {
				t.Fatalf("Policy = %q", got)
			}
			for _, segment := range strings.Split(strings.TrimPrefix(got, "vault:"), "/") {
				if segment == "" || segment == "." || segment == ".." {
					t.Fatalf("Policy = %q has the segment %q", got, segment)
				}
			}
			if len(got) <= dsl.MaxSecretPatternBytes {
				if err := dsl.CheckSecretSource([]string{got}, got); err != nil {
					t.Fatalf("%q does not admit itself as a list entry: %v", got, err)
				}
			}
		}
	})
}
