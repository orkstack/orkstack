package config

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestLoad_AuthSection holds the auth.* block to the variables cmd/ork
// reads: the scalars verbatim, admins comma-separated, and auth.issuers a
// document authn.ParseIssuers decodes as it is, with one level of
// `issuers:` and no more. None of it reaches the Orker.
func TestLoad_AuthSection(t *testing.T) {
	path := writeTemp(t, "orkstack.yaml", `auth:
  mode: observe
  public_url: https://ork.example.com
  admins:
    - user:corp:ada
    - group:corp:ops
  break_glass_key_file: /etc/orkstack/break-glass.pub
  clock_skew: 30s
  policy_refresh: 5s
  policy_stale_limit: 2m
  orkers:
    - subject: serviceaccount:orker-main
      pool: main
      orker_ids: any
  issuers:
    corp:
      issuer: https://login.example.com/realms/acme
      kind: user
      audiences:
        - orkstack
      algorithms:
        - RS256
      username_claim: preferred_username
      require_verified_email: false
      max_lifetime: 1h
`)
	doc, err := Load(path, BinaryOrk)
	must.OKf(t, err, "Load")
	want := map[string]string{
		"ORK_AUTH_MODE":                 "observe",
		"ORK_AUTH_PUBLIC_URL":           "https://ork.example.com",
		"ORK_AUTH_ADMINS":               "user:corp:ada,group:corp:ops",
		"ORK_AUTH_BREAK_GLASS_KEY_FILE": "/etc/orkstack/break-glass.pub",
		"ORK_AUTH_CLOCK_SKEW":           "30s",
		"ORK_AUTH_POLICY_REFRESH":       "5s",
		"ORK_AUTH_POLICY_STALE_LIMIT":   "2m",
	}
	for env, expected := range want {
		if got := doc.values[env].value; got != expected {
			t.Errorf("%s = %q, want %q", env, got, expected)
		}
	}
	issuers, err := authn.ParseIssuers([]byte(doc.values["ORK_AUTH_ISSUERS"].value))
	must.OKf(t, err, "authn.ParseIssuers(re-serialized auth.issuers)")
	if _, ok := issuers.ByName("corp"); !ok {
		t.Errorf("issuers: want corp in %q", doc.values["ORK_AUTH_ISSUERS"].value)
	}

	if got := doc.values["ORK_AUTH_ORKERS"].value; !strings.Contains(got, "orkers:") || !strings.Contains(got, "serviceaccount:orker-main") {
		t.Errorf("ORK_AUTH_ORKERS = %q; want the orkers document", got)
	}
	orker, err := Load(path, BinaryOrker)
	must.OKf(t, err, "Load(orker)")
	for env := range want {
		if _, ok := orker.values[env]; ok {
			t.Errorf("%s: set for the Orker, but only ork reads auth.*", env)
		}
	}
}
