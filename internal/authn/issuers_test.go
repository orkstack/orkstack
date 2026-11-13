package authn

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// specExample is authn-authz.md §3.1's example document.
const specExample = `issuers:
  corp:
    issuer: https://login.acme.dev/realms/acme
    kind: user
    audiences:
      - orkstack
      - orkstack-cli
    algorithms:
      - RS256
    username_claim: email
    require_verified_email: true
    groups_claim: groups
    max_lifetime: 24h
    login:
      client_id: orkstack
      client_secret:
        source: file:/etc/orkstack/oidc-client-secret
      cli_client_id: orkstack-cli
      scopes:
        - openid
        - email
        - profile
        - groups
        - offline_access
      device_flow: true
  github:
    issuer: https://token.actions.githubusercontent.com
    kind: service
    audiences:
      - https://ork.acme.dev
    algorithms:
      - RS256
    username_claim: sub
    max_lifetime: 15m
    required_claims:
      repository_owner_id: "1234567"
    claims:
      - repository
      - ref
      - environment
  cluster:
    issuer: https://oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLE
    kind: service
    audiences:
      - orkstack
    algorithms:
      - RS256
    username_claim: sub
    max_lifetime: 1h
    dedicated: true
    orker_id_claim: kubernetes.io/pod/name
`

func TestParseIssuers_TheSpecExample(t *testing.T) {
	set, err := ParseIssuers([]byte(specExample))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(set.Names(), ","); got != "cluster,corp,github" {
		t.Fatalf("names %s", got)
	}
	corp, _ := set.ByName("corp")
	if corp.Login == nil || corp.Login.ClientSecret != "file:/etc/orkstack/oidc-client-secret" || corp.MaxLifetime != 24*time.Hour || corp.MaxStale != DefaultMaxStale {
		t.Fatalf("corp %+v", corp)
	}
	gh, ok := set.ByIssuer("https://token.actions.githubusercontent.com")
	if !ok || gh.RequiredClaims["repository_owner_id"] != "1234567" || gh.Algorithms[0] != jwt.RS256 {
		t.Fatalf("github %+v", gh)
	}
	cluster, _ := set.ByName("cluster")
	if strings.Join(cluster.OrkerIDClaim, "|") != "kubernetes.io|pod|name" {
		t.Fatalf("orker id path %v", cluster.OrkerIDClaim)
	}
	if p := corp.Policy(time.Minute); p.Validate() != nil {
		t.Fatalf("policy %v", p.Validate())
	}
}

func TestParseIssuers_Empty(t *testing.T) {
	for _, raw := range []string{"", "  \n"} {
		set, err := ParseIssuers([]byte(raw))
		if err != nil || set.Len() != 0 {
			t.Fatalf("%q: %d, %v", raw, set.Len(), err)
		}
	}
}

func TestParseIssuers_IntegerOwnerIDIsTheSameRequirement(t *testing.T) {
	doc := strings.Replace(specExample, `repository_owner_id: "1234567"`, `repository_owner_id: 1234567`, 1)
	set, err := ParseIssuers([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	gh, _ := set.ByName("github")
	if gh.RequiredClaims["repository_owner_id"] != "1234567" {
		t.Fatalf("got %q", gh.RequiredClaims["repository_owner_id"])
	}
}

// one is a valid service issuer; each refusal below edits one line of it.
const one = `issuers:
  ci:
    issuer: https://ci.example
    kind: service
    audiences:
      - orkstack
    algorithms:
      - ES256
    max_lifetime: 10m
    required_claims:
      owner: "7"
`

func TestParseIssuers_Refusals(t *testing.T) {
	if _, err := ParseIssuers([]byte(one)); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, tc := range []struct{ name, from, to, want string }{
		{"unknown key", "    kind: service\n", "    kind: service\n    kindd: x\n", "kindd"},
		{"name not a slug", "  ci:\n", "  CI:\n", "slug"},
		{"name too long", "  ci:\n", "  " + strings.Repeat("a", 33) + ":\n", "slug"},
		{"no issuer", "    issuer: https://ci.example\n", "", "issuer is required"},
		{"http issuer", "https://ci.example", "http://ci.example", "https URL"},
		{"issuer with a query", "https://ci.example", "https://ci.example?x=1", "https URL"},
		{"service-account form", "https://ci.example", "serviceaccount:x", "service account"},
		{"no kind", "    kind: service\n", "", "kind is required"},
		{"bad kind", "kind: service", "kind: robot", "not user or service"},
		{"no audience", "    audiences:\n      - orkstack\n", "", "audience"},
		{"no algorithm", "    algorithms:\n      - ES256\n", "", "algorithms is required"},
		{"HS256", "- ES256", "- HS256", "not accepted"},
		{"none", "- ES256", "- none", "not accepted"},
		{"algorithm twice", "      - ES256\n", "      - ES256\n      - ES256\n", "twice"},
		{"no max lifetime", "    max_lifetime: 10m\n", "", "max_lifetime"},
		{"negative max stale", "    max_lifetime: 10m\n", "    max_lifetime: 10m\n    max_stale: -1s\n", "max_stale"},
		{"service with neither", "    required_claims:\n      owner: \"7\"\n", "", "required_claims"},
		{"required claim a boolean", `owner: "7"`, `owner: true`, "required_claims.owner"},
		{"required claim a float", `owner: "7"`, `owner: 1.5`, "required_claims.owner"},
		{"required claim 16 digits", `owner: "7"`, `owner: 1234567890123456`, "required_claims.owner"},
		{"required claim a list", `owner: "7"`, "owner:\n        - a", "required_claims.owner"},
		{"required claim empty", `owner: "7"`, `owner: ""`, "required_claims.owner"},
		{"email without verification", "    kind: service\n", "    kind: service\n    username_claim: email\n", "require_verified_email"},
		{"dedicated public issuer", "https://ci.example", "https://token.actions.githubusercontent.com", "dedicated"},
		{"jwks uri http", "    max_lifetime: 10m\n", "    max_lifetime: 10m\n    jwks_uri: http://ci.example/keys\n", "jwks_uri"},
		{"jwks uri and file", "    max_lifetime: 10m\n", "    max_lifetime: 10m\n    jwks_uri: https://ci.example/keys\n    jwks_file: /k\n", "not both"},
		{"bad orker id path", "    max_lifetime: 10m\n", "    max_lifetime: 10m\n    orker_id_claim: a//b\n", "orker_id_claim"},
		{"login on a service issuer", "    max_lifetime: 10m\n", "    max_lifetime: 10m\n    login:\n      client_id: x\n      scopes:\n        - openid\n", "login is for a user issuer"},
		{"second document", "", "---\nissuers: {}\n", "second YAML document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := one
			if tc.name == "dedicated public issuer" {
				doc = strings.Replace(doc, "    required_claims:\n      owner: \"7\"\n", "    dedicated: true\n", 1)
			}
			if tc.from == "" {
				doc += tc.to
			} else {
				if !strings.Contains(doc, tc.from) {
					t.Fatalf("fixture lacks %q", tc.from)
				}
				doc = strings.Replace(doc, tc.from, tc.to, 1)
			}
			_, err := ParseIssuers([]byte(doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error naming %q", err, tc.want)
			}
		})
	}
}

func TestParseIssuers_LoginAndDedicatedRules(t *testing.T) {
	user := `issuers:
  corp:
    issuer: https://login.example
    kind: user
    audiences:
      - orkstack
    algorithms:
      - RS256
    max_lifetime: 1h
    login:
      client_id: web
      client_secret:
        source: SECRET
      scopes:
        - email
`
	_, err := ParseIssuers([]byte(user))
	if err == nil || !strings.Contains(err.Error(), "env:NAME or file:/path") || !strings.Contains(err.Error(), "openid") {
		t.Fatalf("a literal secret and no openid scope: %v", err)
	}
	// Microsoft is refused only for its common endpoints.
	ms := strings.Replace(one, "https://ci.example", "https://login.microsoftonline.com/common/v2.0", 1)
	ms = strings.Replace(ms, "    required_claims:\n      owner: \"7\"\n", "    dedicated: true\n", 1)
	if _, err := ParseIssuers([]byte(ms)); err == nil {
		t.Fatal("dedicated on Microsoft's common endpoint accepted")
	}
	tenant := strings.Replace(ms, "/common/v2.0", "/11111111-2222-3333-4444-555555555555/v2.0", 1)
	if _, err := ParseIssuers([]byte(tenant)); err != nil {
		t.Fatalf("a single tenant's endpoint: %v", err)
	}
	for _, bypass := range []string{
		"https://login.microsoftonline.com//common/v2.0",
		"https://login.microsoftonline.com/./common/v2.0",
		"https://gitlab.com.",
		"https://GitLab.com",
	} {
		doc := strings.Replace(ms, "https://login.microsoftonline.com/common/v2.0", bypass, 1)
		if _, err := ParseIssuers([]byte(doc)); err == nil {
			t.Errorf("dedicated accepted for %s", bypass)
		}
	}
	for source, ok := range map[string]bool{
		"env:OIDC_SECRET":                    true,
		"file:/etc/orkstack/secret":          true,
		"env:../../etc":                      false,
		"env:":                               false,
		"file:relative/path":                 false,
		"file:/etc/../etc/shadow":            false,
		"file:/" + strings.Repeat("a", 4096): false,
		"vault:secret/x":                     false,
	} {
		doc := strings.Replace(user, "source: SECRET", "source: "+source, 1)
		doc = strings.Replace(doc, "        - email\n", "        - openid\n", 1)
		_, err := ParseIssuers([]byte(doc))
		if (err == nil) != ok {
			t.Errorf("client_secret source %.40q: err %v, want ok=%v", source, err, ok)
		}
	}
	dup := one + `  ci2:
    issuer: https://ci.example
    kind: service
    audiences:
      - orkstack
    algorithms:
      - ES256
    max_lifetime: 10m
    dedicated: true
`
	if _, err := ParseIssuers([]byte(dup)); err == nil || !strings.Contains(err.Error(), "both declare") {
		t.Fatalf("two entries for one iss: %v", err)
	}
}

func FuzzParseIssuers(f *testing.F) {
	f.Add([]byte(specExample))
	f.Add([]byte(one))
	f.Add([]byte("issuers:\n  x: {}\n"))
	f.Add([]byte("issuers: []\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		set, err := ParseIssuers(raw)
		if err != nil {
			return
		}
		for _, name := range set.Names() {
			i, _ := set.ByName(name)
			if i.Policy(time.Minute).Validate() != nil {
				t.Fatalf("issuer %q accepted with an invalid policy", name)
			}
			if i.Kind == KindService && len(i.RequiredClaims) == 0 && !i.Dedicated {
				t.Fatalf("service issuer %q accepted with neither required_claims nor dedicated", name)
			}
			if i.UsernameClaim == "email" && !i.RequireVerifiedEmail {
				t.Fatalf("issuer %q takes an unverified email", name)
			}
			if got, ok := set.ByIssuer(i.Issuer); !ok || got != i {
				t.Fatalf("issuer %q is not found by its iss", name)
			}
		}
	})
}
