package main

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A contract check's tokens are of an issuer nothing resolves, live seconds
// and differ each time, and each check signs with a key of its own.
func TestTestIdentity_SignsForAnIssuerNothingResolves(t *testing.T) {
	ask := &orkstackv1.MintAttemptTokensRequest{Tokens: []*orkstackv1.TokenRequest{{Name: testIdentityName, Audience: "a"}}}
	kids := map[string]bool{}
	seen := map[string]bool{}
	for range 2 {
		local, err := testIdentity()
		must.OK(t, err)
		for range 2 {
			resp, err := local.Mint(context.Background(), ask)
			must.OK(t, err)
			token := resp.GetTokens()[0]
			kids[token.GetKid()], seen[token.GetToken()] = true, true
			payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token.GetToken(), ".")[1])
			must.OK(t, err)
			var claims struct {
				Iss, Aud string
				Iat, Exp int64
			}
			must.OK(t, json.Unmarshal(payload, &claims, json.MatchCaseInsensitiveNames(true)))
			if claims.Iss != "https://orkstack.invalid" || claims.Aud != "a" || claims.Exp-claims.Iat != 15 || token.GetExpiresInSeconds() != 15 {
				t.Fatalf("claims = %+v", claims)
			}
		}
	}
	if len(kids) != 2 || len(seen) != 4 {
		t.Fatalf("%d keys and %d tokens, want 2 and 4", len(kids), len(seen))
	}
}

func TestActivityTest_WithIdentityIsHiddenBehindTheGate(t *testing.T) {
	f := newActivityTestCommand(&globalFlags{}).Flags().Lookup("with-identity")
	if f == nil || f.Hidden == dsl.TaskIdentityAvailable() {
		t.Fatalf("--with-identity: %+v, with task identity available %v", f, dsl.TaskIdentityAvailable())
	}
}
