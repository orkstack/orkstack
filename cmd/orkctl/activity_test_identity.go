package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/orker"
)

const (
	// testIdentityIssuer is who a contract check's tokens say issued them: a
	// name nothing resolves, so no relying party can come to trust one.
	testIdentityIssuer = "https://orkstack.invalid"
	testIdentityName   = "contract"
	// testIdentityLife is a contract check's token's life: a refresh comes
	// between a third of it and seven twelfths of it later.
	testIdentityLife = 15 * time.Second
	// testIdentityWait is how many one-second looks the workload takes for
	// its token to change: several refreshes' worth.
	testIdentityWait = 45
)

// testIdentity is the stand-in for a contract check: one identity whose
// tokens a key made for this run signs.
func testIdentity() (orker.LocalIdentity, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return orker.LocalIdentity{}, fmt.Errorf("make the check's signing key: %w", err)
	}
	signer, err := jwt.NewSigner(key)
	if err != nil {
		return orker.LocalIdentity{}, fmt.Errorf("make the check's signer: %w", err)
	}
	return orker.LocalIdentity{
		Identities: []*orkstackv1.IdentityRef{{Name: testIdentityName, Audience: testIdentityIssuer}},
		Mint: func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
			now := time.Now()
			resp := &orkstackv1.MintAttemptTokensResponse{}
			for _, want := range req.GetTokens() {
				jti := rand.Text()
				token, err := signer.Sign("JWT", map[string]any{"iss": testIdentityIssuer, "aud": want.GetAudience(),
					"sub": "orkctl-activity-test", "iat": now.Unix(), "exp": now.Add(testIdentityLife).Unix(), "jti": jti})
				if err != nil {
					return nil, fmt.Errorf("sign the check's token: %w", err)
				}
				resp.Tokens = append(resp.Tokens, &orkstackv1.IssuedToken{Name: want.GetName(), Audience: want.GetAudience(), Token: token,
					ExpiresInSeconds: int32(testIdentityLife / time.Second), IssuedAt: timestamppb.New(now), Jti: jti, Kid: signer.KeyID()})
			}
			return resp, nil
		},
	}, nil
}

// identityProbe is the workload's look, in sh and jq alone: task.json names
// the identity, token and expiration are read through current and by their own
// names alike, and current moves to another token while it runs. It prints no
// token.
var identityProbe = fmt.Sprintf(`
d=/orkstack/identity/%[1]s
command -v jq >/dev/null || { echo "the image has no jq, which this check reads task.json with"; exit 10; }
jq -e --arg p "$d/token" '.paths.identity == "/orkstack/identity" and .identities == [{"name":"%[1]s","audience":"%[2]s","path":$p}]' /orkstack/input/task.json >/dev/null || { echo "task.json does not name the identity"; exit 11; }
cd "$d/current" && first=$(cat token) && expires=$(cat expiration) && [ -n "$first" ] && [ -n "$expires" ] || { echo "current holds no token and expiration"; exit 12; }
cd / && [ -s "$d/token" ] && [ -s "$d/expiration" ] || { echo "token and expiration are not there by their own names"; exit 13; }
(echo x > "$d/token") 2>/dev/null && { echo "the identity directory is writable"; exit 14; }
i=0
while [ "$i" -lt %[3]d ]; do
  [ "$(cat "$d/current/token")" = "$first" ] || { [ -n "$(cat "$d/current/expiration")" ] && exit 0; }
  sleep 1; i=$((i+1))
done
echo "the token never changed"; exit 15
`, testIdentityName, testIdentityIssuer, testIdentityWait)

//  7. An image started with an identity directory reads its token and
//     expiration through `current`, and sees a refresh swap them.
func (s *activitySuite) assertIdentityIsSwapped(ctx context.Context) assertion {
	a := assertion{name: "an identity's token and expiration are read through current, and a refresh swaps them"}
	local, err := testIdentity()
	if err != nil {
		a.err = err
		return a
	}
	// A dispatcher of its own: the identity is this check's alone.
	dispatcher, err := s.newDispatcher()
	if err != nil {
		a.err = err
		return a
	}
	dispatcher.UseLocalIdentity(local)
	in, err := s.input("identity", map[string]any{"op": "exec", "command": []any{"sh", "-c", identityProbe}})
	if err != nil {
		a.err = err
		return a
	}
	// Longer than the looks it takes, whatever --timeout is.
	timeout := max(s.taskTimeout, testIdentityWait*time.Second+30*time.Second)
	in.TimeoutSeconds = int32(timeout / time.Second)
	// What the probe said of its failure: it prints nothing else.
	var (
		mu   sync.Mutex
		said string
	)
	dispatcher.LogSinkObserver = func(_ *orkstackv1.ExecuteTaskInput, line string) {
		mu.Lock()
		defer mu.Unlock()
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "[ork]") {
			said = line
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()
	_, err = dispatcher.Execute(ctx, in)
	mu.Lock()
	defer mu.Unlock()
	switch {
	case err != nil && said != "":
		a.err = fmt.Errorf("%s: %w", said, err)
	case err != nil:
		a.err = fmt.Errorf("the workload did not see its identity files as the contract says: %w", err)
	default:
		a.detail = "both files through current and by name, and a new token under the same name"
	}
	return a
}
