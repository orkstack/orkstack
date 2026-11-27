//go:build integration

package vault

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/providertest"
	"github.com/orkstack/orkstack/internal/testsupport/dextest"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/vaulttest"
)

const testIssuer = "https://orkstack.test"

// stand is a real Vault this test owns, with a JWT auth mount that trusts an
// issuer key of the test's, KV mounts of both versions, and roles.
type stand struct {
	t      *testing.T
	ctx    context.Context
	v      *vaulttest.Vault
	proxy  *faultproxy.Proxy
	signer *jwt.Signer
}

func newStand(t *testing.T) *stand { return newStandOf(t, false) }

// newStandOf is newStand, with Vault's leased KV in place of both KV versions
// when leased.
func newStandOf(t *testing.T, leased bool) *stand {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	wd, err := os.Getwd()
	must.OK(t, err)
	root, err := dextest.RepoRoot(wd)
	must.OK(t, err)
	id := os.Getenv(proof.EnvID)
	if id == "" {
		id = "by-hand"
	}
	v, err := vaulttest.Start(ctx, vaulttest.Options{Root: root, Dir: t.TempDir(), ProofID: id, Owner: dextest.ProcessOwner(), LeasedKV: leased})
	if errors.Is(err, vaulttest.ErrNoImage) && os.Getenv("ORK_REQUIRE_INFRA") != "1" {
		t.Skip(err) // under ORK_REQUIRE_INFRA=1 it fails below
	}
	must.OK(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			if out, err := exec.Command("docker", "logs", "--tail", "60", v.ID).CombinedOutput(); err == nil {
				t.Logf("Vault's log:\n%s", out)
			}
		}
		stop, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := v.Stop(stop); err != nil {
			t.Error(err)
		}
	})
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	signer, err := jwt.NewSigner(crypto.Signer(key))
	must.OK(t, err)
	der, err := x509.MarshalPKIXPublicKey(key.Public())
	must.OK(t, err)
	public := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	s := &stand{t: t, ctx: ctx, v: v, proxy: faultproxy.New(t, v.Addr), signer: signer}
	s.do("POST", "sys/auth/jwt", map[string]any{"type": "jwt"})
	s.do("POST", "auth/jwt/config", map[string]any{"jwt_validation_pubkeys": []string{public}, "bound_issuer": testIssuer})
	s.do("POST", "sys/mounts/kv", map[string]any{"type": "kv", "options": map[string]string{"version": "1"}})
	s.do("POST", "kv/app", map[string]any{"password": "kv1-password-value"})
	if !leased {
		s.do("POST", "secret/data/app", map[string]any{"data": map[string]string{"password": "kv2-password-value"}})
	}
	s.do("PUT", "sys/policies/acl/app", map[string]string{"policy": `path "kv/*" { capabilities = ["read"] }
path "secret/data/*" { capabilities = ["read"] }`})
	s.role("app", map[string]any{"token_ttl": "10m", "token_policies": []string{"app"}})
	return s
}

func (s *stand) do(method, path string, in any) {
	s.t.Helper()
	_, err := s.v.Do(s.ctx, method, path, in)
	must.OKf(s.t, err, "%s %s", method, path)
}

func (s *stand) role(name string, more map[string]any) {
	s.t.Helper()
	role := map[string]any{"role_type": "jwt", "user_claim": "sub", "bound_audiences": []string{"vault.internal"}, "bound_subject": "orkstack:run:payments"}
	for k, v := range more {
		role[k] = v
	}
	s.do("POST", "auth/jwt/role/"+name, role)
}

func (s *stand) token(context.Context) (string, time.Duration, error) {
	now := time.Now()
	tok, err := s.signer.Sign("JWT", map[string]any{"iss": testIssuer, "sub": "orkstack:run:payments", "aud": "vault.internal",
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(10 * time.Minute).Unix()})
	return tok, 10 * time.Minute, err
}

func (s *stand) provider() provider.Provider {
	s.t.Helper()
	p, err := declare(s.t, "address: https://"+s.proxy.Addr()+"\naudience: vault.internal\nca_file: "+s.v.CAFile+"\nkv2_mounts:\n  - secret\n")
	must.OK(s.t, err)
	return p
}

func (s *stand) login(role string) (*session, error) {
	got, err := s.provider().Login(s.ctx, role, provider.Attempt{Namespace: "payments", RunID: "ork-x", TaskID: "t", Attempt: 1}, s.token)
	if err != nil {
		return nil, err
	}
	return got.(*session), nil
}

// outage cuts the way to Vault until restore is called.
func (s *stand) outage(*testing.T) func() {
	s.proxy.Cut()
	return s.proxy.Restore
}

// throttle has Vault's own rate limit refuse logins until restore is called:
// one request a minute, and this makes it.
func (s *stand) throttle(t *testing.T) func() {
	s.do("POST", "sys/quotas/rate-limit/login", map[string]any{"path": "auth/jwt/", "rate": 1, "interval": "1m"})
	tok, _, err := s.token(s.ctx)
	must.OK(t, err)
	_, _ = s.v.As(s.ctx, "", http.MethodPost, "auth/jwt/login", map[string]string{"role": "app", "jwt": tok})
	return func() { s.do("DELETE", "sys/quotas/rate-limit/login", nil) }
}

// aged waits until Vault itself says that life, asked by path and field, has
// shrunk by five seconds: more than its rounding, so a renewal that gives it
// back is one, and a renewal that does not is not.
func (s *stand) aged(method, path, field string, in any, life time.Duration) {
	s.t.Helper()
	deadline := time.After(time.Minute)
	for {
		raw, err := s.v.Do(s.ctx, method, path, in)
		must.OKf(s.t, err, "%s %s", method, path)
		var answer struct {
			Data map[string]jsontext.Value `json:"data"`
		}
		must.OK(s.t, json.Unmarshal(raw, &answer))
		var left int64
		must.OKf(s.t, json.Unmarshal(answer.Data[field], &left), "%s of %s", field, raw)
		if left > 0 && time.Duration(left)*time.Second <= life-5*time.Second {
			return
		}
		select {
		case <-deadline:
			s.t.Fatalf("%s stays at %ds", path, left)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *stand) loginAged(l *login) {
	s.t.Helper()
	s.aged("POST", "auth/token/lookup-accessor", "ttl", map[string]string{"accessor": l.accessor}, l.ttl)
}

func (s *stand) leaseAged(b provider.Bundle) {
	s.t.Helper()
	s.aged("PUT", "sys/leases/lookup", "ttl", map[string]string{"lease_id": b.Handle.(lease).id}, b.Lifetime)
}

// live reports whether Vault still knows token.
func (s *stand) live(token string) bool {
	_, err := s.v.As(s.ctx, token, http.MethodGet, "auth/token/lookup-self", nil)
	var status *vaulttest.StatusError
	if err != nil && (!errors.As(err, &status) || status.Code != http.StatusForbidden) {
		s.t.Fatalf("look a token up: %v", err)
	}
	return err == nil
}

// revokeWait bounds how long a closed login's leases may take to go. Vault
// revokes a token at once but only marks its leases expired; its lease
// manager revokes them a moment later (RevokeByToken's lazy revoke), so the
// promise is eventual and this is its deadline, not a pause.
const revokeWait = 30 * time.Second

// leaseRevoked reports whether Vault stops holding a bundle's lease within
// revokeWait, asking again until it does. An answer other than the 400 of a
// lease Vault does not hold is asked again too, and logged if the wait ends.
func (s *stand) leaseRevoked(t *testing.T, b provider.Bundle) bool {
	t.Helper()
	deadline := time.Now().Add(revokeWait)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var last error
	for {
		_, err := s.v.Do(s.ctx, http.MethodPut, "sys/leases/lookup", map[string]string{"lease_id": b.Handle.(lease).id})
		var status *vaulttest.StatusError
		if errors.As(err, &status) && status.Code == http.StatusBadRequest {
			return true
		}
		last = err
		if time.Now().After(deadline) {
			if last != nil {
				t.Logf("the last lookup of the lease: %v", last)
			}
			return false
		}
		select {
		case <-s.ctx.Done():
			t.Logf("the wait for the lease ended: %v", s.ctx.Err())
			return false
		case <-tick.C:
		}
	}
}

// TestIntegration_VaultConformance: the shared suite, against a real Vault
// that verifies the attempt's token itself.
func TestIntegration_VaultConformance(t *testing.T) {
	s := newStand(t)
	providertest.Conformance(t, providertest.Harness{
		Provider: s.provider(), Principal: "app", Refused: "no-such-role", Token: s.token,
		Attempt:     provider.Attempt{Namespace: "payments", RunID: "ork-x", TaskID: "t", Attempt: 1},
		Secret:      &provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/app#password"},
		SecretValue: []byte("kv2-password-value"),
		Missing:     &provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/nothing#password"},
		Credential:  &provider.Ref{Kind: provider.KindCredential},
		Expires:     true,
		Outage:      s.outage,
		Throttle:    s.throttle,
		Revoked:     func(_ *testing.T, b provider.Bundle) bool { return !s.live(string(b.Files[tokenFile])) },
	})
}

// TestIntegration_VaultVerifiesTheAttemptsToken: a token for another
// audience, another subject or another key is refused by Vault itself.
func TestIntegration_VaultVerifiesTheAttemptsToken(t *testing.T) {
	s := newStand(t)
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	stranger, err := jwt.NewSigner(other)
	must.OK(t, err)
	now := time.Now()
	claims := func(aud, sub string) map[string]any {
		return map[string]any{"iss": testIssuer, "sub": sub, "aud": aud, "iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Minute).Unix()}
	}
	for name, c := range map[string]struct {
		by     *jwt.Signer
		claims map[string]any
	}{
		"another audience": {s.signer, claims("sts.amazonaws.com", "orkstack:run:payments")},
		"another subject":  {s.signer, claims("vault.internal", "orkstack:run:billing")},
		"another key":      {stranger, claims("vault.internal", "orkstack:run:payments")},
	} {
		tok, err := c.by.Sign("JWT", c.claims)
		must.OK(t, err)
		_, err = s.provider().Login(s.ctx, "app", provider.Attempt{}, func(context.Context) (string, time.Duration, error) { return tok, time.Minute, nil })
		if err == nil || provider.ClassOf(err) != provider.Refused {
			t.Errorf("%s: login = %v; want refused", name, err)
		}
	}
	got, err := s.login("app")
	must.OKf(t, err, "the control: the attempt's own token")
	must.OK(t, got.Close(s.ctx))
}

// TestIntegration_VaultReadsBothKVVersions: a field and a whole value of
// each, a missing field, and a path the policy does not cover.
func TestIntegration_VaultReadsBothKVVersions(t *testing.T) {
	s := newStand(t)
	p := s.provider()
	got, err := s.login("app")
	must.OK(t, err)
	defer func() { must.OK(t, got.Close(s.ctx)) }()
	for locator, want := range map[string]string{
		"kv/app#password":          "kv1-password-value",
		"secret/data/app#password": "kv2-password-value",
		"kv/app":                   `{"password":"kv1-password-value"}`,
		"secret/data/app":          `{"password":"kv2-password-value"}`,
	} {
		v, err := got.Secret(s.ctx, parsed(t, p, provider.KindSecret, locator))
		if err != nil || string(v.Bytes) != want {
			t.Errorf("%s = %q, %v; want %q", locator, v.Bytes, err, want)
		}
	}
	for locator, want := range map[string]provider.Class{
		"kv/app#user":            provider.NotFound,
		"kv/nothing#password":    provider.NotFound,
		"cubby/app#password":     provider.Refused,
		"secret/data/app#nobody": provider.NotFound,
	} {
		if _, err := got.Secret(s.ctx, parsed(t, p, provider.KindSecret, locator)); err == nil || provider.ClassOf(err) != want {
			t.Errorf("%s = %v; want %s", locator, err, want)
		}
	}
}

// TestIntegration_VaultLoginBound: a role with no TTL gets Vault's 768 h, so
// its login is revoked and refused with both numbers; a batch token is
// refused; a periodic role inside the bound is not.
func TestIntegration_VaultLoginBound(t *testing.T) {
	s := newStand(t)
	s.role("long", nil)
	s.role("batch", map[string]any{"token_type": "batch", "token_ttl": "10m"})
	s.role("brief", map[string]any{"token_ttl": "40s"})
	brief, err := s.login("brief")
	must.OKf(t, err, "a login of 40 s, which may read")
	if _, err := brief.Credential(s.ctx, parsed(t, brief.p, provider.KindCredential, "")); !is(err, "login-ttl-40s-under-45s") || provider.ClassOf(err) != provider.Refused || len(brief.logins) != 1 {
		t.Errorf("a login of 40 s as a credential = %v after %d logins; want it refused with none made again", err, len(brief.logins))
	}
	must.OK(t, brief.Close(s.ctx))
	s.role("periodic", map[string]any{"token_period": "10m"})
	s.role("explicit", map[string]any{"token_ttl": "10m", "token_explicit_max_ttl": "1h"})
	for role, refused := range map[string]bool{"periodic": true, "app": true, "explicit": false} {
		instance, err := s.provider().Login(s.ctx, role, provider.Attempt{Instance: true, MaxLogin: time.Hour}, s.token)
		if refused != is(err, "vault-token-unbounded") || (err == nil) == refused {
			t.Errorf("an instance's login as %s = %v; want refused %v", role, err, refused)
		}
		if err == nil {
			must.OK(t, instance.Close(s.ctx))
		}
	}
	s.role("bare", map[string]any{"token_ttl": "10m", "token_policies": []string{"app"}, "token_no_default_policy": true})
	if _, err := s.login("bare"); !is(err, "login-cannot-be-revoked") || provider.ClassOf(err) != provider.Refused {
		t.Errorf("a role without the default policy = %v", err)
	}
	if _, err := s.login("long"); !is(err, "login-ttl-768h0m0s-over-1h0m0s") || provider.ClassOf(err) != provider.Refused {
		t.Errorf("a role with no TTL = %v", err)
	}
	control, err := s.login("app")
	must.OK(t, err)
	defer func() { must.OK(t, control.Close(s.ctx)) }()
	raw, err := s.v.Do(s.ctx, "LIST", "auth/token/accessors", nil)
	must.OK(t, err)
	var accessors struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	must.OK(t, json.Unmarshal(raw, &accessors))
	alive := 0
	for _, accessor := range accessors.Data.Keys {
		raw, err := s.v.Do(s.ctx, "POST", "auth/token/lookup-accessor", map[string]string{"accessor": accessor})
		must.OK(t, err)
		if strings.Contains(string(raw), `"role":"long"`) {
			t.Error("the over-long login still lives")
		}
		if strings.Contains(string(raw), `"role":"app"`) {
			alive++
		}
	}
	if alive != 1 {
		t.Errorf("%d logins as app are listed, want the control's one", alive)
	}
	if _, err := s.login("batch"); !is(err, "batch-token") {
		t.Errorf("a batch role = %v", err)
	}
	got, err := s.login("periodic")
	must.OK(t, err)
	if d := got.Lifetime(); d <= 9*time.Minute || d > 10*time.Minute {
		t.Errorf("a 10 m period's lifetime = %s", d)
	}
	s.loginAged(got.logins[0])
	if d, err := got.RenewLogin(s.ctx); err != nil || d != 10*time.Minute {
		t.Errorf("renewing a periodic login = %s, %v; want its period", d, err)
	}
	must.OK(t, got.Close(s.ctx))
}

// TestIntegration_VaultRenewsALoginToItsMaximum: a login is renewed while
// its maximum lets it be, and then is asked for anew.
func TestIntegration_VaultRenewsALoginToItsMaximum(t *testing.T) {
	s := newStand(t)
	s.role("renewable", map[string]any{"token_ttl": "10m", "token_max_ttl": "1h"})
	s.role("capped", map[string]any{"token_ttl": "10m", "token_max_ttl": "10m"})
	got, err := s.login("renewable")
	must.OK(t, err)
	s.loginAged(got.logins[0])
	if d, err := got.RenewLogin(s.ctx); err != nil || d != 10*time.Minute {
		t.Errorf("renewed for %s, %v; want 10m", d, err)
	}
	must.OK(t, got.Close(s.ctx))
	capped, err := s.login("capped")
	must.OK(t, err)
	s.loginAged(capped.logins[0])
	if d, err := capped.RenewLogin(s.ctx); !errors.Is(err, provider.ErrReissue) {
		t.Errorf("a login at its maximum renewed for %s, %v; want it issued again", d, err)
	}
	must.OK(t, capped.Close(s.ctx))
}

// TestIntegration_VaultLoginRevokedByAnOperator: a session whose token was
// revoked reads on through one more login, and its close revokes that one.
func TestIntegration_VaultLoginRevokedByAnOperator(t *testing.T) {
	s := newStand(t)
	p := s.provider()
	got, err := s.login("app")
	must.OK(t, err)
	ref := parsed(t, p, provider.KindSecret, "kv/app#password")
	_, err = got.Secret(s.ctx, ref)
	must.OK(t, err)
	first := got.logins[0].token
	s.do("POST", "auth/token/revoke", map[string]string{"token": first})
	if v, err := got.Secret(s.ctx, ref); err != nil || string(v.Bytes) != "kv1-password-value" || len(got.logins) != 2 {
		t.Fatalf("a read after the revoke = %q, %v, %d logins", v.Bytes, err, len(got.logins))
	}
	second := got.logins[1].token
	if s.live(first) || !s.live(second) {
		t.Fatal("the first token lives, or the second does not")
	}
	must.OK(t, got.Close(s.ctx))
	if s.live(second) {
		t.Error("the second login outlived the session")
	}
}

// leaseLives reports whether Vault still holds a bundle's lease.
func (s *stand) leaseLives(b provider.Bundle) bool {
	_, err := s.v.Do(s.ctx, http.MethodPut, "sys/leases/lookup", map[string]string{"lease_id": b.Handle.(lease).id})
	var status *vaulttest.StatusError
	if err != nil && (!errors.As(err, &status) || status.Code != http.StatusBadRequest) {
		s.t.Fatalf("look a lease up: %v; a lease Vault does not hold is a 400", err)
	}
	return err == nil
}

// TestIntegration_VaultLeases: a credential Vault leases is issued, renewed
// while its maximum lets it be, asked for anew when it does not, and revoked
// with the login that made it, all by Vault's own lease manager.
func TestIntegration_VaultLeases(t *testing.T) {
	s := newStandOf(t, true)
	s.do("POST", "kv/creds/app", map[string]string{"username": "u-app", "password": "leased-password-value", "ttl": "5m"})
	s.do("POST", "sys/mounts/capped", map[string]any{"type": "kv", "config": map[string]string{"max_lease_ttl": "5m"}})
	s.do("POST", "capped/app", map[string]string{"password": "capped-password-value", "ttl": "5m"})
	s.do("PUT", "sys/policies/acl/app", map[string]string{"policy": `path "kv/*" { capabilities = ["read"] }
path "capped/*" { capabilities = ["read"] }`})
	p, err := declare(t, "address: https://"+s.proxy.Addr()+"\naudience: vault.internal\nca_file: "+s.v.CAFile+"\n")
	must.OK(t, err)

	t.Run("Conformance", func(t *testing.T) {
		providertest.Conformance(t, providertest.Harness{
			Provider: p, Principal: "app", Refused: "no-such-role", Token: s.token,
			Secret:      &provider.Ref{Kind: provider.KindSecret, Locator: "kv/app#password"},
			SecretValue: []byte("kv1-password-value"),
			Credential:  &provider.Ref{Kind: provider.KindCredential, Locator: "kv/creds/app"},
			Expires:     true,
			Values:      func() [][]byte { return [][]byte{[]byte("leased-password-value")} },
			Revoked:     func(t *testing.T, b provider.Bundle) bool { return s.leaseRevoked(t, b) },
			Outage:      s.outage,
			Throttle:    s.throttle,
		})
	})

	got, err := p.Login(s.ctx, "app", provider.Attempt{}, s.token)
	must.OK(t, err)
	b, err := got.Credential(s.ctx, parsed(t, p, provider.KindCredential, "kv/creds/app"))
	must.OK(t, err)
	if string(b.Files["password"]) != "leased-password-value" || b.Lifetime != 5*time.Minute || !s.leaseLives(b) {
		t.Fatalf("bundle of %s, %d files; want a 5m lease Vault holds", b.Lifetime, len(b.Files))
	}
	s.leaseAged(b)
	renewed, err := got.Renew(s.ctx, b)
	must.OKf(t, err, "renew a lease under its maximum")
	if renewed.Lifetime != 5*time.Minute || renewed.Handle.(lease).id != b.Handle.(lease).id || string(renewed.Files["password"]) != "leased-password-value" {
		t.Errorf("renewed for %s as %+v; want the same lease and files for 5m", renewed.Lifetime, renewed.Handle)
	}
	capped, err := got.Credential(s.ctx, parsed(t, p, provider.KindCredential, "capped/app"))
	must.OK(t, err)
	s.leaseAged(capped)
	if r, err := got.Renew(s.ctx, capped); !errors.Is(err, provider.ErrReissue) {
		t.Errorf("a lease at its maximum renewed for %s, %v; want it issued again", r.Lifetime, err)
	}
	// A lease that is not renewable (no ttl of its own) is asked for anew
	// without asking Vault to renew it.
	fixed, err := got.Credential(s.ctx, parsed(t, p, provider.KindCredential, "kv/app"))
	must.OK(t, err)
	if _, err := got.Renew(s.ctx, fixed); !errors.Is(err, provider.ErrReissue) || fixed.Handle.(lease).renewable {
		t.Errorf("a lease that is not renewable (%+v) renewed: %v", fixed.Handle, err)
	}
	must.OK(t, got.Close(s.ctx))
	if !s.leaseRevoked(t, renewed) || !s.leaseRevoked(t, capped) {
		t.Errorf("a lease outlived the login that made it by more than %s", revokeWait)
	}
}
