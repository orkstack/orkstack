//go:build integration

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TheIssuerPublishesRetiresAndServesItsKeys: two engines on
// a real store hold one signing key. Both publish it, the only key signs at
// once, each engine serves the same documents with no credential and the
// export writes them; a retire on one engine stops the other signing at
// its next read, and neither serves the key again.
func TestIntegration_TheIssuerPublishesRetiresAndServesItsKeys(t *testing.T) {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, cfg)

	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(k)
	must.OK(t, err)
	keyFile := filepath.Join(t.TempDir(), "identity-key.pem")
	must.OK(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600))
	signer, err := jwt.ParsePrivateKeyPEM(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	must.OK(t, err)
	kid := signer.KeyID()

	env := []string{
		"ORK_IDENTITY_ISSUER=https://ork-id.example.test",
		"ORK_IDENTITY_SIGNING_KEY_FILES=" + keyFile,
		"ORK_IDENTITY_SIGNING_ALGORITHM=ES256",
		"ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED=false",
		"ORK_IDENTITY_SPIFFE_TRUST_DOMAIN=ork.example.test",
	}
	s := startScopeStack(t, env, nil)
	second := startReplica(s.ctx, t, "ork-2", testbin.Path(t, "./cmd/ork"), temporalAddress(), s.namespace, s.queue, env...)
	second.awaitReady(s.ctx, t)
	fmt.Fprintf(&s.log, "# the issuer on two engines (%s)\nengines %s and %s, key %s\n\n", time.Now().UTC().Format(time.RFC3339), s.engine.addr, second.addr, kid)
	on := func(addr string, args ...string) string {
		t.Helper()
		// The server flag goes before any "--", after which every word is
		// an argument (a key id may begin with "-").
		full := append([]string(nil), args...)
		if at := slices.Index(full, "--"); at >= 0 {
			full = slices.Concat(full[:at], []string{"--server", "http://" + addr}, full[at:])
		} else {
			full = append(full, "--server", "http://"+addr)
		}
		stdout, stderr, code := runBinary(s.ctx, t, s.orkctl, s.root, full...)
		fmt.Fprintf(&s.log, "$ orkctl %s (engine %s)\n%s%s(exit %d)\n\n", strings.Join(args, " "), addr, stdout, stderr, code)
		if code != 0 {
			t.Fatalf("orkctl %s on %s: exit %d: %s", strings.Join(args, " "), addr, code, stderr)
		}
		return stdout
	}
	keys := func(addr string) []*orkstackv1.SigningKey {
		t.Helper()
		resp := &orkstackv1.ListSigningKeysResponse{}
		out := on(addr, "admin", "identity", "keys", "list", "--json")
		must.OKf(t, api.UnmarshalJSON([]byte(out), resp), "keys list --json: %s", out)
		return resp.GetKeys()
	}
	await := func(what string, addr string, ok func(*orkstackv1.SigningKey) bool) {
		t.Helper()
		deadline := time.Now().Add(150 * time.Second)
		for {
			ks := keys(addr)
			if len(ks) == 1 && ks[0].GetKeyId() == kid && ok(ks[0]) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s on %s: %v", what, addr, ks)
			}
			select {
			case <-s.ctx.Done():
				t.Fatal(s.ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}

	// The only key signs on both, held by both.
	for _, addr := range []string{s.engine.addr, second.addr} {
		await("the only key active, held by both engines", addr, func(k *orkstackv1.SigningKey) bool {
			return k.GetActive() && k.GetActivatedAt() != nil && k.GetHeldHere() && len(k.GetHolders()) == 2
		})
	}

	// The documents: GetIssuer's, served unauthenticated, and exported.
	issuer := &orkstackv1.GetIssuerResponse{}
	out := on(s.engine.addr, "admin", "identity", "issuer", "get", "--json")
	must.OKf(t, api.UnmarshalJSON([]byte(out), issuer), "issuer get --json: %s", out)
	if !issuer.GetEnabled() || issuer.GetIssuer() != "https://ork-id.example.test" || !strings.Contains(issuer.GetJwks(), kid) || !strings.Contains(issuer.GetSpiffeBundle(), "jwt-svid") {
		t.Fatalf("GetIssuer = %v", issuer)
	}
	get := func(addr, path string) string {
		t.Helper()
		resp, err := http.Get("http://" + addr + path)
		must.OK(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		must.OK(t, err)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s on %s = %d: %s", path, addr, resp.StatusCode, body)
		}
		fmt.Fprintf(&s.log, "GET %s (engine %s): %s\n", path, addr, body)
		return string(body)
	}
	for _, addr := range []string{s.engine.addr, second.addr} {
		if got := get(addr, api.IssuerDiscoveryPath); got != issuer.GetDiscovery() {
			t.Fatalf("engine %s serves another discovery document: %s", addr, got)
		}
		if got := get(addr, api.IssuerKeysPath); !strings.Contains(got, kid) {
			t.Fatalf("engine %s's key set lacks %s: %s", addr, kid, got)
		}
		get(addr, api.IssuerBundlePath)
	}
	dir := t.TempDir()
	on(s.engine.addr, "admin", "identity", "issuer", "export", "--out", dir)
	for rel, want := range map[string]string{".well-known/openid-configuration": issuer.GetDiscovery(), "jwks.json": issuer.GetJwks(), "spiffe-bundle.json": issuer.GetSpiffeBundle()} {
		got, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil || string(got) != want {
			t.Fatalf("exported %s = %q, %v", rel, got, err)
		}
	}

	// Retired on the first engine; the second stops at its next read.
	on(s.engine.addr, "admin", "identity", "keys", "retire", "--yes", "--", kid)
	await("the retired key no longer signing", second.addr, func(k *orkstackv1.SigningKey) bool {
		return !k.GetActive() && k.GetRetiredAt() != nil && strings.Contains(k.GetInactiveReason(), "retired")
	})
	// With its only key retired an engine serves no key set at all: never
	// an empty one a relying party would cache.
	for _, addr := range []string{s.engine.addr, second.addr} {
		resp, err := http.Get("http://" + addr + api.IssuerKeysPath)
		must.OK(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Fprintf(&s.log, "GET %s (engine %s) after the retire: %d %s\n", api.IssuerKeysPath, addr, resp.StatusCode, body)
		if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(string(body), kid) {
			t.Fatalf("engine %s after the retire answers %d: %s", addr, resp.StatusCode, body)
		}
	}
	t.Logf("EVIDENCE issuer: two engines published one key, served the same documents with no credential, and a retire on one stopped the other signing")
	proof.Write(t, "orkctl", "identity-issuer", []byte(s.log.String()))
}
