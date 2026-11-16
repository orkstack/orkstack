package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// TestDevtoken: init makes one key pair and the engine's signing key, and
// keeps them; sign writes a token
// the published key set verifies, with the claims asked for.
func TestDevtoken(t *testing.T) {
	dir := t.TempDir()
	if err := initKeys(dir); err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(filepath.Join(dir, keyFile))
	if err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(filepath.Join(dir, keyFile)); info.Mode().Perm() != 0o600 {
		t.Fatalf("the private key has mode %v; want 0600", info.Mode().Perm())
	}
	if err := initKeys(dir); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(filepath.Join(dir, keyFile))
	if string(again) != string(key) {
		t.Fatal("a second init replaced the key: every token signed before would stop verifying")
	}
	engineKey, err := os.ReadFile(filepath.Join(dir, engineKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if signer, err := jwt.ParsePrivateKeyPEM(engineKey); err != nil || signer.Algorithm() != jwt.ES256 {
		t.Fatalf("the engine's signing key = %v, %v; want an ES256 key", signer, err)
	}
	if info, _ := os.Stat(filepath.Join(dir, engineKeyFile)); info.Mode().Perm() != 0o600 || string(engineKey) == string(key) {
		t.Fatalf("the engine's signing key has mode %v, or is the issuer's", info.Mode().Perm())
	}
	out := filepath.Join(dir, "admin.token")
	if err := signToken(dir, signOptions{issuer: "orkstack-dev", audience: "orkstack", subject: "dev-admin", ttl: 720 * time.Hour,
		claims: map[string]string{"orker_id": "orker-1"}, out: out}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	set, err := os.ReadFile(filepath.Join(dir, jwksFile))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := jwt.ParseKeySet(set)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := jwt.Verify(context.Background(), strings.TrimSpace(string(raw)), jwt.Policy{Issuer: "orkstack-dev", Audiences: []string{"orkstack"},
		Algorithms: []jwt.Algorithm{jwt.ES256}, MaxLifetime: 720 * time.Hour}, jwt.StaticFromSet(keys), time.Now())
	if err != nil {
		t.Fatalf("the signed token does not verify: %v", err)
	}
	if claims.Subject != "dev-admin" {
		t.Fatalf("claims = %+v", claims)
	}
}
