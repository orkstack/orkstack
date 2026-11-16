package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func writeECKey(t *testing.T) string {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestIdentityFromEnv: the identity.* settings are read with their
// defaults, and each malformed one refuses the boot naming its setting.
func TestIdentityFromEnv(t *testing.T) {
	c, err := identityFromEnv(lookupFrom(nil))
	if err != nil || c.enabled() || !c.serveDiscovery || !c.verifyPublished || c.activationDelay != identity.DefaultActivationDelay ||
		c.algorithm != identity.DefaultAlgorithm || c.subjectFormat != identity.FormatOrk {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	c, err = identityFromEnv(lookupFrom(map[string]string{
		"ORK_IDENTITY_ISSUER":                   "https://ork-id.acme.dev/",
		"ORK_IDENTITY_SERVE_DISCOVERY":          "false",
		"ORK_IDENTITY_SIGNING_KEY_FILES":        "/a.pem, /b.pem",
		"ORK_IDENTITY_SIGNING_ALGORITHM":        "ES256",
		"ORK_IDENTITY_SIGNING_ACTIVATION_DELAY": "2s",
		"ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED": "false",
		"ORK_IDENTITY_SUBJECT_FORMAT":           "spiffe",
		"ORK_IDENTITY_SPIFFE_TRUST_DOMAIN":      "ork.acme.dev",
	}))
	if err != nil || !c.enabled() || c.issuer != "https://ork-id.acme.dev" || c.serveDiscovery || c.verifyPublished ||
		len(c.keyFiles) != 2 || c.keyFiles[1] != "/b.pem" || c.activationDelay != 2*time.Second || c.subjectFormat != identity.FormatSPIFFE {
		t.Fatalf("set = %+v, %v", c, err)
	}
	for want, env := range map[string]map[string]string{
		"identity.issuer":                   {"ORK_IDENTITY_ISSUER": "http://ork-id.acme.dev"},
		"identity.serve_discovery":          {"ORK_IDENTITY_SERVE_DISCOVERY": "maybe"},
		"identity.signing.algorithm":        {"ORK_IDENTITY_SIGNING_ALGORITHM": "HS256"},
		"identity.signing.activation_delay": {"ORK_IDENTITY_SIGNING_ACTIVATION_DELAY": "-1s"},
		"identity.signing.verify_published": {"ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED": "yes please"},
		"identity.subject_format":           {"ORK_IDENTITY_SUBJECT_FORMAT": "x509"},
		"identity.spiffe.trust_domain":      {"ORK_IDENTITY_SPIFFE_TRUST_DOMAIN": "Ork.Acme"},
		"identity.spiffe.trust_domain ":     {"ORK_IDENTITY_ISSUER": "https://i", "ORK_IDENTITY_SIGNING_KEY_FILES": "/k", "ORK_IDENTITY_SUBJECT_FORMAT": "spiffe"},
		"identity.signing.key_files":        {"ORK_IDENTITY_ISSUER": "https://i"},
	} {
		if _, err := identityFromEnv(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), strings.TrimSpace(want)) {
			t.Errorf("%s: %v", want, err)
		}
	}
}

// TestIdentity_CheckBoot: an issuer on the local backend refuses the boot,
// since another engine could not read the keys a replica published.
func TestIdentity_CheckBoot(t *testing.T) {
	c := identityConfig{issuer: "https://i", keyFiles: []string{"/k"}}
	if err := c.checkBoot(artifacts.BackendLocal, "asserted"); err == nil || !strings.Contains(err.Error(), "identity.issuer") {
		t.Fatalf("local backend: %v", err)
	}
	if err := c.checkBoot(artifacts.BackendS3, "asserted"); err != nil {
		t.Fatal(err)
	}
	if err := (identityConfig{}).checkBoot(artifacts.BackendLocal, "asserted"); err != nil {
		t.Fatalf("identity off: %v", err)
	}
}

// TestIdentityFromEnv_KeysCopy: identity.keys_copy_file and
// identity.keys_copy_max_age are read, the age held to 5 minutes to 24
// hours, and signing keys need no issuer: they then sign no token.
func TestIdentityFromEnv_KeysCopy(t *testing.T) {
	c, err := identityFromEnv(lookupFrom(nil))
	if err != nil || c.copyFile != "" || c.copyMaxAge != identity.DefaultKeysCopyMaxAge {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	c, err = identityFromEnv(lookupFrom(map[string]string{
		"ORK_IDENTITY_SIGNING_KEY_FILES": "/k.pem",
		"ORK_IDENTITY_KEYS_COPY_FILE":    " /var/lib/ork/keys-copy.json ",
		"ORK_IDENTITY_KEYS_COPY_MAX_AGE": "6h",
	}))
	if err != nil || c.enabled() || !c.hasKeys() || c.copyFile != "/var/lib/ork/keys-copy.json" || c.copyMaxAge != 6*time.Hour {
		t.Fatalf("set = %+v, %v", c, err)
	}
	for _, raw := range []string{"4m59s", "24h1s", "soon", "-1h"} {
		if _, err := identityFromEnv(lookupFrom(map[string]string{"ORK_IDENTITY_KEYS_COPY_MAX_AGE": raw})); err == nil || !strings.Contains(err.Error(), "identity.keys_copy_max_age") {
			t.Errorf("%s: %v", raw, err)
		}
	}
	for _, raw := range []string{"5m", "24h"} {
		if _, err := identityFromEnv(lookupFrom(map[string]string{"ORK_IDENTITY_KEYS_COPY_MAX_AGE": raw})); err != nil {
			t.Errorf("%s: %v", raw, err)
		}
	}
	if _, err := identityFromEnv(lookupFrom(map[string]string{"ORK_IDENTITY_KEYS_COPY_FILE": "keys-copy.json"})); err == nil || !strings.Contains(err.Error(), "identity.keys_copy_file") {
		t.Errorf("a relative copy file: %v", err)
	}
}

// TestIdentity_CheckBootNeedsAKeyInObserveAndEnforce: an engine whose
// auth.mode is observe or enforce lists a signing key, issuer or not; in
// asserted mode it need not (O76).
func TestIdentity_CheckBootNeedsAKeyInObserveAndEnforce(t *testing.T) {
	for _, mode := range []string{"observe", "enforce"} {
		err := (identityConfig{}).checkBoot(artifacts.BackendS3, mode)
		if err == nil || !strings.Contains(err.Error(), "identity.signing.key_files") || !strings.Contains(err.Error(), "auth.mode") {
			t.Errorf("%s with no key: %v", mode, err)
		}
		if err := (identityConfig{keyFiles: []string{"/k"}}).checkBoot(artifacts.BackendS3, mode); err != nil {
			t.Errorf("%s with a key: %v", mode, err)
		}
	}
	if err := (identityConfig{}).checkBoot(artifacts.BackendS3, "asserted"); err != nil {
		t.Errorf("asserted with no key: %v", err)
	}
}

// TestIdentity_CheckCopyFile: boot proves it can write beside the copy
// file, and refuses a copy that is a link, not a regular file, or one
// others can write, naming the setting.
func TestIdentity_CheckCopyFile(t *testing.T) {
	dir := t.TempDir()
	if err := (identityConfig{}).checkCopyFile(); err != nil {
		t.Fatalf("no copy file: %v", err)
	}
	ok := identityConfig{copyFile: filepath.Join(dir, "copy.json")}
	if err := ok.checkCopyFile(); err != nil {
		t.Fatalf("a writable directory: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("the probe left %v", entries)
	}
	if err := os.WriteFile(ok.copyFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ok.checkCopyFile(); err != nil {
		t.Fatalf("an existing 0600 copy: %v", err)
	}
	loose := filepath.Join(dir, "loose.json")
	if err := os.WriteFile(loose, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(loose, 0o622); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(ok.copyFile, link); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"others can write": loose,
		"a link":           link,
		"a directory":      dir,
		"a missing parent": filepath.Join(dir, "nope", "copy.json"),
	} {
		if err := (identityConfig{copyFile: path}).checkCopyFile(); err == nil || !strings.Contains(err.Error(), "identity.keys_copy_file") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestIdentity_LoadsItsKeyFiles: the key files are read in order, and a
// missing, unreadable or wrong-kind file refuses the boot naming it.
func TestIdentity_LoadsItsKeyFiles(t *testing.T) {
	a, b := writeECKey(t), writeECKey(t)
	c := identityConfig{issuer: "https://i", keyFiles: []string{a, b}, algorithm: "ES256"}
	keys, err := c.loadKeys()
	if err != nil || len(keys) != 2 || keys[0].KeyID() == keys[1].KeyID() {
		t.Fatalf("keys = %d, %v", len(keys), err)
	}
	notKey := filepath.Join(t.TempDir(), "x.pem")
	os.WriteFile(notKey, []byte("hello"), 0o600)
	for _, bad := range []string{filepath.Join(t.TempDir(), "missing.pem"), notKey} {
		c.keyFiles = []string{a, bad}
		if _, err := c.loadKeys(); err == nil || !strings.Contains(err.Error(), bad) || !strings.Contains(err.Error(), "identity.signing.key_files") {
			t.Errorf("%s: %v", bad, err)
		}
	}
	// A whole issuer boots from them on a store.
	c.keyFiles = []string{a}
	store, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	i, err := c.newIssuer(store, "e1@host@", 0, nil, nil)
	if err != nil || i.URL() != "https://i" {
		t.Fatalf("issuer = %v, %v", i, err)
	}
}

// TestIdentity_AKeyFileMustBeARegularFile: a directory or a FIFO named as a
// key file refuses the boot rather than hanging it.
func TestIdentity_AKeyFileMustBeARegularFile(t *testing.T) {
	dir := t.TempDir()
	c := identityConfig{issuer: "https://i", keyFiles: []string{dir}, algorithm: "ES256"}
	if _, err := c.loadKeys(); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("a directory: %v", err)
	}
}

// TestSetupIssuer_AnUnreadableDocumentRefusesTheBoot: a published keys
// document this release cannot read never clears by waiting, so the engine
// does not start as if it could sign.
func TestSetupIssuer_AnUnreadableDocumentRefusesTheBoot(t *testing.T) {
	dir := t.TempDir()
	cfg := artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir}
	key := filepath.Join(dir, artifacts.PublishedKeysKey)
	if err := os.MkdirAll(filepath.Dir(key), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte(`{"v":9}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c := identityConfig{issuer: "https://i", keyFiles: []string{writeECKey(t)}, algorithm: "ES256"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := setupIssuer(ctx, c, cfg, "e1", 0, nil, slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Fatalf("setupIssuer = %v", err)
	}
}
