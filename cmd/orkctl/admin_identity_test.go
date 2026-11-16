package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestRenderIssuer: the issuer's settings and what the engine last read,
// and a plain statement when task identity is off.
func TestRenderIssuer(t *testing.T) {
	var out bytes.Buffer
	renderIssuer(&out, &orkstackv1.GetIssuerResponse{})
	if !strings.Contains(out.String(), "Task identity is off") {
		t.Fatalf("off: %s", out.String())
	}
	out.Reset()
	at := timestamppb.New(time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC))
	renderIssuer(&out, &orkstackv1.GetIssuerResponse{Enabled: true, Issuer: "https://ork-id.acme.dev", SubjectFormat: "spiffe",
		TrustDomain: "ork.acme.dev", ServeDiscovery: true, KeysSequence: 3, KeysReadAt: at, LiveCheckedAt: at,
		LiveKeyIds: []string{"k1"}, LiveError: "", KeysReadError: "store down"})
	for _, want := range []string{"https://ork-id.acme.dev", "spiffe", "ork.acme.dev", "Serves discovery:", "yes", "sequence 3",
		"2026-11-15", "store down", "k1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q:\n%s", want, out.String())
		}
	}
}

// TestRenderSigningKeys: one row per key with its state, why it does not
// sign, and its holders as live of all.
func TestRenderSigningKeys(t *testing.T) {
	at := timestamppb.New(time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC))
	var out bytes.Buffer
	renderSigningKeys(&out, []*orkstackv1.SigningKey{
		{KeyId: "kid-new", Algorithm: "RS256", Published: true, PublishedAt: at, HeldHere: true, InactiveReason: "after the activation delay",
			Holders: []*orkstackv1.SigningKeyHolder{{Engine: "a", Live: true}, {Engine: "b"}}},
		{KeyId: "kid-old", Algorithm: "RS256", Published: true, PublishedAt: at, ActivatedAt: at, HeldHere: true, Active: true},
		{KeyId: "kid-gone", Algorithm: "RS256", Published: true, PublishedAt: at, RetiredAt: at, RetiredBy: "alice"},
		{KeyId: "kid-lost", Algorithm: "RS256", Published: true, PublishedAt: at, Unheld: true},
	})
	text := out.String()
	for _, want := range []string{"KEY ID", "kid-new", "after the activation delay", "1/2", "kid-old", "signs", "kid-gone", "retired by alice", "kid-lost", "no engine holds it lately"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q:\n%s", want, text)
		}
	}
}

// TestExportIssuer: the documents land where an HTTPS host serves them at
// the issuer URL; the bundle only with a trust domain; an engine with task
// identity off exports nothing.
func TestExportIssuer(t *testing.T) {
	dir := t.TempDir()
	read := timestamppb.Now()
	paths, err := exportIssuer(dir, &orkstackv1.GetIssuerResponse{Enabled: true, KeysReadAt: read, Discovery: `{"d":1}`, Jwks: `{"keys":[]}`, SpiffeBundle: `{"b":1}`, TrustDomain: "t"})
	if err != nil || len(paths) != 3 {
		t.Fatalf("paths = %v, %v", paths, err)
	}
	for rel, want := range map[string]string{".well-known/openid-configuration": `{"d":1}`, "jwks.json": `{"keys":[]}`, "spiffe-bundle.json": `{"b":1}`} {
		got, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v", rel, got, err)
		}
	}
	dir = t.TempDir()
	if paths, err := exportIssuer(dir, &orkstackv1.GetIssuerResponse{Enabled: true, KeysReadAt: read, Discovery: `{}`, Jwks: `{}`}); err != nil || len(paths) != 2 {
		t.Fatalf("without a trust domain: %v, %v", paths, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "spiffe-bundle.json")); !os.IsNotExist(err) {
		t.Fatal("a bundle was written with no trust domain")
	}
	if _, err := exportIssuer(t.TempDir(), &orkstackv1.GetIssuerResponse{}); err == nil || !strings.Contains(err.Error(), "off") {
		t.Fatalf("identity off: %v", err)
	}
	for name, m := range map[string]*orkstackv1.GetIssuerResponse{
		"no discovery":           {Enabled: true, KeysReadAt: read, Jwks: `{}`},
		"no bundle for a domain": {Enabled: true, KeysReadAt: read, Discovery: `{}`, Jwks: `{}`, TrustDomain: "t"},
		"never read":             {Enabled: true, Discovery: `{}`},
		"read failed":            {Enabled: true, KeysReadAt: read, KeysReadError: "store down", Discovery: `{}`, Jwks: `{}`},
	} {
		dir := t.TempDir()
		if _, err := exportIssuer(dir, m); err == nil {
			t.Errorf("%s: exported", name)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("%s: wrote %v", name, entries)
		}
	}
}

// TestRetire_AKeyIDThatBeginsWithADash: a kid is base64url and can begin
// with "-"; after "--" it is the argument, and without it the error says so.
func TestRetire_AKeyIDThatBeginsWithADash(t *testing.T) {
	root, _ := newRootCommand()
	root.SetArgs([]string{"admin", "identity", "keys", "retire", "-ZXDstp"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "goes after") {
		t.Fatalf("without --: %v", err)
	}
	root, _ = newRootCommand()
	root.SetArgs([]string{"admin", "identity", "keys", "retire", "--", "-ZXDstp"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader(""))
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "retire signing key -ZXDstp") {
		t.Fatalf("after --: %v", err)
	}
}
