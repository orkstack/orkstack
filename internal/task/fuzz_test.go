package task

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// prefixOfComponents reports whether the "/"-separated components of prefix
// are the leading components of name: the boundary rule RegistryAllowed
// states, counted without its string tests.
func prefixOfComponents(prefix, name string) bool {
	want, have := strings.Split(prefix, "/"), strings.Split(name, "/")
	if len(want) > len(have) {
		return false
	}
	for i := range want {
		if want[i] != have[i] {
			return false
		}
	}
	return true
}

// FuzzRegistryAllowed: the allowed_registries list is a floor on which images
// may run, matched on "/" boundaries ("ghcr.io/acme" covers ghcr.io/acme/migrate
// and never ghcr.io/acmeevil/migrate). For any entries the store would keep,
// an image is allowed exactly when one entry's components are the leading
// components of its registry/repository; an empty list allows everything;
// adding an entry never forbids anything; and every stored entry allows
// itself.
func FuzzRegistryAllowed(f *testing.F) {
	f.Add("ghcr.io/acme/", "ghcr.io/other", "ghcr.io", "acme/migrate")
	f.Add("ghcr.io/acme", "", "ghcr.io", "acmeevil/migrate")
	f.Add("ghcr.io/acme", "", "ghcr.io", "acme")
	f.Add("localhost:5001/orkstack", "docker.io/library", "localhost:5001", "orkstack/shell")
	f.Add("docker.io/library", "ghcr.io", "docker.io", "library/alpine")
	f.Add("ghcr.io/acme", "", "GHCR.IO", "acme/migrate")
	f.Add("ghcr.io/acme/", "", "ghcr.io", "acme//x")
	f.Add("ghcr.io/a", "ghcr.io/a/b", "ghcr.io", "a/b/c")
	f.Fuzz(func(t *testing.T, first, second, registry, repository string) {
		var entries []string
		for _, entry := range []string{first, second} {
			if entry == "" {
				continue
			}
			if err := ValidRegistryPrefix(entry); err != nil {
				return
			}
			entries = append(entries, entry)
		}
		got := RegistryAllowed(entries, registry, repository)
		name := registry + "/" + repository
		want := len(entries) == 0
		for _, entry := range entries {
			want = want || prefixOfComponents(strings.TrimSuffix(entry, "/"), name)
		}
		if got != want {
			t.Fatalf("RegistryAllowed(%q, %q, %q) = %v, the component rule says %v", entries, registry, repository, got, want)
		}
		if len(entries) > 0 && !RegistryAllowed(append([]string{"ghcr.io/zzz-unused"}, entries...), registry, repository) && got {
			t.Fatalf("adding an entry to %q forbade %s", entries, name)
		}
		for _, entry := range entries {
			trimmed := strings.TrimSuffix(entry, "/")
			host, rest, _ := strings.Cut(trimmed, "/")
			if !RegistryAllowed([]string{entry}, host, rest) {
				t.Fatalf("the entry %q does not allow itself", entry)
			}
		}
	})
}

// FuzzValidRegistryPrefix: an accepted entry is what the rule's text says and
// no more: lower-case, no space, control, "*", "@" or ",", a host first, a
// colon only in that host, no empty segment.
func FuzzValidRegistryPrefix(f *testing.F) {
	for _, seed := range []string{
		"ghcr.io/acme/", "ghcr.io", "localhost:5001/orkstack", "acme/", "docker.io/acme", "", "/", "ghcr.io//x", "ghcr.io/acme:1", "GHCR.io/a",
		"ghcr.io/a b", "ghcr.io/a*", "ghcr.io/a@sha256:x", "ghcr.io/a,b", "ghcr.io/a\x00", "ghcr.io/\u00a0x", "ghcr.io/\u212a", strings.Repeat("a", 256) + ".io",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, entry string) {
		if err := ValidRegistryPrefix(entry); err != nil {
			if err.Error() == "" {
				t.Fatalf("ValidRegistryPrefix(%q) refused with no reason", entry)
			}
			return
		}
		if entry == "" || len(entry) > MaxAllowedRegistryBytes || strings.ToLower(entry) != entry || strings.ContainsAny(entry, "*@,") {
			t.Fatalf("ValidRegistryPrefix accepted %q", entry)
		}
		for _, r := range entry {
			if unicode.IsSpace(r) || unicode.IsControl(r) {
				t.Fatalf("ValidRegistryPrefix accepted %q holding %q", entry, r)
			}
		}
		trimmed := strings.TrimSuffix(entry, "/")
		host, rest, _ := strings.Cut(trimmed, "/")
		if host == "" || !(host == "localhost" || strings.ContainsAny(host, ".:")) || strings.Contains(rest, ":") || strings.Contains(trimmed, "//") {
			t.Fatalf("ValidRegistryPrefix accepted %q (host %q, rest %q)", entry, host, rest)
		}
	})
}

// FuzzParseSigner: require_signed_by is empty or exactly cosign:<key-name>
// with a name from the slug rule, and what it accepts spells itself.
func FuzzParseSigner(f *testing.F) {
	for _, seed := range []string{"", "cosign:release", "cosign:", "cosign", "sigstore:release", "cosign:Release", "cosign:a:b", " cosign:a", strings.Repeat("a", 64), "cosign:" + strings.Repeat("a", 65)} {
		f.Add(seed)
	}
	slug := regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	f.Fuzz(func(t *testing.T, value string) {
		scheme, name, err := ParseSigner(value)
		if err != nil {
			if scheme != "" || name != "" || !strings.Contains(err.Error(), "require_signed_by") {
				t.Fatalf("ParseSigner(%q) = %q, %q, %v", value, scheme, name, err)
			}
			return
		}
		if value == "" {
			if scheme != "" || name != "" {
				t.Fatalf("ParseSigner(%q) = %q, %q", value, scheme, name)
			}
			return
		}
		if scheme != SignerSchemeCosign || !slug.MatchString(name) || len(name) > MaxSigningKeyNameBytes || scheme+":"+name != value {
			t.Fatalf("ParseSigner(%q) = %q, %q", value, scheme, name)
		}
	})
}

// FuzzParseSigningKey: a signing key is exactly one PEM PUBLIC KEY block of an
// ECDSA P-256 key and nothing after it; whatever else is refused by name, and
// what is accepted fingerprints the same after being written out again.
func FuzzParseSigningKey(f *testing.F) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		f.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		f.Fatal(err)
	}
	good := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	f.Add(good)
	f.Add(append(append([]byte(nil), good...), good...))
	f.Add(append(append([]byte(nil), good...), "trailing"...))
	f.Add(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	f.Add(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der[:len(der)-1]}))
	f.Add([]byte(""))
	f.Add([]byte("-----BEGIN PUBLIC KEY-----\n-----END PUBLIC KEY-----\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		parsed, err := ParseSigningKey(data)
		if err != nil {
			if parsed != nil || err.Error() == "" {
				t.Fatalf("ParseSigningKey refused %q as %v with %v", data, err, parsed)
			}
			return
		}
		if parsed.Curve != elliptic.P256() || !parsed.Curve.IsOnCurve(parsed.X, parsed.Y) {
			t.Fatalf("ParseSigningKey(%q) accepted a key that is not on P-256", data)
		}
		fingerprint := SigningKeyFingerprint(parsed)
		if !strings.HasPrefix(fingerprint, "sha256:") || len(fingerprint) != len("sha256:")+64 {
			t.Fatalf("fingerprint %q", fingerprint)
		}
		der, err := x509.MarshalPKIXPublicKey(parsed)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseSigningKey(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
		if err != nil || SigningKeyFingerprint(again) != fingerprint {
			t.Fatalf("a key written out again fingerprints %q, %v; want %q", SigningKeyFingerprint(again), err, fingerprint)
		}
		if got := SigningKeyPEMFingerprint(string(data)); got != fingerprint {
			t.Fatalf("SigningKeyPEMFingerprint = %q, want %q", got, fingerprint)
		}
	})
}
