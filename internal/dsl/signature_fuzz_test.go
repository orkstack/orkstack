package dsl

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
)

// fuzzSigningKey is a fixed P-256 key, so a crasher kept under testdata/fuzz
// carries signatures the next run's key still verifies.
func fuzzSigningKey(tb testing.TB) *cosigntest.Key {
	tb.Helper()
	private, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), bytes.Repeat([]byte{0x2a}, 32))
	if err != nil {
		tb.Fatal(err)
	}
	der, err := x509.MarshalECPrivateKey(private)
	if err != nil {
		tb.Fatal(err)
	}
	key, err := cosigntest.ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
	if err != nil {
		tb.Fatal(err)
	}
	return key
}

// bundleSeed is one seed and whether the key accepts it for the digest.
type bundleSeed struct {
	bundle []byte
	accept bool
}

// bundleSeeds are TestSignatureVerifier_RefusesABundleThatDoesNotVerify's
// bundles and three the key accepts: cosign's own encoding, the same
// envelope in unpadded URL-safe base64 (protobuf JSON reads both), and an
// envelope whose first signature is junk and second is the key's.
func bundleSeeds(tb testing.TB, key, other *cosigntest.Key, digest string) []bundleSeed {
	tb.Helper()
	sign := func(k *cosigntest.Key, payloadType string, payload []byte) []byte {
		b, err := k.BundleOver(payloadType, payload)
		if err != nil {
			tb.Fatal(err)
		}
		return b
	}
	good := sign(key, cosigntest.InTotoPayloadType, cosigntest.Statement(digest))
	var envelope struct {
		DSSEEnvelope struct {
			Signatures []struct {
				Sig string `json:"sig"`
			} `json:"signatures"`
		} `json:"dsseEnvelope"`
	}
	if err := json.Unmarshal(good, &envelope); err != nil {
		tb.Fatal(err)
	}
	sig := envelope.DSSEEnvelope.Signatures[0].Sig
	raw, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		tb.Fatal(err)
	}
	statement := cosigntest.Statement(digest)
	urlSafe := strings.NewReplacer(
		base64.StdEncoding.EncodeToString(statement), base64.RawURLEncoding.EncodeToString(statement),
		sig, base64.RawURLEncoding.EncodeToString(raw)).Replace(string(good))
	elsewhere := cosigntest.Digest([]byte("a different image"))
	return []bundleSeed{
		{good, true},
		{[]byte(urlSafe), true},
		{cosigntest.EnvelopeBundle(key.Hint(), cosigntest.InTotoPayloadType, statement, "AAAA", sig), true},
		{sign(other, cosigntest.InTotoPayloadType, statement), false},
		{sign(key, cosigntest.InTotoPayloadType, cosigntest.Statement(elsewhere)), false},
		{sign(key, cosigntest.InTotoPayloadType, cosigntest.StatementWith(digest, "https://slsa.dev/provenance/v1")), false},
		{sign(key, "application/json", statement), false},
		{cosigntest.EnvelopeBundle(key.Hint(), cosigntest.InTotoPayloadType, statement), false},
		{[]byte(strings.Replace(string(good), cosigntest.BundleType, "application/vnd.dev.sigstore.bundle.v0.2+json", 1)), false},
		{[]byte("not a bundle"), false}, {[]byte(`{"dsseEnvelope":null}`), false}, {[]byte(`{}`), false}, {nil, false},
	}
}

// TestBundleVerifies_ReadsEveryBytesEncodingProtobufJSONWrites: the seeds the
// fuzz target starts from verify and refuse as bundleSeeds says.
func TestBundleVerifies_ReadsEveryBytesEncodingProtobufJSONWrites(t *testing.T) {
	key := fuzzSigningKey(t)
	other, err := cosigntest.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	digest := cosigntest.Digest([]byte("the index"))
	for i, seed := range bundleSeeds(t, key, other, digest) {
		if got := bundleVerifies(&key.Private.PublicKey, digest, seed.bundle); got != seed.accept {
			t.Errorf("seed %d: bundleVerifies = %v, want %v\n%s", i, got, seed.accept, seed.bundle)
		}
	}
}

// FuzzBundleVerifies: a bundle's bytes are the registry's, not ours. The
// verifier never panics on them, and it accepts a bundle only when its
// envelope carries, as an in-toto payload, exactly the statement the key
// signed for this digest: a mutation can re-encode the envelope, but the
// signature binds the payload's bytes and type, and the fuzzer holds no key
// that could sign others. An accepted bundle verifies with no other key and
// for no other digest.
func FuzzBundleVerifies(f *testing.F) {
	key := fuzzSigningKey(f)
	other, err := cosigntest.GenerateKey()
	if err != nil {
		f.Fatal(err)
	}
	digest := cosigntest.Digest([]byte("the index"))
	for _, seed := range bundleSeeds(f, key, other, digest) {
		f.Add(seed.bundle)
	}
	statement := cosigntest.Statement(digest)
	f.Fuzz(func(t *testing.T, bundle []byte) {
		if !bundleVerifies(&key.Private.PublicKey, digest, bundle) {
			return
		}
		var b sigstoreBundle
		if err := json.Unmarshal(bundle, &b); err != nil || b.DSSEEnvelope == nil {
			t.Fatalf("accepted a bundle that does not parse: %q", bundle)
		}
		payload, ok := decodeBase64(b.DSSEEnvelope.Payload)
		if !ok || !bytes.Equal(payload, statement) || b.DSSEEnvelope.PayloadType != cosigntest.InTotoPayloadType {
			t.Fatalf("accepted a payload the key never signed: %q as %q", payload, b.DSSEEnvelope.PayloadType)
		}
		if bundleVerifies(&other.Private.PublicKey, digest, bundle) {
			t.Fatalf("a bundle the key signed verifies with another key too: %q", bundle)
		}
		if bundleVerifies(&key.Private.PublicKey, cosigntest.Digest([]byte("a different image")), bundle) {
			t.Fatalf("a bundle for one digest verifies for another: %q", bundle)
		}
	})
}

// FuzzBundleReferrers: a referrers index and a referrer manifest are the
// registry's too. Neither parser panics; every referrer listed is a
// well-formed digest the index lists as a signature bundle, and those listed
// and those counted unread are never more than the index holds; and a bundle
// layer named is a well-formed digest of a layer within the payload bound.
func FuzzBundleReferrers(f *testing.F) {
	key := fuzzSigningKey(f)
	digest := cosigntest.Digest([]byte("the index"))
	bundle, err := key.Bundle(digest)
	if err != nil {
		f.Fatal(err)
	}
	manifest := cosigntest.BundleManifest(bundle, digest, 1024)
	for _, seed := range [][]byte{
		cosigntest.ReferrersIndex(cosigntest.BundleReferrer(manifest)),
		cosigntest.ReferrersIndex(cosigntest.Referrer{Manifest: manifest, ArtifactType: cosigntest.BundleType,
			PredicateType: "https://slsa.dev/provenance/v1"},
			cosigntest.Referrer{Manifest: []byte(`{"an":"sbom"}`), ArtifactType: "application/spdx+json"}),
		cosigntest.ReferrersIndex(cosigntest.Referrer{Manifest: manifest, ArtifactType: cosigntest.BundleType}),
		cosigntest.ReferrersIndex(cosigntest.Referrer{Manifest: manifest, ArtifactType: "application/vnd.dev.sigstore.bundle.v0.4+json"}),
		cosigntest.ReferrersIndex(),
		manifest,
		[]byte(strings.Replace(string(manifest), `"size":`, `"size":-`, 1)),
		[]byte(`{"manifests":[{"digest":"sha256:x","artifactType":"` + cosigntest.BundleType + `"}]}`),
		[]byte(`{"layers":[{"mediaType":"` + cosigntest.BundleType + `","digest":"` + digest + `","size":2000000}]}`),
		[]byte("not json"), nil,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		var index referrersIndex
		listed := map[string]bool{}
		if json.Unmarshal(body, &index) == nil {
			for _, m := range index.Manifests {
				if m.ArtifactType == MediaTypeSigstoreBundle {
					listed[m.Digest] = true
				}
			}
		}
		got, unread := bundleReferrers(body)
		if unread < 0 || len(got)+unread > len(index.Manifests) {
			t.Fatalf("bundleReferrers(%q) listed %d and counted %d unread of %d manifests", body, len(got), unread, len(index.Manifests))
		}
		for _, d := range got {
			if !ValidDigest(d) || !listed[d] {
				t.Fatalf("bundleReferrers(%q) listed %q", body, d)
			}
		}
		layer, ok := bundleLayer(body)
		if ok != (layer != "") || (ok && !ValidDigest(layer)) {
			t.Fatalf("bundleLayer(%q) = %q, %v", body, layer, ok)
		}
		if !ok {
			return
		}
		var parsed cosignManifest
		if json.Unmarshal(body, &parsed) != nil {
			t.Fatalf("bundleLayer named %q in a manifest that does not parse: %q", layer, body)
		}
		for _, l := range parsed.Layers {
			if l.Digest == layer && l.Size >= 0 && l.Size <= maxSignaturePayloadBytes {
				return
			}
		}
		t.Fatalf("bundleLayer(%q) named %q, which no layer within the bound carries", body, layer)
	})
}
