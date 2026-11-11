package dsl

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const sigRepo = "acme/migrate"

// sigFixture is one registry double serving one image, one key that signs,
// and the ImageRef the engine would have resolved for it.
type sigFixture struct {
	double *cosigntest.Double
	key    *cosigntest.Key
	image  *orkstackv1.ImageRef
	v      *SignatureVerifier
}

func newSigFixture(t *testing.T) *sigFixture {
	t.Helper()
	double := cosigntest.NewDouble(t)
	key, err := cosigntest.GenerateKey()
	must.OK(t, err)
	digest := cosigntest.Digest([]byte("the index " + t.Name()))
	return &sigFixture{
		double: double,
		key:    key,
		image: &orkstackv1.ImageRef{
			Reference: double.Host + "/" + sigRepo + ":v3", Registry: double.Host, Repository: sigRepo,
			IndexDigest: digest, Pinned: double.Host + "/" + sigRepo + "@" + digest,
		},
		v: &SignatureVerifier{Resolver: NewResolver("")},
	}
}

func (f *sigFixture) layer(t *testing.T, key *cosigntest.Key, digest string) cosigntest.Layer {
	t.Helper()
	l, err := key.SignatureLayer(digest, f.image.GetRegistry()+"/"+sigRepo)
	must.OK(t, err)
	return l
}

func (f *sigFixture) verify() error {
	return f.v.Verify(context.Background(), f.image, "cosign:release", f.key.PublicPEM)
}

func requireSignatureCode(t *testing.T, err error, code string) *SignatureError {
	t.Helper()
	var sig *SignatureError
	if !errors.As(err, &sig) {
		t.Fatalf("Verify = %v (%T), want *SignatureError %s", err, err, code)
	}
	if sig.Code != code {
		t.Fatalf("Verify code = %s (%v), want %s", sig.Code, err, code)
	}
	return sig
}

func TestImagePolicy_RequireDigestAgreesWithLooksLikeReference(t *testing.T) {
	for _, uses := range []string{
		"shell", "http", "my-activity", "alpine", "alpine:3.20", "ghcr.io/acme/migrate",
		"ghcr.io/acme/migrate:v3", "localhost:5001/x", "acme/migrate", "repo@sha256:" + strings.Repeat("a", 64),
		"a:b", "x/y",
	} {
		if got, want := task.IsRegistryReference(uses), looksLikeReference(uses); got != want {
			t.Errorf("%q: task.IsRegistryReference = %v, dsl.looksLikeReference = %v", uses, got, want)
		}
	}
}

func TestSignatureVerifier_AcceptsAValidSignature(t *testing.T) {
	f := newSigFixture(t)
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, f.key, f.image.GetIndexDigest()))
	if err := f.verify(); err != nil {
		t.Fatalf("Verify = %v, want nil", err)
	}
}

func TestSignatureVerifier_AcceptsWhenAnyLayerVerifies(t *testing.T) {
	f := newSigFixture(t)
	other, _ := cosigntest.GenerateKey()
	f.double.Sign(sigRepo, f.image.GetIndexDigest(),
		f.layer(t, other, f.image.GetIndexDigest()), f.layer(t, f.key, f.image.GetIndexDigest()))
	if err := f.verify(); err != nil {
		t.Fatalf("Verify = %v, want nil", err)
	}
}

func TestSignatureVerifier_MissingTagIsNotSigned(t *testing.T) {
	f := newSigFixture(t)
	sig := requireSignatureCode(t, f.verify(), task.ReasonImageNotSigned)
	want := fmt.Sprintf("image %s carries no cosign signature (%s/%s:%s is not in the registry, and no cosign bundle refers to %s)",
		f.image.GetPinned(), f.double.Host, sigRepo, cosigntest.SignatureTag(f.image.GetIndexDigest()), f.image.GetIndexDigest())
	if sig.Error() != want {
		t.Fatalf("sentence =\n%s\nwant\n%s", sig.Error(), want)
	}
}

// requireNoTerminalControl fails when a refusal holds a character a
// terminal could obey, or does not show want, the escaped form of what the
// registry sent.
func requireNoTerminalControl(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatal("Verify = nil, want a refusal")
	}
	message := err.Error()
	for _, r := range message {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			t.Fatalf("the refusal holds %U raw:\n%q", r, message)
		}
	}
	if !strings.Contains(message, want) {
		t.Fatalf("the refusal shows no %s:\n%s", want, message)
	}
}

// TestSignatureVerifier_ARegistrysOwnWordsReachNoTerminal: text a registry
// sends that a refusal quotes — the Content-Type of a signature object or of
// a referrers index, a host its token challenge names — reaches the
// sentence escaped, shown rather than dropped. (A header cannot carry a C0
// control over HTTP/1.x; a C1 control and a bidirectional override it can.)
func TestSignatureVerifier_ARegistrysOwnWordsReachNoTerminal(t *testing.T) {
	const hostile = "text/plain \u009b31m\u202eevil"
	const shown = `text/plain \u009b31m\u202eevil`
	t.Run("a signature object's content type", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.PutManifest(sigRepo, cosigntest.SignatureTag(f.image.GetIndexDigest()), hostile, []byte("{}"))
		requireNoTerminalControl(t, f.verify(), shown)
	})
	t.Run("a referrers index's content type", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.PutManifest(sigRepo, cosigntest.ReferrersTag(f.image.GetIndexDigest()), hostile, []byte("{}"))
		requireNoTerminalControl(t, f.verify(), shown)
	})
	t.Run("a token realm's host", func(t *testing.T) {
		// The lookup of the realm's host is the test's own, so the failure
		// quotes that host on every platform and resolver, and nothing leaves
		// the process.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Www-Authenticate", "Bearer realm=\"http://ev\u009bil\u202e.test/token\",service=\"x\"")
			w.WriteHeader(http.StatusUnauthorized)
		}))
		t.Cleanup(server.Close)
		f := newSigFixture(t)
		host := strings.TrimPrefix(server.URL, "http://")
		f.image.Registry, f.image.Reference = host, host+"/"+sigRepo+":v3"
		f.image.Pinned = host + "/" + sigRepo + "@" + f.image.GetIndexDigest()
		f.v.Resolver.Client = &http.Client{Transport: failedLookup{registry: host}}
		requireNoTerminalControl(t, f.verify(), `ev\u009bil\u202e.test`)
	})
}

// failedLookup is a network on which only the registry's host resolves: any
// other host fails as a lookup does, quoting the name it was asked.
type failedLookup struct {
	registry string
}

func (l failedLookup) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == l.registry {
		return http.DefaultTransport.RoundTrip(r)
	}
	return nil, &net.OpError{Op: "dial", Net: "tcp",
		Err: &net.DNSError{Err: "no such host", Name: r.URL.Hostname(), IsNotFound: true}}
}

// ---- cosign 3's bundle layout --------------------------------------------------

func (f *sigFixture) bundle(t *testing.T, key *cosigntest.Key, digest string) []byte {
	t.Helper()
	b, err := key.Bundle(digest)
	must.OK(t, err)
	return b
}

// TestSignatureVerifier_AcceptsABundle: what `cosign sign --key` writes by
// default since cosign 3, on a registry without the referrers API (the index
// under the referrers tag) and on one with it.
func TestSignatureVerifier_AcceptsABundle(t *testing.T) {
	t.Run("referrers tag", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), f.bundle(t, f.key, f.image.GetIndexDigest()))
		if err := f.verify(); err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
	})
	t.Run("referrers API", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.ServeReferrersAPI()
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), f.bundle(t, f.key, f.image.GetIndexDigest()))
		// A registry with the referrers API is never asked for the referrers
		// tag: were it, this one would fail the read and the verdict with it.
		f.double.SetStatusAt("/v2/"+sigRepo+"/manifests/"+cosigntest.ReferrersTag(f.image.GetIndexDigest()), http.StatusInternalServerError)
		if err := f.verify(); err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
	})
	t.Run("the legacy layout is read first and still verifies", func(t *testing.T) {
		f := newSigFixture(t)
		other, _ := cosigntest.GenerateKey()
		f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, f.key, f.image.GetIndexDigest()))
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), f.bundle(t, other, f.image.GetIndexDigest()))
		// A signature that verifies in the legacy layout is the verdict: the
		// bundles are not read, so a registry that fails every bundle read
		// (the referrers API and the referrers tag alike) changes nothing.
		f.double.SetStatus("/referrers/", http.StatusInternalServerError)
		f.double.SetStatusAt("/v2/"+sigRepo+"/manifests/"+cosigntest.ReferrersTag(f.image.GetIndexDigest()), http.StatusInternalServerError)
		if err := f.verify(); err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
	})
	t.Run("a bundle by the key beside a legacy signature by another", func(t *testing.T) {
		f := newSigFixture(t)
		other, _ := cosigntest.GenerateKey()
		f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, other, f.image.GetIndexDigest()))
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(),
			f.bundle(t, other, f.image.GetIndexDigest()), f.bundle(t, f.key, f.image.GetIndexDigest()))
		if err := f.verify(); err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
	})
}

// TestSignatureVerifier_RefusesABundleThatDoesNotVerify: every bundle below is
// stored where cosign stores it, and none is a signature by this key over
// this digest.
func TestSignatureVerifier_RefusesABundleThatDoesNotVerify(t *testing.T) {
	other, _ := cosigntest.GenerateKey()
	tests := []struct {
		name   string
		bundle func(t *testing.T, f *sigFixture) []byte
	}{
		{"by another key", func(t *testing.T, f *sigFixture) []byte {
			return f.bundle(t, other, f.image.GetIndexDigest())
		}},
		{"over another digest", func(t *testing.T, f *sigFixture) []byte {
			return f.bundle(t, f.key, cosigntest.Digest([]byte("a different image")))
		}},
		{"an attestation, not a signature", func(t *testing.T, f *sigFixture) []byte {
			b, err := f.key.BundleOver(cosigntest.InTotoPayloadType,
				cosigntest.StatementWith(f.image.GetIndexDigest(), "https://slsa.dev/provenance/v1"))
			must.OK(t, err)
			return b
		}},
		{"another payload type", func(t *testing.T, f *sigFixture) []byte {
			b, err := f.key.BundleOver("application/json", cosigntest.Statement(f.image.GetIndexDigest()))
			must.OK(t, err)
			return b
		}},
		{"another statement type", func(t *testing.T, f *sigFixture) []byte {
			statement := strings.Replace(string(cosigntest.Statement(f.image.GetIndexDigest())),
				cosigntest.StatementType, "https://in-toto.io/Statement/v0.1", 1)
			b, err := f.key.BundleOver(cosigntest.InTotoPayloadType, []byte(statement))
			must.OK(t, err)
			return b
		}},
		{"the payload changed under the signature", func(t *testing.T, f *sigFixture) []byte {
			// The key's own signature, over the statement for another
			// digest, carried with a payload that names this one.
			var signed struct {
				DSSEEnvelope struct {
					Signatures []struct {
						Sig string `json:"sig"`
					} `json:"signatures"`
				} `json:"dsseEnvelope"`
			}
			if err := json.Unmarshal(f.bundle(t, f.key, cosigntest.Digest([]byte("a different image"))), &signed); err != nil {
				t.Fatal(err)
			}
			return cosigntest.EnvelopeBundle(f.key.Hint(), cosigntest.InTotoPayloadType,
				cosigntest.Statement(f.image.GetIndexDigest()), signed.DSSEEnvelope.Signatures[0].Sig)
		}},
		{"no signature", func(t *testing.T, f *sigFixture) []byte {
			return cosigntest.EnvelopeBundle(f.key.Hint(), cosigntest.InTotoPayloadType,
				cosigntest.Statement(f.image.GetIndexDigest()))
		}},
		{"another bundle version", func(t *testing.T, f *sigFixture) []byte {
			return []byte(strings.Replace(string(f.bundle(t, f.key, f.image.GetIndexDigest())),
				cosigntest.BundleType, "application/vnd.dev.sigstore.bundle.v0.2+json", 1))
		}},
		{"not JSON", func(*testing.T, *sigFixture) []byte { return []byte("not a bundle") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSigFixture(t)
			f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), tt.bundle(t, f))
			sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
			want := fmt.Sprintf("image %s has 1 cosign signature(s) and none verifies with key \"release\" (%s) over this digest",
				f.image.GetPinned(), task.SigningKeyPEMFingerprint(f.key.PublicPEM))
			if sig.Error() != want {
				t.Fatalf("sentence =\n%s\nwant\n%s", sig.Error(), want)
			}
		})
	}
}

// TestSignatureVerifier_RefusesATamperedBundleBlob: the registry serves other
// bytes than the referrer manifest's layer names.
func TestSignatureVerifier_RefusesATamperedBundleBlob(t *testing.T) {
	f := newSigFixture(t)
	good := f.bundle(t, f.key, f.image.GetIndexDigest())
	f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), good)
	tampered := append([]byte(nil), good...)
	tampered[len(tampered)-3] ^= 0x01
	f.double.PutBlobAt(sigRepo, cosigntest.Digest(good), tampered)
	requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
}

// TestSignatureVerifier_CountsBothLayouts: the refusal counts every signature
// it read, legacy and bundle alike.
func TestSignatureVerifier_CountsBothLayouts(t *testing.T) {
	f := newSigFixture(t)
	other, _ := cosigntest.GenerateKey()
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, other, f.image.GetIndexDigest()))
	f.double.SignBundle(sigRepo, f.image.GetIndexDigest(),
		f.bundle(t, other, f.image.GetIndexDigest()), f.bundle(t, other, f.image.GetIndexDigest()))
	sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	if !strings.Contains(sig.Detail, "has 3 cosign signature(s)") {
		t.Fatalf("detail = %s", sig.Detail)
	}
}

// TestSignatureVerifier_AnAttestationAloneIsNotASignature: a referrer whose
// descriptor names another predicate, or another artifact type, is not a
// signature; it is never fetched and never counted.
func TestSignatureVerifier_AnAttestationAloneIsNotASignature(t *testing.T) {
	f := newSigFixture(t)
	b, err := f.key.BundleOver(cosigntest.InTotoPayloadType,
		cosigntest.StatementWith(f.image.GetIndexDigest(), "https://slsa.dev/provenance/v1"))
	must.OK(t, err)
	manifest := cosigntest.BundleManifest(b, f.image.GetIndexDigest(), 1024)
	sbom := []byte(`{"an":"sbom"}`)
	f.double.PutBlob(sigRepo, b)
	f.double.PutManifest(sigRepo, cosigntest.Digest(manifest), cosigntest.OCIManifestType, manifest)
	f.double.PutReferrers(sigRepo, f.image.GetIndexDigest(), cosigntest.ReferrersIndex(cosigntest.Referrer{
		Manifest: manifest, ArtifactType: cosigntest.BundleType, PredicateType: "https://slsa.dev/provenance/v1",
	}, cosigntest.Referrer{Manifest: sbom, ArtifactType: "application/spdx+json"}))
	// Neither referrer is fetched: were either, this registry would fail the
	// read, and the verdict would be a registry problem, not "not signed".
	for _, m := range [][]byte{manifest, sbom} {
		f.double.SetStatusAt("/v2/"+sigRepo+"/manifests/"+cosigntest.Digest(m), http.StatusInternalServerError)
	}
	requireSignatureCode(t, f.verify(), task.ReasonImageNotSigned)
}

// TestSignatureVerifier_BundleRegistryFailuresAreUnavailable: any GET the
// bundle path makes that the registry cannot answer is never a verdict.
func TestSignatureVerifier_BundleRegistryFailuresAreUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name, fragment string
		api, exact     bool
	}{
		{"the referrers API", "/referrers/", true, false},
		// Exactly the tag's path: the legacy .sig path starts with it too.
		{"the referrers tag", "", false, true},
		{"the referrer manifest", "/manifests/sha256:", false, false},
		{"the bundle blob", "/blobs/", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSigFixture(t)
			if tt.api {
				f.double.ServeReferrersAPI()
			}
			f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), f.bundle(t, f.key, f.image.GetIndexDigest()))
			if tt.exact {
				f.double.SetStatusAt("/v2/"+sigRepo+"/manifests/"+cosigntest.ReferrersTag(f.image.GetIndexDigest()), http.StatusBadGateway)
			} else {
				f.double.SetStatus(tt.fragment, http.StatusBadGateway)
			}
			sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
			if !strings.Contains(sig.Error(), "this is a registry problem, not a verdict") {
				t.Fatalf("sentence = %s", sig.Error())
			}
		})
	}
	t.Run("unauthorized names the auth file", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.ServeReferrersAPI()
		f.double.SetStatus("/referrers/", http.StatusUnauthorized)
		f.v.AuthFileVar = "ORK_ORKER_REGISTRY_AUTH_FILE"
		sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
		if !strings.Contains(sig.Error(), "ORK_ORKER_REGISTRY_AUTH_FILE") {
			t.Fatalf("sentence %q does not name the auth file variable", sig.Error())
		}
	})
}

// TestSignatureVerifier_BoundsBundles: at most maxSignatureLayers bundles are
// read, and a bundle blob over the payload bound is refused unread.
func TestSignatureVerifier_BoundsBundles(t *testing.T) {
	t.Run("17 bundles", func(t *testing.T) {
		f := newSigFixture(t)
		other, _ := cosigntest.GenerateKey()
		bundles := make([][]byte, 0, 17)
		for i := 0; i < 16; i++ {
			bundles = append(bundles, f.bundle(t, other, f.image.GetIndexDigest()))
		}
		last := f.bundle(t, f.key, f.image.GetIndexDigest())
		bundles = append(bundles, last)
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), bundles...)
		sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
		if !strings.Contains(sig.Detail, "17 cosign signature(s)") {
			t.Fatalf("detail = %s", sig.Detail)
		}
		if n := f.double.CountSuffix("/blobs/" + cosigntest.Digest(last)); n != 0 {
			t.Fatalf("the 17th bundle was fetched %d time(s)", n)
		}
	})
	t.Run("2 MiB bundle", func(t *testing.T) {
		f := newSigFixture(t)
		good := f.bundle(t, f.key, f.image.GetIndexDigest())
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), good)
		f.double.PutBlobAt(sigRepo, cosigntest.Digest(good), make([]byte, 2<<20))
		requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	})
}

func TestSignatureVerifier_WrongKeyIsInvalid(t *testing.T) {
	f := newSigFixture(t)
	other, _ := cosigntest.GenerateKey()
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, other, f.image.GetIndexDigest()))
	sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	fingerprint := task.SigningKeyPEMFingerprint(f.key.PublicPEM)
	want := fmt.Sprintf("image %s has 1 cosign signature(s) and none verifies with key \"release\" (%s) over this digest",
		f.image.GetPinned(), fingerprint)
	if sig.Error() != want || !strings.Contains(sig.Detail, "1 cosign signature(s)") {
		t.Fatalf("sentence =\n%s\nwant\n%s", sig.Error(), want)
	}
}

func TestSignatureVerifier_DigestMismatchIsInvalid(t *testing.T) {
	f := newSigFixture(t)
	elsewhere := cosigntest.Digest([]byte("a different image"))
	// A perfectly valid signature by the right key — over another digest,
	// stored under this digest's tag.
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, f.key, elsewhere))
	requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
}

func TestSignatureVerifier_TamperedPayloadIsInvalid(t *testing.T) {
	f := newSigFixture(t)
	good := f.layer(t, f.key, f.image.GetIndexDigest())
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), good)
	tampered := append([]byte(nil), good.Payload...)
	tampered[len(tampered)-2] ^= 0x01
	f.double.PutBlobAt(sigRepo, cosigntest.Digest(good.Payload), tampered)
	requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
}

func TestSignatureVerifier_RegistryDownIsUnavailableNotAVerdict(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		f := newSigFixture(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		must.OK(t, err)
		host := listener.Addr().String()
		listener.Close()
		f.image.Registry = host
		f.image.Pinned = host + "/" + sigRepo + "@" + f.image.GetIndexDigest()
		sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
		if !strings.Contains(sig.Error(), "this is a registry problem, not a verdict") {
			t.Fatalf("sentence = %s", sig.Error())
		}
	})
	t.Run("503", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.SetStatus("/manifests/", http.StatusServiceUnavailable)
		sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
		want := fmt.Sprintf("image %s could not be checked for a cosign signature: registry %s answered 503; "+
			"this is a registry problem, not a verdict", f.image.GetPinned(), f.double.Host)
		if sig.Error() != want {
			t.Fatalf("sentence =\n%s\nwant\n%s", sig.Error(), want)
		}
	})
	t.Run("blob GET fails", func(t *testing.T) {
		f := newSigFixture(t)
		f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, f.key, f.image.GetIndexDigest()))
		f.double.SetStatus("/blobs/", http.StatusBadGateway)
		requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
	})
}

func TestSignatureVerifier_UnauthorizedIsUnavailable(t *testing.T) {
	f := newSigFixture(t)
	f.double.SetStatus("/manifests/", http.StatusUnauthorized)
	f.v.AuthFileVar = "ORK_ORKER_REGISTRY_AUTH_FILE"
	sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureUnavailable)
	if !strings.Contains(sig.Error(), "ORK_ORKER_REGISTRY_AUTH_FILE") {
		t.Fatalf("sentence %q does not name the auth file variable", sig.Error())
	}
}

func TestSignatureVerifier_BoundsLayersAndPayloads(t *testing.T) {
	t.Run("17 layers", func(t *testing.T) {
		f := newSigFixture(t)
		other, _ := cosigntest.GenerateKey()
		layers := make([]cosigntest.Layer, 0, 17)
		for i := 0; i < 16; i++ {
			l, err := other.SignatureLayer(f.image.GetIndexDigest(), fmt.Sprintf("x/%d", i))
			must.OK(t, err)
			layers = append(layers, l)
		}
		// The 17th is by the RIGHT key: were it fetched, the image would pass.
		last := f.layer(t, f.key, f.image.GetIndexDigest())
		layers = append(layers, last)
		f.double.Sign(sigRepo, f.image.GetIndexDigest(), layers...)
		sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
		if !strings.Contains(sig.Detail, "17 cosign signature(s)") {
			t.Fatalf("detail = %s", sig.Detail)
		}
		if n := f.double.CountSuffix("/blobs/" + cosigntest.Digest(last.Payload)); n != 0 {
			t.Fatalf("the 17th layer was fetched %d time(s)", n)
		}
	})
	t.Run("2 MiB blob", func(t *testing.T) {
		f := newSigFixture(t)
		good := f.layer(t, f.key, f.image.GetIndexDigest())
		f.double.Sign(sigRepo, f.image.GetIndexDigest(), good)
		// The manifest's size is honest (small), the served body is not.
		f.double.PutBlobAt(sigRepo, cosigntest.Digest(good.Payload), make([]byte, 2<<20))
		requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	})
}

func TestSignatureVerifier_MalformedKeyIsNotPermitted(t *testing.T) {
	f := newSigFixture(t)
	// No request leaves for a key that cannot verify anything: were one to,
	// this registry would fail it, and the verdict would be a registry
	// problem, not a refused policy.
	f.double.SetStatus("/v2/", http.StatusInternalServerError)
	err := f.v.Verify(context.Background(), f.image, "cosign:release", "not a pem")
	sig := requireSignatureCode(t, err, task.ReasonImageNotPermitted)
	if !strings.Contains(sig.Error(), `the run's image policy names signer "cosign:release" with no usable key; this is an engine defect, not a workload error`) {
		t.Fatalf("sentence = %s", sig.Error())
	}
}

func TestGetSignatureBlob_FollowsOneRedirectAndBoundsTheBody(t *testing.T) {
	payload := []byte("the payload")
	var cdnAuth []string
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnAuth = append(cdnAuth, r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/big":
			_, _ = w.Write(make([]byte, maxSignaturePayloadBytes+10))
		case "/again":
			http.Redirect(w, r, "/final", http.StatusTemporaryRedirect)
		default:
			_, _ = w.Write(payload)
		}
	}))
	t.Cleanup(cdn.Close)
	target := "/ok"
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, cdn.URL+target, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(registry.Close)
	ref := Reference{Registry: strings.TrimPrefix(registry.URL, "http://"), Repository: sigRepo}
	r := NewResolver("")

	body, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload), new(string))
	if err != nil || string(body) != string(payload) {
		t.Fatalf("one redirect: %q, %v", body, err)
	}
	for _, a := range cdnAuth {
		if a != "" {
			t.Fatalf("a credential followed the redirect to another host: %q", a)
		}
	}
	target = "/big"
	if _, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload), new(string)); !errors.Is(err, errBlobTooLarge) {
		t.Fatalf("oversized body: %v, want errBlobTooLarge", err)
	}
	target = "/again"
	if _, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload), new(string)); err == nil ||
		!strings.Contains(err.Error(), "more than once") {
		t.Fatalf("two redirects: %v, want a refusal", err)
	}
}

// TestSignatureVerifier_ReusesTheTokenItEarned: on a registry that answers
// every read with a bearer challenge, one Verify asks the token endpoint
// once, whatever it reads after: manifests, referrers and blobs alike.
func TestSignatureVerifier_ReusesTheTokenItEarned(t *testing.T) {
	f := newSigFixture(t)
	other, _ := cosigntest.GenerateKey()
	f.double.Sign(sigRepo, f.image.GetIndexDigest(), f.layer(t, other, f.image.GetIndexDigest()))
	f.double.SignBundle(sigRepo, f.image.GetIndexDigest(),
		f.bundle(t, other, f.image.GetIndexDigest()), f.bundle(t, f.key, f.image.GetIndexDigest()))
	var mu sync.Mutex
	tokens := 0
	var proxy *httptest.Server
	proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			mu.Lock()
			tokens++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"token":"t"}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer t" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+proxy.URL+`/token",service="stub"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, f.double.Server.URL+r.URL.RequestURI(), nil)
		if err != nil {
			t.Error(err)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(proxy.Close)
	host := strings.TrimPrefix(proxy.URL, "http://")
	f.image.Registry = host
	f.image.Pinned = host + "/" + sigRepo + "@" + f.image.GetIndexDigest()
	if err := f.verify(); err != nil {
		t.Fatalf("Verify = %v, want nil", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if tokens != 1 {
		t.Fatalf("one Verify asked the token endpoint %d times, want once", tokens)
	}
}

// TestSignatureVerifier_AReferrersIndexItCannotReadIsAVerdict: a referrers
// index that is not one, or is over the manifest bound, is refused as
// IMAGE_SIGNATURE_INVALID naming it, never read as "no signature".
func TestSignatureVerifier_AReferrersIndexItCannotReadIsAVerdict(t *testing.T) {
	for _, tt := range []struct {
		name string
		api  bool
		body []byte
	}{
		{"the referrers API answers an index that is not JSON", true, []byte("not an index")},
		{"the referrers API answers an index over the bound", true,
			append([]byte(`{"manifests":[],"x":"`), bytes.Repeat([]byte("a"), 5<<20)...)},
		{"the referrers tag holds an image manifest", false, cosigntest.Manifest(nil)},
		{"the referrers tag holds an index that is not JSON", false, []byte("not an index")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSigFixture(t)
			if tt.api {
				f.double.ServeReferrersAPI()
				f.double.PutReferrers(sigRepo, f.image.GetIndexDigest(), tt.body)
			} else {
				f.double.PutManifest(sigRepo, cosigntest.ReferrersTag(f.image.GetIndexDigest()), cosigntest.OCIIndexType, tt.body)
			}
			sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
			if !strings.Contains(sig.Detail, "referrers") || !strings.Contains(sig.Detail, "not an image index") {
				t.Fatalf("detail = %s", sig.Detail)
			}
		})
	}
}

// TestSignatureVerifier_AnotherBundleVersionIsASignatureItCannotRead: a
// referrer in a Sigstore bundle version this verifier does not read is
// counted, and the image is IMAGE_SIGNATURE_INVALID, never "not signed".
func TestSignatureVerifier_AnotherBundleVersionIsASignatureItCannotRead(t *testing.T) {
	f := newSigFixture(t)
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	f.double.PutManifest(sigRepo, cosigntest.Digest(manifest), cosigntest.OCIManifestType, manifest)
	f.double.PutReferrers(sigRepo, f.image.GetIndexDigest(), cosigntest.ReferrersIndex(cosigntest.Referrer{
		Manifest: manifest, ArtifactType: "application/vnd.dev.sigstore.bundle.v0.4+json", PredicateType: cosigntest.SignPredicateType,
	}))
	// It is not fetched: were it, this registry would fail the read, and the
	// verdict would be a registry problem, not an invalid signature.
	f.double.SetStatusAt("/v2/"+sigRepo+"/manifests/"+cosigntest.Digest(manifest), http.StatusInternalServerError)
	sig := requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	if !strings.Contains(sig.Detail, "has 1 cosign signature(s)") {
		t.Fatalf("detail = %s", sig.Detail)
	}
}

// TestSignatureVerifier_ReadsOnlyWhatItsDigestNames: a referrer manifest, or a
// bundle blob, served under another object's digest is skipped, even when the
// object served is itself a valid signature by the key over this digest.
func TestSignatureVerifier_ReadsOnlyWhatItsDigestNames(t *testing.T) {
	t.Run("a referrer manifest under another's digest", func(t *testing.T) {
		f := newSigFixture(t)
		listed := f.bundle(t, other(t), f.image.GetIndexDigest())
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), listed)
		valid := f.bundle(t, f.key, f.image.GetIndexDigest())
		f.double.PutBlob(sigRepo, valid)
		served := cosigntest.BundleManifest(valid, f.image.GetIndexDigest(), 1024)
		f.double.PutManifest(sigRepo, cosigntest.Digest(cosigntest.BundleManifest(listed, f.image.GetIndexDigest(), 1024)),
			cosigntest.OCIManifestType, served)
		requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	})
	t.Run("a bundle blob under another's digest", func(t *testing.T) {
		f := newSigFixture(t)
		listed := f.bundle(t, other(t), f.image.GetIndexDigest())
		f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), listed)
		f.double.PutBlobAt(sigRepo, cosigntest.Digest(listed), f.bundle(t, f.key, f.image.GetIndexDigest()))
		requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	})
}

func other(t *testing.T) *cosigntest.Key {
	t.Helper()
	k, err := cosigntest.GenerateKey()
	must.OK(t, err)
	return k
}

// TestSignatureVerifier_TriesAtMostSixteenEnvelopeSignatures: one envelope's
// signatures are bounded like a manifest's layers; a valid seventeenth is
// never tried.
func TestSignatureVerifier_TriesAtMostSixteenEnvelopeSignatures(t *testing.T) {
	f := newSigFixture(t)
	statement := cosigntest.Statement(f.image.GetIndexDigest())
	junk := sigOfFor(t, other(t), f.image.GetIndexDigest())
	sigs := make([]string, 0, 17)
	for i := 0; i < 16; i++ {
		sigs = append(sigs, junk)
	}
	f.double.SignBundle(sigRepo, f.image.GetIndexDigest(),
		cosigntest.EnvelopeBundle(f.key.Hint(), cosigntest.InTotoPayloadType, statement, append(sigs, sigOfFor(t, f.key, f.image.GetIndexDigest()))...))
	requireSignatureCode(t, f.verify(), task.ReasonImageSignatureInvalid)
	// The same key's signature sixteenth verifies: the bound, not the key.
	g := newSigFixture(t)
	g.key = f.key
	g.double.SignBundle(sigRepo, g.image.GetIndexDigest(), cosigntest.EnvelopeBundle(f.key.Hint(), cosigntest.InTotoPayloadType,
		cosigntest.Statement(g.image.GetIndexDigest()), append(sigs[:15], sigOfFor(t, f.key, g.image.GetIndexDigest()))...))
	if err := g.verify(); err != nil {
		t.Fatalf("a valid sixteenth signature: Verify = %v, want nil", err)
	}
}

// sigOfFor is the envelope signature k makes over the statement for digest.
func sigOfFor(t *testing.T, k *cosigntest.Key, digest string) string {
	t.Helper()
	var b struct {
		DSSEEnvelope struct {
			Signatures []struct {
				Sig string `json:"sig"`
			} `json:"signatures"`
		} `json:"dsseEnvelope"`
	}
	signed, err := k.Bundle(digest)
	if err != nil || json.Unmarshal(signed, &b) != nil {
		t.Fatal(err)
	}
	return b.DSSEEnvelope.Signatures[0].Sig
}

// TestSignatureVerifier_FallsBackToTheReferrersTag: a referrers endpoint that
// answers 400, or 200 with something other than an image index, means the
// registry has no referrers API, and the referrers tag is read instead.
func TestSignatureVerifier_FallsBackToTheReferrersTag(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			f := newSigFixture(t)
			f.double.SignBundle(sigRepo, f.image.GetIndexDigest(), f.bundle(t, f.key, f.image.GetIndexDigest()))
			f.double.SetStatus("/referrers/", status)
			// The bundle is stored only where the referrers tag lists it, so
			// a verified image is the proof that the tag was read.
			if err := f.verify(); err != nil {
				t.Fatalf("Verify = %v, want nil", err)
			}
		})
	}
}
