package dsl

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	want := fmt.Sprintf("image %s carries no cosign signature (%s/%s:%s is not in the registry)",
		f.image.GetPinned(), f.double.Host, sigRepo, cosigntest.SignatureTag(f.image.GetIndexDigest()))
	if sig.Error() != want {
		t.Fatalf("sentence =\n%s\nwant\n%s", sig.Error(), want)
	}
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
		if err != nil {
			t.Fatal(err)
		}
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
			if err != nil {
				t.Fatal(err)
			}
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
	err := f.v.Verify(context.Background(), f.image, "cosign:release", "not a pem")
	sig := requireSignatureCode(t, err, task.ReasonImageNotPermitted)
	if !strings.Contains(sig.Error(), `the run's image policy names signer "cosign:release" with no usable key; this is an engine defect, not a workload error`) {
		t.Fatalf("sentence = %s", sig.Error())
	}
	if len(f.double.Requests()) != 0 {
		t.Fatalf("a malformed key reached the registry: %v", f.double.Requests())
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

	body, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload))
	if err != nil || string(body) != string(payload) {
		t.Fatalf("one redirect: %q, %v", body, err)
	}
	for _, a := range cdnAuth {
		if a != "" {
			t.Fatalf("a credential followed the redirect to another host: %q", a)
		}
	}
	target = "/big"
	if _, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload)); !errors.Is(err, errBlobTooLarge) {
		t.Fatalf("oversized body: %v, want errBlobTooLarge", err)
	}
	target = "/again"
	if _, err := r.getSignatureBlob(context.Background(), ref, cosigntest.Digest(payload)); err == nil ||
		!strings.Contains(err.Error(), "more than once") {
		t.Fatalf("two redirects: %v, want a refusal", err)
	}
}
