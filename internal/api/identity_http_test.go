package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// noLiveIssuer serves no key: nothing in these tests needs a key active.
type noLiveIssuer struct{}

func (noLiveIssuer) PublishedKeyIDs(context.Context) (map[string]bool, error) { return nil, nil }

func testIssuer(t testing.TB, trustDomain string) *identity.Issuer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	store, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	i, err := identity.New(identity.Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256,
		ActivationDelay: time.Hour, VerifyPublished: true, TrustDomain: trustDomain, Engine: "e1"}, store, noLiveIssuer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	return i
}

// testKeysOnly is an engine's signing keys with no issuer URL: they sign
// what engines hand each other, no token, and serve nothing.
func testKeysOnly(t testing.TB) *identity.Issuer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	store, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	i, err := identity.New(identity.Config{Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256, Engine: "e1"}, store, noLiveIssuer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	return i
}

func getBody(t *testing.T, h http.Handler, method, path string) (int, http.Header, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, rec.Header(), string(body)
}

// TestIssuerRoutes_ServeTheIssuersDocuments: with discovery served, every
// engine answers the discovery document, the key set and, with a trust
// domain, the SPIFFE bundle, exactly as the issuer builds them, to a caller
// with no credential even where every other request needs one; without a
// trust domain the bundle is not found, and other methods are refused.
func TestIssuerRoutes_ServeTheIssuersDocuments(t *testing.T) {
	for _, mode := range []string{authn.ModeAsserted, authn.ModeEnforce} {
		t.Run(mode, func(t *testing.T) {
			s := newAccessService(t)
			s.issuer, s.serveDiscovery = testIssuer(t, "ork.acme.dev"), true
			opts := MuxOptions{}
			if mode == authn.ModeEnforce {
				opts.Access, _ = accessFor(t, mode, nil)
				opts.Access.Authenticator = &authn.Authenticator{}
			}
			mux := NewMux(s, opts)
			want, _ := s.issuer.Discovery()
			jwks, _ := s.issuer.JWKS()
			bundle, _ := s.issuer.Bundle()
			for path, body := range map[string][]byte{
				IssuerDiscoveryPath: want, IssuerKeysPath: jwks, IssuerBundlePath: bundle,
			} {
				code, h, got := getBody(t, mux, http.MethodGet, path)
				if code != http.StatusOK || got != string(body) || h.Get("Content-Type") != "application/json" || h.Get("Cache-Control") == "" {
					t.Errorf("GET %s = %d %q %v", path, code, got, h)
				}
				if code, _, _ := getBody(t, mux, http.MethodPost, path); code != http.StatusMethodNotAllowed {
					t.Errorf("POST %s = %d", path, code)
				}
			}
		})
	}
	s := newAccessService(t)
	s.issuer, s.serveDiscovery = testIssuer(t, ""), true
	mux := NewMux(s, MuxOptions{})
	if code, _, _ := getBody(t, mux, http.MethodGet, IssuerBundlePath); code != http.StatusNotFound {
		t.Errorf("a bundle with no trust domain = %d", code)
	}
}

// TestIssuerRoutes_NotServedUnlessAsked: with serve_discovery off, or no
// issuer (signing keys or not), the routes are not found.
func TestIssuerRoutes_NotServedUnlessAsked(t *testing.T) {
	s := newAccessService(t)
	s.issuer = testIssuer(t, "ork.acme.dev")
	off := NewMux(s, MuxOptions{})
	noIssuer := newAccessService(t)
	noIssuer.serveDiscovery = true
	none := NewMux(noIssuer, MuxOptions{})
	keysOnly := newAccessService(t)
	keysOnly.issuer, keysOnly.serveDiscovery = testKeysOnly(t), true
	keys := NewMux(keysOnly, MuxOptions{})
	for _, path := range []string{IssuerDiscoveryPath, IssuerKeysPath, IssuerBundlePath} {
		for name, mux := range map[string]http.Handler{"off": off, "no issuer": none, "keys with no issuer": keys} {
			if code, _, _ := getBody(t, mux, http.MethodGet, path); code != http.StatusNotFound {
				t.Errorf("%s: GET %s = %d", name, path, code)
			}
		}
	}
}

// TestGetSystemInfo_IdentityIssuer: an engine with an issuer names it, and
// one without answers it unset.
func TestGetSystemInfo_IdentityIssuer(t *testing.T) {
	s := newAccessService(t)
	resp, err := s.GetSystemInfo(context.Background(), connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil || resp.Msg.GetIdentityIssuer() != "" {
		t.Fatalf("no issuer: %q, %v", resp.Msg.GetIdentityIssuer(), err)
	}
	s.issuer = testIssuer(t, "")
	resp, err = s.GetSystemInfo(context.Background(), connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil || resp.Msg.GetIdentityIssuer() != "https://ork-id.acme.dev" {
		t.Fatalf("issuer: %q, %v", resp.Msg.GetIdentityIssuer(), err)
	}
}

// TestIssuerRoutes_NothingBeforeTheFirstRead: an engine that has not read
// the published keys answers the key set and bundle 503, never an empty set
// a relying party would cache, and GetIssuer leaves them empty.
func TestIssuerRoutes_NothingBeforeTheFirstRead(t *testing.T) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	signer, _ := jwt.NewSigner(k)
	store, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	i, err := identity.New(identity.Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256,
		TrustDomain: "ork.acme.dev", Engine: "e1"}, store, noLiveIssuer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := newAccessService(t)
	s.issuer, s.serveDiscovery = i, true
	mux := NewMux(s, MuxOptions{})
	for _, path := range []string{IssuerKeysPath, IssuerBundlePath} {
		if code, _, body := getBody(t, mux, http.MethodGet, path); code != http.StatusServiceUnavailable {
			t.Errorf("GET %s = %d %s", path, code, body)
		}
	}
	resp, err := s.GetIssuer(context.Background(), connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if err != nil || resp.Msg.GetJwks() != "" || resp.Msg.GetSpiffeBundle() != "" || resp.Msg.GetKeysReadAt() != nil {
		t.Fatalf("GetIssuer = %v, %v", resp.Msg, err)
	}
}
