// Package signintest stands up what a test of web sign-in needs: an
// oidctest provider served over TLS, its trusted-issuer entry named "dev"
// with a login, the key cache, a sign-in key and a record store, and a
// browser with a cookie jar that trusts both servers.
package signintest

import (
	"crypto/rand"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The issuer's name and its clients.
const (
	Issuer       = "dev"
	WebClientID  = "orkstack-web"
	WebSecret    = "web-secret"
	CLIClientID  = "orkctl"
	secretSource = "env:ORK_SIGNINTEST_WEB_SECRET"
)

// Env is one provider and what an engine needs to trust it.
type Env struct {
	Provider *oidctest.Provider
	IDP      *httptest.Server
	Issuers  authn.Issuers
	Keys     map[string]jwt.KeySource
	Sealer   *authn.SigninKeys
	Store    *artifacts.SigninStore
}

// New serves a provider over TLS whose web client redirects to engineURL's
// callback and whose CLI client redirects to a loopback port.
func New(t testing.TB, engineURL string, opts oidctest.Options) *Env {
	t.Helper()
	idp := httptest.NewUnstartedServer(nil)
	issuerURL := "https://" + idp.Listener.Addr().String()
	opts.Clients = append(opts.Clients,
		oidctest.Client{ID: WebClientID, Secret: WebSecret, RedirectURIs: []string{engineURL + "/v1/auth/callback/" + Issuer}},
		oidctest.Client{ID: CLIClientID, RedirectURIs: []string{"http://127.0.0.1/callback"}})
	if opts.User.Subject == "" {
		opts.User = oidctest.User{Subject: "u-ana", Email: "ana@example.test", Groups: []string{"ops"}}
	}
	p, err := oidctest.New(issuerURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	idp.Config.Handler = p
	idp.StartTLS()
	t.Cleanup(idp.Close)
	issuers, err := authn.ParseIssuers([]byte(`issuers:
  ` + Issuer + `:
    issuer: ` + issuerURL + `
    kind: user
    audiences:
      - ` + WebClientID + `
      - ` + CLIClientID + `
    algorithms:
      - ES256
    username_claim: email
    require_verified_email: true
    groups_claim: groups
    max_lifetime: 1h
    login:
      client_id: ` + WebClientID + `
      client_secret:
        source: ` + secretSource + `
      cli_client_id: ` + CLIClientID + `
      device_flow: true
      scopes:
        - openid
        - email
`))
	if err != nil {
		t.Fatal(err)
	}
	dev, _ := issuers.ByName(Issuer)
	keys, err := authn.NewKeyCache(dev, idp.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, authn.SigninKeySize)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "signin.key")
	if err := os.WriteFile(keyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	sealer, err := authn.LoadSigninKeys([]string{keyPath})
	if err != nil {
		t.Fatal(err)
	}
	return &Env{Provider: p, IDP: idp, Issuers: issuers, Keys: map[string]jwt.KeySource{Issuer: keys}, Sealer: sealer,
		Store: artifacts.NewMemorySigninStore(nil)}
}

// Signins is sign-in for an engine at public, trusting the provider.
func (e *Env) Signins(public *url.URL, now func() time.Time) *authn.Signins {
	return &authn.Signins{Issuers: e.Issuers, Keys: e.Keys, Sealer: e.Sealer, Records: e.Store, PublicURL: public,
		Leeway: time.Minute, Now: now,
		Secret: func(source string) (string, error) {
			if source != secretSource {
				return "", errors.New("unknown source")
			}
			return WebSecret, nil
		},
		HTTPClient: func(*authn.Issuer) (*http.Client, error) { return e.IDP.Client(), nil }}
}

// Browser is a client with a cookie jar that trusts the provider and,
// when given, the engine's certificate.
func (e *Env) Browser(engineCert *x509.Certificate) *http.Client {
	jar, _ := cookiejar.New(nil)
	transport := e.IDP.Client().Transport.(*http.Transport).Clone()
	if engineCert != nil {
		transport.TLSClientConfig.RootCAs.AddCert(engineCert)
	}
	return &http.Client{Jar: jar, Transport: transport}
}
