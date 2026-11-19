//go:build integration

package api

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/dextest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_SigninWithDex: OrkStack's web sign-in against a real
// OpenID Connect provider, Dex (deploy/dex-image.txt, deploy/dex/config.yaml),
// not the stub the unit tests drive. Discovery is accepted (https endpoints,
// RS256, PKCE with S256); the authorization code flow with PKCE signs a
// person in through Dex's own login form; the ID token's claims become the
// principal (the email as its name, verified, and its groups); the sign-in is
// refreshed with Dex's refresh token, twice, so a rotated token is redeemed;
// and signing out ends it. Dex offers no token revocation and no
// end-session endpoint, so signing out ends the sign-in here alone.
func TestIntegration_SigninWithDex(t *testing.T) {
	ctx := context.Background()
	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	dex := startDex(t, engineURL+"/v1/auth/callback/people")
	idp, err := dex.Client()
	if err != nil {
		t.Fatal(err)
	}

	// Discovery: what the engine requires of an issuer, from a real one.
	meta := dexDiscovery(t, idp, dex.Issuer)
	if !slices.Contains(meta.CodeChallengeMethods, "S256") {
		t.Fatalf("Dex offers PKCE methods %v, without S256", meta.CodeChallengeMethods)
	}
	if meta.RevocationEndpoint != "" {
		t.Errorf("Dex advertises a revocation_endpoint %q; this test and docs/AUTHENTICATION.md say it offers none", meta.RevocationEndpoint)
	}
	if meta.EndSessionEndpoint != "" {
		t.Errorf("Dex advertises an end_session_endpoint %q; this test and docs/AUTHENTICATION.md say it offers none", meta.EndSessionEndpoint)
	}

	issuers, err := authn.ParseIssuers([]byte(`issuers:
  people:
    issuer: ` + dex.Issuer + `
    kind: user
    audiences:
      - ` + dextest.WebClientID + `
      - ` + dextest.CLIClientID + `
    algorithms:
      - RS256
    username_claim: email
    require_verified_email: true
    groups_claim: groups
    max_lifetime: 2h
    login:
      client_id: ` + dextest.WebClientID + `
      client_secret:
        source: env:ORK_DEX_WEB_SECRET
      cli_client_id: ` + dextest.CLIClientID + `
      device_flow: true
      scopes:
        - openid
        - email
        - groups
        - offline_access
`))
	if err != nil {
		t.Fatal(err)
	}
	people, _ := issuers.ByName("people")
	keys, err := authn.NewKeyCache(people, idp, nil)
	if err != nil {
		t.Fatal(err)
	}
	public, _ := url.Parse(engineURL)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "admins", Role: authz.RoleAdmin, Scope: "deployment",
		Subjects: []string{"group:people:admins"}})
	a.Authenticator = &authn.Authenticator{Issuers: issuers, Keys: map[string]jwt.KeySource{"people": keys}}
	var skew atomic.Int64
	a.Signins = &authn.Signins{Issuers: issuers, Keys: map[string]jwt.KeySource{"people": keys}, Sealer: signinKey(t),
		Records: artifacts.NewMemorySigninStore(nil), PublicURL: public, Leeway: time.Minute,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) },
		Secret: func(source string) (string, error) {
			if source != "env:ORK_DEX_WEB_SECRET" {
				return "", errors.New("unknown source")
			}
			return dex.WebSecret, nil
		},
		HTTPClient: func(*authn.Issuer) (*http.Client, error) { return idp, nil }}
	a.Signins.Settings.RevalidateInterval = time.Minute
	engine.Config.Handler = NewMux(newAccessService(t), MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)

	browser := dexBrowser(t, idp, engine.Certificate())
	origin, token := engineURL, ""
	opts := connect.WithInterceptors(csrfHeader{origin: &origin, token: &token})
	auth := orkstackv1connect.NewAuthServiceClient(browser, engineURL, opts)
	access := orkstackv1connect.NewAccessServiceClient(browser, engineURL, opts)

	// The authorization code flow with PKCE, through Dex's login form.
	landed := dexLogin(t, browser, engineURL, "/runs?ns=default", dextest.AdminUser)
	if landed.Path != "/runs" || landed.Query().Get("ns") != "default" {
		t.Fatalf("signed in to %s, want /runs?ns=default", landed)
	}

	// The ID token's claims, as the principal.
	who, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		t.Fatalf("GetSignin after signing in at Dex: %v", err)
	}
	if got := who.Msg.GetPrincipalId(); got != "user:people:"+dextest.AdminUser {
		t.Errorf("principal %q, want user:people:%s", got, dextest.AdminUser)
	}
	if !slices.Equal(who.Msg.GetGroups(), []string{"admins"}) || who.Msg.GetMethod() != authn.MethodOIDC || who.Msg.GetIssuer() != "people" {
		t.Errorf("GetSignin = groups %v, method %q, issuer %q", who.Msg.GetGroups(), who.Msg.GetMethod(), who.Msg.GetIssuer())
	}
	token = who.Msg.GetCsrfToken()
	if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); err != nil {
		t.Fatalf("an admin's call, bound by Dex's groups claim: %v", err)
	}

	// Refresh: past revalidation every call is refused stale until the page
	// refreshes the sign-in with Dex's refresh token. Twice, so the token
	// Dex rotated on the first is the one the second redeems.
	for i, past := range []time.Duration{2 * time.Minute, 4 * time.Minute} {
		skew.Store(int64(past))
		_, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated || !strings.Contains(err.Error(), authn.ReasonSigninStale) {
			t.Fatalf("refresh %d: a call past revalidation = %v, want %s", i+1, err, authn.ReasonSigninStale)
		}
		if _, err := auth.RefreshSignin(ctx, connect.NewRequest(&orkstackv1.RefreshSigninRequest{})); err != nil {
			t.Fatalf("refresh %d: RefreshSignin with Dex's refresh token: %v", i+1, err)
		}
		if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); err != nil {
			t.Fatalf("refresh %d: a call after the refresh: %v", i+1, err)
		}
	}

	// Sign out: the sign-in ends here, and there is no end-session URL to
	// send the browser to.
	out, err := auth.SignOut(ctx, connect.NewRequest(&orkstackv1.SignOutRequest{}))
	if err != nil {
		t.Fatalf("SignOut: %v", err)
	}
	if end := out.Msg.GetEndSessionUrl(); end != "" {
		t.Errorf("SignOut's end-session URL %q, from an issuer that offers none", end)
	}
	if _, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("GetSignin after signing out = %v, want unauthenticated", err)
	}
}

// startDex starts a Dex for this test, its container labelled with this
// process so a sweep stops it should the test die before its cleanup.
func startDex(t *testing.T, redirect string) *dextest.Dex {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := dextest.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d, err := dextest.Start(ctx, dextest.Options{Root: root, Dir: t.TempDir(), Addr: "127.0.0.1:0", WebRedirect: redirect,
		ProofID: proofID(), Owner: dextest.ProcessOwner()})
	if errors.Is(err, dextest.ErrNoImage) {
		image, _ := dextest.Image(root)
		dexAbsent(t, "%v; docker pull %s (the proof host's setup pulls it)", err, image)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Dex's own account of a failure, before --rm removes it with its log.
		if t.Failed() {
			if out, err := exec.Command("docker", "logs", "--tail", "80", d.ID).CombinedOutput(); err == nil {
				t.Logf("Dex's log:\n%s", out)
			}
		}
		if err := dextest.Stop(context.Background(), d.ID); err != nil {
			t.Errorf("stop Dex: %v", err)
		}
	})
	return d
}

// proofID is the proof this run belongs to, or one of its own, so the
// container's orkstack.proof label always names something.
func proofID() string {
	if id := os.Getenv("ORK_PROOF_ID"); id != "" {
		return id
	}
	return "test-" + strconv.Itoa(os.Getpid())
}

// dexAbsent is a missing prerequisite: a skip, or a failure under
// ORK_REQUIRE_INFRA (make test-integration, the proofs).
func dexAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

func dexDiscovery(t *testing.T, c *http.Client, issuer string) authn.ProviderMetadata {
	t.Helper()
	resp, err := c.Get(issuer + "/.well-known/openid-configuration")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := authn.CheckProviderMetadata(raw, issuer)
	if err != nil {
		t.Fatalf("the engine refuses Dex's discovery: %v", err)
	}
	return meta
}

func signinKey(t *testing.T) *authn.SigninKeys {
	t.Helper()
	raw := make([]byte, authn.SigninKeySize)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "signin.key")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := authn.LoadSigninKeys([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// dexBrowser is a client with a cookie jar that trusts Dex's CA and the
// engine's certificate.
func dexBrowser(t *testing.T, idp *http.Client, engineCert *x509.Certificate) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := idp.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.RootCAs.AddCert(engineCert)
	return &http.Client{Jar: jar, Transport: transport, Timeout: 30 * time.Second}
}

var loginForm = regexp.MustCompile(`<form[^>]*method="post"[^>]*action="([^"]+)"`)

// dexLogin starts a sign-in at the engine, fills Dex's login form as email
// with the configured password, and returns where the browser landed: the
// return path on the engine, which this test's engine serves no page at.
func dexLogin(t *testing.T, browser *http.Client, engineURL, ret, email string) *url.URL {
	t.Helper()
	return dexLoginAs(t, browser, engineURL, ret, email, dextest.Password)
}

// dexLoginAs is dexLogin with the password given.
func dexLoginAs(t *testing.T, browser *http.Client, engineURL, ret, email, password string) *url.URL {
	t.Helper()
	resp, err := browser.Get(engineURL + "/v1/auth/login/people?return=" + url.QueryEscape(ret))
	if err != nil {
		t.Fatalf("start the sign-in: %v", err)
	}
	page, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	m := loginForm.FindSubmatch(page)
	if m == nil {
		t.Fatalf("no login form at %s (%s):\n%.2000s", resp.Request.URL, resp.Status, page)
	}
	action, err := resp.Request.URL.Parse(html.UnescapeString(string(m[1])))
	if err != nil {
		t.Fatal(err)
	}
	resp, err = browser.PostForm(action.String(), url.Values{"login": {email}, "password": {password}})
	if err != nil {
		t.Fatalf("submit Dex's login form: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.HasPrefix(resp.Request.URL.String(), engineURL) {
		t.Fatalf("after Dex's login the browser is at %s (%s), not the engine:\n%.2000s", resp.Request.URL, resp.Status, body)
	}
	return resp.Request.URL
}
