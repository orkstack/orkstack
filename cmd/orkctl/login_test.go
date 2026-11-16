package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
	"github.com/orkstack/orkstack/internal/testsupport/signintest"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
)

// No test reads the developer's own logins.
func init() {
	if os.Getenv(credentialsEnvVar) == "" {
		_ = os.Setenv(credentialsEnvVar, filepath.Join(os.TempDir(), fmt.Sprintf("orkctl-test-credentials-%d.json", os.Getpid())))
	}
}

// loginEngine answers GetAuthConfig with one issuer and GetSignin with the
// bearer it was sent.
type loginEngine struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	issuer string
	mu     sync.Mutex
	seen   []string
}

func (e *loginEngine) GetAuthConfig(context.Context, *connect.Request[orkstackv1.GetAuthConfigRequest]) (*connect.Response[orkstackv1.GetAuthConfigResponse], error) {
	return connect.NewResponse(&orkstackv1.GetAuthConfigResponse{Mode: "enforce", Issuers: []*orkstackv1.SigninIssuer{{
		Name: signintest.Issuer, IssuerUrl: e.issuer, LoginPath: "/v1/auth/login/dev", CliClientId: signintest.CLIClientID,
		Scopes: []string{"openid", "email"}, DeviceFlow: true}}}), nil
}

func (e *loginEngine) GetSignin(_ context.Context, req *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	bearer := req.Header().Get("Authorization")
	e.mu.Lock()
	e.seen = append(e.seen, bearer)
	e.mu.Unlock()
	if !strings.HasPrefix(bearer, "Bearer ey") {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&orkstackv1.GetSigninResponse{PrincipalId: "user:dev:ana@example.test"}), nil
}

func loginFixture(t *testing.T, opts oidctest.Options) (*signintest.Env, *loginEngine, string, string) {
	t.Helper()
	t.Setenv(credentialsEnvVar, filepath.Join(t.TempDir(), "credentials.json"))
	opts.TokenLifetime = 2 * time.Minute
	env := signintest.New(t, "https://ork.example.test", opts)
	engine := &loginEngine{issuer: env.Provider.Issuer()}
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAuthServiceHandler(engine))
	url := startFakeEngine(t, mux)
	ca := testpki.Write(t, t.TempDir(), "idp.pem", pemOf(env.IDP.Certificate().Raw))
	return env, engine, url, ca
}

func TestLogin_ABrowserSignsTheTerminalIn(t *testing.T) {
	env, engine, url, ca := loginFixture(t, oidctest.Options{})
	browser := env.Browser(nil)
	orig := openBrowser
	t.Cleanup(func() { openBrowser = orig })
	openBrowser = func(u string) error {
		go func() {
			resp, err := browser.Get(u)
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
		return nil
	}
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--server", url, "--issuer-ca-file", ca}, &out, &errOut); code != exitOK {
		t.Fatalf("login = %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "Signed in to "+url+" as user:dev:ana@example.test") {
		t.Fatalf("login said %q", out.String())
	}
	info, err := os.Stat(os.Getenv(credentialsEnvVar))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credentials file: %v %v", info, err)
	}
	// Another command sends the stored ID token.
	out.Reset()
	if code := Execute([]string{"whoami", "--server", url}, &out, &errOut); code != exitOK {
		t.Fatalf("whoami after login = %d: %s", code, errOut.String())
	}
	engine.mu.Lock()
	last := engine.seen[len(engine.seen)-1]
	engine.mu.Unlock()
	if !strings.HasPrefix(last, "Bearer ey") {
		t.Fatalf("whoami sent %q", last)
	}
	// Within a minute of expiry the login is refreshed first.
	f, path, _ := loadCredentials()
	login := f.Servers[serverKey(url)]
	login.ExpiresAt = time.Now().Add(30 * time.Second)
	f.Servers[serverKey(url)] = login
	if err := saveCredentials(f, path); err != nil {
		t.Fatal(err)
	}
	if code := Execute([]string{"whoami", "--server", url}, &out, &errOut); code != exitOK {
		t.Fatalf("whoami near expiry = %d: %s", code, errOut.String())
	}
	if env.Provider.Refreshes() != 1 {
		t.Errorf("refreshes = %d; want the login refreshed once", env.Provider.Refreshes())
	}
	// logout forgets it and revokes the refresh token.
	out.Reset()
	if code := Execute([]string{"logout", "--server", url}, &out, &errOut); code != exitOK || !strings.Contains(out.String(), "Logged out") {
		t.Fatalf("logout = %d %q %s", code, out.String(), errOut.String())
	}
	if env.Provider.Revocations() != 1 {
		t.Errorf("revocations = %d", env.Provider.Revocations())
	}
	if f, _, _ := loadCredentials(); len(f.Servers) != 0 {
		t.Errorf("logins after logout = %v", f.Servers)
	}
}

func TestLogin_TheDeviceGrant(t *testing.T) {
	_, _, url, ca := loginFixture(t, oidctest.Options{AutoApproveDevice: true})
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--device", "--server", url, "--issuer-ca-file", ca}, &out, &errOut); code != exitOK {
		t.Fatalf("login --device = %d: %s", code, errOut.String())
	}
	if !strings.Contains(errOut.String(), "enter the code") || !strings.Contains(out.String(), "Signed in") {
		t.Fatalf("login --device said %q / %q", out.String(), errOut.String())
	}
}

func TestLogin_Refusals(t *testing.T) {
	_, _, url, ca := loginFixture(t, oidctest.Options{})
	var out, errOut strings.Builder
	for name, args := range map[string][]string{
		"plain http to a remote server": {"login", "--server", "http://engine.example.com:8080"},
		"an issuer it does not offer":   {"login", "--server", url, "--issuer", "corp", "--issuer-ca-file", ca},
		"an issuer it cannot reach":     {"login", "--server", url, "--no-browser"},
	} {
		errOut.Reset()
		if code := Execute(args, &out, &errOut); code == exitOK {
			t.Errorf("%s: logged in", name)
		}
	}
	path := os.Getenv(credentialsEnvVar)
	if err := os.WriteFile(path, []byte(`{"servers":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadCredentials(); err == nil || !strings.Contains(err.Error(), "readable by others") {
		t.Errorf("a world-readable credentials file = %v", err)
	}
}

func TestPickIssuer(t *testing.T) {
	a := &orkstackv1.SigninIssuer{Name: "a", CliClientId: "c"}
	b := &orkstackv1.SigninIssuer{Name: "b"}
	if got, err := pickIssuer([]*orkstackv1.SigninIssuer{a}, ""); err != nil || got != a {
		t.Errorf("the only one = %v %v", got, err)
	}
	for name, tc := range map[string]struct {
		in   []*orkstackv1.SigninIssuer
		name string
	}{
		"none":               {nil, ""},
		"several, unnamed":   {[]*orkstackv1.SigninIssuer{a, b}, ""},
		"one not offered":    {[]*orkstackv1.SigninIssuer{a}, "z"},
		"one with no CLI id": {[]*orkstackv1.SigninIssuer{a, b}, "b"},
	} {
		if _, err := pickIssuer(tc.in, tc.name); err == nil {
			t.Errorf("%s: picked", name)
		}
	}
}

func pemOf(der []byte) []byte {
	return []byte("-----BEGIN CERTIFICATE-----\n" + base64Lines(der) + "-----END CERTIFICATE-----\n")
}

func base64Lines(der []byte) string {
	s := base64.StdEncoding.EncodeToString(der)
	var b strings.Builder
	for len(s) > 64 {
		b.WriteString(s[:64] + "\n")
		s = s[64:]
	}
	b.WriteString(s + "\n")
	return b.String()
}

func FuzzTokenExpiry(f *testing.F) {
	f.Add("eyJhbGciOiJFUzI1NiJ9.eyJleHAiOjE3MDAwMDAwMDB9.c2ln")
	f.Add("a.b.c")
	f.Add("")
	f.Fuzz(func(t *testing.T, token string) {
		// Property: a time is answered only for a three-part token whose
		// payload states a non-zero exp, and never panics.
		exp, err := tokenExpiry(token)
		if err == nil && (exp.IsZero() || strings.Count(token, ".") != 2) {
			t.Fatalf("tokenExpiry(%q) = %v", token, exp)
		}
	})
}

// TestLogin_AStrayRedirectDoesNotEndIt: a request to the loopback redirect
// with another state is refused, and the login still completes.
func TestLogin_AStrayRedirectDoesNotEndIt(t *testing.T) {
	env, _, url, ca := loginFixture(t, oidctest.Options{})
	browser := env.Browser(nil)
	orig := openBrowser
	t.Cleanup(func() { openBrowser = orig })
	openBrowser = func(u string) error {
		go func() {
			redirect := strings.SplitN(strings.SplitN(u, "redirect_uri=", 2)[1], "&", 2)[0]
			stray, _ := neturl.QueryUnescape(redirect)
			if resp, err := http.Get(stray + "?state=wrong&code=x"); err == nil {
				_ = resp.Body.Close()
			}
			if resp, err := browser.Get(u); err == nil {
				_ = resp.Body.Close()
			}
		}()
		return nil
	}
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--server", url, "--issuer-ca-file", ca}, &out, &errOut); code != exitOK {
		t.Fatalf("login after a stray redirect = %d: %s", code, errOut.String())
	}
}

// TestLogin_ConcurrentCommandsRefreshOnce: commands near a login's expiry,
// at once, redeem its rotating refresh token once.
func TestLogin_ConcurrentCommandsRefreshOnce(t *testing.T) {
	env, _, url, ca := loginFixture(t, oidctest.Options{AutoApproveDevice: true})
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--device", "--server", url, "--issuer-ca-file", ca}, &out, &errOut); code != exitOK {
		t.Fatal(errOut.String())
	}
	err := withCredentials(func(f credentialsFile, path string) error {
		l := f.Servers[serverKey(url)]
		l.ExpiresAt = time.Now()
		f.Servers[serverKey(url)] = l
		return saveCredentials(f, path)
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := (loginBearer{server: url}).token(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := env.Provider.Refreshes(); n != 1 {
		t.Errorf("refreshes = %d; want one", n)
	}
}

func TestLogin_RefusesAnotherCredential(t *testing.T) {
	_, _, url, ca := loginFixture(t, oidctest.Options{})
	token := writeFile(t, "token", "eyJ.x.y\n")
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--server", url, "--issuer-ca-file", ca, "--token-file", token}, &out, &errOut); code == exitOK {
		t.Fatal("login with a token file given was accepted")
	}
}

// TestLogin_AnExpiredLoginIsRefusedHereSayingSo: a login past its expiry
// with nothing to refresh it is never sent as nobody.
func TestLogin_AnExpiredLoginIsRefusedHereSayingSo(t *testing.T) {
	_, engine, url, ca := loginFixture(t, oidctest.Options{NoRefreshTokens: true, AutoApproveDevice: true})
	var out, errOut strings.Builder
	if code := Execute([]string{"login", "--device", "--server", url, "--issuer-ca-file", ca}, &out, &errOut); code != exitOK {
		t.Fatal(errOut.String())
	}
	if !strings.Contains(errOut.String(), "no refresh token") {
		t.Errorf("login said nothing of the missing refresh token: %q", errOut.String())
	}
	_ = withCredentials(func(f credentialsFile, path string) error {
		l := f.Servers[serverKey(url)]
		l.ExpiresAt = time.Now().Add(-time.Second)
		f.Servers[serverKey(url)] = l
		return saveCredentials(f, path)
	})
	engine.mu.Lock()
	before := len(engine.seen)
	engine.mu.Unlock()
	errOut.Reset()
	if code := Execute([]string{"whoami", "--server", url}, &out, &errOut); code == exitOK || !strings.Contains(errOut.String(), "orkctl login") {
		t.Fatalf("whoami with an expired login = %d %q; want a refusal naming orkctl login", code, errOut.String())
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.seen) != before {
		t.Error("the request was sent with no credential")
	}
}
