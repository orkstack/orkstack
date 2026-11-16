package authn

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

type signinFixture struct {
	provider   *oidctest.Provider
	engine     *httptest.Server
	signins    *Signins
	store      *artifacts.SigninStore
	browser    *http.Client
	clock      *fakeClock
	events     []string
	disabled   map[string]bool
	policyDown bool
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newSigninFixture(t *testing.T, opts oidctest.Options) *signinFixture {
	t.Helper()
	f := &signinFixture{clock: &fakeClock{now: time.Now()}, disabled: map[string]bool{}}
	idp := httptest.NewUnstartedServer(nil)
	engine := httptest.NewUnstartedServer(nil)
	issuerURL := "https://" + idp.Listener.Addr().String()
	engineURL := "https://" + engine.Listener.Addr().String()
	opts.Clients = append(opts.Clients, oidctest.Client{ID: "orkstack-web", Secret: "web-secret",
		RedirectURIs: []string{engineURL + "/v1/auth/callback/dev"}})
	if opts.User.Subject == "" {
		opts.User = oidctest.User{Subject: "u-ana", Email: "ana@example.test", Groups: []string{"ops"}}
	}
	p, err := oidctest.New(issuerURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.SetNow(f.clock.Now)
	f.provider = p
	idp.Config.Handler = p
	idp.StartTLS()
	t.Cleanup(idp.Close)

	issuers, err := ParseIssuers([]byte(`
issuers:
  dev:
    issuer: ` + issuerURL + `
    kind: user
    audiences:
      - orkstack-web
    algorithms:
      - ES256
    username_claim: email
    require_verified_email: true
    groups_claim: groups
    max_lifetime: 1h
    login:
      client_id: orkstack-web
      client_secret:
        source: env:ORK_TEST_WEB_SECRET
      scopes:
        - openid
        - email
`))
	if err != nil {
		t.Fatal(err)
	}
	dev, _ := issuers.ByName("dev")
	keys, err := NewKeyCache(dev, idp.Client(), f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	keyPath := filepath.Join(t.TempDir(), "signin.key")
	if err := os.WriteFile(keyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	sealer, err := LoadSigninKeys([]string{keyPath})
	if err != nil {
		t.Fatal(err)
	}
	public, _ := url.Parse(engineURL)
	f.store = artifacts.NewMemorySigninStore(nil)
	f.signins = &Signins{
		Issuers: issuers, Keys: map[string]jwt.KeySource{"dev": keys}, Sealer: sealer, Records: f.store,
		PublicURL: public, Leeway: time.Minute, Now: f.clock.Now,
		Secret: func(source string) (string, error) {
			if source != "env:ORK_TEST_WEB_SECRET" {
				return "", errors.New("unknown source")
			}
			return "web-secret", nil
		},
		HTTPClient: func(*Issuer) (*http.Client, error) { return idp.Client(), nil },
		Disabled: func(_ context.Context, id string) (bool, error) {
			if f.policyDown {
				return false, errors.New("the access document is past its stale limit")
			}
			return f.disabled[id], nil
		},
		OnEvent: func(_ context.Context, e SigninEvent) {
			f.events = append(f.events, e.Action)
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+LoginPath, f.signins.Login)
	mux.HandleFunc("GET "+CallbackPath, f.signins.Callback)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("page " + r.URL.RequestURI()))
	})
	engine.Config.Handler = mux
	engine.StartTLS()
	t.Cleanup(engine.Close)
	f.engine = engine

	jar, _ := cookiejar.New(nil)
	transport := idp.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.RootCAs.AddCert(engine.Certificate())
	f.browser = &http.Client{Jar: jar, Transport: transport}
	return f
}

// signIn walks the browser through the flow and answers where it ended.
func (f *signinFixture) signIn(t *testing.T, ret string) *url.URL {
	t.Helper()
	resp, err := f.browser.Get(f.engine.URL + "/v1/auth/login/dev?return=" + url.QueryEscape(ret))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.Request.URL
}

func (f *signinFixture) cookieRequest(t *testing.T) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, f.engine.URL+"/api/x", nil)
	u, _ := url.Parse(f.engine.URL)
	for _, c := range f.browser.Jar.Cookies(u) {
		req.AddCookie(c)
	}
	return req
}

func TestSignin_TheCodeFlowSignsAPersonIn(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	if got := f.signIn(t, "/runs/ork-1?tab=log"); got.Path != "/runs/ork-1" || got.RawQuery != "tab=log" {
		t.Fatalf("returned to %s; want /runs/ork-1?tab=log", got)
	}
	p, v, err := f.signins.Authenticate(context.Background(), f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != "user:dev:ana@example.test" || p.Method != MethodOIDC || p.Issuer != "dev" || len(p.Groups) != 1 || p.Signin == "" {
		t.Errorf("principal = %+v", p)
	}
	if v.Stale || v.CSRF == "" || v.PublicID != p.Signin {
		t.Errorf("view = %+v", v)
	}
	if strings.Join(f.events, ",") != "auth.signin.created" {
		t.Errorf("events = %v", f.events)
	}
	// The record in the bucket reveals nothing.
	ids, _, _ := f.store.List(context.Background(), "")
	sealed, _, _ := f.store.Get(context.Background(), ids[0])
	if strings.Contains(string(sealed), "ana@example.test") || strings.Contains(string(sealed), v.CSRF) {
		t.Error("the stored record is readable")
	}
	// An unsafe return path returns to /.
	if got := f.signIn(t, "//evil.example/x"); got.Path != "/" || got.Host != f.engine.Listener.Addr().String() {
		t.Errorf("an unsafe return path returned to %s", got)
	}
}

func TestSignin_CallbackRefusals(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	noFollow := *f.browser
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	start := func() *url.URL {
		resp, err := noFollow.Get(f.engine.URL + "/v1/auth/login/dev")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		auth, _ := url.Parse(resp.Header.Get("Location"))
		q := auth.Query()
		if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("nonce") == "" || q.Get("state") == "" {
			t.Fatalf("authorization request = %s", auth)
		}
		resp, err = noFollow.Get(auth.String())
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		back, _ := url.Parse(resp.Header.Get("Location"))
		return back
	}
	refused := func(name string, mutate func(u *url.URL)) {
		t.Helper()
		back := start()
		mutate(back)
		resp, err := noFollow.Get(back.String())
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		loc := resp.Header.Get("Location")
		if resp.StatusCode != http.StatusFound || !strings.HasPrefix(loc, "/signin?error=") {
			t.Errorf("%s: %d to %q; want a refusal", name, resp.StatusCode, loc)
		}
		for _, c := range resp.Cookies() {
			if c.Name == SigninCookie && c.MaxAge > 0 {
				t.Errorf("%s: a sign-in cookie was set", name)
			}
		}
	}
	refused("another state", func(u *url.URL) { q := u.Query(); q.Set("state", "x"); u.RawQuery = q.Encode() })
	refused("another issuer in the path", func(u *url.URL) { u.Path = "/v1/auth/callback/other" })
	refused("another iss", func(u *url.URL) {
		q := u.Query()
		q.Set("iss", "https://evil.example")
		u.RawQuery = q.Encode()
	})
	refused("no iss where the issuer sends one", func(u *url.URL) { q := u.Query(); q.Del("iss"); u.RawQuery = q.Encode() })
	// No login cookie at all.
	resp, err := (&http.Client{Transport: noFollow.Transport, CheckRedirect: noFollow.CheckRedirect}).Get(f.engine.URL + "/v1/auth/callback/dev?code=x&state=y")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Location"), "/signin?error=") {
		t.Errorf("a callback with no login cookie = %q", resp.Header.Get("Location"))
	}
}

func TestSignin_ADisabledPrincipalIsRefused(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.disabled["user:dev:ana@example.test"] = true
	if got := f.signIn(t, "/"); got.Path != "/signin" || got.Query().Get("error") != "disabled" {
		t.Fatalf("a disabled principal returned to %s", got)
	}
	if _, _, err := f.signins.Authenticate(context.Background(), f.cookieRequest(t)); !errors.Is(err, ErrNoCredential) {
		t.Fatalf("Authenticate = %v; want no credential", err)
	}
}

func TestSignin_RefreshRevalidatesAndEnds(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil || !v.Stale {
		t.Fatalf("past validated_until: %+v %v; want stale", v, err)
	}
	f.provider.SetUser(oidctest.User{Subject: "u-ana", Email: "ana@example.test", Groups: []string{"ops", "dba"}})
	next, err := f.signins.Refresh(ctx, v)
	if err != nil || next.Stale || f.provider.Refreshes() != 1 {
		t.Fatalf("Refresh = %+v %v (refreshes %d)", next, err, f.provider.Refreshes())
	}
	p, _, _ := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if strings.Join(p.Groups, ",") != "ops,dba" {
		t.Errorf("groups after refresh = %v; want the issuer's new ones", p.Groups)
	}
	// A refresh right after another redeems nothing.
	if _, err := f.signins.Refresh(ctx, v); err != nil || f.provider.Refreshes() != 1 {
		t.Errorf("a second refresh at once: %v, refreshes %d", err, f.provider.Refreshes())
	}
	// Removed at the issuer: the next refresh ends the sign-in.
	f.provider.RemoveUser("u-ana")
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	if _, err := f.signins.Refresh(ctx, v); !errors.Is(err, ErrSigninEnded) {
		t.Fatalf("a refresh refused by the issuer = %v; want ErrSigninEnded", err)
	}
	if _, _, err := f.signins.Authenticate(ctx, f.cookieRequest(t)); err == nil {
		t.Fatal("an ended sign-in still authenticates")
	}
	if !strings.Contains(strings.Join(f.events, ","), "auth.signin.ended") {
		t.Errorf("events = %v", f.events)
	}
}

func TestSignin_LimitsExpireTheSignin(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{NoRefreshTokens: true})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil || !v.Stale {
		t.Fatalf("Authenticate = %+v %v", v, err)
	}
	if _, err := f.signins.Refresh(ctx, v); !errors.Is(err, ErrNoRefreshToken) {
		t.Fatalf("Refresh with no refresh token = %v", err)
	}
	f.clock.Advance(DefaultSigninIdleTimeout)
	if _, _, err := f.signins.Authenticate(ctx, f.cookieRequest(t)); err == nil {
		t.Fatal("a sign-in past its idle timeout authenticates")
	}
	if ids, _, _ := f.store.List(ctx, ""); len(ids) != 0 {
		t.Errorf("an expired record was kept: %v", ids)
	}
}

func TestSignin_SignOutEndsItAtTheIssuerToo(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	end, err := f.signins.SignOut(ctx, v)
	if err != nil || !strings.HasPrefix(end, f.provider.Issuer()+"/end-session?client_id=orkstack-web") {
		t.Fatalf("SignOut = %q %v", end, err)
	}
	if f.provider.Revocations() != 1 {
		t.Errorf("revocations = %d; want the refresh token revoked", f.provider.Revocations())
	}
	if _, _, err := f.signins.Authenticate(ctx, f.cookieRequest(t)); err == nil {
		t.Fatal("a signed-out cookie authenticates")
	}
}

func TestSignin_ListAndRevoke(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	f.browser.Jar, _ = cookiejar.New(nil)
	f.signIn(t, "/")
	f.provider.SetUser(oidctest.User{Subject: "u-bo", Email: "bo@example.test"})
	f.browser.Jar, _ = cookiejar.New(nil)
	f.signIn(t, "/")
	ctx := context.Background()
	all, err := f.signins.List(ctx, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("List = %d %v", len(all), err)
	}
	ana, _ := f.signins.List(ctx, "user:dev:ana@example.test")
	if len(ana) != 2 {
		t.Fatalf("ana's sign-ins = %d", len(ana))
	}
	n, err := f.signins.Revoke(ctx, "user:dev:ana@example.test", []string{ana[0].PublicID})
	if err != nil || n != 1 {
		t.Fatalf("Revoke one = %d %v", n, err)
	}
	n, _ = f.signins.Revoke(ctx, "user:dev:ana@example.test", nil)
	if n != 1 {
		t.Fatalf("Revoke the rest = %d", n)
	}
	// bo's own sign-in, the last one made, still stands.
	if _, _, err := f.signins.Authenticate(ctx, f.cookieRequest(t)); err != nil {
		t.Fatalf("another principal's sign-in ended: %v", err)
	}
}

func TestSignins_CheckCSRF(t *testing.T) {
	public, _ := url.Parse("https://ork.example.test")
	s := &Signins{PublicURL: public}
	v := &SigninView{CSRF: "tok"}
	req := func(method, origin, csrf string) *http.Request {
		r := httptest.NewRequest(method, "https://ork.example.test/x", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if csrf != "" {
			r.Header.Set(CSRFHeader, csrf)
		}
		return r
	}
	for name, tc := range map[string]struct {
		r      *http.Request
		exempt bool
		ok     bool
	}{
		"a POST with the token":           {req("POST", "https://ork.example.test", "tok"), false, true},
		"a POST with no Origin":           {req("POST", "", "tok"), false, true},
		"a POST without the token":        {req("POST", "https://ork.example.test", ""), false, false},
		"a POST with another token":       {req("POST", "", "nope"), false, false},
		"a POST from another origin":      {req("POST", "https://evil.example", "tok"), false, false},
		"a GET without the token":         {req("GET", "", ""), false, true},
		"a GET from another origin":       {req("GET", "https://evil.example", ""), false, false},
		"the exempt call without token":   {req("POST", "https://ork.example.test", ""), true, true},
		"the exempt call, another origin": {req("POST", "https://evil.example", ""), true, false},
		"the default port written":        {req("POST", "https://ork.example.test:443", "tok"), false, true},
		"another port":                    {req("POST", "https://ork.example.test:8443", "tok"), false, false},
		"plain http":                      {req("POST", "http://ork.example.test", "tok"), false, false},
		"the null origin":                 {req("POST", "null", "tok"), false, false},
	} {
		if err := s.CheckCSRF(tc.r, v, tc.exempt); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestMiddleware_SigninCookie: a request with no bearer is authenticated by
// its sign-in cookie; a stale one is refused SIGNIN_STALE except by the
// calls that serve one; a request that is not GET needs its CSRF token,
// except the one call that hands it out.
func TestMiddleware_SigninCookie(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	mux := http.NewServeMux()
	var seen []*Principal
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, FromContext(r.Context()))
		if SigninFrom(r.Context()) == nil {
			t.Error("no sign-in in the context")
		}
	})
	mux.Handle("/api/{x}", ok)
	mux.Handle("/refresh", ok)
	mux.Handle("/whoami", ok)
	auth := &Authenticator{Issuers: f.signins.Issuers, Keys: f.signins.Keys, Now: f.clock.Now}
	h := Middleware(mux, MiddlewareOptions{Mode: ModeEnforce, Mux: mux, Authenticator: auth, Signins: f.signins,
		StaleSigninServed: func(p string) bool { return p == "/refresh" },
		CSRFExempt:        func(p string) bool { return p == "/whoami" }})
	_, v, err := f.signins.Authenticate(context.Background(), f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	send := func(path, csrf string) *httptest.ResponseRecorder {
		r := f.cookieRequest(t)
		r.URL.Path = path
		r.Header.Set("Origin", f.engine.URL)
		if csrf != "" {
			r.Header.Set(CSRFHeader, csrf)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	if rec := send("/api/x", v.CSRF); rec.Code != http.StatusOK {
		t.Fatalf("a cookie request with its token = %d %s", rec.Code, rec.Body)
	}
	if seen[0].ID != "user:dev:ana@example.test" {
		t.Errorf("principal = %+v", seen[0])
	}
	if rec := send("/api/x", ""); rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), ReasonCSRF) {
		t.Errorf("without its token = %d %s", rec.Code, rec.Body)
	}
	if rec := send("/whoami", ""); rec.Code != http.StatusOK {
		t.Errorf("the exempt call without its token = %d", rec.Code)
	}
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	if rec := send("/api/x", v.CSRF); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), ReasonSigninStale) {
		t.Errorf("a stale sign-in = %d %s", rec.Code, rec.Body)
	}
	if rec := send("/refresh", v.CSRF); rec.Code != http.StatusOK {
		t.Errorf("the refresh under a stale sign-in = %d", rec.Code)
	}
	// A bearer takes precedence over the cookie: a refused bearer is refused.
	r := f.cookieRequest(t)
	r.URL.Path = "/api/x"
	r.Header.Set("Authorization", "Bearer not.a.token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), ReasonSigninStale) {
		t.Errorf("a bad bearer beside a cookie = %d %s", rec.Code, rec.Body)
	}
	// A cookie naming no record is refused, ended.
	r = httptest.NewRequest(http.MethodGet, "/api/x", nil)
	r.AddCookie(&http.Cookie{Name: SigninCookie, Value: "bm9wZQ"})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), ReasonSigninEnded) {
		t.Errorf("a cookie naming nothing = %d %s", rec.Code, rec.Body)
	}
}

// TestSignin_ConcurrentRefreshesRedeemOnce: two tabs refreshing one
// sign-in on one replica redeem its rotating refresh token once, and both
// are answered.
func TestSignin_ConcurrentRefreshesRedeemOnce(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = f.signins.Refresh(ctx, v)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("refresh %d: %v", i, err)
		}
	}
	if n := f.provider.Refreshes(); n != 1 {
		t.Errorf("refreshes = %d; want the token redeemed once", n)
	}
}

// staleOnce answers the first Get with the record as it was when armed:
// another replica that read it just before this one's refresh wrote.
type staleOnce struct {
	SigninRecords
	mu    sync.Mutex
	armed bool
	body  []byte
	etag  string
}

func (s *staleOnce) Get(ctx context.Context, id string) ([]byte, string, error) {
	s.mu.Lock()
	if s.armed {
		s.armed = false
		s.mu.Unlock()
		return s.body, s.etag, nil
	}
	s.mu.Unlock()
	return s.SigninRecords.Get(ctx, id)
}

// TestSignin_ARacingReplicaTakesTheWinnersAnswer: a replica that read the
// record before another refreshed it, and whose redemption the issuer then
// refuses as reused, takes the other's answer and does not end the sign-in.
func TestSignin_ARacingReplicaTakesTheWinnersAnswer(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	ids, _, _ := f.store.List(ctx, "")
	body, etag, _ := f.store.Get(ctx, ids[0])
	other := &Signins{Issuers: f.signins.Issuers, Keys: f.signins.Keys, Sealer: f.signins.Sealer,
		Records: &staleOnce{SigninRecords: f.store, armed: true, body: body, etag: etag}, PublicURL: f.signins.PublicURL,
		Leeway: time.Minute, Now: f.clock.Now, Secret: f.signins.Secret, HTTPClient: f.signins.HTTPClient}
	if _, err := f.signins.Refresh(ctx, v); err != nil {
		t.Fatal(err)
	}
	got, err := other.Refresh(ctx, v)
	if err != nil || got.Stale {
		t.Fatalf("the racing replica = %+v %v; want the winner's answer", got, err)
	}
	if _, _, err := f.signins.Authenticate(ctx, f.cookieRequest(t)); err != nil {
		t.Fatalf("the sign-in ended: %v", err)
	}
}

// TestSignin_AnIssuerErrorThatIsNotAboutTheSigninKeepsIt: a rate limit or a
// refused client leaves the sign-in standing; only invalid_grant ends it.
func TestSignin_AnIssuerErrorThatIsNotAboutTheSigninKeepsIt(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{http.StatusTooManyRequests, "slow_down"}, {http.StatusUnauthorized, "invalid_client"}, {http.StatusInternalServerError, "server_error"}} {
		f := newSigninFixture(t, oidctest.Options{})
		f.signIn(t, "/")
		ctx := context.Background()
		f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
		_, v, _ := f.signins.Authenticate(ctx, f.cookieRequest(t))
		f.provider.SetRefreshFailure(tc.status, tc.code)
		if _, err := f.signins.Refresh(ctx, v); !errors.Is(err, ErrIssuerUnavailable) {
			t.Errorf("%d %s: Refresh = %v; want ErrIssuerUnavailable", tc.status, tc.code, err)
		}
		if ids, _, _ := f.store.List(ctx, ""); len(ids) != 1 {
			t.Errorf("%d %s: the record was deleted", tc.status, tc.code)
		}
	}
}

// TestSignin_ARevokedSigninIsNotCachedByARacingRead: a read that began
// before a revocation does not put the revoked record back in the cache.
func TestSignin_ARevokedSigninIsNotCachedByARacingRead(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	ids, _, _ := f.store.List(ctx, "")
	began := f.signins.readBegins()
	f.signins.forget(ids[0])
	rec := &signinRecord{Format: signinRecordFormat, ExpiresAt: f.clock.Now().Add(time.Hour), IdleExpiresAt: f.clock.Now().Add(time.Hour)}
	f.signins.rememberRead(ids[0], rec, "e", began)
	f.signins.mu.Lock()
	_, cached := f.signins.cache[ids[0]]
	f.signins.mu.Unlock()
	if cached {
		t.Fatal("a read that began before the revocation cached the record")
	}
}

// TestSignin_ANewSigninEndsTheBrowsersLastOne: signing in again in the same
// browser ends the sign-in it held.
func TestSignin_ANewSigninEndsTheBrowsersLastOne(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	f.signIn(t, "/")
	ids, _, _ := f.store.List(context.Background(), "")
	if len(ids) != 1 {
		t.Fatalf("records after signing in twice = %d; want the last one only", len(ids))
	}
}

// TestSignin_AnOutageEndsNothing: an access document that cannot be read
// refuses a new sign-in for now and leaves a standing one alone; a
// standing one whose record cannot be deleted is not reported ended.
func TestSignin_AnOutageEndsNothing(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, err := f.signins.Authenticate(ctx, f.cookieRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	f.policyDown = true
	if _, err := f.signins.Refresh(ctx, v); !errors.Is(err, ErrSigninUnavailable) {
		t.Fatalf("Refresh with the document unreadable = %v; want ErrSigninUnavailable", err)
	}
	if ids, _, _ := f.store.List(ctx, ""); len(ids) != 1 {
		t.Fatal("an unreadable document ended the sign-in")
	}
	if strings.Contains(strings.Join(f.events, ","), "auth.signin.ended") {
		t.Errorf("events = %v; nothing ended", f.events)
	}
	resp, err := f.browser.Get(f.engine.URL + "/v1/auth/login/dev")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a sign-in with the document unreadable = %d; want 503", resp.StatusCode)
	}
}

type failingDelete struct{ SigninRecords }

func (failingDelete) Delete(context.Context, string) error { return errors.New("store down") }

func TestSignin_AnEndThatCannotDeleteIsNotAnEnd(t *testing.T) {
	f := newSigninFixture(t, oidctest.Options{})
	f.signIn(t, "/")
	ctx := context.Background()
	f.clock.Advance(DefaultSigninRevalidateInterval + time.Second)
	_, v, _ := f.signins.Authenticate(ctx, f.cookieRequest(t))
	f.disabled["user:dev:ana@example.test"] = true
	f.signins.Records = failingDelete{f.store}
	if _, err := f.signins.Refresh(ctx, v); !errors.Is(err, ErrSigninUnavailable) {
		t.Fatalf("Refresh = %v; want ErrSigninUnavailable while the record cannot be deleted", err)
	}
}
