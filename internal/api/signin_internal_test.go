package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
	"github.com/orkstack/orkstack/internal/testsupport/signintest"
)

type csrfHeader struct{ origin, token *string }

func (c csrfHeader) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Origin", *c.origin)
		if *c.token != "" {
			req.Header().Set(authn.CSRFHeader, *c.token)
		}
		return next(ctx, req)
	}
}
func (c csrfHeader) WrapStreamingClient(n connect.StreamingClientFunc) connect.StreamingClientFunc {
	return n
}
func (c csrfHeader) WrapStreamingHandler(n connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return n
}

// TestSignin_ThroughTheAPI signs a person in through the real mux and
// walks what the page does: GetAuthConfig with no credential, GetSignin
// for the CSRF token, a call without and with it, the access calls on
// sign-ins, and SignOut.
func TestSignin_ThroughTheAPI(t *testing.T) {
	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	env := signintest.New(t, engineURL, oidctest.Options{})
	public, _ := url.Parse(engineURL)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "ana", Role: authz.RoleAdmin, Scope: "deployment",
		Subjects: []string{"user:dev:ana@example.test"}})
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	a.Signins = env.Signins(public, nil)
	svc := newAccessService(t)
	engine.Config.Handler = NewMux(svc, MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)
	browser := env.Browser(engine.Certificate())
	ctx := context.Background()
	origin, token := engineURL, ""
	opts := connect.WithInterceptors(csrfHeader{origin: &origin, token: &token})
	auth := orkstackv1connect.NewAuthServiceClient(browser, engineURL, opts)
	access := orkstackv1connect.NewAccessServiceClient(browser, engineURL, opts)

	cfg, err := auth.GetAuthConfig(ctx, connect.NewRequest(&orkstackv1.GetAuthConfigRequest{}))
	if err != nil {
		t.Fatalf("GetAuthConfig with no credential: %v", err)
	}
	if len(cfg.Msg.GetIssuers()) != 1 || cfg.Msg.GetIssuers()[0].GetLoginPath() != "/v1/auth/login/dev" ||
		cfg.Msg.GetIssuers()[0].GetCliClientId() != signintest.CLIClientID || cfg.Msg.GetMode() != authn.ModeEnforce {
		t.Fatalf("GetAuthConfig = %v", cfg.Msg)
	}
	if _, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("GetSignin before signing in = %v", err)
	}

	resp, err := browser.Get(engineURL + "/v1/auth/login/dev?return=/runs")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.Request.URL.Path != "/runs" {
		t.Fatalf("signed in to %s", resp.Request.URL)
	}
	who, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		t.Fatalf("GetSignin with the cookie and no CSRF token: %v", err)
	}
	if who.Msg.GetPrincipalId() != "user:dev:ana@example.test" || who.Msg.GetMethod() != authn.MethodOIDC ||
		who.Msg.GetCsrfToken() == "" || who.Msg.GetSignin().GetPublicId() == "" {
		t.Fatalf("GetSignin = %v", who.Msg)
	}
	if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("a call without the CSRF token = %v; want permission denied", err)
	}
	token = who.Msg.GetCsrfToken()
	list, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{PrincipalId: "user:dev:ana@example.test"}))
	if err != nil || len(list.Msg.GetSignins()) != 1 || list.Msg.GetSignins()[0].GetPublicId() != who.Msg.GetSignin().GetPublicId() {
		t.Fatalf("ListSignins = %v %v", list, err)
	}
	if strings.Contains(list.Msg.String(), token) {
		t.Fatal("a listing shows the CSRF token")
	}
	origin = "https://evil.example"
	if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("a call from another origin = %v; want permission denied", err)
	}
	origin = engineURL

	out, err := auth.SignOut(ctx, connect.NewRequest(&orkstackv1.SignOutRequest{}))
	if err != nil || !strings.HasPrefix(out.Msg.GetEndSessionUrl(), env.Provider.Issuer()+"/end-session") {
		t.Fatalf("SignOut = %v %v", out, err)
	}
	if cookies := browser.Jar.Cookies(public); len(cookies) != 0 {
		t.Errorf("the cookie survived sign-out: %v", cookies)
	}
	if _, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("GetSignin after sign-out = %v", err)
	}
}

// TestSignin_RevokeSigninsEndsThem: an admin's RevokeSignins ends another
// principal's sign-in at once on this replica.
func TestSignin_RevokeSigninsEndsThem(t *testing.T) {
	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	env := signintest.New(t, engineURL, oidctest.Options{})
	public, _ := url.Parse(engineURL)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "ana", Role: authz.RoleAdmin, Scope: "deployment",
		Subjects: []string{"user:dev:ana@example.test"}})
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	a.Signins = env.Signins(public, nil)
	engine.Config.Handler = NewMux(newAccessService(t), MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)
	ctx := context.Background()
	signIn := func(user oidctest.User) (*http.Client, string) {
		env.Provider.SetUser(user)
		b := env.Browser(engine.Certificate())
		resp, err := b.Get(engineURL + "/v1/auth/login/dev")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		who, err := orkstackv1connect.NewAuthServiceClient(b, engineURL).GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		return b, who.Msg.GetCsrfToken()
	}
	admin, token := signIn(oidctest.User{Subject: "u-ana", Email: "ana@example.test"})
	bo, _ := signIn(oidctest.User{Subject: "u-bo", Email: "bo@example.test"})
	origin := engineURL
	access := orkstackv1connect.NewAccessServiceClient(admin, engineURL, connect.WithInterceptors(csrfHeader{origin: &origin, token: &token}))
	out, err := access.RevokeSignins(ctx, connect.NewRequest(&orkstackv1.RevokeSigninsRequest{PrincipalId: "user:dev:bo@example.test"}))
	if err != nil || out.Msg.GetRevoked() != 1 {
		t.Fatalf("RevokeSignins = %v %v", out, err)
	}
	if _, err := orkstackv1connect.NewAuthServiceClient(bo, engineURL).GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("a revoked sign-in still authenticates: %v", err)
	}
}

// TestAccessRows_PublicRowsAreExemptPatterns: a row anyone may call is
// exempt from the middleware's authentication, and an exempt Connect
// procedure is a public row: neither is public by halves.
func TestAccessRows_PublicRowsAreExemptPatterns(t *testing.T) {
	for procedure, row := range accessRows {
		if _, exempt := exemptPatterns[procedure]; row.public != exempt {
			t.Errorf("%s: public %v, exempt %v", procedure, row.public, exempt)
		}
	}
	for pattern := range exemptPatterns {
		if strings.HasPrefix(pattern, "/orkstack.v1.") && !accessRows[pattern].public {
			t.Errorf("exempt procedure %s has no public row", pattern)
		}
	}
}

// TestSignin_AStreamClosesWhenItsSigninEnds: a stream opened under a
// sign-in is closed once the sign-in is signed out, not at its
// revalidation.
func TestSignin_AStreamClosesWhenItsSigninEnds(t *testing.T) {
	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	env := signintest.New(t, engineURL, oidctest.Options{})
	public, _ := url.Parse(engineURL)
	a, _ := accessFor(t, authn.ModeEnforce, nil)
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	a.Signins = env.Signins(public, nil)
	reads := &countingRecords{SigninRecords: a.Signins.Records}
	a.Signins.Records = reads
	a.signinCheck = 20 * time.Millisecond
	svc := newAccessService(t)
	engine.Config.Handler = NewMux(svc, MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)
	browser := env.Browser(engine.Certificate())
	resp, err := browser.Get(engineURL + "/v1/auth/login/dev")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	req := httptest.NewRequest(http.MethodGet, engineURL+"/x", nil)
	for _, c := range browser.Jar.Cookies(public) {
		req.AddCookie(c)
	}
	p, view, err := a.Signins.Authenticate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ctx := authn.WithSignin(authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), p), view)
	streamCtx, stop := svc.watchAccess(ctx, &admission{row: accessRow{noPermission: true}})
	defer stop()
	// The positive control: the watcher reads the record three times, each
	// answered "standing", and the stream is still open.
	before := reads.gets.Load()
	deadline := time.Now().Add(5 * time.Second)
	for reads.gets.Load() < before+3 {
		if time.Now().After(deadline) {
			t.Fatal("the watcher never read the sign-in")
		}
		runtime.Gosched()
	}
	if streamCtx.Err() != nil {
		t.Fatal("the stream closed while its sign-in stood")
	}
	if _, err := a.Signins.SignOut(context.Background(), view); err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamCtx.Done():
		if connect.CodeOf(context.Cause(streamCtx)) != connect.CodeUnauthenticated {
			t.Errorf("closed with %v", context.Cause(streamCtx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream outlived its sign-in")
	}
}

type countingRecords struct {
	authn.SigninRecords
	gets atomic.Int64
}

func (c *countingRecords) Get(ctx context.Context, id string) ([]byte, string, error) {
	c.gets.Add(1)
	return c.SigninRecords.Get(ctx, id)
}

// TestSignin_APageLoadedPastRevalidationRefreshes: a page loaded afresh after
// its sign-in passed revalidation holds no CSRF token yet. GetSignin answers
// it, with the token, so the page's next call can be refused stale and its
// refresh carry the token; a sign-in that only GetSignin refused would send
// the person to sign in again at every reload.
func TestSignin_APageLoadedPastRevalidationRefreshes(t *testing.T) {
	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	env := signintest.New(t, engineURL, oidctest.Options{})
	public, _ := url.Parse(engineURL)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "ana", Role: authz.RoleAdmin, Scope: "deployment",
		Subjects: []string{"user:dev:ana@example.test"}})
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	var skew atomic.Int64
	a.Signins = env.Signins(public, func() time.Time { return time.Now().Add(time.Duration(skew.Load())) })
	a.Signins.Settings.RevalidateInterval = time.Minute
	engine.Config.Handler = NewMux(newAccessService(t), MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)
	browser := env.Browser(engine.Certificate())
	ctx := context.Background()
	origin, token := engineURL, ""
	opts := connect.WithInterceptors(csrfHeader{origin: &origin, token: &token})
	auth := orkstackv1connect.NewAuthServiceClient(browser, engineURL, opts)
	access := orkstackv1connect.NewAccessServiceClient(browser, engineURL, opts)
	resp, err := browser.Get(engineURL + "/v1/auth/login/dev?return=/runs")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	skew.Store(int64(2 * time.Minute)) // past revalidation, within the ID token's life
	who, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		t.Fatalf("GetSignin past revalidation, with no CSRF token: %v", err)
	}
	if who.Msg.GetCsrfToken() == "" {
		t.Fatal("GetSignin past revalidation handed out no CSRF token")
	}
	token = who.Msg.GetCsrfToken()
	_, err = access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated || !strings.Contains(err.Error(), authn.ReasonSigninStale) {
		t.Fatalf("a call past revalidation = %v; want %s", err, authn.ReasonSigninStale)
	}
	if _, err := auth.RefreshSignin(ctx, connect.NewRequest(&orkstackv1.RefreshSigninRequest{})); err != nil {
		t.Fatalf("RefreshSignin with the token GetSignin handed out: %v", err)
	}
	if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); err != nil {
		t.Fatalf("a call after the refresh: %v", err)
	}
}
