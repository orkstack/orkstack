package authn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

const middlewareIssuers = `issuers:
  corp:
    issuer: https://login.example.test
    kind: user
    audiences:
      - orkstack
    algorithms:
      - ES256
    username_claim: email
    require_verified_email: true
    groups_claim: groups
    max_lifetime: 1h
`

type middlewareFixture struct {
	signer *jwt.Signer
	glass  *jwt.Signer
	auth   *Authenticator
	mux    *http.ServeMux
	served []*Principal
	breaks []string
}

func newMiddlewareFixture(t *testing.T) *middlewareFixture {
	t.Helper()
	issuers, err := ParseIssuers([]byte(middlewareIssuers))
	if err != nil {
		t.Fatal(err)
	}
	f := &middlewareFixture{signer: newSigner(t), glass: newSigner(t)}
	keys, _ := jwt.StaticByThumbprint(f.signer.Public())
	glassKeys, _ := jwt.StaticByThumbprint(f.glass.Public())
	public, _ := url.Parse("https://ork.example.test")
	f.auth = &Authenticator{
		Issuers: issuers, Keys: map[string]jwt.KeySource{"corp": keys}, Leeway: time.Minute,
		BreakGlass:  &BreakGlass{Keys: glassKeys, Audience: "https://ork.example.test", PublicURL: public, Replay: jwt.NewReplayCache()},
		HostAllowed: func(h string) bool { return h == "ork.example.test" },
		Now:         func() time.Time { return now0 },
	}
	f.mux = http.NewServeMux()
	record := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.served = append(f.served, FromContext(r.Context()))
		_, _ = w.Write([]byte(ModeFrom(r.Context())))
	})
	f.mux.Handle("/api/{x}", record)
	f.mux.Handle("/open/{x}", record)
	return f
}

func (f *middlewareFixture) handler(mode string) http.Handler {
	return Middleware(f.mux, MiddlewareOptions{
		Mode: mode, Mux: f.mux, Authenticator: f.auth,
		Exempt: func(p string) bool { return p == "/open/{x}" },
		OnBreakGlass: func(_ *http.Request, p *Principal, reason string) {
			f.breaks = append(f.breaks, p.ID+" "+reason)
		},
	})
}

func (f *middlewareFixture) token(t *testing.T, claims map[string]any) string {
	t.Helper()
	base := map[string]any{"iss": "https://login.example.test", "sub": "u-1", "aud": "orkstack",
		"iat": now0.Unix(), "exp": now0.Add(time.Minute).Unix(), "email": "alice@example.test", "email_verified": true}
	for k, v := range claims {
		base[k] = v
	}
	tok, err := f.signer.Sign("JWT", base)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func do(h http.Handler, method, target, bearer string, header map[string]string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMiddleware_ModesAndMethods(t *testing.T) {
	f := newMiddlewareFixture(t)
	good := f.token(t, nil)
	expired := f.token(t, map[string]any{"exp": now0.Add(-time.Hour).Unix(), "iat": now0.Add(-2 * time.Hour).Unix()})
	cases := []struct {
		name, mode, path, bearer string
		status                   int
		principal                string
	}{
		{"enforce: no credential is refused", ModeEnforce, "/api/x", "", http.StatusUnauthorized, ""},
		{"observe: no credential is served with no principal", ModeObserve, "/api/x", "", http.StatusOK, ""},
		{"enforce: a verified token is served", ModeEnforce, "/api/x", good, http.StatusOK, "user:corp:alice@example.test"},
		{"observe: a verified token sets the principal", ModeObserve, "/api/x", good, http.StatusOK, "user:corp:alice@example.test"},
		{"enforce: a refused token never falls through", ModeEnforce, "/api/x", expired, http.StatusUnauthorized, ""},
		{"observe: a presented token that fails is refused too", ModeObserve, "/api/x", expired, http.StatusUnauthorized, ""},
		{"an untrusted issuer", ModeEnforce, "/api/x", f.token(t, map[string]any{"iss": "https://evil.example.test"}), http.StatusUnauthorized, ""},
		{"a service account's assertion is not accepted yet", ModeEnforce, "/api/x", mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", ""), http.StatusUnauthorized, ""},
		{"garbage", ModeEnforce, "/api/x", "not.a.token", http.StatusUnauthorized, ""},
		{"an exempt pattern is served without a credential", ModeEnforce, "/open/x", "", http.StatusOK, ""},
		{"an exempt pattern ignores a bad credential", ModeEnforce, "/open/x", "garbage", http.StatusOK, ""},
		{"a path no pattern serves is authenticated", ModeEnforce, "/nowhere", "", http.StatusUnauthorized, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.served = nil
			rec := do(f.handler(tc.mode), http.MethodPost, tc.path, tc.bearer, nil, "{}")
			if rec.Code != tc.status {
				t.Fatalf("status = %d (%s); want %d", rec.Code, rec.Body, tc.status)
			}
			if tc.status != http.StatusOK {
				if len(f.served) != 0 {
					t.Fatal("a refused request reached the handler")
				}
				return
			}
			got := ""
			if len(f.served) == 1 && f.served[0] != nil {
				got = f.served[0].ID
			}
			if got != tc.principal {
				t.Errorf("principal = %q; want %q", got, tc.principal)
			}
			if rec.Body.String() != tc.mode {
				t.Errorf("the handler saw mode %q; want %q", rec.Body.String(), tc.mode)
			}
		})
	}
}

func mustSignRequest(t *testing.T, s *jwt.Signer, account, method, target, body string) string {
	t.Helper()
	tok, err := s.SignRequest(jwt.AssertionRequest{Account: account, Audience: "https://ork.example.test", Method: method, URL: target, Body: []byte(body)}, now0, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestMiddleware_BreakGlass(t *testing.T) {
	f := newMiddlewareFixture(t)
	h := f.handler(ModeEnforce)
	sign := func(body string) string {
		return mustSignRequest(t, f.glass, BreakGlassAccount, "POST", "https://ork.example.test/api/x", body)
	}
	reason := map[string]string{ReasonHeader: "the issuer is down, INC-42"}

	if rec := do(h, "POST", "/api/x", sign("{}"), reason, "{}"); rec.Code != http.StatusOK {
		t.Fatalf("a bound break-glass assertion with a reason = %d (%s)", rec.Code, rec.Body)
	}
	if len(f.breaks) != 1 || f.breaks[0] != "serviceaccount:break-glass the issuer is down, INC-42" {
		t.Fatalf("break-glass uses recorded = %v", f.breaks)
	}
	if p := f.served[0]; p == nil || p.Method != MethodBreakGlass {
		t.Fatalf("principal = %+v", p)
	}
	refused := map[string]struct {
		token  string
		header map[string]string
		body   string
	}{
		"no reason":             {sign("{}"), nil, "{}"},
		"a reason on two lines": {sign("{}"), map[string]string{ReasonHeader: "a\nb"}, "{}"},
		"another body":          {sign("{}"), reason, `{"x":1}`},
		"another key":           {mustSignRequest(t, f.signer, BreakGlassAccount, "POST", "https://ork.example.test/api/x", "{}"), reason, "{}"},
		"another path":          {mustSignRequest(t, f.glass, BreakGlassAccount, "POST", "https://ork.example.test/api/y", "{}"), reason, "{}"},
		"another method":        {mustSignRequest(t, f.glass, BreakGlassAccount, "PUT", "https://ork.example.test/api/x", "{}"), reason, "{}"},
		"another host":          {mustSignRequest(t, f.glass, BreakGlassAccount, "POST", "https://evil.example.test/api/x", "{}"), reason, "{}"},
	}
	for name, tc := range refused {
		if rec := do(h, "POST", "/api/x", tc.token, tc.header, tc.body); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d; want 401", name, rec.Code)
		}
	}
	// The same assertion twice: the second is a replay.
	tok := sign("{}")
	_ = do(h, "POST", "/api/x", tok, reason, "{}")
	if rec := do(h, "POST", "/api/x", tok, reason, "{}"); rec.Code != http.StatusUnauthorized {
		t.Errorf("a replayed assertion = %d; want 401", rec.Code)
	}
	f.auth.BreakGlass = nil
	if rec := do(h, "POST", "/api/x", sign("{}"), reason, "{}"); rec.Code != http.StatusUnauthorized {
		t.Errorf("break-glass with no key configured = %d; want 401", rec.Code)
	}
}

func TestRecorded_ReadsTheHeaderOnlyInAssertedMode(t *testing.T) {
	h := http.Header{}
	h.Set(PrincipalHeader, "someone-a-gateway-named")
	if got := Recorded(context.Background(), h); got.ID != "someone-a-gateway-named" || got.Verified {
		t.Errorf("asserted = %+v; want the header, unverified", got)
	}
	for _, mode := range []string{ModeObserve, ModeEnforce} {
		ctx := WithMode(context.Background(), mode)
		if got := Recorded(ctx, h); got.ID != "" {
			t.Errorf("%s with no principal = %+v; the header must reach nothing", mode, got)
		}
		p := &Principal{ID: "user:corp:a", Method: MethodJWT, Issuer: "corp"}
		if got := Recorded(WithPrincipal(ctx, p), h); got.ID != p.ID || !got.Verified || got.Method != MethodJWT || got.Issuer != "corp" {
			t.Errorf("%s with a principal = %+v", mode, got)
		}
	}
}

type countingBody struct{ n int }

func (b *countingBody) Read(p []byte) (int, error) {
	b.n += len(p)
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
func (b *countingBody) Close() error { return nil }

// TestMiddleware_BodyIsReadOnlyForAConfiguredBreakGlass: a body is buffered
// to hash it only for a token that claims break-glass where break-glass is
// configured, and never past BreakGlassMaxBody (security review M2).
func TestMiddleware_BodyIsReadOnlyForAConfiguredBreakGlass(t *testing.T) {
	f := newMiddlewareFixture(t)
	send := func(token string) (int, int) {
		body := &countingBody{}
		req := httptest.NewRequest(http.MethodPost, "/api/x", nil)
		req.Body = body
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set(ReasonHeader, "r")
		rec := httptest.NewRecorder()
		f.handler(ModeEnforce).ServeHTTP(rec, req)
		return rec.Code, body.n
	}
	sa := mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", "")
	if code, read := send(sa); code != http.StatusUnauthorized || read != 0 {
		t.Errorf("a service account's assertion: %d after reading %d bytes; want 401 and nothing read", code, read)
	}
	glass := mustSignRequest(t, f.glass, BreakGlassAccount, "POST", "https://ork.example.test/api/x", "")
	if code, read := send(glass); code != http.StatusRequestEntityTooLarge || read > BreakGlassMaxBody+64<<10 {
		t.Errorf("an endless break-glass body: %d after reading %d bytes; want 413 within %d", code, read, BreakGlassMaxBody)
	}
	f.auth.BreakGlass = nil
	if code, read := send(glass); code != http.StatusUnauthorized || read != 0 {
		t.Errorf("break-glass not configured: %d after reading %d bytes; want 401 and nothing read", code, read)
	}
}
