package vault

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// fault is a server that answers as a Vault in trouble does: overloaded,
// sealed, a standby that redirects, slow, or with an answer that is not one.
// A real Vault does none of these on demand; what a real one does is tested
// against a real one (the integration tests).
type fault struct {
	t      *testing.T
	server *httptest.Server

	mu     sync.Mutex
	answer map[string]http.HandlerFunc // by method and path
	asked  []string
}

func newFault(t *testing.T) *fault {
	f := &fault{t: t, answer: map[string]http.HandlerFunc{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + strings.TrimPrefix(r.URL.Path, "/v1/")
		f.mu.Lock()
		f.asked = append(f.asked, key+" "+r.Header.Get("X-Vault-Token"))
		h := f.answer[key]
		f.mu.Unlock()
		if h == nil && key == capabilitiesPath {
			h = body(selfCapabilities)
		}
		if h == nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		h(w, r)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fault) on(key string, h http.HandlerFunc) {
	f.mu.Lock()
	f.answer[key] = h
	f.mu.Unlock()
}

func (f *fault) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.asked {
		if strings.HasPrefix(a, prefix) {
			n++
		}
	}
	return n
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"errors":["a word of Vault's own"]}`))
	}
}

func body(text string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(text)) }
}

const (
	loginPath  = "POST auth/jwt/login"
	lookupPath = "GET auth/token/lookup-self"
	revokePath = "POST auth/token/revoke-self"
	renewPath  = "POST auth/token/renew-self"
	leasePath  = "PUT sys/leases/renew"

	capabilitiesPath = "POST sys/capabilities-self"
)

const selfCapabilities = `{"data":{"auth/token/lookup-self":["read"],"auth/token/revoke-self":["update"],"capabilities":["deny"]}}`

func loginBody(token string, ttl int) string {
	return fmt.Sprintf(`{"auth":{"client_token":%q,"accessor":"acc-%s","lease_duration":%d,"renewable":true,"token_type":"service"}}`, token, token, ttl)
}

func (f *fault) provider() provider.Provider {
	f.t.Helper()
	ca := filepath.Join(f.t.TempDir(), "ca.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw})
	if err := os.WriteFile(ca, cert, 0o600); err != nil {
		f.t.Fatal(err)
	}
	p, err := declare(f.t, "address: "+f.server.URL+"\naudience: a\nca_file: "+ca+"\n")
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

const identityJWT = "header.payload.signature"

func identity(context.Context) (string, time.Duration, error) { return identityJWT, time.Hour, nil }

func (f *fault) login(p provider.Provider) (provider.Session, error) {
	return p.Login(context.Background(), "app", provider.Attempt{}, identity)
}

func (f *fault) mustLogin(p provider.Provider) *session {
	f.t.Helper()
	s, err := f.login(p)
	if err != nil {
		f.t.Fatalf("login: %v", err)
	}
	return s.(*session)
}

func parsed(t *testing.T, p provider.Provider, kind provider.Kind, locator string) provider.Parsed {
	t.Helper()
	out, err := p.Parse(provider.Ref{Kind: kind, Locator: locator})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestLogin_Answers: each answer to a login is the class the table says, and
// no error repeats Vault's words, the role or the token.
func TestLogin_Answers(t *testing.T) {
	for name, c := range map[string]struct {
		answer http.HandlerFunc
		class  provider.Class
		code   string
	}{
		"refused":               {status(403), provider.Refused, "403"},
		"a role it has not":     {status(400), provider.Refused, "400"},
		"no such mount":         {status(404), provider.Unavailable, "404"},
		"throttled":             {status(429), provider.Unavailable, "429"},
		"replication behind":    {status(412), provider.Unavailable, "412"},
		"a DR secondary":        {status(472), provider.Unavailable, "472"},
		"a performance standby": {status(473), provider.Unavailable, "473"},
		"sealed":                {status(503), provider.Unavailable, "503"},
		"broken":                {status(500), provider.Unavailable, "500"},
		"a code of its own":     {status(418), provider.Unavailable, "418"},
		"not JSON":              {body("<html>"), provider.Unavailable, "malformed-answer"},
		"no token":              {body(`{"auth":{"lease_duration":60}}`), provider.Unavailable, "malformed-answer"},
		"a batch token":         {body(`{"auth":{"client_token":"hvb.x","lease_duration":60,"token_type":"batch"}}`), provider.Refused, "batch-token"},
		"a number of uses":      {body(`{"auth":{"client_token":"hvs.x","lease_duration":60,"num_uses":1}}`), provider.Refused, "login-num-uses"},
		"too large":             {body(strings.Repeat(" ", maxAnswerBytes+1)), provider.Invalid, "answer-too-large"},
	} {
		f := newFault(t)
		f.on(loginPath, c.answer)
		_, err := f.login(f.provider())
		var pe *provider.Error
		if !errors.As(err, &pe) || pe.Class != c.class || pe.Code != c.code {
			t.Errorf("%s: %v; want %s (%s)", name, err, c.class, c.code)
			continue
		}
		for _, secret := range []string{"word of Vault", identityJWT, "app", "hvb.x"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: the error repeats %q: %v", name, secret, err)
			}
		}
	}
}

// TestLogin_FollowsNoRedirect: a standby that sends the login elsewhere is
// unavailable, and nothing is sent there.
func TestLogin_FollowsNoRedirect(t *testing.T) {
	elsewhere, f := newFault(t), newFault(t)
	f.on(loginPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.server.URL+r.URL.Path, http.StatusTemporaryRedirect)
	})
	if _, err := f.login(f.provider()); !is(err, "redirect") || provider.ClassOf(err) != provider.Unavailable {
		t.Fatalf("a redirected login = %v", err)
	}
	if n := elsewhere.count(""); n != 0 {
		t.Fatalf("%d requests followed the redirect", n)
	}
}

// TestLogin_TrustsOnlyItsCA: a server whose certificate the declared CA did
// not sign is unavailable, and never sees the attempt's token.
func TestLogin_TrustsOnlyItsCA(t *testing.T) {
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 60)))
	p := f.provider().(*vaultProvider)
	ca := filepath.Join(t.TempDir(), "other.pem")
	if err := os.WriteFile(ca, []byte("no certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	p.http = &httpClient{caFile: ca}
	if _, err := f.login(p); !is(err, "ca-file") || provider.ClassOf(err) != provider.Unavailable {
		t.Fatalf("a CA file with no certificate = %v", err)
	}
	p.http = &httpClient{}
	if _, err := f.login(p); !is(err, "no-answer") {
		t.Fatalf("an unknown issuer = %v", err)
	}
	if n := f.count(""); n != 0 {
		t.Fatalf("%d requests reached a server that was not trusted", n)
	}
}

// TestLogin_IgnoresTheEnvironment: no proxy and no Vault variable of the
// Orker's environment changes where a login goes or what it carries.
func TestLogin_IgnoresTheEnvironment(t *testing.T) {
	other := newFault(t)
	for _, key := range []string{"HTTPS_PROXY", "https_proxy", "ALL_PROXY", "VAULT_ADDR"} {
		t.Setenv(key, other.server.URL)
	}
	t.Setenv("VAULT_TOKEN", "hvs.the-orkers-own")
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 60)))
	p := f.provider().(*vaultProvider)
	f.mustLogin(p)
	// The standard proxy lookup never proxies a loopback address, so its
	// absence is read from the client itself.
	client, err := p.http.get()
	if err != nil || client.Transport.(*http.Transport).Proxy != nil {
		t.Fatalf("the client has a proxy function, or none: %v", err)
	}
	if other.count("") != 0 || f.count(loginPath+" hvs") != 0 || f.count(loginPath) != 1 {
		t.Fatalf("the environment was used: %d calls elsewhere, asked %v", other.count(""), f.asked)
	}
}

// TestLogin_LongerThanItsBoundIsRevokedAndRefused: the refusal carries both
// numbers; when the revoke fails every time, the accessor is logged and the
// token is not.
func TestLogin_LongerThanItsBoundIsRevokedAndRefused(t *testing.T) {
	var log bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	f := newFault(t)
	p := f.provider()
	f.on(loginPath, body(loginBody("hvs.long", 2764800)))
	f.on(revokePath, status(204))
	if _, err := f.login(p); !is(err, "login-ttl-768h0m0s-over-1h0m0s") || provider.ClassOf(err) != provider.Refused {
		t.Fatalf("a 768 h login = %v", err)
	}
	if f.count(revokePath+" hvs.long") != 1 || log.Len() != 0 {
		t.Fatalf("revoked %d times, logged %q; want once and nothing", f.count(revokePath), log.String())
	}
	f.on(revokePath, status(500))
	if _, err := f.login(p); err == nil {
		t.Fatal("an over-long login was used")
	}
	if n := f.count(revokePath); n != 1+revokeTries {
		t.Fatalf("%d revokes, want %d more", n, revokeTries)
	}
	if out := log.String(); !strings.Contains(out, "acc-hvs.long") || strings.Contains(out, "=hvs.long") {
		t.Fatalf("logged %q; want the accessor and never the token", out)
	}
	f.on(loginPath, body(loginBody("hvs.never", 0)))
	f.on(revokePath, status(204))
	if _, err := f.login(p); !is(err, "login-never-expires") || f.count(revokePath+" hvs.never") != 1 {
		t.Fatalf("a login that never expires = %v", err)
	}
}

// TestLogin_ACancelledLoginIsNotAbandoned: a cancellation while Vault is slow
// does not cut the call: the login comes back, for whoever gave it up to
// close.
func TestLogin_ACancelledLoginIsNotAbandoned(t *testing.T) {
	f := newFault(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		_, _ = w.Write([]byte(loginBody("hvs.slow", 60)))
	})
	f.on(revokePath, status(204))
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		s   provider.Session
		err error
	}
	done := make(chan result, 1)
	p := f.provider()
	go func() {
		s, err := p.Login(ctx, "app", provider.Attempt{}, identity)
		done <- result{s, err}
	}()
	<-arrived
	cancel()
	close(release)
	r := <-done
	if r.err != nil {
		t.Fatalf("the cancelled login = %v; its token is now nobody's", r.err)
	}
	if err := r.s.Close(context.Background()); err != nil || f.count(revokePath+" hvs.slow") != 1 {
		t.Fatalf("close = %v, revoked %d times", err, f.count(revokePath))
	}
}

// TestLogin_AnInstancesTokenIsBounded: an event source's instance logs in
// only with a token that has no period and an explicit maximum at or below
// its own bound, and one that is refused is revoked; a task's login is not
// held to it.
func TestLogin_AnInstancesTokenIsBounded(t *testing.T) {
	for name, c := range map[string]struct {
		lookup http.HandlerFunc
		limit  time.Duration
		code   string
	}{
		"bounded":          {body(`{"data":{"explicit_max_ttl":3600,"period":0}}`), time.Hour, ""},
		"no period stated": {body(`{"data":{"explicit_max_ttl":60}}`), time.Hour, ""},
		"a period":         {body(`{"data":{"explicit_max_ttl":3600,"period":60}}`), time.Hour, "vault-token-unbounded"},
		"no maximum":       {body(`{"data":{"explicit_max_ttl":0}}`), time.Hour, "vault-token-unbounded"},
		"a longer maximum": {body(`{"data":{"explicit_max_ttl":3601}}`), time.Hour, "vault-token-unbounded"},
		"an outage":        {status(503), time.Hour, "503"},
		"no answer":        {body(`{"data":null}`), time.Hour, "malformed-answer"},
		"no bound stated":  {body(`{}`), 0, "instance-login-unbounded"},
	} {
		f := newFault(t)
		f.on(loginPath, body(loginBody("hvs.a", 60)))
		f.on(lookupPath, c.lookup)
		f.on(revokePath, status(204))
		p := f.provider()
		_, err := p.Login(context.Background(), "app", provider.Attempt{Instance: true, MaxLogin: c.limit}, identity)
		if (err == nil) != (c.code == "") || (c.code != "" && !is(err, c.code)) {
			t.Errorf("%s: an instance's login = %v; want %q", name, err, c.code)
		}
		wantRevokes := 0
		if c.code != "" && c.limit > 0 {
			wantRevokes = 1
		}
		if n := f.count(revokePath); n != wantRevokes {
			t.Errorf("%s: revoked %d times, want %d", name, n, wantRevokes)
		}
		if _, err := f.login(p); err != nil {
			t.Errorf("%s: a task's login = %v", name, err)
		}
	}
}

// TestSession_AnEndedLoginIsMadeAgainOnce: a 403 whose token Vault no longer
// knows is answered by one more login and the call repeated, and the close
// revokes both; a 403 of a live token is Vault's answer, and
// costs no login.
func TestSession_AnEndedLoginIsMadeAgainOnce(t *testing.T) {
	f := newFault(t)
	var made atomic.Int64
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loginBody(fmt.Sprintf("hvs.%d", made.Add(1)), 600)))
	})
	f.on(revokePath, status(204))
	ended := map[string]bool{}
	var mu sync.Mutex
	gone := func(r *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		return ended[r.Header.Get("X-Vault-Token")]
	}
	f.on(lookupPath, func(w http.ResponseWriter, r *http.Request) {
		if gone(r) {
			w.WriteHeader(403)
			return
		}
		_, _ = w.Write([]byte(`{"data":{}}`))
	})
	f.on("GET kv/app", func(w http.ResponseWriter, r *http.Request) {
		if gone(r) {
			w.WriteHeader(403)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"a":"b"}}`))
	})
	p := f.provider()
	s := f.mustLogin(p)
	ref := parsed(t, p, provider.KindSecret, "kv/app#a")
	if _, err := s.Secret(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ended["hvs.1"] = true
	mu.Unlock()
	if v, err := s.Secret(context.Background(), ref); err != nil || string(v.Bytes) != "b" || made.Load() != 2 {
		t.Fatalf("a read past the login's end = %q, %v after %d logins", v.Bytes, err, made.Load())
	}
	if _, err := s.Secret(context.Background(), parsed(t, p, provider.KindSecret, "kv/forbidden#a")); provider.ClassOf(err) != provider.Refused || made.Load() != 2 {
		t.Fatalf("a refused read = %v after %d logins, want refused and no login", err, made.Load())
	}
	f.on(lookupPath, status(503))
	if _, err := s.Secret(context.Background(), parsed(t, p, provider.KindSecret, "kv/forbidden#a")); !is(err, "503") || provider.ClassOf(err) != provider.Unavailable {
		t.Fatalf("a 403 whose cause could not be asked = %v, want unavailable", err)
	}
	f.on(lookupPath, status(403))
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.count(revokePath+" hvs.1") != 1 || f.count(revokePath+" hvs.2") != 1 {
		t.Fatalf("revoked %v; want both logins", f.asked)
	}
	if _, err := s.Secret(context.Background(), ref); err == nil {
		t.Fatal("a closed session read a secret")
	}
}

// TestRenew: a lease or a login is renewed while the renewal leaves more than
// was left and at least half of its first life; a renewal that shrinks, or
// one Vault refuses, asks for it anew, and an outage does not.
func TestRenew(t *testing.T) {
	leased := func(d int) string {
		return fmt.Sprintf(`{"lease_id":"db/creds/app/1","lease_duration":%d,"renewable":true,"data":{"username":"u"}}`, d)
	}
	renewed := func(d int) http.HandlerFunc {
		return body(fmt.Sprintf(`{"lease_id":"db/creds/app/1","lease_duration":%d,"renewable":true}`, d))
	}
	for name, c := range map[string]struct {
		answer  http.HandlerFunc
		reissue bool
		class   provider.Class
		life    time.Duration
	}{
		"extended":              {renewed(3600), false, 0, time.Hour},
		"extended less":         {renewed(1800), false, 0, 30 * time.Minute},
		"only Vault's rounding": {renewed(1201), true, 0, 0},
		"shrunk past half":      {renewed(1799), true, 0, 0},
		"gone":                  {status(400), true, 0, 0},
		"not allowed":           {status(403), true, 0, 0},
		"an outage":             {status(503), false, provider.Unavailable, 0},
	} {
		f := newFault(t)
		f.on(loginPath, body(loginBody("hvs.a", 3600)))
		f.on(lookupPath, body(`{}`))
		f.on("GET db/creds/app", body(leased(3600)))
		f.on(leasePath, c.answer)
		p := f.provider()
		s := f.mustLogin(p)
		b, err := s.Credential(context.Background(), parsed(t, p, provider.KindCredential, "db/creds/app"))
		if err != nil {
			t.Fatal(err)
		}
		// A third of the lease is gone.
		h := b.Handle.(lease)
		h.got, h.life = time.Now(), 20*time.Minute
		if name == "only Vault's rounding" {
			h.first = 30 * time.Minute // half of it is under what is left
		}
		b.Handle = h
		got, err := s.Renew(context.Background(), b)
		switch {
		case c.reissue:
			if !errors.Is(err, provider.ErrReissue) {
				t.Errorf("%s: %v; want it issued again", name, err)
			}
		case c.life > 0:
			if err != nil || got.Lifetime != c.life || string(got.Files["username"]) != "u" {
				t.Errorf("%s: %+v, %v; want the same files for %s", name, got, err, c.life)
			}
		default:
			if err == nil || errors.Is(err, provider.ErrReissue) || provider.ClassOf(err) != c.class {
				t.Errorf("%s: %v; want %s", name, err, c.class)
			}
		}
	}
}

func TestRenewLogin(t *testing.T) {
	for name, c := range map[string]struct {
		answer  http.HandlerFunc
		want    time.Duration
		reissue bool
	}{
		"extended":              {body(loginBody("hvs.a", 3600)), time.Hour, false},
		"only Vault's rounding": {body(loginBody("hvs.a", 2401)), 0, true},
		"again at once":         {body(loginBody("hvs.a", 3600)), time.Hour, false},
		"at its maximum":        {body(loginBody("hvs.a", 100)), 0, true},
		"over the bound":        {body(loginBody("hvs.a", 7200)), 0, true},
		"refused":               {status(403), 0, true},
		"an outage":             {status(500), 0, false},
		"an answer of nothing":  {body(`{}`), 0, false},
	} {
		f := newFault(t)
		f.on(loginPath, body(loginBody("hvs.a", 3600)))
		f.on(renewPath, c.answer)
		p := f.provider()
		s := f.mustLogin(p)
		if name != "again at once" {
			s.logins[0].ttl = 40 * time.Minute
		}
		d, err := s.RenewLogin(context.Background())
		if d != c.want || errors.Is(err, provider.ErrReissue) != c.reissue || (err == nil) != (c.want > 0) {
			t.Errorf("%s: %s, %v; want %s, issued again %v", name, d, err, c.want, c.reissue)
		}
		if name == "over the bound" && s.logins[0].ttl != 2*time.Hour {
			t.Errorf("%s: the token is held to live %s, and Vault gave it 2h", name, s.logins[0].ttl)
		}
		b, err := s.Credential(context.Background(), parsed(t, p, provider.KindCredential, ""))
		if err != nil || string(b.Files["token"]) != "hvs.a" || !b.Plain["address"] || b.Plain["token"] ||
			len(b.Secrets) != 1 || string(b.Secrets[0]) != "hvs.a" {
			t.Errorf("%s: the login's bundle = %+v, %v", name, b.Plain, err)
		}
	}
}

// TestCall_NamesItsNamespace: every call of a declaration with a Vault
// namespace carries it, and the request says it is one.
func TestCall_NamesItsNamespace(t *testing.T) {
	f := newFault(t)
	f.on(loginPath, func(w http.ResponseWriter, r *http.Request) {
		var in loginRequest
		if err := json.UnmarshalRead(r.Body, &in); err != nil || in.Role != "app" || in.JWT != identityJWT ||
			r.Header.Get("X-Vault-Namespace") != "team/a" || r.Header.Get("X-Vault-Request") != "true" {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write([]byte(loginBody("hvs.a", 60)))
	})
	p := f.provider().(*vaultProvider)
	p.namespace = "team/a"
	f.mustLogin(p)
}

// TestLogin_MustBeAbleToEndItself: a role whose token may not look itself up
// or revoke itself is refused, since a 403 could then not be told from its
// end and nothing it was issued could be revoked; its accessor is logged.
func TestLogin_MustBeAbleToEndItself(t *testing.T) {
	for name, c := range map[string]struct {
		answer http.HandlerFunc
		class  provider.Class
		ok     bool
	}{
		"both":              {body(selfCapabilities), 0, true},
		"root":              {body(`{"data":{"auth/token/lookup-self":["root"],"auth/token/revoke-self":["root"]}}`), 0, true},
		"may not ask":       {status(403), provider.Refused, false},
		"denied the revoke": {body(`{"data":{"auth/token/lookup-self":["read"],"auth/token/revoke-self":["deny"]}}`), provider.Refused, false},
		"denied the lookup": {body(`{"data":{"auth/token/lookup-self":["deny"],"auth/token/revoke-self":["update"]}}`), provider.Refused, false},
		"silent on one":     {body(`{"data":{"auth/token/lookup-self":["read"]}}`), provider.Refused, false},
		"an outage":         {status(503), provider.Unavailable, false},
		"not an answer":     {body(`{"data":[]}`), provider.Unavailable, false},
	} {
		f := newFault(t)
		f.on(loginPath, body(loginBody("hvs.a", 60)))
		f.on(revokePath, status(204))
		f.on(capabilitiesPath, c.answer)
		_, err := f.login(f.provider())
		if (err == nil) != c.ok || (err != nil && provider.ClassOf(err) != c.class) {
			t.Errorf("%s: login = %v; want accepted %v (%s)", name, err, c.ok, c.class)
		}
		if n := f.count(revokePath); (n == 0) != c.ok {
			t.Errorf("%s: revoked %d times", name, n)
		}
	}
}

// TestLogin_AMalformedAnswersTokenIsRevoked: an answer that cannot be used
// and holds a token all the same leaves none behind.
func TestLogin_AMalformedAnswersTokenIsRevoked(t *testing.T) {
	f := newFault(t)
	f.on(loginPath, body(`{"auth":{"client_token":"hvs.stray","accessor":"a","lease_duration":60},"auth":{"client_token":"hvs.stray","accessor":"a"}}`))
	f.on(revokePath, status(204))
	if _, err := f.login(f.provider()); !is(err, "malformed-answer") || f.count(revokePath+" hvs.stray") != 1 {
		t.Fatalf("login = %v, revoked %d times", err, f.count(revokePath))
	}
}

// TestClose_KeepsWhatItCouldNotRevoke: a revoke Vault does not answer is
// tried again, one it refuses is not, either is an error and logged by its
// accessor, and a second Close tries it again.
func TestClose_KeepsWhatItCouldNotRevoke(t *testing.T) {
	var log bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 600)))
	f.on(revokePath, status(503))
	s := f.mustLogin(f.provider())
	if err := s.Close(context.Background()); provider.ClassOf(err) != provider.Unavailable || err == nil || f.count(revokePath) != revokeTries {
		t.Fatalf("close in an outage = %v after %d tries", err, f.count(revokePath))
	}
	if !strings.Contains(log.String(), "acc-hvs.a") || strings.Contains(log.String(), "=hvs.a") {
		t.Fatalf("logged %q; want the accessor and never the token", log.String())
	}
	f.on(revokePath, status(403))
	f.on(lookupPath, body(`{}`)) // Vault still knows it: the refusal is one
	if err := s.Close(context.Background()); !is(err, "403") || f.count(revokePath) != revokeTries+1 {
		t.Fatalf("close refused = %v after %d calls", err, f.count(revokePath))
	}
	f.on(revokePath, status(204))
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := f.count(revokePath)
	if err := s.Close(context.Background()); err != nil || f.count(revokePath) != calls {
		t.Fatalf("a close with nothing left = %v after %d more calls", err, f.count(revokePath)-calls)
	}
	if d := s.Lifetime(); d > time.Second || d == 0 {
		t.Fatalf("a closed session lasts %s", d)
	}
}

// endedLogin is a session whose token Vault no longer knows, and how many
// logins were made.
func endedLogin(t *testing.T, f *fault) (*session, provider.Parsed, *atomic.Int64) {
	var made atomic.Int64
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loginBody(fmt.Sprintf("hvs.%d", made.Add(1)), 600)))
	})
	f.on(revokePath, status(204))
	f.on(lookupPath, status(403))
	f.on("GET kv/app", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") == "hvs.1" {
			w.WriteHeader(403)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"a":"b"}}`))
	})
	p := f.provider()
	return f.mustLogin(p), parsed(t, p, provider.KindSecret, "kv/app#a"), &made
}

// TestSession_ConcurrentCallsLogInOnce: calls that all find the token ended
// share one new login.
func TestSession_ConcurrentCallsLogInOnce(t *testing.T) {
	f := newFault(t)
	s, ref, made := endedLogin(t, f)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if v, err := s.Secret(context.Background(), ref); err != nil || string(v.Bytes) != "b" {
				t.Errorf("read = %q, %v", v.Bytes, err)
			}
		})
	}
	wg.Wait()
	if made.Load() != 2 {
		t.Fatalf("%d logins for eight calls, want two in all", made.Load())
	}
}

// TestSession_ALoginThatOutlivesItsSessionIsRevoked: a session closed while
// it logs in again leaves no token behind.
func TestSession_ALoginThatOutlivesItsSessionIsRevoked(t *testing.T) {
	f := newFault(t)
	s, ref, _ := endedLogin(t, f)
	arrived, release := make(chan struct{}), make(chan struct{})
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		_, _ = w.Write([]byte(loginBody("hvs.late", 600)))
	})
	done := make(chan error, 1)
	go func() {
		_, err := s.Secret(context.Background(), ref)
		done <- err
	}()
	<-arrived
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("a closed session read a secret")
	}
	if f.count(revokePath+" hvs.late") != 1 || f.count(revokePath+" hvs.1") != 1 {
		t.Fatalf("revoked %v; want both tokens", f.asked)
	}
}

// TestSession_ARefusedLoginIsNotTheSecretsRefusal: a login refused inside a
// read is unavailable there; the token source's own refusal passes through;
// and a token that keeps ending stops being replaced.
func TestSession_ARefusedLoginIsNotTheSecretsRefusal(t *testing.T) {
	f := newFault(t)
	s, ref, made := endedLogin(t, f)
	f.on(loginPath, status(403))
	if _, err := s.Secret(context.Background(), ref); !is(err, "login-refused") || provider.ClassOf(err) != provider.Unavailable {
		t.Errorf("a login refused inside a read = %v", err)
	}
	s.token = func(context.Context) (string, time.Duration, error) {
		return "", 0, &provider.TokenRefused{Reason: "IDENTITY_REVOKED"}
	}
	var refused *provider.TokenRefused
	if _, err := s.Secret(context.Background(), ref); !errors.As(err, &refused) {
		t.Errorf("a revoked identity inside a read = %v", err)
	}
	s.token = identity
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		made.Add(1)
		_, _ = w.Write([]byte(loginBody("hvs.x", 600)))
	})
	f.on("GET kv/app", status(403))
	for range 2 * maxLogins {
		_, _ = s.Secret(context.Background(), ref)
	}
	if n := made.Load(); n != maxLogins {
		t.Errorf("%d logins by one session, want at most %d", n, maxLogins)
	}
}

// TestCredential_ALoginNearItsEndIsMadeAgain: the login as a credential never
// carries a lifetime of zero, which would say it does not end.
func TestCredential_ALoginNearItsEndIsMadeAgain(t *testing.T) {
	f := newFault(t)
	s, _, made := endedLogin(t, f)
	ref := parsed(t, s.p, provider.KindCredential, "")
	s.logins[0].got = time.Now().Add(-time.Hour)
	if d := s.Lifetime(); d <= 0 {
		t.Fatalf("an ended login's lifetime = %s; zero says it never ends", d)
	}
	b, err := s.Credential(context.Background(), ref)
	if err != nil || string(b.Files["token"]) != "hvs.2" || b.Lifetime < 9*time.Minute || made.Load() != 2 {
		t.Fatalf("the credential = %s for %s, %v", b.Files["token"], b.Lifetime, err)
	}
	if _, err := s.Renew(context.Background(), provider.Bundle{Handle: loginHandle{&login{}}}); !errors.Is(err, provider.ErrReissue) {
		t.Fatalf("renewing the ended login's bundle = %v", err)
	}
	for _, ttl := range []int{10, 30} { // 30 s is under 30 s left at once
		f.on(loginPath, body(loginBody("hvs.short", ttl)))
		short := f.mustLogin(s.p)
		if _, err := short.Credential(context.Background(), ref); !is(err, fmt.Sprintf("login-ttl-%ds-not-over-30s", ttl)) || provider.ClassOf(err) != provider.Refused {
			t.Fatalf("a %d s role's credential = %v", ttl, err)
		}
	}
}

// TestClose_ATokenVaultNoLongerKnowsIsRevokedAlready: a token that expired or
// was revoked by an operator is closed with no error, and an accessor for
// whoever wants to check.
func TestClose_ATokenVaultNoLongerKnowsIsRevokedAlready(t *testing.T) {
	var log bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 600)))
	f.on(revokePath, status(403))
	var asked atomic.Int64
	f.on(lookupPath, func(w http.ResponseWriter, _ *http.Request) {
		if asked.Add(1) == 1 {
			w.WriteHeader(503) // could not be asked: tried again
			return
		}
		w.WriteHeader(403)
	})
	if err := f.mustLogin(f.provider()).Close(context.Background()); err != nil || !strings.Contains(log.String(), "level=INFO") || !strings.Contains(log.String(), "acc-hvs.a") {
		t.Fatalf("close = %v, logged %q", err, log.String())
	}
}

// TestSession_LoginsThatRanOutAreNotCounted: a long attempt whose logins are
// short makes one after another for as long as it lasts.
func TestSession_LoginsThatRanOutAreNotCounted(t *testing.T) {
	f := newFault(t)
	s, _, made := endedLogin(t, f)
	ref := parsed(t, s.p, provider.KindCredential, "")
	for range 2 * maxKept {
		s.logins[len(s.logins)-1].got = time.Now().Add(-time.Hour)
		if _, err := s.Credential(context.Background(), ref); err != nil {
			t.Fatalf("after %d logins: %v", made.Load(), err)
		}
	}
	if len(s.logins) > maxKept+1 {
		t.Fatalf("%d logins kept, want at most %d", len(s.logins), maxKept+1)
	}
}

// TestRenewLogin_MayRunTwiceAtOnce: a renewal given up on may still run
// beside the next, and beside a read, a lifetime and a close.
func TestRenewLogin_MayRunTwiceAtOnce(t *testing.T) {
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 3600)))
	f.on(renewPath, body(loginBody("hvs.a", 3600)))
	f.on(revokePath, status(204))
	p := f.provider()
	s := f.mustLogin(p)
	ref := parsed(t, p, provider.KindCredential, "")
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { _, _ = s.RenewLogin(context.Background()) })
		wg.Go(func() { _, _ = s.Credential(context.Background(), ref) })
		wg.Go(func() { s.Lifetime() })
	}
	wg.Go(func() { _ = s.Close(context.Background()) })
	wg.Wait()
	if err := s.Close(context.Background()); err != nil || f.count(revokePath+" hvs.a") != 1 {
		t.Fatalf("close = %v, revoked %d times", err, f.count(revokePath))
	}
}

// TestRenewLogin_AnOlderAnswerDoesNotReplaceANewer: a renewal given up on
// that answers after a later one leaves the later one's life.
func TestRenewLogin_AnOlderAnswerDoesNotReplaceANewer(t *testing.T) {
	f := newFault(t)
	f.on(loginPath, body(loginBody("hvs.a", 1800)))
	arrived, release := make(chan struct{}), make(chan struct{})
	var asked atomic.Int64
	f.on(renewPath, func(w http.ResponseWriter, _ *http.Request) {
		if asked.Add(1) == 1 {
			close(arrived)
			<-release
			_, _ = w.Write([]byte(loginBody("hvs.a", 3000)))
			return
		}
		_, _ = w.Write([]byte(loginBody("hvs.a", 3600)))
	})
	s := f.mustLogin(f.provider())
	older := make(chan time.Duration, 1)
	go func() {
		d, _ := s.RenewLogin(context.Background())
		older <- d
	}()
	<-arrived
	if d, err := s.RenewLogin(context.Background()); err != nil || d != time.Hour {
		t.Fatalf("the later renewal = %s, %v", d, err)
	}
	close(release)
	if d := <-older; d < 55*time.Minute {
		t.Fatalf("the older renewal says %s; the newer one's life stands", d)
	}
	if d := s.Lifetime(); d < 55*time.Minute {
		t.Fatalf("the login lasts %s; the older answer replaced the newer", d)
	}
}
