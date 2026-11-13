package authn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

var now0 = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

func newSigner(t testing.TB) *jwt.Signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// verified signs claims and verifies them against i, returning the claims
// PrincipalFromClaims is handed in production.
func verified(t *testing.T, i *Issuer, extra map[string]any) *jwt.Claims {
	t.Helper()
	s := newSigner(t)
	claims := map[string]any{"iss": i.Issuer, "sub": "s-1", "aud": i.Audiences[0], "iat": now0.Unix(), "exp": now0.Add(time.Minute).Unix()}
	for k, v := range extra {
		claims[k] = v
	}
	token, err := s.Sign("JWT", claims)
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := jwt.StaticByThumbprint(s.Public())
	p := i.Policy(time.Minute)
	p.Algorithms = []jwt.Algorithm{jwt.ES256}
	c, err := jwt.Verify(context.Background(), token, p, keys, now0)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func issuersFor(t *testing.T) Issuers {
	t.Helper()
	set, err := ParseIssuers([]byte(specExample))
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestPrincipalFromClaims(t *testing.T) {
	set := issuersFor(t)
	corp, _ := set.ByName("corp")
	gh, _ := set.ByName("github")
	cluster, _ := set.ByName("cluster")

	t.Run("a person by verified email, with groups", func(t *testing.T) {
		c := verified(t, corp, map[string]any{"email": "ana@acme.dev", "email_verified": true, "groups": []string{"ops", "dev"}, "secret": "dropped"})
		p, dropped, err := PrincipalFromClaims(corp, c, MethodOIDC)
		if err != nil || dropped {
			t.Fatal(err, dropped)
		}
		if p.ID != "user:corp:ana@acme.dev" || p.Kind != KindUser || p.Display != "ana@acme.dev" || strings.Join(p.Groups, ",") != "ops,dev" || p.Method != MethodOIDC || p.Issuer != "corp" {
			t.Fatalf("%+v", p)
		}
		if len(p.Claims) != 0 {
			t.Fatalf("an unlisted claim was kept: %v", p.Claims)
		}
	})
	t.Run("an unverified email is refused", func(t *testing.T) {
		for _, v := range []any{false, "true", nil} {
			extra := map[string]any{"email": "ana@acme.dev"}
			if v != nil {
				extra["email_verified"] = v
			}
			if _, _, err := PrincipalFromClaims(corp, verified(t, corp, extra), MethodOIDC); !errors.Is(err, ErrClaims) {
				t.Fatalf("email_verified %v: %v", v, err)
			}
		}
	})
	t.Run("groups over the bound yield none", func(t *testing.T) {
		many := make([]string, MaxGroups+1)
		for i := range many {
			many[i] = fmt.Sprint("g", i)
		}
		for name, groups := range map[string]any{
			"too many":       many,
			"one too long":   []string{strings.Repeat("g", MaxGroupBytes+1)},
			"overage object": map[string]any{"src1": "endpoint"},
		} {
			c := verified(t, corp, map[string]any{"email": "a@acme.dev", "email_verified": true, "groups": groups})
			p, dropped, err := PrincipalFromClaims(corp, c, MethodOIDC)
			if err != nil || !dropped || len(p.Groups) != 0 {
				t.Fatalf("%s: groups %v dropped %v err %v", name, p.Groups, dropped, err)
			}
		}
	})
	t.Run("a service, its owner required, its listed claims kept", func(t *testing.T) {
		for _, owner := range []any{"1234567", 1234567} {
			c := verified(t, gh, map[string]any{"repository_owner_id": owner, "repository": "acme/app", "ref": "refs/heads/main", "job_workflow_ref": "dropped"})
			p, _, err := PrincipalFromClaims(gh, c, MethodJWT)
			if err != nil {
				t.Fatalf("owner %v: %v", owner, err)
			}
			if p.ID != "service:github:s-1" || p.Kind != KindService || p.Claims["repository"] != "acme/app" || p.Claims["ref"] != "refs/heads/main" || len(p.Claims) != 2 {
				t.Fatalf("%+v", p)
			}
		}
		for _, owner := range []any{"7654321", 1234567.5, true, "12345670", nil} {
			extra := map[string]any{}
			if owner != nil {
				extra["repository_owner_id"] = owner
			}
			if _, _, err := PrincipalFromClaims(gh, verified(t, gh, extra), MethodJWT); !errors.Is(err, ErrClaims) {
				t.Fatalf("owner %v accepted: %v", owner, err)
			}
		}
	})
	t.Run("an Orker's id from its claim path", func(t *testing.T) {
		c := verified(t, cluster, map[string]any{"kubernetes.io": map[string]any{"pod": map[string]any{"name": "orker-7"}}})
		p, _, err := PrincipalFromClaims(cluster, c, MethodJWT)
		if err != nil || p.OrkerID != "orker-7" {
			t.Fatalf("%+v %v", p, err)
		}
		c = verified(t, cluster, map[string]any{"kubernetes.io": map[string]any{"pod": map[string]any{"name": true}}})
		if p, _, _ := PrincipalFromClaims(cluster, c, MethodJWT); p.OrkerID != "" {
			t.Fatalf("a boolean pod name became an Orker id %q", p.OrkerID)
		}
	})
	t.Run("a control character in an id is refused", func(t *testing.T) {
		for _, sub := range []string{"a\nforged: line", "tab\there", "nul\x00"} {
			c := verified(t, gh, map[string]any{"repository_owner_id": "1234567", "sub": sub})
			if _, _, err := PrincipalFromClaims(gh, c, MethodJWT); !errors.Is(err, ErrClaims) {
				t.Fatalf("sub %q: %v", sub, err)
			}
		}
		c := verified(t, corp, map[string]any{"email": "a@acme.dev", "email_verified": true, "name": "Ana\nAdmin"})
		if p, _, err := PrincipalFromClaims(corp, c, MethodOIDC); err != nil || p.Display != "a@acme.dev" {
			t.Fatalf("a display name with a newline: %+v %v", p, err)
		}
	})
	t.Run("an id over the bound is refused", func(t *testing.T) {
		c := verified(t, gh, map[string]any{"repository_owner_id": "1234567", "sub": strings.Repeat("s", MaxIDBytes)})
		if _, _, err := PrincipalFromClaims(gh, c, MethodJWT); !errors.Is(err, ErrClaims) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("another issuer's claims are refused", func(t *testing.T) {
		c := verified(t, gh, map[string]any{"repository_owner_id": "1234567"})
		if _, _, err := PrincipalFromClaims(cluster, c, MethodJWT); !errors.Is(err, ErrClaims) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestPrincipal_Context(t *testing.T) {
	ctx := context.Background()
	if FromContext(ctx) != nil {
		t.Fatal("an empty context carries a principal")
	}
	p := &Principal{ID: ServiceAccountID("ci")}
	if got := FromContext(WithPrincipal(ctx, p)); got != p || got.ID != "serviceaccount:ci" {
		t.Fatalf("got %+v", got)
	}
	if CertID("dns:a") != "cert:dns:a" || UserID("corp", "x") != "user:corp:x" || ServiceID("gh", "s") != "service:gh:s" {
		t.Fatal("principal id forms")
	}
}

// fakeIssuer serves discovery and a key set over TLS, counting fetches.
type fakeIssuer struct {
	srv          *httptest.Server
	mu           sync.Mutex
	jwks         []byte
	cacheControl string
	issuer       string // what discovery states; the server's URL by default
	jwksURI      string // what discovery states; the server's /keys by default
	down         atomic.Bool
	discoveries  atomic.Int32
	fetches      atomic.Int32
}

func newFakeIssuer(t *testing.T, keys ...jwt.PublicKey) *fakeIssuer {
	t.Helper()
	f := &fakeIssuer{}
	f.setKeys(t, keys...)
	f.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			f.discoveries.Add(1)
			iss, uri := f.issuer, f.jwksURI
			if iss == "" {
				iss = f.srv.URL
			}
			if uri == "" {
				uri = f.srv.URL + "/keys"
			}
			_ = json.MarshalWrite(w, map[string]string{"issuer": iss, "jwks_uri": uri})
		case "/keys":
			f.fetches.Add(1)
			if f.cacheControl != "" {
				w.Header().Set("Cache-Control", f.cacheControl)
			}
			_, _ = w.Write(f.jwks)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIssuer) setKeys(t *testing.T, keys ...jwt.PublicKey) {
	t.Helper()
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		tp, _ := k.Thumbprint()
		raw, err := k.MarshalJWK(tp)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, string(raw))
	}
	f.mu.Lock()
	f.jwks = []byte(`{"keys":[` + strings.Join(parts, ",") + `]}`)
	f.mu.Unlock()
}

type clock struct{ t atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.t.Load()) }
func (c *clock) advance(d time.Duration) { c.t.Add(int64(d)) }
func newClock() *clock                   { c := &clock{}; c.t.Store(now0.UnixNano()); return c }

func cacheFor(t *testing.T, f *fakeIssuer, c *clock, maxStale time.Duration) *KeyCache {
	t.Helper()
	i := &Issuer{Name: "fake", Issuer: f.srv.URL, MaxStale: maxStale}
	k, err := NewKeyCache(i, f.srv.Client(), c.now)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func kidOf(k jwt.PublicKey) string { tp, _ := k.Thumbprint(); return tp }

func TestKeyCache_RefreshesOnItsAgeWithinBounds(t *testing.T) {
	a := newSigner(t).Public()
	f := newFakeIssuer(t, a)
	c := newClock()
	k := cacheFor(t, f, c, time.Hour)
	ctx := context.Background()
	if _, ok, err := k.Key(ctx, kidOf(a)); !ok || err != nil {
		t.Fatalf("first lookup: %v %v", ok, err)
	}
	if f.discoveries.Load() != 1 || f.fetches.Load() != 1 {
		t.Fatalf("discoveries %d fetches %d", f.discoveries.Load(), f.fetches.Load())
	}
	// No Cache-Control: fresh for the minimum, 5 min.
	c.advance(4 * time.Minute)
	_, _, _ = k.Key(ctx, kidOf(a))
	if f.fetches.Load() != 1 {
		t.Fatal("refreshed before its age")
	}
	c.advance(time.Minute)
	_, _, _ = k.Key(ctx, kidOf(a))
	if f.fetches.Load() != 2 || f.discoveries.Load() != 1 {
		t.Fatalf("after 5 min: fetches %d discoveries %d", f.fetches.Load(), f.discoveries.Load())
	}
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"max-age=60", MinKeySetAge},
		{"public, max-age=1800", 30 * time.Minute},
		{"max-age=86400", MaxKeySetAge},
		{"no-store", MinKeySetAge},
		{"max-age=-5", MinKeySetAge},
		{"max-age=99999999999999999999", MinKeySetAge},
	} {
		h := http.Header{}
		h.Set("Cache-Control", tc.header)
		if got := keySetAge(h); got != tc.want {
			t.Errorf("%q: %s, want %s", tc.header, got, tc.want)
		}
	}
}

func TestKeyCache_UnknownKidRefreshesAtMostOnceAMinute(t *testing.T) {
	a, b := newSigner(t).Public(), newSigner(t).Public()
	f := newFakeIssuer(t, a)
	f.cacheControl = "max-age=3600"
	c := newClock()
	k := cacheFor(t, f, c, time.Hour)
	ctx := context.Background()
	_, _, _ = k.Key(ctx, kidOf(a))
	f.setKeys(t, a, b) // the issuer rotates in b
	for range 20 {
		_, _, _ = k.Key(ctx, "nobody")
	}
	if n := f.fetches.Load(); n != 2 {
		t.Fatalf("twenty unknown kids in one minute fetched %d times; want one refresh", n)
	}
	// That refresh brought b in.
	if _, ok, _ := k.Key(ctx, kidOf(b)); !ok {
		t.Fatal("the rotated key was not found after the refresh")
	}
	c.advance(UnknownKidInterval)
	_, _, _ = k.Key(ctx, "nobody")
	if n := f.fetches.Load(); n != 3 {
		t.Fatalf("after a minute: %d fetches", n)
	}
}

func TestKeyCache_StaleForItsBoundThenClosed(t *testing.T) {
	a := newSigner(t).Public()
	f := newFakeIssuer(t, a)
	c := newClock()
	k := cacheFor(t, f, c, time.Hour)
	ctx := context.Background()
	_, _, _ = k.Key(ctx, kidOf(a))
	f.down.Store(true)

	c.advance(MinKeySetAge + 59*time.Minute) // past its age, inside max_stale
	if _, ok, err := k.Key(ctx, kidOf(a)); !ok || err != nil {
		t.Fatalf("inside the stale bound the last set is served: %v %v", ok, err)
	}
	c.advance(time.Minute) // age + max_stale reached
	_, ok, err := k.Key(ctx, kidOf(a))
	if ok || jwt.ReasonOf(err) != jwt.ReasonUnavailable {
		t.Fatalf("past the stale bound the issuer must be unavailable: %v %v", ok, err)
	}
	f.down.Store(false)
	before := f.fetches.Load()
	if _, ok, _ := k.Key(ctx, kidOf(a)); ok || f.fetches.Load() != before {
		t.Fatal("a refresh that failed moments ago was retried inside the backoff")
	}
	c.advance(FailureBackoff)
	if _, ok, err := k.Key(ctx, kidOf(a)); !ok || err != nil {
		t.Fatalf("a refresh that succeeds reopens it: %v %v", ok, err)
	}
}

func TestKeyCache_AFailingIssuerIsNotFetchedOnEveryRequest(t *testing.T) {
	a := newSigner(t).Public()
	f := newFakeIssuer(t, a)
	c := newClock()
	k := cacheFor(t, f, c, time.Hour)
	ctx := context.Background()
	_, _, _ = k.Key(ctx, kidOf(a))
	f.down.Store(true)
	c.advance(MinKeySetAge) // stale, still servable
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if _, ok, err := k.Key(ctx, kidOf(a)); !ok || err != nil {
				t.Errorf("inside the stale bound: %v %v", ok, err)
			}
		})
	}
	wg.Wait()
	// One failed refresh, then the backoff: the 31 others serve the stale set.
	if n := f.fetches.Load(); n != 1 {
		t.Fatalf("32 lookups against a failing issuer made %d fetches after the first", n-1)
	}
}

func TestKeyCache_AWaiterLeavesWhenItsContextEnds(t *testing.T) {
	a := newSigner(t).Public()
	f := newFakeIssuer(t, a)
	k := cacheFor(t, f, newClock(), time.Hour)
	k.refresh <- struct{}{} // a refresh in progress that never ends
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok, err := k.Key(ctx, kidOf(a))
	if ok || jwt.ReasonOf(err) != jwt.ReasonUnavailable || !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled waiter: %v %v", ok, err)
	}
	<-k.refresh
}

func TestKeyCache_NeverFetchedIsUnavailable(t *testing.T) {
	f := newFakeIssuer(t)
	f.down.Store(true)
	k := cacheFor(t, f, newClock(), time.Hour)
	if _, ok, err := k.Key(context.Background(), "x"); ok || jwt.ReasonOf(err) != jwt.ReasonUnavailable {
		t.Fatalf("got %v %v", ok, err)
	}
}

func TestKeyCache_DiscoveryRules(t *testing.T) {
	a := newSigner(t).Public()
	for name, mutate := range map[string]func(f *fakeIssuer){
		"another issuer stated": func(f *fakeIssuer) { f.issuer = f.srv.URL + "/" },
		"keys on another host":  func(f *fakeIssuer) { f.jwksURI = "https://keys.evil.example/keys" },
		"keys over http":        func(f *fakeIssuer) { f.jwksURI = strings.Replace(f.srv.URL, "https:", "http:", 1) + "/keys" },
		"oversized key set": func(f *fakeIssuer) {
			f.jwks = []byte(`{"keys":[],"pad":"` + strings.Repeat("x", jwt.MaxKeySetBytes) + `"}`)
		},
		"a private key in the key set": func(f *fakeIssuer) { f.jwks = []byte(`{"keys":[{"kty":"oct","kid":"h","k":"AA"}]}`) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeIssuer(t, a)
			mutate(f)
			k := cacheFor(t, f, newClock(), time.Hour)
			if _, ok, err := k.Key(context.Background(), kidOf(a)); ok || jwt.ReasonOf(err) != jwt.ReasonUnavailable {
				t.Fatalf("got %v %v", ok, err)
			}
		})
	}
	t.Run("a pinned jwks_uri skips discovery", func(t *testing.T) {
		f := newFakeIssuer(t, a)
		i := &Issuer{Name: "fake", Issuer: "https://elsewhere.example", JWKSURI: f.srv.URL + "/keys", MaxStale: time.Hour}
		k, err := NewKeyCache(i, f.srv.Client(), newClock().now)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := k.Key(context.Background(), kidOf(a)); !ok || err != nil || f.discoveries.Load() != 0 {
			t.Fatalf("got %v %v, %d discoveries", ok, err, f.discoveries.Load())
		}
	})
}

func TestKeyCache_JWKSFile(t *testing.T) {
	a := newSigner(t).Public()
	raw, _ := a.MarshalJWK("file-key")
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, []byte(`{"keys":[`+string(raw)+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := NewKeyCache(&Issuer{Name: "spire", Issuer: "spiffe://acme.dev", JWKSFile: path}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := k.Key(context.Background(), "file-key"); !ok || err != nil {
		t.Fatalf("got %v %v", ok, err)
	}
	if _, err := NewKeyCache(&Issuer{Name: "x", JWKSFile: filepath.Join(t.TempDir(), "missing")}, nil, nil); err == nil {
		t.Fatal("a missing jwks_file was accepted")
	}
}

func TestKeyCache_ConcurrentLookupsFetchOnce(t *testing.T) {
	a := newSigner(t).Public()
	f := newFakeIssuer(t, a)
	k := cacheFor(t, f, newClock(), time.Hour)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { _, _, _ = k.Key(context.Background(), kidOf(a)) })
	}
	wg.Wait()
	if n := f.fetches.Load(); n != 1 {
		t.Fatalf("32 concurrent lookups fetched %d times", n)
	}
}
