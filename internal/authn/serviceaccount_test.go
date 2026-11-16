package authn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/jwt"
)

// withAccounts gives the fixture's authenticator the service accounts
// "bot", whose key is the fixture's signer, and "gpu-1", an Orker's.
func withAccounts(t *testing.T, f *middlewareFixture, lookupErr error) *jwt.Signer {
	t.Helper()
	orker := newSigner(t)
	botKeys, _ := jwt.StaticByThumbprint(f.signer.Public())
	orkerKeys, _ := jwt.StaticByThumbprint(orker.Public())
	accounts := map[string]authz.ServiceAccount{
		"bot":   {Name: "bot", Keys: botKeys},
		"gpu-1": {Name: "gpu-1", Keys: orkerKeys, OrkerID: "gpu-1"},
	}
	public, _ := url.Parse("https://ork.example.test")
	f.auth.ServiceAccounts = &ServiceAccounts{
		Lookup: func(_ context.Context, name string) (authz.ServiceAccount, bool, error) {
			if lookupErr != nil {
				return authz.ServiceAccount{}, false, lookupErr
			}
			sa, ok := accounts[name]
			return sa, ok, nil
		},
		Audience: "https://ork.example.test", PublicURL: public, Replay: jwt.NewReplayCache(),
	}
	return orker
}

func TestMiddleware_ServiceAccountAssertions(t *testing.T) {
	f := newMiddlewareFixture(t)
	orker := withAccounts(t, f, nil)
	h := f.handler(ModeEnforce)
	sign := func(s *jwt.Signer, account, method, u, body string) string {
		return mustSignRequest(t, s, account, method, u, body)
	}

	if rec := do(h, "POST", "/api/x", sign(f.signer, "bot", "POST", "https://ork.example.test/api/x", `{"a":1}`), nil, `{"a":1}`); rec.Code != http.StatusOK {
		t.Fatalf("a bound assertion = %d (%s)", rec.Code, rec.Body)
	}
	p := f.served[len(f.served)-1]
	if p == nil || p.ID != "serviceaccount:bot" || p.Method != MethodServiceAccount || p.Kind != KindService ||
		p.Issuer != "serviceaccount" || p.OrkerID != "" || p.Expires.IsZero() {
		t.Fatalf("principal = %+v", p)
	}
	if rec := do(h, "GET", "/api/x", sign(orker, "gpu-1", "GET", "https://ork.example.test/api/x", ""), nil, ""); rec.Code != http.StatusOK {
		t.Fatalf("an Orker's account = %d (%s)", rec.Code, rec.Body)
	}
	if p := f.served[len(f.served)-1]; p.OrkerID != "gpu-1" {
		t.Errorf("an Orker's account names Orker %q; want gpu-1", p.OrkerID)
	}

	plain, err := f.signer.Sign("JWT", map[string]any{"iss": "serviceaccount:bot", "sub": "serviceaccount:bot",
		"aud": "https://ork.example.test", "iat": now0.Unix(), "exp": now0.Add(30 * time.Second).Unix(), "jti": "j-1"})
	if err != nil {
		t.Fatal(err)
	}
	refused := map[string]struct{ token, method, body string }{
		"another body":    {sign(f.signer, "bot", "POST", "https://ork.example.test/api/x", "{}"), "POST", `{"a":2}`},
		"another path":    {sign(f.signer, "bot", "POST", "https://ork.example.test/api/y", "{}"), "POST", "{}"},
		"another method":  {sign(f.signer, "bot", "PUT", "https://ork.example.test/api/x", "{}"), "POST", "{}"},
		"another host":    {sign(f.signer, "bot", "POST", "https://evil.example.test/api/x", "{}"), "POST", "{}"},
		"another's key":   {sign(orker, "bot", "POST", "https://ork.example.test/api/x", "{}"), "POST", "{}"},
		"unknown account": {sign(f.signer, "nobody", "POST", "https://ork.example.test/api/x", "{}"), "POST", "{}"},
		"an unbound JWT":  {plain, "POST", "{}"},
	}
	for name, tc := range refused {
		if rec := do(h, tc.method, "/api/x", tc.token, nil, tc.body); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d; want 401", name, rec.Code)
		}
	}

	tok := sign(f.signer, "bot", "POST", "https://ork.example.test/api/x", "{}")
	if rec := do(h, "POST", "/api/x", tok, nil, "{}"); rec.Code != http.StatusOK {
		t.Fatalf("first use = %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/x", tok, nil, "{}"); rec.Code != http.StatusUnauthorized {
		t.Errorf("a replayed assertion = %d; want 401", rec.Code)
	}
}

func TestMiddleware_ServiceAccountsUnavailable(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, errors.New("the access document is past its stale limit"))
	tok := mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", "{}")
	if rec := do(f.handler(ModeEnforce), "POST", "/api/x", tok, nil, "{}"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("accounts unreadable = %d; want 503", rec.Code)
	}
}

// TestMiddleware_ServiceAccountBodyIsBounded: the body of a service
// account's request is read to hash it, never past AssertionMaxBody.
func TestMiddleware_ServiceAccountBodyIsBounded(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, nil)
	body := &countingBody{}
	req := httptest.NewRequest(http.MethodPost, "/api/x", nil)
	req.Body = body
	req.Header.Set("Authorization", "Bearer "+mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", ""))
	rec := httptest.NewRecorder()
	f.handler(ModeEnforce).ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || body.n > AssertionMaxBody+64<<10 {
		t.Errorf("an endless body: %d after reading %d bytes; want 413 within %d", rec.Code, body.n, AssertionMaxBody)
	}
}

// TestMiddleware_AnUnknownAccountsBodyIsNeverRead: a bearer that claims an
// account the document does not hold, or announces a body over the bound,
// is refused before a byte is read, and an unknown account is answered as
// a key it does not hold.
func TestMiddleware_AnUnknownAccountsBodyIsNeverRead(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, nil)
	send := func(token string, length int64) (int, int, string) {
		body := &countingBody{}
		req := httptest.NewRequest(http.MethodPost, "/api/x", nil)
		req.Body, req.ContentLength = body, length
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		f.handler(ModeEnforce).ServeHTTP(rec, req)
		return rec.Code, body.n, rec.Body.String()
	}
	unknown := mustSignRequest(t, f.signer, "nobody", "POST", "https://ork.example.test/api/x", "")
	code, read, msg := send(unknown, -1)
	if code != http.StatusUnauthorized || read != 0 {
		t.Errorf("an unknown account: %d after reading %d bytes; want 401 and nothing read", code, read)
	}
	wrongKey := mustSignRequest(t, newSigner(t), "bot", "POST", "https://ork.example.test/api/x", "")
	other := do(f.handler(ModeEnforce), "POST", "/api/x", wrongKey, nil, "").Body.String()
	if !strings.Contains(msg, "(key)") || !strings.Contains(other, "(key)") {
		t.Errorf("an unknown account says %q, a wrong key %q; want both the key refusal", msg, other)
	}
	known := mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", "")
	if code, read, _ := send(known, AssertionMaxBody+1); code != http.StatusRequestEntityTooLarge || read != 0 {
		t.Errorf("an announced body over the bound: %d after reading %d bytes", code, read)
	}
}

// TestMiddleware_ATrickledBodyGivesItsSlotBack: a body that does not arrive
// within the bound is refused, and its slot is free again, so callers who
// stall cannot starve assertions.
func TestMiddleware_ATrickledBodyGivesItsSlotBack(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, nil)
	f.auth.BodyTimeout = 100 * time.Millisecond
	srv := httptest.NewServer(f.handler(ModeEnforce))
	defer srv.Close()
	token := mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/x", "")
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /api/x HTTP/1.1\r\nHost: ork.example.test\r\nAuthorization: Bearer %s\r\nContent-Length: 1000\r\n\r\nabc", token)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no answer to a stalled body: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a stalled body = %d; want 400", resp.StatusCode)
	}
	held := 0
	for {
		release, ok := f.auth.bodySlot()
		if !ok {
			break
		}
		defer release()
		held++
	}
	if held != MaxBufferedBodies {
		t.Errorf("free slots after a stalled body = %d; want %d", held, MaxBufferedBodies)
	}
}

// TestMiddleware_AForgedAssertionHoldsNoSlot: a token that claims an
// account, or break-glass, but that no key of it signed for this request is
// refused before a byte of its body is read.
func TestMiddleware_AForgedAssertionHoldsNoSlot(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, nil)
	stranger := newSigner(t)
	for name, token := range map[string]string{
		"break-glass":  mustSignRequest(t, stranger, BreakGlassAccount, "POST", "https://ork.example.test/api/x", ""),
		"an account":   mustSignRequest(t, stranger, "bot", "POST", "https://ork.example.test/api/x", ""),
		"another path": mustSignRequest(t, f.signer, "bot", "POST", "https://ork.example.test/api/y", ""),
	} {
		body := &countingBody{}
		req := httptest.NewRequest(http.MethodPost, "/api/x", nil)
		req.Body, req.ContentLength = body, -1
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set(ReasonHeader, "r")
		rec := httptest.NewRecorder()
		f.handler(ModeEnforce).ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || body.n != 0 {
			t.Errorf("%s: %d after reading %d bytes; want 401 and nothing read", name, rec.Code, body.n)
		}
	}
}
