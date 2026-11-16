package authn

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestAssertionTransport_SignsEachRequestForTheEngine: every request the
// transport sends carries a fresh assertion bound to its method, URL and
// body, which the engine's middleware accepts.
func TestAssertionTransport_SignsEachRequestForTheEngine(t *testing.T) {
	f := newMiddlewareFixture(t)
	withAccounts(t, f, nil)
	f.auth.Now = time.Now
	f.auth.HostAllowed = func(string) bool { return true }
	srv := httptest.NewServer(f.handler(ModeEnforce))
	defer srv.Close()
	public, _ := url.Parse(srv.URL)
	f.auth.ServiceAccounts.Audience, f.auth.ServiceAccounts.PublicURL = srv.URL, public

	client := &http.Client{Transport: &AssertionTransport{Signer: f.signer, Account: "bot", Audience: srv.URL}}
	for i, body := range []string{"", `{"a":1}`, `{"a":1}`} {
		var rd io.Reader
		method := http.MethodGet
		if body != "" {
			rd, method = strings.NewReader(body), http.MethodPost
		}
		req, _ := http.NewRequest(method, srv.URL+"/api/x?q=1", rd)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d (%s %q) = %d", i, method, body, resp.StatusCode)
		}
	}
	if p := f.served[len(f.served)-1]; p.ID != "serviceaccount:bot" {
		t.Errorf("principal = %+v", p)
	}
}

func TestAssertionTransport_RefusesABodyOverTheBound(t *testing.T) {
	tr := &AssertionTransport{Signer: newSigner(t), Account: "bot", Audience: "https://ork.example.test",
		Base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("an oversized request was sent")
			return nil, nil
		})}
	req, _ := http.NewRequest(http.MethodPost, "https://ork.example.test/api/x", strings.NewReader(strings.Repeat("x", AssertionMaxBody+1)))
	if _, err := tr.RoundTrip(req); err == nil {
		t.Fatal("a body over the bound was signed")
	}
}

func TestAssertionTransport_BreakGlassCarriesItsReason(t *testing.T) {
	var got *http.Request
	tr := &AssertionTransport{Signer: newSigner(t), Account: BreakGlassAccount, Audience: "https://ork.example.test",
		Reason: "INC-42", Base: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			got = r
			return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
		})}
	req, _ := http.NewRequest(http.MethodGet, "https://ork.example.test/api/x", nil)
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got.Header.Get(ReasonHeader) != "INC-42" || !strings.HasPrefix(got.Header.Get("Authorization"), "Bearer ") {
		t.Errorf("headers = %v", got.Header)
	}
	if req.Header.Get("Authorization") != "" {
		t.Error("the caller's request was changed")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
