package identity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/jwt"
)

// TestHTTPLiveIssuer: the issuer URL's discovery document is read, its
// jwks_uri followed only to https on the issuer's own host, and the kids
// of the set it serves answered; every other answer is an error.
func TestHTTPLiveIssuer(t *testing.T) {
	k := newKey(t)
	set, err := jwt.MarshalKeySet([]jwt.PublicKey{k.Public()})
	if err != nil {
		t.Fatal(err)
	}
	var discovery, keys string
	status := http.StatusOK
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		switch r.URL.Path {
		case "/tenant/.well-known/openid-configuration":
			w.Write([]byte(discovery))
		case "/tenant/jwks.json":
			w.Write([]byte(keys))
		}
	}))
	defer srv.Close()
	issuer := srv.URL + "/tenant"
	live := HTTPLiveIssuer{URL: issuer, Client: srv.Client()}

	discovery, keys = `{"issuer":"`+issuer+`","jwks_uri":"`+issuer+`/jwks.json"}`, string(set)
	kids, err := live.PublishedKeyIDs(context.Background())
	if err != nil || !kids[k.KeyID()] || len(kids) != 1 {
		t.Fatalf("kids = %v, %v", kids, err)
	}
	for name, tc := range map[string]struct{ discovery, keys, want string }{
		"another issuer":         {`{"issuer":"https://other","jwks_uri":"` + issuer + `/jwks.json"}`, string(set), "states issuer"},
		"jwks on another host":   {`{"issuer":"` + issuer + `","jwks_uri":"https://evil.example/jwks.json"}`, string(set), "own host"},
		"jwks over http":         {`{"issuer":"` + issuer + `","jwks_uri":"` + strings.Replace(issuer, "https", "http", 1) + `/jwks.json"}`, string(set), "own host"},
		"discovery not JSON":     {`{`, string(set), "discovery"},
		"keys not a set":         {`{"issuer":"` + issuer + `","jwks_uri":"` + issuer + `/jwks.json"}`, `[]`, "key set"},
		"an oversized discovery": {`{"issuer":"` + strings.Repeat("x", maxDiscoveryBytes) + `"}`, string(set), "more than"},
	} {
		discovery, keys = tc.discovery, tc.keys
		if _, err := live.PublishedKeyIDs(context.Background()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	status = http.StatusNotFound
	if _, err := live.PublishedKeyIDs(context.Background()); err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("a 404: %v", err)
	}
}

// TestHTTPLiveIssuer_AKeyServedUnderAnotherKeysIDIsNotServed: the live set
// counts a kid only when the key under it is the key of that thumbprint,
// and a jwks_uri with a query is refused.
func TestHTTPLiveIssuer_AKeyServedUnderAnotherKeysIDIsNotServed(t *testing.T) {
	mine, other := newKey(t), newKey(t)
	wrong, _ := other.Public().MarshalJWK(mine.KeyID())
	var discovery string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			w.Write([]byte(discovery))
			return
		}
		w.Write([]byte(`{"keys":[` + string(wrong) + `]}`))
	}))
	defer srv.Close()
	live := HTTPLiveIssuer{URL: srv.URL, Client: srv.Client()}
	discovery = `{"issuer":"` + srv.URL + `","jwks_uri":"` + srv.URL + `/jwks.json"}`
	kids, err := live.PublishedKeyIDs(context.Background())
	if err != nil || kids[mine.KeyID()] {
		t.Fatalf("kids = %v, %v", kids, err)
	}
	discovery = `{"issuer":"` + srv.URL + `","jwks_uri":"` + srv.URL + `/jwks.json?x=1"}`
	if _, err := live.PublishedKeyIDs(context.Background()); err == nil {
		t.Fatal("a jwks_uri with a query was followed")
	}
}
