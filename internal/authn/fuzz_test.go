package authn

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// FuzzKeySetAge: whatever Cache-Control an issuer sends, the key set's age
// stays within [MinKeySetAge, MaxKeySetAge].
func FuzzKeySetAge(f *testing.F) {
	for _, seed := range []string{"", "max-age=600", "max-age=0", "max-age=99999999999", `max-age="300"`, "no-cache, max-age=-1", "MAX-AGE=10,max-age=7200"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cacheControl string) {
		h := http.Header{}
		h.Set("Cache-Control", cacheControl)
		if age := keySetAge(h); age < MinKeySetAge || age > MaxKeySetAge {
			t.Fatalf("Cache-Control %q gave %s, outside [%s, %s]", cacheControl, age, MinKeySetAge, MaxKeySetAge)
		}
	})
}

// FuzzDiscoveryDocument: an accepted discovery document stated the
// configured issuer and a jwks_uri that is https on the issuer's own host
// with no user information.
func FuzzDiscoveryDocument(f *testing.F) {
	const issuer = "https://login.example.test/realms/acme"
	for _, seed := range []string{
		`{"issuer":"https://login.example.test/realms/acme","jwks_uri":"https://login.example.test/keys"}`,
		`{"issuer":"https://login.example.test/realms/acme","jwks_uri":"https://evil.test/keys"}`,
		`{"issuer":"https://other.test","jwks_uri":"https://login.example.test/keys"}`,
		`{"issuer":"https://login.example.test/realms/acme","jwks_uri":"http://login.example.test/keys"}`,
		`{"issuer":"https://login.example.test/realms/acme","jwks_uri":"https://u:p@login.example.test/keys"}`,
		`[]`, ``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		uri, err := checkDiscovery(raw, issuer)
		if err != nil {
			return
		}
		u, perr := url.Parse(uri)
		if perr != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, "login.example.test") || u.User != nil {
			t.Fatalf("accepted jwks_uri %q", uri)
		}
	})
}

// FuzzBearer: an accepted Authorization header is exactly one Bearer token
// with no whitespace in it; a token is never taken from anything else.
func FuzzBearer(f *testing.F) {
	for _, seed := range []string{"", "Bearer abc.def.ghi", "bearer x", "Basic dXNlcg==", "Bearer ", "Bearer a b", "Bearer\tx", "Token x"} {
		f.Add(seed, false)
		f.Add(seed, true)
	}
	f.Fuzz(func(t *testing.T, header string, twice bool) {
		r, err := http.NewRequest(http.MethodGet, "http://ork.test/", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Add("Authorization", header)
		if twice {
			r.Header.Add("Authorization", header)
		}
		token, err := bearer(r)
		if err != nil || token == "" {
			return
		}
		if twice {
			t.Fatal("a token was taken from two Authorization headers")
		}
		scheme, rest, _ := strings.Cut(header, " ")
		if !strings.EqualFold(scheme, "Bearer") || rest != token || strings.ContainsAny(token, " \t") {
			t.Fatalf("header %q gave token %q", header, token)
		}
	})
}
