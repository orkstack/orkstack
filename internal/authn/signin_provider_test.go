package authn

import (
	"strings"
	"testing"
)

func TestSafeReturnPath(t *testing.T) {
	for in, want := range map[string]string{
		"/runs/ork-1":             "/runs/ork-1",
		"/runs?status=FAILED&n=2": "/runs?status=FAILED&n=2",
		"/":                       "/",
		"/orkflows/":              "/orkflows/",
		"":                        "/",
		"runs":                    "/",
		"//evil.example":          "/",
		"/\\evil.example":         "/",
		"/%5cevil.example":        "/",
		"/%2F%2Fevil.example":     "/",
		"https://evil.example/":   "/",
		"javascript:alert(1)":     "/",
		"/runs/../../etc":         "/",
		"/runs/./x":               "/",
		"/runs\n/x":               "/",
		"/runs#frag":              "/",
		"/a%00b":                  "/",
		"///evil.example":         "/",
		"/\t/evil.example":        "/",
		"http:/evil.example":      "/",
	} {
		if got := SafeReturnPath(in); got != want {
			t.Errorf("SafeReturnPath(%q) = %q; want %q", in, got, want)
		}
	}
}

func FuzzSafeReturnPath(f *testing.F) {
	for _, s := range []string{"/runs/x", "//evil", "/\\evil", "/%2f%2fevil", "https://e/", "/a/../b"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		// Properties: the answer is always a path of this origin, and
		// answering again changes nothing.
		got := SafeReturnPath(in)
		if got == "" || got[0] != '/' || (len(got) > 1 && (got[1] == '/' || got[1] == '\\')) {
			t.Fatalf("SafeReturnPath(%q) = %q leaves the origin", in, got)
		}
		if again := SafeReturnPath(got); again != got {
			t.Fatalf("SafeReturnPath is not stable: %q -> %q -> %q", in, got, again)
		}
	})
}

func TestCheckProviderMetadata(t *testing.T) {
	const iss = "https://login.example.test"
	ok := `{"issuer":"https://login.example.test","authorization_endpoint":"https://login.example.test/a","token_endpoint":"https://login.example.test/t"}`
	if _, err := CheckProviderMetadata([]byte(ok), iss); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"another issuer":      `{"issuer":"https://evil.test","authorization_endpoint":"https://a/a","token_endpoint":"https://a/t"}`,
		"no token endpoint":   `{"issuer":"https://login.example.test","authorization_endpoint":"https://a/a"}`,
		"plain http endpoint": `{"issuer":"https://login.example.test","authorization_endpoint":"http://a/a","token_endpoint":"https://a/t"}`,
		"userinfo endpoint":   `{"issuer":"https://login.example.test","authorization_endpoint":"https://u:p@a/a","token_endpoint":"https://a/t"}`,
		"no S256":             `{"issuer":"https://login.example.test","authorization_endpoint":"https://a/a","token_endpoint":"https://a/t","code_challenge_methods_supported":["plain"]}`,
		"not JSON":            `{`,
	} {
		if _, err := CheckProviderMetadata([]byte(doc), iss); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func FuzzCheckProviderMetadata(f *testing.F) {
	const iss = "https://login.example.test"
	f.Add([]byte(`{"issuer":"https://login.example.test","authorization_endpoint":"https://login.example.test/a","token_endpoint":"https://login.example.test/t"}`))
	f.Add([]byte(`{"issuer":"https://login.example.test","authorization_endpoint":"http://a/a","token_endpoint":"https://a/t"}`))
	f.Add([]byte(`{`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		// Properties: what is accepted states the configured issuer and
		// gives only https endpoints, with S256 when it lists methods.
		m, err := CheckProviderMetadata(raw, iss)
		if err != nil {
			return
		}
		if m.Issuer != iss {
			t.Fatalf("accepted issuer %q", m.Issuer)
		}
		for _, e := range []string{m.AuthorizationEndpoint, m.TokenEndpoint, m.EndSessionEndpoint, m.RevocationEndpoint, m.DeviceEndpoint} {
			if e != "" && !strings.HasPrefix(e, "https://") {
				t.Fatalf("accepted endpoint %q", e)
			}
		}
	})
}
