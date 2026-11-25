package authn

import (
	"net/url"
	"strings"
	"testing"
	"unicode"
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
		// Dot segments are read decoded, as a browser reads them.
		"/%2e%2e/x":         "/",
		"/%2E":              "/",
		"/.%2e/":            "/",
		"/%2e%2e//evil.com": "/",
		// What is kept is escaped once, and never again.
		"/a b":    "/a%20b",
		"/é":      "/%C3%A9",
		"/[":      "/[",
		"/[ ":     "/%5B%20",
		"/a%7Eb":  "/a%7Eb",
		"/a%252f": "/a%252f",
		"/x?a b":  "/x?a b",
		"/%zz":    "/",
	} {
		got := SafeReturnPath(in)
		if got != want {
			t.Errorf("SafeReturnPath(%q) = %q; want %q", in, got, want)
		}
		if again := SafeReturnPath(got); again != got {
			t.Errorf("SafeReturnPath is not stable: %q -> %q -> %q", in, got, again)
		}
	}
}

// The bound is on the path a sign-in returns to, which escaping can make
// three times as long as the path asked for: one that fits only before it is
// escaped gives "/", as one too long to begin with does.
func TestSafeReturnPath_BoundsTheEscapedPath(t *testing.T) {
	fits := (MaxReturnPathBytes - len("/")) / len("%5B")
	for _, tt := range []struct {
		name, in, want string
	}{
		{"as long as the bound, nothing to escape", "/" + strings.Repeat("a", MaxReturnPathBytes-1), "/" + strings.Repeat("a", MaxReturnPathBytes-1)},
		{"one byte over it", "/" + strings.Repeat("a", MaxReturnPathBytes), "/"},
		{"fits once escaped", "/" + strings.Repeat("[", fits-1) + " ", "/" + strings.Repeat("%5B", fits-1) + "%20"},
		{"fits only before it is escaped", "/" + strings.Repeat("[", fits) + " ", "/"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeReturnPath(tt.in); got != tt.want {
				t.Fatalf("SafeReturnPath of %d bytes is %d bytes, want %d", len(tt.in), len(got), len(tt.want))
			}
		})
	}
}

func FuzzSafeReturnPath(f *testing.F) {
	for _, s := range []string{"/runs/x", "//evil", "/\\evil", "/%2f%2fevil", "https://e/", "/a/../b"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		// Properties: the answer is always a path of this origin, resolves
		// against it without leaving it, is within the bound, and answering
		// again changes nothing.
		got := SafeReturnPath(in)
		if len(got) > MaxReturnPathBytes {
			t.Fatalf("SafeReturnPath of %d bytes is %d bytes, over the bound", len(in), len(got))
		}
		if strings.ContainsFunc(got, func(r rune) bool {
			return unicode.IsControl(r) || r == '\\' || r == '#' || r == unicode.ReplacementChar
		}) {
			t.Fatalf("SafeReturnPath(%q) = %q holds a character it refuses", in, got)
		}
		for _, refused := range []string{"%2f", "%5c", "%00"} {
			if strings.Contains(strings.ToLower(got), refused) {
				t.Fatalf("SafeReturnPath(%q) = %q holds %s", in, got, refused)
			}
		}
		origin := &url.URL{Scheme: "https", Host: "ork.example.test"}
		ref, err := url.Parse(got)
		if err != nil {
			t.Fatalf("SafeReturnPath(%q) = %q does not parse: %v", in, got, err)
		}
		if to := origin.ResolveReference(ref); to.Scheme != origin.Scheme || to.Host != origin.Host || to.User != nil {
			t.Fatalf("SafeReturnPath(%q) = %q resolves to %s", in, got, to)
		}
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
