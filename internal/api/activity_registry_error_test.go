package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

// TestInspectActivity_ARegistrysOwnWordsReachNoTerminal: what a registry
// writes into a resolution error — the body of a manifest or config read it
// refused, or a Docker-Content-Digest that does not match — reaches
// InspectActivity's error escaped, shown rather than dropped, and a long body
// is cut, so a client that prints the error obeys none of it.
func TestInspectActivity_ARegistrysOwnWordsReachNoTerminal(t *testing.T) {
	long := strings.Repeat("x", 4096)
	for _, tc := range []struct {
		name string
		// fail answers one request in the registry's place, or declines.
		fail func(w http.ResponseWriter, r *http.Request) bool
		want []string
	}{
		{"a refused manifest", func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, "/blobs/") {
				return false
			}
			w.WriteHeader(http.StatusTeapot)
			io.WriteString(w, "\x1b]0;pwned\x07 \u009b31m \u202eevil\n"+long)
			return true
		}, []string{`\u001b]0;pwned\u0007`, `\u009b31m`, `\u202eevil`, `\u000a`}},
		{"a refused config blob", func(w http.ResponseWriter, r *http.Request) bool {
			if !strings.Contains(r.URL.Path, "/blobs/") {
				return false
			}
			w.WriteHeader(http.StatusTeapot)
			io.WriteString(w, "\x1b]0;pwned\x07 \u2028"+long)
			return true
		}, []string{`\u001b]0;pwned\u0007`, `\u2028`}},
		{"a digest header that does not match", func(w http.ResponseWriter, r *http.Request) bool {
			// A header cannot carry a C0 control over HTTP/1.x; a C1
			// control and a bidirectional override it can.
			if !strings.HasSuffix(r.URL.Path, "/manifests/dev") {
				return false
			}
			w.Header().Set("Docker-Content-Digest", "sha256:\u009b31m\u202eevil")
			w.Header().Set("Content-Type", dsl.MediaTypeOCIIndex)
			io.WriteString(w, "{}")
			return true
		}, []string{`\u009b31m\u202eevil`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := newMovableRegistry(t)
			registry.setImage(t, map[string]string{dsl.InterfaceLabel: fileLabel(t)}, "linux/amd64")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !tc.fail(w, r) {
					registry.ServeHTTP(w, r)
				}
			}))
			t.Cleanup(server.Close)
			svc := api.NewService(api.Config{Resolver: dsl.NewResolver("")})
			_, err := inspect(svc, strings.TrimPrefix(server.URL, "http://")+"/orkstack/activity-file:dev", false)
			if err == nil {
				t.Fatal("InspectActivity resolved an image its registry refused")
			}
			message := err.Error()
			for _, r := range message {
				if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == 0x2028 || r == 0x2029 {
					t.Fatalf("the error holds %U raw:\n%q", r, message)
				}
			}
			for _, want := range tc.want {
				if !strings.Contains(message, want) {
					t.Fatalf("the error shows no %s:\n%s", want, message)
				}
			}
			if strings.Contains(message, long[:600]) {
				t.Fatalf("the error holds the registry's body uncut (%d bytes)", len(message))
			}
		})
	}
}
