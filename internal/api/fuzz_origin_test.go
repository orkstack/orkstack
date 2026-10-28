package api

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// FuzzOriginAllowed: the socket's early Origin check is the rule the
// WebSocket library applies when it accepts the upgrade, applied sooner. The
// library, handed the same request and patterns, is the oracle: the early
// check must not turn away an origin the library would let in, and the
// library's own refusal of a request is final, so the early check never lets
// in an origin it would refuse.
func FuzzOriginAllowed(f *testing.F) {
	for _, seed := range [][4]string{
		{"", "ork.example", "", ""},
		{"https://ork.example", "ork.example", "", ""},
		{"https://ORK.example", "ork.example", "", ""},
		{"https://evil.example", "ork.example", "", ""},
		{"https://app.example.com", "ork.example", "*.example.com", ""},
		{"http://localhost:5173", "ork.example", "localhost:5173", ""},
		{"http://localhost:5173", "ork.example", "http://localhost:5173", ""},
		{"https://localhost:5173", "ork.example", "http://localhost:5173", ""},
		{"null", "ork.example", "", ""},
		{"https://", "ork.example", "", ""},
		{"https://app.example", "ork.example", "[", "app.example"},
		{"https://app.example", "ork.example", " app.example ", ""},
		{"https://app.example", "ork.example", "", ""},
		{"https://s.example", "ſ.example", "", ""},
		{"https://ſ.example", "s.example", "", ""},
		{"https://K.example", "k.example", "", ""},
		{"%zz", "ork.example", "*", ""},
		{"https://a.example", "ork.example", "*", ""},
		{"https://a.example/path?x", "a.example", "", ""},
		{"https://user@a.example", "a.example", "", ""},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3])
	}
	// The library logs a pattern it cannot parse; a fuzz run would drown in it.
	log.SetOutput(io.Discard)
	f.Cleanup(func() { log.SetOutput(os.Stderr) })
	f.Fuzz(func(t *testing.T, origin, host, first, second string) {
		patterns := originPatterns([]string{first, second})
		header := http.Header{}
		header.Set("Connection", "Upgrade")
		header.Set("Upgrade", "websocket")
		header.Set("Sec-WebSocket-Version", "13")
		header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		if origin != "" {
			header["Origin"] = []string{origin}
		}
		request := &http.Request{
			Method: "GET", URL: &url.URL{Path: "/ws/runs/x"}, Host: host, Header: header,
			Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		}
		got := originAllowed(request, patterns)

		_, err := websocket.Accept(httptest.NewRecorder(), request, &websocket.AcceptOptions{OriginPatterns: patterns})
		if err == nil {
			t.Fatalf("Accept upgraded a recorder")
		}
		switch {
		case strings.Contains(err.Error(), "Hijacker"):
			// The library let the origin through and failed only for want of a socket.
			if !got {
				t.Fatalf("originAllowed(%q on %q, %q) refused an origin the library accepts", origin, host, patterns)
			}
		case strings.Contains(err.Error(), "Origin"), strings.Contains(err.Error(), "Forbidden"):
			if got && origin != "" {
				t.Fatalf("originAllowed(%q on %q, %q) accepted an origin the library refuses: %v", origin, host, patterns, err)
			}
		default:
			t.Fatalf("Accept failed for a reason that is not the origin or the recorder: %v", err)
		}
	})
}
