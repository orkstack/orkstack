package webui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"testing/fstest"
)

// FuzzAcceptsEncoding: a client's Accept-Encoding header is input OrkStack
// does not control. Whatever it says, the parser does not panic, and it
// accepts a coding only when the header names it.
func FuzzAcceptsEncoding(f *testing.F) {
	for _, seed := range []string{
		"", "gzip", "br", "gzip, deflate, br", "br;q=0, gzip;q=0", "gzip; q=0.5", "GZIP", "*",
		"gzip;q=abc", "identity", ",,,", "br;q=1;level=2", "gzip;q=0.000", "br ; q = 0.1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, header string) {
		for _, coding := range []string{"br", "gzip"} {
			if acceptsEncoding(header, coding) && !strings.Contains(strings.ToLower(header), coding) {
				t.Fatalf("acceptsEncoding(%q, %q) is true, but the header does not name it", header, coding)
			}
		}
	})
}

// FuzzHandlerPath: the request path is input OrkStack does not control.
// Whatever it is, the handler answers 200 or 404, never serves a dot file,
// answers every path that is reserved once cleaned 404, and answers 200 only
// with a file of the build or the app's page.
func FuzzHandlerPath(f *testing.F) {
	const (
		page   = "<!doctype html>app"
		asset  = "export {}"
		secret = "SECRET"
	)
	build := fstest.MapFS{
		"index.html":                 {Data: []byte(page)},
		"_app/immutable/a.js":        {Data: []byte(asset)},
		".orkstack-build-hash":       {Data: []byte(secret)},
		"_app/.hidden":               {Data: []byte(secret)},
		"apply/examples/a.ork.yaml":  {Data: []byte(asset)},
		"apply/.hidden/b.ork.yaml":   {Data: []byte(secret)},
		"apply/examples/.c.ork.yaml": {Data: []byte(secret)},
	}
	for _, seed := range []string{
		"/", "/runs/ork-1", "/_app/immutable/a.js", "/.orkstack-build-hash", "/../.orkstack-build-hash",
		"/apply/./.hidden/b.ork.yaml", "/orkstack.v1.RunService/GetRun", "//_app/.hidden", "/%2e/x",
		"/apply/examples/.c.ork.yaml", "/healthz", "", "/ws", "/a\x00b",
	} {
		f.Add(seed)
	}
	h := Handler(build)
	f.Fuzz(func(t *testing.T, p string) {
		req := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: p}, Header: http.Header{}}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		body := rec.Body.String()
		switch rec.Code {
		case http.StatusOK:
			if body != page && body != asset {
				t.Fatalf("GET %q: 200 with %q, neither the app nor a file of the build", p, body)
			}
		case http.StatusNotFound:
		default:
			t.Fatalf("GET %q: status %d, want 200 or 404", p, rec.Code)
		}
		if strings.Contains(body, secret) {
			t.Fatalf("GET %q served a dot file", p)
		}
		if Reserved(path.Clean("/"+p)) && rec.Code != http.StatusNotFound {
			t.Fatalf("GET %q is reserved but answered %d", p, rec.Code)
		}
	})
}
