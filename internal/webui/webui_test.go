package webui_test

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/orkstack/orkstack/internal/webui"
)

// build is a UI build as adapter-static writes one: the SPA fallback page,
// hashed assets under _app/immutable with their precompressed twins, the
// unhashed runtime files, and the example gallery's prerendered files.
func build() fstest.MapFS {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	return fstest.MapFS{
		"index.html":                           file("<!doctype html><title>OrkStack</title>"),
		"index.html.br":                        file("BR(index)"),
		"index.html.gz":                        file("GZ(index)"),
		"_app/immutable/entry/start.abc.js":    file("export {}"),
		"_app/immutable/entry/start.abc.js.br": file("BR(start)"),
		"_app/immutable/entry/start.abc.js.gz": file("GZ(start)"),
		"_app/immutable/assets/0.def.css":      file("body{}"),
		"_app/immutable/assets/face.ghi.woff2": file("wOF2"),
		"_app/version.json":                    file(`{"version":"1"}`),
		"_app/env.js":                          file("export const env={}"),
		"apply/examples.json":                  file("[]"),
		"apply/examples/hello.ork.yaml":        file("name: hello\n"),
		".orkstack-build-hash":                 file("0123\n"),
	}
}

func get(t *testing.T, h http.Handler, method, target string, header map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func body(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const (
	noCache   = "no-cache"
	immutable = "public, max-age=31536000, immutable"
	index     = "<!doctype html><title>OrkStack</title>"
)

func TestHandler_ServesTheBuild(t *testing.T) {
	h := webui.Handler(build())
	cases := []struct {
		name, method, path, acceptEncoding string
		status                             int
		contentType, cacheControl, body    string
		contentEncoding                    string
	}{
		{name: "the root is the app", method: "GET", path: "/", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: index},
		{name: "index.html by name", method: "GET", path: "/index.html", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: index},
		{name: "a client route falls back to the app", method: "GET", path: "/runs/ork-1-2", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: index},
		{name: "a nested client route falls back to the app", method: "GET", path: "/admin/namespaces", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: index},
		{name: "a build directory with no extension is a client route", method: "GET", path: "/apply/examples", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: index},
		{name: "a hashed asset is immutable", method: "GET", path: "/_app/immutable/entry/start.abc.js", status: 200,
			contentType: "text/javascript; charset=utf-8", cacheControl: immutable, body: "export {}"},
		{name: "a stylesheet", method: "GET", path: "/_app/immutable/assets/0.def.css", status: 200,
			contentType: "text/css; charset=utf-8", cacheControl: immutable, body: "body{}"},
		{name: "a font", method: "GET", path: "/_app/immutable/assets/face.ghi.woff2", status: 200,
			contentType: "font/woff2", cacheControl: immutable, body: "wOF2"},
		{name: "gzip when accepted", method: "GET", path: "/_app/immutable/entry/start.abc.js", acceptEncoding: "gzip, deflate", status: 200,
			contentType: "text/javascript; charset=utf-8", cacheControl: immutable, body: "GZ(start)", contentEncoding: "gzip"},
		{name: "brotli before gzip", method: "GET", path: "/_app/immutable/entry/start.abc.js", acceptEncoding: "gzip, br", status: 200,
			contentType: "text/javascript; charset=utf-8", cacheControl: immutable, body: "BR(start)", contentEncoding: "br"},
		{name: "an encoding refused with q=0 is not used", method: "GET", path: "/_app/immutable/entry/start.abc.js", acceptEncoding: "br;q=0, gzip;q=0", status: 200,
			contentType: "text/javascript; charset=utf-8", cacheControl: immutable, body: "export {}"},
		{name: "the fallback page is compressed too", method: "GET", path: "/runs", acceptEncoding: "br", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: "BR(index)", contentEncoding: "br"},
		{name: "a file with no compressed twin is sent as it is", method: "GET", path: "/_app/immutable/assets/0.def.css", acceptEncoding: "br, gzip", status: 200,
			contentType: "text/css; charset=utf-8", cacheControl: immutable, body: "body{}"},
		{name: "the version file is revalidated", method: "GET", path: "/_app/version.json", status: 200,
			contentType: "application/json", cacheControl: noCache, body: `{"version":"1"}`},
		{name: "the gallery", method: "GET", path: "/apply/examples.json", status: 200,
			contentType: "application/json", cacheControl: noCache, body: "[]"},
		{name: "an example", method: "GET", path: "/apply/examples/hello.ork.yaml", status: 200,
			contentType: "text/yaml; charset=utf-8", cacheControl: noCache, body: "name: hello\n"},
		{name: "a HEAD has no body", method: "HEAD", path: "/", status: 200,
			contentType: "text/html; charset=utf-8", cacheControl: noCache, body: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := map[string]string{}
			if tc.acceptEncoding != "" {
				header["Accept-Encoding"] = tc.acceptEncoding
			}
			res := get(t, h, tc.method, tc.path, header)
			if res.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", res.StatusCode, tc.status)
			}
			if got := res.Header.Get("Content-Type"); got != tc.contentType {
				t.Errorf("Content-Type %q, want %q", got, tc.contentType)
			}
			if got := res.Header.Get("Cache-Control"); got != tc.cacheControl {
				t.Errorf("Cache-Control %q, want %q", got, tc.cacheControl)
			}
			if got := res.Header.Get("Content-Encoding"); got != tc.contentEncoding {
				t.Errorf("Content-Encoding %q, want %q", got, tc.contentEncoding)
			}
			if got := body(t, res); got != tc.body {
				t.Errorf("body %q, want %q", got, tc.body)
			}
			if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options %q, want nosniff", got)
			}
		})
	}
}

// A compressible file's response varies with Accept-Encoding whichever
// encoding was chosen, so a shared cache never hands brotli to a client that
// did not ask for it.
func TestHandler_VariesOnAcceptEncoding(t *testing.T) {
	h := webui.Handler(build())
	for _, ae := range []string{"", "gzip", "br"} {
		res := get(t, h, "GET", "/_app/immutable/entry/start.abc.js", map[string]string{"Accept-Encoding": ae})
		if got := res.Header.Get("Vary"); got != "Accept-Encoding" {
			t.Errorf("Accept-Encoding %q: Vary %q, want Accept-Encoding", ae, got)
		}
	}
}

// What is not in the build is never answered with the app: a missing asset
// with the page's HTML and a 200 would be cached as the asset, and a module
// script served as HTML fails with a MIME error that names nothing.
func TestHandler_RefusesWhatIsNotThere(t *testing.T) {
	h := webui.Handler(build())
	for _, path := range []string{
		"/_app/immutable/entry/start.old.js", // an asset of an earlier build
		"/_app/immutable",                    // a directory of the build
		"/_app/nothing",                      // anything under _app
		"/favicon.ico",                       // a file name the build does not hold
		"/apply/examples/nope.ork.yaml",      // an example that does not exist
		"/.orkstack-build-hash",              // a dot file is never served
		"/_app/.hidden",
	} {
		res := get(t, h, "GET", path, nil)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, res.StatusCode)
		}
		if strings.Contains(body(t, res), index) {
			t.Errorf("GET %s answered with the app", path)
		}
	}
}

// The UI is mounted at / beside the API, so every path the API owns — and
// the admin listener's, which a load balancer probing the wrong port must not
// see answered — is a 404 from here, whatever the method, never the app.
func TestHandler_NeverShadowsTheAPI(t *testing.T) {
	h := webui.Handler(build())
	for _, path := range []string{
		"/orkstack.v1.RunService/GetRun",
		"/orkstack.v1.NoSuchService/Method",
		"/orkstack.v2.RunService/GetRun",
		"/v1/triggers/trg-1",
		"/v1/webhooks",
		"/v1/runs/ork-1/awaits/approve",
		"/v1/anything/else",
		"/ws/runs/ork-1",
		"/ws/runs/ork-1/tasks/build/logs",
		"/ws/",
		"/healthz",
		"/healthz/",
		"/readyz",
		"/metrics",
		"/debug/pprof/",
	} {
		for _, method := range []string{"GET", "HEAD", "POST", "DELETE"} {
			res := get(t, h, method, path, nil)
			if res.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s: status %d, want 404", method, path, res.StatusCode)
			}
			if strings.Contains(body(t, res), index) {
				t.Errorf("%s %s answered with the app", method, path)
			}
		}
		if !webui.Reserved(path) {
			t.Errorf("Reserved(%q) is false", path)
		}
	}
	for _, path := range []string{"/", "/runs", "/runs/ork-1", "/orkflows/v1", "/admin/webhooks", "/apply/examples.json", "/_app/version.json"} {
		if webui.Reserved(path) {
			t.Errorf("Reserved(%q) is true; the UI owns it", path)
		}
	}
}

func TestHandler_OnlyReads(t *testing.T) {
	h := webui.Handler(build())
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		res := get(t, h, method, "/runs", nil)
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s /runs: status %d, want 405", method, res.StatusCode)
		}
		if got := res.Header.Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s /runs: Allow %q, want GET, HEAD", method, got)
		}
	}
}

// A revalidated file carries a strong validator of the bytes it sends, so a
// browser's no-cache revalidation of the app costs a 304, not the page.
func TestHandler_RevalidatesWithAnETag(t *testing.T) {
	h := webui.Handler(build())
	first := get(t, h, "GET", "/runs", nil)
	etag := first.Header.Get("ETag")
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) || len(etag) < 10 {
		t.Fatalf("ETag %q is not a strong validator", etag)
	}
	if again := get(t, h, "GET", "/", nil).Header.Get("ETag"); again != etag {
		t.Errorf("the same bytes have two ETags: %q and %q", etag, again)
	}
	if br := get(t, h, "GET", "/", map[string]string{"Accept-Encoding": "br"}).Header.Get("ETag"); br == etag {
		t.Errorf("the brotli bytes share the plain bytes' ETag %q", etag)
	}
	res := get(t, h, "GET", "/admin", map[string]string{"If-None-Match": etag})
	if res.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match with the current ETag: status %d, want 304", res.StatusCode)
	}
}

// The app's page may not be framed by another origin: it carries the
// operator's destructive controls (cancel, delete, promote).
func TestHandler_PageRefusesFraming(t *testing.T) {
	h := webui.Handler(build())
	res := get(t, h, "GET", "/runs", nil)
	if got := res.Header.Get("Content-Security-Policy"); got != "frame-ancestors 'none'" {
		t.Errorf("Content-Security-Policy %q, want frame-ancestors 'none'", got)
	}
	if got := res.Header.Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options %q, want DENY", got)
	}
	if got := res.Header.Get("Referrer-Policy"); got != "same-origin" {
		t.Errorf("Referrer-Policy %q, want same-origin", got)
	}
}

func TestContentType(t *testing.T) {
	for name, want := range map[string]string{
		"a.html":        "text/html; charset=utf-8",
		"a.js":          "text/javascript; charset=utf-8",
		"a.mjs":         "text/javascript; charset=utf-8",
		"a.css":         "text/css; charset=utf-8",
		"a.json":        "application/json",
		"a.map":         "application/json",
		"a.svg":         "image/svg+xml",
		"a.png":         "image/png",
		"a.ico":         "image/x-icon",
		"a.webp":        "image/webp",
		"a.woff2":       "font/woff2",
		"a.woff":        "font/woff",
		"a.txt":         "text/plain; charset=utf-8",
		"a.ork.yaml":    "text/yaml; charset=utf-8",
		"a.yml":         "text/yaml; charset=utf-8",
		"a.webmanifest": "application/manifest+json",
		"a.wasm":        "application/wasm",
		"a.unknown":     "application/octet-stream",
		"noextension":   "application/octet-stream",
	} {
		if got := webui.ContentType(name); got != want {
			t.Errorf("ContentType(%q) = %q, want %q", name, got, want)
		}
	}
}

// A binary built without the UI still serves something that says so, and
// says how to build it.
func TestPlaceholder_SaysHowToBuildTheUI(t *testing.T) {
	h := webui.Handler(webui.Placeholder())
	res := get(t, h, "GET", "/runs/ork-1", nil)
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("status %d, Content-Type %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	page := body(t, res)
	for _, want := range []string{"make ui", "ORK_UI_DIR", "make up"} {
		if !strings.Contains(page, want) {
			t.Errorf("the placeholder does not mention %q", want)
		}
	}
}

// Embedded reports a build exactly when this package's dist directory held
// one at compile time: the same file decides both.
func TestEmbedded_IsTheBuildInDist(t *testing.T) {
	_, statErr := os.Stat(filepath.Join("dist", "index.html"))
	build, ok := webui.Embedded()
	if ok != (statErr == nil) {
		t.Fatalf("Embedded() = %v, but dist/index.html exists: %v", ok, statErr == nil)
	}
	if ok {
		if _, err := fs.Stat(build, "index.html"); err != nil {
			t.Errorf("the embedded build has no index.html: %v", err)
		}
	}
}

func TestDir(t *testing.T) {
	t.Run("a build directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("from disk"), 0o644); err != nil {
			t.Fatal(err)
		}
		build, err := webui.Dir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := body(t, get(t, webui.Handler(build), "GET", "/runs", nil)); got != "from disk" {
			t.Errorf("body %q, want the directory's index.html", got)
		}
	})
	t.Run("a directory with no index.html is refused", func(t *testing.T) {
		if _, err := webui.Dir(t.TempDir()); err == nil || !strings.Contains(err.Error(), "index.html") {
			t.Errorf("err = %v, want one naming index.html", err)
		}
	})
	t.Run("a missing directory is refused", func(t *testing.T) {
		if _, err := webui.Dir(filepath.Join(t.TempDir(), "absent")); err == nil {
			t.Error("a missing directory was accepted")
		}
	})
	t.Run("a link out of the directory is not followed", func(t *testing.T) {
		outside := t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o644); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("app"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "leak.txt")); err != nil {
			t.Fatal(err)
		}
		build, err := webui.Dir(dir)
		if err != nil {
			t.Fatal(err)
		}
		res := get(t, webui.Handler(build), "GET", "/leak.txt", nil)
		if got := body(t, res); res.StatusCode == http.StatusOK || strings.Contains(got, "secret") {
			t.Errorf("GET /leak.txt: status %d, body %q: a link out of the UI directory was followed", res.StatusCode, got)
		}
	})
}

// A reserved path spelled with encoded dot segments is still reserved: the
// handler judges the path it would serve, not the spelling.
func TestHandler_ReservedAfterCleaning(t *testing.T) {
	h := webui.Handler(build())
	for _, target := range []string{
		"/x/..%2forkstack.v1.RunService/GetRun",
		"/%2e%2e/v1/foo",
		"/%2fv1/foo",
		"/runs/../healthz",
	} {
		res := get(t, h, "GET", target, nil)
		if res.StatusCode != http.StatusNotFound || strings.Contains(body(t, res), index) {
			t.Errorf("GET %s = %d: a reserved path answered with the app", target, res.StatusCode)
		}
	}
}

// Every response carries its length, compressed or not, and a HEAD says it
// too, so nothing goes out chunked when the size is known.
func TestHandler_SendsContentLength(t *testing.T) {
	h := webui.Handler(build())
	for _, tc := range []struct{ method, path, ae, want string }{
		{"GET", "/runs", "", fmt.Sprint(len(index))},
		{"GET", "/runs", "br", fmt.Sprint(len("BR(index)"))},
		{"HEAD", "/_app/immutable/entry/start.abc.js", "gzip", fmt.Sprint(len("GZ(start)"))},
	} {
		res := get(t, h, tc.method, tc.path, map[string]string{"Accept-Encoding": tc.ae})
		if got := res.Header.Get("Content-Length"); got != tc.want {
			t.Errorf("%s %s (Accept-Encoding %q): Content-Length %q, want %s", tc.method, tc.path, tc.ae, got, tc.want)
		}
	}
}

// An error from the UI is never cached: a CDN that cached the 404 of a
// hashed asset during a rollout would keep serving it after the asset exists.
func TestHandler_ErrorsAreNotCached(t *testing.T) {
	h := webui.Handler(build())
	for _, tc := range []struct{ method, path string }{
		{"GET", "/_app/immutable/entry/start.old.js"},
		{"GET", "/orkstack.v1.RunService/GetRun"},
		{"POST", "/runs"},
	} {
		if got := get(t, h, tc.method, tc.path, nil).Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s %s: Cache-Control %q, want no-store", tc.method, tc.path, got)
		}
	}
}

// Framing protection follows the type served, whatever the case of the name.
func TestHandler_EveryPageRefusesFraming(t *testing.T) {
	h := webui.Handler(fstest.MapFS{
		"index.html": {Data: []byte(index)},
		"OTHER.HTML": {Data: []byte("<p>other</p>")},
	})
	res := get(t, h, "GET", "/OTHER.HTML", nil)
	if res.Header.Get("Content-Type") != "text/html; charset=utf-8" || res.Header.Get("X-Frame-Options") != "DENY" {
		t.Errorf("GET /OTHER.HTML: Content-Type %q, X-Frame-Options %q", res.Header.Get("Content-Type"), res.Header.Get("X-Frame-Options"))
	}
}

// ORK_UI_DIR follows the directory, not the inode it had at boot: a UI
// rebuilt in place (`bun run build` deletes and rewrites ui/build) is served
// without restarting the engine.
func TestDir_FollowsARebuild(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "build")
	write := func(page string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(page), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("first build")
	build, err := webui.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := webui.Handler(build)
	if got := body(t, get(t, h, "GET", "/runs", nil)); got != "first build" {
		t.Fatalf("body %q, want the first build", got)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	write("second build")
	if res := get(t, h, "GET", "/runs", nil); res.StatusCode != http.StatusOK || body(t, res) != "second build" {
		t.Errorf("after a rebuild: status %d, want the second build", res.StatusCode)
	}
}
