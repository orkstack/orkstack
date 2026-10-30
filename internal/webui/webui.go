// Package webui is OrkStack's web UI as the engine serves it: the static
// build of ui/ (SvelteKit with adapter-static, a single-page app), embedded
// into the ork binary, and the handler that serves it at / on the API
// listener, beside Connect, the WebSocket bridge and the plain-HTTP routes.
// One process and one origin: the browser calls the API and opens its
// sockets where the page came from, so nothing proxies and nothing needs
// CORS.
//
// `make ui` (and the release and source builds) writes the UI's build into
// dist/ before ork is compiled. A binary compiled without it — `go build
// ./cmd/ork`, `go test`, `make check` — embeds dist/.gitkeep alone and
// serves Placeholder instead, a page that says how to build the UI.
package webui

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

//go:embed all:dist
var dist embed.FS

//go:embed placeholder
var placeholder embed.FS

// Embedded returns the UI build compiled into this binary, and false when
// it was compiled without one (dist/ held no index.html).
func Embedded() (fs.FS, bool) {
	build, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, false
	}
	if _, err := fs.Stat(build, "index.html"); err != nil {
		return nil, false
	}
	return build, true
}

// Placeholder is what a binary compiled without the UI serves in its place:
// one page, saying how to build the UI into ork or point ork at a build.
func Placeholder() fs.FS {
	build, err := fs.Sub(placeholder, "placeholder")
	if err != nil {
		// The directory is embedded by name above; Sub fails only on an
		// invalid name, which "placeholder" is not.
		panic(err)
	}
	return build
}

// Dir returns the UI build in dir (ORK_UI_DIR): `bun run build`'s ui/build,
// served in place of the embedded one. It must hold index.html — a directory
// that is not a UI build is refused at boot rather than answered with 404s.
//
// Every file is opened through an os.Root of dir, so a link inside it that
// leads out of it is never followed. The Root is opened for each file, never
// held: `bun run build` deletes and rewrites its output directory, and a
// Root held from boot would keep naming the deleted one, so every page would
// fail until the engine restarted.
func Dir(dir string) (fs.FS, error) {
	build := rootDir(dir)
	if _, err := fs.Stat(build, indexFile); err != nil {
		return nil, fmt.Errorf("%s is not a UI build: it holds no index.html "+
			"(build one with `cd ui && bun run build`, which writes ui/build): %w", dir, err)
	}
	return build, nil
}

// rootDir is an fs.FS of a directory whose every Open goes through a fresh
// os.Root of it (Dir says why). A file opened through a Root stays open
// after the Root is closed.
type rootDir string

func (d rootDir) Open(name string) (fs.File, error) {
	root, err := os.OpenRoot(string(d))
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	defer func() { _ = root.Close() }()
	return root.FS().Open(name)
}

// Reserved reports whether urlPath belongs to something other than the UI,
// so the UI answers it 404 and never with the app: the Connect services
// (/orkstack.v1.*), the plain-HTTP routes (/v1/triggers, /v1/webhooks,
// /v1/runs/…/awaits), the WebSocket bridge (/ws/), and the admin listener's
// endpoints (/healthz, /readyz, /metrics, /debug/), which live on another
// port and must not look answered on this one to a probe aimed at the wrong
// port. internal/api's mux test holds every route it mounts to this.
func Reserved(urlPath string) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(urlPath, "/"), "/")
	switch first {
	case "v1", "ws", "healthz", "readyz", "metrics", "debug":
		return true
	}
	return strings.HasPrefix(first, "orkstack.")
}

const (
	cacheImmutable  = "public, max-age=31536000, immutable"
	cacheRevalidate = "no-cache"
	// immutableDir holds the build's content-hashed files: a new build names
	// a changed file anew, so a browser may keep each one for good.
	immutableDir = "_app/immutable/"
	appDir       = "_app"
	indexFile    = "index.html"
)

// contentTypes names every type the build holds, so a response's type never
// depends on the host's MIME database (Go consults /etc/mime.types, which a
// distroless image does not have and a developer's machine may spell
// differently).
var contentTypes = map[string]string{
	".html":        "text/html; charset=utf-8",
	".js":          "text/javascript; charset=utf-8",
	".mjs":         "text/javascript; charset=utf-8",
	".css":         "text/css; charset=utf-8",
	".json":        "application/json",
	".map":         "application/json",
	".svg":         "image/svg+xml",
	".png":         "image/png",
	".ico":         "image/x-icon",
	".webp":        "image/webp",
	".woff2":       "font/woff2",
	".woff":        "font/woff",
	".txt":         "text/plain; charset=utf-8",
	".yaml":        "text/yaml; charset=utf-8",
	".yml":         "text/yaml; charset=utf-8",
	".webmanifest": "application/manifest+json",
	".wasm":        "application/wasm",
}

// ContentType is the Content-Type the UI is served with for a file named
// name: application/octet-stream for an extension the build never holds.
func ContentType(name string) string {
	if t, ok := contentTypes[strings.ToLower(path.Ext(name))]; ok {
		return t
	}
	return "application/octet-stream"
}

// encodings are the precompressed twins the build writes beside a file
// (adapter-static's precompress), in the order they are preferred.
var encodings = []struct{ coding, suffix string }{
	{"br", ".br"},
	{"gzip", ".gz"},
}

// Handler serves build, a UI build, as the single-page app it is:
//
//   - a file of the build is served as it is, with its content type and,
//     when the client accepts one, its precompressed twin; a file under
//     _app/immutable/ (content-hashed) may be cached for a year, everything
//     else is revalidated by its ETag on every use;
//   - any other path is a route of the app and gets index.html, unless it
//     names a file (its last segment has an extension), lies under _app/,
//     or is Reserved: those are 404, so a missing asset is never answered
//     with HTML and a 200 a browser would cache as the asset;
//   - a dot file is never served;
//   - only GET and HEAD are answered; anything else outside the reserved
//     paths is 405.
func Handler(build fs.FS) http.Handler {
	return &handler{build: build}
}

type handler struct {
	build fs.FS
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Set("X-Content-Type-Options", "nosniff")
	// An error is never cached: a cache that kept the 404 of a hashed asset
	// asked for during a rollout would go on serving it once the asset is
	// there. serve replaces this for a file it answers.
	header.Set("Cache-Control", "no-store")
	// The path is judged as it will be served: cleaned once, here, so an
	// encoded dot segment (/x/..%2fv1/…) cannot spell a reserved path the
	// reserved check does not see.
	urlPath := path.Clean("/" + r.URL.Path)
	if Reserved(urlPath) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		header.Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	name, ok := h.resolve(urlPath)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.serve(w, r, name)
}

// resolve maps a cleaned request path to the build file that answers it.
func (h *handler) resolve(urlPath string) (string, bool) {
	name := strings.TrimPrefix(urlPath, "/")
	if name == "" {
		return indexFile, true
	}
	if !fs.ValidPath(name) {
		return "", false
	}
	for _, segment := range strings.Split(name, "/") {
		if strings.HasPrefix(segment, ".") {
			return "", false
		}
	}
	if info, err := fs.Stat(h.build, name); err == nil && info.Mode().IsRegular() {
		return name, true
	}
	if name == appDir || strings.HasPrefix(name, appDir+"/") || strings.Contains(path.Base(name), ".") {
		return "", false
	}
	return indexFile, true
}

func (h *handler) serve(w http.ResponseWriter, r *http.Request, name string) {
	header := w.Header()
	revalidate := !strings.HasPrefix(name, immutableDir)
	served, coding := name, ""
	twins := false
	for _, e := range encodings {
		if _, err := fs.Stat(h.build, name+e.suffix); err != nil {
			continue
		}
		twins = true
		if coding == "" && acceptsEncoding(r.Header.Get("Accept-Encoding"), e.coding) {
			served, coding = name+e.suffix, e.coding
		}
	}
	data, err := fs.ReadFile(h.build, served)
	if err != nil {
		http.Error(w, "read the UI build: "+err.Error(), http.StatusInternalServerError)
		return
	}
	contentType := ContentType(name)
	header.Set("Content-Type", contentType)
	// The length is known, so it is sent; ServeContent leaves it unset when
	// Content-Encoding is, and replaces it for a range or a 304 itself.
	header.Set("Content-Length", strconv.Itoa(len(data)))
	if twins {
		header.Set("Vary", "Accept-Encoding")
	}
	if coding != "" {
		header.Set("Content-Encoding", coding)
	}
	if revalidate {
		header.Set("Cache-Control", cacheRevalidate)
		sum := sha256.Sum256(data)
		header.Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
	} else {
		header.Set("Cache-Control", cacheImmutable)
	}
	if contentType == contentTypes[".html"] {
		// The app carries the operator's destructive controls (cancel,
		// delete, promote), so no other origin may frame it.
		header.Set("Content-Security-Policy", "frame-ancestors 'none'")
		header.Set("X-Frame-Options", "DENY")
		header.Set("Referrer-Policy", "same-origin")
	}
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

// acceptsEncoding reports whether an Accept-Encoding header accepts coding:
// it names coding with a q-value above zero (no q-value is q=1). A
// wildcard is not taken as naming it, which RFC 9110 permits: identity is
// always acceptable, and only the encodings a client names are sent.
func acceptsEncoding(header, coding string) bool {
	for _, element := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(element, ";")
		if !strings.EqualFold(strings.TrimSpace(name), coding) {
			continue
		}
		q := 1.0
		for _, param := range strings.Split(params, ";") {
			key, value, ok := strings.Cut(param, "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				return false
			}
			q = parsed
		}
		return q > 0
	}
	return false
}
