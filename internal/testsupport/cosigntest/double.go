package cosigntest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Double is an httptest registry that serves manifests and blobs from memory,
// the way a registry does. It is the unit suites' stand-in for a real
// registry: it answers the signature object's GETs, can refuse or stall any
// of them, and logs every request so a test can assert order and "never
// asked".
type Double struct {
	Server *httptest.Server
	// Host is "127.0.0.1:<port>", which the resolver treats as plain HTTP.
	Host string

	mu        sync.Mutex
	manifests map[string]manifestEntry // "<repo>/manifests/<ref>"
	blobs     map[string][]byte        // "<repo>/blobs/<digest>"
	// Status, when set for a path suffix ("manifests/" or "blobs/"), answers
	// that status instead of the content.
	status map[string]int
	// Block, when non-nil, is waited on before answering a request whose path
	// contains BlockPath (the test closes it, or cancels the caller).
	Block     chan struct{}
	BlockPath string
	// OnRequest, when set, is called with "GET <path>" for every request,
	// before it is answered — the shared event log a dispatcher test reads.
	OnRequest func(event string)
	requests  []string
}

type manifestEntry struct {
	body      []byte
	mediaType string
}

// NewDouble starts an empty registry double, closed at test end.
func NewDouble(t testing.TB) *Double {
	t.Helper()
	d := &Double{
		manifests: map[string]manifestEntry{},
		blobs:     map[string][]byte{},
		status:    map[string]int{},
	}
	d.Server = httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(d.Server.Close)
	d.Host = hostOf(d.Server.URL)
	return d
}

// PutManifest serves body at <repo>/manifests/<ref> (and at its digest).
func (d *Double) PutManifest(repo, ref, mediaType string, body []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.manifests[repo+"/manifests/"+ref] = manifestEntry{body: body, mediaType: mediaType}
	d.manifests[repo+"/manifests/"+Digest(body)] = manifestEntry{body: body, mediaType: mediaType}
}

// PutBlob serves blob at <repo>/blobs/<digest>.
func (d *Double) PutBlob(repo string, blob []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blobs[repo+"/blobs/"+Digest(blob)] = blob
}

// PutBlobAt serves blob under an explicit digest — the tamper case.
func (d *Double) PutBlobAt(repo, digest string, blob []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blobs[repo+"/blobs/"+digest] = blob
}

// Sign serves a signature object for repo@digest holding layers.
func (d *Double) Sign(repo, digest string, layers ...Layer) {
	for _, l := range layers {
		d.PutBlob(repo, l.Payload)
	}
	d.PutBlob(repo, configBlob)
	d.PutManifest(repo, SignatureTag(digest), OCIManifestType, Manifest(layers))
}

// Unsign removes repo@digest's signature object: its tag answers 404.
func (d *Double) Unsign(repo, digest string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.manifests, repo+"/manifests/"+SignatureTag(digest))
}

// SetStatus makes every request whose path contains fragment answer status.
// A zero status removes the override.
func (d *Double) SetStatus(fragment string, status int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if status == 0 {
		delete(d.status, fragment)
		return
	}
	d.status[fragment] = status
}

// Requests returns every "GET <path>" served so far.
func (d *Double) Requests() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.requests...)
}

// CountSuffix counts requests whose path ends in suffix.
func (d *Double) CountSuffix(suffix string) int {
	n := 0
	for _, r := range d.Requests() {
		if strings.HasSuffix(r, suffix) {
			n++
		}
	}
	return n
}

func (d *Double) serve(w http.ResponseWriter, r *http.Request) {
	event := r.Method + " " + r.URL.Path
	d.mu.Lock()
	d.requests = append(d.requests, event)
	hook := d.OnRequest
	block, blockPath := d.Block, d.BlockPath
	status := 0
	for fragment, s := range d.status {
		if strings.Contains(r.URL.Path, fragment) {
			status = s
		}
	}
	d.mu.Unlock()
	if hook != nil {
		hook(event)
	}
	if block != nil && strings.Contains(r.URL.Path, blockPath) {
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
	}
	if status != 0 {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(http.StatusText(status)))
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v2/")
	d.mu.Lock()
	manifest, isManifest := d.manifests[key]
	blob, isBlob := d.blobs[key]
	d.mu.Unlock()
	switch {
	case strings.Contains(key, "/manifests/") && isManifest:
		w.Header().Set("Content-Type", manifest.mediaType)
		w.Header().Set("Docker-Content-Digest", Digest(manifest.body))
		_, _ = w.Write(manifest.body)
	case strings.Contains(key, "/blobs/") && isBlob:
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(blob)
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))
	}
}
