package artifacts

import (
	"context"
	"errors"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// ArtifactRef is the reference every store returns. It is an alias of
// plugin.ArtifactRef so a reference produced here is the same type the plugin
// contract and the Orker seam already exchange (uri, size_bytes, sha256,
// content_type, plus name and metadata).
type ArtifactRef = plugin.ArtifactRef

// PutOptions carries optional attributes for a Put. The logical name is the
// explicit Put argument, never an option.
type PutOptions struct {
	// ContentType overrides sniffing. When empty the store detects the type
	// from the first bytes of the stream (net/http.DetectContentType).
	ContentType string
	// Metadata is optional key/value data stored alongside the object and
	// returned by Stat.
	Metadata map[string]string
}

// Store persists artifacts and hands back references. Implementations are
// content-addressed: an object's canonical key is "<name>/<sha256>", so the
// same bytes under the same name always land on the same URI.
//
// A new backend implements Store and calls Register at init time; nothing else
// in the codebase needs to change.
type Store interface {
	// Put streams r into the store under the logical name and returns the
	// stored reference. The reader need not be seekable and its length need not
	// be known in advance.
	Put(ctx context.Context, name string, r io.Reader, opts PutOptions) (ArtifactRef, error)
	// Get opens the artifact at uri. The caller closes the reader.
	// A missing artifact returns an error matching ErrNotFound.
	Get(ctx context.Context, uri string) (io.ReadCloser, error)
	// Stat returns the reference for uri without transferring the bytes.
	// A missing artifact returns an error matching ErrNotFound.
	Stat(ctx context.Context, uri string) (ArtifactRef, error)
	// List returns references whose logical name starts with prefix, ordered by
	// URI. An empty prefix lists everything in the store.
	List(ctx context.Context, prefix string) ([]ArtifactRef, error)
	// Presign returns a URL granting time-limited read access to uri.
	// Backends without signed URLs return an error matching ErrUnsupported.
	Presign(ctx context.Context, uri string, ttl time.Duration) (string, error)
	// Delete removes the artifact at uri. Delete is idempotent: removing an
	// artifact that is already gone returns nil.
	Delete(ctx context.Context, uri string) error
}

// Sentinel errors. Providers wrap these with %w, so callers use errors.Is.
var (
	// ErrNotFound reports a missing artifact.
	ErrNotFound = errors.New("artifact not found")
	// ErrInvalidURI reports a URI this store cannot address.
	ErrInvalidURI = errors.New("invalid artifact URI")
	// ErrInvalidName reports a logical name that is unsafe or malformed.
	ErrInvalidName = errors.New("invalid artifact name")
	// ErrUnsupported reports an operation the backend cannot perform.
	ErrUnsupported = errors.New("operation unsupported by artifact backend")
	// ErrUnknownBackend reports an unregistered ORK_ARTIFACT_BACKEND value.
	ErrUnknownBackend = errors.New("unknown artifact backend")
)

// digestPattern matches the lowercase hex sha256 that terminates every key.
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// cleanName validates a caller-supplied logical name and returns it in
// canonical slash form. Names are relative, forward-slash separated, and may
// not escape the store root.
func cleanName(name string) (string, error) {
	if name == "" {
		return "", wrapName(name, "must not be empty")
	}
	if strings.ContainsRune(name, '\x00') {
		return "", wrapName(name, "contains a NUL byte")
	}
	if strings.ContainsRune(name, '\\') {
		return "", wrapName(name, "must use '/' separators")
	}
	if strings.HasPrefix(name, "/") {
		return "", wrapName(name, "must be relative")
	}
	if strings.HasSuffix(name, "/") {
		return "", wrapName(name, "must not end with '/'")
	}
	if strings.Contains(name, "//") {
		return "", wrapName(name, "must not contain empty path segments")
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "." || seg == ".." {
			return "", wrapName(name, "must not contain '.' or '..' segments")
		}
	}
	if !filepath.IsLocal(filepath.FromSlash(name)) {
		// Catches platform-specific escapes (volume names, reserved names).
		return "", wrapName(name, "must be a local relative path")
	}
	cleaned := path.Clean(name)
	if cleaned != name {
		return "", wrapName(name, "must already be in canonical form")
	}
	return cleaned, nil
}

func wrapName(name, why string) error {
	return &nameError{name: name, why: why}
}

type nameError struct {
	name string
	why  string
}

func (e *nameError) Error() string {
	return "artifact name " + quote(e.name) + " " + e.why
}

func (e *nameError) Unwrap() error { return ErrInvalidName }

func quote(s string) string {
	return "\"" + s + "\""
}

// objectKey builds the canonical content-addressed key for a name and digest.
func objectKey(name, sha string) string { return name + "/" + sha }

// splitKey reverses objectKey, returning the logical name and digest.
func splitKey(key string) (name, sha string, ok bool) {
	idx := strings.LastIndex(key, "/")
	if idx <= 0 {
		return "", "", false
	}
	name, sha = key[:idx], key[idx+1:]
	if !digestPattern.MatchString(sha) {
		return "", "", false
	}
	return name, sha, true
}
