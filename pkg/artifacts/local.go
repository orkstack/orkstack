package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Local store layout, rooted at the configured directory:
//
//	<root>/objects/<encoded-name>/<sha256>            artifact bytes
//	<root>/.metadata/<encoded-name>/<sha256>.meta.json attributes
//	<root>/.tmp/<random>                              in-flight writes
//
// Logical names never become paths directly. A reversible, chunked base64url
// encoding preserves every name while keeping provider bookkeeping outside the
// object namespace. The canonical URI addresses the generated object path.
const (
	localTempDir     = ".tmp"
	localObjectsDir  = objectNamespace
	localMetadataDir = ".metadata"
	localMetaSuffix  = ".meta.json"
	localDirPerm     = 0o755
	localFilePerm    = 0o644
)

// LocalStore is the filesystem-backed Store used for development and
// single-host deployments.
type LocalStore struct {
	// dir is the absolute, cleaned root directory (as configured; symlinks in
	// the configured path itself are preserved so URIs stay predictable).
	dir string
	// root confines every file operation to dir, so a symlink planted inside
	// the tree cannot redirect a read or write outside it.
	root *os.Root
}

// compile-time contract check.
var _ Store = (*LocalStore)(nil)

// NewLocalStore opens (creating if needed) the artifact root at dir.
func NewLocalStore(dir string) (*LocalStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, localObjectsDir, localMetadataDir} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local store: create %q: %w", subdir, err)
		}
	}
	return &LocalStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the store.
func (s *LocalStore) Dir() string { return s.dir }

// Close releases the directory handle held by the store.
func (s *LocalStore) Close() error { return s.root.Close() }

// Put streams r to a temporary file inside the root, hashes it, then atomically
// renames it onto the physical content-addressed key for the opaque name.
func (s *LocalStore) Put(ctx context.Context, name string, r io.Reader, opts PutOptions) (ArtifactRef, error) {
	if r == nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: nil reader", name)
	}
	if err := ctx.Err(); err != nil {
		return ArtifactRef{}, err
	}

	tmpKey := path.Join(localTempDir, "put-"+randomToken())
	tmp, err := s.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_RDWR, localFilePerm)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: create temp file: %w", name, err)
	}
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = s.root.Remove(tmpKey)
		}
	}()

	hasher := sha256.New()
	sniff := &sniffWriter{}
	size, err := io.Copy(io.MultiWriter(tmp, hasher, sniff), &ctxReader{ctx: ctx, r: r})
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: write: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: sync: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: close: %w", name, err)
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	key := objectKey(name, sum)
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: create object dir: %w", name, err)
	}
	if err := s.root.Rename(tmpKey, key); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local put %q: commit: %w", name, err)
	}
	committed = true

	ref := ArtifactRef{
		Name:        name,
		URI:         s.uri(key),
		ContentType: resolveContentType(opts.ContentType, sniff.head),
		SizeBytes:   size,
		SHA256:      sum,
		Metadata:    copyMetadata(opts.Metadata),
	}
	if err := s.writeSidecar(key, ref); err != nil {
		return ArtifactRef{}, err
	}
	return ref, nil
}

// Get opens the artifact bytes at uri.
func (s *LocalStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	key, _, _, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.root.Open(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifacts: local get %q: %w", uri, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: local get %q: %w", uri, err)
	}
	return f, nil
}

// Stat returns the stored reference for uri without reading the bytes.
func (s *LocalStore) Stat(ctx context.Context, uri string) (ArtifactRef, error) {
	key, name, sum, err := s.parseURI(uri)
	if err != nil {
		return ArtifactRef{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArtifactRef{}, err
	}
	return s.statKey(key, name, sum)
}

// List returns every artifact whose decoded logical name starts with prefix,
// ordered by URI.
func (s *LocalStore) List(ctx context.Context, prefix string) ([]ArtifactRef, error) {
	var refs []ArtifactRef
	err := fs.WalkDir(s.root.FS(), localObjectsDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if d.IsDir() {
			return nil
		}
		name, sum, ok := splitKey(p)
		if !ok || !strings.HasPrefix(name, prefix) {
			return nil
		}
		ref, serr := s.statKey(p, name, sum)
		if serr != nil {
			if errors.Is(serr, ErrNotFound) {
				return nil // removed concurrently
			}
			return serr
		}
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: local list %q: %w", prefix, err)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].URI < refs[j].URI })
	return refs, nil
}

// Presign is not supported: a file:// path carries no expiring grant. Callers
// use Get, or the URI directly, for local artifacts.
func (s *LocalStore) Presign(ctx context.Context, uri string, ttl time.Duration) (string, error) {
	if _, _, _, err := s.parseURI(uri); err != nil {
		return "", err
	}
	return "", fmt.Errorf("artifacts: local presign %q: %w", uri, ErrUnsupported)
}

// Delete removes the artifact and its metadata sidecar. It is idempotent.
func (s *LocalStore) Delete(ctx context.Context, uri string) error {
	key, _, _, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifacts: local delete %q: %w", uri, err)
	}
	metaKey := metadataKey(key)
	if err := s.root.Remove(metaKey); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifacts: local delete %q: remove sidecar: %w", uri, err)
	}
	s.pruneEmptyDirs(path.Dir(key), localObjectsDir)
	s.pruneEmptyDirs(path.Dir(metaKey), localMetadataDir)
	return nil
}

// statKey builds a reference for an existing physical object key.
func (s *LocalStore) statKey(key, name, sum string) (ArtifactRef, error) {
	info, err := s.root.Stat(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ArtifactRef{}, fmt.Errorf("artifacts: local stat %q: %w", s.uri(key), ErrNotFound)
		}
		return ArtifactRef{}, fmt.Errorf("artifacts: local stat %q: %w", s.uri(key), err)
	}
	if info.IsDir() {
		return ArtifactRef{}, fmt.Errorf("artifacts: local stat %q: %w", s.uri(key), ErrNotFound)
	}

	ref := ArtifactRef{
		Name:      name,
		URI:       s.uri(key),
		SizeBytes: info.Size(),
		SHA256:    sum,
	}
	side, err := s.readSidecar(key)
	switch {
	case err != nil:
		return ArtifactRef{}, err
	case side != nil:
		ref.ContentType = side.ContentType
		ref.Metadata = copyMetadata(side.Metadata)
	default:
		ref.ContentType = s.sniffContentType(key)
	}
	return ref, nil
}

// sidecar is the on-disk companion record for attributes the filesystem cannot
// hold itself.
type sidecar struct {
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func metadataKey(key string) string {
	return path.Join(localMetadataDir, strings.TrimPrefix(key, localObjectsDir+"/")) + localMetaSuffix
}

func (s *LocalStore) writeSidecar(key string, ref ArtifactRef) error {
	data, err := json.Marshal(sidecar{ContentType: ref.ContentType, Metadata: ref.Metadata})
	if err != nil {
		return fmt.Errorf("artifacts: local put %q: encode sidecar: %w", ref.Name, err)
	}
	metaKey := metadataKey(key)
	if err := s.root.MkdirAll(path.Dir(metaKey), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local put %q: create metadata dir: %w", ref.Name, err)
	}
	tmpKey := path.Join(localTempDir, "meta-"+randomToken())
	if err := s.root.WriteFile(tmpKey, data, localFilePerm); err != nil {
		return fmt.Errorf("artifacts: local put %q: write sidecar: %w", ref.Name, err)
	}
	if err := s.root.Rename(tmpKey, metaKey); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local put %q: commit sidecar: %w", ref.Name, err)
	}
	return nil
}

func (s *LocalStore) readSidecar(key string) (*sidecar, error) {
	data, err := s.root.ReadFile(metadataKey(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: local stat %q: read sidecar: %w", s.uri(key), err)
	}
	var side sidecar
	if err := json.Unmarshal(data, &side); err != nil {
		return nil, fmt.Errorf("artifacts: local stat %q: decode sidecar: %w", s.uri(key), err)
	}
	return &side, nil
}

// sniffContentType is the fallback when a sidecar is missing (an artifact
// copied into the physical object tree by hand, for example).
func (s *LocalStore) sniffContentType(key string) string {
	f, err := s.root.Open(key)
	if err != nil {
		return ""
	}
	defer f.Close()
	var head [sniffLen]byte
	n, err := io.ReadFull(f, head[:])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return ""
	}
	return resolveContentType("", head[:n])
}

// pruneEmptyDirs removes now-empty generated directories, retaining stop.
func (s *LocalStore) pruneEmptyDirs(dir, stop string) {
	for dir != stop && dir != "." && dir != "/" && dir != "" {
		entries, err := fs.ReadDir(s.root.FS(), dir)
		if err != nil || len(entries) != 0 {
			return
		}
		if err := s.root.Remove(dir); err != nil {
			return
		}
		dir = path.Dir(dir)
	}
}

// uri renders the canonical file:// URI for a generated object key.
func (s *LocalStore) uri(key string) string {
	abs := filepath.Join(s.dir, filepath.FromSlash(key))
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	return u.String()
}

// parseURI validates a file:// URI against this store and returns the physical
// object key, exact logical name, and digest.
func (s *LocalStore) parseURI(uri string) (key, name, sum string, err error) {
	u, perr := url.Parse(uri)
	if perr != nil {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: %v", uri, ErrInvalidURI, perr)
	}
	if u.Scheme != "file" {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: scheme must be file://", uri, ErrInvalidURI)
	}
	if u.Host != "" && u.Host != "localhost" {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: unexpected host %q", uri, ErrInvalidURI, u.Host)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: query and fragment are not allowed", uri, ErrInvalidURI)
	}

	abs := filepath.Clean(filepath.FromSlash(u.Path))
	rel, rerr := filepath.Rel(s.dir, abs)
	if rerr != nil {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: outside store root", uri, ErrInvalidURI)
	}
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: outside store root %s", uri, ErrInvalidURI, s.dir)
	}
	name, sum, ok := splitKey(rel)
	if !ok {
		return "", "", "", fmt.Errorf("artifacts: local %q: %w: malformed physical object key", uri, ErrInvalidURI)
	}
	return rel, name, sum, nil
}

func randomToken() string {
	var b [12]byte
	// crypto/rand.Read is documented never to return an error.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
