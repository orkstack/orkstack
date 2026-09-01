package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// LocalRunSnapshotStore keeps run snapshots on the filesystem, under exactly
// the S3 key layout as directories — the same mirroring LocalLogStore and
// LocalRunArchive do, so the two backends answer identical reads and a local
// tree can be uploaded to a bucket verbatim.
type LocalRunSnapshotStore struct {
	dir  string
	root *os.Root
}

// compile-time contract check.
var _ RunSnapshotStore = (*LocalRunSnapshotStore)(nil)

// NewLocalRunSnapshotStore opens (creating if needed) the snapshot tree under
// dir.
func NewLocalRunSnapshotStore(dir string) (*LocalRunSnapshotStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local snapshot store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local snapshot store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local snapshot store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local snapshot store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, SnapshotPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local snapshot store: create %q: %w", subdir, err)
		}
	}
	return &LocalRunSnapshotStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the snapshot tree.
func (s *LocalRunSnapshotStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalRunSnapshotStore) Close() error { return s.root.Close() }

// WriteSnapshot writes one snapshot through a temp file and a rename, so a
// concurrent reader never sees a half-written snapshot. The key is derived
// from the coordinates alone, so writing the same coordinates twice lands on
// the same key and returns the same URI.
func (s *LocalRunSnapshotStore) WriteSnapshot(
	ctx context.Context,
	namespace, runID string,
	sequence int64,
	data []byte,
) (string, error) {
	key, err := RunSnapshotKey(namespace, runID, sequence)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("artifacts: local snapshot %q: empty snapshot", key)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return "", fmt.Errorf("artifacts: local snapshot %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "snapshot-"+randomToken())
	file, err := s.root.Create(tmpKey)
	if err != nil {
		return "", fmt.Errorf("artifacts: local snapshot %q: create temp: %w", key, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = s.root.Remove(tmpKey)
		return "", fmt.Errorf("artifacts: local snapshot %q: write: %w", key, err)
	}
	if err := file.Close(); err != nil {
		_ = s.root.Remove(tmpKey)
		return "", fmt.Errorf("artifacts: local snapshot %q: close: %w", key, err)
	}
	if err := s.root.Rename(tmpKey, key); err != nil {
		_ = s.root.Remove(tmpKey)
		return "", fmt.Errorf("artifacts: local snapshot %q: commit: %w", key, err)
	}
	return s.uri(key), nil
}

// ReadSnapshot returns the bytes at a URI WriteSnapshot returned.
func (s *LocalRunSnapshotStore) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	key, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := fs.ReadFile(s.root.FS(), key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifacts: local snapshot %q: %w", uri, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: local snapshot %q: %w", uri, err)
	}
	return data, nil
}

// DeleteSnapshot removes one snapshot. Removing a snapshot that is already
// gone returns nil.
func (s *LocalRunSnapshotStore) DeleteSnapshot(ctx context.Context, uri string) error {
	key, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifacts: local snapshot %q: delete: %w", uri, err)
	}
	return nil
}

// uri renders the canonical file:// URI for a snapshot key.
func (s *LocalRunSnapshotStore) uri(key string) string {
	abs := filepath.Join(s.dir, filepath.FromSlash(key))
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	return u.String()
}

// parseURI reverses uri: it accepts only a file URI inside this store's own
// snapshot prefix, so a foreign or workload URI can never be read as a
// snapshot.
func (s *LocalRunSnapshotStore) parseURI(uri string) (string, error) {
	u, perr := url.Parse(uri)
	if perr != nil {
		return "", fmt.Errorf("artifacts: local snapshot %q: %w: %v", uri, ErrInvalidURI, perr)
	}
	if u.Scheme != "file" {
		return "", fmt.Errorf("artifacts: local snapshot %q is not a file uri: %w", uri, ErrInvalidURI)
	}
	abs := filepath.FromSlash(u.Path)
	rel, err := filepath.Rel(s.dir, abs)
	if err != nil {
		return "", fmt.Errorf("artifacts: local snapshot %q is outside the store: %w", uri, ErrInvalidURI)
	}
	key := filepath.ToSlash(rel)
	if !strings.HasPrefix(key, SnapshotPrefix+"/") || !strings.HasSuffix(key, snapshotSuffix) {
		return "", fmt.Errorf("artifacts: local snapshot %q is not a snapshot key: %w", uri, ErrInvalidURI)
	}
	return key, nil
}
