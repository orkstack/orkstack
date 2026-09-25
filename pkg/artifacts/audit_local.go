package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// LocalAuditStore keeps audit records on the filesystem under exactly the S3
// key layout as directories, so the two backends answer identical reads and a
// local tree can be uploaded to a bucket verbatim. Everything goes through an
// os.Root, and an id is validated before any path operation.
type LocalAuditStore struct {
	dir  string
	root *os.Root
}

var _ AuditStore = (*LocalAuditStore)(nil)

// NewLocalAuditStore opens (creating if needed) the audit tree under dir.
func NewLocalAuditStore(dir string) (*LocalAuditStore, error) {
	if dir == "" {
		return nil, errors.New("artifacts: local audit store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local audit store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local audit store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local audit store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, AuditPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local audit store: create %q: %w", subdir, err)
		}
	}
	return &LocalAuditStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the audit tree.
func (s *LocalAuditStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalAuditStore) Close() error { return s.root.Close() }

// PutAuditRecord writes one record create-only: the bytes go to an exclusive
// temp sibling first, and Link publishes the fully written inode without ever
// replacing an existing record — the same publication LocalTriggerStore.Create
// uses. A crash before the link leaves only an ignored temp file.
func (s *LocalAuditStore) PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	key, document, err := encodeAuditForStore(rec)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local audit record %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "audit-"+randomToken())
	f, err := s.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("artifacts: local audit record %q: create temp: %w", key, err)
	}
	if _, err = f.Write(document); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local audit record %q: write: %w", key, err)
	}
	err = s.root.Link(tmpKey, key)
	_ = s.root.Remove(tmpKey)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("artifacts: local audit record %q: %w", key, ErrAuditRecordExists)
	}
	if err != nil {
		return fmt.Errorf("artifacts: local audit record %q: publish: %w", key, err)
	}
	return nil
}

// ListAuditRecords walks the day directories newest first.
func (s *LocalAuditStore) ListAuditRecords(ctx context.Context, q AuditQuery) (AuditPage, error) {
	return walkAudit(ctx, s, q)
}

func (s *LocalAuditStore) listAuditDay(ctx context.Context, dayPrefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(dayPrefix, "/")
	entries, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: local audit day %q: %w", dir, err)
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			keys = append(keys, dir+"/"+entry.Name())
		}
	}
	return keys, nil
}

func (s *LocalAuditStore) fetchAudit(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	document, err := fs.ReadFile(s.root.FS(), key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: local audit record %q: %w", key, err)
	}
	return document, true, nil
}
