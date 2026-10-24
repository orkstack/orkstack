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

// LocalDeliveryStore keeps delivery records on the filesystem under exactly
// the S3 key layout as directories, so the two backends answer identical
// reads and a local tree can be uploaded to a bucket verbatim. Everything goes
// through an os.Root, and a key is derived (and so validated) before any path
// operation.
type LocalDeliveryStore struct {
	dir  string
	root *os.Root
}

var _ DeliveryStore = (*LocalDeliveryStore)(nil)

// NewLocalDeliveryStore opens (creating if needed) the delivery tree under dir.
func NewLocalDeliveryStore(dir string) (*LocalDeliveryStore, error) {
	if dir == "" {
		return nil, errors.New("artifacts: local delivery store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local delivery store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local delivery store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local delivery store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, DeliveryPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local delivery store: create %q: %w", subdir, err)
		}
	}
	return &LocalDeliveryStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the delivery tree.
func (s *LocalDeliveryStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalDeliveryStore) Close() error { return s.root.Close() }

// PutDelivery writes one record create-only: the bytes go to an exclusive
// temp sibling under .tmp/ first, and Link publishes the fully written inode
// without ever replacing an existing record — the publication
// LocalTriggerStore.Create and LocalAuditStore use. A crash before the link
// leaves only an ignored temp file.
func (s *LocalDeliveryStore) PutDelivery(ctx context.Context, rec *orkstackv1.Delivery) error {
	key, document, err := encodeDeliveryForStore(rec)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local delivery record %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "delivery-"+randomToken())
	f, err := s.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("artifacts: local delivery record %q: create temp: %w", key, err)
	}
	if _, err = f.Write(document); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local delivery record %q: write: %w", key, err)
	}
	err = s.root.Link(tmpKey, key)
	_ = s.root.Remove(tmpKey)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("artifacts: local delivery record %q: %w", key, ErrDeliveryExists)
	}
	if err != nil {
		return fmt.Errorf("artifacts: local delivery record %q: publish: %w", key, err)
	}
	return nil
}

// GetDelivery walks one id's directory, then reads its one record.
func (s *LocalDeliveryStore) GetDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, bool, error) {
	return getDelivery(ctx, s, id)
}

// ListDeliveries walks the day directories newest first.
func (s *LocalDeliveryStore) ListDeliveries(ctx context.Context, q DeliveryQuery) (DeliveryPage, error) {
	return walkDeliveries(ctx, s, q)
}

func (s *LocalDeliveryStore) listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(dayPrefix, "/")
	entries, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: local delivery day %q: %w", dir, err)
	}
	var keys []string
	// ReadDir sorts by name, and an id directory's keys all start with
	// dayPrefix+id+"/", so the range can be decided per id directory.
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		idPrefix := dayPrefix + entry.Name() + "/"
		if idPrefix < from {
			continue
		}
		if idPrefix >= to {
			break
		}
		found, err := s.listDeliveryID(ctx, idPrefix)
		if err != nil {
			return nil, err
		}
		for _, key := range found {
			if key > from && key < to {
				keys = append(keys, key)
			}
		}
		if len(keys) > max {
			return keys, nil
		}
	}
	return keys, nil
}

func (s *LocalDeliveryStore) listDeliveryID(ctx context.Context, idPrefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(idPrefix, "/")
	var keys []string
	err := fs.WalkDir(s.root.FS(), dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			keys = append(keys, name)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: local delivery %q: %w", dir, err)
	}
	return keys, nil
}

func (s *LocalDeliveryStore) fetchDelivery(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	document, err := fs.ReadFile(s.root.FS(), key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: local delivery record %q: %w", key, err)
	}
	return document, true, nil
}
