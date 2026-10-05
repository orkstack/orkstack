package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// LocalMemoStore keeps memos under ORK_ARTIFACT_DIR at the S3 key layout, as
// LocalAttemptStore keeps attempt records: a local tree can be uploaded to a
// bucket verbatim. Every path goes through an os.Root.
type LocalMemoStore struct {
	dir  string
	root *os.Root
}

var _ MemoStore = (*LocalMemoStore)(nil)

// NewLocalMemoStore opens (creating if needed) the memo tree under dir.
func NewLocalMemoStore(dir string) (*LocalMemoStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local memo store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local memo store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local memo store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local memo store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, MemoPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local memo store: create %q: %w", subdir, err)
		}
	}
	return &LocalMemoStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the memo tree.
func (s *LocalMemoStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalMemoStore) Close() error { return s.root.Close() }

// GetMemo reads one memo.
func (s *LocalMemoStore) GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error) {
	key, err := MemoKey(ref)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	document, err := fs.ReadFile(s.root.FS(), key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifacts: local memo %q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: local memo %q: %w", key, err)
	}
	return decodeStoredMemo(key, document)
}

// PutMemo writes one memo through a temp file and a rename, so a reader never
// sees a half-written memo. Last writer wins.
func (s *LocalMemoStore) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	key, document, err := encodeMemoForStore(memo)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local memo %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "memo-"+randomToken())
	file, err := s.root.Create(tmpKey)
	if err != nil {
		return fmt.Errorf("artifacts: local memo %q: create temp: %w", key, err)
	}
	if _, err := file.Write(document); err != nil {
		_ = file.Close()
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local memo %q: write: %w", key, err)
	}
	if err := file.Close(); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local memo %q: close: %w", key, err)
	}
	if err := s.root.Rename(tmpKey, key); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local memo %q: commit: %w", key, err)
	}
	return nil
}

// ListMemos walks the scope's directory and pages its keys in key order.
// LastModified is the file's mtime: when the memo was last written.
func (s *LocalMemoStore) ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error) {
	prefix, err := MemoScopePrefix(scope)
	if err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	dir := strings.TrimSuffix(prefix, "/")
	type info struct {
		modified time.Time
		size     int64
	}
	found := map[string]info{}
	err = fs.WalkDir(s.root.FS(), dir, func(key string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return ctx.Err()
		}
		fi, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		found[key] = info{modified: fi.ModTime().UTC(), size: fi.Size()}
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []MemoEntry{}, "", nil
		}
		return nil, "", fmt.Errorf("artifacts: local memos %q: %w", dir, err)
	}
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries, cursor := pageMemoKeys(keys, after, limit, func(key string) (time.Time, int64) {
		return found[key].modified, found[key].size
	})
	return entries, cursor, nil
}

// DeleteMemo removes one memo; an absent memo is not an error.
func (s *LocalMemoStore) DeleteMemo(ctx context.Context, ref MemoRef) error {
	key, err := MemoKey(ref)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifacts: local memo %q: delete: %w", key, err)
	}
	return nil
}
