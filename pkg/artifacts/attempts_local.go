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

// LocalAttemptStore keeps task attempt records on the filesystem, under exactly
// the S3 key layout as directories — the same mirroring LocalLogStore and
// LocalRunArchive do, so the two backends answer identical reads and a local
// tree can be uploaded to a bucket verbatim.
//
// Everything goes through an os.Root: the run and task ids are already encoded
// into single safe segments by AttemptKey, and the root refuses anything that
// still escapes.
type LocalAttemptStore struct {
	dir  string
	root *os.Root
}

// compile-time contract checks.
var (
	_ AttemptStore          = (*LocalAttemptStore)(nil)
	_ AttemptDocumentReader = (*LocalAttemptStore)(nil)
	_ RunAttemptReader      = (*LocalAttemptStore)(nil)
)

// NewLocalAttemptStore opens (creating if needed) the attempt tree under dir.
func NewLocalAttemptStore(dir string) (*LocalAttemptStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local attempt store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local attempt store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local attempt store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local attempt store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, AttemptPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local attempt store: create %q: %w", subdir, err)
		}
	}
	return &LocalAttemptStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the attempt tree.
func (s *LocalAttemptStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalAttemptStore) Close() error { return s.root.Close() }

// PutAttempt writes one record through a temp file and a rename, so a reader
// listing the directory never sees a half-written record. A partially readable
// record is worse than a missing one: the reader would report an attempt whose
// outcome it could not decode.
func (s *LocalAttemptStore) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	key, document, err := encodeAttemptForStore(attempt)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local task attempt %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "attempt-"+randomToken())
	file, err := s.root.Create(tmpKey)
	if err != nil {
		return fmt.Errorf("artifacts: local task attempt %q: create temp: %w", key, err)
	}
	if _, err := file.Write(document); err != nil {
		_ = file.Close()
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local task attempt %q: write: %w", key, err)
	}
	if err := file.Close(); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local task attempt %q: close: %w", key, err)
	}
	if err := s.root.Rename(tmpKey, key); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local task attempt %q: commit: %w", key, err)
	}
	return nil
}

// ListAttempts returns one task's records, ascending by attempt.
func (s *LocalAttemptStore) ListAttempts(
	ctx context.Context, runID, taskID string,
) ([]*orkstackv1.TaskAttempt, error) {
	documents, err := s.ReadAttemptDocuments(ctx, runID, taskID)
	if err != nil {
		return nil, err
	}
	return attemptRecords(documents), nil
}

// ReadAttemptDocuments returns one task's stored records with their bytes,
// ascending by attempt.
func (s *LocalAttemptStore) ReadAttemptDocuments(
	ctx context.Context, runID, taskID string,
) ([]AttemptDocument, error) {
	prefix, err := AttemptTaskPrefix(runID, taskID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(prefix, "/")
	entries, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A task with no records reads empty. That is the ordinary state of
			// a task that has not run yet, and of every run finished before
			// this key family existed.
			return []AttemptDocument{}, nil
		}
		return nil, fmt.Errorf("artifacts: local task attempts %q: %w", dir, err)
	}
	found := make([]AttemptDocument, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		key := dir + "/" + entry.Name()
		ref, ok := ParseAttemptKey(key)
		if !ok || ref.RunID != runID || ref.TaskID != taskID {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		document, err := fs.ReadFile(s.root.FS(), key)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Expired under a retention rule between the listing and the
				// read. A missing record reads as a missing record.
				continue
			}
			return nil, fmt.Errorf("artifacts: local task attempt %q: %w", key, err)
		}
		record, err := UnmarshalAttempt(document)
		if err != nil {
			return nil, fmt.Errorf("artifacts: local task attempt %q: %w", key, err)
		}
		found = append(found, AttemptDocument{Ref: ref, Key: key, Raw: document, Record: record})
	}
	sortAttemptDocuments(found)
	return found, nil
}

// ListRunAttemptRefs reads the run's directory, then each task directory, as
// ListAttempts reads one task's — keys only, no record is opened.
func (s *LocalAttemptStore) ListRunAttemptRefs(ctx context.Context, runID string) ([]AttemptRef, error) {
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(prefix, "/")
	tasks, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A run with no records reads empty: never run, or reclaimed.
			return []AttemptRef{}, nil
		}
		return nil, fmt.Errorf("artifacts: local run attempts %q: %w", dir, err)
	}
	var keys []string
	for _, task := range tasks {
		if !task.IsDir() {
			continue
		}
		taskDir := dir + "/" + task.Name()
		entries, err := fs.ReadDir(s.root.FS(), taskDir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("artifacts: local task attempts %q: %w", taskDir, err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				keys = append(keys, taskDir+"/"+entry.Name())
			}
		}
	}
	return runAttemptRefs(runID, keys), nil
}

// GetAttempt reads one record; a missing one matches ErrNotFound.
func (s *LocalAttemptStore) GetAttempt(ctx context.Context, ref AttemptRef) (*orkstackv1.TaskAttempt, error) {
	key, err := AttemptKey(ref)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	document, err := fs.ReadFile(s.root.FS(), key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifacts: local task attempt %q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: local task attempt %q: %w", key, err)
	}
	record, err := UnmarshalAttempt(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local task attempt %q: %w", key, err)
	}
	return record, nil
}
