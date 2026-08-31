package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// LocalRunArchive is the filesystem-backed RunArchive used for development and
// single-host deployments. It mirrors the S3 key layout exactly — the same
// metadata/runs and metadata/index/runs hierarchy, as directories — so the two
// backends answer identical queries and a local archive can be uploaded to S3
// verbatim.
type LocalRunArchive struct {
	dir  string
	root *os.Root
}

// compile-time contract check.
var _ RunArchive = (*LocalRunArchive)(nil)

// NewLocalRunArchive opens (creating if needed) the archive tree under dir.
func NewLocalRunArchive(dir string) (*LocalRunArchive, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local run archive: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local run archive: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local run archive: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local run archive: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, RunArchiveNamespace, RunIndexNamespace, RunCleanupNamespace} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local run archive: create %q: %w", subdir, err)
		}
	}
	return &LocalRunArchive{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the archive.
func (a *LocalRunArchive) Dir() string { return a.dir }

// Close releases the directory handle held by the archive.
func (a *LocalRunArchive) Close() error { return a.root.Close() }

// PutRun writes the body, then the index key, and leaves an already-archived
// body untouched — the same immutable, crash-safe order as the S3 archive.
func (a *LocalRunArchive) PutRun(ctx context.Context, rec RunRecord) error {
	bodyKey, err := RunArchiveKey(rec.RunID)
	if err != nil {
		return err
	}
	indexKey, err := RunIndexKey(rec)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	switch _, err := a.root.Stat(bodyKey); {
	case err == nil: // already archived; the body is immutable
	case errors.Is(err, fs.ErrNotExist):
		if err := a.writeAtomic(bodyKey, rec.JSONL); err != nil {
			return fmt.Errorf("artifacts: local archive run %q: %w", rec.RunID, err)
		}
	default:
		return fmt.Errorf("artifacts: local archive run %q: stat body: %w", rec.RunID, err)
	}

	if err := a.writeAtomic(indexKey, nil); err != nil {
		return fmt.Errorf("artifacts: local archive run %q: %w", rec.RunID, err)
	}
	return nil
}

// GetRun returns the archived JSONL body for runID.
func (a *LocalRunArchive) GetRun(ctx context.Context, runID string) ([]byte, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := a.root.ReadFile(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifacts: local archive get %q: %w", runID, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: local archive get %q: %w", runID, err)
	}
	return body, nil
}

// CleanupState returns a run's immutable cleanup sidecar.
func (a *LocalRunArchive) CleanupState(ctx context.Context, runID string) (RunCleanupState, error) {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return RunCleanupState{}, err
	}
	if err := ctx.Err(); err != nil {
		return RunCleanupState{}, err
	}
	body, err := a.root.ReadFile(key)
	if errors.Is(err, fs.ErrNotExist) {
		return RunCleanupState{}, fmt.Errorf("artifacts: local cleanup state %q: %w", runID, ErrNotFound)
	}
	if err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: local cleanup state %q: %w", runID, err)
	}
	var state RunCleanupState
	if err := json.Unmarshal(body, &state); err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: local cleanup state %q: decode: %w", runID, err)
	}
	return state, nil
}

// MarkArtifactsCleaned writes the cleanup sidecar once.
func (a *LocalRunArchive) MarkArtifactsCleaned(ctx context.Context, runID string, deletedAt time.Time) error {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := a.root.Stat(key); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("artifacts: local mark cleanup %q: %w", runID, err)
	}
	body, err := json.Marshal(RunCleanupState{RunID: runID, ArtifactsCleaned: true, TemporalDeletedAt: deletedAt.UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: local mark cleanup %q: %w", runID, err)
	}
	if err := a.writeOnce(key, body); err != nil {
		return fmt.Errorf("artifacts: local mark cleanup %q: %w", runID, err)
	}
	return nil

}

// DeleteRunArtifacts removes the log and commit subtrees owned by one run.
func (a *LocalRunArchive) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (CleanupResult, error) {
	logPrefix, err := RunLogPrefix(runID)
	if err != nil {
		return CleanupResult{}, err
	}
	commitPrefix, err := RunCommitPrefix(namespace, runID)
	if err != nil {
		return CleanupResult{}, err
	}
	var result CleanupResult
	for _, prefix := range []string{logPrefix, commitPrefix} {
		err := fs.WalkDir(a.root.FS(), strings.TrimSuffix(prefix, "/"), func(_ string, d fs.DirEntry, walkErr error) error {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return fs.SkipAll
			}
			if walkErr != nil || d.IsDir() {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			result.ObjectsDeleted++
			result.BytesReclaimed += info.Size()
			return nil
		})
		if err != nil {
			return result, fmt.Errorf("artifacts: local cleanup run %q list %q: %w", runID, prefix, err)
		}
		if err := a.root.RemoveAll(strings.TrimSuffix(prefix, "/")); err != nil {
			return result, fmt.Errorf("artifacts: local cleanup run %q delete %q: %w", runID, prefix, err)
		}
	}
	return result, nil
}

// ListRuns walks the narrowest index subtree the query can match and reads
// nothing but names, which is the filesystem's equivalent of an S3 prefix scan.
func (a *LocalRunArchive) ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	prefix, _, err := q.ScanPrefix()
	if err != nil {
		return nil, err
	}
	walkRoot := strings.TrimSuffix(prefix, "/")

	var entries []RunIndexEntry
	err = fs.WalkDir(a.root.FS(), walkRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll // nothing archived under this prefix yet
			}
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if d.IsDir() {
			return nil
		}
		entry, ok := ParseRunIndexKey(p)
		if !ok || !q.Matches(entry) {
			return nil
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: local archive list %q: %w", prefix, err)
	}
	return q.finish(entries), nil
}

// writeAtomic commits data at key through a temp file and a rename, so a reader
// never sees a half-written archive.
func (a *LocalRunArchive) writeAtomic(key string, data []byte) error {
	if err := a.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("create dir for %q: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "archive-"+randomToken())
	if err := a.root.WriteFile(tmpKey, data, localFilePerm); err != nil {
		return fmt.Errorf("write %q: %w", key, err)
	}
	if err := a.root.Rename(tmpKey, key); err != nil {
		_ = a.root.Remove(tmpKey)
		return fmt.Errorf("commit %q: %w", key, err)
	}
	return nil
}

// writeOnce publishes data atomically without replacing an existing winner.
func (a *LocalRunArchive) writeOnce(key string, data []byte) error {
	if err := a.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return err
	}
	tmpKey := path.Join(localTempDir, "cleanup-"+randomToken())
	if err := a.root.WriteFile(tmpKey, data, localFilePerm); err != nil {
		return err
	}
	defer a.root.Remove(tmpKey)
	if err := a.root.Link(tmpKey, key); errors.Is(err, fs.ErrExist) {
		return nil
	} else {
		return err
	}
}
