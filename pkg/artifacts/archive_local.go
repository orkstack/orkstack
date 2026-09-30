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
	"sort"
	"strings"
	"sync/atomic"
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
	// indexKeysScanned counts index keys enumerated by ListRuns. See
	// IndexKeysScanned.
	indexKeysScanned atomic.Int64
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
	for _, subdir := range []string{localTempDir, RunArchiveNamespace, RunIndexNamespace, RunOriginIndexNamespace, RunObjectRefNamespace, RunObjectListNamespace, RunCleanupNamespace} {
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

// PutRun writes the body and object references before publishing the index key,
// and leaves an already-archived body untouched. An interrupted write can be
// retried without advertising a run whose reference accounting is incomplete.
func (a *LocalRunArchive) PutRun(ctx context.Context, rec RunRecord) error {
	bodyKey, err := RunArchiveKey(rec.RunID)
	if err != nil {
		return err
	}
	indexKey, err := RunIndexKey(rec)
	if err != nil {
		return err
	}
	originKey, hasOrigin, err := RunOriginIndexKey(rec)
	if err != nil {
		return err
	}
	objectIDs := dedupeObjectIDs(rec.ObjectIDs)
	listKey, err := RunObjectListKey(rec.RunID)
	if err != nil {
		return err
	}
	listBody, err := json.Marshal(objectIDs)
	if err != nil {
		return fmt.Errorf("artifacts: local archive run %q: encode object list: %w", rec.RunID, err)
	}
	refKeys := make([]string, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		refKey, err := RunObjectRefKey(objectID, rec.RunID)
		if err != nil {
			return err
		}
		refKeys = append(refKeys, refKey)
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

	// The list keeps exact object ids out of marker-key components and makes
	// deletion proportional to this run's references rather than the archive.
	if err := a.writeAtomic(listKey, listBody); err != nil {
		return fmt.Errorf("artifacts: local archive run %q: write object list: %w", rec.RunID, err)
	}
	for _, refKey := range refKeys {
		if err := a.writeAtomic(refKey, nil); err != nil {
			return fmt.Errorf("artifacts: local archive run %q: write object reference: %w", rec.RunID, err)
		}
	}
	// The origin row goes before the run-index row so the run index stays the
	// last key published, which is what lets cleanup see every other record
	// of a run before it sees the run.
	if hasOrigin {
		if err := a.writeAtomic(originKey, nil); err != nil {
			return fmt.Errorf("artifacts: local archive run %q: write origin index: %w", rec.RunID, err)
		}
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

// DeleteRunArtifacts removes run-owned logs, attempt records, commits,
// snapshots, and reference markers. Removing a marker only drops this run's
// claim: shared workload objects stay untouched until
// DeleteUnreferencedObjects observes that no claims remain.
func (a *LocalRunArchive) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (CleanupResult, error) {
	logPrefix, err := RunLogPrefix(runID)
	if err != nil {
		return CleanupResult{}, err
	}
	// Attempt records explain the logs they sit beside, so they are reclaimed
	// on the same pass and therefore have exactly the log prefix's lifetime.
	attemptPrefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return CleanupResult{}, err
	}
	commitPrefix, err := RunCommitPrefix(namespace, runID)
	if err != nil {
		return CleanupResult{}, err
	}
	snapshotPrefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return CleanupResult{}, err
	}
	var result CleanupResult
	for _, prefix := range []string{logPrefix, attemptPrefix, commitPrefix, snapshotPrefix} {
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

	listKey, err := RunObjectListKey(runID)
	if err != nil {
		return result, err
	}
	listBody, err := a.root.ReadFile(listKey)
	if errors.Is(err, fs.ErrNotExist) {
		// PutRun writes the object list before it publishes the index, so
		// an archived run with no list is a corrupt archive. Do nothing rather
		// than fall back to a namespace-wide scan, which would make every
		// cleanup unbounded.
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("artifacts: local cleanup run %q read object list: %w", runID, err)
	}
	var objectIDs []string
	if err := json.Unmarshal(listBody, &objectIDs); err != nil {
		return result, fmt.Errorf("artifacts: local cleanup run %q decode object list: %w", runID, err)
	}
	for _, objectID := range objectIDs {
		key, err := RunObjectRefKey(objectID, runID)
		if err != nil {
			return result, err
		}
		if err := a.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return result, fmt.Errorf("artifacts: local cleanup run %q delete object reference %q: %w", runID, key, err)
		}
	}
	if err := a.root.Remove(listKey); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return result, fmt.Errorf("artifacts: local cleanup run %q delete object list: %w", runID, err)
	}
	return result, nil
}

// ListObjectReferences walks one object's marker prefix exactly as
// DeleteUnreferencedObjects does, collecting every marker's run id. A prefix
// that does not exist is an object nothing references: an empty list.
func (a *LocalRunArchive) ListObjectReferences(ctx context.Context, objectID string) ([]string, error) {
	if _, _, ok := splitKey(objectID); !ok {
		return nil, fmt.Errorf("%w: %q is not a canonical local object id", ErrInvalidURI, objectID)
	}
	collector := newObjectReferenceCollector()
	err := fs.WalkDir(a.root.FS(), strings.TrimSuffix(runObjectRefPrefix(objectID), "/"), func(key string, d fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, fs.ErrNotExist) {
			return fs.SkipAll
		}
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		return collector.add(key)
	})
	if err != nil {
		if errors.Is(err, ErrTooManyReferences) {
			return nil, err
		}
		return nil, fmt.Errorf("artifacts: local list references of %q: %w", objectID, err)
	}
	return collector.runIDs, nil
}

// DeleteUnreferencedObjects removes local content-addressed objects only after
// their reference prefix is empty. Missing objects make retries a no-op.
func (a *LocalRunArchive) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (CleanupResult, error) {
	var result CleanupResult
	var errs []error
	seen := make(map[string]struct{}, len(objectIDs))
	for _, objectID := range objectIDs {
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		if _, _, ok := splitKey(objectID); !ok {
			errs = append(errs, fmt.Errorf("object %q: not a canonical local object id", objectID))
			continue
		}
		prefix := runObjectRefPrefix(objectID)
		referenced := false
		err := fs.WalkDir(a.root.FS(), strings.TrimSuffix(prefix, "/"), func(_ string, d fs.DirEntry, walkErr error) error {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return fs.SkipAll
			}
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !d.IsDir() {
				referenced = true
				return fs.SkipAll
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: list references: %w", objectID, err))
			continue
		}
		if referenced {
			continue
		}
		info, err := a.root.Stat(objectID)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: stat: %w", objectID, err))
			continue
		}
		if err := a.root.Remove(objectID); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("object %q: delete: %w", objectID, err))
			continue
		}
		result.ObjectsDeleted++
		result.BytesReclaimed += info.Size()
		if err := a.root.Remove(metadataKey(objectID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("object %q: delete metadata: %w", objectID, err))
			continue
		}
	}
	return result, errors.Join(errs...)
}

// ListRuns walks the narrowest index subtree the query can match and reads
// nothing but names, which is the filesystem's equivalent of an S3 prefix scan.
//
// A KeyOrder query takes the resumable walk instead, which stops at the page
// boundary rather than enumerating the subtree and sorting it.
func (a *LocalRunArchive) ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	prefix, _, err := q.ScanPrefix()
	if err != nil {
		return nil, err
	}
	walkRoot := strings.TrimSuffix(prefix, "/")
	if q.KeyOrder {
		return a.listRunsInKeyOrder(ctx, q, walkRoot, prefix)
	}

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
		a.indexKeysScanned.Add(1)
		entry, ok := q.parseKey(p)
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

// listRunsInKeyOrder descends the index in exact key order, skipping subtrees
// that lie wholly at or before the cursor and stopping as soon as the page is
// full. Cost is proportional to the page, not to the subtree, so paging a whole
// traversal enumerates each key once rather than once per page.
//
// It does not use fs.WalkDir. WalkDir orders siblings by name, and a name is
// not a key: '-' (0x2D) sorts below the '/' (0x2F) that separates segments, so
// sibling directories "a" and "a-b" are visited in an order that contradicts
// the order of the keys beneath them. Comparing each child by the string its
// keys actually start with — name+"/" for a directory, name for a file — makes
// the descent agree with the cursor comparisons, which is what lets a page
// boundary be resumed without skipping or repeating a run.
func (a *LocalRunArchive) listRunsInKeyOrder(ctx context.Context, q RunQuery, walkRoot, prefix string) ([]RunIndexEntry, error) {
	entries := make([]RunIndexEntry, 0, q.pageCapacity())
	full := func() bool { return q.Limit > 0 && len(entries) >= q.Limit }

	var descend func(dir string) error
	descend = func(dir string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		children, err := fs.ReadDir(a.root.FS(), dir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // nothing archived under this prefix yet
			}
			return err
		}
		sort.Slice(children, func(i, j int) bool {
			return keyOrderName(children[i]) < keyOrderName(children[j])
		})
		for _, child := range children {
			if full() {
				return nil
			}
			p := path.Join(dir, child.Name())
			if child.IsDir() {
				if q.prunesDir(p) {
					continue
				}
				if err := descend(p); err != nil {
					return err
				}
				continue
			}
			if !q.afterCursor(p) {
				continue
			}
			a.indexKeysScanned.Add(1)
			entry, ok := q.parseKey(p)
			if !ok || !q.Matches(entry) {
				continue
			}
			entries = append(entries, entry)
		}
		return nil
	}

	if err := descend(walkRoot); err != nil {
		return nil, fmt.Errorf("artifacts: local archive list %q: %w", prefix, err)
	}
	return entries, nil
}

// keyOrderName renders the string a child's keys begin with, so sorting by it
// orders siblings the same way their keys sort.
func keyOrderName(d fs.DirEntry) string {
	if d.IsDir() {
		return d.Name() + "/"
	}
	return d.Name()
}

// IndexKeysScanned reports how many index keys this archive has enumerated
// across every ListRuns call. It is the instrument behind the cleanup worker's
// scan-cost contract: a bounded page is only bounded if the backend stops at
// the page boundary, and elapsed time cannot tell the difference on a warm
// cache. Tests assert on it; nothing in the running system reads it.
func (a *LocalRunArchive) IndexKeysScanned() int64 { return a.indexKeysScanned.Load() }

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
