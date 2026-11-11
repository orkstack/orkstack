package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// BlobRunArchive is the RunArchive over the shared object store. It writes
// metadata/runs/{run-id}.jsonl and the index keys beside it, and answers
// queries with prefix listings that transfer keys only. A local directory
// holds the same keys as directories, so the two backends answer identical
// queries and a local archive can be uploaded to a bucket verbatim.
type BlobRunArchive struct {
	blobBase
	// local is set for a local directory, whose content-addressed objects
	// are LocalStore's (a sidecar beside each) rather than S3Store's (a name
	// manifest beside some).
	local bool
	// indexKeysScanned counts index keys enumerated by ListRuns. See
	// IndexKeysScanned.
	indexKeysScanned atomic.Int64
}

// LocalRunArchive and S3RunArchive are the run archive over a local directory
// and over a bucket: one implementation, two backends.
type (
	LocalRunArchive = BlobRunArchive
	S3RunArchive    = BlobRunArchive
)

// compile-time contract check.
var _ RunArchive = (*BlobRunArchive)(nil)

// runArchiveDirs are the key families a local archive creates up front.
var runArchiveDirs = []string{
	RunArchiveNamespace, RunIndexNamespace, RunOriginIndexNamespace, RunRootIndexNamespace,
	RunObjectRefNamespace, RunObjectListNamespace, RunCleanupNamespace,
}

// NewLocalRunArchive opens (creating if needed) the archive tree under dir.
func NewLocalRunArchive(dir string) (*LocalRunArchive, error) {
	b, err := openLocalBlobs("run archive", dir, runArchiveDirs...)
	if err != nil {
		return nil, err
	}
	return &BlobRunArchive{blobBase: blobBase{b}, local: true}, nil
}

// NewS3RunArchive builds an S3-backed run archive from cfg. It shares the
// artifact store's client construction, so one set of ORK_S3_* variables
// configures both.
func NewS3RunArchive(ctx context.Context, cfg Config) (*S3RunArchive, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: s3 run archive: %w", err)
	}
	return &BlobRunArchive{blobBase: blobBase{b}}, nil
}

// NewRunArchive builds the RunArchive named by cfg.Backend, so one
// ORK_ARTIFACT_BACKEND selects where artifacts and run history both live.
func NewRunArchive(cfg Config) (RunArchive, error) {
	b, err := openBlobs(context.Background(), cfg, "run archive", runArchiveDirs...)
	if err != nil {
		return nil, err
	}
	_, local := b.(*localBlobs)
	return &BlobRunArchive{blobBase: blobBase{b}, local: local}, nil
}

// NewRunArchiveFromEnv builds the RunArchive described by the ORK_* environment.
func NewRunArchiveFromEnv() (RunArchive, error) { return NewRunArchive(ConfigFromEnv()) }

// URI renders the canonical URI of an archived run body.
func (a *BlobRunArchive) URI(runID string) (string, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return "", err
	}
	return a.b.locate(key), nil
}

// TrimBucketURI reports the object key inside uri when it addresses this
// archive's store. It exists so proof tooling can turn a URI back into a key.
func (a *BlobRunArchive) TrimBucketURI(uri string) (string, bool) {
	return strings.CutPrefix(uri, a.b.locate(""))
}

// PutRun writes the object references before publishing the index key, and the
// body after it. An interrupted write can be retried without advertising a run
// whose reference accounting is incomplete, and a body means every other record
// of the run was written (HasRun): an index row with no body is an interrupted
// write, which every reader of the index skips and the archive sweep finishes.
//
// The body is immutable. A run that already has one keeps it — this is what
// makes an activity retry, a workflow replay and a second engine writing the
// same terminal event converge instead of racing.
func (a *BlobRunArchive) PutRun(ctx context.Context, rec RunRecord) error {
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
	rootKey, hasRoot, err := RunRootIndexKey(rec)
	if err != nil {
		return err
	}
	recentKey, err := RunRecentIndexKey(rec)
	if err != nil {
		return err
	}
	objectIDs := dedupeObjectIDs(rec.ObjectIDs)
	listKey, err := RunObjectListKey(rec.RunID)
	if err != nil {
		return err
	}
	listBody, err := stablejson.Marshal(objectIDs)
	if err != nil {
		return fmt.Errorf("artifacts: archive run %q: encode object list: %w", rec.RunID, err)
	}
	refKeys := make([]string, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		refKey, err := RunObjectRefKey(objectID, rec.RunID)
		if err != nil {
			return err
		}
		refKeys = append(refKeys, refKey)
	}

	exists, err := a.exists(ctx, bodyKey)
	if err != nil {
		return fmt.Errorf("artifacts: archive run %q: %w", rec.RunID, err)
	}

	// The list keeps exact object ids out of marker-key components and makes
	// deletion proportional to this run's references rather than the archive.
	if err := a.b.put(ctx, listKey, RunCleanupContentType, listBody); err != nil {
		return fmt.Errorf("artifacts: archive run %q: write object list %q: %w", rec.RunID, listKey, err)
	}
	if err := a.putObjectRefs(ctx, refKeys); err != nil {
		return fmt.Errorf("artifacts: archive run %q: %w", rec.RunID, err)
	}
	// The origin and root rows go before the run-index row, which is what
	// lets cleanup see every record but the body before it sees the run. It
	// checks for the body itself before it retires one.
	if hasOrigin {
		if err := a.b.put(ctx, originKey, "", nil); err != nil {
			return fmt.Errorf("artifacts: archive run %q: write origin index %q: %w", rec.RunID, originKey, err)
		}
	}
	if hasRoot {
		if err := a.b.put(ctx, rootKey, "", nil); err != nil {
			return fmt.Errorf("artifacts: archive run %q: write root index %q: %w", rec.RunID, rootKey, err)
		}
	}
	if err := a.b.put(ctx, recentKey, "", nil); err != nil {
		return fmt.Errorf("artifacts: archive run %q: write recent index %q: %w", rec.RunID, recentKey, err)
	}
	if err := a.b.put(ctx, indexKey, "", nil); err != nil {
		return fmt.Errorf("artifacts: archive run %q: write index %q: %w", rec.RunID, indexKey, err)
	}
	// The body last: it is what says the archive is whole (HasRun). A body
	// already there is immutable.
	if !exists {
		if err := a.b.put(ctx, bodyKey, RunArchiveContentType, rec.JSONL); err != nil {
			return fmt.Errorf("artifacts: archive run %q: write body %q: %w", rec.RunID, bodyKey, err)
		}
	}
	return nil
}

// putObjectRefs writes every reference marker with bounded concurrency.
//
// One write per referenced object made archiving linear in a run's artifact
// count, and archiving happens inside a projection activity with a 10-second
// budget whose failure is only logged — so a wide fan-out run could quietly
// end up with no archive body at all, which is the only record of it once
// Temporal retention passes. Overlapping the round trips keeps that cost flat
// enough to stay inside the budget; it does not remove the linearity.
//
// Every marker is still written before PutRun's caller publishes the index
// key, and any single failure fails the whole call. A dropped marker would
// under-count references and let DeleteUnreferencedObjects delete an object
// another run still needs, so there is no partial-success path here.
func (a *BlobRunArchive) putObjectRefs(ctx context.Context, refKeys []string) error {
	if len(refKeys) == 0 {
		return nil
	}
	errs := make([]error, len(refKeys))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, archiveRefWriteConcurrency)
	)
	for i, refKey := range refKeys {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, refKey string) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := a.b.put(ctx, refKey, "", nil); err != nil {
				errs[i] = fmt.Errorf("write object reference %q: %w", refKey, err)
			}
		}(i, refKey)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// GetRun returns the archived JSONL body for runID.
func (a *BlobRunArchive) GetRun(ctx context.Context, runID string) ([]byte, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return nil, err
	}
	body, _, err := a.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, fmt.Errorf("artifacts: archive get %q: %w", runID, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: archive get %q: %w", runID, err)
	}
	return body, nil
}

// CleanupState returns a run's immutable cleanup sidecar.
func (a *BlobRunArchive) CleanupState(ctx context.Context, runID string) (RunCleanupState, error) {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return RunCleanupState{}, err
	}
	body, _, err := a.b.get(ctx, key, 0)
	if errors.Is(err, errBlobNotFound) {
		return RunCleanupState{}, fmt.Errorf("artifacts: cleanup state %q: %w", runID, ErrNotFound)
	}
	if err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: cleanup state %q: %w", runID, err)
	}
	var state RunCleanupState
	if err := json.Unmarshal(body, &state); err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: cleanup state %q: decode: %w", runID, err)
	}
	return state, nil
}

// MarkArtifactsCleaned writes cleanup state once, create-only: a sidecar
// already there is the winner and is kept.
func (a *BlobRunArchive) MarkArtifactsCleaned(ctx context.Context, runID string, deletedAt time.Time) error {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return err
	}
	body, err := stablejson.Marshal(RunCleanupState{RunID: runID, ArtifactsCleaned: true, TemporalDeletedAt: deletedAt.UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: mark cleanup %q: %w", runID, err)
	}
	if err := a.b.create(ctx, key, RunCleanupContentType, body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: mark cleanup %q: %w", runID, err)
	}
	return nil
}

// DeleteRunArtifacts removes run-owned logs, attempt records, commits,
// snapshots, and reference markers. Removing a marker only drops this run's
// claim: shared workload objects stay untouched until
// DeleteUnreferencedObjects observes that no claims remain.
func (a *BlobRunArchive) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (CleanupResult, error) {
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
	// The four prefixes are independent: delete them concurrently.
	prefixes := []string{logPrefix, attemptPrefix, commitPrefix, snapshotPrefix}
	results := make([]CleanupResult, len(prefixes))
	errs := make([]error, len(prefixes))
	var wg sync.WaitGroup
	for i, prefix := range prefixes {
		wg.Add(1)
		go func(i int, prefix string) {
			defer wg.Done()
			results[i], errs[i] = a.deletePrefix(ctx, runID, prefix)
		}(i, prefix)
	}
	wg.Wait()
	var result CleanupResult
	for _, r := range results {
		result.ObjectsDeleted += r.ObjectsDeleted
		result.BytesReclaimed += r.BytesReclaimed
	}
	if err := errors.Join(errs...); err != nil {
		return result, err
	}

	listKey, err := RunObjectListKey(runID)
	if err != nil {
		return result, err
	}
	listBody, _, err := a.b.get(ctx, listKey, 0)
	if errors.Is(err, errBlobNotFound) {
		// PutRun writes the object list before it publishes the index, so
		// an archived run with no list is a corrupt archive. Do nothing rather
		// than fall back to a namespace-wide scan, which would make every
		// cleanup unbounded.
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("artifacts: cleanup run %q read object list: %w", runID, err)
	}
	var objectIDs []string
	if err := json.Unmarshal(listBody, &objectIDs); err != nil {
		return result, fmt.Errorf("artifacts: cleanup run %q decode object list: %w", runID, err)
	}
	markers := make([]string, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		key, err := RunObjectRefKey(objectID, runID)
		if err != nil {
			return result, err
		}
		markers = append(markers, key)
	}
	for start := 0; start < len(markers); start += blobRemoveBatch {
		batch := markers[start:min(start+blobRemoveBatch, len(markers))]
		if failed, err := a.b.removeKeys(ctx, batch); err != nil || len(failed) > 0 {
			return result, fmt.Errorf("artifacts: cleanup run %q delete object references: %w", runID, errors.Join(err, joinDeleteFailures(failed)))
		}
	}
	if err := a.b.remove(ctx, listKey); err != nil {
		return result, fmt.Errorf("artifacts: cleanup run %q delete object list %q: %w", runID, listKey, err)
	}
	return result, nil
}

// deletePrefix empties one run-owned prefix: one batch removal per listing
// page. Bytes are counted from the listing, for the keys the store reports
// deleted. A key the store refused is reported, never counted, and the walk
// continues so one bad key does not hide the state of the rest.
func (a *BlobRunArchive) deletePrefix(ctx context.Context, runID, prefix string) (CleanupResult, error) {
	var (
		result CleanupResult
		errs   []error
	)
	var removeErr error
	err := listPages(ctx, a.b, blobListInput{Prefix: prefix}, func(page blobListPage) (bool, error) {
		keys := make([]string, 0, len(page.Objects))
		sizes := make(map[string]int64, len(page.Objects))
		for _, object := range page.Objects {
			keys = append(keys, object.Key)
			sizes[object.Key] = object.Size
		}
		for start := 0; start < len(keys); start += blobRemoveBatch {
			batch := keys[start:min(start+blobRemoveBatch, len(keys))]
			failed, err := a.b.removeKeys(ctx, batch)
			if err != nil {
				removeErr = fmt.Errorf("artifacts: cleanup run %q delete under %q: %w", runID, prefix, err)
				return true, nil
			}
			for _, key := range batch {
				if _, refused := failed[key]; refused {
					continue
				}
				result.ObjectsDeleted++
				result.BytesReclaimed += sizes[key]
			}
			if len(failed) > 0 {
				errs = append(errs, fmt.Errorf("artifacts: cleanup run %q delete under %q: %w", runID, prefix, joinDeleteFailures(failed)))
			}
		}
		return false, nil
	})
	if err != nil {
		errs = append(errs, fmt.Errorf("artifacts: cleanup run %q list %q: %w", runID, prefix, err))
	}
	if removeErr != nil {
		errs = append(errs, removeErr)
	}
	return result, errors.Join(errs...)
}

// joinDeleteFailures joins per-key refusals in key order, so a message is
// stable.
func joinDeleteFailures(failed map[string]error) error {
	if len(failed) == 0 {
		return nil
	}
	keys := make([]string, 0, len(failed))
	for key := range failed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	errs := make([]error, 0, len(keys))
	for _, key := range keys {
		errs = append(errs, failed[key])
	}
	return errors.Join(errs...)
}

// canonicalObjectID reports whether objectID names a content-addressed object
// of this backend's artifact store, and the S3 key segments when it is one of
// S3Store's. A bucket accepts both layouts; a local directory only its own.
func (a *BlobRunArchive) canonicalObjectID(objectID string) (s3Segments []string, ok bool) {
	_, _, localOK := splitKey(objectID)
	if a.local {
		return nil, localOK
	}
	s3Segments, s3OK := splitS3ObjectKey(objectID)
	return s3Segments, localOK || s3OK
}

// objectCompanion is the record beside a content-addressed object that goes
// with it: LocalStore's sidecar, or S3Store's name manifest for an object
// whose name had to be sanitized. "" when there is none.
func (a *BlobRunArchive) objectCompanion(objectID string, s3Segments []string) string {
	if a.local {
		return metadataKey(objectID)
	}
	if s3Segments != nil && !isCleanS3Component(s3Segments[len(s3Segments)-2]) {
		return s3ManifestKey(objectID)
	}
	return ""
}

// ListObjectReferences pages one object's marker prefix — the prefix
// DeleteUnreferencedObjects probes with MaxKeys 1 — to the end or until the
// bound is exceeded. It reads keys only: no marker (they are zero bytes) and
// no run body is fetched. A prefix with nothing under it is an object nothing
// references: an empty list.
func (a *BlobRunArchive) ListObjectReferences(ctx context.Context, objectID string) ([]string, error) {
	if _, ok := a.canonicalObjectID(objectID); !ok {
		return nil, fmt.Errorf("%w: %q is not a canonical object id", ErrInvalidURI, objectID)
	}
	collector := newObjectReferenceCollector()
	err := listObjects(ctx, a.b, blobListInput{Prefix: runObjectRefPrefix(objectID)}, func(object blobObject) (bool, error) {
		return false, collector.add(object.Key)
	})
	if err != nil {
		if errors.Is(err, ErrTooManyReferences) {
			return nil, err
		}
		return nil, fmt.Errorf("artifacts: list references of %q: %w", objectID, err)
	}
	return collector.runIDs, nil
}

// DeleteUnreferencedObjects removes content-addressed objects only after
// their reference prefix is empty. Missing objects make retries a no-op.
func (a *BlobRunArchive) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (CleanupResult, error) {
	var result CleanupResult
	var errs []error
	seen := make(map[string]struct{}, len(objectIDs))
	for _, objectID := range objectIDs {
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		s3Segments, ok := a.canonicalObjectID(objectID)
		if !ok {
			errs = append(errs, fmt.Errorf("object %q: not a canonical object id", objectID))
			continue
		}
		page, err := a.b.list(ctx, blobListInput{Prefix: runObjectRefPrefix(objectID), MaxKeys: 1})
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: list references: %w", objectID, err))
			continue
		}
		if len(page.Objects) != 0 {
			continue
		}
		size, err := a.b.head(ctx, objectID)
		if errors.Is(err, errBlobNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: stat: %w", objectID, err))
			continue
		}
		if err := a.b.remove(ctx, objectID); err != nil {
			errs = append(errs, fmt.Errorf("object %q: delete: %w", objectID, err))
			continue
		}
		result.ObjectsDeleted++
		result.BytesReclaimed += size
		if companion := a.objectCompanion(objectID, s3Segments); companion != "" {
			if err := a.b.remove(ctx, companion); err != nil {
				errs = append(errs, fmt.Errorf("object %q: delete %s: %w", objectID, a.companionName(), err))
				continue
			}
		}
	}
	return result, errors.Join(errs...)
}

func (a *BlobRunArchive) companionName() string {
	if a.local {
		return "metadata"
	}
	return "name manifest"
}

// ListRuns answers a query with one prefix scan. Nothing but keys crosses the
// wire: every filtered field lives in the key itself.
//
// A KeyOrder query resumes at StartAfterKey and returns as soon as the page is
// full, so paging a whole traversal costs one enumeration rather than one per
// page. The listing is already in key order, so the cursor is just
// StartAfter.
func (a *BlobRunArchive) ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	if q.usesRecent() {
		return a.listRecent(ctx, q)
	}
	prefix, startAfter, err := q.ScanPrefix()
	if err != nil {
		return nil, err
	}
	if q.KeyOrder && q.StartAfterKey != "" {
		startAfter = q.StartAfterKey
	}
	stopAfterUntil := q.timeOrderedPrefix() && !q.Until.IsZero()
	stopKey := prefix + RunStamp(q.Until)

	entries := make([]RunIndexEntry, 0, q.pageCapacity())
	full := false
	err = listObjects(ctx, a.b, blobListInput{Prefix: prefix, StartAfter: startAfter}, func(object blobObject) (bool, error) {
		if stopAfterUntil && object.Key >= stopKey {
			return true, nil
		}
		a.indexKeysScanned.Add(1)
		entry, ok := q.parseKey(object.Key)
		if !ok || !q.Matches(entry) {
			return false, nil
		}
		entries = append(entries, entry)
		full = q.KeyOrder && q.Limit > 0 && len(entries) >= q.Limit
		return full, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: archive list %q: %w", prefix, err)
	}
	if q.KeyOrder {
		return entries, nil
	}
	return q.finish(entries), nil
}

// listRecent walks the newest-first family from the cursor and stops at the
// page (recentWalk). The listing is in key order, so a page is consumed in the
// order the walk needs, and the walk usually ends inside the first page.
func (a *BlobRunArchive) listRecent(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	prefix, startAfter, err := q.recentScan()
	if err != nil {
		return nil, err
	}
	walk := newRecentWalk(q)
	err = listObjects(ctx, a.b, blobListInput{Prefix: prefix, StartAfter: startAfter}, func(object blobObject) (bool, error) {
		a.indexKeysScanned.Add(1)
		return walk.add(object.Key), nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: archive list %q: %w", prefix, err)
	}
	return walk.page(), nil
}

// IndexKeysScanned reports how many index keys this archive has enumerated
// across every ListRuns call. It is the instrument behind the cleanup worker's
// scan-cost contract: a bounded page is only bounded if the backend stops at
// the page boundary, and elapsed time cannot tell the difference on a warm
// cache. Tests assert on it; nothing in the running system reads it.
func (a *BlobRunArchive) IndexKeysScanned() int64 { return a.indexKeysScanned.Load() }

// HasRun reports whether runID has an archive body, without reading it.
// PutRun writes the body last, so a body means the run's archive is whole.
func (a *BlobRunArchive) HasRun(ctx context.Context, runID string) (bool, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return false, err
	}
	exists, err := a.exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("artifacts: archive has %q: %w", runID, err)
	}
	return exists, nil
}

func (a *BlobRunArchive) exists(ctx context.Context, key string) (bool, error) {
	if _, err := a.b.head(ctx, key); err != nil {
		if errors.Is(err, errBlobNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("head %q: %w", key, err)
	}
	return true, nil
}
