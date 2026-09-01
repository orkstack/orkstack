package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

// The run archive is the queryable record of a finished run. Temporal stays the
// source of truth while a run executes; the archive is what survives an engine
// restart and what answers "which runs of this orkflow succeeded last week".
//
// Two object families, both under one prefix:
//
//	metadata/runs/{run-id}.jsonl                                  the immutable body
//	metadata/index/runs/{namespace}/{orkflow}/{status}/{stamp}/{run-id}
//
// The body holds the run and every one of its events, one JSON record per line.
// It is written once, when the run reaches a terminal state, and never
// rewritten.
//
// The index holds no bytes at all: the *key* carries every field a query
// filters on, so listing by namespace, orkflow, status and time range is one
// prefix scan that reads keys only. Segment order matches how queries narrow —
// namespace, then orkflow, then status, then a lexicographically sortable UTC
// timestamp — so a query that pins the first segments scans only its own
// subtree, and a time range starts the scan at its lower bound instead of at
// the first key. Cleanup state is a separate immutable sidecar, so cleanup
// never rewrites run history.
const (
	// RunArchiveNamespace prefixes every archived run body.
	RunArchiveNamespace = "metadata/runs"
	// RunIndexNamespace prefixes every query-index key.
	RunIndexNamespace = "metadata/index/runs"
	// RunCleanupNamespace prefixes immutable cleanup-state sidecars.
	RunCleanupNamespace = "metadata/cleanup/runs"
	// RunObjectRefNamespace prefixes reference markers connecting archived runs
	// to the physical content-addressed objects their completions reference.
	RunObjectRefNamespace = "metadata/index/objects"
	// RunObjectListNamespace prefixes the directly addressable object-id list
	// used to remove one run's markers without scanning every stored reference.
	RunObjectListNamespace = "metadata/index/run-objects"
	// RunArchiveSuffix is the body's extension: JSON Lines.
	RunArchiveSuffix = ".jsonl"
	// RunArchiveContentType is the media type recorded for a body.
	RunArchiveContentType = "application/x-ndjson"
	// RunCleanupContentType is the media type recorded for cleanup state.
	RunCleanupContentType = "application/json"
	// RunStampLayout renders an index timestamp: UTC, fixed width, and ordered
	// the same lexicographically as chronologically.
	RunStampLayout = "20060102T150405.000000000Z"
	// runIndexSegments is the fixed segment count of an index key:
	// metadata, index, runs, namespace, orkflow, status, stamp, run-id.
	runIndexSegments = 8
	// archiveRefWriteConcurrency bounds the parallel reference-marker writes
	// one PutRun issues. It matches the engine's archiveFetchConcurrency: the
	// same shape of problem, so the same number rather than a second knob.
	archiveRefWriteConcurrency = 8
	// archiveMaxComponent bounds one generated segment. Orkflow names and task
	// ids are DSL slugs and run ids are ork-{timestamp}-{sequence}, so a real
	// segment is far shorter; the bound only rejects abuse.
	archiveMaxComponent = 200
	// archiveEncodedMarker introduces a segment that is not already a clean S3
	// component. A clean component cannot start with it, so the two forms never
	// collide and decoding is exact.
	archiveEncodedMarker = "~"
)

// ErrInvalidRunKey reports a run coordinate that cannot be addressed.
var ErrInvalidRunKey = errors.New("invalid run archive key")

// RunRecord is one archived run: the JSONL body plus the coordinates the index
// key is built from. The body's encoding belongs to the caller (the engine
// marshals protobuf with protojson); this package treats it as opaque bytes.
type RunRecord struct {
	// RunID identifies the run, e.g. "ork-1786856120616785000-1".
	RunID string
	// Namespace is the OrkStack namespace the run's orkflow belongs to. It is
	// not a Temporal namespace. Empty means DefaultNamespace, resolved where
	// the key is built.
	Namespace string
	// Orkflow is the orkflow name the run executed.
	Orkflow string
	// Status is the run's terminal status, lowercase and unprefixed
	// ("succeeded", "failed", "canceled").
	Status string
	// Time orders the run in the index. The engine passes the run's creation
	// time, so a date range means "runs created in this window".
	Time time.Time
	// ObjectIDs are the physical content-addressed object keys referenced by
	// this completion. The whole key is the identity, not only its terminal
	// digest: equal bytes stored under different logical names are distinct
	// physical objects.
	ObjectIDs []string
	// JSONL is the archive body: one JSON record per line.
	JSONL []byte
}

// RunCleanupState records when Temporal retention removed a run and OrkStack
// reclaimed its run-scoped storage. It is separate from the immutable body.
type RunCleanupState struct {
	RunID             string    `json:"run_id"`
	ArtifactsCleaned  bool      `json:"artifacts_cleaned"`
	TemporalDeletedAt time.Time `json:"temporal_deleted_at"`
}

// CleanupResult reports storage reclaimed for one run.
type CleanupResult struct {
	ObjectsDeleted int
	BytesReclaimed int64
}

// RunIndexEntry is one row of a prefix scan. Cleanup state is returned by
// RunArchive.CleanupState without changing the immutable index.
type RunIndexEntry struct {
	RunID     string
	Namespace string
	Orkflow   string
	Status    string
	Time      time.Time
}

// RunQuery narrows a prefix scan. Zero values mean "no filter".
type RunQuery struct {
	// Namespace selects one namespace. It is the outermost segment, so
	// selecting one is a narrower scan rather than a filter over every
	// namespace's keys.
	Namespace string
	// Orkflow selects one orkflow by name. It narrows only within a
	// namespace: an orkflow name is unique inside one, not across the
	// deployment.
	Orkflow string
	// Status selects one status; matching is case-insensitive and accepts the
	// short form ("succeeded").
	Status string
	// Since is the inclusive lower bound on RunRecord.Time.
	Since time.Time
	// Until is the exclusive upper bound on RunRecord.Time.
	Until time.Time
	// KeyOrder switches the scan from "newest first, then truncate" to a
	// resumable walk in index-key order that stops as soon as Limit matching
	// entries are collected.
	//
	// It exists because Limit alone cannot make a page cheap. The default
	// ordering is global and newest-first, but the stamp is the fourth key
	// segment, so producing it requires enumerating every key under
	// metadata/index/runs before sorting. A caller that only needs to visit
	// every matching run once — cleanup is the only one — pays that full
	// enumeration per page instead of per traversal. Walking in key order
	// lets the backend stop at the page boundary, so a whole traversal costs
	// one enumeration no matter how many pages it takes.
	//
	// Entries come back in key order, not newest first, and finish() does not
	// run. Resume by rebuilding the last entry's key with RunIndexKey.
	KeyOrder bool
	// StartAfterKey is the exclusive index-key cursor for a KeyOrder walk. It
	// is ignored unless KeyOrder is set.
	StartAfterKey string
	// Limit caps the number of entries returned, newest first. 0 means all.
	Limit int
}

// RunArchive stores and queries archived runs. Implementations are immutable:
// a run body is written once and never rewritten, so re-archiving the same run
// is a no-op rather than a corruption.
type RunArchive interface {
	// PutRun writes the record's body and its index key. Writing a run that is
	// already archived leaves the body untouched and returns nil.
	PutRun(ctx context.Context, rec RunRecord) error
	// GetRun returns the JSONL body archived for runID. A run that was never
	// archived returns an error matching ErrNotFound.
	GetRun(ctx context.Context, runID string) ([]byte, error)
	// ListRuns scans the index and returns matching entries, newest first.
	ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error)
	// CleanupState returns the immutable cleanup sidecar. A run that has not
	// been cleaned returns an error matching ErrNotFound.
	CleanupState(ctx context.Context, runID string) (RunCleanupState, error)
	// DeleteRunArtifacts removes run-scoped logs, completion commits, and this
	// run's object-reference markers. It never deletes shared workload objects;
	// callers use DeleteUnreferencedObjects after all run references are gone.
	DeleteRunArtifacts(ctx context.Context, namespace, runID string) (CleanupResult, error)
	// DeleteUnreferencedObjects removes physical objects with no run-reference
	// markers. Missing objects are already clean and do not count as deletions.
	DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (CleanupResult, error)
	// MarkArtifactsCleaned writes cleanup state once. Re-marking is a no-op.
	MarkArtifactsCleaned(ctx context.Context, runID string, temporalDeletedAt time.Time) error
}

// RunCleanupKey renders the immutable cleanup-state key for a run id.
func RunCleanupKey(runID string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run cleanup key: run id: %w", err)
	}
	return path.Join(RunCleanupNamespace, segment) + ".json", nil
}

// RunLogPrefix renders the prefix containing every log chunk for a run.
func RunLogPrefix(runID string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run log prefix: run id: %w", err)
	}
	return path.Join(LogPrefix, segment) + "/", nil
}

// RunCommitPrefix renders the prefix containing every commit for a run.
func RunCommitPrefix(namespace, runID string) (string, error) {
	namespaceSegment, err := encodeArchiveSegment(ResolveNamespace(namespace))
	if err != nil {
		return "", fmt.Errorf("artifacts: run commit prefix: namespace: %w", err)
	}
	runSegment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run commit prefix: run id: %w", err)
	}
	return path.Join(CommitPrefix, namespaceSegment, runSegment) + "/", nil
}

// RunObjectRefKey renders the zero-byte marker connecting one physical object
// key to one archived run. The object segment is the SHA-256 of the complete
// object id: this index is a refcount, not a directory of object ids, and an
// object key is unbounded while one storage-key component is not. The fixed
// digest keeps every canonical object addressable regardless of name length.
func RunObjectRefKey(objectID, runID string) (string, error) {
	runSegment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run object reference key: run id: %w", err)
	}
	return path.Join(RunObjectRefNamespace, objectRefDigest(objectID), runSegment), nil
}

// ParseRunObjectRefRunID returns the exact run id carried by a reference
// marker. The object segment is deliberately one-way and is validated as a
// lowercase SHA-256 rather than presented as a decodable object id.
func ParseRunObjectRefRunID(key string) (runID string, ok bool) {
	prefix := RunObjectRefNamespace + "/"
	parts := strings.Split(key, "/")
	if len(parts) != 5 || !strings.HasPrefix(key, prefix) || !digestPattern.MatchString(parts[3]) {
		return "", false
	}
	runID, ok = decodeArchiveSegment(parts[4])
	return runID, ok
}

func runObjectRefPrefix(objectID string) string {
	return path.Join(RunObjectRefNamespace, objectRefDigest(objectID)) + "/"
}

func objectRefDigest(objectID string) string {
	digest := sha256.Sum256([]byte(objectID))
	return hex.EncodeToString(digest[:])
}

// RunObjectListKey renders the JSON object-id list owned by one archived run.
func RunObjectListKey(runID string) (string, error) {
	runSegment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run object list key: run id: %w", err)
	}
	return path.Join(RunObjectListNamespace, runSegment) + ".json", nil
}

func dedupeObjectIDs(objectIDs []string) []string {
	unique := make([]string, 0, len(objectIDs))
	seen := make(map[string]struct{}, len(objectIDs))
	for _, objectID := range objectIDs {
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		unique = append(unique, objectID)
	}
	return unique
}

// RunArchiveKey renders the body key for a run id.
func RunArchiveKey(runID string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run archive key: run id: %w", err)
	}
	return RunArchiveNamespace + "/" + segment + RunArchiveSuffix, nil
}

// RunIndexKey renders the index key for a record.
func RunIndexKey(rec RunRecord) (string, error) {
	namespace, err := encodeArchiveSegment(ResolveNamespace(rec.Namespace))
	if err != nil {
		return "", fmt.Errorf("artifacts: run index key: namespace: %w", err)
	}
	orkflow, err := encodeArchiveSegment(rec.Orkflow)
	if err != nil {
		return "", fmt.Errorf("artifacts: run index key: orkflow: %w", err)
	}
	status, err := encodeArchiveSegment(NormalizeRunStatus(rec.Status))
	if err != nil {
		return "", fmt.Errorf("artifacts: run index key: status: %w", err)
	}
	runID, err := encodeArchiveSegment(rec.RunID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run index key: run id: %w", err)
	}
	return path.Join(RunIndexNamespace, namespace, orkflow, status,
		RunStamp(rec.Time), runID), nil
}

// RunIndexKeyForEntry renders the index key a scanned entry came from. It is
// how a KeyOrder caller turns the last entry of a page into the exclusive
// cursor for the next one, without inventing a second key convention.
func RunIndexKeyForEntry(entry RunIndexEntry) (string, error) {
	return RunIndexKey(RunRecord{
		RunID:     entry.RunID,
		Namespace: entry.Namespace,
		Orkflow:   entry.Orkflow,
		Status:    entry.Status,
		Time:      entry.Time,
	})
}

// ParseRunIndexKey reverses RunIndexKey. A key this package did not generate
// reports ok=false rather than an error, so a scan can skip foreign objects.
func ParseRunIndexKey(key string) (RunIndexEntry, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != runIndexSegments || !strings.HasPrefix(key, RunIndexNamespace+"/") {
		return RunIndexEntry{}, false
	}
	namespace, ok := decodeArchiveSegment(parts[3])
	if !ok {
		return RunIndexEntry{}, false
	}
	orkflow, ok := decodeArchiveSegment(parts[4])
	if !ok {
		return RunIndexEntry{}, false
	}
	status, ok := decodeArchiveSegment(parts[5])
	if !ok {
		return RunIndexEntry{}, false
	}
	stamp, err := time.Parse(RunStampLayout, parts[6])
	if err != nil {
		return RunIndexEntry{}, false
	}
	runID, ok := decodeArchiveSegment(parts[7])
	if !ok {
		return RunIndexEntry{}, false
	}
	return RunIndexEntry{
		RunID:     runID,
		Namespace: namespace,
		Orkflow:   orkflow,
		Status:    status,
		Time:      stamp,
	}, true
}

// RunStamp renders t as an index segment: UTC, fixed width, sortable.
func RunStamp(t time.Time) string { return t.UTC().Format(RunStampLayout) }

// NormalizeRunStatus folds a status to the form the index stores. It accepts
// the proto wire name ("STATUS_SUCCEEDED") and the short form, in any case.
func NormalizeRunStatus(raw string) string {
	status := strings.ToLower(strings.TrimSpace(raw))
	return strings.TrimPrefix(status, "status_")
}

// ScanPrefix is the narrowest index prefix that still contains every key q can
// match, and the key to start the scan after. Narrowing follows the key's own
// order — namespace, orkflow, status — and pinning all three makes the
// remaining segments start with the sortable timestamp, so a lower time bound
// skips everything older without reading it.
//
// A query that names an orkflow but no namespace cannot narrow past the root:
// an orkflow name is unique within a namespace, so the same name may exist in
// several, and the scan must see them all and filter with Matches.
func (q RunQuery) ScanPrefix() (prefix, startAfter string, err error) {
	prefix = RunIndexNamespace + "/"
	if q.Namespace == "" {
		return prefix, "", nil
	}
	namespace, err := encodeArchiveSegment(ResolveNamespace(q.Namespace))
	if err != nil {
		return "", "", fmt.Errorf("artifacts: run query: namespace: %w", err)
	}
	prefix += namespace + "/"
	if q.Orkflow == "" {
		return prefix, "", nil
	}
	orkflow, err := encodeArchiveSegment(q.Orkflow)
	if err != nil {
		return "", "", fmt.Errorf("artifacts: run query: orkflow: %w", err)
	}
	prefix += orkflow + "/"
	if q.Status == "" {
		return prefix, "", nil
	}
	status, err := encodeArchiveSegment(NormalizeRunStatus(q.Status))
	if err != nil {
		return "", "", fmt.Errorf("artifacts: run query: status: %w", err)
	}
	prefix += status + "/"
	if q.Since.IsZero() {
		return prefix, "", nil
	}
	// Keys are prefix+stamp+"/"+run-id, so every key at exactly Since sorts
	// after prefix+stamp; an exclusive start-after still includes them.
	return prefix, prefix + RunStamp(q.Since), nil
}

// Matches reports whether an entry satisfies every filter in q.
func (q RunQuery) Matches(entry RunIndexEntry) bool {
	if q.Namespace != "" && entry.Namespace != ResolveNamespace(q.Namespace) {
		return false
	}
	if q.Orkflow != "" && entry.Orkflow != q.Orkflow {
		return false
	}
	if q.Status != "" && entry.Status != NormalizeRunStatus(q.Status) {
		return false
	}
	if !q.Since.IsZero() && entry.Time.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && !entry.Time.Before(q.Until) {
		return false
	}
	return true
}

// afterCursor reports whether an index key sorts strictly after the KeyOrder
// cursor, which is what makes a resumed page exclusive of the previous one.
func (q RunQuery) afterCursor(key string) bool {
	return q.StartAfterKey == "" || key > q.StartAfterKey
}

// prunesDir reports whether every key beneath dir sorts at or before the
// KeyOrder cursor, so a resumed walk can skip the whole subtree.
//
// The comparison is against dir+"/", never dir, because dir+"/" is what every
// key beneath it actually starts with. The distinction is load-bearing rather
// than pedantic: '-' (0x2D) sorts below '/' (0x2F), so for sibling orkflows
// "f" and "f-g" the plain name "f" sorts before the cursor ".../f-g/..." while
// every key under it — ".../f/..." — sorts after. Pruning on the name alone
// therefore skipped a whole subtree of runs that had not been visited yet, and
// they would never have been cleaned.
//
// If the cursor lies inside dir's own subtree the walk must descend, because
// deeper keys may still sort after it. Otherwise the subtree is wholly on one
// side of the cursor, and dir+"/" decides which.
func (q RunQuery) prunesDir(dir string) bool {
	if q.StartAfterKey == "" {
		return false
	}
	subtree := dir + "/"
	if strings.HasPrefix(q.StartAfterKey, subtree) {
		return false
	}
	return q.StartAfterKey > subtree
}

// finish orders scanned entries newest first and applies the query limit.
// Equal timestamps fall back to the run id, so results are deterministic.
func (q RunQuery) finish(entries []RunIndexEntry) []RunIndexEntry {
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].Time.Equal(entries[j].Time) {
			return entries[i].Time.After(entries[j].Time)
		}
		return entries[i].RunID > entries[j].RunID
	})
	if q.Limit > 0 && len(entries) > q.Limit {
		entries = entries[:q.Limit]
	}
	return entries
}

// encodeArchiveSegment renders one coordinate as a single key segment. A value
// that is already a clean, bounded S3 component is used verbatim — which every
// real orkflow name, status and run id is — and anything else is escaped
// reversibly, so decoding always returns the exact original bytes.
func encodeArchiveSegment(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%w: empty segment", ErrInvalidRunKey)
	}
	if isCleanArchiveComponent(raw) {
		return raw, nil
	}
	encoded := archiveEncodedMarker + base64.RawURLEncoding.EncodeToString([]byte(raw))
	if len(encoded) > archiveMaxComponent {
		return "", fmt.Errorf("%w: segment is %d bytes, limit is %d",
			ErrInvalidRunKey, len(raw), archiveMaxComponent)
	}
	return encoded, nil
}

func decodeArchiveSegment(segment string) (string, bool) {
	if segment == "" {
		return "", false
	}
	if !strings.HasPrefix(segment, archiveEncodedMarker) {
		if !isCleanArchiveComponent(segment) {
			return "", false
		}
		return segment, true
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(segment, archiveEncodedMarker))
	if err != nil || len(decoded) == 0 {
		return "", false
	}
	return string(decoded), true
}

// isCleanArchiveComponent reuses the object layout's readable-segment rule and
// only widens its length budget: an index segment carries a name, not a
// four-deep hierarchy, so it can afford more bytes.
func isCleanArchiveComponent(raw string) bool {
	return len(raw) <= archiveMaxComponent && cleanComponentPattern.MatchString(raw)
}
