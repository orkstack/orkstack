package artifacts

import (
	"context"
	"encoding/base64"
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
// Two object families, both under one namespace:
//
//	metadata/runs/{run-id}.jsonl                              the immutable body
//	metadata/index/runs/{orkflow}/{status}/{stamp}/{run-id}    the query index
//
// The body holds the run and every one of its events, one JSON record per line.
// It is written once, when the run reaches a terminal state, and never
// rewritten.
//
// The index holds no bytes at all: the *key* carries every field a query
// filters on, so listing by orkflow, status and time range is one prefix scan
// that reads keys only. Segment order matches how queries narrow — orkflow,
// then status, then a lexicographically sortable UTC timestamp — so a query
// that pins the first segments scans only its own subtree, and a time range
// starts the scan at its lower bound instead of at the first key.
const (
	// RunArchiveNamespace prefixes every archived run body.
	RunArchiveNamespace = "metadata/runs"
	// RunIndexNamespace prefixes every query-index key.
	RunIndexNamespace = "metadata/index/runs"
	// RunArchiveSuffix is the body's extension: JSON Lines.
	RunArchiveSuffix = ".jsonl"
	// RunArchiveContentType is the media type recorded for a body.
	RunArchiveContentType = "application/x-ndjson"
	// RunStampLayout renders an index timestamp: UTC, fixed width, and ordered
	// the same lexicographically as chronologically.
	RunStampLayout = "20060102T150405.000000000Z"
	// runIndexSegments is the fixed segment count of an index key:
	// metadata, index, runs, orkflow, status, stamp, run-id.
	runIndexSegments = 7
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
	// Orkflow is the orkflow name the run executed.
	Orkflow string
	// Status is the run's terminal status, lowercase and unprefixed
	// ("succeeded", "failed", "canceled").
	Status string
	// Time orders the run in the index. The engine passes the run's creation
	// time, so a date range means "runs created in this window".
	Time time.Time
	// JSONL is the archive body: one JSON record per line.
	JSONL []byte
}

// RunIndexEntry is one row of a prefix scan. It is built from the index key
// alone, so a query answers without reading a single object body.
type RunIndexEntry struct {
	RunID   string
	Orkflow string
	Status  string
	Time    time.Time
}

// RunQuery narrows a prefix scan. Zero values mean "no filter".
type RunQuery struct {
	// Orkflow selects one orkflow by name.
	Orkflow string
	// Status selects one status; matching is case-insensitive and accepts the
	// short form ("succeeded").
	Status string
	// Since is the inclusive lower bound on RunRecord.Time.
	Since time.Time
	// Until is the exclusive upper bound on RunRecord.Time.
	Until time.Time
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
	return path.Join(RunIndexNamespace, orkflow, status, RunStamp(rec.Time), runID), nil
}

// ParseRunIndexKey reverses RunIndexKey. A key this package did not generate
// reports ok=false rather than an error, so a scan can skip foreign objects.
func ParseRunIndexKey(key string) (RunIndexEntry, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != runIndexSegments || !strings.HasPrefix(key, RunIndexNamespace+"/") {
		return RunIndexEntry{}, false
	}
	orkflow, ok := decodeArchiveSegment(parts[3])
	if !ok {
		return RunIndexEntry{}, false
	}
	status, ok := decodeArchiveSegment(parts[4])
	if !ok {
		return RunIndexEntry{}, false
	}
	stamp, err := time.Parse(RunStampLayout, parts[5])
	if err != nil {
		return RunIndexEntry{}, false
	}
	runID, ok := decodeArchiveSegment(parts[6])
	if !ok {
		return RunIndexEntry{}, false
	}
	return RunIndexEntry{RunID: runID, Orkflow: orkflow, Status: status, Time: stamp}, true
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
// match, and the key to start the scan after. Pinning orkflow and status makes
// the remaining segments start with the sortable timestamp, so a lower time
// bound skips everything older without reading it.
func (q RunQuery) ScanPrefix() (prefix, startAfter string, err error) {
	prefix = RunIndexNamespace + "/"
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
