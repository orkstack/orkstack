package artifacts

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A third zero-byte family indexes the same runs NEWEST FIRST:
//
//	metadata/index/recent/{namespace}/{inverted-stamp}/{orkflow}/{status}/{run-id}
//
// The run index cannot answer "the newest page of this namespace" without
// reading every key: its stamp is the fourth segment, below orkflow and
// status, so key order is orkflow, then status, then time. This family puts
// time first, and inverts it — each digit d of RunStamp becomes 9-d — so
// ascending key order IS newest-first time order. A list page walks it from
// the cursor and stops once the page is full and the boundary instant's ties
// are collected, so a page costs the page rather than the namespace.
//
// It is written beside the run-index row in PutRun, before it, so the run
// index stays the last key published. It is never deleted, like the run
// index. A run archived before this family existed has no row in it, and is
// readable by id but absent from a default list (there is no backfill:
// OrkStack has no deployment to stay compatible with).
const (
	// RunRecentIndexNamespace prefixes the newest-first index.
	RunRecentIndexNamespace = "metadata/index/recent"
	// runRecentIndexSegments is the fixed segment count of a recent-index
	// key: metadata, index, recent, namespace, inverted-stamp, orkflow,
	// status, run-id.
	runRecentIndexSegments = 8
)

// invertStamp maps each decimal digit d of a RunStamp to 9-d and keeps every
// other byte, so for two stamps of the same fixed width the inverted forms
// sort in the opposite order. It is its own inverse.
func invertStamp(stamp string) string {
	out := []byte(stamp)
	for i, c := range out {
		if c >= '0' && c <= '9' {
			out[i] = '9' - (c - '0')
		}
	}
	return string(out)
}

// RunRecentIndexKey renders the newest-first index key for a record. The
// orkflow, status and run id are rendered exactly as RunIndexKey renders them.
func RunRecentIndexKey(rec RunRecord) (string, error) {
	namespace, err := encodeArchiveSegment(ResolveNamespace(rec.Namespace))
	if err != nil {
		return "", fmt.Errorf("artifacts: run recent index key: namespace: %w", err)
	}
	orkflow, err := encodeArchiveSegment(rec.Orkflow)
	if err != nil {
		return "", fmt.Errorf("artifacts: run recent index key: orkflow: %w", err)
	}
	status, err := encodeArchiveSegment(NormalizeRunStatus(rec.Status))
	if err != nil {
		return "", fmt.Errorf("artifacts: run recent index key: status: %w", err)
	}
	runID, err := encodeArchiveSegment(rec.RunID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run recent index key: run id: %w", err)
	}
	return path.Join(RunRecentIndexNamespace, namespace, invertStamp(RunStamp(rec.Time)),
		orkflow, status, runID), nil
}

// RunRecentIndexKeyForEntry renders the recent key a scanned entry came from
// verbatim. It is the exact cursor for the next newest-first page: an instant
// prefix is not, because some S3 implementations start after the whole
// prefix and skip the instant's remaining ties.
func RunRecentIndexKeyForEntry(entry RunIndexEntry) (string, error) {
	return RunRecentIndexKey(RunRecord{
		RunID: entry.RunID, Namespace: entry.Namespace,
		Orkflow: entry.Orkflow, Status: entry.Status, Time: entry.Time,
	})
}

// ParseRunRecentIndexKey reverses RunRecentIndexKey; a foreign key is
// ok=false, so a scan can skip it.
func ParseRunRecentIndexKey(key string) (RunIndexEntry, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != runRecentIndexSegments || !strings.HasPrefix(key, RunRecentIndexNamespace+"/") {
		return RunIndexEntry{}, false
	}
	namespace, ok := decodeArchiveSegment(parts[3])
	if !ok {
		return RunIndexEntry{}, false
	}
	stamp, ok := parseRunStamp(invertStamp(parts[4]))
	if !ok {
		return RunIndexEntry{}, false
	}
	orkflow, ok := decodeArchiveSegment(parts[5])
	if !ok {
		return RunIndexEntry{}, false
	}
	status, ok := decodeArchiveSegment(parts[6])
	if !ok {
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

// usesRecent reports whether a query is answered from the newest-first
// family. It is when the caller asked (Recent), the answer is a bounded
// newest-first page of one namespace, and no narrower family serves it
// better: an origin filter keeps the origin family, a query pinning both
// orkflow and status keeps that run-index subtree (already time-ordered), and
// an unbounded query reads its narrowest subtree whole either way.
func (q RunQuery) usesRecent() bool {
	return q.Recent && !q.KeyOrder && q.Limit > 0 && q.Namespace != "" &&
		q.OriginKind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && q.OriginID == "" &&
		(q.Orkflow == "" || q.Status == "")
}

// recentScan is the prefix a newest-first walk lists and the key it starts
// after: the later of the Until bound and the BeforeTime cursor, each as its
// inverted stamp. Every key AT that instant sorts after it, so the instant's
// ties are walked and Matches decides them (exclusive Until, id-before
// cursor); every key newer than it is skipped unread.
func (q RunQuery) recentScan() (prefix, startAfter string, err error) {
	namespace, err := encodeArchiveSegment(ResolveNamespace(q.Namespace))
	if err != nil {
		return "", "", fmt.Errorf("artifacts: run query: namespace: %w", err)
	}
	prefix = RunRecentIndexNamespace + "/" + namespace + "/"
	if q.BeforeKey != "" && strings.HasPrefix(q.BeforeKey, prefix) && q.BeforeKey > startAfter {
		startAfter = q.BeforeKey
	}
	for _, bound := range []time.Time{q.Until, q.BeforeTime} {
		if bound.IsZero() {
			continue
		}
		if candidate := prefix + invertStamp(RunStamp(bound)); candidate > startAfter {
			startAfter = candidate
		}
	}
	return prefix, startAfter, nil
}

// recentWalk collects one newest-first page from keys handed to it in key
// order. It stops once the page holds Limit entries AND the instant of the
// last one has been read whole — its ties are ordered by orkflow, status and
// run id in the key, not by run id alone, so the page is only cut after
// finish sorts the whole tie group — and as soon as a key is older than
// Since, since every key after it is older still.
type recentWalk struct {
	q       RunQuery
	entries []RunIndexEntry
	stamp   string // the inverted-stamp segment of the instant being read
}

func newRecentWalk(q RunQuery) *recentWalk {
	return &recentWalk{q: q, entries: make([]RunIndexEntry, 0, q.pageCapacity())}
}

// add takes the next key in key order and reports whether the walk is done.
// A key that reports done is not part of the page.
func (w *recentWalk) add(key string) (done bool) {
	entry, ok := ParseRunRecentIndexKey(key)
	if !ok {
		return false
	}
	stamp := strings.Split(key, "/")[4]
	if stamp != w.stamp {
		if len(w.entries) >= w.q.Limit {
			return true
		}
		w.stamp = stamp
	}
	if !w.q.Since.IsZero() && entry.Time.Before(w.q.Since) {
		return true
	}
	if w.q.Matches(entry) {
		w.entries = append(w.entries, entry)
	}
	return false
}

// page orders the collected entries newest first, ties by run id descending.
// It does not cut at Limit: add() stops only after a whole instant has been
// read, so a page may exceed Limit by the boundary instant's ties. Cutting
// them would lose runs the next page cannot resume into, because a store's
// StartAfter is an instant prefix at best and may be a directory boundary.
func (w *recentWalk) page() []RunIndexEntry {
	entries := append([]RunIndexEntry{}, w.entries...)
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].Time.Equal(entries[j].Time) {
			return entries[i].Time.After(entries[j].Time)
		}
		return entries[i].RunID > entries[j].RunID
	})
	return entries
}
