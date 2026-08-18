package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// day builds a UTC instant, so date-range assertions read like dates.
func day(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func newLocalArchive(t *testing.T) *artifacts.LocalRunArchive {
	t.Helper()
	archive, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	return archive
}

func archiveRun(t *testing.T, archive artifacts.RunArchive, rec artifacts.RunRecord) {
	t.Helper()
	if rec.JSONL == nil {
		rec.JSONL = []byte(fmt.Sprintf("{\"kind\":\"run\",\"run\":{\"id\":%q}}\n", rec.RunID))
	}
	if err := archive.PutRun(context.Background(), rec); err != nil {
		t.Fatalf("PutRun(%s): %v", rec.RunID, err)
	}
}

func runIDs(entries []artifacts.RunIndexEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.RunID)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRunArchiveKey_IsTheDocumentedJSONLPath pins the contract path: a run's
// events live at metadata/runs/{run-id}.jsonl and nowhere else.
func TestRunArchiveKey_IsTheDocumentedJSONLPath(t *testing.T) {
	key, err := artifacts.RunArchiveKey("ork-1786856120616785000-1")
	if err != nil {
		t.Fatalf("RunArchiveKey: %v", err)
	}
	if want := "metadata/runs/ork-1786856120616785000-1.jsonl"; key != want {
		t.Fatalf("archive key = %q, want %q", key, want)
	}
}

// TestRunIndexKey_CarriesEveryQueriedFieldAndParsesBack is why a list query
// reads keys only: namespace, orkflow, status, time and run id are all in the
// key.
func TestRunIndexKey_CarriesEveryQueriedFieldAndParsesBack(t *testing.T) {
	record := artifacts.RunRecord{
		RunID:     "ork-1786856120616785000-7",
		Namespace: "payments",
		Orkflow:   "hello-ork",
		Status:    "SUCCEEDED",
		Time:      day(t, "2026-08-14T09:30:15Z"),
	}
	key, err := artifacts.RunIndexKey(record)
	if err != nil {
		t.Fatalf("RunIndexKey: %v", err)
	}
	want := "metadata/index/runs/payments/hello-ork/succeeded/" +
		"20260814T093015.000000000Z/ork-1786856120616785000-7"
	if key != want {
		t.Fatalf("index key = %q, want %q", key, want)
	}

	entry, ok := artifacts.ParseRunIndexKey(key)
	if !ok {
		t.Fatalf("ParseRunIndexKey(%q) reported not ok", key)
	}
	if entry.RunID != record.RunID || entry.Namespace != "payments" ||
		entry.Orkflow != "hello-ork" || entry.Status != "succeeded" {
		t.Fatalf("parsed entry = %+v, want the record's coordinates", entry)
	}
	if !entry.Time.Equal(record.Time) {
		t.Fatalf("parsed time = %s, want %s", entry.Time, record.Time)
	}

	// A record that names no namespace is indexed under the default one, so
	// every key has the segment and a namespace-scoped scan misses nothing.
	record.Namespace = ""
	unnamespaced, err := artifacts.RunIndexKey(record)
	if err != nil {
		t.Fatalf("RunIndexKey (no namespace): %v", err)
	}
	entry, ok = artifacts.ParseRunIndexKey(unnamespaced)
	if !ok || entry.Namespace != artifacts.DefaultNamespace {
		t.Fatalf("key %q parsed to namespace %q, want %q",
			unnamespaced, entry.Namespace, artifacts.DefaultNamespace)
	}

	// Keys of the pre-namespace shape are not index keys any more: they parse
	// as foreign and are skipped, never mistaken for a run in some namespace.
	// Nor is a key carrying a segment this layout does not define: the key is
	// the whole query contract, so a writer that invents a segment is a
	// different index, not a compatible one.
	for _, foreign := range []string{
		"metadata/index/runs/hello-ork/succeeded/20260814T093015.000000000Z/ork-1786856120616785000-7",
		"metadata/index/runs/payments/hello-ork/succeeded/20260814T093015.000000000Z/000000001500/ork-1786856120616785000-7",
	} {
		if entry, ok := artifacts.ParseRunIndexKey(foreign); ok {
			t.Fatalf("ParseRunIndexKey(%q) = %+v, true; want false", foreign, entry)
		}
	}
}

// TestRunIndexKey_PreservesNamesAKeyCannotHoldVerbatim keeps the index honest
// for values outside the slug shape: they survive a round trip exactly.
func TestRunIndexKey_PreservesNamesAKeyCannotHoldVerbatim(t *testing.T) {
	record := artifacts.RunRecord{
		RunID:   "run/with spaces",
		Orkflow: "orkflow/with slash",
		Status:  "succeeded",
		Time:    day(t, "2026-08-14T00:00:00Z"),
	}
	key, err := artifacts.RunIndexKey(record)
	if err != nil {
		t.Fatalf("RunIndexKey: %v", err)
	}
	entry, ok := artifacts.ParseRunIndexKey(key)
	if !ok {
		t.Fatalf("ParseRunIndexKey(%q) reported not ok", key)
	}
	if entry.Orkflow != record.Orkflow || entry.RunID != record.RunID {
		t.Fatalf("round trip lost bytes: %+v, want orkflow %q run %q",
			entry, record.Orkflow, record.RunID)
	}
}

// TestRunQuery_ScanPrefixNarrowsWithEveryPinnedField proves the scan is a real
// prefix scan: each pinned filter shortens the subtree S3 has to enumerate, and
// a lower time bound starts the scan instead of filtering from the beginning.
func TestRunQuery_ScanPrefixNarrowsWithEveryPinnedField(t *testing.T) {
	since := day(t, "2026-08-14T00:00:00Z")
	tests := []struct {
		name           string
		query          artifacts.RunQuery
		wantPrefix     string
		wantStartAfter string
	}{
		{
			name:       "no filter scans the whole index",
			query:      artifacts.RunQuery{},
			wantPrefix: "metadata/index/runs/",
		},
		{
			name:       "namespace pins the first segment",
			query:      artifacts.RunQuery{Namespace: "payments"},
			wantPrefix: "metadata/index/runs/payments/",
		},
		{
			// An orkflow name is unique within a namespace, not across the
			// deployment, so the same name may live in several and the scan
			// has to see them all.
			name:       "an orkflow without a namespace cannot narrow",
			query:      artifacts.RunQuery{Orkflow: "hello-ork"},
			wantPrefix: "metadata/index/runs/",
		},
		{
			name:       "orkflow pins the second segment",
			query:      artifacts.RunQuery{Namespace: "payments", Orkflow: "hello-ork"},
			wantPrefix: "metadata/index/runs/payments/hello-ork/",
		},
		{
			name:       "status pins the third",
			query:      artifacts.RunQuery{Namespace: "payments", Orkflow: "hello-ork", Status: "SUCCEEDED"},
			wantPrefix: "metadata/index/runs/payments/hello-ork/succeeded/",
		},
		{
			// An explicit default namespace selects the same key segment as a
			// record that named no namespace.
			name:       "an unnamed namespace scans the default one",
			query:      artifacts.RunQuery{Namespace: artifacts.DefaultNamespace, Orkflow: "hello-ork"},
			wantPrefix: "metadata/index/runs/default/hello-ork/",
		},
		{
			name: "a lower bound starts the scan at its timestamp",
			query: artifacts.RunQuery{
				Namespace: "payments", Orkflow: "hello-ork", Status: "succeeded", Since: since,
			},
			wantPrefix: "metadata/index/runs/payments/hello-ork/succeeded/",
			wantStartAfter: "metadata/index/runs/payments/hello-ork/succeeded/" +
				"20260814T000000.000000000Z",
		},
		{
			name:       "a lower bound alone cannot narrow an unpinned scan",
			query:      artifacts.RunQuery{Since: since},
			wantPrefix: "metadata/index/runs/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prefix, startAfter, err := tc.query.ScanPrefix()
			if err != nil {
				t.Fatalf("ScanPrefix: %v", err)
			}
			if prefix != tc.wantPrefix {
				t.Fatalf("prefix = %q, want %q", prefix, tc.wantPrefix)
			}
			if startAfter != tc.wantStartAfter {
				t.Fatalf("start after = %q, want %q", startAfter, tc.wantStartAfter)
			}
		})
	}
}

// TestLocalRunArchive_StoresJSONLAtTheContractPath checks the body lands on
// disk at exactly the key the S3 backend would use, byte for byte.
func TestLocalRunArchive_StoresJSONLAtTheContractPath(t *testing.T) {
	dir := t.TempDir()
	archive, err := artifacts.NewLocalRunArchive(dir)
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	defer archive.Close()

	body := []byte("{\"kind\":\"run\"}\n{\"kind\":\"event\"}\n")
	archiveRun(t, archive, artifacts.RunRecord{
		RunID:   "ork-42-1",
		Orkflow: "hello-ork",
		Status:  "succeeded",
		Time:    day(t, "2026-08-14T09:30:15Z"),
		JSONL:   body,
	})

	onDisk, err := os.ReadFile(filepath.Join(dir, "metadata", "runs", "ork-42-1.jsonl"))
	if err != nil {
		t.Fatalf("read archived body: %v", err)
	}
	if string(onDisk) != string(body) {
		t.Fatalf("stored body = %q, want %q", onDisk, body)
	}

	read, err := archive.GetRun(context.Background(), "ork-42-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if string(read) != string(body) {
		t.Fatalf("GetRun body = %q, want %q", read, body)
	}
}

// TestLocalRunArchive_BodyIsImmutable proves re-archiving cannot rewrite
// history: the activity that writes it is retried, and a retry must converge.
func TestLocalRunArchive_BodyIsImmutable(t *testing.T) {
	archive := newLocalArchive(t)
	record := artifacts.RunRecord{
		RunID:   "ork-42-1",
		Orkflow: "hello-ork",
		Status:  "succeeded",
		Time:    day(t, "2026-08-14T09:30:15Z"),
		JSONL:   []byte("first\n"),
	}
	archiveRun(t, archive, record)

	record.JSONL = []byte("second\n")
	archiveRun(t, archive, record)

	body, err := archive.GetRun(context.Background(), "ork-42-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if string(body) != "first\n" {
		t.Fatalf("body = %q, want the first archived copy", body)
	}

	entries, err := archive.ListRuns(context.Background(), artifacts.RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("re-archiving produced %d index entries, want 1", len(entries))
	}
}

// TestLocalRunArchive_GetMissingRunIsNotFound is the signal the API's archive
// fallback depends on to answer NotFound instead of Internal.
func TestLocalRunArchive_GetMissingRunIsNotFound(t *testing.T) {
	archive := newLocalArchive(t)
	if _, err := archive.GetRun(context.Background(), "ork-does-not-exist-1"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetRun error = %v, want ErrNotFound", err)
	}
}

// TestRunArchive_ListFiltersByOrkflowStatusAndDateRange is the query API the
// CLI's `run list` rides on: name, status and a time window, newest first.
func TestRunArchive_ListFiltersByOrkflowStatusAndDateRange(t *testing.T) {
	archive := newLocalArchive(t)
	seed := []artifacts.RunRecord{
		{RunID: "ork-1", Orkflow: "hello-ork", Status: "succeeded", Time: day(t, "2026-08-10T10:00:00Z")},
		{RunID: "ork-2", Orkflow: "hello-ork", Status: "failed", Time: day(t, "2026-08-11T10:00:00Z")},
		{RunID: "ork-3", Orkflow: "hello-ork", Status: "succeeded", Time: day(t, "2026-08-12T10:00:00Z")},
		{RunID: "ork-4", Orkflow: "other-ork", Status: "succeeded", Time: day(t, "2026-08-13T10:00:00Z")},
		{RunID: "ork-5", Orkflow: "hello-ork", Status: "succeeded", Time: day(t, "2026-08-14T10:00:00Z")},
	}
	for _, record := range seed {
		archiveRun(t, archive, record)
	}

	tests := []struct {
		name  string
		query artifacts.RunQuery
		want  []string
	}{
		{
			name:  "everything, newest first",
			query: artifacts.RunQuery{},
			want:  []string{"ork-5", "ork-4", "ork-3", "ork-2", "ork-1"},
		},
		{
			name:  "by status",
			query: artifacts.RunQuery{Status: "SUCCEEDED"},
			want:  []string{"ork-5", "ork-4", "ork-3", "ork-1"},
		},
		{
			name:  "by orkflow",
			query: artifacts.RunQuery{Orkflow: "hello-ork"},
			want:  []string{"ork-5", "ork-3", "ork-2", "ork-1"},
		},
		{
			name:  "by orkflow and status",
			query: artifacts.RunQuery{Orkflow: "hello-ork", Status: "succeeded"},
			want:  []string{"ork-5", "ork-3", "ork-1"},
		},
		{
			name: "by date range, lower bound inclusive and upper exclusive",
			query: artifacts.RunQuery{
				Since: day(t, "2026-08-11T10:00:00Z"),
				Until: day(t, "2026-08-14T10:00:00Z"),
			},
			want: []string{"ork-4", "ork-3", "ork-2"},
		},
		{
			name: "all three filters together",
			query: artifacts.RunQuery{
				Orkflow: "hello-ork",
				Status:  "succeeded",
				Since:   day(t, "2026-08-11T00:00:00Z"),
				Until:   day(t, "2026-08-14T00:00:00Z"),
			},
			want: []string{"ork-3"},
		},
		{
			name:  "limit keeps the newest",
			query: artifacts.RunQuery{Limit: 2},
			want:  []string{"ork-5", "ork-4"},
		},
		{
			name:  "an unmatched orkflow lists nothing",
			query: artifacts.RunQuery{Orkflow: "never-applied"},
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := archive.ListRuns(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			if got := runIDs(entries); !equalStrings(got, tc.want) {
				t.Fatalf("run ids = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNewRunArchive_SelectsBackendByConfig proves one ORK_ARTIFACT_BACKEND
// selects where both artifacts and run history land.
func TestNewRunArchive_SelectsBackendByConfig(t *testing.T) {
	archive, err := artifacts.NewRunArchive(artifacts.Config{
		Backend:  artifacts.BackendLocal,
		LocalDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewRunArchive(local): %v", err)
	}
	if _, ok := archive.(*artifacts.LocalRunArchive); !ok {
		t.Fatalf("local backend built %T, want *artifacts.LocalRunArchive", archive)
	}

	if _, err := artifacts.NewRunArchive(artifacts.Config{Backend: "nope"}); !errors.Is(err, artifacts.ErrUnknownBackend) {
		t.Fatalf("unknown backend error = %v, want ErrUnknownBackend", err)
	}

	backends := artifacts.RunArchiveBackends()
	if !equalStrings(backends, []string{artifacts.BackendLocal, artifacts.BackendS3}) {
		t.Fatalf("run archive backends = %v, want [local s3]", backends)
	}
}
