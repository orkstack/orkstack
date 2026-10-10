package artifacts

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// recentArchive is a RunArchive that reports how many index keys it walked.
type recentArchive interface {
	RunArchive
	IndexKeysScanned() int64
}

func recentBackends(t *testing.T) map[string]recentArchive {
	t.Helper()
	local, err := NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	return map[string]recentArchive{
		"local": local,
		"s3":    s3CleanupArchive(t, newS3Double(t)),
	}
}

// seedRecent archives n runs into namespace "payments" (and a few into
// "beta"), five runs to every instant so ties are the norm, across three
// orkflows and two statuses.
func seedRecent(t *testing.T, archive RunArchive, n int) []RunIndexEntry {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	orkflows := []string{"reconcile", "ingest", "nightly-build"}
	statuses := []string{"succeeded", "failed"}
	rng := rand.New(rand.NewSource(int64(n)))
	var all []RunIndexEntry
	for i := 0; i < n; i++ {
		rec := RunRecord{
			RunID:     fmt.Sprintf("ork-%d-%04d", base.Unix(), rng.Intn(1_000_000)*10000+i),
			Namespace: "payments",
			Orkflow:   orkflows[rng.Intn(len(orkflows))],
			Status:    statuses[rng.Intn(len(statuses))],
			Time:      base.Add(time.Duration(i/5) * time.Second),
			JSONL:     []byte("body\n"),
		}
		if i%50 == 0 {
			rec.Namespace = "beta"
		}
		if err := archive.PutRun(ctx, rec); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		all = append(all, RunIndexEntry{RunID: rec.RunID, Namespace: rec.Namespace,
			Orkflow: rec.Orkflow, Status: rec.Status, Time: rec.Time})
	}
	return all
}

// pageThrough pages a newest-first query by its (time, id) cursor until a
// short page, returning every entry in order.
func pageThrough(t *testing.T, archive RunArchive, q RunQuery) []RunIndexEntry {
	t.Helper()
	var out []RunIndexEntry
	for pages := 0; ; pages++ {
		if pages > 10_000 {
			t.Fatal("paging did not terminate")
		}
		page, err := archive.ListRuns(context.Background(), q)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		out = append(out, page...)
		if len(page) < q.Limit {
			return out
		}
		last := page[len(page)-1]
		q.BeforeTime, q.BeforeRunID = last.Time, last.RunID
	}
}

func sameEntries(t *testing.T, label string, got, want []RunIndexEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d entries, want %d", label, len(got), len(want))
	}
	for i := range want {
		if got[i].RunID != want[i].RunID || !got[i].Time.Equal(want[i].Time) ||
			got[i].Orkflow != want[i].Orkflow || got[i].Status != want[i].Status || got[i].Namespace != want[i].Namespace {
			t.Fatalf("%s: entry %d = %+v, want %+v", label, i, got[i], want[i])
		}
	}
}

// Page one of a namespace holding 1000 runs reads the page and its boundary
// instant, not the namespace — and answers exactly what the full scan does.
func TestRunArchive_ARecentPageReadsThePageNotTheNamespace(t *testing.T) {
	for name, archive := range recentBackends(t) {
		t.Run(name, func(t *testing.T) {
			seedRecent(t, archive, 1000)
			const limit, ties = 25, 5
			q := RunQuery{Namespace: "payments", Limit: limit}

			full, err := archive.ListRuns(context.Background(), q)
			if err != nil {
				t.Fatalf("ListRuns (full scan): %v", err)
			}
			q.Recent = true
			before := archive.IndexKeysScanned()
			page, err := archive.ListRuns(context.Background(), q)
			if err != nil {
				t.Fatalf("ListRuns (recent): %v", err)
			}
			scanned := archive.IndexKeysScanned() - before
			sameEntries(t, "page one", page, full)
			// The page, the rest of its last instant, and the one key that
			// showed the next instant began.
			if scanned > limit+ties+1 {
				t.Errorf("page one scanned %d index keys, want at most %d", scanned, limit+ties+1)
			}
		})
	}
}

// Paging by cursor visits every run of the namespace exactly once, newest
// first with ties by id descending — exactly the full scan's order — under
// every filter decided from the key, and reads no body to decide them.
func TestRunArchive_RecentPagingVisitsEveryRunOnceInTheFullScansOrder(t *testing.T) {
	for name, archive := range recentBackends(t) {
		t.Run(name, func(t *testing.T) {
			seedRecent(t, archive, 600)
			window := time.Date(2026, 10, 1, 0, 0, 30, 0, time.UTC)
			for _, q := range []RunQuery{
				{Namespace: "payments"},
				{Namespace: "beta"},
				{Namespace: "payments", Orkflow: "ingest"},
				{Namespace: "payments", Status: "failed"},
				{Namespace: "payments", Since: window, Until: window.Add(40 * time.Second)},
				{Namespace: "payments", Keep: func(e RunIndexEntry) bool { return e.Orkflow != "reconcile" }},
				// Both pinned: the run-index subtree answers; Recent is ignored.
				{Namespace: "payments", Orkflow: "nightly-build", Status: "succeeded"},
			} {
				for _, limit := range []int{1, 7, 50} {
					q.Limit = limit
					want := pageThrough(t, archive, q)
					recent := q
					recent.Recent = true
					got := pageThrough(t, archive, recent)
					label := fmt.Sprintf("%+v limit %d", RunQuery{Namespace: q.Namespace, Orkflow: q.Orkflow, Status: q.Status, Since: q.Since}, limit)
					sameEntries(t, label, got, want)
					seen := map[string]bool{}
					for _, entry := range got {
						if seen[entry.RunID] {
							t.Fatalf("%s: run %s listed twice", label, entry.RunID)
						}
						seen[entry.RunID] = true
					}
				}
			}
		})
	}
}

func TestRunRecentIndexKey_RoundTripsAndSortsNewestFirst(t *testing.T) {
	older := RunRecord{RunID: "ork-1-1", Namespace: "", Orkflow: "flow name", Status: "STATUS_FAILED",
		Time: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)}
	newer := older
	newer.RunID, newer.Time = "ork-1-2", older.Time.Add(time.Nanosecond)
	olderKey, err := RunRecentIndexKey(older)
	if err != nil {
		t.Fatal(err)
	}
	newerKey, err := RunRecentIndexKey(newer)
	if err != nil {
		t.Fatal(err)
	}
	if !(newerKey < olderKey) {
		t.Fatalf("the newer key %q does not sort before the older %q", newerKey, olderKey)
	}
	entry, ok := ParseRunRecentIndexKey(olderKey)
	if !ok || entry.RunID != "ork-1-1" || entry.Namespace != DefaultNamespace || entry.Orkflow != "flow name" ||
		entry.Status != "failed" || !entry.Time.Equal(older.Time) {
		t.Fatalf("ParseRunRecentIndexKey(%q) = %+v, %v", olderKey, entry, ok)
	}
	zero := older
	zero.Time = time.Time{}
	zeroKey, err := RunRecentIndexKey(zero)
	if err != nil {
		t.Fatal(err)
	}
	if entry, ok := ParseRunRecentIndexKey(zeroKey); !ok || !entry.Time.IsZero() || !(olderKey < zeroKey) {
		t.Fatalf("a run with no time: %q parses %+v %v, and must sort last", zeroKey, entry, ok)
	}
	if _, ok := ParseRunRecentIndexKey("metadata/index/runs/default/f/succeeded/20260102T030405.000000006Z/ork-1-1"); ok {
		t.Fatal("a run-index key parsed as a recent-index key")
	}
}
