package artifacts

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// keyOrderCorpus builds an archive whose index deliberately stresses the walk:
// sibling names where one is a prefix of another, names carrying '-' (which
// sorts BELOW the '/' separator, so directory-name order and key order
// disagree), names that force reversible segment encoding, repeated timestamps,
// and several namespaces, orkflows and statuses.
func keyOrderCorpus(t *testing.T, seed int64) (*LocalRunArchive, []RunIndexEntry) {
	t.Helper()
	store, err := NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	namespaces := []string{"", "default", "a", "a-b", "a-", "ab", "team-1", "team"}
	orkflows := []string{"f", "f-g", "f-", "fg", "nightly-build", "nightly", "flow name", "flow/slash", "flöw"}
	statuses := []string{"succeeded", "failed", "canceled"}
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	var want []RunIndexEntry
	for i := 0; i < 240; i++ {
		rec := RunRecord{
			RunID:     fmt.Sprintf("ork-%d-%d", seed, i),
			Namespace: namespaces[rng.Intn(len(namespaces))],
			Orkflow:   orkflows[rng.Intn(len(orkflows))],
			Status:    statuses[rng.Intn(len(statuses))],
			// i/8 makes eight runs share every timestamp, so ties are the norm
			// rather than an edge case the corpus might miss.
			Time:  base.Add(time.Duration(i/8) * time.Minute),
			JSONL: []byte("body\n"),
		}
		if err := store.PutRun(ctx, rec); err != nil {
			t.Fatalf("PutRun %q: %v", rec.RunID, err)
		}
		want = append(want, RunIndexEntry{
			RunID: rec.RunID, Namespace: ResolveNamespace(rec.Namespace),
			Orkflow: rec.Orkflow, Status: rec.Status, Time: rec.Time.UTC(),
		})
	}
	sortByIndexKey(t, want)
	return store, want
}

func sortByIndexKey(t *testing.T, entries []RunIndexEntry) {
	t.Helper()
	keys := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, err := RunIndexKeyForEntry(entry)
		if err != nil {
			t.Fatalf("RunIndexKeyForEntry(%+v): %v", entry, err)
		}
		keys[entry.RunID] = key
	}
	sort.Slice(entries, func(i, j int) bool {
		return keys[entries[i].RunID] < keys[entries[j].RunID]
	})
}

// A paged key-order traversal must return exactly the runs a full walk returns,
// in exactly the same order, each one once. The walk prunes whole subtrees that
// sort at or before the cursor, and a wrong prune would silently drop runs that
// then never get cleaned — a loss no assertion on the returned page could see.
func TestLocalRunArchive_KeyOrderPagingMatchesFullWalk(t *testing.T) {
	for _, pageSize := range []int{1, 2, 7, 50, 239, 240, 241} {
		t.Run(fmt.Sprintf("page-%d", pageSize), func(t *testing.T) {
			store, want := keyOrderCorpus(t, int64(pageSize))
			ctx := context.Background()

			var got []RunIndexEntry
			cursor := ""
			for pages := 0; ; pages++ {
				if pages > len(want)+2 {
					t.Fatalf("paging did not terminate after %d pages", pages)
				}
				page, err := store.ListRuns(ctx, RunQuery{KeyOrder: true, Limit: pageSize, StartAfterKey: cursor})
				if err != nil {
					t.Fatal(err)
				}
				if len(page) == 0 {
					break
				}
				if len(page) > pageSize {
					t.Fatalf("page of %d exceeds limit %d", len(page), pageSize)
				}
				got = append(got, page...)
				next, err := RunIndexKeyForEntry(page[len(page)-1])
				if err != nil {
					t.Fatal(err)
				}
				if next <= cursor {
					t.Fatalf("cursor did not advance: %q -> %q", cursor, next)
				}
				cursor = next
				if len(page) < pageSize {
					break
				}
			}

			if len(got) != len(want) {
				t.Fatalf("paged traversal returned %d entries, full walk has %d", len(got), len(want))
			}
			seen := make(map[string]struct{}, len(got))
			for i, entry := range got {
				if _, duplicate := seen[entry.RunID]; duplicate {
					t.Fatalf("run %q returned twice", entry.RunID)
				}
				seen[entry.RunID] = struct{}{}
				if entry.RunID != want[i].RunID {
					t.Fatalf("position %d = %q, want %q: paged order diverges from key order",
						i, entry.RunID, want[i].RunID)
				}
				if entry.Namespace != want[i].Namespace || entry.Orkflow != want[i].Orkflow ||
					entry.Status != want[i].Status || !entry.Time.Equal(want[i].Time) {
					t.Fatalf("position %d = %+v, want %+v", i, entry, want[i])
				}
			}
		})
	}
}

// The key-order walk must agree with the default newest-first scan about which
// runs exist. They are separate traversals — a manual descent and fs.WalkDir —
// so this is a genuine cross-check rather than a restatement.
func TestLocalRunArchive_KeyOrderSeesTheSameRunsAsDefaultOrder(t *testing.T) {
	store, want := keyOrderCorpus(t, 99)
	ctx := context.Background()

	listed, err := store.ListRuns(ctx, RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != len(want) {
		t.Fatalf("default order returned %d, corpus has %d", len(listed), len(want))
	}
	sortByIndexKey(t, listed)
	for i := range listed {
		if listed[i].RunID != want[i].RunID {
			t.Fatalf("position %d: default order has %q, key order has %q", i, listed[i].RunID, want[i].RunID)
		}
	}
}

// A bounded page must cost the page, not the archive. This is the property that
// makes cleanup's paging linear instead of quadratic, and it cannot be observed
// from the returned slice — only from how much of the index the backend read.
func TestLocalRunArchive_KeyOrderPageStopsAtTheLimit(t *testing.T) {
	store, want := keyOrderCorpus(t, 7)
	ctx := context.Background()

	before := store.IndexKeysScanned()
	page, err := store.ListRuns(ctx, RunQuery{KeyOrder: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	scanned := store.IndexKeysScanned() - before
	if len(page) != 10 {
		t.Fatalf("page = %d entries, want 10", len(page))
	}
	if scanned != 10 {
		t.Fatalf("a 10-entry page enumerated %d index keys, want 10 (corpus has %d)", scanned, len(want))
	}

	// The default ordering cannot stop early: it has to read everything to know
	// which runs are newest. That contrast is the whole reason KeyOrder exists.
	before = store.IndexKeysScanned()
	if _, err := store.ListRuns(ctx, RunQuery{Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if full := store.IndexKeysScanned() - before; full != int64(len(want)) {
		t.Fatalf("default-order scan enumerated %d keys, want the whole corpus %d", full, len(want))
	}
}

// Paging the whole archive must cost one pass, which is the claim the cleanup
// worker's scan-cost contract rests on.
func TestLocalRunArchive_KeyOrderTraversalCostsOnePass(t *testing.T) {
	store, want := keyOrderCorpus(t, 11)
	ctx := context.Background()

	before := store.IndexKeysScanned()
	cursor := ""
	pages := 0
	for {
		page, err := store.ListRuns(ctx, RunQuery{KeyOrder: true, Limit: 20, StartAfterKey: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		cursor, err = RunIndexKeyForEntry(page[len(page)-1])
		if err != nil {
			t.Fatal(err)
		}
		if len(page) < 20 {
			break
		}
	}
	scanned := store.IndexKeysScanned() - before
	if scanned != int64(len(want)) {
		t.Fatalf("full traversal over %d pages enumerated %d keys, want each of %d exactly once",
			pages, scanned, len(want))
	}
	t.Logf("EVIDENCE local_runs=%d page_size=20 pages=%d keys_enumerated=%d keys_per_run=%.2f",
		len(want), pages, scanned, float64(scanned)/float64(len(want)))
}
