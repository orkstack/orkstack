package artifacts_test

import (
	"context"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRunQuery_KeepRunsDuringTheKeyScanBeforeLimit pins where the residual
// predicate runs: inside Matches, during the index scan, before Limit. A Keep
// applied after Limit would hand back a short page whenever the newest keys
// failed it; one applied after the scan would let an engine hydrate a body it
// is about to throw away. Both the run family and the origin family call it,
// and it sees the fields the key carries.
func TestRunQuery_KeepRunsDuringTheKeyScanBeforeLimit(t *testing.T) {
	archive := newLocalArchive(t)
	base := day(t, "2026-10-01T09:00:00Z")
	statuses := []string{"failed", "succeeded", "failed", "succeeded", "failed", "failed"}
	for i, status := range statuses {
		archiveRun(t, archive, artifacts.RunRecord{
			RunID: "ork-" + string(rune('a'+i)), Namespace: "default", Orkflow: "deploy", Status: status,
			Time:       base.Add(time.Duration(i) * time.Minute),
			OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-1",
		})
	}

	var seen []artifacts.RunIndexEntry
	got, err := archive.ListRuns(context.Background(), artifacts.RunQuery{
		Namespace: "default",
		Limit:     2,
		Keep: func(e artifacts.RunIndexEntry) bool {
			seen = append(seen, e)
			return e.Status == "succeeded"
		},
	})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	// The two newest runs (f, e) are failed; a Keep after Limit would have
	// returned nothing. Keep before Limit returns the two newest succeeded.
	if ids := runIDs(got); !equalStrings(ids, []string{"ork-d", "ork-b"}) {
		t.Fatalf("Keep with Limit 2 listed %v, want [ork-d ork-b]", ids)
	}
	if len(seen) != len(statuses) {
		t.Fatalf("Keep saw %d index entries, want every one of the %d scanned", len(seen), len(statuses))
	}
	for _, e := range seen {
		if e.Namespace != "default" || e.Orkflow != "deploy" || e.Time.IsZero() || e.RunID == "" {
			t.Fatalf("Keep saw %+v, want the key's namespace, orkflow, time and id", e)
		}
	}

	// The origin family carries the origin, and Keep sees it there.
	seen = nil
	got, err = archive.ListRuns(context.Background(), artifacts.RunQuery{
		Namespace:  "default",
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER,
		Limit:      1,
		Keep: func(e artifacts.RunIndexEntry) bool {
			seen = append(seen, e)
			return e.OriginID == "trg-1" && e.Status == "succeeded"
		},
	})
	if err != nil {
		t.Fatalf("ListRuns(origin): %v", err)
	}
	if ids := runIDs(got); !equalStrings(ids, []string{"ork-d"}) {
		t.Fatalf("origin Keep listed %v, want [ork-d]", ids)
	}
	for _, e := range seen {
		if e.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER || e.OriginID != "trg-1" {
			t.Fatalf("Keep on the origin family saw %+v, want its origin", e)
		}
	}

	// A nil Keep keeps everything the other fields keep.
	all, err := archive.ListRuns(context.Background(), artifacts.RunQuery{Namespace: "default"})
	if err != nil || len(all) != len(statuses) {
		t.Fatalf("nil Keep listed %d (%v), want %d", len(all), err, len(statuses))
	}
}
