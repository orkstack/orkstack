package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/archive"
)

// OpenRunIDs lists the runs the index shows open, once each, and leaves out
// closed and switched records.
func TestOpenRunIDs_ListsOnlyOpenRuns(t *testing.T) {
	index := indexOf(t,
		record{id: "ork-open", status: running, start: visibilityNow.Add(-time.Hour)},
		record{id: "ork-done", status: completed, start: visibilityNow.Add(-3 * time.Hour), closed: visibilityNow.Add(-time.Hour)},
		switched("ork-chain", visibilityNow.Add(-5*time.Hour), 10),
		record{id: "ork-chain", status: running, start: visibilityNow.Add(-10 * time.Minute)},
	)
	ids, truncated, err := readerOverIndex(index).OpenRunIDs(context.Background(), 10)
	if err != nil || truncated {
		t.Fatalf("ids %v truncated %v err %v", ids, truncated, err)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"ork-chain", "ork-open"}) {
		t.Fatalf("ids = %v; want [ork-chain ork-open]", ids)
	}
}

// The listing stops at its bound and says more are open.
func TestOpenRunIDs_IsBounded(t *testing.T) {
	index := indexOf(t,
		record{id: "ork-a", status: running, start: visibilityNow},
		record{id: "ork-b", status: running, start: visibilityNow},
		record{id: "ork-c", status: running, start: visibilityNow},
	)
	ids, truncated, err := readerOverIndex(index).OpenRunIDs(context.Background(), 2)
	if err != nil || !truncated || len(ids) != 2 {
		t.Fatalf("ids %v truncated %v err %v; want two and truncated", ids, truncated, err)
	}
	if _, _, err := readerOverIndex(index).OpenRunIDs(context.Background(), 0); err == nil {
		t.Fatal("an unbounded listing was accepted")
	}
}

// An index that never ends is an error, never a partial answer.
func TestOpenRunIDs_ScanBoundIsAnError(t *testing.T) {
	index := indexOf(t, record{id: "ork-a", status: completed, start: visibilityNow, closed: visibilityNow})
	index.endless = true
	if _, _, err := readerOverIndex(index).OpenRunIDs(context.Background(), 10); err == nil {
		t.Fatal("a listing that never reached the end of the index answered")
	}
}

// The reader the engine wires is the restore reconcile's cluster.
var _ archive.RestoreCluster = (*TemporalRunReader)(nil)
