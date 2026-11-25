package artifacts

import (
	"context"
	"testing"
)

// TestHighestSnapshot: the highest sequence of any full snapshot or overlay
// of a run, whatever else its prefix holds; none for a run with none.
func TestHighestSnapshot(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalRunSnapshotStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.HighestSnapshot(ctx, "default", "ork-none"); err != nil || got != 0 {
		t.Fatalf("a run with no snapshots = %d, %v", got, err)
	}
	for _, seq := range []int64{2, 9, 10, 100} {
		if _, err := store.WriteSnapshot(ctx, "default", "ork-r", seq, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.WriteSnapshot(ctx, "default", "ork-r2", 5000, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got, err := store.HighestSnapshot(ctx, "default", "ork-r"); err != nil || got != 100 {
		t.Fatalf("highest = %d, %v; want 100", got, err)
	}
	if _, err := store.WriteSnapshotOverlay(ctx, "default", "ork-r", 101, []byte("x")); err != nil {
		t.Fatal(err)
	}
	plan, err := ResumePlanKey("default", "ork-r", 900, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.b.create(ctx, plan, "application/json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if got, err := store.HighestSnapshot(ctx, "default", "ork-r"); err != nil || got != 101 {
		t.Fatalf("highest = %d, %v; want the overlay's 101", got, err)
	}
}
