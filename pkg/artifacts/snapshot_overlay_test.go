package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunSnapshotOverlayKey_SitsBesideTheFullSnapshots: an overlay's key is
// its event's full-snapshot key with the .ovl suffix, under the same run
// prefix, so run cleanup reclaims it with them.
func TestRunSnapshotOverlayKey_SitsBesideTheFullSnapshots(t *testing.T) {
	key, err := RunSnapshotOverlayKey("payments", "ork-42-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	if want := "metadata/snapshots/payments/ork-42-1/9.ovl"; key != want {
		t.Fatalf("RunSnapshotOverlayKey = %q, want %q", key, want)
	}
	prefix, _ := RunSnapshotPrefix("payments", "ork-42-1")
	if !strings.HasPrefix(key, prefix) {
		t.Fatalf("overlay key %q is outside its run prefix %q", key, prefix)
	}
	if _, err := RunSnapshotOverlayKey("default", "ork-1", 0); err == nil {
		t.Fatal("RunSnapshotOverlayKey accepted sequence 0")
	}
}

// TestOverlayBaseURIMatches_OnlyAFullSnapshotOfTheSameRun: an overlay's base
// is a canonical {seq}.pb of exactly the named run, under any store root;
// never an overlay, a continuation object, another run's or namespace's
// snapshot, a non-canonical sequence or a path that could climb out of its
// root.
func TestOverlayBaseURIMatches_OnlyAFullSnapshotOfTheSameRun(t *testing.T) {
	const run = "ork-1700000000-7"
	for uri, want := range map[string]bool{
		"s3://bucket/metadata/snapshots/default/" + run + "/12.pb":                                                               true,
		"file:///var/ork/metadata/snapshots/default/" + run + "/1.pb":                                                            true,
		"s3://bucket/metadata/snapshots/default/" + run + "/12.ovl":                                                              false,
		"s3://bucket/metadata/snapshots/default/" + run + "/012.pb":                                                              false,
		"s3://bucket/metadata/snapshots/default/" + run + "/0.pb":                                                                false,
		"s3://bucket/metadata/snapshots/default/" + run + "/x.pb":                                                                false,
		"s3://bucket/metadata/snapshots/default/ork-other/12.pb":                                                                 false,
		"s3://bucket/metadata/snapshots/payments/" + run + "/12.pb":                                                              false,
		"s3://bucket/metadata/snapshots/default/" + run + "/continuations/000001-snapshot-" + strings.Repeat("a", 64) + ".pb.gz": false,
		"s3:///metadata/snapshots/default/" + run + "/12.pb":                                                                     false,
		"file:///var/ork/../metadata/snapshots/default/" + run + "/12.pb":                                                        false,
		"https://bucket/metadata/snapshots/default/" + run + "/12.pb":                                                            false,
		"s3://bucket/metadata/snapshots/default/" + run + "/12.pb?versionId=1":                                                   false,
	} {
		if got := OverlayBaseURIMatches(uri, "default", run); got != want {
			t.Errorf("OverlayBaseURIMatches(%q) = %v, want %v", uri, got, want)
		}
	}
}

// TestLocalRunSnapshotStore_WritesAndReadsAnOverlay: an overlay lands at its
// documented key, reads back, overwrites like a full snapshot and deletes;
// IsSnapshotOverlayURI tells it from a full snapshot.
func TestLocalRunSnapshotStore_WritesAndReadsAnOverlay(t *testing.T) {
	ctx := context.Background()
	store, root := newLocalSnapshotStore(t)
	uri, err := store.WriteSnapshotOverlay(ctx, "default", "ork-ovl-1", 5, []byte("overlay"))
	if err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(root, "metadata", "snapshots", "default", "ork-ovl-1", "5.ovl")); err != nil ||
		string(body) != "overlay" {
		t.Fatalf("the overlay is not at its documented key: %v", err)
	}
	if !IsSnapshotOverlayURI(uri) {
		t.Fatalf("IsSnapshotOverlayURI(%q) = false", uri)
	}
	if again, err := store.WriteSnapshotOverlay(ctx, "default", "ork-ovl-1", 5, []byte("rewritten")); err != nil || again != uri {
		t.Fatalf("rewrite = %q, %v", again, err)
	}
	if got, err := store.ReadSnapshot(ctx, uri); err != nil || string(got) != "rewritten" {
		t.Fatalf("ReadSnapshot = %q, %v", got, err)
	}
	full, err := store.WriteSnapshot(ctx, "default", "ork-ovl-1", 5, []byte("full"))
	if err != nil || IsSnapshotOverlayURI(full) || full == uri {
		t.Fatalf("the full snapshot of the same event = %q, %v", full, err)
	}
	if err := store.DeleteSnapshot(ctx, uri); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadSnapshot(ctx, uri); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a deleted overlay reads %v, want ErrNotFound", err)
	}
	if _, err := store.WriteSnapshotOverlay(ctx, "default", "ork-ovl-1", 6, nil); err == nil {
		t.Fatal("an empty overlay was written")
	}
}
