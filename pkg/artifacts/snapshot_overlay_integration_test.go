//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_S3WritesAndReadsASnapshotOverlay: on MinIO, an overlay
// lands at metadata/snapshots/{ns}/{run}/{seq}.ovl beside the full snapshots,
// reads back, is overwritten like one, and deletes; its URI names an overlay
// and a full snapshot of the same run is a valid base for it.
func TestIntegration_S3WritesAndReadsASnapshotOverlay(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	runID := "ork-1700000000-" + uniqueToken(t)
	base, err := store.WriteSnapshot(ctx, "default", runID, 3, []byte("full snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	uri, err := store.WriteSnapshotOverlay(ctx, "default", runID, 4, []byte("overlay "+runID))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := artifacts.RunSnapshotOverlayKey("default", runID, 4)
	if uri != "s3://"+cfg.S3Bucket+"/"+key || !artifacts.IsSnapshotOverlayURI(uri) {
		t.Fatalf("overlay URI %q, want the key %q", uri, key)
	}
	if !artifacts.OverlayBaseURIMatches(base, "default", runID) || artifacts.OverlayBaseURIMatches(uri, "default", runID) {
		t.Fatalf("base %q / overlay %q: only the full snapshot is a base", base, uri)
	}
	if _, err := store.WriteSnapshotOverlay(ctx, "default", runID, 4, []byte("rewritten")); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadSnapshot(ctx, uri)
	if err != nil || string(got) != "rewritten" {
		t.Fatalf("ReadSnapshot = %q, %v", got, err)
	}
	if err := store.DeleteSnapshot(ctx, uri); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadSnapshot(ctx, uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a deleted overlay reads %v, want ErrNotFound", err)
	}
	if _, err := store.ReadSnapshot(ctx, strings.Replace(uri, cfg.S3Bucket, "another-bucket", 1)); !errors.Is(err, artifacts.ErrInvalidURI) {
		t.Fatalf("another bucket's overlay reads %v, want ErrInvalidURI", err)
	}
	t.Logf("overlay %s written beside %s, read, rewritten and deleted", uri, base)
}
