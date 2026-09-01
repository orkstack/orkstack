package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func newSnapshotStore(t *testing.T) (engine.SnapshotStore, artifacts.Store) {
	t.Helper()
	store, err := artifacts.New(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New artifact store: %v", err)
	}
	return engine.NewSnapshotStore(store), store
}

func TestSnapshotStore_WriteIsIdempotentAndReadable(t *testing.T) {
	ctx := context.Background()
	store, _ := newSnapshotStore(t)
	data := []byte("test snapshot data")

	first, err := store.WriteSnapshot(ctx, "default", "ork-test-123", 1, data)
	if err != nil {
		t.Fatalf("first WriteSnapshot: %v", err)
	}
	second, err := store.WriteSnapshot(ctx, "default", "ork-test-123", 1, data)
	if err != nil {
		t.Fatalf("second WriteSnapshot: %v", err)
	}
	if first != second {
		t.Fatalf("same snapshot URI = %q then %q", first, second)
	}
	got, err := store.ReadSnapshot(ctx, second)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("snapshot = %q, want %q", got, data)
	}
}

func TestSnapshotStore_UsesDeterministicLogicalKey(t *testing.T) {
	ctx := context.Background()
	snapshots, backend := newSnapshotStore(t)
	first, err := snapshots.WriteSnapshot(ctx, "default", "ork-xyz-789", 42, []byte("snapshot"))
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	second, err := snapshots.WriteSnapshot(ctx, "default", "ork-xyz-789", 42, []byte("snapshot"))
	if err != nil {
		t.Fatalf("WriteSnapshot again: %v", err)
	}
	if first != second {
		t.Fatalf("URI = %q then %q", first, second)
	}
	refs, err := backend.List(ctx, "snapshots/default/ork-xyz-789/")
	if err != nil {
		t.Fatalf("List snapshots: %v", err)
	}
	if len(refs) != 1 || refs[0].Name != "snapshots/default/ork-xyz-789/42.pb" {
		t.Fatalf("stored references = %+v, want deterministic snapshot key", refs)
	}
}

func TestSnapshotStore_ReadPreservesNotFound(t *testing.T) {
	ctx := context.Background()
	snapshots, backend := newSnapshotStore(t)
	uri, err := snapshots.WriteSnapshot(ctx, "default", "ork-missing", 1, []byte("snapshot"))
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := backend.Delete(ctx, uri); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := snapshots.ReadSnapshot(ctx, uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("ReadSnapshot error = %v, want errors.Is(ErrNotFound)", err)
	}
}

func TestSnapshotStore_ReadRejectsEmptyURI(t *testing.T) {
	store, _ := newSnapshotStore(t)
	if _, err := store.ReadSnapshot(context.Background(), ""); err == nil {
		t.Fatal("ReadSnapshot empty URI returned nil error")
	}
}
