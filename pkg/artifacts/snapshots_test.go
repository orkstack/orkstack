package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newLocalSnapshotStore(t *testing.T) (*LocalRunSnapshotStore, string) {
	t.Helper()
	root := t.TempDir()
	store, err := NewLocalRunSnapshotStore(root)
	if err != nil {
		t.Fatalf("NewLocalRunSnapshotStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, root
}

// TestRunSnapshotKey_StaysInTheMetadataNamespace pins the root-cause-A
// boundary: a snapshot is a platform record and must live under metadata/,
// beside commits, logs and run bodies — never under the orkflows/ or objects/
// namespaces where workload artifacts land.
func TestRunSnapshotKey_StaysInTheMetadataNamespace(t *testing.T) {
	key, err := RunSnapshotKey("default", "ork-1700000000-1", 7)
	if err != nil {
		t.Fatalf("RunSnapshotKey: %v", err)
	}
	if want := "metadata/snapshots/default/ork-1700000000-1/7.pb"; key != want {
		t.Fatalf("RunSnapshotKey = %q, want %q", key, want)
	}
	if !strings.HasPrefix(key, "metadata/") {
		t.Fatalf("snapshot key %q escaped the metadata namespace", key)
	}
	for _, forbidden := range []string{"orkflows/", "objects/"} {
		if strings.HasPrefix(key, forbidden) {
			t.Fatalf("snapshot key %q reached the workload namespace %q", key, forbidden)
		}
	}
}

func TestRunSnapshotKey_ResolvesTheDefaultNamespaceAndRejectsBadSequences(t *testing.T) {
	key, err := RunSnapshotKey("", "ork-1", 1)
	if err != nil {
		t.Fatalf("RunSnapshotKey with empty namespace: %v", err)
	}
	if want := "metadata/snapshots/default/ork-1/1.pb"; key != want {
		t.Fatalf("RunSnapshotKey = %q, want %q", key, want)
	}
	if _, err := RunSnapshotKey("default", "ork-1", 0); err == nil {
		t.Fatal("RunSnapshotKey accepted sequence 0")
	}
	if _, err := RunSnapshotKey("default", "", 1); err == nil {
		t.Fatal("RunSnapshotKey accepted an empty run id")
	}
}

func TestRunSnapshotPrefix_IsTheCleanupSweepBoundary(t *testing.T) {
	prefix, err := RunSnapshotPrefix("payments", "ork-42-1")
	if err != nil {
		t.Fatalf("RunSnapshotPrefix: %v", err)
	}
	if want := "metadata/snapshots/payments/ork-42-1/"; prefix != want {
		t.Fatalf("RunSnapshotPrefix = %q, want %q", prefix, want)
	}
	key, err := RunSnapshotKey("payments", "ork-42-1", 3)
	if err != nil {
		t.Fatalf("RunSnapshotKey: %v", err)
	}
	if !strings.HasPrefix(key, prefix) {
		t.Fatalf("key %q is outside its run prefix %q, cleanup could not sweep it", key, prefix)
	}
}

// TestLocalRunSnapshotStore_StoresUnderTheExactDeterministicKey proves the
// STORED key — not merely a logical name an index echoes back — is the
// documented metadata/snapshots/{namespace}/{run}/{sequence}.pb layout, so an
// operator's lifecycle rule over the prefix genuinely addresses the objects.
func TestLocalRunSnapshotStore_StoresUnderTheExactDeterministicKey(t *testing.T) {
	ctx := context.Background()
	store, root := newLocalSnapshotStore(t)
	uri, err := store.WriteSnapshot(ctx, "default", "ork-key-1", 42, []byte("snapshot"))
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	stored := filepath.Join(root, "metadata", "snapshots", "default", "ork-key-1", "42.pb")
	body, err := os.ReadFile(stored)
	if err != nil {
		t.Fatalf("snapshot is not at its documented key: %v", err)
	}
	if string(body) != "snapshot" {
		t.Fatalf("stored bytes = %q, want %q", body, "snapshot")
	}
	if !strings.HasSuffix(uri, "metadata/snapshots/default/ork-key-1/42.pb") {
		t.Fatalf("URI %q does not address the documented key", uri)
	}
}

func TestLocalRunSnapshotStore_WriteIsIdempotentAndReadable(t *testing.T) {
	ctx := context.Background()
	store, _ := newLocalSnapshotStore(t)
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

func TestLocalRunSnapshotStore_ReadPreservesNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newLocalSnapshotStore(t)
	uri, err := store.WriteSnapshot(ctx, "default", "ork-missing", 1, []byte("snapshot"))
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := store.DeleteSnapshot(ctx, uri); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if _, err := store.ReadSnapshot(ctx, uri); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadSnapshot error = %v, want errors.Is(ErrNotFound)", err)
	}
	// Deleting again is a no-op, which is what lets cleanup retry.
	if err := store.DeleteSnapshot(ctx, uri); err != nil {
		t.Fatalf("repeated DeleteSnapshot: %v", err)
	}
}

func TestLocalRunSnapshotStore_RejectsForeignURIs(t *testing.T) {
	ctx := context.Background()
	store, root := newLocalSnapshotStore(t)
	for _, uri := range []string{
		"",
		"file://" + filepath.ToSlash(filepath.Join(root, "objects", "loose.pb")),
		"file://" + filepath.ToSlash(filepath.Join(root, "metadata", "commits", "default", "ork-1", "task", "commit.json")),
		"s3://bucket/metadata/snapshots/default/ork-1/1.pb",
	} {
		if _, err := store.ReadSnapshot(ctx, uri); !errors.Is(err, ErrInvalidURI) {
			t.Fatalf("ReadSnapshot(%q) error = %v, want errors.Is(ErrInvalidURI)", uri, err)
		}
	}
}

func TestNewRunSnapshotStore_SelectsTheConfiguredBackend(t *testing.T) {
	store, err := NewRunSnapshotStore(Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewRunSnapshotStore: %v", err)
	}
	if _, ok := store.(*LocalRunSnapshotStore); !ok {
		t.Fatalf("store = %T, want *LocalRunSnapshotStore", store)
	}
	if _, err := NewRunSnapshotStore(Config{Backend: "unregistered"}); !errors.Is(err, ErrUnknownBackend) {
		t.Fatalf("unknown backend error = %v, want errors.Is(ErrUnknownBackend)", err)
	}
}
