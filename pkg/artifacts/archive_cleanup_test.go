package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalRunArchive_CleanupSidecarAndRunScopedDeletion(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	at := time.Date(2026, 8, 31, 12, 0, 0, 123, time.FixedZone("offset", 3600))
	rec := RunRecord{RunID: "ork-run", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: at, JSONL: []byte("archive\n")}
	if err := store.PutRun(ctx, rec); err != nil {
		t.Fatal(err)
	}

	logPrefix, _ := RunLogPrefix(rec.RunID)
	commitPrefix, _ := RunCommitPrefix(rec.Namespace, rec.RunID)
	snapshotPrefix, _ := RunSnapshotPrefix(rec.Namespace, rec.RunID)
	for name, body := range map[string][]byte{
		filepath.Join(root, filepath.FromSlash(logPrefix), "task", "attempt", "chunk"):  []byte("logs"),
		filepath.Join(root, filepath.FromSlash(commitPrefix), "task", CommitObjectName): []byte("commit"),
		filepath.Join(root, filepath.FromSlash(snapshotPrefix), "7.pb"):                 []byte("snapshot"),
		filepath.Join(root, "objects", "shared"):                                        []byte("shared"),
	} {
		if err := os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	result, err := store.DeleteRunArtifacts(ctx, rec.Namespace, rec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 3 || result.BytesReclaimed != int64(len("logs")+len("commit")+len("snapshot")) {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(snapshotPrefix), "7.pb")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run snapshot survived cleanup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "objects", "shared")); err != nil {
		t.Fatalf("shared object: %v", err)
	}
	if got, err := store.GetRun(ctx, rec.RunID); err != nil || string(got) != "archive\n" {
		t.Fatalf("archive = %q, %v", got, err)
	}

	if _, err := store.CleanupState(ctx, rec.RunID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unmarked state = %v", err)
	}
	if err := store.MarkArtifactsCleaned(ctx, rec.RunID, at); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkArtifactsCleaned(ctx, rec.RunID, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, err := store.CleanupState(ctx, rec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !state.ArtifactsCleaned || !state.TemporalDeletedAt.Equal(at.UTC()) {
		t.Fatalf("state = %+v", state)
	}
	entries, err := store.ListRuns(ctx, RunQuery{})
	if err != nil || len(entries) != 1 || entries[0].RunID != rec.RunID {
		t.Fatalf("entries = %+v, %v", entries, err)
	}
}

func TestLocalRunArchive_DeletesObjectsOnlyAfterLastReference(t *testing.T) {
	root := t.TempDir()
	archive, err := NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	store, err := NewLocalStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	shared, err := store.Put(ctx, "shared", bytes.NewBufferString("shared bytes"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sole, err := store.Put(ctx, "sole", bytes.NewBufferString("sole bytes"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sharedID, ok := ObjectIDFromURI(shared.URI)
	if !ok {
		t.Fatalf("shared URI is not canonical: %q", shared.URI)
	}
	soleID, ok := ObjectIDFromURI(sole.URI)
	if !ok {
		t.Fatalf("sole URI is not canonical: %q", sole.URI)
	}
	at := time.Now().UTC()
	for _, rec := range []RunRecord{
		{RunID: "ork-first", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: at, ObjectIDs: []string{sharedID, soleID, sharedID}, JSONL: []byte("first\n")},
		{RunID: "ork-second", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: at.Add(time.Second), ObjectIDs: []string{sharedID}, JSONL: []byte("second\n")},
	} {
		if err := archive.PutRun(ctx, rec); err != nil {
			t.Fatalf("PutRun(%s): %v", rec.RunID, err)
		}
	}

	if _, err := archive.DeleteRunArtifacts(ctx, "default", "ork-first"); err != nil {
		t.Fatal(err)
	}
	result, err := archive.DeleteUnreferencedObjects(ctx, []string{sharedID, soleID, soleID})
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 1 || result.BytesReclaimed != sole.SizeBytes {
		t.Fatalf("first unreferenced result = %+v", result)
	}
	if _, err := store.Stat(ctx, shared.URI); err != nil {
		t.Fatalf("shared object was deleted while second run referenced it: %v", err)
	}
	if _, err := store.Stat(ctx, sole.URI); !errors.Is(err, ErrNotFound) {
		t.Fatalf("sole object still exists: %v", err)
	}

	if _, err := archive.DeleteRunArtifacts(ctx, "default", "ork-second"); err != nil {
		t.Fatal(err)
	}
	result, err = archive.DeleteUnreferencedObjects(ctx, []string{sharedID})
	if err != nil || result.ObjectsDeleted != 1 || result.BytesReclaimed != shared.SizeBytes {
		t.Fatalf("last-reference result = %+v, %v", result, err)
	}
	result, err = archive.DeleteUnreferencedObjects(ctx, []string{sharedID, sharedID})
	if err != nil || result != (CleanupResult{}) {
		t.Fatalf("idempotent result = %+v, %v", result, err)
	}
	for runID, body := range map[string]string{"ork-first": "first\n", "ork-second": "second\n"} {
		got, err := archive.GetRun(ctx, runID)
		if err != nil || string(got) != body {
			t.Fatalf("GetRun(%s) = %q, %v", runID, got, err)
		}
	}
	entries, err := archive.ListRuns(ctx, RunQuery{})
	if err != nil || len(entries) != 2 {
		t.Fatalf("run index after cleanup = %+v, %v", entries, err)
	}
}

func TestLocalRunArchive_MissingObjectListDoesNotScanReferenceIndex(t *testing.T) {
	root := t.TempDir()
	archive, err := NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	objectID := objectKey("legacy", strings.Repeat("a", 64))
	record := RunRecord{RunID: "ork-legacy", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: time.Now().UTC(), ObjectIDs: []string{objectID}, JSONL: []byte("archive\n")}
	if err := archive.PutRun(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	listKey, _ := RunObjectListKey(record.RunID)
	markerKey, _ := RunObjectRefKey(objectID, record.RunID)
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(listKey))); err != nil {
		t.Fatal(err)
	}
	result, err := archive.DeleteRunArtifacts(context.Background(), record.Namespace, record.RunID)
	if err != nil || result != (CleanupResult{}) {
		t.Fatalf("DeleteRunArtifacts without list = %+v, %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(markerKey))); err != nil {
		t.Fatalf("missing-list cleanup scanned and removed marker: %v", err)
	}
}

func TestLocalRunArchive_LongObjectIDsRemainArchivable(t *testing.T) {
	for _, size := range []int{300, 1000} {
		t.Run(fmt.Sprintf("name-%d", size), func(t *testing.T) {
			root := t.TempDir()
			archive, err := NewLocalRunArchive(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = archive.Close() })
			store, err := NewLocalStore(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })

			ref, err := store.Put(context.Background(), strings.Repeat("a", size), bytes.NewBufferString("long object"), PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			objectID, ok := ObjectIDFromURI(ref.URI)
			if !ok {
				t.Fatalf("ObjectIDFromURI(%q) failed", ref.URI)
			}
			markerKey, err := RunObjectRefKey(objectID, "ork-long")
			if err != nil {
				t.Fatalf("RunObjectRefKey for %d-byte name: %v", size, err)
			}
			listKey, err := RunObjectListKey("ork-long")
			if err != nil {
				t.Fatal(err)
			}
			record := RunRecord{RunID: "ork-long", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: time.Now().UTC(), ObjectIDs: []string{objectID}, JSONL: []byte("archive\n")}
			if err := archive.PutRun(context.Background(), record); err != nil {
				t.Fatalf("PutRun for %d-byte name: %v", size, err)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(markerKey))); err != nil {
				t.Fatalf("reference marker: %v", err)
			}
			if body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(listKey))); err != nil || !bytes.Contains(body, []byte(objectID)) {
				t.Fatalf("object list = %q, %v", body, err)
			}

			cleanupResult, err := archive.DeleteRunArtifacts(context.Background(), record.Namespace, record.RunID)
			if err != nil || cleanupResult != (CleanupResult{}) {
				t.Fatalf("DeleteRunArtifacts = %+v, %v", cleanupResult, err)
			}
			for _, key := range []string{markerKey, listKey} {
				if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(key))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("reference bookkeeping %q remains: %v", key, err)
				}
			}
			result, err := archive.DeleteUnreferencedObjects(context.Background(), []string{objectID})
			if err != nil || result.ObjectsDeleted != 1 || result.BytesReclaimed != ref.SizeBytes {
				t.Fatalf("DeleteUnreferencedObjects = %+v, %v", result, err)
			}
		})
	}
}

// TestLocalRunArchive_ObjectDeletedMidScanIsIdempotent covers the race cleanup
// cannot exclude: the reference index says an object is unreferenced, and the
// object is gone by the time the delete is issued — a concurrent cleaner, an
// operator, or a bucket lifecycle rule got there first. The outcome must be a
// no-op, never an error and never a phantom byte count, and a second full pass
// over the same ids must change nothing.
func TestLocalRunArchive_ObjectDeletedMidScanIsIdempotent(t *testing.T) {
	root := t.TempDir()
	archive, err := NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	store, err := NewLocalStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	raced, err := store.Put(ctx, "raced", bytes.NewBufferString("raced bytes"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := store.Put(ctx, "retained", bytes.NewBufferString("retained bytes"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	racedID, ok := ObjectIDFromURI(raced.URI)
	if !ok {
		t.Fatalf("ObjectIDFromURI(%q) failed", raced.URI)
	}
	retainedID, ok := ObjectIDFromURI(retained.URI)
	if !ok {
		t.Fatalf("ObjectIDFromURI(%q) failed", retained.URI)
	}

	at := time.Now().UTC()
	if err := archive.PutRun(ctx, RunRecord{RunID: "ork-raced", Namespace: "default", Orkflow: "flow",
		Status: "succeeded", Time: at, ObjectIDs: []string{racedID}, JSONL: []byte("raced\n")}); err != nil {
		t.Fatal(err)
	}
	if err := archive.PutRun(ctx, RunRecord{RunID: "ork-holder", Namespace: "default", Orkflow: "flow",
		Status: "succeeded", Time: at.Add(time.Second), ObjectIDs: []string{retainedID}, JSONL: []byte("holder\n")}); err != nil {
		t.Fatal(err)
	}

	// The object is still referenced, so vanishing bytes must not be counted as
	// a reclamation and must not be reported as a failure either.
	if err := store.Delete(ctx, retained.URI); err != nil {
		t.Fatal(err)
	}
	result, err := archive.DeleteUnreferencedObjects(ctx, []string{retainedID})
	if err != nil || result != (CleanupResult{}) {
		t.Fatalf("referenced-but-missing object = %+v, %v", result, err)
	}

	// Now the unreferenced path races: markers are gone, then the bytes vanish
	// before the delete is issued.
	if _, err := archive.DeleteRunArtifacts(ctx, "default", "ork-raced"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, raced.URI); err != nil {
		t.Fatal(err)
	}
	result, err = archive.DeleteUnreferencedObjects(ctx, []string{racedID})
	if err != nil || result != (CleanupResult{}) {
		t.Fatalf("raced delete = %+v, %v", result, err)
	}

	// A second full pass over every id converges on the same state.
	for pass := range 2 {
		result, err = archive.DeleteUnreferencedObjects(ctx, []string{racedID, retainedID, racedID})
		if err != nil || result != (CleanupResult{}) {
			t.Fatalf("recovery pass %d = %+v, %v", pass, result, err)
		}
	}
	// The retained run's claim survived the whole sequence: cleanup never
	// silently dropped a reference it did not own.
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(mustRefKey(t, retainedID, "ork-holder")))); err != nil {
		t.Fatalf("retained marker: %v", err)
	}
	if body, err := archive.GetRun(ctx, "ork-raced"); err != nil || string(body) != "raced\n" {
		t.Fatalf("archived body = %q, %v", body, err)
	}
}

func mustRefKey(t *testing.T, objectID, runID string) string {
	t.Helper()
	key, err := RunObjectRefKey(objectID, runID)
	if err != nil {
		t.Fatalf("RunObjectRefKey: %v", err)
	}
	return key
}
