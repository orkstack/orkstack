package artifacts

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func s3CleanupArchive(t *testing.T, d *s3Double) *S3RunArchive {
	t.Helper()
	archive, err := NewS3RunArchive(context.Background(), d.config())
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	return archive
}

// seedRunScoped writes n objects under each of a run's four run-owned
// prefixes, plus its object list naming markers objects.
func seedRunScoped(t *testing.T, d *s3Double, namespace, runID string, n int, markers []string) map[string][]string {
	t.Helper()
	logs, _ := RunLogPrefix(runID)
	attempts, _ := RunAttemptPrefix(runID)
	commits, _ := RunCommitPrefix(namespace, runID)
	snapshots, _ := RunSnapshotPrefix(namespace, runID)
	seeded := map[string][]string{}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, prefix := range []string{logs, attempts, commits, snapshots} {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("%sobj-%06d", prefix, i)
			d.objects[key] = []byte("0123456789")
			seeded[prefix] = append(seeded[prefix], key)
		}
	}
	listKey, err := RunObjectListKey(runID)
	if err != nil {
		t.Fatalf("RunObjectListKey: %v", err)
	}
	body := "[]"
	if len(markers) > 0 {
		body = `["` + strings.Join(markers, `","`) + `"]`
		for _, objectID := range markers {
			key, err := RunObjectRefKey(objectID, runID)
			if err != nil {
				t.Fatalf("RunObjectRefKey: %v", err)
			}
			d.objects[key] = nil
		}
	}
	d.objects[listKey] = []byte(body)
	return seeded
}

func TestS3RunArchive_DeleteRunArtifactsDeletesEachListingPageWithOneRequest(t *testing.T) {
	d := newS3Double(t)
	archive := s3CleanupArchive(t, d)
	const runID, perPrefix = "ork-1-1", 2500
	seedRunScoped(t, d, "payments", runID, perPrefix, []string{"orkflows/payments/a/ork-1-1/out.txt"})

	result, err := archive.DeleteRunArtifacts(context.Background(), "payments", runID)
	if err != nil {
		t.Fatalf("DeleteRunArtifacts: %v", err)
	}
	if result.ObjectsDeleted != 4*perPrefix || result.BytesReclaimed != int64(4*perPrefix*10) {
		t.Errorf("result = %+v, want %d objects and %d bytes", result, 4*perPrefix, 4*perPrefix*10)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// ceil(2500/1000) = 3 per prefix, plus one for the reference markers.
	if want := 4*3 + 1; d.batchDeletes > want {
		t.Errorf("DeleteObjects requests = %d, want at most %d", d.batchDeletes, want)
	}
	for _, n := range d.batchKeys {
		if n > s3DeleteBatch {
			t.Errorf("a DeleteObjects named %d keys, more than %d", n, s3DeleteBatch)
		}
	}
	// The object list itself is the one single delete.
	if d.deletes != 1 {
		t.Errorf("single DeleteObject requests = %d, want 1 (the object list)", d.deletes)
	}
	if len(d.objects) != 0 {
		t.Errorf("%d objects survived cleanup", len(d.objects))
	}
}

func TestS3RunArchive_ARefusedKeyFailsCleanupAndIsNotCounted(t *testing.T) {
	d := newS3Double(t)
	archive := s3CleanupArchive(t, d)
	const runID = "ork-1-2"
	seeded := seedRunScoped(t, d, "payments", runID, 5, nil)
	logs, _ := RunLogPrefix(runID)
	refused := seeded[logs][2]
	d.mu.Lock()
	d.refuse[refused] = true
	d.mu.Unlock()

	result, err := archive.DeleteRunArtifacts(context.Background(), "payments", runID)
	if err == nil {
		t.Fatal("a refused key reported a clean run")
	}
	if !strings.Contains(err.Error(), refused) || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("error %q does not name the refused key and its code", err)
	}
	if result.ObjectsDeleted != 4*5-1 {
		t.Errorf("ObjectsDeleted = %d, want %d (the refused key is not counted)", result.ObjectsDeleted, 4*5-1)
	}
	if _, ok := d.object(refused); !ok {
		t.Error("the refused key is gone")
	}
}
