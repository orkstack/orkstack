package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	for name, body := range map[string][]byte{
		filepath.Join(root, filepath.FromSlash(logPrefix), "task", "attempt", "chunk"):  []byte("logs"),
		filepath.Join(root, filepath.FromSlash(commitPrefix), "task", CommitObjectName): []byte("commit"),
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
	if result.ObjectsDeleted != 2 || result.BytesReclaimed != int64(len("logs")+len("commit")) {
		t.Fatalf("result = %+v", result)
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
