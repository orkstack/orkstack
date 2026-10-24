package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// archiveWithoutIndex is a RunArchive that is not an ObjectReferenceLister:
// embedding the interface hides every method the local archive has beyond it.
type archiveWithoutIndex struct{ artifacts.RunArchive }

func TestRunArchiver_ObjectReferencesWithoutAnArchiveIsErrNoRunArchive(t *testing.T) {
	var archiver *engine.RunArchiver
	got, err := archiver.ObjectReferences(context.Background(), "objects/n-eA/"+strings.Repeat("a", 64))
	if !errors.Is(err, engine.ErrNoRunArchive) {
		t.Fatalf("nil archiver ObjectReferences = %v, %v; want ErrNoRunArchive", got, err)
	}
	if got != nil {
		t.Fatalf("nil archiver returned a list %v; no archive is not no reference", got)
	}
	if _, err := archiver.CleanupState(context.Background(), "ork-1-1"); !errors.Is(err, engine.ErrNoRunArchive) {
		t.Fatalf("nil archiver CleanupState = %v; want ErrNoRunArchive", err)
	}
}

func TestRunArchiver_ObjectReferencesWithoutAnIndexIsErrNoObjectIndex(t *testing.T) {
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	archiver := engine.NewRunArchiver(archiveWithoutIndex{local})
	if _, err := archiver.ObjectReferences(context.Background(), "objects/n-eA/"+strings.Repeat("a", 64)); !errors.Is(err, engine.ErrNoObjectIndex) {
		t.Fatalf("ObjectReferences without an index = %v; want ErrNoObjectIndex", err)
	}
	// CleanupState is part of every RunArchive, so it still answers.
	if _, err := archiver.CleanupState(context.Background(), "ork-1-1"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("CleanupState of an uncleaned run = %v; want ErrNotFound", err)
	}
}

// The provenance read means "this run recorded this object as a task output"
// only because the writer records nothing else. This pins that writer
// contract: every task's artifacts, values ref and system artifacts, and no
// other reference on the run (a commit, an input, a non-content-addressed URI).
func TestRunArchiver_ObjectIDsOfAnArchivedRunAreExactlyItsRecordedOutputs(t *testing.T) {
	archiver, _, dir := newArchiveFixture(t)
	run := sampleRun(t, "ork-writer-contract", orkstackv1.Status_STATUS_SUCCEEDED, at(t, "2026-08-14T09:30:15Z"))
	digest := strings.Repeat("a", 64)
	id := func(name string) string { return "objects/n-" + name + "/" + digest }
	run.TaskRuns = []*orkstackv1.TaskRun{
		{
			TaskId:          "first",
			Status:          orkstackv1.Status_STATUS_SUCCEEDED,
			Artifacts:       []*orkstackv1.ArtifactRef{{Name: "a", Uri: "file:///root/" + id("YQ")}, {Name: "mem", Uri: "mem://nowhere"}},
			ValuesRef:       &orkstackv1.ArtifactRef{Name: "values.json", Uri: "file:///root/" + id("dmFsdWVz")},
			SystemArtifacts: []*orkstackv1.ArtifactRef{{Name: "stdout.log", Uri: "s3://bucket/" + id("c3Rkb3V0")}},
			Commit:          &orkstackv1.CommitRef{Uri: "file:///root/" + id("Y29tbWl0"), Sha256: digest},
		},
		{
			TaskId:          "second",
			Status:          orkstackv1.Status_STATUS_SUCCEEDED,
			Artifacts:       []*orkstackv1.ArtifactRef{{Name: "b", Uri: "file:///root/" + id("Yg")}},
			SystemArtifacts: []*orkstackv1.ArtifactRef{{Name: "stderr.log", Uri: "s3://bucket/" + id("c3RkZXJy")}},
		},
	}
	if err := archiver.Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	listKey, err := artifacts.RunObjectListKey(run.GetId())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(listKey)))
	if err != nil {
		t.Fatalf("read object list: %v", err)
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode object list: %v", err)
	}
	want := []string{id("YQ"), id("c3Rkb3V0"), id("dmFsdWVz"), id("Yg"), id("c3RkZXJy")}
	if !slices.Equal(got, want) {
		t.Fatalf("recorded object ids = %v\nwant exactly the task outputs %v", got, want)
	}
	for _, objectID := range want {
		refs, err := archiver.ObjectReferences(context.Background(), objectID)
		if err != nil || !slices.Equal(refs, []string{run.GetId()}) {
			t.Fatalf("ObjectReferences(%s) = %v, %v; want [%s]", objectID, refs, err, run.GetId())
		}
	}
	// The commit is a run-scoped record, not an output: no marker.
	refs, err := archiver.ObjectReferences(context.Background(), id("Y29tbWl0"))
	if err != nil || len(refs) != 0 {
		t.Fatalf("commit object references = %v, %v; want none", refs, err)
	}
}

func TestRunArchiver_CleanupStateReturnsTheImmutableRecord(t *testing.T) {
	archiver, local, _ := newArchiveFixture(t)
	at := time.Date(2026, 1, 9, 3, 4, 5, 0, time.UTC)
	if err := local.MarkArtifactsCleaned(context.Background(), "ork-1-1", at); err != nil {
		t.Fatal(err)
	}
	state, err := archiver.CleanupState(context.Background(), "ork-1-1")
	if err != nil || !state.TemporalDeletedAt.Equal(at) || !state.ArtifactsCleaned {
		t.Fatalf("CleanupState = %+v, %v; want cleaned at %s", state, err, at)
	}
}
