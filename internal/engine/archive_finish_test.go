package engine_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRunArchiver_RunFinishedAtDatesARunFromItsArchiveBody: cleanup dates a run
// whose execution is missing from the finish its archive body records, and a
// body that is missing or records no finish dates nothing.
func TestRunArchiver_RunFinishedAtDatesARunFromItsArchiveBody(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	archive := engine.NewRunArchiver(store)
	created := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	finished := created.Add(3 * time.Hour)
	for _, run := range []*orkstackv1.Run{
		{Id: "ork-dated", OrkflowName: "flow", Namespace: "default", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(finished)},
		{Id: "ork-undated", OrkflowName: "flow", Namespace: "default", Status: orkstackv1.Status_STATUS_FAILED,
			CreatedAt: timestamppb.New(created)},
	} {
		if err := archive.Archive(ctx, run, nil); err != nil {
			t.Fatalf("archive %s: %v", run.GetId(), err)
		}
	}
	var reader archivecleanup.RunFinishReader = archive

	got, err := reader.RunFinishedAt(ctx, "ork-dated")
	if err != nil || !got.Equal(finished) {
		t.Fatalf("RunFinishedAt(ork-dated) = %v, %v; want %v", got, err, finished)
	}
	for _, id := range []string{"ork-undated", "ork-never-archived"} {
		if got, err := reader.RunFinishedAt(ctx, id); err == nil {
			t.Fatalf("RunFinishedAt(%s) = %v with no error, want an error", id, got)
		}
	}
}
