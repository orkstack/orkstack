package artifacts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// newReferenceArchive is a local run archive over a fresh directory.
func newReferenceArchive(t *testing.T) *LocalRunArchive {
	t.Helper()
	archive, err := NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	return archive
}

// archiveWithObjects writes one run record holding markers on objectIDs,
// through PutRun so the markers are the production writer's.
func archiveWithObjects(t *testing.T, archive RunArchive, runID string, objectIDs ...string) {
	t.Helper()
	rec := RunRecord{
		RunID: runID, Namespace: "default", Orkflow: "flow", Status: "succeeded",
		Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ObjectIDs: objectIDs,
		JSONL: []byte(runID + "\n"),
	}
	if err := archive.PutRun(context.Background(), rec); err != nil {
		t.Fatalf("PutRun(%s): %v", runID, err)
	}
}

func TestLocalRunArchive_ListObjectReferencesReturnsEveryRunHoldingAMarker(t *testing.T) {
	archive := newReferenceArchive(t)
	shared := objectKey("report.txt", strings.Repeat("a", 64))
	private := map[string]string{
		"ork-1-1": objectKey("one.txt", strings.Repeat("b", 64)),
		"ork-1-2": objectKey("two.txt", strings.Repeat("c", 64)),
		"ork-1-3": objectKey("three.txt", strings.Repeat("d", 64)),
	}
	for runID, own := range private {
		archiveWithObjects(t, archive, runID, shared, own)
	}

	got, err := archive.ListObjectReferences(context.Background(), shared)
	if err != nil {
		t.Fatalf("ListObjectReferences(shared): %v", err)
	}
	if want := []string{"ork-1-1", "ork-1-2", "ork-1-3"}; !slices.Equal(got, want) {
		t.Fatalf("shared references = %v, want %v (key order)", got, want)
	}
	for runID, own := range private {
		got, err := archive.ListObjectReferences(context.Background(), own)
		if err != nil {
			t.Fatalf("ListObjectReferences(%s): %v", own, err)
		}
		if !slices.Equal(got, []string{runID}) {
			t.Fatalf("private references of %s = %v, want [%s]", runID, got, runID)
		}
	}
}

func TestLocalRunArchive_ListObjectReferencesIsEmptyForAnUnreferencedObject(t *testing.T) {
	archive := newReferenceArchive(t)
	archiveWithObjects(t, archive, "ork-1-1", objectKey("report.txt", strings.Repeat("a", 64)))

	got, err := archive.ListObjectReferences(context.Background(), objectKey("never.txt", strings.Repeat("e", 64)))
	if err != nil {
		t.Fatalf("ListObjectReferences(unreferenced): %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("unreferenced object = %#v, want an empty non-nil slice", got)
	}
}

func TestLocalRunArchive_ListObjectReferencesDropsAReleasedRun(t *testing.T) {
	archive := newReferenceArchive(t)
	shared := objectKey("report.txt", strings.Repeat("a", 64))
	for _, runID := range []string{"ork-1-1", "ork-1-2", "ork-1-3"} {
		archiveWithObjects(t, archive, runID, shared)
	}
	if _, err := archive.DeleteRunArtifacts(context.Background(), "default", "ork-1-2"); err != nil {
		t.Fatalf("DeleteRunArtifacts: %v", err)
	}

	got, err := archive.ListObjectReferences(context.Background(), shared)
	if err != nil {
		t.Fatalf("ListObjectReferences: %v", err)
	}
	if want := []string{"ork-1-1", "ork-1-3"}; !slices.Equal(got, want) {
		t.Fatalf("references after release = %v, want %v", got, want)
	}
}

func TestLocalRunArchive_ListObjectReferencesRefusesMoreThanTheBound(t *testing.T) {
	archive := newReferenceArchive(t)
	shared := objectKey("report.txt", strings.Repeat("a", 64))
	for i := 0; i < MaxObjectReferences+1; i++ {
		archiveWithObjects(t, archive, fmt.Sprintf("ork-1-%04d", i), shared)
	}

	got, err := archive.ListObjectReferences(context.Background(), shared)
	if !errors.Is(err, ErrTooManyReferences) {
		t.Fatalf("ListObjectReferences over the bound = %d ids, %v; want ErrTooManyReferences", len(got), err)
	}
	if got != nil {
		t.Fatalf("a refused listing returned %d ids; a list cut short is not the list", len(got))
	}

	// Exactly at the bound is still an answer.
	bounded := objectKey("bounded.txt", strings.Repeat("f", 64))
	for i := 0; i < MaxObjectReferences; i++ {
		archiveWithObjects(t, archive, fmt.Sprintf("ork-2-%04d", i), bounded)
	}
	got, err = archive.ListObjectReferences(context.Background(), bounded)
	if err != nil || len(got) != MaxObjectReferences {
		t.Fatalf("ListObjectReferences at the bound = %d ids, %v; want %d", len(got), err, MaxObjectReferences)
	}
}

func TestLocalRunArchive_ListObjectReferencesRefusesANonCanonicalObjectID(t *testing.T) {
	archive := newReferenceArchive(t)
	for _, objectID := range []string{"not/an/object", "", "objects/n-x/not-a-digest"} {
		if _, err := archive.ListObjectReferences(context.Background(), objectID); !errors.Is(err, ErrInvalidURI) {
			t.Fatalf("ListObjectReferences(%q) = %v, want ErrInvalidURI", objectID, err)
		}
	}
}

// A marker whose run segment does not parse is not this package's key: it is
// skipped, as ListRuns skips foreign index keys.
func TestLocalRunArchive_ListObjectReferencesSkipsAForeignKey(t *testing.T) {
	archive := newReferenceArchive(t)
	shared := objectKey("report.txt", strings.Repeat("a", 64))
	archiveWithObjects(t, archive, "ork-1-1", shared)
	if err := archive.writeAtomic(runObjectRefPrefix(shared)+"~!!notbase64", nil); err != nil {
		t.Fatal(err)
	}
	got, err := archive.ListObjectReferences(context.Background(), shared)
	if err != nil || !slices.Equal(got, []string{"ork-1-1"}) {
		t.Fatalf("ListObjectReferences with a foreign key = %v, %v; want [ork-1-1]", got, err)
	}
}

var (
	_ ObjectReferenceLister = (*LocalRunArchive)(nil)
	_ ObjectReferenceLister = (*S3RunArchive)(nil)
)
