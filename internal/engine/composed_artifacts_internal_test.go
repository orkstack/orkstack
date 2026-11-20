package engine

import (
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRunObjectIDs_IncludesEveryClaimedObject: what a child received and what
// a composed task passed are objects the run claimed while open, so its
// archive lists them and its retirement releases the claims.
func TestRunObjectIDs_IncludesEveryClaimedObject(t *testing.T) {
	received := "s3://bucket/orkflows/fit/ork-0/build/data.tar/" + strings.Repeat("a", 64)
	passed := "s3://bucket/orkflows/fit/ork-1/build/model.bin/" + strings.Repeat("b", 64)
	run := &orkstackv1.Run{
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "data", Artifact: &orkstackv1.ArtifactRef{Uri: received}}},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "train", Child: &orkstackv1.TaskChild{
			InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "data", Artifact: &orkstackv1.ArtifactRef{Uri: passed}}}}}},
	}
	ids := runObjectIDs(run)
	for _, uri := range []string{received, passed} {
		id, ok := artifacts.ObjectIDFromURI(uri)
		if !ok {
			t.Fatalf("%s is no object URI", uri)
		}
		if !slices.Contains(ids, id) {
			t.Fatalf("runObjectIDs = %v, missing %s", ids, id)
		}
	}
}

// TestReclaimObjects_IncludesEveryClaimedObject: the GC's walk lists the same
// claimed objects runObjectIDs does, so its candidates are what PutRun marked.
func TestReclaimObjects_IncludesEveryClaimedObject(t *testing.T) {
	received := "s3://bucket/orkflows/fit/ork-0/build/data.tar/" + strings.Repeat("a", 64)
	passed := "s3://bucket/orkflows/fit/ork-1/build/model.bin/" + strings.Repeat("b", 64)
	run := &orkstackv1.Run{
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "data", Artifact: &orkstackv1.ArtifactRef{Uri: received}}},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "train", Child: &orkstackv1.TaskChild{
			InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "data", Artifact: &orkstackv1.ArtifactRef{Uri: passed}}}}}},
	}
	var reclaimIDs []string
	for _, o := range reclaimObjects(run) {
		reclaimIDs = append(reclaimIDs, o.ID)
	}
	if ids := runObjectIDs(run); !slices.Equal(reclaimIDs, ids) || len(ids) != 2 {
		t.Fatalf("reclaimObjects %v, runObjectIDs %v: want the same two claimed objects", reclaimIDs, ids)
	}
}
