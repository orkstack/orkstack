package api

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run that forwards what it received holds the object twice: under the
// task that recorded it, then as _inputs, after every task. A damaged
// received record is passed over, never a panic.
func TestReferencesIn_ReceivedArtifactsFollowTheTasks(t *testing.T) {
	uri := "s3://bucket/orkflows/fit/ork-0/build/data.tar/" + strings.Repeat("a", 64)
	id, ok := artifacts.ObjectIDFromURI(uri)
	if !ok {
		t.Fatalf("%s is no object URI", uri)
	}
	ref := func() *orkstackv1.ArtifactRef { return &orkstackv1.ArtifactRef{Name: "data", Uri: uri} }
	run := &orkstackv1.Run{Id: "ork-child",
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "damaged"}, {Name: "data", Artifact: ref()}},
		TaskRuns:       []*orkstackv1.TaskRun{{TaskId: "other"}, {TaskId: "train", Artifacts: []*orkstackv1.ArtifactRef{ref()}}},
	}
	var got []string
	for _, r := range referencesIn(run, id) {
		got = append(got, r.GetTaskId())
	}
	if want := "train " + dsl.InputsProducer; strings.Join(got, " ") != want {
		t.Fatalf("references = %v, want %s", got, want)
	}
}

// A reference recorded by a shared Orkflow's child run names the Orkflow's
// owner, so it is not read as the running namespace's own of that name.
func TestReferencesIn_NameASharedOrkflowsOwner(t *testing.T) {
	uri := "s3://bucket/orkflows/build/ork-0/compile/app.tar/" + strings.Repeat("a", 64)
	id, _ := artifacts.ObjectIDFromURI(uri)
	for _, owner := range []string{"platform", ""} {
		refs := referencesIn(&orkstackv1.Run{Id: "ork-1", Namespace: "payments", OrkflowName: "build", OrkflowNamespace: owner,
			TaskRuns: []*orkstackv1.TaskRun{{TaskId: "compile", Artifacts: []*orkstackv1.ArtifactRef{{Name: "app.tar", Uri: uri}}}}}, id)
		if len(refs) != 1 || refs[0].GetOrkflowName() != "build" || refs[0].GetOrkflowNamespace() != owner {
			t.Errorf("owner %q: references = %v", owner, refs)
		}
	}
}
