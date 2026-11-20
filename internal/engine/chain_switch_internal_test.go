package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestWriteCheckpoint_CompletesAnAdmittedRootsChain: a switch's checkpoint
// write completes the run's chain record after the checkpoint is stored,
// for the admission it is handed; a completion that fails fails nothing,
// and a checkpoint over its cap, or a run with no admission, asks nothing.
func TestWriteCheckpoint_CompletesAnAdmittedRootsChain(t *testing.T) {
	sha := strings.Repeat("c", 64)
	var asked []string
	var answer error
	a := NewSnapshotActivities(newMemSnapshotStore()).WithChainCompleter(func(_ context.Context, ns, runID, admission string) error {
		asked = append(asked, ns+"/"+runID+"/"+admission)
		return answer
	})
	run := observedRun()
	want := run.GetNamespace() + "/" + run.GetId() + "/" + sha
	if _, err := a.WriteCheckpoint(context.Background(), run, 0, 1<<30, ContinuationObservationInput{}, nil, sha); err != nil {
		t.Fatal(err)
	}
	answer = errors.New("store down")
	if _, err := a.WriteCheckpoint(context.Background(), run, 0, 1<<30, ContinuationObservationInput{}, nil, sha); err != nil {
		t.Fatalf("a failed completion failed the checkpoint: %v", err)
	}
	if _, err := a.WriteCheckpoint(context.Background(), run, 0, 8, ContinuationObservationInput{}, nil, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := a.WriteCheckpoint(context.Background(), run, 0, 1<<30, ContinuationObservationInput{}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 2 || asked[0] != want || asked[1] != want {
		t.Fatalf("completions %v; want two of %s", asked, want)
	}
	// An attempt without the time to spare writes its checkpoint and asks
	// nothing more.
	short, cancel := context.WithTimeout(context.Background(), chainCompleteBound)
	defer cancel()
	if _, err := a.WriteCheckpoint(short, run, 0, 1<<30, ContinuationObservationInput{}, nil, sha); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 2 {
		t.Fatalf("a completion ran with %d asked; the attempt had no time to spare", len(asked))
	}
}

// TestChainAdmission: a switch completes the chain of an admitted run an
// engine started itself, never of a child or of a run a fire started.
func TestChainAdmission(t *testing.T) {
	sha := strings.Repeat("c", 64)
	for _, c := range []struct {
		name   string
		plan   RunPlan
		wanted string
	}{
		{"not admitted", RunPlan{}, ""},
		{"submitted", RunPlan{Identity: &IdentityPlan{AdmissionSHA256: sha}, Origin: &RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT.String()}}, sha},
		{"no origin", RunPlan{Identity: &IdentityPlan{AdmissionSHA256: sha}}, sha},
		{"a child", RunPlan{Identity: &IdentityPlan{AdmissionSHA256: sha}, Origin: &RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN.String()}}, ""},
		{"fired", RunPlan{Identity: &IdentityPlan{AdmissionSHA256: sha}, Origin: &RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE.String()}}, ""},
	} {
		if got := (&runState{plan: c.plan}).chainAdmission(); got != c.wanted {
			t.Errorf("%s: %q; want %q", c.name, got, c.wanted)
		}
	}
}
