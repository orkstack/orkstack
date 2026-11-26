package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Every part the table holds is refused, whichever unit needs it, and a
// plan of plain tasks needs none of them.
func TestResumeNeedsUnbuilt_RefusesEveryPartNotYetBuilt(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "build"}, {Id: "child", Uses: "orkflow:signer@1"}}}
	if part, _, refused := ResumeNeedsUnbuilt(wf, RunPlan{}); refused {
		t.Fatalf("a run of plain tasks was refused for %q", part)
	}
	signs := &orkstackv1.Orkflow{Tasks: append(wf.GetTasks(), &orkstackv1.Task{Id: "signer", Identities: []*orkstackv1.IdentityRef{{}}})}
	for name, tc := range map[string]struct {
		wf   *orkstackv1.Orkflow
		plan RunPlan
		want ResumePart
	}{
		"a task that holds an identity":             {wf: signs, want: ResumePartAdmittedRun},
		"a root whose child holds one, by its plan": {wf: wf, plan: RunPlan{Identity: &IdentityPlan{}}, want: ResumePartAdmittedRun},
	} {
		if got, why, refused := ResumeNeedsUnbuilt(tc.wf, tc.plan); !refused || got != tc.want || why == "" {
			t.Errorf("%s: refused %v for %q (%q), want %q", name, refused, got, why, tc.want)
		}
	}
	for part := range resumePartsUnbuilt {
		if part != ResumePartAdmittedRun {
			t.Errorf("part %q has no case here: add one", part)
		}
	}
}
