package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Every part of a resume is built: a run whose tasks, or whose child runs'
// tasks, hold an identity is resumed like any other, its results read from
// what the engine recorded.
func TestResumeNeedsUnbuilt_EveryPartIsBuilt(t *testing.T) {
	signs := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "build"}, {Id: "signer", Identities: []*orkstackv1.IdentityRef{{}}}}}
	composes := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "build"}, {Id: "child", Uses: "orkflow:signer@1"}}}
	for name, tc := range map[string]struct {
		wf   *orkstackv1.Orkflow
		plan RunPlan
	}{
		"plain tasks":                               {composes, RunPlan{}},
		"a task that holds an identity":             {signs, RunPlan{Identity: &IdentityPlan{}}},
		"a root whose child holds one, by its plan": {composes, RunPlan{Identity: &IdentityPlan{}}},
	} {
		if part, why, refused := ResumeNeedsUnbuilt(tc.wf, tc.plan); refused {
			t.Errorf("%s: refused for %q (%s)", name, part, why)
		}
	}
	if len(resumePartsUnbuilt) != 0 {
		t.Errorf("parts not built: %v", resumePartsUnbuilt)
	}
}
