package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Every part the table holds is refused, whichever unit needs it, and a
// plan of plain tasks needs none of them.
func TestResumeNeedsUnbuilt_RefusesEveryPartNotYetBuilt(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "build"}, {Id: "convert", Each: &orkstackv1.EachSpec{}}}}
	plain := []ResumeUnitNeed{
		{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN},
		{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT},
		{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED},
	}
	if part, _, refused := ResumeNeedsUnbuilt(wf, plain, false); refused {
		t.Fatalf("a plan of plain tasks was refused for %q", part)
	}
	needs := map[ResumePart]func() ([]ResumeUnitNeed, bool){
		ResumePartFanOutItems: func() ([]ResumeUnitNeed, bool) {
			return []ResumeUnitNeed{{Task: "convert", Item: 3, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT}}, false
		},
		ResumePartSettle: func() ([]ResumeUnitNeed, bool) {
			return []ResumeUnitNeed{{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE}}, false
		},
		ResumePartWaitAgain: func() ([]ResumeUnitNeed, bool) {
			return []ResumeUnitNeed{{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT}}, false
		},
		ResumePartSleepUntil: func() ([]ResumeUnitNeed, bool) {
			return []ResumeUnitNeed{{Task: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL}}, false
		},
		ResumePartContinued: func() ([]ResumeUnitNeed, bool) { return plain, true },
		ResumePartAdmittedRun: func() ([]ResumeUnitNeed, bool) {
			return []ResumeUnitNeed{{Task: "signer", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT}}, false
		},
	}
	admitted := &orkstackv1.Orkflow{Tasks: append(wf.GetTasks(), &orkstackv1.Task{Id: "signer", Identities: []*orkstackv1.IdentityRef{{}}})}
	for part := range resumePartsUnbuilt {
		build, ok := needs[part]
		if !ok {
			t.Fatalf("part %q has no case here: add one", part)
		}
		units, continued := build()
		def := wf
		if part == ResumePartAdmittedRun {
			def = admitted
		}
		got, why, refused := ResumeNeedsUnbuilt(def, units, continued)
		if !refused || got != part || why == "" {
			t.Errorf("a plan needing %q: refused %v for %q (%q)", part, refused, got, why)
		}
	}
}
