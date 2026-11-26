package engine

import (
	"sort"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A resume is built in parts (E85 PR4a–PR4d). Until a part is built, a
// resume that needs it is refused — by ResumeRun's preflight, and by a
// reset run that meets one anyway — so nothing is ever resumed half-way.
// Each later part deleted its own row from resumePartsUnbuilt, the last one a
// run that holds an identity (P14r-3); the table is empty by the release that
// ships resume.

// ResumePart is one thing a resume plan may need beyond plain tasks.
type ResumePart string

// resumePartsUnbuilt is every part not yet built, with what a person reads
// when a resume is refused for it.
var resumePartsUnbuilt = map[ResumePart]string{}

// ResumeNeedsUnbuilt is the first part (in a fixed order) that resuming a run
// of wf under plan needs and this build has not built, with what a person
// reads; false when every part it needs is built. ResumeRun's preflight and a
// reset run both ask it, so the two never disagree.
func ResumeNeedsUnbuilt(_ *orkstackv1.Orkflow, _ RunPlan) (ResumePart, string, bool) {
	need := map[ResumePart]bool{}
	parts := make([]string, 0, len(resumePartsUnbuilt))
	for p := range resumePartsUnbuilt {
		parts = append(parts, string(p))
	}
	sort.Strings(parts)
	for _, p := range parts {
		if need[ResumePart(p)] {
			return ResumePart(p), resumePartsUnbuilt[ResumePart(p)], true
		}
	}
	return "", "", false
}
