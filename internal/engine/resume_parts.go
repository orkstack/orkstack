package engine

import (
	"sort"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A resume is built in parts (E85 PR4a–PR4d). Until a part is built, a
// resume that needs it is refused — by ResumeRun's preflight, and by a
// reset run that meets one anyway — so nothing is ever resumed half-way.
// Each later part deletes its own row from resumePartsUnbuilt; the table is
// empty by the release that ships resume (docs/RESUME.md lists its rows).

// ResumePart is one thing a resume plan may need beyond plain tasks.
type ResumePart string

const (
	// ResumePartAdmittedRun: a run of a tree that holds an identity. Its attested
	// commit keys carry no resume scope yet, so a resume could adopt an
	// earlier attempt's attested commit; identity's P14r-2 (resumed admitted
	// runs) adds the scope segment and deletes this row.
	ResumePartAdmittedRun ResumePart = "admitted_run"
)

// resumePartsUnbuilt is every part not yet built, with what a person reads
// when a resume is refused for it.
var resumePartsUnbuilt = map[ResumePart]string{
	ResumePartAdmittedRun: "a run whose tasks, or whose child runs' tasks, hold an identity (identities:) cannot be resumed yet; run it again",
}

// ResumeNeedsUnbuilt is the first part (in a fixed order) that resuming a run
// of wf under plan needs and this build has not built, with what a person
// reads; false when every part it needs is built. A run is admitted by its
// plan, whatever its own definition declares: a root whose child holds an
// identity records its results too, and one of them is never kept without its
// record. ResumeRun's preflight and a reset run both ask it, so the two never
// disagree.
func ResumeNeedsUnbuilt(wf *orkstackv1.Orkflow, plan RunPlan) (ResumePart, string, bool) {
	need := map[ResumePart]bool{ResumePartAdmittedRun: plan.Identity != nil}
	for _, t := range wf.GetTasks() {
		if len(t.GetIdentities()) > 0 {
			need[ResumePartAdmittedRun] = true
		}
	}
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
