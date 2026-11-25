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
	// ResumePartFanOutItems: a fan-out whose items are kept or run again.
	ResumePartFanOutItems ResumePart = "fan_out_items"
	// ResumePartSettle: an awaited or composed task kept from its record.
	ResumePartSettle ResumePart = "settle"
	// ResumePartWaitAgain: an awaited task that waits again.
	ResumePartWaitAgain ResumePart = "wait_again"
	// ResumePartSleepUntil: a sleep that wakes at its recorded time.
	ResumePartSleepUntil ResumePart = "sleep_until"
	// ResumePartContinued: a run that had carried on into a later history.
	ResumePartContinued ResumePart = "continued_run"
	// ResumePartAdmittedRun: a run whose tasks hold an identity. Its attested
	// commit keys carry no resume scope yet, so a resume could adopt an
	// earlier attempt's attested commit; identity's P14r-2 (resumed admitted
	// runs) adds the scope segment and deletes this row.
	ResumePartAdmittedRun ResumePart = "admitted_run"
)

// resumePartsUnbuilt is every part not yet built, with what a person reads
// when a resume is refused for it.
var resumePartsUnbuilt = map[ResumePart]string{
	ResumePartFanOutItems: "a fan-out task's items cannot be kept or run again by a resume yet",
	ResumePartSettle:      "an awaited or composed task's recorded result cannot be kept by a resume yet",
	ResumePartWaitAgain:   "an awaited task cannot wait again after a resume yet",
	ResumePartSleepUntil:  "a sleep cannot be kept by a resume yet",
	ResumePartContinued:   "a run that carried on into a later history cannot be resumed yet",
	ResumePartAdmittedRun: "a run whose tasks hold an identity (identities:) cannot be resumed yet; run it again",
}

// ResumeUnitNeed is one unit of a plan as ResumeNeedsUnbuilt reads it.
type ResumeUnitNeed struct {
	Task        string
	Item        int32 // -1: the task itself
	Disposition orkstackv1.ResumeDisposition
}

// ResumeNeedsUnbuilt is the first part (in a fixed order) that a plan of
// units over wf needs and this build has not built, with what a person
// reads; false when every part it needs is built. continued says the base
// had carried on into a later history. ResumeRun's preflight and a reset run
// both ask it, so the two never disagree.
func ResumeNeedsUnbuilt(wf *orkstackv1.Orkflow, units []ResumeUnitNeed, continued bool) (ResumePart, string, bool) {
	need := map[ResumePart]bool{ResumePartContinued: continued}
	eaches := map[string]bool{}
	for _, t := range wf.GetTasks() {
		if t.GetEach() != nil {
			eaches[t.GetId()] = true
		}
		if len(t.GetIdentities()) > 0 {
			need[ResumePartAdmittedRun] = true
		}
	}
	for _, u := range units {
		if u.Item >= 0 || eaches[u.Task] {
			need[ResumePartFanOutItems] = true
		}
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE:
			need[ResumePartSettle] = true
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT:
			need[ResumePartWaitAgain] = true
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL:
			need[ResumePartSleepUntil] = true
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

// resumeUnitsOf are a plan's units as ResumeNeedsUnbuilt reads them.
func resumeUnitsOf(units []ResumeUnitPlan) []ResumeUnitNeed {
	needs := make([]ResumeUnitNeed, 0, len(units))
	for _, u := range units {
		needs = append(needs, ResumeUnitNeed{Task: u.Task, Item: int32(u.Item), Disposition: u.Disposition})
	}
	return needs
}
