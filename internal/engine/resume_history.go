package engine

import (
	"errors"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RefuseResumeFromHistory is a resume's first phase (design §26.4): the
// refusals that no saved result and no clock can change. It reads
// in.Evidence nowhere, so ResumeRun asks it before it reads storage or waits
// for anything to stop. A *ResumeRefusal says the run cannot be resumed as
// asked; any other error says its input is inconsistent.
//
// Whatever it refuses, PlanResume refuses too for any evidence: it works on
// the part of S that is certain, which evidence can only widen.
func RefuseResumeFromHistory(in ResumePlanInput) error {
	if in.Definition == nil || in.Base == nil {
		return errors.New("engine: refuse resume: definition and base are required")
	}
	if r := refuseBase(in); r != nil {
		return r
	}
	in.Evidence = ResumeEvidence{}
	p, err := newPlanner(in)
	if err != nil {
		return err
	}
	if r := p.refuseRequest(in.Action, in.FromTask); r != nil {
		return r
	}
	p.addCertain()
	if r := p.refuseComposed(); r != nil {
		return r
	}
	return nil
}

// addCertain puts in S what reruns whatever storage holds: each task or item
// that runs a container and failed, or, in a FAILED base, never finished;
// the chosen task; and what the closure rules take from them. An await or a
// composed task that did not succeed is left out, because an answer or a
// child's outputs on record may settle it, and a force-cancelled base's
// unfinished task, because it may have committed.
func (p *planner) addCertain() {
	for _, id := range p.order {
		if p.in.Facts.Earlier[id] {
			continue
		}
		task := p.tasks[id]
		for _, u := range task.units() {
			row := task.unitRow(u)
			if task.unitKind(u) != kindContainer || keptCandidate(row) ||
				row.GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
				continue
			}
			switch {
			case isFailedSeed(row):
				p.add(u, CategoryRerunFailed)
			case p.in.Class == ResumeBaseFailed:
				p.add(u, CategoryRerunUnfinished)
			}
		}
	}
	if p.in.Action == ResumeActionRetryFrom {
		p.addTaskWhole(p.in.FromTask, CategoryRerunSelected)
	}
	p.close()
}
