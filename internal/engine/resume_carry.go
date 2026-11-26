package engine

import (
	"cmp"
	"fmt"
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// What a resumed run carries of its resume into a fresh history: the resume
// itself in the carry, and in the saved state the units it still runs again
// that read no memo. Its rows say everything else.

// resumeCarry is the resume a switch carries; nil for a run never resumed.
func (s *runState) resumeCarry() *orkstackv1.RunResumeCarry {
	if !s.resumed {
		return nil
	}
	return &orkstackv1.RunResumeCarry{
		SeqBase: uint64(s.resumeEntry.GetSequence()), ResumedAfter: s.resumeEntry.GetResumedAfterSequence(),
		ResumeCount: s.resumeCount, Resume: s.resumeEntry,
	}
}

// resumeCarryFault is why carry's resume is not one a run carries, or "".
func resumeCarryFault(carry *orkstackv1.RunContinuation) string {
	r := carry.GetResume()
	if r == nil {
		return ""
	}
	e := r.GetResume()
	switch {
	case e == nil:
		return "it says the run was resumed and not how"
	case r.GetSeqBase() < 1 || r.GetSeqBase() > maxResumeScope || int64(r.GetSeqBase()) > carry.GetEventSequence():
		return fmt.Sprintf("it says the run was resumed at event %d, and its last is %d", r.GetSeqBase(), carry.GetEventSequence())
	case uint64(e.GetSequence()) != r.GetSeqBase() || e.GetScope() != r.GetSeqBase():
		return fmt.Sprintf("it says the run was resumed at event %d and at event %d under scope %d", r.GetSeqBase(), e.GetSequence(), e.GetScope())
	case e.GetResumedAfterSequence() != r.GetResumedAfter() || r.GetResumedAfter() < 1 || r.GetResumedAfter() >= int64(r.GetSeqBase()):
		return fmt.Sprintf("it says the resume at event %d set aside an ending at %d and at %d", r.GetSeqBase(), r.GetResumedAfter(), e.GetResumedAfterSequence())
	case r.GetResumeCount() < 1 || r.GetResumeCount() > MaxResumesPerRun:
		return fmt.Sprintf("it says the run was resumed %d times", r.GetResumeCount())
	case e.GetAt().CheckValid() != nil || e.GetEnded() != nil:
		return "it does not say when the run was resumed, or says how it ended since"
	}
	return ""
}

// adoptResumeCarry takes up the resume a carry holds, which validateCarry
// checked.
func (s *runState) adoptResumeCarry(carry *orkstackv1.RunContinuation) {
	if r := carry.GetResume(); r != nil {
		s.resumed, s.resumeEntry, s.resumeCount = true, r.GetResume(), r.GetResumeCount()
	}
}

// resumeState is what a switch's saved state holds of a resume: the units
// still to run that read no memo. A unit an interruption put back is one.
func (s *runState) resumeState() *orkstackv1.RunResumeState {
	if len(s.fresh) == 0 {
		return nil
	}
	items := map[string][]int32{}
	for u := range s.fresh {
		if u.Item >= 0 {
			items[u.Task] = append(items[u.Task], int32(u.Item))
		}
	}
	out := &orkstackv1.RunResumeState{}
	for i, id := range s.order {
		row := s.taskRuns[id]
		if isTerminal(row.GetStatus()) {
			continue
		}
		f := &orkstackv1.ResumeFreshUnits{Task: int32(i), All: s.fresh[nodeUnit(id)]}
		if !f.All {
			f.Items = freshRanges(row, items[id])
		}
		if f.All || len(f.Items) > 0 {
			out.Fresh = append(out.Fresh, f)
		}
	}
	return out
}

// freshRanges are the items of row still to run among items, as ranges.
func freshRanges(row *orkstackv1.TaskRun, items []int32) []*orkstackv1.ResumeItemRange {
	slices.SortFunc(items, cmp.Compare[int32])
	var out []*orkstackv1.ResumeItemRange
	for _, i := range items {
		if rows := row.GetItems(); len(rows) > 0 && (int(i) >= len(rows) || isTerminal(rows[i].GetStatus())) {
			continue
		}
		if n := len(out); n > 0 && out[n-1].GetFirst()+out[n-1].GetCount() == i {
			out[n-1].Count++
			continue
		}
		out = append(out, &orkstackv1.ResumeItemRange{First: i, Count: 1})
	}
	return out
}

// adoptResumeState takes up the units a saved state says read no memo, once
// its rows are adopted, and refuses one that is not of a cached task's that
// still runs again: it returns what failed, or "".
func (s *runState) adoptResumeState(state *orkstackv1.RunResumeState) string {
	if state == nil {
		return ""
	}
	if !s.resumed || len(state.GetFresh()) == 0 {
		return "it says what a resume runs again, and names no resume or no unit"
	}
	last := int32(-1)
	for _, f := range state.GetFresh() {
		if f.GetTask() <= last || int(f.GetTask()) >= len(s.order) {
			return fmt.Sprintf("it names task %d of a resume out of order, or of no task", f.GetTask())
		}
		last = f.GetTask()
		id := s.order[f.GetTask()]
		row := s.taskRuns[id]
		if s.tasks[id].GetCache() == nil || row.GetResumeDisposition() != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN {
			return fmt.Sprintf("it has task %q read no memo, and it is not a cached task a resume runs again", id)
		}
		if isTerminal(row.GetStatus()) {
			return fmt.Sprintf("it has task %q read no memo, and it ran", id)
		}
		if f.GetAll() == (len(f.GetItems()) > 0) || len(f.GetItems()) > 0 && s.eaches[id] == nil {
			return fmt.Sprintf("it has neither task %q nor its items read no memo", id)
		}
		if f.GetAll() {
			s.readNoMemo(nodeUnit(id))
			continue
		}
		if what := s.adoptFreshItems(id, row, f.GetItems()); what != "" {
			return what
		}
	}
	return ""
}

// adoptFreshItems takes up the items of fan-out id that read no memo.
func (s *runState) adoptFreshItems(id string, row *orkstackv1.TaskRun, ranges []*orkstackv1.ResumeItemRange) string {
	next, most := int64(0), int64(s.tasks[id].GetEach().GetMaxItems())
	if rows := row.GetItems(); len(rows) > 0 {
		most = int64(len(rows))
	}
	for n, r := range ranges {
		first, end := int64(r.GetFirst()), int64(r.GetFirst())+int64(r.GetCount())
		// Two ranges that touch are one.
		if r.GetCount() < 1 || first < next || n > 0 && first == next || end > most {
			return fmt.Sprintf("it has items %d to %d of fan-out %q read no memo, out of order or of none", first, end-1, id)
		}
		next = end
		for i := first; i < end; i++ {
			if rows := row.GetItems(); len(rows) > 0 {
				item := rows[i]
				if isTerminal(item.GetStatus()) || item.GetResumeDisposition() != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN {
					return fmt.Sprintf("it has item %d of fan-out %q read no memo, and it ran or is kept", i, id)
				}
			}
			s.readNoMemo(ResumeUnit{Task: id, Item: int(i)})
		}
	}
	return ""
}

// keptFanOut reports whether row is a fan-out a resume kept items of: their
// results are of an earlier execution, so they were never dispatched here and
// what runs again is not a prefix of them.
func keptFanOut(row *orkstackv1.TaskRun) bool {
	return slices.ContainsFunc(row.GetItems(), func(item *orkstackv1.TaskRun) bool { return item.GetAdopted() })
}

// validKeptItems checks the items of a restored fan-out a resume kept items
// of: each is kept, has ended, or waits to run.
func validKeptItems(id string, row *orkstackv1.TaskRun) string {
	expanded := row.GetEach().GetExpanded()
	for i, item := range row.GetItems() {
		if item.GetItem() != int32(i) {
			return fmt.Sprintf("fan-out %q item %d is out of order", id, i)
		}
		dispatched := item.GetScheduledAt() != nil
		ok := false
		switch status := item.GetStatus(); {
		case item.GetAdopted():
			ok = status == orkstackv1.Status_STATUS_SUCCEEDED && !dispatched &&
				item.GetResumeDisposition() == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT
		case item.GetResumeDisposition() != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
		case status == orkstackv1.Status_STATUS_PENDING:
			ok = !dispatched || expanded && waitingToRunAgain(item)
		default:
			ok = expanded && dispatched && isTerminal(status)
		}
		if !ok {
			return fmt.Sprintf("fan-out %q item %d is %s, neither kept nor run again", id, i, statusWord(item.GetStatus()))
		}
	}
	return ""
}

// restoreKeptFanOut takes up a restored, expanded fan-out a resume kept items
// of, as its expansion did (resumeItems): no index is new, and what was never
// dispatched runs in the order of its indexes.
func restoreKeptFanOut(e *eachState, row *orkstackv1.TaskRun) {
	e.next, e.settled = e.count, 0
	for i, item := range row.GetItems() {
		switch {
		case isTerminal(item.GetStatus()):
			e.settled++
		case item.GetScheduledAt() == nil:
			e.again = append(e.again, int32(i))
		}
	}
}
