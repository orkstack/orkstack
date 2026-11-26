package engine

import (
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A resumed fan-out (E85 §7.1) keeps the items its plan keeps and runs the
// others again under the resume's scope. Its item rows are there from R, so a
// kept item is read back as any kept row is; a fan-out none of whose items
// rerun is done once they are, and one with any that does is expanded again
// when its needs are in, as any fan-out: over the same array, or the same
// lines, because a fan-out whose source reruns reruns whole. A count that is
// not the base's is refused (ResumeFanOutChangedCode), never matched up.

// ResumeFanOutChangedCode: a resumed fan-out's source no longer has the items
// its kept results are of.
const ResumeFanOutChangedCode = "RESUME_FAN_OUT_CHANGED"

// applyFanOut is step 1 for one fan-out: its own unit, and its items'.
func (s *runState) applyFanOut(ctx workflow.Context, b *orkstackv1.ResumeBlock, node ResumeUnitPlan, items []ResumeUnitPlan) error {
	row, e := s.taskRuns[node.Task], s.eaches[node.Task]
	kept := 0
	for _, u := range items {
		if u.Fresh {
			s.readNoMemo(u.unit())
		}
		if u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT {
			kept++
		}
	}
	switch node.Disposition {
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED:
		if kept > 0 {
			return fmt.Errorf("its block keeps items of the skipped fan-out %q", node.Task)
		}
		return nil
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
		if kept == 0 {
			// Every item runs again: the expansion makes them anew, however
			// many its source has now.
			return nil
		}
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
		if kept != len(items) {
			return fmt.Errorf("its block keeps fan-out %q, %d of whose %d items it does not keep", node.Task, len(items)-kept, len(items))
		}
	}
	row.Items = make([]*orkstackv1.TaskRun, len(items))
	for i, u := range items {
		if u.Task != node.Task || u.Item != i {
			return fmt.Errorf("its block gives %s where item %d of %q belongs", u.unit(), i, node.Task)
		}
		item := &orkstackv1.TaskRun{TaskId: node.Task, Item: proto.Int32(int32(i)),
			Status: orkstackv1.Status_STATUS_PENDING, ResumeDisposition: u.Disposition}
		switch u.Disposition {
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
			item.ReadingBack, item.ResultEpoch = true, int64(u.Epoch)
		case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			item.ResultEpoch = int64(b.GetSeqBase())
		default:
			return fmt.Errorf("its block gives %s the disposition %v, which this build does not carry out", u.unit(), u.Disposition)
		}
		row.Items[i] = item
	}
	if node.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT {
		// Not expanded until its items are read back: an ending before that
		// leaves them as they are.
		e.count = int32(len(items))
		if len(items) == 0 {
			// A fan-out over nothing has no result to read back.
			s.keepFanOut(ctx, node.Task)
		}
	}
	return nil
}

// keptItems says whether fan-out id was resumed with items it keeps.
func (s *runState) keptItems(id string) bool {
	return s.resumed && len(s.taskRuns[id].GetItems()) > 0 && !s.eaches[id].expanded
}

// resumedCount refuses a resumed fan-out whose source counts other than the
// items it keeps results of.
func (s *runState) resumedCount(id string, count int32) (string, bool) {
	if !s.keptItems(id) || int(count) == len(s.taskRuns[id].GetItems()) {
		return "", true
	}
	return fmt.Sprintf("fan-out %q has %d items now and had %d when its kept items ran; run it again whole (--from %s)",
		id, count, len(s.taskRuns[id].GetItems()), id), false
}

// resumeItems takes up a resumed fan-out's items where the base left them, at
// its expansion: the kept ones are settled, and the others run in the order
// of their indexes, before any index is new, of which there is none.
func (s *runState) resumeItems(id string) (kept int32) {
	e := s.eaches[id]
	e.next = e.count
	for i, item := range s.taskRuns[id].GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			kept++
			continue
		}
		e.again = append(e.again, int32(i))
	}
	e.settled = kept
	return kept
}

// settleKeptFanOuts ends, silently, every kept fan-out whose items have all
// been read back.
func (s *runState) settleKeptFanOuts(ctx workflow.Context) {
	for _, id := range s.order {
		row := s.taskRuns[id]
		if s.eaches[id] == nil || !row.GetReadingBack() || !s.allItemsSucceeded(id) {
			continue
		}
		s.keepFanOut(ctx, id)
	}
}

// keepFanOut ends a kept fan-out, whose items it has: the earliest of their
// starts and the latest of their ends are its own.
func (s *runState) keepFanOut(ctx workflow.Context, id string) {
	row, e := s.taskRuns[id], s.eaches[id]
	e.expanded, e.next, e.settled = true, e.count, e.count
	e.limit, e.stride = eachPacing(e.spec, e.count)
	row.Each = &orkstackv1.TaskEach{Expanded: true, Count: e.count}
	for _, item := range row.GetItems() {
		if at := item.GetStartedAt(); at != nil && (row.StartedAt == nil || at.AsTime().Before(row.StartedAt.AsTime())) {
			row.StartedAt = at
		}
		if at := item.GetFinishedAt(); at != nil && (row.FinishedAt == nil || at.AsTime().After(row.FinishedAt.AsTime())) {
			row.FinishedAt = at
		}
	}
	if row.FinishedAt == nil {
		row.FinishedAt = timestamppb.New(workflow.Now(ctx))
	}
	row.Error = nil
	row.Status = orkstackv1.Status_STATUS_SUCCEEDED
	row.ReadingBack, row.Adopted = false, true
}

// cancelUnexpandedItems ends the items a resumed fan-out was to run again and
// never expanded for, when its run ends; kept ones stay as they are.
func (s *runState) cancelUnexpandedItems(ctx workflow.Context, id string) {
	if !s.keptItems(id) {
		return
	}
	for _, item := range s.taskRuns[id].GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_PENDING && !item.GetReadingBack() {
			item.Status = orkstackv1.Status_STATUS_CANCELED
			item.FinishedAt = timestamppb.New(workflow.Now(ctx))
		}
	}
}
