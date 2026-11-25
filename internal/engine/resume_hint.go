package engine

import orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

// ResumeHintOf is what a run's own record says of resuming it, for a client
// to offer it or say why not: nil for a run that is not failed. It reads no
// history, so it is advice: ResumeRun decides.
func ResumeHintOf(run *orkstackv1.Run) *orkstackv1.ResumeHint {
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		return nil
	}
	h := &orkstackv1.ResumeHint{State: orkstackv1.ResumeHintState_RESUME_HINT_STATE_RESUMABLE, ResumesUsed: int32(len(run.GetResumes()))}
	// A base whose history would have a point to start from.
	r := refuseBase(ResumePlanInput{
		Base: run, Class: ResumeBaseFailed, ResumesUsed: len(run.GetResumes()),
		Facts: ResumeBaseFacts{ResetPoint: ResetPoint{CompletedEventID: 1}},
	})
	if r != nil {
		h.State = orkstackv1.ResumeHintState_RESUME_HINT_STATE_NOT_RESUMABLE
		h.Refusal, h.Message = string(r.Code), r.Message
		return h
	}
	for _, row := range run.GetTaskRuns() {
		a := &orkstackv1.ResumeTaskAction{TaskId: row.GetTaskId(), Resume: isFailedSeed(row)}
		switch row.GetStatus() {
		case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
			a.RetryFrom = row.GetChild() == nil
		}
		if a.Resume || a.RetryFrom {
			h.TaskActions = append(h.TaskActions, a)
		}
	}
	return h
}
