package engine

import orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

// ResumeHintOf is what a run's own record says of resuming it, for a client
// to offer it or say why not: nil for a run that is not failed. It reads no
// history, so it is advice: ResumeRun decides.
func ResumeHintOf(run *orkstackv1.Run) *orkstackv1.ResumeHint {
	if run.GetStatus() == orkstackv1.Status_STATUS_CANCELED && run.GetError().GetCode() == RunForceCanceledCode {
		return forceCanceledHint(run)
	}
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

// forceCanceledHint is a force-cancelled run's: it is resumed from a task the
// person picks (design §23, G-a), so no task is offered to resume from, and
// every task is offered to retry from.
func forceCanceledHint(run *orkstackv1.Run) *orkstackv1.ResumeHint {
	h := &orkstackv1.ResumeHint{State: orkstackv1.ResumeHintState_RESUME_HINT_STATE_RESUMABLE_FROM_TASK, ResumesUsed: int32(len(run.GetResumes()))}
	r := refuseBase(ResumePlanInput{
		Base: run, Class: ResumeBaseForceCanceled, ResumesUsed: len(run.GetResumes()),
		Facts: ResumeBaseFacts{ResetPoint: ResetPoint{CompletedEventID: 1}},
	})
	// Asking for the task to start from is what this state says.
	if r != nil && r.Code != RefuseFromTaskRequired {
		h.State = orkstackv1.ResumeHintState_RESUME_HINT_STATE_NOT_RESUMABLE
		h.Refusal, h.Message = string(r.Code), r.Message
		return h
	}
	for _, row := range run.GetTaskRuns() {
		if row.GetChild() == nil {
			h.TaskActions = append(h.TaskActions, &orkstackv1.ResumeTaskAction{TaskId: row.GetTaskId(), RetryFrom: true})
		}
	}
	return h
}
