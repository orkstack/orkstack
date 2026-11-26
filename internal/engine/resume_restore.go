package engine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/workflow"
)

// A resume of a run that had carried on starts where its last history did
// (E85 §8): the history's saved state is read again, what had ended in it
// stays as it was saved, and every other task starts over as the plan treats
// it, so no wait, half-run fan-out or pause of the saved state goes on.

// sequenceBefore is the last sequence an ending before RUN_RESUMED comes
// after: the saved state's, and below h's scope, which is above every event
// the base published, whatever its plan says.
func (h *resumeHolder) sequenceBefore(carry *orkstackv1.RunContinuation) int64 {
	last := max(0, carry.GetEventSequence())
	if h == nil {
		return last
	}
	return max(last, int64(h.key.Scope)-1)
}

// unread is a resume whose saved state, or definition, the store did not answer
// for in time: nothing of it was carried out, and the run can be resumed again.
func (h *resumeHolder) unread(message string) error {
	return &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: strings.TrimSuffix(message, runStopped) + "; nothing ran again", scope: h.key.Scope}
}

// resumeRows puts every row of a saved state's that the plan treats back to
// one that has not run. The plan leaves exactly what had ended in the saved
// state: a plan made of another state is never carried out. It returns what
// failed, or "".
func (s *runState) resumeRows(run *orkstackv1.Run) string {
	units, err := DecodeResumeUnits(s.wf, s.resume.block(), true)
	if err != nil {
		return err.Error()
	}
	rows := map[string]int{}
	for i, id := range s.order {
		rows[id] = i
	}
	items := 0
	for at, u := range units {
		row := run.TaskRuns[rows[u.Task]]
		left := u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED
		ended := isTerminal(row.GetStatus())
		switch {
		case left && !ended:
			return fmt.Sprintf("its plan leaves %s as it was saved, and it had not ended", u.unit())
		case !left && ended:
			return fmt.Sprintf("its plan keeps %s or runs it again, and it had ended before the run carried on", u.unit())
		}
		if u.Item < 0 {
			items = 0
		} else {
			items++
		}
		if left && (at+1 == len(units) || units[at+1].Item < 0) && items != len(row.GetItems()) {
			return fmt.Sprintf("its plan names %d items of fan-out %q, and it had %d", items, u.Task, len(row.GetItems()))
		}
	}
	for _, u := range units {
		if u.Item < 0 && u.Disposition != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED {
			run.TaskRuns[rows[u.Task]] = s.taskRuns[u.Task]
		}
	}
	return ""
}

// dropCarriedPause sets aside the pause a saved state held (E103 §16): the run
// is resumed unpaused, its last pause's ending stays, and what the cluster
// indexes of it is cleared.
func (s *runState) dropCarriedPause(ctx workflow.Context) {
	if s.paused() {
		paused, expires := true, ""
		if s.pause != nil {
			expires = s.pause.expiresAt.UTC().Format(time.RFC3339Nano)
		}
		s.pauseIndex.paused, s.pauseIndex.expires = &paused, &expires
	}
	s.pause, s.pausedByParent, s.lastCascadeSeq, s.restoredPause = nil, nil, 0, nil
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.syncPauseIndex(ctx)
}

// endUnresumed ends a reset run whose resume was not carried out whole.
func (s *runState) endUnresumed(
	ctx workflow.Context, cancelRun func(), inFlight map[string]workflow.Future, err error,
) (*orkstackv1.Run, error) {
	var unrunnable *resumeLoadFailure
	var unread *readBackFailure
	switch {
	case errors.As(err, &unrunnable):
		return s.endResumeUnrunnable(ctx, err)
	case errors.As(err, &unread):
		return s.fail(ctx, cancelRun, inFlight, unread.task, unread.cause)
	default:
		return s.fail(ctx, cancelRun, inFlight, "", err)
	}
}
