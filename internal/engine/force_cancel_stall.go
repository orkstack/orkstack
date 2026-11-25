package engine

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

// ForceCancelStall is what a force cancel read of a run's history since l0,
// the event its cancel was requested at (design §22.14.2).
type ForceCancelStall struct {
	// Progressing: the run did something since l0, so its grace is extended
	// (until the force cancel's ceiling) rather than the run ended.
	Progressing bool
	// Newest is the newest event id read: the l0 the next window starts from.
	Newest int64
}

// ForceCancelProgress decides, from events read newest first, whether a run
// made progress since event l0. A workflow task completed after l0 is
// progress. So is a window that was not read down to l0 (too much happened to
// call it stuck), and one where a task timed out and none failed: its worker
// went away, which is the cluster's to recover, not a loop. A run whose every
// task since l0 failed did not progress: it is the workflow-task loop a force
// cancel exists to end.
func ForceCancelProgress(newestFirst []*historypb.HistoryEvent, l0 int64) ForceCancelStall {
	if len(newestFirst) == 0 {
		return ForceCancelStall{Newest: l0}
	}
	out := ForceCancelStall{Newest: max(l0, newestFirst[0].GetEventId())}
	timedOut, failed := false, false
	for _, e := range newestFirst {
		if e.GetEventId() <= l0 {
			break
		}
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			out.Progressing = true
			return out
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			timedOut = true
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			failed = true
		}
	}
	reached := newestFirst[len(newestFirst)-1].GetEventId() <= l0+1
	out.Progressing = !reached || timedOut && !failed
	return out
}

// forceCancelSkew is how far the engine's clock may be from the cluster's.
const forceCancelSkew = 5 * time.Second

// forceCancelStalled reads a stall test's window again, newest first down to
// its l0: moved when it shows progress now, or was not read through; early
// when l0 is, by this engine's clock, less than grace ago beyond what clocks
// may differ by. It is asked just before the record is signed, so a run that
// moved after its stall test is not ended, and the record claims nothing the
// cluster does not show.
func forceCancelStalled(newestFirst []*historypb.HistoryEvent, l0 int64, grace time.Duration, now time.Time) (moved, early bool) {
	if len(newestFirst) == 0 || ForceCancelProgress(newestFirst, l0).Progressing {
		return true, false
	}
	oldest := newestFirst[len(newestFirst)-1]
	if oldest.GetEventId() > l0 {
		return true, false
	}
	return false, now.Sub(oldest.GetEventTime().AsTime()) < grace-forceCancelSkew
}
