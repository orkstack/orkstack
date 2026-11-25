package engine

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// No workflow code of a force-canceled run runs after its termination, so the
// run's ending is built from its last recorded state and its termination
// record (AGENTS.md §7, the sequence carve-out). ForceCanceledTerminalEvent is
// that one function: the reader, the archive, the evidence bundle and the
// force cancel that publishes the event all build it here, to the same bytes:
// the event is a pure function of the two, so it carries no signature. The
// record does.

// ForceCanceledTerminalEvent is the terminal event at seq of a run a force
// cancel ended, and the run as it ended, from last, its last recorded state,
// which is not changed. The run reads closed's outcome whatever it said
// before, canceled by the record's principal, verified only when the record
// says so and is itself verified; its task rows stay as of its last event, but for a row still reading
// back a resumed run's result, which no longer can (CANCELED
// RESUME_NOT_READ_BACK).
func ForceCanceledTerminalEvent(last *orkstackv1.Run, closed ForceCancelClose, seq int64, closedAt time.Time) (*orkstackv1.Event, *orkstackv1.Run) {
	run := proto.Clone(last).(*orkstackv1.Run)
	status, cause := closed.Outcome("")
	run.Status, run.Error, run.FinishedAt = status, cause, timestamppb.New(closedAt)
	run.Queued, run.DispatchHold = nil, orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED // it waits for nothing
	if p := run.GetPause(); p != nil {
		// Its pause ended with it, as any run's does (endAtRunEnd).
		p.End, p.EndedAt = orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED, timestamppb.New(closedAt)
		if at := p.GetExpiresAt(); at.IsValid() && !closedAt.Before(at.AsTime()) {
			p.End, p.EndedAt = orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT, at
		}
		run.Pause, run.LastPause = nil, p
	}
	run.PausedByParent = nil
	run.ForceCancelRecord = closed.Carried
	run.CanceledBy = closed.Record.Principal
	run.CanceledByVerified = closed.Verification == ForceCancelVerified && closed.Record.PrincipalVerified
	for _, row := range run.GetTaskRuns() {
		if row.GetReadingBack() {
			row.ReadingBack = false
			row.Status = orkstackv1.Status_STATUS_CANCELED
			row.Error = &orkstackv1.Error{Code: ReasonResumeNotReadBack,
				Message:  "its result was not read back before the run was force-canceled",
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
		}
	}
	return &orkstackv1.Event{
		RunId:     run.GetId(),
		Sequence:  seq,
		Type:      terminalEventFor(status),
		Status:    status,
		Message:   cause.GetMessage(),
		Timestamp: timestamppb.New(closedAt),
		RequestId: closed.Record.RequestID,
	}, run
}
