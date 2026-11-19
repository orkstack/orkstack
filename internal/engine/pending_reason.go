package engine

import (
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TaskRun.pending_reason (E92, design §4.9) is derived on read and never
// stored: GetRun says why a PENDING task, or item, is not running yet. This
// file holds the reason a row itself carries — an interruption's not_before
// still ahead — which needs nothing but the row and the reader's clock.

// applyPendingReasons sets pending_reason on every PENDING row and item of a
// run still in flight whose interruption's not_before is after now. A
// finished run, and every other row, is left as it is.
func applyPendingReasons(run *orkstackv1.Run, now time.Time) {
	applyPendingReasonsWith(run, now, dispatchHeld)
}

// applyPendingReasonsWith is applyPendingReasons with how a row is told held
// by its run (dispatchHeld in production; a fixture's marks in a test).
func applyPendingReasonsWith(run *orkstackv1.Run, now time.Time, held func(*orkstackv1.TaskRun) bool) {
	if run == nil || isTerminal(run.GetStatus()) {
		return
	}
	hold := run.GetDispatchHold()
	paused := run.GetPause() != nil || run.GetPausedByParent() != nil
	reason := func(row *orkstackv1.TaskRun) string {
		if paused {
			if why := pausedPendingReason(row, now); why != "" {
				return why
			}
		}
		return rowPendingReason(row, hold, held(row), now)
	}
	for _, row := range run.GetTaskRuns() {
		row.PendingReason = reason(row)
		for _, item := range row.GetItems() {
			item.PendingReason = reason(item)
		}
	}
}

// pausedPendingReason is the reason of a unit an interruption put back, while
// its run is paused (by its own pause or its parent's): it starts again only
// once the run is unpaused, so its wait is named beside the pause rather than
// read as the time it runs again. Every other row keeps its own reason: a
// paused run still lets what it dispatched, and an open pin's members, run,
// so a pause is no reason a row of those waits. "" for a row that is not such
// a unit.
func pausedPendingReason(row *orkstackv1.TaskRun, now time.Time) string {
	nb := row.GetInterruptions().GetNotBefore()
	if row.GetStatus() != orkstackv1.Status_STATUS_PENDING || nb == nil {
		return ""
	}
	const reason = "held: the run is paused"
	if nb.AsTime().After(now) {
		return fmt.Sprintf("%s (its wait to run again ends at %s)", reason, nb.AsTime().UTC().Format("15:04:05 UTC"))
	}
	return reason
}

func rowPendingReason(row *orkstackv1.TaskRun, hold orkstackv1.RunDispatchHold, held bool, now time.Time) string {
	if reason := interruptionPendingReason(row, now); reason != "" {
		return reason
	}
	// A held row waits PENDING, or is a fan-out still RUNNING whose next
	// items the run holds back (the interpreter marks the fan-out's row,
	// not its items).
	waiting := row.GetStatus() == orkstackv1.Status_STATUS_PENDING ||
		row.GetStatus() == orkstackv1.Status_STATUS_RUNNING && len(row.GetItems()) > 0
	if !waiting {
		return ""
	}
	return holdPendingReason(hold, held)
}

// dispatchHeld reports whether the interpreter marked row as ready work its
// run held back (TaskRun.dispatch_held): a PENDING row, or a fan-out still
// RUNNING whose next items wait. Only a marked row is held: a PENDING row the
// run did not mark waits on its needs, which the run-wide Run.dispatch_hold
// cannot tell. A run from before the field reads false, so says nothing.
func dispatchHeld(row *orkstackv1.TaskRun) bool { return row.GetDispatchHeld() }

// holdPendingReason is the reason a row the run itself held back waits, ""
// for a row it did not hold or a run that holds nothing.
func holdPendingReason(hold orkstackv1.RunDispatchHold, held bool) string {
	if !held {
		return ""
	}
	switch hold {
	case orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP:
		return "held: the run is at its in-flight limit"
	case orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_CARRYING_ON:
		return "held: the run is carrying on"
	}
	return ""
}

// interruptionPendingReason is the reason a PENDING row waits to run again,
// "" when it does not.
func interruptionPendingReason(row *orkstackv1.TaskRun, now time.Time) string {
	st := row.GetInterruptions()
	nb := st.GetNotBefore()
	if row.GetStatus() != orkstackv1.Status_STATUS_PENDING || nb == nil || !nb.AsTime().After(now) {
		return ""
	}
	until := nb.AsTime().UTC().Format("15:04:05 UTC")
	last := st.GetLast()
	switch reason := last.GetReason(); {
	case reason == orkerHeartbeatLostReason:
		return fmt.Sprintf("waiting until %s to run again (its Orker was lost)", until)
	case reason == attemptStoreUnavailableReason:
		return fmt.Sprintf("waiting until %s to run again (handed back before it started %d times: object storage refused the attempt record: %s)",
			until, st.GetStoreFaults(), last.GetStoreError())
	case isNeverStartedFault(reason):
		return fmt.Sprintf("waiting until %s to run again (handed back before it started %d times: %s)",
			until, st.GetStoreFaults(), reason)
	case last.GetNeverStarted():
		return fmt.Sprintf("waiting until %s to run again (handed back before it started %d times)", until, st.GetNeverStarted())
	default:
		return fmt.Sprintf("waiting until %s to run again (%s)", until, interruptionCause(last))
	}
}
