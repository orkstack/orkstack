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
	if run == nil || isTerminal(run.GetStatus()) {
		return
	}
	for _, row := range run.GetTaskRuns() {
		row.PendingReason = interruptionPendingReason(row, now)
		for _, item := range row.GetItems() {
			item.PendingReason = interruptionPendingReason(item, now)
		}
	}
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
