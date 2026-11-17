// Package runstatus is the one classifier of a run's status: live (it may
// still change, so a reader waits on it and counts it open) or terminal (it
// has ended). Every reader that asks either question asks here, so a status
// added to the proto is classified once, and TestRunStatus_EveryValueClassified
// fails until it is.
package runstatus

import orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

// IsTerminal reports that status is an ended run's: SUCCEEDED, FAILED,
// CANCELED or SKIPPED.
func IsTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

// IsLive reports that status is a run's that has not ended: PENDING,
// RUNNING, AWAITING, SLEEPING or PAUSED. UNSPECIFIED, the absence of a
// status, is neither live nor terminal.
func IsLive(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_PENDING,
		orkstackv1.Status_STATUS_RUNNING,
		orkstackv1.Status_STATUS_AWAITING,
		orkstackv1.Status_STATUS_SLEEPING,
		orkstackv1.Status_STATUS_PAUSED:
		return true
	default:
		return false
	}
}
