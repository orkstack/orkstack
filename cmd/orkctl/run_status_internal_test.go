package main

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A watched run that is paused has not ended: watch keeps waiting, gives no
// exit code, and a paused run that is failing reads "PAUSED (failing)".
func TestRunStatus_PausedRunIsStillGoing(t *testing.T) {
	paused := &orkstackv1.Run{Id: "ork-x", Status: orkstackv1.Status_STATUS_PAUSED}
	if !runStillGoing(paused) {
		t.Error("watch would stop on a PAUSED run; it has not ended")
	}
	if err := runExitError(paused); err != nil {
		t.Errorf("a PAUSED run maps to exit error %v; it has not ended", err)
	}
	failing := &orkstackv1.Run{Id: "ork-x", Status: orkstackv1.Status_STATUS_PAUSED,
		Error: &orkstackv1.Error{Code: "RUN_FAILED"}}
	if got := runStatusLabel(failing); got != "PAUSED (failing)" {
		t.Errorf("label %q, want PAUSED (failing)", got)
	}
	for _, status := range []orkstackv1.Status{orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED} {
		if runStillGoing(&orkstackv1.Run{Status: status}) {
			t.Errorf("watch would keep waiting on a %s run", status)
		}
	}
}
