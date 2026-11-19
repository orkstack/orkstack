package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestHoldPendingReason: a row the run itself held back says which hold;
// a row it did not hold says nothing, whatever the run's hold.
func TestHoldPendingReason(t *testing.T) {
	tests := []struct {
		name string
		hold orkstackv1.RunDispatchHold
		held bool
		want string
	}{
		{"at the cap", orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP, true, "held: the run is at its in-flight limit"},
		{"carrying on", orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_CARRYING_ON, true, "held: the run is carrying on"},
		{"not held: it waits on its needs", orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP, false, ""},
		{"held, but the run holds nothing now", orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := holdPendingReason(tt.hold, tt.held); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestApplyPendingReasons_TheRunsHoldComesAfterTheRowsOwnWait: on a fixture
// snapshot of a run at its in-flight cap, a held row and a held item say so, a
// held row whose interruption wait is still ahead keeps that reason (it comes
// first), a held fan-out still RUNNING says so on its own row, and a PENDING
// row the run did not hold, one that waits on its needs, says nothing.
func TestApplyPendingReasons_TheRunsHoldComesAfterTheRowsOwnWait(t *testing.T) {
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	heldIDs := map[string]bool{"held": true, "held-and-waiting": true, "fan/1": true, "running": true, "held-fan": true}
	marked := func(row *orkstackv1.TaskRun) bool { return heldIDs[row.GetTaskId()] }

	pending := func(id string) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: id, Status: orkstackv1.Status_STATUS_PENDING}
	}
	waiting := pending("held-and-waiting")
	waiting.Interruptions = &orkstackv1.TaskInterruptions{NotBefore: timestamppb.New(now.Add(time.Minute)),
		Last: &orkstackv1.TaskInterruption{Reason: orkerHeartbeatLostReason}}
	fan := &orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING,
		Items: []*orkstackv1.TaskRun{pending("fan/0"), pending("fan/1")}}
	run := &orkstackv1.Run{
		Status:       orkstackv1.Status_STATUS_RUNNING,
		DispatchHold: orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP,
		TaskRuns: []*orkstackv1.TaskRun{pending("held"), waiting, pending("needs"), fan,
			{TaskId: "running", Status: orkstackv1.Status_STATUS_RUNNING},
			{TaskId: "held-fan", Status: orkstackv1.Status_STATUS_RUNNING, Items: []*orkstackv1.TaskRun{pending("held-fan/0")}}},
	}

	applyPendingReasonsWith(run, now, marked)

	want := map[string]string{
		"held":             "held: the run is at its in-flight limit",
		"held-and-waiting": "waiting until 10:01:00 UTC to run again (its Orker was lost)",
		"needs":            "",
		"fan":              "",
		"fan/0":            "",
		"fan/1":            "held: the run is at its in-flight limit",
		"running":          "",
		"held-fan":         "held: the run is at its in-flight limit",
		"held-fan/0":       "",
	}
	got := map[string]string{}
	for _, row := range run.GetTaskRuns() {
		got[row.GetTaskId()] = row.GetPendingReason()
		for _, item := range row.GetItems() {
			got[item.GetTaskId()] = item.GetPendingReason()
		}
	}
	for id, reason := range want {
		if got[id] != reason {
			t.Errorf("%s: pending_reason %q, want %q", id, got[id], reason)
		}
	}

	// The same marks under the other holds, and on a finished run.
	for _, tt := range []struct {
		name   string
		status orkstackv1.Status
		hold   orkstackv1.RunDispatchHold
		want   string
	}{
		{"carrying on", orkstackv1.Status_STATUS_RUNNING, orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_CARRYING_ON, "held: the run is carrying on"},
		{"the run holds nothing now", orkstackv1.Status_STATUS_RUNNING, orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_UNSPECIFIED, ""},
		{"finished", orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP, ""},
	} {
		run := &orkstackv1.Run{Status: tt.status, DispatchHold: tt.hold, TaskRuns: []*orkstackv1.TaskRun{pending("held")}}
		applyPendingReasonsWith(run, now, marked)
		if reason := run.GetTaskRuns()[0].GetPendingReason(); reason != tt.want {
			t.Errorf("%s: pending_reason %q, want %q", tt.name, reason, tt.want)
		}
	}

	// Production reads the interpreter's mark off the snapshot itself.
	marked2 := pending("held")
	marked2.DispatchHeld = true
	run = &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING,
		DispatchHold: orkstackv1.RunDispatchHold_RUN_DISPATCH_HOLD_IN_FLIGHT_CAP,
		TaskRuns:     []*orkstackv1.TaskRun{marked2, pending("needs")}}
	applyPendingReasons(run, now)
	if reason := run.GetTaskRuns()[0].GetPendingReason(); reason != "held: the run is at its in-flight limit" {
		t.Errorf("a row the interpreter marked says %q, want it held", reason)
	}
	if reason := run.GetTaskRuns()[1].GetPendingReason(); reason != "" {
		t.Errorf("an unmarked row says %q, want nothing", reason)
	}
}

// TestApplyPendingReasons_APausedRunsUnitsSayThePauseHoldsThem: a unit an
// interruption put back, in a run paused by its own pause or its parent's,
// says the pause holds it, never that it runs again when its wait ends; that
// wait is named beside the pause while it is still ahead. Every other row
// keeps its own reason: a paused run still lets what it dispatched run.
func TestApplyPendingReasons_APausedRunsUnitsSayThePauseHoldsThem(t *testing.T) {
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	pending := func(id string) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: id, Status: orkstackv1.Status_STATUS_PENDING}
	}
	interrupted := func(id string, notBefore time.Time) *orkstackv1.TaskRun {
		row := pending(id)
		row.Interruptions = &orkstackv1.TaskInterruptions{NotBefore: timestamppb.New(notBefore),
			Last: &orkstackv1.TaskInterruption{Reason: orkerHeartbeatLostReason}}
		return row
	}
	fan := &orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING,
		Items: []*orkstackv1.TaskRun{interrupted("fan/0", now.Add(time.Minute)), {TaskId: "fan/1", Status: orkstackv1.Status_STATUS_RUNNING}}}
	run := &orkstackv1.Run{
		Status: orkstackv1.Status_STATUS_RUNNING,
		Pause:  &orkstackv1.RunPause{},
		// A paused run publishes no hold of its own (the pause says why).
		TaskRuns: []*orkstackv1.TaskRun{interrupted("lost", now.Add(42*time.Second)), interrupted("passed", now.Add(-time.Second)),
			pending("plain"), fan, {TaskId: "running", Status: orkstackv1.Status_STATUS_RUNNING}},
	}

	applyPendingReasons(run, now)

	want := map[string]string{
		"lost":    "held: the run is paused (its wait to run again ends at 10:00:42 UTC)",
		"passed":  "held: the run is paused",
		"plain":   "",
		"fan":     "",
		"fan/0":   "held: the run is paused (its wait to run again ends at 10:01:00 UTC)",
		"fan/1":   "",
		"running": "",
	}
	for _, row := range run.GetTaskRuns() {
		check := func(r *orkstackv1.TaskRun) {
			if got := r.GetPendingReason(); got != want[r.GetTaskId()] {
				t.Errorf("%s: pending_reason %q, want %q", r.GetTaskId(), got, want[r.GetTaskId()])
			}
		}
		check(row)
		for _, item := range row.GetItems() {
			check(item)
		}
	}

	// Paused by its parent, the same.
	run.Pause = nil
	run.PausedByParent = &orkstackv1.InheritedPause{}
	applyPendingReasons(run, now)
	if got := run.GetTaskRuns()[0].GetPendingReason(); got != want["lost"] {
		t.Errorf("a run paused by its parent: lost says %q, want %q", got, want["lost"])
	}

	// Unpaused, the same rows say their own wait again.
	run.PausedByParent = nil
	applyPendingReasons(run, now)
	if got := run.GetTaskRuns()[0].GetPendingReason(); got != "waiting until 10:00:42 UTC to run again (its Orker was lost)" {
		t.Errorf("an unpaused run's lost row says %q", got)
	}
}
