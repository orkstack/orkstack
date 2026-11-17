package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestInterruptionPendingReason: a PENDING row whose not_before is ahead says
// until when and why; a row whose wait has passed, a running row and a row
// with no interruption say nothing.
func TestInterruptionPendingReason(t *testing.T) {
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	ahead := timestamppb.New(now.Add(42 * time.Second))
	pending := func(st *orkstackv1.TaskInterruptions) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_PENDING, Interruptions: st}
	}
	tests := []struct {
		name string
		row  *orkstackv1.TaskRun
		want string
	}{
		{"lost", pending(&orkstackv1.TaskInterruptions{NotBefore: ahead, Last: &orkstackv1.TaskInterruption{Reason: orkerHeartbeatLostReason}}),
			"waiting until 10:00:42 UTC to run again (its Orker was lost)"},
		{"store fault", pending(&orkstackv1.TaskInterruptions{NotBefore: ahead, StoreFaults: 2,
			Last: &orkstackv1.TaskInterruption{Reason: attemptStoreUnavailableReason, StoreError: "503 Slow Down"}}),
			"waiting until 10:00:42 UTC to run again (handed back before it started 2 times: object storage refused the attempt record: 503 Slow Down)"},
		{"profile drift", pending(&orkstackv1.TaskInterruptions{NotBefore: ahead, StoreFaults: 3,
			Last: &orkstackv1.TaskInterruption{Reason: profileDriftedReason}}),
			"waiting until 10:00:42 UTC to run again (handed back before it started 3 times: PROFILE_DRIFTED)"},
		{"stop bounce", pending(&orkstackv1.TaskInterruptions{NotBefore: ahead, NeverStarted: 21,
			Last: &orkstackv1.TaskInterruption{Reason: orkerStoppedReason, NeverStarted: true}}),
			"waiting until 10:00:42 UTC to run again (handed back before it started 21 times)"},
		{"lost while stopping", pending(&orkstackv1.TaskInterruptions{NotBefore: ahead,
			Last: &orkstackv1.TaskInterruption{Reason: orkerStoppedReason}}),
			"waiting until 10:00:42 UTC to run again (handed back by a stopping Orker)"},
		{"wait passed", pending(&orkstackv1.TaskInterruptions{NotBefore: timestamppb.New(now),
			Last: &orkstackv1.TaskInterruption{Reason: orkerHeartbeatLostReason}}), ""},
		{"running", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_RUNNING,
			Interruptions: &orkstackv1.TaskInterruptions{NotBefore: ahead}}, ""},
		{"never interrupted", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_PENDING}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := interruptionPendingReason(tt.row, now); got != tt.want {
				t.Fatalf("got %q\nwant %q", got, tt.want)
			}
		})
	}
}

// TestApplyPendingReasons_OnlyARunInFlight: rows and items of a running run
// get their reason; a finished run's rows get none.
func TestApplyPendingReasons_OnlyARunInFlight(t *testing.T) {
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	waiting := func() *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_PENDING, Interruptions: &orkstackv1.TaskInterruptions{
			NotBefore: timestamppb.New(now.Add(time.Minute)), Last: &orkstackv1.TaskInterruption{Reason: orkerHeartbeatLostReason}}}
	}
	running := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: []*orkstackv1.TaskRun{
		waiting(), {Status: orkstackv1.Status_STATUS_RUNNING, Items: []*orkstackv1.TaskRun{waiting()}}}}
	applyPendingReasons(running, now)
	if running.GetTaskRuns()[0].GetPendingReason() == "" || running.GetTaskRuns()[1].GetItems()[0].GetPendingReason() == "" {
		t.Fatalf("a running run's waiting row and item got no reason: %v", running)
	}
	done := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, TaskRuns: []*orkstackv1.TaskRun{waiting()}}
	applyPendingReasons(done, now)
	if got := done.GetTaskRuns()[0].GetPendingReason(); got != "" {
		t.Fatalf("a finished run's row got %q", got)
	}
}
