package engine_test

import (
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The recorded runs behind the emit-past-cancel gate
// (testdata/history/README.md): cancelBesideDocument on a real cluster,
// with build's completion and the operator's cancel delivered to one workflow
// task (the recorder stops the run's worker until both are in history, which
// is continuation chaos C4's race). The gated one is today's code; the
// legacy one runs with the gate off, the command sequence of every build
// before it.
const (
	emitPastCancelGated  = "testdata/history/run-emit-past-cancel-v1.json"
	emitPastCancelLegacy = "testdata/history/run-emit-past-cancel-legacy-v1.json"
)

// recordedPublishes is every event a history's PublishEvent activities
// carried, in the order they were scheduled.
func recordedPublishes(t *testing.T, recorded *historypb.History) []*orkstackv1.Event {
	t.Helper()
	var out []*orkstackv1.Event
	for _, attrs := range recordedScheduled(recorded, engine.ActivityPublishEvent) {
		var event *orkstackv1.Event
		if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &event); err != nil {
			t.Fatalf("decode a recorded publish: %v", err)
		}
		out = append(out, event)
	}
	return out
}

// requireEmitPastCancelShape: both histories recorded the race — build's
// ExecuteTask completed, then the cancel was requested, with no workflow task
// started between them — and ended RUN_CANCELED. The gated one published
// build's TASK_SUCCEEDED and numbered every event without a hole; the legacy
// one lost it and left the hole, which is what the gate fixes.
func requireEmitPastCancelShape(t *testing.T, path string, recorded *historypb.History, legacy bool) {
	t.Helper()
	executeTask := map[int64]bool{}
	completed, canceled, between := int64(0), int64(0), false
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			executeTask[event.GetEventId()] = true
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil && completed == 0 &&
			executeTask[attrs.GetScheduledEventId()] {
			completed = event.GetEventId()
		}
		if event.GetWorkflowExecutionCancelRequestedEventAttributes() != nil && canceled == 0 {
			canceled = event.GetEventId()
		}
		if completed != 0 && canceled == 0 && event.GetWorkflowTaskStartedEventAttributes() != nil {
			between = true
		}
	}
	if completed == 0 || canceled < completed || between {
		t.Fatalf("%s: build's completion (event %d) and the cancel request (event %d) did not reach one workflow task",
			path, completed, canceled)
	}
	events := recordedPublishes(t, recorded)
	if len(events) == 0 || events[len(events)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("%s: the run did not end RUN_CANCELED", path)
	}
	succeeded, hole := false, false
	for i, event := range events {
		if event.GetSequence() != int64(i+1) {
			hole = true
		}
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED && event.GetTaskId() == "build" {
			succeeded = true
		}
	}
	if legacy {
		if succeeded || !hole {
			t.Fatalf("%s: the legacy run published build's TASK_SUCCEEDED=%v and a hole=%v; want the event lost and the hole",
				path, succeeded, hole)
		}
		return
	}
	if !succeeded || hole {
		t.Fatalf("%s: published build's TASK_SUCCEEDED=%v and a hole=%v; want the event and no hole", path, succeeded, hole)
	}
}

// TestRunWorkflow_ReplaysCommittedEmitPastCancelHistories is the command half,
// for both: a history recorded before the gate replays its DefaultVersion
// branch, the snapshot write canceled with the run, and one recorded with the
// gate replays its emit on a context the cancel does not reach.
func TestRunWorkflow_ReplaysCommittedEmitPastCancelHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", emitPastCancelGated, false}, {"legacy", emitPastCancelLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireE1Fixture(t, tc.path, "TestIntegration_RecordEmitPastCancelHistories")
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, _, _ := startArgs(t, recorded)
			markers := versionMarkers(t, recorded)
			if tc.legacy != (markers[engine.EmitPastCancelChange] == 0) {
				t.Fatalf("%s records markers %v, want legacy=%v for %s", tc.path, markers, tc.legacy, engine.EmitPastCancelChange)
			}
			requireEmitPastCancelShape(t, tc.path, recorded, tc.legacy)
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			requireOneDecision(t, tc.path, capture, engine.EmitPastCancelChange, want)
		})
	}
}

// TestEmitPastCancelHistory_CatchesTheGateRemoved is the negative test: the
// gated fixture replayed with the gate skipped writes the success's snapshot
// on the canceled context, which loses it, and the replay fails.
func TestEmitPastCancelHistory_CatchesTheGateRemoved(t *testing.T) {
	requireE1Fixture(t, emitPastCancelGated, "TestIntegration_RecordEmitPastCancelHistories")
	recorded := loadHistoryFile(t, emitPastCancelGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableEmitPastCancelGateForTest()()
	if _, err := replayCapturingDispatch(t, recorded, runID); err == nil {
		t.Fatalf("%s replayed clean with the gate skipped: the fixture does not see the emit past the cancel", emitPastCancelGated)
	}
}
