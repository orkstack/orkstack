package engine_test

import (
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The recorded runs behind the task-publish-window gate
// (testdata/history/README.md), both from ONE plan that pins the publish
// window and a byte budget small enough to bind: the gated one by today's
// code, the legacy one with the gate off, which is the command sequence of
// every build before it — a build that ignores the plan's window and awaits
// every publish.
const (
	taskPublishGated  = "testdata/history/run-task-publish-window-v1.json"
	taskPublishLegacy = "testdata/history/run-task-publish-window-legacy-v1.json"

	// taskPublishDispatchBytes is the fixture plan's byte budget: a few
	// tasks' inputs, so it ends workflow tasks before the window does.
	taskPublishDispatchBytes = 16 << 10

	// taskPublishTasks is how many plain tasks the fixture's stage starts
	// together, more than the window holds publishes for.
	taskPublishTasks = 40
)

// publishShape is what one recorded history's workflow-task completions
// carried: the most PublishEvent commands one completion scheduled, and the
// most ExecuteTask input bytes, as recorded.
type publishShape struct {
	maxPublishes, maxTaskBytes, completions int
}

func publishShapeOf(recorded *historypb.History) publishShape {
	var shape publishShape
	publishes, taskBytes := 0, 0
	flush := func() {
		shape.maxPublishes = max(shape.maxPublishes, publishes)
		shape.maxTaskBytes = max(shape.maxTaskBytes, taskBytes)
		publishes, taskBytes = 0, 0
	}
	for _, event := range recorded.GetEvents() {
		switch {
		case event.GetWorkflowTaskCompletedEventAttributes() != nil:
			flush()
			shape.completions++
		case event.GetActivityTaskScheduledEventAttributes() != nil:
			attrs := event.GetActivityTaskScheduledEventAttributes()
			switch attrs.GetActivityType().GetName() {
			case engine.ActivityPublishEvent:
				publishes++
			case task.ActivityExecuteTask:
				for _, p := range attrs.GetInput().GetPayloads() {
					taskBytes += len(p.GetData())
				}
			}
		}
	}
	flush()
	return shape
}

// requireTaskPublishShape: the gated history scheduled several tasks'
// publishes in one completion, never more than the window, and never more
// task input than the budget; the legacy one at most one publish per
// completion, each awaited.
func requireTaskPublishShape(t *testing.T, path string, recorded *historypb.History, legacy bool) {
	t.Helper()
	shape := publishShapeOf(recorded)
	if legacy {
		if shape.maxPublishes > 1 {
			t.Fatalf("%s scheduled %d publishes in one workflow task, want each awaited (at most 1)", path, shape.maxPublishes)
		}
		return
	}
	if shape.maxPublishes < 4 || shape.maxPublishes > int(engine.DefaultTaskPublishWindow) {
		t.Fatalf("%s scheduled at most %d publishes in one workflow task, want several tasks' and at most the window's %d",
			path, shape.maxPublishes, engine.DefaultTaskPublishWindow)
	}
	if shape.maxTaskBytes > taskPublishDispatchBytes {
		t.Fatalf("%s scheduled %d bytes of task input in one workflow task, over the plan's %d",
			path, shape.maxTaskBytes, taskPublishDispatchBytes)
	}
}

// TestRunWorkflow_ReplaysCommittedTaskPublishHistories is the command half,
// for both: a history recorded before the gate replays its DefaultVersion
// branch, awaiting every publish though its plan pins the window, and one
// recorded with the gate replays its detached path.
func TestRunWorkflow_ReplaysCommittedTaskPublishHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", taskPublishGated, false}, {"legacy", taskPublishLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireE1Fixture(t, tc.path, "TestIntegration_RecordTaskPublishWindowHistories")
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if plan.InFlight == nil || plan.InFlight.PublishWindow != engine.DefaultTaskPublishWindow ||
				plan.InFlight.DispatchBytes != taskPublishDispatchBytes ||
				plan.InFlight.PublishEvents != engine.DefaultTaskPublishEvents {
				t.Fatalf("%s was started with in-flight %+v: the fixture does not say what it was recorded to say",
					tc.path, plan.InFlight)
			}
			markers := versionMarkers(t, recorded)
			if tc.legacy != (markers[engine.TaskPublishChange] == 0) {
				t.Fatalf("%s records markers %v, want legacy=%v for %s", tc.path, markers, tc.legacy, engine.TaskPublishChange)
			}
			requireTaskPublishShape(t, tc.path, recorded, tc.legacy)
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			requireOneDecision(t, tc.path, capture, engine.TaskPublishChange, want)
		})
	}
}

// TestTaskPublishHistory_CatchesTheGateRemoved is the negative test: the
// gated fixture replayed with the gate skipped awaits every publish, which
// is not the command stream it recorded, and the replay fails.
func TestTaskPublishHistory_CatchesTheGateRemoved(t *testing.T) {
	requireE1Fixture(t, taskPublishGated, "TestIntegration_RecordTaskPublishWindowHistories")
	recorded := loadHistoryFile(t, taskPublishGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableTaskPublishGateForTest()()
	if _, err := replayCapturingDispatch(t, recorded, runID); err == nil {
		t.Fatalf("%s replayed clean with the gate skipped: the fixture does not see the detached path", taskPublishGated)
	}
}

// TestTaskPublishHistory_APublishAppendsWhatItIsReserved derives
// DefaultTaskPublishEvents from the events the SDK wrote on the recorded
// detached publish path. Once a PublishEvent is scheduled (that event is in
// history when the reserve counts it), it records exactly two more: its
// ActivityTaskStarted and the event that closes it. The reserve adds the
// workflow task that collects it, three events. Their bytes, at their
// largest in the recording, fit what the guard charges for those events,
// e × BytesPerEvent.
func TestTaskPublishHistory_APublishAppendsWhatItIsReserved(t *testing.T) {
	requireE1Fixture(t, taskPublishGated, "TestIntegration_RecordTaskPublishWindowHistories")
	recorded := loadHistoryFile(t, taskPublishGated)
	publishes := map[int64][]*historypb.HistoryEvent{}
	workflowTasks := map[int64]int{} // a workflow task's scheduled event id -> its three events' bytes
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == engine.ActivityPublishEvent {
			publishes[event.GetEventId()] = nil
		}
	}
	for _, event := range recorded.GetEvents() {
		var scheduled int64
		switch {
		case event.GetActivityTaskStartedEventAttributes() != nil:
			scheduled = event.GetActivityTaskStartedEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskCompletedEventAttributes() != nil:
			scheduled = event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskFailedEventAttributes() != nil:
			scheduled = event.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskTimedOutEventAttributes() != nil:
			scheduled = event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskCanceledEventAttributes() != nil:
			scheduled = event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()
		case event.GetWorkflowTaskScheduledEventAttributes() != nil:
			workflowTasks[event.GetEventId()] += proto.Size(event)
		case event.GetWorkflowTaskStartedEventAttributes() != nil:
			workflowTasks[event.GetWorkflowTaskStartedEventAttributes().GetScheduledEventId()] += proto.Size(event)
		case event.GetWorkflowTaskCompletedEventAttributes() != nil:
			workflowTasks[event.GetWorkflowTaskCompletedEventAttributes().GetScheduledEventId()] += proto.Size(event)
		}
		if _, ok := publishes[scheduled]; ok {
			publishes[scheduled] = append(publishes[scheduled], event)
		}
	}
	if len(publishes) < 2*taskPublishTasks {
		t.Fatalf("%s records %d publishes, want at least the %d tasks' two each", taskPublishGated, len(publishes), taskPublishTasks)
	}
	mostPublish, mostWorkflowTask := 0, 0
	for id, after := range publishes {
		if len(after) != 2 {
			t.Fatalf("the publish scheduled at event %d recorded %d events after it, want its start and its close", id, len(after))
		}
		mostPublish = max(mostPublish, proto.Size(after[0])+proto.Size(after[1]))
	}
	for _, bytes := range workflowTasks {
		mostWorkflowTask = max(mostWorkflowTask, bytes)
	}
	const workflowTaskEvents = 3
	if want := int64(2 + workflowTaskEvents); engine.DefaultTaskPublishEvents != want {
		t.Fatalf("DefaultTaskPublishEvents = %d, want the 2 events a publish records and the %d of the workflow task that collects it",
			engine.DefaultTaskPublishEvents, workflowTaskEvents)
	}
	charged := engine.DefaultTaskPublishEvents * engine.DefaultHistoryReserve.BytesPerEvent
	t.Logf("%d publishes: each recorded 2 events after its scheduling, at most %d bytes; a workflow task at most %d bytes; "+
		"reserved %d events, %d bytes", len(publishes), mostPublish, mostWorkflowTask, engine.DefaultTaskPublishEvents, charged)
	if int64(mostPublish+mostWorkflowTask) > charged {
		t.Fatalf("a publish's events (%d bytes) and a workflow task's (%d) exceed the %d bytes reserved for them",
			mostPublish, mostWorkflowTask, charged)
	}
}
