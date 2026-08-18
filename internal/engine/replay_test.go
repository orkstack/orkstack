package engine_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// goldenHistory is a real, committed history from a successful multi-task run,
// recorded against a real cluster by a VERSIONED worker
// (golden_history_integration_test.go writes it, opt-in, and nothing else
// touches it).
//
// Its provenance, stated exactly, because two docs used to overstate it: it
// was recorded by the commit that added it, so on the day it landed it was
// same-code, not old-code. It is a frozen command stream that AGES into the
// cross-version guard — every commit after that one is measured against a
// build that is genuinely older than itself. What proves the fixture can
// detect the interesting change on day one is
// TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream below, which replays
// it against a body carrying one extra emit — the exact shape of the change
// M8 made to RunWorkflow.
//
// The baseline was reset at M10.1. M10.1 removed the user-facing task routing
// field and added the selected default execution channel as a fourth workflow
// input, so the pre-change history could not decode against this clean-cut
// contract. Nothing was deployed; the fixture was re-recorded by the opt-in
// real-cluster test, and both command and payload guards remain active. See
// testdata/history/README.md and proof/M10.1/.
const goldenHistory = "testdata/history/run-succeeded-v1.json"

// TestRunWorkflow_ReplaysCommittedV1History is half of the cross-version
// determinism guard: the COMMAND half.
//
// Under Worker Deployment Versioning a Pinned run keeps executing on the build
// that started it, so today's code will one day be handed a history recorded
// by an older one — on a rollback (`set-current-version v1` after v2 ran), on
// a versioning override, and on every replay Temporal performs after a worker
// restart. The existing check in integration_test.go records a history and
// replays it against the SAME code, so it can only catch a workflow body that
// is non-deterministic with itself. This one holds the command stream still:
// the fixture is frozen in git, so reordering the emit calls, adding or
// deleting one, or changing readyTasks' order fails here. Each was
// mutation-tested against this fixture; each fails with TMPRL1100.
//
// What it does NOT catch, and must not be trusted for: what is INSIDE a
// payload. The replayer compares an activity's type and its position in the
// command stream, never its input. Adding a field to task.ExecuteTaskInput, or
// emitting a different EventType from the same emit call, replays perfectly
// clean here — both were measured. That is
// TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends' job
// (replay_payload_test.go), and the two are only a guard together.
//
// It is a unit test on purpose: no cluster, runs in `make test`, so the guard
// is in front of every change rather than behind an integration tag.
func TestRunWorkflow_ReplaysCommittedV1History(t *testing.T) {
	if _, err := os.Stat(goldenHistory); err != nil {
		t.Fatalf("the committed replay fixture is missing (%v). It is the only "+
			"old-history-meets-new-code guard there is; re-record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
			"-run RecordGoldenRunHistory", err)
	}

	replayer := worker.NewWorkflowReplayer()
	// Registered exactly as cmd/ork registers it, minus the versioning
	// behaviour, which is worker configuration and plays no part in replay:
	// nothing inside RunWorkflow reads it and no history event depends on it.
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name: engine.RunWorkflowType,
	})

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, goldenHistory); err != nil {
		t.Fatalf("replaying the committed history against current code failed: %v\n\n"+
			"That is a cross-version determinism break: a run started by an older build "+
			"would fail its next workflow task on this one. Either restore the old command "+
			"stream (workflow.GetVersion for a deliberate change), or — if the contract "+
			"genuinely changed — re-record the fixture and say so in the commit.", err)
	}
}

// TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream is what makes the
// guard above worth having on the day the fixture is recorded, instead of only
// from the next change onward.
//
// The fixture is same-code the moment it lands, so nothing in the test above
// yet demonstrates it can catch old-history-meets-new-code. This one supplies
// the missing half by playing the other side: it replays the committed history
// against a workflow body that publishes ONE extra event before doing exactly
// what RunWorkflow does. History without the emit, code with it — which is
// precisely the relationship between a pre-M8 history and this tree, whose
// RunWorkflow inserted EVENT_TYPE_RUN_CREATED as command 1 of every run
// (internal/engine/workflow.go; before M8 that event was published in-process
// by internal/api/service.go and never appeared in a history).
//
// So the replay MUST fail. If it ever passes, the replayer is not comparing
// what this repo believes it compares, and both `make test` replay guards are
// worth nothing — including the green one above.
func TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(runWorkflowWithAnExtraEmit, workflow.RegisterOptions{
		Name: engine.RunWorkflowType,
	})

	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, goldenHistory)
	if err == nil {
		t.Fatal("replaying the committed history against a body with one extra emit SUCCEEDED. " +
			"The command-stream guard cannot see an inserted activity, so " +
			"TestRunWorkflow_ReplaysCommittedV1History proves nothing about a build that added one.")
	}
	// Any error would satisfy the line above — a missing fixture, a decode
	// failure — so name the class. TMPRL1100 is the SDK's determinism-violation
	// code (sdk@v1.47.0/internal/internal_command_state_machine.go, via
	// panicIllegalState); here it is "lookup failed for scheduledEventID to
	// activityID", the history running out of the activity the extra command
	// needs.
	message := err.Error()
	if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v\n\n"+
			"The guard is only meaningful while an inserted command is rejected for being "+
			"an inserted command. Check the fixture is readable and the SDK still reports "+
			"TMPRL1100 for this case.", err)
	}
	t.Logf("inserted emit rejected by the replayer, as it must be: %v", err)
}

// runWorkflowWithAnExtraEmit is RunWorkflow plus one projection activity at the
// front: the smallest change of the class Pinned exists to protect a run from.
// It delegates to the real body so the rest of the command stream is not a
// hand-written imitation of it.
func runWorkflowWithAnExtraEmit(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan engine.RunPlan,
) (*orkstackv1.Run, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           engine.EventQueue,
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	event := &orkstackv1.Event{
		RunId:     runID,
		Sequence:  0,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:    orkstackv1.Status_STATUS_PENDING,
		Message:   "an event a later build decided to publish",
		Timestamp: timestamppb.New(workflow.Now(ctx)),
	}
	// The error is ignored exactly as runState.emit ignores it: a failed
	// projection never stops a run, and here the call is only present for the
	// command it schedules.
	_ = workflow.ExecuteActivity(activityCtx, engine.ActivityPublishEvent,
		event, &orkstackv1.Run{Id: runID}, runID+"#0").Get(ctx, nil)

	return engine.RunWorkflow(ctx, runID, wf, input, plan)
}

// TestGoldenHistory_StillDescribesAPinnedMultiTaskRun keeps the fixture
// honest. A replay of a two-event history passes trivially, so the guard is
// only worth having while the recorded run is the shape it was recorded for:
// several activities, a terminal completion, and the Pinned behaviour a
// versioned build stamps onto every workflow task it completes.
func TestGoldenHistory_StillDescribesAPinnedMultiTaskRun(t *testing.T) {
	recorded := loadGoldenHistory(t)

	var executeTasks, pinnedTasks int
	completed := false
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			if attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				executeTasks++
			}
		}
		if attrs := event.GetWorkflowTaskCompletedEventAttributes(); attrs != nil {
			if attrs.GetVersioningBehavior() == enums.VERSIONING_BEHAVIOR_PINNED {
				pinnedTasks++
			}
		}
		if event.GetWorkflowExecutionCompletedEventAttributes() != nil {
			completed = true
		}
	}

	if !completed {
		t.Error("the fixture is not a completed run; replaying a truncated history proves less")
	}
	if executeTasks < 4 {
		t.Errorf("the fixture scheduled %d task activities, want at least 4: a fixture with one task "+
			"cannot catch a change to scheduling order", executeTasks)
	}
	if pinnedTasks == 0 {
		t.Error("no workflow task in the fixture was completed with VERSIONING_BEHAVIOR_PINNED; " +
			"it was not recorded by a versioned build, so it is not the cross-version case")
	}
}
