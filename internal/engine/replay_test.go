package engine_test

import (
	"os"
	"testing"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// goldenHistory is a real, committed history from a successful multi-task run,
// recorded by an earlier build against a real cluster
// (golden_history_integration_test.go writes it, opt-in, and nothing else
// touches it).
const goldenHistory = "testdata/history/run-succeeded-v1.json"

// TestRunWorkflow_ReplaysCommittedV1History is the cross-version determinism
// guard.
//
// Under Worker Deployment Versioning a Pinned run keeps executing on the build
// that started it, so today's code will one day be handed a history recorded
// by an older one — on a rollback (`set-current-version v1` after v2 ran), on
// a versioning override, and on every replay Temporal performs after a worker
// restart. The existing check in integration_test.go records a history and
// replays it against the SAME code, so it can only catch a workflow body that
// is non-deterministic with itself. This one is old-history-meets-new-code:
// the fixture is frozen in git, so reordering the emit calls, renaming an
// event, changing readyTasks' order, or altering task.ExecuteTaskInput fails
// here the day it is written.
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
		Name: engine.WorkflowName,
	})

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, goldenHistory); err != nil {
		t.Fatalf("replaying the committed history against current code failed: %v\n\n"+
			"That is a cross-version determinism break: a run started by an older build "+
			"would fail its next workflow task on this one. Either restore the old command "+
			"stream (workflow.GetVersion for a deliberate change), or — if the contract "+
			"genuinely changed — re-record the fixture and say so in the commit.", err)
	}
}

// TestGoldenHistory_StillDescribesAPinnedMultiTaskRun keeps the fixture
// honest. A replay of a two-event history passes trivially, so the guard is
// only worth having while the recorded run is the shape it was recorded for:
// several activities, a terminal completion, and the Pinned behaviour a
// versioned build stamps onto every workflow task it completes.
func TestGoldenHistory_StillDescribesAPinnedMultiTaskRun(t *testing.T) {
	raw, err := os.ReadFile(goldenHistory)
	if err != nil {
		t.Fatalf("read %s: %v", goldenHistory, err)
	}
	recorded := &historypb.History{}
	if err := (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded); err != nil {
		t.Fatalf("decode %s: %v", goldenHistory, err)
	}

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
