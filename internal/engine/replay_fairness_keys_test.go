package engine_test

import (
	"os"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The recorded runs of linearDocument whose plan carries a fairness selection
// (TestIntegration_RecordFairnessKeysHistories, testdata/history/README.md),
// both from the SAME plan:
//
//   - gated: by today's code, so the fairness-keys marker is in the history,
//     once, and every task activity was scheduled with the plan's key and
//     weight;
//   - legacy: with the gate off, which is the command sequence of every build
//     before it — no marker and no priority, whatever the plan said.
const (
	fairnessKeysGated  = "testdata/history/run-fairness-keys-v1.json"
	fairnessKeysLegacy = "testdata/history/run-fairness-keys-legacy-v1.json"

	fairnessKeysPlannedKey    = "fixture"
	fairnessKeysPlannedWeight = float32(2)
)

func requireFairnessKeysFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the committed fairness keys fixture %s is missing (%v); record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
			"-run TestIntegration_RecordFairnessKeysHistories", path, err)
	}
}

// recordedPriorities is the priority of every ExecuteTask a history scheduled.
// Temporal's replayer never compares a priority, so the replay tests compare
// it themselves, against what current code sends on replay.
func recordedPriorities(recorded *historypb.History) []temporal.Priority {
	var out []temporal.Priority
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		p := attrs.GetPriority()
		out = append(out, temporal.Priority{
			PriorityKey: int(p.GetPriorityKey()), FairnessKey: p.GetFairnessKey(), FairnessWeight: p.GetFairnessWeight(),
		})
	}
	return out
}

func wantPriority(legacy bool) temporal.Priority {
	if legacy {
		return temporal.Priority{}
	}
	return temporal.Priority{FairnessKey: fairnessKeysPlannedKey, FairnessWeight: fairnessKeysPlannedWeight}
}

func requireRecordedFairness(t *testing.T, path string, recorded *historypb.History, legacy bool) {
	t.Helper()
	got := recordedPriorities(recorded)
	if len(got) < 2 {
		t.Fatalf("%s scheduled %d task activities, want linearDocument's every task", path, len(got))
	}
	for _, p := range got {
		if p != wantPriority(legacy) {
			t.Fatalf("%s recorded a task scheduled with %+v, want %+v", path, p, wantPriority(legacy))
		}
	}
}

func requireCapturedFairness(t *testing.T, path string, capture *fairnessCapture, legacy bool) {
	t.Helper()
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.priorities) < 2 {
		t.Fatalf("replaying %s scheduled %d task activities", path, len(capture.priorities))
	}
	for _, p := range capture.priorities {
		if p != wantPriority(legacy) {
			t.Fatalf("current code sent %+v on replaying %s, want %+v, the recorded one", p, path, wantPriority(legacy))
		}
	}
	want := workflow.Version(1)
	if legacy {
		want = workflow.DefaultVersion
	}
	if len(capture.asked) != 1 || capture.asked[0] != want {
		t.Fatalf("replaying %s was answered %v for %s, want exactly %d", path, capture.asked, engine.FairnessKeysChange, want)
	}
}

// replayCapturingFairness replays recorded against current code with the
// interceptor that records the priority each task is sent and the versions
// the gate is answered.
func replayCapturingFairness(t *testing.T, recorded *historypb.History, runID string) (*fairnessCapture, error) {
	t.Helper()
	capture := &fairnessCapture{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{capture},
	})
	if err != nil {
		t.Fatalf("new replayer: %v", err)
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return capture, replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	})
}

// TestRunWorkflow_ReplaysCommittedFairnessKeysHistories is the command half,
// for both: a history recorded before the gate replays its DefaultVersion
// branch and sends no priority though its plan carries a selection, and a
// history recorded with the gate replays its new path, the plan's key and
// weight on every task.
func TestRunWorkflow_ReplaysCommittedFairnessKeysHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", fairnessKeysGated, false}, {"legacy", fairnessKeysLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireFairnessKeysFixture(t, tc.path)
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if plan.Fairness == nil || plan.Fairness.Key != fairnessKeysPlannedKey {
				t.Fatalf("%s was started with a plan carrying %+v: the fixture does not say what it was recorded to say",
					tc.path, plan.Fairness)
			}
			markers := versionMarkers(t, recorded)[engine.FairnessKeysChange]
			if tc.legacy != (markers == 0) {
				t.Fatalf("%s records %d %s markers, want legacy=%v", tc.path, markers, engine.FairnessKeysChange, tc.legacy)
			}
			requireRecordedFairness(t, tc.path, recorded, tc.legacy)
			capture, err := replayCapturingFairness(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			requireCapturedFairness(t, tc.path, capture, tc.legacy)
		})
	}
}

// TestFairnessKeysHistories_CatchTheGateRemoved is the negative test: the
// gated fixture replayed with the gate skipped sends no priority where it
// recorded one, which the comparison above refuses.
func TestFairnessKeysHistories_CatchTheGateRemoved(t *testing.T) {
	requireFairnessKeysFixture(t, fairnessKeysGated)
	recorded := loadHistoryFile(t, fairnessKeysGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableFairnessKeysGateForTest()()
	capture, _ := replayCapturingFairness(t, recorded, runID)
	capture.mu.Lock()
	defer capture.mu.Unlock()
	for _, p := range capture.priorities {
		if p == wantPriority(false) {
			t.Fatalf("with the gate skipped, replay still sent %+v: the comparison cannot catch the gate removed", p)
		}
	}
}
