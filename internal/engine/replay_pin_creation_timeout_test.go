package engine_test

import (
	"os"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
)

// The recorded runs of an Orkflow that opens two pins, one after the other
// with an unpinned task between them (twoPinsDocument), whose plan carries a
// 30 second pin creation timeout (TestIntegration_RecordPinCreationTimeoutHistories,
// testdata/history/README.md). Both were recorded from the SAME plan:
//
//   - gated: by today's code, so the pin-creation-timeout marker is in the
//     history, once, and both pins' creation activities were scheduled with a
//     30 second ScheduleToStartTimeout, the plan's number;
//   - legacy: with the gate off, which is the command sequence of every build
//     before it — no marker, and the creation activity scheduled with the
//     five minutes those builds hardcoded, whatever a plan said. It is also
//     what a build that predates the setting does with a plan an newer engine
//     submitted.
const (
	pinCreationTimeoutGated  = "testdata/history/run-pin-creation-timeout-v1.json"
	pinCreationTimeoutLegacy = "testdata/history/run-pin-creation-timeout-legacy-v1.json"

	// pinCreationTimeoutPlanned is the number both fixtures' plans carry.
	pinCreationTimeoutPlanned = 30 * time.Second
)

func requirePinCreationTimeoutFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the committed pin creation timeout fixture %s is missing (%v); record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
			"-run TestIntegration_RecordPinCreationTimeoutHistories", path, err)
	}
}

// recordedCreationTimeouts is the ScheduleToStartTimeout of every session
// creation activity a history recorded, in order. Temporal's replayer matches
// commands by type and id and never compares a timeout, so the replay tests
// of this gate compare it themselves, against what current code sends on
// replay (replayCapturingPinCreation).
func recordedCreationTimeouts(recorded *historypb.History) []time.Duration {
	var out []time.Duration
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != sessionCreationActivityName {
			continue
		}
		out = append(out, attrs.GetScheduleToStartTimeout().AsDuration())
	}
	return out
}

// replayCapturingPinCreation replays recorded against current code, as the
// execution it was, with the interceptor that records the creation options
// the code sends and the versions it is answered.
func replayCapturingPinCreation(t *testing.T, recorded *historypb.History, runID string) (*pinCreationCapture, error) {
	t.Helper()
	capture := &pinCreationCapture{}
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

// TestRunWorkflow_ReplaysCommittedPinCreationTimeoutHistories is the command
// half, for both generations: a history recorded before the gate replays its
// DefaultVersion branch and waits the five minutes it was recorded with, for
// both of its pins — even
// though the plan it started with says 30 seconds, which is exactly what makes
// the legacy half a test of the gate and not of the plan — and a history
// recorded with the gate replays its new path, the plan's 30 seconds.
//
// Because the replayer does not compare timeouts, each half compares the
// ScheduleToStartTimeout current code sends on replay with the one the
// history recorded, and the version it was answered with the recorded marker.
func TestRunWorkflow_ReplaysCommittedPinCreationTimeoutHistories(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        string
		legacy      bool
		wantTimeout time.Duration
	}{
		{name: "gated", path: pinCreationTimeoutGated, wantTimeout: pinCreationTimeoutPlanned},
		{name: "legacy", path: pinCreationTimeoutLegacy, legacy: true, wantTimeout: 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requirePinCreationTimeoutFixture(t, tc.path)
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if got := time.Duration(plan.PinCreationTimeoutSeconds) * time.Second; got != pinCreationTimeoutPlanned {
				t.Fatalf("%s was started with a plan carrying %s, want %s: the fixture does not say what it was recorded to say",
					tc.path, got, pinCreationTimeoutPlanned)
			}
			markers := versionMarkers(t, recorded)[engine.PinCreationTimeoutChange]
			if tc.legacy && markers != 0 {
				t.Fatalf("%s records a %s marker: it is not a history from before the gate", tc.path, engine.PinCreationTimeoutChange)
			}
			if !tc.legacy && markers == 0 {
				t.Fatalf("%s records no %s marker: it does not exercise the gated path", tc.path, engine.PinCreationTimeoutChange)
			}
			if got := recordedCreationTimeouts(recorded); len(got) != 2 || got[0] != tc.wantTimeout || got[1] != tc.wantTimeout {
				t.Fatalf("%s recorded creation timeouts %v, want two of %s", tc.path, got, tc.wantTimeout)
			}

			capture, err := replayCapturingPinCreation(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			if got := capture.creationTimeouts(); len(got) != 2 || got[0] != tc.wantTimeout || got[1] != tc.wantTimeout {
				t.Errorf("current code sent creation timeouts %v on replaying %s, want two of %s, the recorded ones",
					got, tc.path, tc.wantTimeout)
			}
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 1 || asked[0] != want {
				t.Errorf("replaying %s was answered %v for %s, want exactly %d", tc.path, asked, engine.PinCreationTimeoutChange, want)
			}
		})
	}
}

// TestPinCreationTimeoutHistories_CatchTheGateRemoved is the negative test:
// with the gate skipped — a build that took the legacy branch unconditionally
// — the gated fixture is sent five minutes where it recorded 30 seconds. The
// replayer would not say so on its own (it compares no timeout), which is the
// reason the comparison above exists; this proves the comparison can fail, so
// the gated fixture pins the plan's number and the marker that brings it.
func TestPinCreationTimeoutHistories_CatchTheGateRemoved(t *testing.T) {
	requirePinCreationTimeoutFixture(t, pinCreationTimeoutGated)
	recorded := loadHistoryFile(t, pinCreationTimeoutGated)
	runID, _, _, _, _ := startArgs(t, recorded)

	restore := engine.DisablePinCreationTimeoutGateForTest()
	defer restore()
	capture, err := replayCapturingPinCreation(t, recorded, runID)
	if err != nil {
		return // the replayer itself refused the history: the gate is pinned
	}
	got := capture.creationTimeouts()
	if len(got) != 2 || got[0] != 5*time.Minute || got[1] != 5*time.Minute {
		t.Fatalf("with the gate removed current code sent %v, want two of the legacy five minutes", got)
	}
	if recordedTimeouts := recordedCreationTimeouts(recorded); len(recordedTimeouts) != 2 || recordedTimeouts[0] == got[0] {
		t.Fatalf("%s replayed with the gate removed and sent what it recorded (%v): it does not pin the gate",
			pinCreationTimeoutGated, recordedTimeouts)
	}
}
