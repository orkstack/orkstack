package engine_test

import (
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The interruption-retry command paths (E92 PR 7c), each replayed from its
// committed fixture (recorded by TestIntegration_RecordInterruptionRetryHistories):
// a loss after the container started, a loss while the Orker was stopping, a
// store fault past its budget, a profile drift, an item waiting across a
// continuation, and a run recorded with the gate off.

// The committed fixtures; the recorder names the same paths.
const (
	replayInterruptionBeatStarted  = "testdata/history/run-interruption-retry-running-beat-started-v1.json"
	replayInterruptionLostStopping = "testdata/history/run-interruption-retry-lost-while-stopping-v1.json"
	replayInterruptionStoreFault   = "testdata/history/run-interruption-retry-store-fault-budget-v1.json"
	replayInterruptionDrifted      = "testdata/history/run-interruption-retry-profile-drifted-v1.json"
	replayInterruptionItemG0       = "testdata/history/run-interruption-retry-item-continuation-g0-v1.json"
	replayInterruptionItemG1       = "testdata/history/run-interruption-retry-item-continuation-g1-v1.json"
	replayInterruptionLegacy       = "testdata/history/run-interruption-retry-legacy-v1.json"
	interruptionRecorder           = "TestIntegration_RecordInterruptionRetryHistories"
)

// interruptionFixtureNames are every interruption fixture's file name: the
// only committed histories whose plans carry interruption budgets.
var interruptionFixtureNames = []string{
	filepath.Base(replayInterruptionBeatStarted), filepath.Base(replayInterruptionLostStopping),
	filepath.Base(replayInterruptionStoreFault), filepath.Base(replayInterruptionDrifted),
	filepath.Base(replayInterruptionItemG0), filepath.Base(replayInterruptionItemG1),
	filepath.Base(replayInterruptionLegacy),
}

// continuedInterruptionFixtureNames are the interruption fixtures whose plan
// carries a continuation budget, recorded after the item input floor existed.
var continuedInterruptionFixtureNames = []string{
	filepath.Base(replayInterruptionItemG0), filepath.Base(replayInterruptionItemG1),
}

// recordedTimers is the duration of every Timer a history started.
func recordedTimers(h *historypb.History) []time.Duration {
	var out []time.Duration
	for _, e := range h.GetEvents() {
		if attrs := e.GetTimerStartedEventAttributes(); attrs != nil {
			out = append(out, attrs.GetStartToFireTimeout().AsDuration())
		}
	}
	return out
}

// fenceTimer says d is what remained of a 31 s loss fence (a task's 1 s grace
// and the 30 s heartbeat timeout) when the wait was armed, the notice before
// it having taken a moment of the workflow's clock.
func fenceTimer(d time.Duration) bool { return d > 30*time.Second && d <= 31*time.Second }

func longestTimer(h *historypb.History) time.Duration {
	var longest time.Duration
	for _, d := range recordedTimers(h) {
		longest = max(longest, d)
	}
	return longest
}

func closedFailed(h *historypb.History) bool {
	events := h.GetEvents()
	return events[len(events)-1].GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
}

// TestRunWorkflow_ReplaysCommittedInterruptionRetryHistories: every fixture
// replays against current code, holds the gate's marker as its shape says,
// and records the commands its path issues.
func TestRunWorkflow_ReplaysCommittedInterruptionRetryHistories(t *testing.T) {
	g0RunID := ""
	for _, tc := range []struct {
		path   string
		legacy bool
		check  func(t *testing.T, h *historypb.History)
	}{
		{replayInterruptionBeatStarted, false, func(t *testing.T, h *historypb.History) {
			if got := len(recordedScheduled(h, task.ActivityExecuteTask)); got != 2 || !fenceTimer(longestTimer(h)) {
				t.Fatalf("%d dispatches, longest timer %v; want 2 and what remains of the 31 s loss fence", got, longestTimer(h))
			}
		}},
		{replayInterruptionLostStopping, false, func(t *testing.T, h *historypb.History) {
			if got := len(recordedScheduled(h, task.ActivityExecuteTask)); got != 2 || !fenceTimer(longestTimer(h)) {
				t.Fatalf("%d dispatches, longest timer %v; want 2 and what remains of the 31 s loss fence", got, longestTimer(h))
			}
		}},
		{replayInterruptionStoreFault, false, func(t *testing.T, h *historypb.History) {
			if got := len(recordedScheduled(h, task.ActivityExecuteTask)); got != 3 || !closedFailed(h) {
				t.Fatalf("%d dispatches, failed %v; want 3 and the run failed with the real reason", got, closedFailed(h))
			}
		}},
		{replayInterruptionDrifted, false, func(t *testing.T, h *historypb.History) {
			if got := len(recordedScheduled(h, task.ActivityExecuteTask)); got != 2 || closedFailed(h) {
				t.Fatalf("%d dispatches, failed %v; want 2 and success", got, closedFailed(h))
			}
		}},
		{replayInterruptionItemG0, false, func(t *testing.T, h *historypb.History) {
			if !continuedAsNewHistory(h) || longestTimer(h) < 20*time.Second {
				t.Fatalf("carried on %v, longest timer %v; want a switch with the item's 20 s wait open", continuedAsNewHistory(h), longestTimer(h))
			}
		}},
		{replayInterruptionLegacy, true, func(t *testing.T, h *historypb.History) {
			for _, attrs := range recordedScheduled(h, task.ActivityExecuteTask) {
				if types := attrs.GetRetryPolicy().GetNonRetryableErrorTypes(); len(types) != 0 {
					t.Fatalf("the legacy run listed %v", types)
				}
			}
		}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			requireE1Fixture(t, tc.path, interruptionRecorder)
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if tc.path == replayInterruptionItemG0 {
				g0RunID = runID
			}
			if plan.Interruptions == nil {
				t.Fatalf("%s was started with no budgets: it does not record what it says", tc.path)
			}
			if markers := versionMarkers(t, recorded); tc.legacy != (markers[engine.InterruptionRetryChange] == 0) {
				t.Fatalf("%s records markers %v, want legacy=%v", tc.path, markers, tc.legacy)
			}
			tc.check(t, recorded)
			must.OKf(t, replayAs(t, recorded, runID), "replaying %s against current code failed", tc.path)
		})
	}
	t.Run(replayInterruptionItemG1, func(t *testing.T) {
		requireE1Fixture(t, replayInterruptionItemG1, interruptionRecorder)
		if g0RunID == "" {
			t.Fatal("generation 0's run id was not read")
		}
		recorded := loadHistoryFile(t, replayInterruptionItemG1)
		if got := len(recordedScheduled(recorded, task.ActivityExecuteTask)); got != 1 {
			t.Fatalf("generation 1 dispatched %d items, want the waiting one once", got)
		}
		must.OKf(t, replayAs(t, recorded, g0RunID), "replaying %s against current code failed", replayInterruptionItemG1)
	})
}

// TestInterruptionRetryHistories_CatchTheGateRemoved: with the gate taken
// away, current code cannot replay a history recorded on the path.
func TestInterruptionRetryHistories_CatchTheGateRemoved(t *testing.T) {
	requireE1Fixture(t, replayInterruptionBeatStarted, interruptionRecorder)
	recorded := loadHistoryFile(t, replayInterruptionBeatStarted)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableInterruptionRetryGateForTest()()
	if err := replayAs(t, recorded, runID); err == nil {
		t.Fatal("the fixture replayed with the gate removed: it cannot see the path")
	}
}

func continuedAsNewHistory(h *historypb.History) bool {
	events := h.GetEvents()
	return events[len(events)-1].GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
}
