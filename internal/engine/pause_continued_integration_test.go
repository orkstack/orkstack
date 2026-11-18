//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// pauseCarryBehavior: "slow" heartbeats for six seconds, then succeeds; every
// other task succeeds at once.
func pauseCarryBehavior(ctx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() == "slow" {
		done := time.After(6 * time.Second)
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				return &orkstackv1.ExecuteTaskResult{TaskId: "slow", Attempt: 1, OrkerId: "fixture",
					ValuesJson: valuesJSON(map[string]any{"task": "slow"})}, nil
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
		ValuesJson: valuesJSON(map[string]any{"task": in.GetTaskId()})}, nil
}

// pauseCarryDocument is n quick tasks beside slow, and final after slow.
func pauseCarryDocument(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: pause-carry\nversion: \"1.0.0\"\ntasks:\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - id: q%d\n    uses: shell\n    with: {op: exec}\n", i)
	}
	b.WriteString("  - id: slow\n    uses: shell\n    with: {op: exec}\n")
	b.WriteString("  - id: final\n    uses: shell\n    needs: [slow]\n    with: {op: exec}\n")
	return b.String()
}

// pauseSignalFor is the API's pause or unpause request.
func pauseSignalFor(pause bool) map[string]any {
	return map[string]any{"format": 1, "pause": pause, "principal": "alice", "reason": "fixture",
		"requestId": fmt.Sprintf("req-%v", pause), "timeoutSeconds": "3600"}
}

// TestIntegration_RecordPauseContinuedHistories records the pause carry's
// two generations (pauseContinuedFixtures, replay_pause_test.go) from a real
// run on a real cluster by a versioned worker: the quick tasks cross the
// budget while slow runs; the run is paused before slow ends, so the switch
// carries the pause; the next generation restores it, starts nothing until
// the unpause, then runs final and succeeds. It writes the v2 fixtures: the
// v1 ones, recorded unpaced with a pause sent after a fixed sleep, stay and
// still replay.
func TestIntegration_RecordPauseContinuedHistories(t *testing.T) {
	// It never skips: the recordings land under ORK_PROOF_DIR, and only
	// ORK_RECORD_GOLDEN_HISTORY=1 also writes the committed fixtures.
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startPacedCANStack(t, ctx, "pause-carry", pausedByLastQuickTask(), nil, "slow")
	budget := engine.DefaultContinuationBudget(60, 1<<20, 3*time.Minute)
	plan := s.plan(engine.DefaultHistoryMaxEvents, &budget)
	plan.Pause = &engine.PausePlan{DefaultTimeoutSeconds: 7 * 24 * 3600, MaxTimeoutSeconds: 31 * 24 * 3600,
		ApplyReserveEvents: 20, CascadeEventsPerChild: 4, InheritedGraceSeconds: 600, InheritedGraceMaxLevels: 8}
	runID := fmt.Sprintf("ork-pause-carry-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, pauseCarryDocument(30)), nil, plan), "StartRun")
	first, err := describeExecution(s.client, ctx, runID, "")
	must.OK(t, err)
	firstExecution := first.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// The pause lands from q30 while slow (six seconds) still runs, so the
	// switch, held for it, carries the pause; the unpause once the next
	// generation, restored paused, is idle.
	current := waitForNextExecution(t, s, ctx, runID, firstExecution)
	must.OKf(t, s.waitIdle(runID, 90*time.Second), "the restored generation never went idle")
	must.OKf(t, s.client.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(false)), "unpause")
	must.OKf(t, s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil), "the run did not succeed")

	chain := s.chainOf(runID, current)
	if len(chain) != 2 {
		t.Fatalf("%d generations, want 2", len(chain))
	}
	g0, g1 := s.history(runID, chain[0]), s.history(runID, chain[1])
	if !continuedAsNew(g0) || carryOf(t, g1).GetPause() == nil {
		t.Fatalf("g0 switched %v, g1 carried pause %v; want the switch to carry the pause", continuedAsNew(g0), carryOf(t, g1).GetPause())
	}
	if !dispatchedTasks(t, g1)["final"] || len(dispatchedTasks(t, g1)) != 1 {
		t.Fatalf("g1 dispatched %v, want final alone", dispatchedTasks(t, g1))
	}
	if signalIndex, dispatchIndex := firstEventIndex(g1, isSignal), firstEventIndex(g1, isExecuteTaskScheduled); signalIndex < 0 || dispatchIndex < signalIndex {
		t.Fatalf("g1 dispatched at event %d, before its unpause at %d", dispatchIndex, signalIndex)
	}
	for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
		must.OKf(t, replayRecorded(h, runID), "%s just recorded does not replay", name)
	}
	for path, h := range map[string]*historypb.History{pauseCarriedV2G0: g0, pauseCarriedV2G1: g1} {
		data := encodeHistory(t, h)
		proof.Write(t, "pause", filepath.Base(path), data)
		if write {
			must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
		}
		t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
	}
}

// pausedByLastQuickTask is pauseCarryBehavior with the pause sent by q30
// itself, before it completes, once: on a paced stack the pause is then
// recorded at one point of the run whatever the clock, so a recording's
// shape is the plan's and the script's, never the host's. slow is kept out
// of the pacing by its recorder, as the continued recorders keep it.
func pausedByLastQuickTask() canBehavior {
	var once sync.Once
	return func(ctx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "q30" {
			var err error
			once.Do(func() {
				err = s.client.SignalWorkflow(ctx, activity.GetInfo(ctx).WorkflowExecution.ID, "", engine.PauseSignal, pauseSignalFor(true))
			})
			if err != nil {
				return nil, fmt.Errorf("pause from q30: %w", err)
			}
		}
		return pauseCarryBehavior(ctx, s, in)
	}
}

// TestIntegration_RecordPauseIndexedHistories records the paused index's
// two generations (pauseIndexedFixtures, replay_pause_test.go): the pause
// carry's shape with an Indexed plan, so generation 0 writes the paused
// attribute and memo when it pauses, and generation 1, starting paused,
// writes them again, then removes them at the unpause.
func TestIntegration_RecordPauseIndexedHistories(t *testing.T) {
	// It never skips: the recordings land under ORK_PROOF_DIR, and only
	// ORK_RECORD_GOLDEN_HISTORY=1 also writes the committed fixtures.
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startPacedCANStack(t, ctx, "pause-indexed", pausedByLastQuickTask(), nil, "slow")
	indexed, err := task.EnsureRunPausedSearchAttribute(ctx, s.client, s.namespace)
	must.OKf(t, err, "register the paused attribute")
	if !indexed {
		t.Fatal("the proof cluster does not index the paused attribute")
	}
	budget := engine.DefaultContinuationBudget(60, 1<<20, 3*time.Minute)
	plan := s.plan(engine.DefaultHistoryMaxEvents, &budget)
	plan.Pause = &engine.PausePlan{DefaultTimeoutSeconds: 7 * 24 * 3600, MaxTimeoutSeconds: 31 * 24 * 3600,
		ApplyReserveEvents: 20, CascadeEventsPerChild: 4, InheritedGraceSeconds: 600, InheritedGraceMaxLevels: 8,
		Indexed: true}
	runID := fmt.Sprintf("ork-pause-indexed-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, pauseCarryDocument(30)), nil, plan), "StartRun")
	first, err := describeExecution(s.client, ctx, runID, "")
	must.OK(t, err)
	firstExecution := first.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// The pause lands from q30 while slow still runs; the unpause once the
	// next generation, restored paused, is idle.
	current := waitForNextExecution(t, s, ctx, runID, firstExecution)
	must.OKf(t, s.waitIdle(runID, 90*time.Second), "the restored generation never went idle")
	must.OKf(t, s.client.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(false)), "unpause")
	must.OKf(t, s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil), "the run did not succeed")

	chain := s.chainOf(runID, current)
	if len(chain) != 2 {
		t.Fatalf("%d generations, want 2", len(chain))
	}
	g0, g1 := s.history(runID, chain[0]), s.history(runID, chain[1])
	if !continuedAsNew(g0) || carryOf(t, g1).GetPause() == nil {
		t.Fatalf("g0 switched %v, g1 carried pause %v; want the switch to carry the pause", continuedAsNew(g0), carryOf(t, g1).GetPause())
	}
	if n0, n1 := pausedAttributeWrites(g0), pausedAttributeWrites(g1); n0 != 1 || n1 != 2 {
		t.Fatalf("paused attribute written %d times in g0 and %d in g1, want 1 (the pause) and 2 (restored, unpaused)", n0, n1)
	}
	for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
		must.OKf(t, replayRecorded(h, runID), "%s just recorded does not replay", name)
	}
	for path, h := range map[string]*historypb.History{pauseIndexedG0: g0, pauseIndexedG1: g1} {
		data := encodeHistory(t, h)
		proof.Write(t, "pause", filepath.Base(path), data)
		if write {
			must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
		}
		t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
	}
}

// pausedAttributeWrites counts h's writes of the paused attribute.
func pausedAttributeWrites(h *historypb.History) int {
	n := 0
	for _, e := range h.GetEvents() {
		if attrs := e.GetUpsertWorkflowSearchAttributesEventAttributes(); attrs != nil {
			if _, ok := attrs.GetSearchAttributes().GetIndexedFields()[task.SearchAttrOrkflowRunPaused]; ok {
				n++
			}
		}
	}
	return n
}

// waitForNextExecution waits until the run has carried on from execution.
func waitForNextExecution(t *testing.T, s *canStack, ctx context.Context, runID, execution string) string {
	t.Helper()
	for {
		described, err := describeExecution(s.client, ctx, runID, "")
		must.OK(t, err)
		if id := described.GetWorkflowExecutionInfo().GetExecution().GetRunId(); id != execution {
			return id
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the run never carried on: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func isSignal(e *historypb.HistoryEvent) bool {
	return e.GetWorkflowExecutionSignaledEventAttributes() != nil
}

func isExecuteTaskScheduled(e *historypb.HistoryEvent) bool {
	attrs := e.GetActivityTaskScheduledEventAttributes()
	return attrs != nil && attrs.GetActivityType().GetName() == "ExecuteTask"
}

// firstEventIndex is the index of the first event match accepts, or -1.
func firstEventIndex(h *historypb.History, match func(*historypb.HistoryEvent) bool) int {
	for i, e := range h.GetEvents() {
		if match(e) {
			return i
		}
	}
	return -1
}
