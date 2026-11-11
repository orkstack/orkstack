//go:build integration

package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_RecordEmitPastCancelHistories records the emit-past-cancel
// fixtures: cancelBesideDocument on a real cluster, with build's completion
// and the operator's cancel delivered to one workflow task, continuation
// chaos C4's race made deterministic. build is held at its gate; the run's
// worker is stopped; build is released and its completion reaches history;
// the cancel is requested; a second worker then takes the workflow task that
// holds both. The gated run publishes build's TASK_SUCCEEDED and ends
// canceled with no hole; the legacy one, gate off, loses the event and leaves
// the hole. Each recording runs on a stack of its own, so a gate is held for
// one run only.
func TestIntegration_RecordEmitPastCancelHistories(t *testing.T) {
	for _, isLegacy := range []bool{true, false} {
		path := emitPastCancelGated
		if isLegacy {
			path = emitPastCancelLegacy
		}
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			s := newChaosStack(t, ctx, "emit-past-cancel-fixture")
			g := newGates()
			s.startProjection(nil)
			s.startOrker("orker-1", gated(g, "build"))
			if isLegacy {
				// Off for the run's whole life, both workers included.
				defer engine.DisableEmitPastCancelGateForTest()()
			}
			first := s.startReplica("replica-a", replicaOptions{})
			plan := rawPlan(s.orkerQueue)
			plan.CommitRoot = "mem://commits"
			runID := s.newRunID()
			s.transcript.run(runID)
			s.start(runID, compile(t, cancelBesideDocument), nil, plan)
			g.waitEntered(t, "build", time.Minute)

			first.stop()
			g.open("build")
			deadline := time.Now().Add(time.Minute)
			for !executeTaskCompleted(s.history(runID, "")) {
				if time.Now().After(deadline) {
					t.Fatal("build's completion never reached the run's history")
				}
				time.Sleep(100 * time.Millisecond)
			}
			if err := s.control.CancelWorkflow(ctx, runID, ""); err != nil {
				t.Fatalf("cancel %s: %v", runID, err)
			}
			s.startReplica("replica-b", replicaOptions{})
			if status, _ := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
				t.Fatalf("the recorded run closed %v, want Canceled", status)
			}

			recorded := s.history(runID, "")
			markers := versionMarkers(t, recorded)
			if isLegacy != (markers[engine.EmitPastCancelChange] == 0) {
				t.Fatalf("legacy=%v but markers = %v", isLegacy, markers)
			}
			requireEmitPastCancelShape(t, path, recorded, isLegacy)
			if _, err := replayCapturingDispatch(t, recorded, runID); err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			encoded := encodeHistory(t, recorded)
			proof.Write(t, "emit-past-cancel", filepath.Base(path), encoded)
			written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
			if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
				if err := os.WriteFile(path, encoded, 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
				written = path
			}
			t.Logf("recorded %d events of run %s into %s, replayed clean; markers %v",
				len(recorded.GetEvents()), runID, written, markers)
		})
	}
}

// executeTaskCompleted reports whether a history holds an ExecuteTask's
// completion.
func executeTaskCompleted(recorded *historypb.History) bool {
	scheduled := map[int64]bool{}
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			scheduled[event.GetEventId()] = true
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil && scheduled[attrs.GetScheduledEventId()] {
			return true
		}
	}
	return false
}
