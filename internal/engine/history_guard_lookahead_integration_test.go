//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_RecordHistoryGuardLookaheadHistory records the guard
// lookahead's fixture from a REAL run on a real cluster, by a versioned
// worker, as the golden recorders do: a 30-task chain under a 480 KiB
// history limit and no continuation stops RUN_HISTORY_LIMIT, a few tasks in,
// before a dispatch whose command — counted at the plan's 256 KiB values
// bound — its failure path could not follow — recording the lazy run-history-guard-lookahead marker —
// and the cluster records it Failed, never Terminated. It never skips: by
// default the recording lands under ORK_PROOF_DIR and the committed fixture
// is left alone; only ORK_RECORD_GOLDEN_HISTORY=1 also writes
// run-history-guard-lookahead-v1.json. It FAILS unless the shape lands and the
// recording replays.
func TestIntegration_RecordHistoryGuardLookaheadHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "guard-lookahead-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-lookahead-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)
	// The guard alone: a history limit, no continuation budget, no cap — the
	// shape of the history-limit fixture's plan.
	plan := rawPlan(s.orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: 480 << 10, Reserve: engine.DefaultHistoryReserve}
	runID := fmt.Sprintf("ork-history-guard-lookahead-%d", time.Now().UnixNano())
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: guard-lookahead\nversion: \"1.0.0\"\ninputs:\n  blob: {type: string}\ntasks:\n"+guardChain("c", 30, "")),
		blobInput(t, 4<<10), plan)
	if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the recorded run closed %v, want Failed (never Terminated)", status)
	}
	recorded := s.history(runID, "")
	if got := failureType(t, recorded); got != engine.RunHistoryLimitCode {
		t.Fatalf("the recorded run failed with type %q", got)
	}
	markers := versionMarkers(t, recorded)
	if markers[engine.HistoryGuardLookaheadChange] == 0 {
		t.Fatalf("markers %v: the run did not stop at the lookahead", markers)
	}
	if facts := factsOf(t, recorded); len(facts.completedAt["c00"]) == 0 {
		t.Fatal("the run stopped before any task ran: lower the input or raise the limit so the fixture shows tasks before the crossing")
	}
	published := publishedOf(t, recorded)
	requireContiguous(t, published)
	if last := published[len(published)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetTaskId() != "" {
		t.Fatalf("the recorded run ended %v owned by %q", last.GetType(), last.GetTaskId())
	}
	if err := replayRecorded(recorded, runID); err != nil {
		t.Fatalf("the history just recorded does not replay against current code: %v", err)
	}
	encoded := encodeHistory(t, recorded)
	proof.Write(t, "history-guard-lookahead", filepath.Base(historyGuardLookaheadFixture), encoded)
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		if err := os.WriteFile(historyGuardLookaheadFixture, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", historyGuardLookaheadFixture, err)
		}
		written = historyGuardLookaheadFixture
	}
	t.Logf("recorded %d events of run %s into %s (deployment %s), replayed clean; %d events published, contiguous",
		len(recorded.GetEvents()), runID, written, deployment, len(published))
}
