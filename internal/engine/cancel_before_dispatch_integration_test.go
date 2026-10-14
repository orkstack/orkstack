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

// TestIntegration_RecordCancelBeforeDispatchHistory records the
// cancel-before-dispatch fixture from a REAL run on a real cluster, by a
// versioned worker, as the golden recorders do: the operator's cancel is
// recorded while a switch's checkpoint write is held (continuation chaos
// C4), so the server refuses the continue-as-new (UNHANDLED_COMMAND) and the
// retried workflow task sees the cancel at the run's next dispatch: it
// records the lazy cancel-before-dispatch marker and ends canceled without
// dispatching the task. It never skips: by default the recording lands under
// ORK_PROOF_DIR and the committed fixture is left alone; only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes run-cancel-before-dispatch-v1.json.
// It FAILS unless the shape lands and the recording replays.
func TestIntegration_RecordCancelBeforeDispatchHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "cancel-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-cancel-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	hook := newBlockingHook(func(kind string, _ int64) bool { return kind == "checkpoint" })
	t.Cleanup(func() { hook.open(nil) })
	s.startReplica("replica-a", replicaOptions{beforeWrite: hook.write, deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)
	plan, _ := s.tcPlan()
	runID := fmt.Sprintf("ork-cancel-before-dispatch-%d", time.Now().UnixNano())
	s.transcript.run(runID)
	s.start(runID, compile(t, "name: cancel-before-dispatch\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 12, "")), nil, plan)
	entered := hook.waitEntered(t, 2*time.Minute)
	if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	hook.open(nil)
	if since := time.Since(entered); since >= 5*time.Second {
		t.Fatalf("the hook was released %s after its entry, outside the 5 s window", since)
	}
	if status, _ := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the recorded run closed %v, want Canceled", status)
	}
	recorded := s.history(runID, "")
	facts := factsOf(t, recorded)
	marker := false
	for change := range versionMarkers(t, recorded) {
		marker = marker || change == engine.CancelBeforeDispatchChange
	}
	if len(facts.unhandledCommandFailures()) == 0 || !marker || facts.continuedAt != 0 {
		t.Fatalf("UNHANDLED_COMMAND %d, marker %t, switched %t: the shape did not land",
			len(facts.unhandledCommandFailures()), marker, facts.continuedAt != 0)
	}
	published := publishedOf(t, recorded)
	requireContiguous(t, published)
	if last := published[len(published)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || last.GetTaskId() != "" {
		t.Fatalf("the recorded run ended %v owned by %q", last.GetType(), last.GetTaskId())
	}
	if err := replayRecorded(recorded, runID); err != nil {
		t.Fatalf("the history just recorded does not replay against current code: %v", err)
	}
	encoded := encodeHistory(t, recorded)
	proof.Write(t, "cancel-before-dispatch", filepath.Base(cancelBeforeDispatchFixture), encoded)
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		if err := os.WriteFile(cancelBeforeDispatchFixture, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", cancelBeforeDispatchFixture, err)
		}
		written = cancelBeforeDispatchFixture
	}
	t.Logf("recorded %d events of run %s into %s (deployment %s), replayed clean; %d events published, contiguous",
		len(recorded.GetEvents()), runID, written, deployment, len(published))
}
