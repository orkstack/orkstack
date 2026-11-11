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
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// orderedCancelDocument is the recorded run: two awaits that open once
// prepare has succeeded, and a task that needs both, which the cancel leaves
// never started. prepare gives the run an idle point before the awaits, so
// the continuation tests that switch every fixture at each one have one to
// switch at.
const orderedCancelDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: ordered-cancel
tasks:
  - id: prepare
    uses: shell
    with: {command: "echo prepare"}
  - id: approve
    needs: [prepare]
    await: {timeout_seconds: 3600}
  - id: sign-off
    needs: [prepare]
    await: {timeout_seconds: 3600}
  - id: release
    uses: shell
    needs: [approve, sign-off]
    with: {command: "echo release"}
`

// TestIntegration_RecordOrderedCancelHistory records the ordered-cancel
// fixture from a REAL run on a real cluster, by a versioned worker: a run
// with two awaits open is canceled by its operator, the cancel reaches
// runCtx with both deadline contexts as its children, and the Temporal Go
// SDK records the ordered-cancel flag in the live history. It never skips:
// by default the recording lands under ORK_PROOF_DIR and the committed
// fixture is left alone; only ORK_RECORD_GOLDEN_HISTORY=1 also writes it. It
// FAILS unless the run ends canceled, its history records the flag for that
// cancel, and the history replays against current code.
func TestIntegration_RecordOrderedCancelHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "ordered-cancel-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-ordered-cancel-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	plan := rawPlan(s.orkerQueue)
	plan.CommitRoot = "mem://commits"
	runID := fmt.Sprintf("ork-ordered-cancel-%d", time.Now().UnixNano())
	s.transcript.run(runID)
	s.start(runID, compile(t, orderedCancelDocument), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	for _, id := range []string{"approve", "sign-off"} {
		stream.waitFor(t, time.Minute, id+" awaiting", func(e *orkstackv1.Event) bool {
			return e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING && e.GetTaskId() == id
		})
	}
	// The stream shows an event before its publish activity has finished;
	// the cancel waits for the run to hold nothing but the two deadline
	// timers, so the one cancel reaches runCtx and nothing else.
	idle := time.NewTicker(100 * time.Millisecond)
	defer idle.Stop()
	for deadline := time.Now().Add(time.Minute); ; <-idle.C {
		described, err := s.control.DescribeWorkflowExecution(ctx, runID, "")
		must.OKf(t, err, "describe %s", runID)
		if len(described.GetPendingActivities()) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s still has %d activities pending after a minute", runID, len(described.GetPendingActivities()))
		}
	}
	must.OKf(t, s.control.CancelWorkflow(ctx, runID, ""), "cancel %s", runID)
	s.transcript.step("operator canceled run %s with both awaits open", runID)
	if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the recorded run closed %v, want Canceled", status)
	}
	recorded := s.history(runID, "")
	requireOrderedCancel(t, "the recorded run", recorded)
	published := publishedOf(t, recorded)
	requireContiguous(t, published)
	if last := published[len(published)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("the recorded run's stream ends with %v, want RUN_CANCELED", last.GetType())
	}
	must.OKf(t, replayRecorded(recorded, runID), "the history just recorded does not replay against current code")

	encoded := encodeHistory(t, recorded)
	proof.Write(t, "ordered-cancel", filepath.Base(orderedCancelFixture), encoded)
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		must.OKf(t, os.WriteFile(orderedCancelFixture, encoded, 0o644), "write %s", orderedCancelFixture)
		written = orderedCancelFixture
	}
	cancelAt, _ := orderedCancelOf(recorded)
	t.Logf("recorded %d events of run %s into %s, replayed clean; flag %d recorded at event %d, timers canceled %v",
		len(recorded.GetEvents()), runID, written, sdkFlagOrderedChildCancel, cancelAt.completedAt, cancelAt.canceled)
}
