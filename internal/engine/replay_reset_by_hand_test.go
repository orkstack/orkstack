package engine_test

import (
	"os"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// A run reset by hand to a point past R replays its copied prefix as a
// reset run: its original run id is not its own, so the reset-run predicate
// is true from its first event. The prefix passed R with no run-resume
// marker, so the lazy gate answers DefaultVersion and the run replays
// exactly what it recorded, issuing nothing new (E85 §30.2). This is a
// correctness guard for runs of this release, not compatibility: without
// the gate, every such reset would fail replay and stall.
func TestRunWorkflow_AResetByHandPastRReplaysCleanly(t *testing.T) {
	f, err := os.Open(goldenHistory)
	must.OK(t, err)
	t.Cleanup(func() { _ = f.Close() })
	recorded, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	must.OK(t, err)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	own := replayExecution("ork-golden", recorded)
	resetByHand := workflow.Execution{ID: own.ID, RunID: own.RunID + "-reset-by-hand"}
	if err := replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: resetByHand,
	}); err != nil {
		t.Fatalf("a history replayed as a reset run past R failed: %v", err)
	}
}
