package engine

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestComposedHistory_CatchesAChildStartWithoutItsRender is what makes the
// composed parent fixture worth having on the day it is recorded: it replays
// the parent against the real RunWorkflow with ONE thing moved — the child is
// started without first recording its input render — and that replay MUST
// fail. If it passes, the fixture cannot see a dropped render marker and
// TestRunWorkflow_ReplaysCommittedComposedHistories proves nothing.
func TestComposedHistory_CatchesAChildStartWithoutItsRender(t *testing.T) {
	const path = "testdata/history/run-composed-parent-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_COMPOSED_FIXTURES") != "" {
			t.Fatalf("the composed replay fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordComposedRunHistory", path, err)
	}
	composeStartWithoutRender = true
	defer func() { composeStartWithoutRender = false }()

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the composed fixture against a body that starts the child without its recorded " +
			"input render SUCCEEDED: the fixture cannot see a dropped render")
	}
	message := err.Error()
	if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") &&
		!strings.Contains(strings.ToLower(message), "marker") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
	t.Logf("a child start without its render rejected by the replayer, as it must be: %v", err)
}
