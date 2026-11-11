package engine

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestSleepingHistory_CatchesAMovedTimer is what makes the sleeping fixture
// worth having on the day it is recorded: it replays the fixture against the
// real RunWorkflow with ONE thing moved — openSleep starts its timer after
// the TASK_SLEEPING emit instead of before it — and that replay MUST fail. If
// it passes, the fixture cannot see a reordered sleep command and
// TestRunWorkflow_ReplaysCommittedSleepingHistory proves nothing.
func TestSleepingHistory_CatchesAMovedTimer(t *testing.T) {
	const path = "testdata/history/run-sleeping-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_SLEEPING_FIXTURES") != "" {
			t.Fatalf("the sleeping replay fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	sleepTimerAfterEmit = true
	defer func() { sleepTimerAfterEmit = false }()

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the sleeping fixture against a body whose timer moved after the emit " +
			"SUCCEEDED: the fixture cannot see a reordered sleep command")
	}
	message := err.Error()
	if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
	t.Logf("moved timer rejected by the replayer, as it must be: %v", err)
}
