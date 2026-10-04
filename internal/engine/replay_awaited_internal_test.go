package engine

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestAwaitedHistory_CatchesAMovedTimer is what makes the awaited fixture worth
// having on the day it is recorded: it replays the resolved fixture against
// the real RunWorkflow with ONE thing moved — openAwait starts the deadline
// timer after the TASK_AWAITING emit instead of before it — and that replay
// MUST fail. If it passes, the fixture cannot see a reordered await command
// and TestRunWorkflow_ReplaysCommittedAwaitedHistories proves nothing.
func TestAwaitedHistory_CatchesAMovedTimer(t *testing.T) {
	const path = "testdata/history/run-awaited-resolved-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_AWAITED_FIXTURES") != "" {
			t.Fatalf("the awaited replay fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	awaitDeadlineAfterEmit = true
	defer func() { awaitDeadlineAfterEmit = false }()

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the awaited fixture against a body whose deadline timer moved after the emit " +
			"SUCCEEDED: the fixture cannot see a reordered await command")
	}
	message := err.Error()
	if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
	t.Logf("moved timer rejected by the replayer, as it must be: %v", err)
}
