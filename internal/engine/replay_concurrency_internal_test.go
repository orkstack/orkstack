package engine

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestGateRebuildFirstHistory_CatchesALateRebuildState replays the
// rebuild-first fixture against a gate that sets its rebuilding state only
// when the rebuild begins, so the acquire sent with its start is granted.
// That replay MUST fail: otherwise the fixture cannot see an acquire granted
// past a holder the rebuild restores.
func TestGateRebuildFirstHistory_CatchesALateRebuildState(t *testing.T) {
	const path = "testdata/history/concurrency-gate-rebuild-first-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
			t.Fatalf("the rebuild-first fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	gateRebuildNotFirst = true
	defer func() { gateRebuildNotFirst = false }()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: ConcurrencyGateWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the rebuild-first fixture against a gate that grants the acquire sent with its start " +
			"SUCCEEDED: the fixture cannot see the rebuild's state")
	}
	if message := err.Error(); !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
}
