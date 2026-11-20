package engine

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestGateOrdinalHistory_CatchesAGateThatDoesNotReplace replays the
// ordinal-replace fixture against a gate that reads a holder's newer ordinal
// as its own entry, so it never frees the slot nor grants the run waiting
// behind it. That replay MUST fail: otherwise the fixture cannot see the
// replacement's commands, and its replay test proves nothing.
func TestGateOrdinalHistory_CatchesAGateThatDoesNotReplace(t *testing.T) {
	const path = "testdata/history/concurrency-gate-ordinal-replace-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
			t.Fatalf("the ordinal fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	gateOrdinalNotReplacing = true
	defer func() { gateOrdinalNotReplacing = false }()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: ConcurrencyGateWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the ordinal fixture against a gate that does not replace SUCCEEDED: " +
			"the fixture cannot see the replacement")
	}
	if message := err.Error(); !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
}

// TestConcurrencyQueuedHistory_CatchesADeadlineLeftRunning replays the
// queued fixture against a run side that leaves its deadline timer pending
// at the grant. That replay MUST fail: otherwise the fixture cannot see the
// deadline's cancel, and a run would keep a timer for a wait it left.
func TestConcurrencyQueuedHistory_CatchesADeadlineLeftRunning(t *testing.T) {
	const path = "testdata/history/concurrency-run-queued-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
			t.Fatalf("the queued fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	concDeadlineLeftRunning = true
	defer func() { concDeadlineLeftRunning = false }()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if err == nil {
		t.Fatal("replaying the queued fixture against a run that leaves its deadline running SUCCEEDED: " +
			"the fixture cannot see the deadline's cancel")
	}
	if message := err.Error(); !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("replay failed, but not with a determinism violation: %v", err)
	}
}

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
