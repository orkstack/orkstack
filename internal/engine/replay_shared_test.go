package engine_test

import (
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
)

// crossNamespaceHistory is the cross-namespace fixture: one run of the cached
// pipeline whose planned definition another namespace shared (it runs in
// "search", owned by "platform"), recorded by a versioned worker against a
// real cluster by TestIntegration_RecordCrossNamespaceRunHistory, never
// hand-edited (testdata/history/README.md). It holds the first
// cross-namespace version marker, the first snapshot naming an
// orkflow_namespace and the first CacheContext naming one.
const crossNamespaceHistory = "testdata/history/run-cross-namespace-v1.json"

// requireCrossNamespaceFixture skips while the fixture has not been recorded
// yet; ORK_REQUIRE_CROSS_NAMESPACE_FIXTURE makes its absence a failure.
func requireCrossNamespaceFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(crossNamespaceHistory); err != nil {
		hint := "record it with ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ " +
			"-run TestIntegration_RecordCrossNamespaceRunHistory"
		if os.Getenv("ORK_REQUIRE_CROSS_NAMESPACE_FIXTURE") != "" {
			t.Fatalf("the cross-namespace replay fixture %s is missing (%v); %s", crossNamespaceHistory, err, hint)
		}
		t.Skipf("%s is not recorded yet (%v): %s", crossNamespaceHistory, err, hint)
	}
}

func TestRunWorkflow_ReplaysCommittedCrossNamespaceHistory(t *testing.T) {
	requireCrossNamespaceFixture(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, crossNamespaceHistory); err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A shared child run in flight when this build was deployed would fail its next workflow task. "+
			"Restore the command stream, or re-record and say why.", crossNamespaceHistory, err)
	}
}

func TestCrossNamespaceHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireCrossNamespaceFixture(t)
	recorded := loadHistoryFile(t, crossNamespaceHistory)
	recordedInputs, recordedEvents, _, recordedRun := goldenActivityPayloads(t, recorded)
	// The fixture is what it claims to be: a cached task's input names the
	// owner, and the run it finished with does too.
	if owner := recordedInputs["featurize"]["cache"].(map[string]any)["orkflowNamespace"]; owner != "platform" {
		t.Fatalf("the fixture's featurize cache context names owner %v, want platform", owner)
	}
	sent, events, run := reexecuteCached(t, recorded)
	for _, diff := range cachedInputDiffs(recordedInputs, sent) {
		t.Errorf("the ExecuteTask input is not the one the recorded build sent: %s", diff)
	}
	compareEventPayloads(t, recordedEvents, events)
	if diff := payloadDiff(compactRecordedRun(t, recordedRun), run); diff != "" {
		t.Errorf("the Run this code returns no longer matches the recorded one:\n%s", diff)
	}
}

// TestCrossNamespaceHistory_PayloadCheckCatchesAMissingOwner: with the gate's
// DefaultVersion branch taken, the same comparison MUST find the owner gone.
func TestCrossNamespaceHistory_PayloadCheckCatchesAMissingOwner(t *testing.T) {
	requireCrossNamespaceFixture(t)
	restore := engine.DisableCrossNamespaceGateForTest()
	defer restore()
	recorded := loadHistoryFile(t, crossNamespaceHistory)
	recordedInputs, _, _, _ := goldenActivityPayloads(t, recorded)
	sent, _, _ := reexecuteCached(t, recorded)
	diffs := cachedInputDiffs(recordedInputs, sent)
	joined := strings.Join(diffs, "\n")
	if len(diffs) == 0 || !strings.Contains(joined, "orkflowNamespace") {
		t.Fatalf("a body that names no owner sent the recorded inputs: the payload check cannot see it (%s)", joined)
	}
}
