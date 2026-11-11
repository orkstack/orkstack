package engine_test

import (
	"testing"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRootRun_ChangesNoCommand is why recording a chain's root takes no
// workflow.GetVersion gate: it replays the recorded composed parent with the
// one thing the root depends on turned on in its start payload, in memory and
// never on disk, and the replay still passes. Current code then sends a child
// plan and memo the recorded build never sent, and the same commands in the
// same order. Everything the root changes is inside a payload, and a recorded
// plan without the field keeps the bytes it recorded
// (TestComposedHistory_PayloadsMatchWhatCurrentCodeSends).
//
// TestComposedHistory_CatchesAChildStartWithoutItsRender is the negative
// control: this same fixture and replayer do fail when a command moves.
func TestRootRun_ChangesNoCommand(t *testing.T) {
	requireComposedFixtures(t)
	recorded := loadHistoryFile(t, composedParentHistory)
	_, _, _, plan := goldenWorkflowArgs(t, recorded)
	if plan.RecordRootRun {
		t.Fatal("the recorded parent already asks for the root: it cannot show the difference")
	}
	plan.RecordRootRun = true
	plan.RootRunIndexed = true
	encoded, err := converter.GetDefaultDataConverter().ToPayload(plan)
	must.OK(t, err)
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			attrs.GetInput().GetPayloads()[3] = encoded
		}
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	must.OKf(t, replayer.ReplayWorkflowHistory(nil, recorded), "recording the root moved a command, so it needs a gate (AGENTS.md §7.2)")
}
