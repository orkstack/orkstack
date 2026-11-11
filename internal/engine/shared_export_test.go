package engine

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// CompositionRowsForTest builds the run state of wf as RunWorkflow does —
// newRunState, then decideSharedOwner inside a workflow — and returns each
// composed task's TaskChild row.
func CompositionRowsForTest(t *testing.T, wf *orkstackv1.Orkflow) map[string]*orkstackv1.TaskChild {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	rows := map[string]*orkstackv1.TaskChild{}
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		s := newRunState(ctx, "run-rows", wf, nil, RunPlan{})
		s.decideSharedOwner(ctx)
		for id, row := range s.taskRuns {
			if row.GetChild() != nil {
				rows[id] = row.GetChild()
			}
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("build the run state: %v", err)
	}
	return rows
}
