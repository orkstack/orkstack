package engine

import (
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/workflow"
)

// CutRecordedRow is what a plan records of row, of task t, for external tests.
func CutRecordedRow(t *orkstackv1.Task, row *orkstackv1.TaskRun) *orkstackv1.TaskRun {
	return cutRecordedRow((&planTask{def: t}).kind(), row)
}

// ResetRunWorkflow is RunWorkflow as a reset run executes it, for external
// tests: a test environment cannot reset a workflow, so the predicate is
// stood in for.
func ResetRunWorkflow() func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, RunPlan, *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
	deps := workflowDeps{isResetRun: func(*workflow.Info) bool { return true }}
	return func(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan RunPlan, carry *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
		return runWorkflowReleasing(ctx, deps, runID, wf, input, plan, carry)
	}
}
