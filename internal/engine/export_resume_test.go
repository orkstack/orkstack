package engine

import (
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/workflow"
)

// ResetRunWorkflow is RunWorkflow as a reset run executes it, for external
// tests: a test environment cannot reset a workflow, so the predicate is
// stood in for, and so is whether the plan has a concurrency limit.
func ResetRunWorkflow(hasConcurrency bool) func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, RunPlan, *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
	deps := workflowDeps{
		isResetRun:     func(*workflow.Info) bool { return true },
		hasConcurrency: func(RunPlan) bool { return hasConcurrency },
	}
	return func(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan RunPlan, carry *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
		return runWorkflowReleasing(ctx, deps, runID, wf, input, plan, carry)
	}
}
