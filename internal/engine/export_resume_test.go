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
// stood in for, and so is whether the plan has a concurrency limit.
func ResetRunWorkflow(limitUnbuilt bool) func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, RunPlan, *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
	deps := workflowDeps{
		isResetRun:   func(*workflow.Info) bool { return true },
		limitUnbuilt: func(RunPlan) bool { return limitUnbuilt },
	}
	return func(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan RunPlan, carry *orkstackv1.RunContinuation) (*orkstackv1.Run, error) {
		return runWorkflowReleasing(ctx, deps, runID, wf, input, plan, carry)
	}
}

// ResumeAdmittedRunsForTest has this build resume runs that hold an identity
// until the test ends: what a reset run of one does is built, and what
// ResumeRun checks before it is not (ResumePartAdmittedRun).
func ResumeAdmittedRunsForTest(t interface{ Cleanup(func()) }) {
	why := resumePartsUnbuilt[ResumePartAdmittedRun]
	delete(resumePartsUnbuilt, ResumePartAdmittedRun)
	t.Cleanup(func() { resumePartsUnbuilt[ResumePartAdmittedRun] = why })
}
