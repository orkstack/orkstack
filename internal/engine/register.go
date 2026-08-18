package engine

import (
	"go.temporal.io/sdk/workflow"
)

// WorkflowRegistrar is the slice of worker.Worker that registers workflow
// types. Naming it lets a test — and any harness that stands up an engine
// worker — register through the same code the binary runs, so a lost or
// changed VersioningBehavior fails a test rather than only a production
// rollout.
type WorkflowRegistrar interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

// RegisterWorkflows registers the engine's workflow types, each with the
// versioning behaviour it needs. Under Worker Deployment Versioning a workflow
// type with no declared behaviour panics at registration, so this list is also
// the statement that both types have been considered.
//
// It lives in the engine package, not in cmd/ork, so that the integration
// tests which stand up engine workers (the versioning rollout, drain and
// first-release suites) register exactly what `bin/ork` registers. When it sat
// in package main, every one of those tests declared its own Pinned and the
// behaviours could only diverge from the binary silently.
func RegisterWorkflows(w WorkflowRegistrar) {
	// Pinned: an in-flight run finishes on the code that started it.
	//
	// RunWorkflow interprets a DAG whose scheduling order, event sequence and
	// activity arguments are all decided by current code. Change readyTasks'
	// ordering, the emit counter, or the shape of task.ExecuteTaskInput, and
	// replaying a half-finished history against the new build produces a
	// different command stream — a non-determinism error that kills a run the
	// operator did nothing to. AutoUpgrade would hand exactly that history to
	// the new build on its next workflow task. Pinned keeps the run on its own
	// version; the price is that v1 workers must stay up until the deployment
	// version reports Drained (docs/DEPLOY.md).
	w.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{
		Name:               RunWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	// AutoUpgrade: the claim is a long-lived key holder, not a run.
	//
	// Its history is one update and one timer, and its command stream is
	// independent of run semantics — replaying it against a newer build issues
	// the same commands — so the determinism argument above does not apply. It
	// lives for ORK_IDEMPOTENCY_TTL (default 24h), so pinning it would strand
	// a key holder on a version an operator wants to decommission, and that
	// version would never report Drained.
	w.RegisterWorkflowWithOptions(IdempotencyClaimWorkflow, workflow.RegisterOptions{
		Name:               ClaimWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
}
