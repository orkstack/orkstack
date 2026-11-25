package engine

import (
	"go.temporal.io/sdk/activity"
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
// the statement that every type has been considered.
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
	w.RegisterWorkflowWithOptions(RunWorkflowRaw, workflow.RegisterOptions{
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

	// AutoUpgrade, for the claim's reason: a concurrency gate holds a key for
	// as long as runs hold or queue at it, so pinning it would keep a version
	// from draining. Its commands come from its carried state, and every later
	// change to them takes a concurrency-gate-<change> GetVersion (E88 §5.1.10).
	w.RegisterWorkflowWithOptions(ConcurrencyGateWorkflow, workflow.RegisterOptions{
		Name:               ConcurrencyGateWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	// AutoUpgrade: a force cancel outlives no version a release waits on
	// beyond its own ceiling, and it acts on a run whatever version that run
	// is pinned to; every step is a local activity whose answer its history
	// records (design §22.1.2).
	w.RegisterWorkflowWithOptions(ForceCancelWorkflow, workflow.RegisterOptions{
		Name:               ForceCancelWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	// AutoUpgrade: a census fire is one local activity on a Schedule the
	// engine owns, and pins no version a release would wait on.
	w.RegisterWorkflowWithOptions(ConcurrencyCensusWorkflow, workflow.RegisterOptions{
		Name:               ConcurrencyCensusWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})

	// Pinned, for the reason RunWorkflow is and with none of the cost the
	// claim workflow avoids: a fire lives exactly as long as the run it
	// started, which is pinned to the same version, so it holds no version
	// open that the run does not, and its history never replays on a newer
	// build.
	w.RegisterWorkflowWithOptions(ScheduleFireWorkflow, workflow.RegisterOptions{
		Name:               ScheduleFireWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	// Pinned: a delivery lives at most its ladder and its settle (about a
	// minute at the default policy), so it holds its version open no longer
	// than the runs whose transitions queued it, and its history — two
	// activities — never replays on a build it did not start on.
	w.RegisterWorkflowWithOptions(WebhookDeliveryWorkflow, workflow.RegisterOptions{
		Name:               WebhookDeliveryWorkflowType,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})
}

// RegisterWebhookDeliveryActivities registers a delivery's two activities on
// the engine's own channel (EventQueue), which no Orker polls.
func RegisterWebhookDeliveryActivities(w ActivityRegistrar, a *WebhookDeliveryActivities) {
	w.RegisterActivityWithOptions(a.SendWebhookAttempt, activity.RegisterOptions{Name: ActivitySendWebhookAttempt})
	w.RegisterActivityWithOptions(a.SettleWebhookDelivery, activity.RegisterOptions{Name: ActivitySettleWebhookDelivery})
}

// ActivityRegistrar is the slice of worker.Worker that registers activities.
type ActivityRegistrar interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}
