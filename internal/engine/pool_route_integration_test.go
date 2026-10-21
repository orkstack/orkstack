//go:build integration

package engine_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_AHandWiredPlanOnAReservedChannelFailsReservedQueue: a
// RunPlan that SubmitRun would never build — one routing the default pool
// onto the engine's own event channel, or one with no route for a task's
// pool — started against real Temporal ends FAILED, the task FAILED with its
// own failure code naming the rule it broke, and nothing is dispatched to the
// channel it was given. The plan is wired by hand, the way a harness or a replica with a
// stale plan builder would start it (routeForTask, workflow.go).
func TestIntegration_AHandWiredPlanOnAReservedChannelFailsReservedQueue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pool   string
		routes map[string]string
		code   string
	}{
		{name: "the default pool routed to the engine's event channel", routes: map[string]string{
			task.DefaultPool: task.EngineEventQueue,
		}, code: engine.ReasonReservedQueue},
		{name: "a pool the plan does not route", pool: "gpu", routes: map[string]string{
			task.DefaultPool: task.DefaultQueue,
		}, code: engine.ReasonInvalidPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newAwaitStack(t, awaitStackOptions{noOrker: true})
			wf := &orkstackv1.Orkflow{Name: "hand-wired", Version: "1.0.0", Tasks: []*orkstackv1.Task{
				{Id: "crunch", Pool: tc.pool, Image: &orkstackv1.ImageRef{Reference: "alpine"}},
			}}
			runID := engine.NewRunID()
			if err := s.starter.StartRun(s.ctx, runID, wf, nil,
				engine.RunPlan{Namespace: "default", Routes: tc.routes}); err != nil {
				t.Fatalf("StartRun: %v", err)
			}
			run := s.waitRun(runID)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" ||
				run.GetError().GetDetails() != `task "crunch"` {
				t.Fatalf("run %v, error %v; want FAILED, RUN_FAILED, naming task crunch", run.GetStatus(), run.GetError())
			}
			crunch := taskRunNamed(run, "crunch")
			if crunch.GetStatus() != orkstackv1.Status_STATUS_FAILED || crunch.GetError().GetCode() != tc.code ||
				crunch.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
				t.Fatalf("crunch %v, error %v; want FAILED with the task's code %s, a contract violation",
					crunch.GetStatus(), crunch.GetError(), tc.code)
			}
			if crunch.GetAttempt() != 0 || crunch.GetStartedAt() != nil || crunch.GetScheduledAt() != nil {
				t.Fatalf("crunch was dispatched (attempt %d, scheduled %v, started %v); a refused route starts nothing",
					crunch.GetAttempt(), crunch.GetScheduledAt(), crunch.GetStartedAt())
			}
		})
	}
}
