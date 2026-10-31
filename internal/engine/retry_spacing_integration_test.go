//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// slowRetryDocument retries a task whose first wait is two minutes and that
// sets no maximum interval: above both the legacy 100 seconds and the
// default minute.
const slowRetryDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: slow-retry
tasks:
  - id: fetch
    uses: shell
    retry: {maximum_attempts: 3, initial_interval_seconds: 120}
    with: {command: "echo fetch"}
`

// TestIntegration_ARetryWithALongInitialIntervalIsScheduled: the cluster
// refuses a retry policy whose maximum interval is below its initial one, and
// refuses it by failing the workflow task, which it then retries forever. A
// retry: whose initial interval is above the default maximum, and that sets
// no maximum of its own, is therefore scheduled with a maximum equal to its
// initial interval — under a plan that carries the number and under one that
// does not (every plan built before the field) — and the run completes with
// no failed workflow task.
func TestIntegration_ARetryWithALongInitialIntervalIsScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "slow-retry", false, canFixtureBehavior)
	for _, tc := range []struct {
		name    string
		seconds int32
	}{{"a plan with the number", 60}, {"a plan without it", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			plan := s.plan(1000, nil)
			plan.RetryMaxIntervalSeconds = tc.seconds
			runID := fmt.Sprintf("ork-slow-retry-%d", time.Now().UnixNano())
			if err := s.starter.StartRun(ctx, runID, compile(t, slowRetryDocument), nil, plan); err != nil {
				t.Fatalf("StartRun: %v", err)
			}
			waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
			defer stop()
			runErr := s.client.GetWorkflow(waitCtx, runID, "").Get(waitCtx, nil)
			recorded := s.history(runID, "")
			for _, event := range recorded.GetEvents() {
				if attrs := event.GetWorkflowTaskFailedEventAttributes(); attrs != nil {
					_ = s.client.TerminateWorkflow(ctx, runID, "", "the test saw its workflow task fail")
					t.Fatalf("the run's workflow task failed (%v): %s", attrs.GetCause(), attrs.GetFailure().GetMessage())
				}
			}
			if runErr != nil {
				_ = s.client.TerminateWorkflow(ctx, runID, "", "the test gave up on it")
				t.Fatalf("the run did not complete: %v", runErr)
			}
			if last := recorded.GetEvents()[len(recorded.GetEvents())-1]; last.GetEventType() != enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
				t.Fatalf("the run closed with %v, want completed", last.GetEventType())
			}
			scheduled := recordedScheduled(recorded, task.ActivityExecuteTask)
			if len(scheduled) != 1 {
				t.Fatalf("%d task activities scheduled, want 1", len(scheduled))
			}
			policy := scheduled[0].GetRetryPolicy()
			if policy.GetInitialInterval().AsDuration() != 2*time.Minute || policy.GetMaximumInterval().AsDuration() != 2*time.Minute {
				t.Fatalf("the task was scheduled with initial %s and maximum %s, want both 2m0s",
					policy.GetInitialInterval().AsDuration(), policy.GetMaximumInterval().AsDuration())
			}
			t.Logf("EVIDENCE %s: retry policy initial %s, maximum %s; run completed with no failed workflow task (markers %v)",
				tc.name, policy.GetInitialInterval().AsDuration(), policy.GetMaximumInterval().AsDuration(),
				versionMarkers(t, recorded)[engine.RetryMaxIntervalChange])
		})
	}
}
