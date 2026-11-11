//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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
			recorded, policy := runRetryToCompletion(t, ctx, s, compile(t, slowRetryDocument), plan)
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

// TestIntegration_AVersionStoredBeforeTheRetryRuleIsScheduled: a version
// applied before docs/DSL.md rule 47 may carry a retry: the cluster refuses —
// an explicit maximum interval below the initial one, or a backoff
// coefficient below 1 — and the cluster refuses it by failing the workflow
// task, which it then retries forever. Such a task is scheduled with the
// nearest policy the cluster accepts, and the run completes with no failed
// workflow task. So does a stored value rule 47 refuses that the cluster
// would not: a negative interval, a coefficient that is not a number, and
// +Inf, which is scheduled as it is. A NaN reads as the default only in a run
// whose plan carries the maximum retry interval; in a run submitted without
// it the cluster is sent the NaN, and accepts it.
func TestIntegration_AVersionStoredBeforeTheRetryRuleIsScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "stored-retry", false, canFixtureBehavior)
	for _, tc := range []struct {
		name        string
		retry       *orkstackv1.RetryPolicy
		maximum     time.Duration
		coefficient float64
		unplanned   bool
	}{
		{"a maximum below the initial interval",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10}, 30 * time.Second, 2, false},
		{"a coefficient below 1",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.5, MaximumIntervalSeconds: 20}, 20 * time.Second, 1, false},
		{"a coefficient that is not a number",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN(), MaximumIntervalSeconds: 20}, 20 * time.Second, 2, false},
		{"a coefficient that is not a number, in a run submitted without the number",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN(), MaximumIntervalSeconds: 20}, 20 * time.Second, math.NaN(), true},
		{"an infinite coefficient",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.Inf(1), MaximumIntervalSeconds: 20}, 20 * time.Second, math.Inf(1), false},
		{"a negative initial interval",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: -5, MaximumIntervalSeconds: 20}, 20 * time.Second, 2, false},
		{"a negative maximum",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, MaximumIntervalSeconds: -1}, time.Minute, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := compile(t, slowRetryDocument)
			wf.GetTasks()[0].Retry = tc.retry
			plan := s.plan(1000, nil)
			if !tc.unplanned {
				plan.RetryMaxIntervalSeconds = 60
			}
			_, policy := runRetryToCompletion(t, ctx, s, wf, plan)
			if policy.GetMaximumInterval().AsDuration() != tc.maximum || !sameCoefficient(policy.GetBackoffCoefficient(), tc.coefficient) {
				t.Fatalf("the task was scheduled with maximum %s and coefficient %g, want %s and %g",
					policy.GetMaximumInterval().AsDuration(), policy.GetBackoffCoefficient(), tc.maximum, tc.coefficient)
			}
			t.Logf("EVIDENCE %s: retry policy initial %s, coefficient %g, maximum %s; run completed with no failed workflow task",
				tc.name, policy.GetInitialInterval().AsDuration(), policy.GetBackoffCoefficient(), policy.GetMaximumInterval().AsDuration())
		})
	}
}

// runRetryToCompletion starts one run of wf, fails the test on any failed
// workflow task or a run that does not complete, and returns its history and
// the retry policy its one task activity was scheduled with.
func runRetryToCompletion(t *testing.T, ctx context.Context, s *canStack, wf *orkstackv1.Orkflow, plan engine.RunPlan) (*historypb.History, *commonpb.RetryPolicy) {
	t.Helper()
	runID := fmt.Sprintf("ork-retry-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, wf, nil, plan), "StartRun")
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
	return recorded, scheduled[0].GetRetryPolicy()
}
