package engine_test

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The spacing between a task's retries (docs/DSL.md "Tasks"): a `retry:` that
// sets no maximum_interval_seconds waits at most the number its run's plan
// carries (RunPlan.RetryMaxIntervalSeconds), decided at submit, so a release
// that changes the default moves only new runs. A run whose history predates
// the retry-max-interval gate, or whose plan carries no number, keeps the
// 100 seconds every earlier build hardcoded.

// retrySpacingDocument is one task with the retry policy given, an item of a fan-out
// with the same policy, and a task without retry:.
func retrySpacingDocument(retry string) string {
	return fmt.Sprintf(`
apiVersion: orkstack/v1
kind: Orkflow
name: retry-spacing
inputs:
  parts: {type: array}
tasks:
  - id: first
    uses: shell
    retry: %s
    with: {op: exec}
  - id: spread
    uses: shell
    needs: [first]
    retry: %s
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: plain
    uses: shell
    needs: [spread]
    with: {op: exec}
`, retry, retry)
}

func runRetrySpacing(t *testing.T, retry string, plan engine.RunPlan) *dispatchCapture {
	t.Helper()
	capture := &dispatchCapture{}
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, itemBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-retry-spacing",
		compile(t, retrySpacingDocument(retry)), spreadInput(t, 2), plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	return capture
}

// maximumIntervals is the MaximumInterval of every ExecuteTask with a retry
// policy of more than one attempt, in schedule order.
func maximumIntervals(capture *dispatchCapture) []time.Duration {
	var out []time.Duration
	for _, options := range capture.of(task.ActivityExecuteTask) {
		if options.RetryPolicy != nil && options.RetryPolicy.MaximumAttempts > 1 {
			out = append(out, options.RetryPolicy.MaximumInterval)
		}
	}
	return out
}

func requireIntervals(t *testing.T, got []time.Duration, n int, want time.Duration) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("%d task activities retry, want %d (got %v)", len(got), n, got)
	}
	for i, d := range got {
		if d != want {
			t.Fatalf("retrying task activity %d waits at most %s between attempts, want %s (all: %v)", i, d, want, got)
		}
	}
}

func retryPlan(seconds int32) engine.RunPlan {
	plan := defaultPlan()
	plan.RetryMaxIntervalSeconds = seconds
	return plan
}

// TestRunWorkflow_AnUnsetMaximumIntervalWaitsThePlansNumber: a task and every
// item whose retry: sets no maximum interval are scheduled with the plan's
// number, never below the policy's own initial interval (Temporal refuses a
// maximum below the initial). The gate is asked once, where the first such
// task is scheduled.
func TestRunWorkflow_AnUnsetMaximumIntervalWaitsThePlansNumber(t *testing.T) {
	cases := []struct {
		name  string
		retry string
		plan  int32
		want  time.Duration
	}{
		{name: "the default minute", retry: "{maximum_attempts: 3}", plan: 60, want: time.Minute},
		{name: "another number", retry: "{maximum_attempts: 3}", plan: 45, want: 45 * time.Second},
		{name: "an initial interval above it", retry: "{maximum_attempts: 3, initial_interval_seconds: 120}", plan: 60, want: 120 * time.Second},
		{name: "an initial interval below it", retry: "{maximum_attempts: 3, initial_interval_seconds: 5}", plan: 60, want: time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := runRetrySpacing(t, tc.retry, retryPlan(tc.plan))
			requireIntervals(t, maximumIntervals(capture), 3, tc.want)
			if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 1 || asked[0] != 1 {
				t.Fatalf("%s was answered %v, want one question answered 1", engine.RetryMaxIntervalChange, asked)
			}
		})
	}
}

// TestRunWorkflow_AnExplicitMaximumIntervalAsksNoRetryGate: the author's own
// maximum interval is never replaced, and a run none of whose tasks needs
// the plan's number never asks the gate.
func TestRunWorkflow_AnExplicitMaximumIntervalAsksNoRetryGate(t *testing.T) {
	capture := runRetrySpacing(t, "{maximum_attempts: 3, maximum_interval_seconds: 30}", retryPlan(60))
	requireIntervals(t, maximumIntervals(capture), 3, 30*time.Second)
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 0 {
		t.Fatalf("a run whose every retry sets its maximum asked %s %v", engine.RetryMaxIntervalChange, asked)
	}
}

// TestRunWorkflow_AGateOffRetryKeepsTheLegacy100Seconds is the DefaultVersion
// branch, the command sequence of every build before the gate: 100 seconds,
// whatever the plan says, and no version asked.
func TestRunWorkflow_AGateOffRetryKeepsTheLegacy100Seconds(t *testing.T) {
	defer engine.DisableRetryMaxIntervalGateForTest()()
	capture := runRetrySpacing(t, "{maximum_attempts: 3}", retryPlan(60))
	requireIntervals(t, maximumIntervals(capture), 3, 100*time.Second)
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 0 {
		t.Fatalf("a gate-off run asked %s %v", engine.RetryMaxIntervalChange, asked)
	}
}

// TestRunWorkflow_APlanWithoutTheRetryNumberKeeps100Seconds: a plan built
// before RunPlan.RetryMaxIntervalSeconds carries none, so its tasks keep the
// 100 seconds and the run asks no gate.
func TestRunWorkflow_APlanWithoutTheRetryNumberKeeps100Seconds(t *testing.T) {
	capture := runRetrySpacing(t, "{maximum_attempts: 3}", defaultPlan())
	requireIntervals(t, maximumIntervals(capture), 3, 100*time.Second)
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 0 {
		t.Fatalf("a plan without the number asked %s %v", engine.RetryMaxIntervalChange, asked)
	}
}

// TestRunWorkflow_ACarriedGenerationKeepsTheRetryDecision: a run that carries
// on asks the gate once, in the generation where its first such task is
// scheduled, carries the decision (VersionDecisions), and schedules every
// later generation's tasks with the same number without asking again.
func TestRunWorkflow_ACarriedGenerationKeepsTheRetryDecision(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	capture := &dispatchCapture{}
	plan := retryPlan(60)
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
		MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget

	g := runGenerations(t, "run-carried-retry", compile(t, retrySpacingDocument("{maximum_attempts: 3}")), spreadInput(t, 2), plan,
		itemBehavior, func(h *harness, _ int) {
			h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
		})
	if g.err != nil {
		t.Fatalf("the run ended with %v", g.err)
	}
	if len(g.carries) < 2 {
		t.Fatalf("the run carried on %d times, want at least 2", len(g.carries))
	}
	if got := g.carries[len(g.carries)-1].GetVersionDecisions()[engine.RetryMaxIntervalChange]; got != 1 {
		t.Errorf("the last carry holds %s = %d, want the decision 1", engine.RetryMaxIntervalChange, got)
	}
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 1 {
		t.Errorf("%s was asked %v across the generations, want exactly one question", engine.RetryMaxIntervalChange, asked)
	}
	requireIntervals(t, maximumIntervals(capture), 3, time.Minute)
}

// TestRunWorkflow_AnInitialIntervalAboveTheLegacyMaximumIsScheduled: under a
// plan without the number — every plan built before the field — a retry:
// whose initial interval is above the legacy 100 seconds, and that sets no
// maximum, is scheduled with a maximum equal to its initial interval. Every
// earlier build sent a maximum below the initial, which the cluster refuses
// (TestIntegration_ARetryWithALongInitialIntervalIsScheduled).
func TestRunWorkflow_AnInitialIntervalAboveTheLegacyMaximumIsScheduled(t *testing.T) {
	capture := runRetrySpacing(t, "{maximum_attempts: 3, initial_interval_seconds: 120}", defaultPlan())
	requireIntervals(t, maximumIntervals(capture), 3, 120*time.Second)
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 0 {
		t.Fatalf("a plan without the number asked %s %v", engine.RetryMaxIntervalChange, asked)
	}
}

// storedBeforeRule47 is retrySpacingDocument with every retry: replaced by
// one the DSL now refuses (docs/DSL.md rule 47), as a version applied before
// the rule is stored.
func storedBeforeRule47(t *testing.T, retry *orkstackv1.RetryPolicy) *orkstackv1.Orkflow {
	t.Helper()
	wf := compile(t, retrySpacingDocument("{maximum_attempts: 3}"))
	for _, spec := range wf.GetTasks() {
		if spec.GetRetry() != nil {
			spec.Retry = proto.Clone(retry).(*orkstackv1.RetryPolicy)
		}
	}
	return wf
}

// TestRunWorkflow_AVersionStoredBeforeTheRetryRuleIsScheduled: a version
// applied before rule 47 may carry a retry: the cluster refuses — an explicit
// maximum interval below the initial one, or a backoff coefficient below 1 —
// and the cluster refuses it by failing the workflow task forever. Such a
// task is scheduled with the nearest policy the cluster accepts: the maximum
// raised to the initial interval, the coefficient raised to 1. No gate is
// asked for either: no recorded history can hold the refused command. A NaN
// coefficient, which the cluster accepts, reads as the default only on the
// retry-max-interval gate's new path, in a run whose plan carries the number:
// a run submitted without it, and a history recorded with the gate off, keep
// scheduling the NaN as every earlier build did.
func TestRunWorkflow_AVersionStoredBeforeTheRetryRuleIsScheduled(t *testing.T) {
	for _, tc := range []struct {
		name        string
		retry       *orkstackv1.RetryPolicy
		planned     int32
		gateOff     bool
		maximum     time.Duration
		coefficient float64
		asked       []workflow.Version
	}{
		{"a maximum below the initial interval",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10}, 60, false, 30 * time.Second, 2.0, nil},
		{"a coefficient below 1",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.5, MaximumIntervalSeconds: 20}, 60, false, 20 * time.Second, 1, nil},
		{"a coefficient that is not a number",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN(), MaximumIntervalSeconds: 20}, 60, false, 20 * time.Second, 2,
			[]workflow.Version{1}},
		{"a coefficient that is not a number, in a plan without the number",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN(), MaximumIntervalSeconds: 20}, 0, false, 20 * time.Second, math.NaN(), nil},
		{"a coefficient that is not a number, with the gate off",
			&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN(), MaximumIntervalSeconds: 20}, 60, true, 20 * time.Second, math.NaN(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.gateOff {
				defer engine.DisableRetryMaxIntervalGateForTest()()
			}
			capture := &dispatchCapture{}
			h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, itemBehavior)
			h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
			h.env.ExecuteWorkflow(engine.RunWorkflow, "run-stored-before-rule-47",
				storedBeforeRule47(t, tc.retry), spreadInput(t, 2), retryPlan(tc.planned), (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			requireIntervals(t, maximumIntervals(capture), 3, tc.maximum)
			for _, options := range capture.of(task.ActivityExecuteTask) {
				if policy := options.RetryPolicy; policy.MaximumAttempts > 1 && !sameCoefficient(policy.BackoffCoefficient, tc.coefficient) {
					t.Fatalf("scheduled with backoff coefficient %g, want %g", policy.BackoffCoefficient, tc.coefficient)
				}
			}
			if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); !slices.Equal(asked, tc.asked) {
				t.Fatalf("%s asked %v, want %v", engine.RetryMaxIntervalChange, asked, tc.asked)
			}
		})
	}
}

// sameCoefficient is == with a NaN equal to a NaN.
func sameCoefficient(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}
