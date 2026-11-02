package engine_test

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
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

// TestRunWorkflow_ALegacyRunKeepsTheLegacyMaximumInterval is the
// DefaultVersion branch, the command sequence of every build before the gate,
// reached both ways a run can meet it: the gate off (the history of a run
// recorded before it) and a plan built before RunPlan.RetryMaxIntervalSeconds,
// which carries none. 100 seconds, whatever else the plan says, raised to the
// policy's own initial interval where that is longer (every earlier build sent
// a maximum below it, which the cluster refuses:
// TestIntegration_ARetryWithALongInitialIntervalIsScheduled), and no version
// asked. The recorded legacy histories pin the same 100 seconds
// (TestRunWorkflow_ReplaysCommittedRetryMaxIntervalHistories, legacy).
func TestRunWorkflow_ALegacyRunKeepsTheLegacyMaximumInterval(t *testing.T) {
	cases := []struct {
		name    string
		retry   string
		gateOff bool
		plan    int32
		want    time.Duration
	}{
		{name: "the gate is off and the plan carries a number", retry: "{maximum_attempts: 3}", gateOff: true, plan: 60, want: 100 * time.Second},
		{name: "the plan carries no number", retry: "{maximum_attempts: 3}", plan: 0, want: 100 * time.Second},
		{name: "the gate is off and the initial interval is above 100 s",
			retry: "{maximum_attempts: 3, initial_interval_seconds: 150}", gateOff: true, plan: 60, want: 150 * time.Second},
		{name: "the plan carries no number and the initial interval is above 100 s",
			retry: "{maximum_attempts: 3, initial_interval_seconds: 120}", plan: 0, want: 120 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.gateOff {
				defer engine.DisableRetryMaxIntervalGateForTest()()
			}
			capture := runRetrySpacing(t, tc.retry, retryPlan(tc.plan))
			requireIntervals(t, maximumIntervals(capture), 3, tc.want)
			if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 0 {
				t.Fatalf("a legacy run asked %s %v", engine.RetryMaxIntervalChange, asked)
			}
		})
	}
}

// TestRunWorkflow_ARetryKeepsEveryOtherDefault: the gate moves the maximum
// interval alone. The single attempt of a task without retry: (or with one
// attempt), the 1 s initial interval and the 2.0 coefficient are what they
// always were. The document's plain task has no retry: at all.
func TestRunWorkflow_ARetryKeepsEveryOtherDefault(t *testing.T) {
	single := temporal.RetryPolicy{MaximumAttempts: 1}
	cases := []struct {
		name     string
		retry    string
		retrying int
		want     temporal.RetryPolicy
	}{
		{name: "one attempt is no retry", retry: "{maximum_attempts: 1}", retrying: 0},
		{name: "attempts and nothing else", retry: "{maximum_attempts: 4}", retrying: 3,
			want: temporal.RetryPolicy{MaximumAttempts: 4, InitialInterval: time.Second, BackoffCoefficient: 2.0, MaximumInterval: time.Minute}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := runRetrySpacing(t, tc.retry, retryPlan(60))
			retrying, singles := 0, 0
			for _, options := range capture.of(task.ActivityExecuteTask) {
				want := single
				if options.RetryPolicy != nil && options.RetryPolicy.MaximumAttempts > 1 {
					retrying++
					want = tc.want
				} else {
					singles++
				}
				if options.RetryPolicy == nil || !reflect.DeepEqual(*options.RetryPolicy, want) {
					t.Errorf("a task was scheduled with the retry policy %+v, want %+v", options.RetryPolicy, want)
				}
			}
			if retrying != tc.retrying || singles == 0 {
				t.Fatalf("%d retrying and %d single-attempt tasks, want %d retrying and the plain task single", retrying, singles, tc.retrying)
			}
		})
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

// retryChainDocument is three retrying tasks in a row, so a run that carries
// on at every idle point schedules each in a generation of its own.
const retryChainDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: retry-chain
tasks:
  - id: a
    uses: shell
    retry: {maximum_attempts: 3}
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    retry: {maximum_attempts: 3}
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    retry: {maximum_attempts: 3}
    with: {op: exec}
`

// TestRunWorkflow_ALaterGenerationAnswersFromTheCarriedRetryDecision: a
// generation that is handed the decision of an earlier one spaces its retries
// by it and never asks again, whichever build carries the run on. The carry
// handed to the second generation is the one a run recorded before the gate
// would hand (DefaultVersion: a recorded history is what produces that answer,
// TestRunWorkflow_ReplaysCommittedRetryMaxIntervalHistories), so the tasks it
// schedules wait at most 100 seconds though the plan carries 60 and a run that
// asked now would be answered 1.
func TestRunWorkflow_ALaterGenerationAnswersFromTheCarriedRetryDecision(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	capture := &dispatchCapture{}
	plan := retryPlan(60)
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
		MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget

	g := runGenerationsWithArgs(t, "run-carried-legacy-retry", func(*harness) []any {
		return []any{"run-carried-legacy-retry", compile(t, retryChainDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
	}, func(h *harness, _ int) {
		h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	}, func(generation int, _ *engine.RunPlan, carry *orkstackv1.RunContinuation) {
		if generation == 1 {
			carry.VersionDecisions[engine.RetryMaxIntervalChange] = int32(workflow.DefaultVersion)
		}
	})
	if g.err != nil {
		t.Fatalf("the run ended with %v", g.err)
	}
	if len(g.carries) < 2 {
		t.Fatalf("the run carried on %d times, want at least 2", len(g.carries))
	}
	got := maximumIntervals(capture)
	if len(got) != 3 {
		t.Fatalf("%d tasks retry, want 3 (got %v)", len(got), got)
	}
	// Task a was scheduled before the carry, under the new path; b and c
	// after it, under the carried legacy answer.
	if got[0] != time.Minute || got[1] != 100*time.Second || got[2] != 100*time.Second {
		t.Fatalf("maximum intervals %v, want [1m0s 1m40s 1m40s]", got)
	}
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 1 {
		t.Fatalf("%s was asked %v, want one question in the first generation and none after it", engine.RetryMaxIntervalChange, asked)
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
// asked for either: no recorded history can hold the refused command. What
// the cluster accepts is scheduled as it is: a zero or negative coefficient
// reads as the default 2.0, a negative initial interval as 1 s and a negative
// maximum as unset, as they always have, and +Inf is sent unchanged. A NaN
// coefficient, which the cluster accepts, reads as the default only on the
// retry-max-interval gate's new path, in a run whose plan carries the number:
// a run submitted without it, and a history recorded with the gate off, keep
// scheduling the NaN as every earlier build did. The gate is asked, once,
// only where a retry: sets no maximum or a NaN coefficient needs the new path.
func TestRunWorkflow_AVersionStoredBeforeTheRetryRuleIsScheduled(t *testing.T) {
	nan := math.NaN()
	for _, tc := range []struct {
		name        string
		retry       *orkstackv1.RetryPolicy
		planned     int32
		gateOff     bool
		initial     time.Duration
		maximum     time.Duration
		coefficient float64
		asked       []workflow.Version
	}{
		{name: "a maximum below the initial interval",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10},
			planned: 60, initial: 30 * time.Second, maximum: 30 * time.Second, coefficient: 2.0},
		{name: "a maximum above the initial interval",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 10, MaximumIntervalSeconds: 30},
			planned: 60, initial: 10 * time.Second, maximum: 30 * time.Second, coefficient: 2.0},
		{name: "a coefficient between 0 and 1",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.5, MaximumIntervalSeconds: 20},
			planned: 60, initial: time.Second, maximum: 20 * time.Second, coefficient: 1},
		{name: "a coefficient of 1",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
			planned: 60, initial: time.Second, maximum: time.Minute, coefficient: 1, asked: []workflow.Version{1}},
		{name: "a negative coefficient",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: -1},
			planned: 60, initial: time.Second, maximum: time.Minute, coefficient: 2, asked: []workflow.Version{1}},
		{name: "an infinite coefficient",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.Inf(1), MaximumIntervalSeconds: 20},
			planned: 60, initial: time.Second, maximum: 20 * time.Second, coefficient: math.Inf(1)},
		{name: "a negative initial interval",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: -5, MaximumIntervalSeconds: 20},
			planned: 60, initial: time.Second, maximum: 20 * time.Second, coefficient: 2},
		{name: "a negative maximum",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 5, MaximumIntervalSeconds: -1},
			planned: 60, initial: 5 * time.Second, maximum: time.Minute, coefficient: 2, asked: []workflow.Version{1}},
		{name: "a coefficient that is not a number",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: nan, MaximumIntervalSeconds: 20},
			planned: 60, initial: time.Second, maximum: 20 * time.Second, coefficient: 2, asked: []workflow.Version{1}},
		{name: "a coefficient that is not a number, in a plan without the number",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: nan, MaximumIntervalSeconds: 20},
			planned: 0, initial: time.Second, maximum: 20 * time.Second, coefficient: nan},
		{name: "a coefficient that is not a number, with the gate off",
			retry:   &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: nan, MaximumIntervalSeconds: 20},
			planned: 60, gateOff: true, initial: time.Second, maximum: 20 * time.Second, coefficient: nan},
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
				policy := options.RetryPolicy
				if policy.MaximumAttempts <= 1 {
					continue
				}
				if !sameCoefficient(policy.BackoffCoefficient, tc.coefficient) || policy.InitialInterval != tc.initial {
					t.Fatalf("scheduled with backoff coefficient %g and initial interval %s, want %g and %s",
						policy.BackoffCoefficient, policy.InitialInterval, tc.coefficient, tc.initial)
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
