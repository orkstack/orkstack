package engine

import (
	"math"
	"reflect"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestRetryPolicyFor_AnswersFromTheRunsOwnDecisions: a generation that
// carries the decision of an earlier one answers from it and never asks
// again (version), so a run's retries are spaced alike in every generation
// whichever build a later generation runs on. The context is never touched:
// a decision already made is returned before the SDK is consulted.
func TestRetryPolicyFor_AnswersFromTheRunsOwnDecisions(t *testing.T) {
	unset := &orkstackv1.Task{Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3}}
	slow := &orkstackv1.Task{Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 90}}
	cases := []struct {
		name     string
		decision workflow.Version
		seconds  int32
		task     *orkstackv1.Task
		want     time.Duration
	}{
		{name: "gated, the default", decision: 1, seconds: 60, task: unset, want: time.Minute},
		{name: "gated, no number", decision: 1, seconds: 0, task: unset, want: 100 * time.Second},
		{name: "gated, an initial interval above it", decision: 1, seconds: 60, task: slow, want: 90 * time.Second},
		{name: "legacy, a number is ignored", decision: workflow.DefaultVersion, seconds: 60, task: unset, want: 100 * time.Second},
		{name: "legacy, no number", decision: workflow.DefaultVersion, seconds: 0, task: unset, want: 100 * time.Second},
		{name: "legacy, an initial interval above it", decision: workflow.DefaultVersion, seconds: 0, task: &orkstackv1.Task{
			Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 150}}, want: 150 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &runState{
				plan:     RunPlan{RetryMaxIntervalSeconds: tc.seconds},
				versions: map[string]workflow.Version{RetryMaxIntervalChange: tc.decision},
			}
			if got := s.retryPolicyFor(nil, tc.task).MaximumInterval; got != tc.want {
				t.Errorf("MaximumInterval = %s, want %s", got, tc.want)
			}
			if got := s.versions[RetryMaxIntervalChange]; got != tc.decision {
				t.Errorf("the decision became %d, want it left at %d", got, tc.decision)
			}
		})
	}
}

// TestRetryPolicyFor_KeepsEveryOtherDefault: the gate moves the maximum
// interval alone; the single attempt of a task without retry:, the 1 s
// initial interval and the 2.0 coefficient are what they always were.
func TestRetryPolicyFor_KeepsEveryOtherDefault(t *testing.T) {
	s := &runState{plan: RunPlan{RetryMaxIntervalSeconds: 60}, versions: map[string]workflow.Version{RetryMaxIntervalChange: 1}}
	if got := s.retryPolicyFor(nil, &orkstackv1.Task{}); !reflect.DeepEqual(*got, temporal.RetryPolicy{MaximumAttempts: 1}) {
		t.Errorf("no retry: = %+v, want a single attempt", got)
	}
	if got := s.retryPolicyFor(nil, &orkstackv1.Task{Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 1}}); got.MaximumAttempts != 1 {
		t.Errorf("maximum_attempts 1 = %+v, want a single attempt", got)
	}
	got := s.retryPolicyFor(nil, &orkstackv1.Task{Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 4}})
	want := temporal.RetryPolicy{MaximumAttempts: 4, InitialInterval: time.Second, BackoffCoefficient: 2.0, MaximumInterval: time.Minute}
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("retry: {maximum_attempts: 4} = %+v, want %+v", *got, want)
	}
}

// TestLegacyRetryMaxInterval_IsFrozen freezes the number every build before
// the gate used. The DefaultVersion branch of a recorded history is only
// faithful while it stays exactly this; a release that changes the default
// (DefaultRetryMaxInterval) must not move it.
func TestLegacyRetryMaxInterval_IsFrozen(t *testing.T) {
	if legacyRetryMaxInterval != 100*time.Second {
		t.Fatalf("legacyRetryMaxInterval = %s: it is frozen at the 100 seconds recorded histories were built with",
			legacyRetryMaxInterval)
	}
	if DefaultRetryMaxInterval != time.Minute {
		t.Errorf("DefaultRetryMaxInterval = %s, want AGENTS.md §4's suggested maximum interval of 1m0s", DefaultRetryMaxInterval)
	}
}

// TestPinCreationPriorityFor_AnswersFromTheRunsOwnDecisions is
// TestRetryPolicyFor_AnswersFromTheRunsOwnDecisions for a pin's creation
// request.
func TestPinCreationPriorityFor_AnswersFromTheRunsOwnDecisions(t *testing.T) {
	selection := &FairnessSelection{Key: "payments", Weight: 3, PinRequests: true}
	before := &FairnessSelection{Key: "payments", Weight: 3}
	cases := []struct {
		name      string
		decision  workflow.Version
		selection *FairnessSelection
		want      temporal.Priority
	}{
		{name: "gated", decision: 1, selection: selection, want: temporal.Priority{FairnessKey: "payments", FairnessWeight: 3}},
		{name: "legacy", decision: workflow.DefaultVersion, selection: selection, want: temporal.Priority{}},
		{name: "no selection", decision: 1, selection: nil, want: temporal.Priority{}},
		{name: "a selection from before pin_requests", decision: 1, selection: before, want: temporal.Priority{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &runState{
				plan:     RunPlan{Fairness: tc.selection},
				versions: map[string]workflow.Version{FairnessPinCreationChange: tc.decision},
			}
			if got := s.pinCreationPriorityFor(nil); got != tc.want {
				t.Errorf("pinCreationPriorityFor = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestChildStartPlan_CarriesEveryRunTreeValue: every run-tree value is set
// once, on the top-level plan, and a child run starts with its parent's —
// the retry spacing and the fairness among them — whatever the nested plan a
// submit built holds, and without the nested plan being changed.
func TestChildStartPlan_CarriesEveryRunTreeValue(t *testing.T) {
	budget := DefaultContinuationBudget(500, 1<<20, time.Hour)
	overlay := DefaultOverlayPolicy()
	parent := &runState{runID: "ork-parent", plan: RunPlan{
		History:                   &HistoryLimit{MaxEvents: 1000, MaxBytes: 1 << 20, Reserve: DefaultHistoryReserve},
		Continuation:              &budget,
		InFlight:                  &InFlightLimit{Max: 30, HardStop: 1900},
		SnapshotOverlay:           &overlay,
		PinCreationTimeoutSeconds: 45,
		Fairness:                  &FairnessSelection{Key: "payments", Weight: 3},
		RetryMaxIntervalSeconds:   60,
	}}
	planned := &ChildRunPlan{Plan: RunPlan{Namespace: "default"}}
	child := parent.childStartPlan(planned)
	if child.History != parent.plan.History || child.Continuation != parent.plan.Continuation ||
		child.InFlight != parent.plan.InFlight || child.SnapshotOverlay != parent.plan.SnapshotOverlay ||
		child.Fairness != parent.plan.Fairness {
		t.Errorf("a child's start plan does not carry its parent's run-tree limits: %+v", child)
	}
	if child.PinCreationTimeoutSeconds != 45 || child.RetryMaxIntervalSeconds != 60 {
		t.Errorf("child plan pin creation %d s, retry maximum %d s; want the parent's 45 and 60",
			child.PinCreationTimeoutSeconds, child.RetryMaxIntervalSeconds)
	}
	if planned.Plan.RetryMaxIntervalSeconds != 0 || planned.Plan.Fairness != nil {
		t.Errorf("the planned child plan was mutated: %+v", planned.Plan)
	}

	// A plan built before the fields has none, and its children start with
	// none: byte-identical to what that build sent.
	old := (&runState{runID: "ork-old"}).childStartPlan(planned)
	if old.RetryMaxIntervalSeconds != 0 || old.Fairness != nil {
		t.Errorf("a child of a plan without the fields starts with %+v", old)
	}
}

// TestRetryPolicyFor_RaisesOnlyWhatTheClusterRefuses: for a version stored
// before docs/DSL.md rule 47, the coefficient is raised to 1 only where the
// cluster refuses it (between 0 and 1) and an explicit maximum only where it
// is below the initial interval. A value the cluster accepts — and so a
// recorded run may have used — is scheduled as it always was: zero and a
// negative coefficient read as the default 2.0, and a NaN passes through.
func TestRetryPolicyFor_RaisesOnlyWhatTheClusterRefuses(t *testing.T) {
	s := &runState{plan: RunPlan{RetryMaxIntervalSeconds: 60}, versions: map[string]workflow.Version{RetryMaxIntervalChange: 1}}
	for _, tc := range []struct {
		name        string
		retry       *orkstackv1.RetryPolicy
		coefficient float64
		maximum     time.Duration
	}{
		{"a coefficient between 0 and 1", &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.5}, 1, time.Minute},
		{"a negative coefficient", &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: -1}, 2, time.Minute},
		{"a coefficient of 1", &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1}, 1, time.Minute},
		{"a maximum below the initial", &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10}, 2, 30 * time.Second},
		{"a maximum above the initial", &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 10, MaximumIntervalSeconds: 30}, 2, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := s.retryPolicyFor(nil, &orkstackv1.Task{Retry: tc.retry})
			if got.BackoffCoefficient != tc.coefficient || got.MaximumInterval != tc.maximum {
				t.Errorf("coefficient %g and maximum %s, want %g and %s", got.BackoffCoefficient, got.MaximumInterval, tc.coefficient, tc.maximum)
			}
		})
	}
	if got := s.retryPolicyFor(nil, &orkstackv1.Task{Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN()}}); !math.IsNaN(got.BackoffCoefficient) {
		t.Errorf("a NaN coefficient became %g, want it scheduled as it always was", got.BackoffCoefficient)
	}
}
