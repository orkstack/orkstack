package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

var admissionSince = time.Date(2026, 9, 22, 2, 14, 7, 0, time.UTC)

func TestHoldRefusal_ReadsOnlyTheDocument(t *testing.T) {
	if got := engine.HoldRefusal(&orkstackv1.Namespace{Name: "payments"}); got != nil {
		t.Fatalf("HoldRefusal(accepting) = %v, want nil", got)
	}
	if got := engine.HoldRefusal(nil); got != nil {
		t.Fatalf("HoldRefusal(nil) = %v, want nil", got)
	}
	hold := &orkstackv1.NamespaceHold{
		Reason: "partner CI loop, INC-4412",
		Since:  timestamppb.New(admissionSince),
	}
	got := engine.HoldRefusal(&orkstackv1.Namespace{Name: "payments", Hold: hold, MaxRunning: 1})
	if got == nil {
		t.Fatal("HoldRefusal(held) = nil, want a refusal")
	}
	if got.Reason != engine.AdmissionHeld || got.Namespace != "payments" {
		t.Errorf("refusal = %+v, want NAMESPACE_HELD for payments", got)
	}
	if !proto.Equal(got.Hold, hold) {
		t.Errorf("refusal.Hold = %v, want %v", got.Hold, hold)
	}
	const want = "namespace \"payments\" is on hold since 2026-09-22T02:14:07Z: \"partner CI loop, INC-4412\". " +
		"Release it with `orkctl namespace update payments --release-hold`"
	if got.Error() != want {
		t.Errorf("Error() =\n%s\nwant\n%s", got.Error(), want)
	}
	// The default namespace is named as such even when the stored document
	// left its name empty.
	if d := engine.HoldRefusal(&orkstackv1.Namespace{Hold: hold}); d == nil || d.Namespace != "default" {
		t.Errorf("HoldRefusal(unnamed) = %+v, want namespace default", d)
	}
}

func TestMaxRunningRefusal_ZeroIsUnlimitedAndTheCapIsExclusive(t *testing.T) {
	cases := []struct {
		max     int32
		running int64
		refused bool
	}{
		{0, 1_000_000, false},
		{20, 19, false},
		{20, 20, true},
		{20, 21, true},
	}
	for _, tc := range cases {
		got := engine.MaxRunningRefusal(&orkstackv1.Namespace{Name: "payments", MaxRunning: tc.max}, tc.running)
		if !tc.refused {
			if got != nil {
				t.Errorf("MaxRunningRefusal(%d, %d) = %v, want nil", tc.max, tc.running, got)
			}
			continue
		}
		if got == nil {
			t.Fatalf("MaxRunningRefusal(%d, %d) = nil, want a refusal", tc.max, tc.running)
		}
		if got.Reason != engine.AdmissionMaxRunning || got.Running != tc.running || got.MaxRunning != tc.max {
			t.Errorf("refusal = %+v, want MAX_RUNNING_REACHED running=%d max=%d", got, tc.running, tc.max)
		}
	}
	got := engine.MaxRunningRefusal(&orkstackv1.Namespace{Name: "payments", MaxRunning: 20}, 20)
	const want = "namespace \"payments\" already has 20 runs in flight, which is its max_running. " +
		"Wait for one to finish, or raise the cap with `orkctl namespace update payments --max-running N`"
	if got.Error() != want {
		t.Errorf("Error() =\n%s\nwant\n%s", got.Error(), want)
	}
}

// TestRefusalTelemetry_IsTheSelectionTheRunWouldHaveCarried pins that a
// refusal and a run are stamped by one function.
func TestRefusalTelemetry_IsTheSelectionTheRunWouldHaveCarried(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name: "payments",
		Telemetry: &orkstackv1.TelemetryPolicy{
			Sink:       "b",
			Attributes: map[string]string{"team": "payments"},
		},
	}
	wf := compile(t, `
name: reconcile
labels:
  Tier: gold
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`)
	got := engine.RefusalTelemetry(ns, wf)
	if got == nil {
		t.Fatal("RefusalTelemetry = nil, want the selection")
	}
	if got.GetSink() != "b" {
		t.Errorf("sink = %q, want b", got.GetSink())
	}
	want := map[string]string{"ork.ns.team": "payments", "ork.label.tier": "gold"}
	if len(got.GetAttributes()) != len(want) {
		t.Fatalf("attributes = %v, want exactly %v", got.GetAttributes(), want)
	}
	for key, value := range want {
		if got.GetAttributes()[key] != value {
			t.Errorf("attributes[%q] = %q, want %q", key, got.GetAttributes()[key], value)
		}
	}
	for _, bare := range []string{"team", "Tier", "tier"} {
		if _, ok := got.GetAttributes()[bare]; ok {
			t.Errorf("attributes carry the bare key %q", bare)
		}
	}
	_, plan, err := (&engine.PoolResolver{Probe: everyPoolLive{}}).Plan(context.Background(), ns, wf, false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !proto.Equal(got, plan.Telemetry.Proto()) {
		t.Errorf("RefusalTelemetry = %v, Plan's = %v; a refusal must be stamped as the run would have been",
			got, plan.Telemetry.Proto())
	}

	if got := engine.RefusalTelemetry(&orkstackv1.Namespace{}, compile(t, telemetryDocument)); got != nil {
		t.Errorf("RefusalTelemetry(no policy, no labels) = %v, want nil", got)
	}
}

func enforcedBudget(cpu, gib int64) *orkstackv1.Namespace {
	return &orkstackv1.Namespace{Name: "analytics", Budget: &orkstackv1.Budget{
		CpuSeconds: cpu, GibSeconds: gib, Period: durationpb.New(720 * time.Hour), Enforce: true,
	}}
}

var budgetMeasured = time.Date(2026, 9, 24, 10, 49, 2, 0, time.UTC)

func TestBudgetReached_AtTheBoundIsReachedAndZeroIsUnbounded(t *testing.T) {
	cpuOnly := &orkstackv1.Budget{CpuSeconds: 5000}
	cases := []struct {
		name   string
		budget *orkstackv1.Budget
		usage  engine.BudgetUsage
		want   bool
	}{
		{"just under", cpuOnly, engine.BudgetUsage{CPUSeconds: 4999.9}, false},
		{"at the bound", cpuOnly, engine.BudgetUsage{CPUSeconds: 5000}, true},
		{"over", cpuOnly, engine.BudgetUsage{CPUSeconds: 9000}, true},
		{"GiB unbounded at 0", cpuOnly, engine.BudgetUsage{GiBSeconds: 1e12}, false},
		{"GiB bound reached", &orkstackv1.Budget{GibSeconds: 10}, engine.BudgetUsage{GiBSeconds: 10}, true},
		{"nil budget", nil, engine.BudgetUsage{CPUSeconds: 1e12, GiBSeconds: 1e12}, false},
	}
	for _, tc := range cases {
		if got := engine.BudgetReached(tc.budget, tc.usage); got != tc.want {
			t.Errorf("%s: BudgetReached = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestBudgetRefusal_OnlyAnEnforcedReachedBudgetRefuses(t *testing.T) {
	over := engine.BudgetUsage{CPUSeconds: 8205762, GiBSeconds: 16411524, MeasuredAt: budgetMeasured}
	soft := enforcedBudget(5000000, 0)
	soft.Budget.Enforce = false
	if got := engine.BudgetRefusal(soft, over); got != nil {
		t.Fatalf("a soft, reached budget refused: %v", got)
	}
	if got := engine.BudgetRefusal(enforcedBudget(9000000, 0), over); got != nil {
		t.Fatalf("an enforced budget not reached refused: %v", got)
	}
	if got := engine.BudgetRefusal(&orkstackv1.Namespace{Name: "analytics"}, over); got != nil {
		t.Fatalf("a namespace without a budget refused: %v", got)
	}
	cpuSentence := "namespace \"analytics\" has reserved 8205762 cpu-seconds over the last 720h0m0s, at or above its " +
		"enforced budget of 5000000 (measured 2026-09-24T10:49:02Z). New runs are refused until the period's usage " +
		"falls below the budget; raise it with `orkctl namespace update analytics --budget-cpu-seconds N`, or stop " +
		"enforcing with `orkctl namespace update analytics --budget-enforce=false`"
	gibSentence := "namespace \"analytics\" has reserved 16411524 GiB-seconds over the last 720h0m0s, at or above its " +
		"enforced budget of 10000000 (measured 2026-09-24T10:49:02Z). New runs are refused until the period's usage " +
		"falls below the budget; raise it with `orkctl namespace update analytics --budget-gib-seconds N`, or stop " +
		"enforcing with `orkctl namespace update analytics --budget-enforce=false`"
	cases := []struct {
		name string
		ns   *orkstackv1.Namespace
		want string
	}{
		{"cpu reached", enforcedBudget(5000000, 0), cpuSentence},
		{"GiB reached only", enforcedBudget(9000000, 10000000), gibSentence},
		{"both reached names cpu", enforcedBudget(5000000, 10000000), cpuSentence},
	}
	for _, tc := range cases {
		refusal := engine.BudgetRefusal(tc.ns, over)
		if refusal == nil {
			t.Fatalf("%s: no refusal", tc.name)
		}
		if refusal.Reason != engine.AdmissionBudgetExceeded || refusal.Budget == nil || refusal.Usage != over {
			t.Errorf("%s: refusal = %+v", tc.name, refusal)
		}
		if refusal.Error() != tc.want {
			t.Errorf("%s: sentence =\n  %s\nwant\n  %s", tc.name, refusal.Error(), tc.want)
		}
	}
}

func TestBudgetUnmeasurableRefusal_NamesTheCauseAndBothFixes(t *testing.T) {
	cause := errors.New(`usage of namespace "analytics" over 720h0m0s would read 12004 finished runs, more than the 10000 a usage read is bounded to`)
	refusal := engine.BudgetUnmeasurableRefusal(enforcedBudget(5000000, 0), cause)
	want := "namespace \"analytics\" enforces a budget over 720h0m0s and its usage could not be measured: usage of " +
		"namespace \"analytics\" over 720h0m0s would read 12004 finished runs, more than the 10000 a usage read is " +
		"bounded to. New runs are refused while an enforced budget cannot be measured; shorten the period with " +
		"`orkctl namespace update analytics --budget-period D`, or stop enforcing with `orkctl namespace update " +
		"analytics --budget-enforce=false`"
	if refusal.Reason != engine.AdmissionBudgetUnmeasurable || refusal.Unmeasured != cause || refusal.Budget == nil {
		t.Fatalf("refusal = %+v", refusal)
	}
	if refusal.Error() != want {
		t.Fatalf("sentence =\n  %s\nwant\n  %s", refusal.Error(), want)
	}
}
