package engine_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
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
