package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// defaultPlan is the routing a submit binds for an orkflow whose tasks name
// only the pools listed (plus the built-in default, which every task that
// names none resolves to). It is what PoolResolver.Plan produces for the
// default namespace with no operator override, written out so a workflow test
// states its own routing instead of depending on the resolver.
func defaultPlan(pools ...string) engine.RunPlan {
	return rawPlan(task.DefaultQueue, pools...)
}

// rawPlan is defaultPlan with the default route pinned to an operator's own
// channel, as ORK_ORKER_QUEUE does.
//
// It routes through task.RouteForPool, the same single resolution the resolver
// uses, so a hand-written test plan cannot state a route the deployment would
// never produce — in particular, it collapses `main` onto the bound channel's
// own pool name rather than listing one set of Orkers twice.
func rawPlan(defaultQueue string, pools ...string) engine.RunPlan {
	routes := map[string]string{}
	for _, pool := range append([]string{task.DefaultPool}, pools...) {
		route, err := task.RouteForPool(pool, defaultQueue)
		if err != nil {
			panic("test plan names an unroutable pool " + pool + ": " + err.Error())
		}
		routes[route.Pool] = route.Queue
	}
	return engine.RunPlan{Namespace: "default", Routes: routes}
}

// planFor resolves a compiled document exactly as SubmitRun does: through
// PoolResolver.Plan, against a deployment whose bound default channel is
// rawQueue. It returns the definition the workflow is started with — every
// task stamped with the pool it resolved to — and the run's route table.
//
// A workflow test that wants the real submit decision uses this; one that
// wants to state a routing of its own uses defaultPlan/rawPlan.
func planFor(t *testing.T, wf *orkstackv1.Orkflow, rawQueue string) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	resolver := &engine.PoolResolver{RawQueue: rawQueue}
	ns := &orkstackv1.Namespace{Name: wf.GetNamespace()}
	planned, plan, err := resolver.Plan(context.Background(), ns, wf, false)
	if err != nil {
		t.Fatalf("Plan(%s, bound channel %q): %v", wf.GetName(), rawQueue, err)
	}
	return planned, plan
}

// TestRunPlan_RefusesAPoolItHasNoRouteFor is the workflow-side half of the
// routing contract: a plan answers only for the pools it was built for, and
// says so rather than guessing.
//
// Guessing is the failure this prevents. A plan that fell back to the default
// channel would run a gpu task on the shared pool — the wrong machines,
// silently, with a green run to show for it.
func TestRunPlan_RefusesAPoolItHasNoRouteFor(t *testing.T) {
	plan := defaultPlan("gpu")

	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool: %v", err)
	}
	if route, err := plan.Route("gpu"); err != nil || route != gpuQueue {
		t.Fatalf("Route(gpu) = %q, %v; want %q", route, err, gpuQueue)
	}
	if route, err := plan.Route(task.DefaultPool); err != nil || route != task.DefaultQueue {
		t.Fatalf("Route(main) = %q, %v; want %q", route, err, task.DefaultQueue)
	}

	route, err := plan.Route("batch")
	if err == nil {
		t.Fatalf("Route(batch) = %q with no error; an unplanned pool must not resolve", route)
	}
	// The error names what was asked for and what the plan holds, because the
	// only way to reach it is a plan built for a different definition.
	for _, want := range []string{"batch", "gpu", task.DefaultPool} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// TestRunPlan_TelemetryOmittedWhenNil pins the golden-history invariant
// straight to the wire bytes: a plan with no telemetry selection — every plan
// before this field existed, and every plan today for a namespace with no
// policy and an orkflow with no labels — must not grow a "telemetry" key at
// all. `omitempty` alone does not prove that; only the marshaled bytes do.
func TestRunPlan_TelemetryOmittedWhenNil(t *testing.T) {
	plan := defaultPlan()
	plan.Telemetry = nil

	out, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(out), "telemetry") {
		t.Fatalf("encoded plan names \"telemetry\" with a nil selection: %s", out)
	}
}

// TestRunPlan_TelemetryRoundTrips is the companion case: a plan that DOES
// carry a selection encodes it and reads it back unchanged, including a
// non-default TaskLogs enum name and a populated attribute map.
func TestRunPlan_TelemetryRoundTrips(t *testing.T) {
	plan := defaultPlan()
	plan.Telemetry = &engine.TelemetrySelection{
		Sink:       "otel-prod",
		TaskLogs:   "TASK_LOG_EXPORT_ENABLED",
		Attributes: map[string]string{"ork.ns.team": "payments", "ork.label.tier": "gold"},
	}

	out, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(out), `"telemetry"`) {
		t.Fatalf("encoded plan does not name \"telemetry\" with a selection set: %s", out)
	}

	var back engine.RunPlan
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.Telemetry == nil {
		t.Fatal("round trip lost the telemetry selection entirely")
	}
	if got, want := *back.Telemetry, *plan.Telemetry; got.Sink != want.Sink ||
		got.TaskLogs != want.TaskLogs || len(got.Attributes) != len(want.Attributes) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestTelemetrySelection_ProtoIsNilSafe is the reason ExecuteTaskInput's and
// Run's telemetry fields can be set unconditionally in workflow.go: a nil
// *TelemetrySelection (the common case) renders as a nil proto message rather
// than panicking or producing a zero-value message that would show up as
// "telemetry": {} on the wire.
func TestTelemetrySelection_ProtoIsNilSafe(t *testing.T) {
	var nilSelection *engine.TelemetrySelection
	if got := nilSelection.Proto(); got != nil {
		t.Fatalf("nil selection Proto() = %v, want nil", got)
	}

	set := &engine.TelemetrySelection{Sink: "otel-prod", TaskLogs: "TASK_LOG_EXPORT_DISABLED"}
	got := set.Proto()
	if got.GetSink() != "otel-prod" {
		t.Errorf("Proto().Sink = %q, want %q", got.GetSink(), "otel-prod")
	}
	if got.GetTaskLogs() != orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED {
		t.Errorf("Proto().TaskLogs = %v, want DISABLED", got.GetTaskLogs())
	}
}
