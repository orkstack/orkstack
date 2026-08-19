package engine_test

import (
	"context"
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
