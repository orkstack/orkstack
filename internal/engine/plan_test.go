package engine_test

import (
	"strings"
	"testing"

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
func rawPlan(defaultQueue string, pools ...string) engine.RunPlan {
	routes := map[string]string{task.DefaultPool: defaultQueue}
	for _, pool := range pools {
		queue, err := task.QueueForPool(pool)
		if err != nil {
			panic("test plan names an unmappable pool " + pool + ": " + err.Error())
		}
		routes[pool] = queue
	}
	return engine.RunPlan{Namespace: "default", Routes: routes}
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

	gpuQueue, err := task.QueueForPool("gpu")
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
