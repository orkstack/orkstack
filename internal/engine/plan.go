package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/internal/task"
)

// RunPlan is a run's routing, decided at submit and bound into the workflow
// input.
//
// It exists because the two halves of the decision cannot live in the same
// place. Which pool a task runs on is the user's document plus the namespace's
// default — configuration, read from a store, at submit. Which channel that
// pool is, is process configuration, which workflow code may not read at all
// (AGENTS.md §1.3). So the decision is made once, by the process that accepts
// the submission, and travels as an argument: recorded in history, identical
// on every replay, and readable by a second engine finishing a run this one
// started.
//
// It is a plain Go type on purpose. The data converter encodes a top-level
// protobuf message with protojson and everything else with encoding/json;
// nesting the orkflow inside a struct would break the former, so the plan
// travels beside the messages rather than inside one.
type RunPlan struct {
	// Namespace is the namespace the run belongs to.
	Namespace string `json:"namespace"`

	// Routes maps every pool this run uses to the Temporal task queue that
	// carries it. Exactly the pools the run uses: a route table is this run's
	// routing record, not a snapshot of the deployment.
	Routes map[string]string `json:"routes"`
}

// Route answers where one pool's tasks go.
//
// A pool with no route is an error, never a fallback. Falling back would run a
// gpu task on the shared pool — the wrong machines, silently, which is worse
// than not running it. The error names the pool and the routes that do exist,
// because the only way this happens is a plan built for a different definition.
//
// A route onto one of the engine's own queues is refused for the same reason
// task.QueueForPool refuses it: ork-engine-events carries the engine's
// projection activities, and an Orker polling it would take engine work off
// the queue the engine is waiting on. The resolver cannot produce such a plan
// (task.RouteForPool refuses first) and `ork` will not boot on that channel;
// the check is here because this is the last place the plan is read, and a
// plan is data a harness can hand-build.
func (p RunPlan) Route(pool string) (string, error) {
	queue := p.Routes[pool]
	if queue == "" {
		return "", fmt.Errorf("pool %q has no route in this run's plan (planned pools: %s)",
			pool, p.pools())
	}
	if queue == task.EngineEventQueue {
		return "", fmt.Errorf("%w: this run's plan routes pool %q to %q, which carries "+
			"the engine's own projection activities", task.ErrReservedQueue, pool, queue)
	}
	return queue, nil
}

func (p RunPlan) pools() string {
	if len(p.Routes) == 0 {
		return "none"
	}
	names := make([]string, 0, len(p.Routes))
	for pool := range p.Routes {
		names = append(names, pool)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
