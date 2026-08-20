package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A white-box test, because the property spans the two halves of one decision
// and one half is unexported. What it holds is entirely user-facing: the pool a
// client is told a task ran on must name the Orkers the task was dispatched to,
// and the two ways of spelling the default pool must be one route.
//
// The defect it closes: `pool: main` mapped through task.QueueForPool to
// task.DefaultQueue while a task that named no pool went to the deployment's
// own channel. Under the ORK_ORKER_QUEUE isolation AGENTS.md §6 gives every
// parallel agent, that made `pool: main` an escape hatch onto the shared
// queue — from a document whose author was told the two spellings mean the
// same thing (docs/DSL.md). The same arithmetic, run the other way, reported
// one set of Orkers as two pools: an engine started with ORK_ORKER_POOL=gpu
// answered `main` and `gpu` with the same machines, and told a client a task
// ran on `main` while gpu's Orkers ran it.
//
// The decision is made at submit now, in PoolResolver.Plan, so that is where
// this is asserted; the workflow's half is that it reports what the plan
// recorded, which is the second half of the file.

// defaultChannels is the space the route decision is a function of: the
// execution channel this deployment bound as its default, which
// TemporalStarter.StartRun resolves at submit and binds into the run's input.
//
// wantPool is the name that channel answers to. A raw isolation queue is
// nobody's pool, so it reports the built-in default — the pool a document that
// named none asked for. A channel an operator pointed at a pool's own queue
// reports THAT pool: it is the deployment's default pool, and `main` is a name
// for the deployment's default pool, not for one fixed queue.
var defaultChannels = []struct {
	name     string
	bound    string
	wantPool string
}{
	// The zero-configuration stack: `make run-ork` + `make run-orker`.
	{name: "the unconfigured default channel", bound: task.DefaultQueue, wantPool: task.DefaultPool},
	// StartRun always binds a channel; a zero value must still resolve.
	{name: "nothing bound at all", bound: "", wantPool: task.DefaultPool},
	// ORK_ORKER_QUEUE for one parallel agent (AGENTS.md §6).
	{name: "a plain operator override", bound: "ork-tasks-m7-9", wantPool: task.DefaultPool},
	// ORK_ORKER_POOL=gpu on BOTH binaries, which docs/DEPLOY.md documents as
	// the preferred way to run a dedicated pool: the engine's bound channel is
	// then that pool's own queue.
	{name: "an override that is itself pool-shaped", bound: "ork-pool-gpu", wantPool: "gpu"},
}

// planPools resolves an orkflow whose tasks name the given pools, exactly as a
// submit does, against a deployment whose bound default channel is `bound`.
// It returns the pool stamped on each task and the run's route table.
func planPools(t *testing.T, bound string, pools ...string) ([]string, RunPlan) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "route", Version: "v1"}
	for i, pool := range pools {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Pool: pool})
	}
	resolver := &PoolResolver{RawQueue: bound}
	planned, plan, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "default"}, wf, false)
	if err != nil {
		t.Fatalf("Plan(bound %q, pools %v): %v", bound, pools, err)
	}
	stamped := make([]string, 0, len(planned.GetTasks()))
	for _, task := range planned.GetTasks() {
		stamped = append(stamped, task.GetPool())
	}
	return stamped, plan
}

// TestPoolResolver_TheDefaultPoolIsTheDeploymentsDefaultChannel: `pool: main`
// and an omitted pool are the SAME route, in BOTH projections, under every
// configuration — by construction, not by coincidence.
//
// `main` names the deployment's default pool. Whatever channel an operator
// bound as the default, that channel is what `main` resolves to; it is not a
// synonym for task.DefaultQueue, which is only what the default pool resolves
// to when nothing was configured.
func TestPoolResolver_TheDefaultPoolIsTheDeploymentsDefaultChannel(t *testing.T) {
	for _, channel := range defaultChannels {
		t.Run(channel.name, func(t *testing.T) {
			stamped, plan := planPools(t, channel.bound, "", task.DefaultPool)

			if stamped[0] != stamped[1] {
				t.Errorf("no pool is reported on %q and `pool: %s` on %q under the bound channel "+
					"%q: the same Orkers under two names",
					stamped[0], task.DefaultPool, stamped[1], channel.bound)
			}
			if stamped[1] != channel.wantPool {
				t.Errorf("`pool: %s` is reported on pool %q, want %q — the name the bound "+
					"channel %q answers to", task.DefaultPool, stamped[1], channel.wantPool, channel.bound)
			}
			// One route, because it is one set of Orkers. A second entry here
			// is the report that counts the same machines twice.
			if len(plan.Routes) != 1 {
				t.Fatalf("routes = %v; the two spellings of the default pool are one route", plan.Routes)
			}

			wantQueue := channel.bound
			if wantQueue == "" {
				wantQueue = task.DefaultQueue
			}
			queue, err := plan.Route(channel.wantPool)
			if err != nil {
				t.Fatalf("Route(%q): %v", channel.wantPool, err)
			}
			if queue != wantQueue {
				t.Errorf("pool %q dispatches to %q, want the bound channel %q",
					channel.wantPool, queue, wantQueue)
			}
		})
	}
}

// TestPoolResolver_OnePoolNameMeansOneSetOfOrkers is the other half of the same
// invariant: within one run, the reported pool and the dispatch queue determine
// each other. Two tasks reported on the same pool go to the same queue; two
// tasks on different queues are reported on different pools.
func TestPoolResolver_OnePoolNameMeansOneSetOfOrkers(t *testing.T) {
	for _, channel := range defaultChannels {
		t.Run(channel.name, func(t *testing.T) {
			stamped, plan := planPools(t, channel.bound, "", task.DefaultPool, "gpu", "etl")

			poolOf := map[string]string{} // queue -> pool
			for pool, queue := range plan.Routes {
				if seen, ok := poolOf[queue]; ok {
					t.Errorf("pools %q and %q both dispatch to %q: one set of Orkers, two names",
						seen, pool, queue)
				}
				poolOf[queue] = pool
			}
			for i, pool := range stamped {
				if _, ok := plan.Routes[pool]; !ok {
					t.Errorf("task t%d is reported on pool %q, which this run's plan has no "+
						"route for (%v)", i, pool, plan.Routes)
				}
			}
		})
	}
}

// TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute is the
// isolation half, stated on its own because it is the one that bites: an agent
// running under ORK_ORKER_QUEUE (AGENTS.md §6) must not be able to reach the
// shared `ork-tasks` by writing `pool: main`, however the override is spelled.
func TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute(t *testing.T) {
	for _, channel := range defaultChannels {
		if channel.bound == task.DefaultQueue || channel.bound == "" {
			continue // no isolation was asked for
		}
		t.Run(channel.name, func(t *testing.T) {
			stamped, plan := planPools(t, channel.bound, task.DefaultPool)
			queue, err := plan.Route(stamped[0])
			if err != nil {
				t.Fatalf("Route(%q): %v", stamped[0], err)
			}
			if queue != channel.bound {
				t.Errorf("`pool: %s` dispatched to %q under a bound channel of %q; a document must "+
					"not be able to leave the operator's route", task.DefaultPool, queue, channel.bound)
			}
			if queue == task.DefaultQueue {
				t.Errorf("`pool: %s` reached the shared queue %q, which is what the isolation "+
					"exists to prevent", task.DefaultPool, task.DefaultQueue)
			}
		})
	}
}

// TestPoolResolver_AnUnroutablePoolFailsTheSubmit keeps the other half of the
// contract: a pool nothing can route to is an error at submit, never a fallback
// onto whatever queue happens to be at hand. It is refused before a run id
// exists, so there is nothing to strand.
func TestPoolResolver_AnUnroutablePoolFailsTheSubmit(t *testing.T) {
	resolver := &PoolResolver{}
	for _, pool := range []string{"GPU", "gpu.1", "1gpu", "ork pool", strings.Repeat("g", task.MaxPoolName+1)} {
		wf := &orkstackv1.Orkflow{
			Name:  "route",
			Tasks: []*orkstackv1.Task{{Id: "crunch", Pool: pool}},
		}
		_, plan, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "default"}, wf, false)
		if err == nil {
			t.Errorf("Plan(pool: %.32q) = %v, want the submit to fail on an unroutable pool", pool, plan.Routes)
			continue
		}
		if !errors.Is(err, task.ErrInvalidPool) {
			t.Errorf("Plan(pool: %.32q) failed with %v, want task.ErrInvalidPool", pool, err)
		}
	}
}

// TestRunState_ReportsThePoolTheWorkWasDispatchedTo is the workflow's half: it
// reports what the plan recorded, and both projections come out of the one
// lookup. A run whose plan says gpu is reported on gpu, whatever the process
// finishing it was configured with — the property that lets a second replica
// finish a run this one started.
func TestRunState_ReportsThePoolTheWorkWasDispatchedTo(t *testing.T) {
	for _, channel := range defaultChannels {
		t.Run(channel.name, func(t *testing.T) {
			stamped, plan := planPools(t, channel.bound, "", task.DefaultPool, "gpu")
			// The workflow is deliberately given no configuration of its own:
			// everything it needs is the bound plan.
			s := &runState{plan: plan}

			for i, pool := range stamped {
				tk := &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Pool: pool}
				queue, err := s.queueForTask(tk)
				if err != nil {
					t.Fatalf("queueForTask(t%d): %v", i, err)
				}
				if got := s.poolForTask(tk); got != pool {
					t.Errorf("task t%d was planned on pool %q and is reported on %q", i, pool, got)
				}
				if want := plan.Routes[pool]; queue != want {
					t.Errorf("task t%d on pool %q dispatched to %q, want %q", i, pool, queue, want)
				}
			}
		})
	}
}

// TestRunState_APoolWithNoRouteFailsTheRun: the workflow never invents a
// channel. A definition and a plan that disagree cannot be reached through
// SubmitRun — the plan is built from that very definition — so this is what a
// hand-started RunWorkflow gets, and it says so instead of running the work on
// whatever queue is at hand.
func TestRunState_APoolWithNoRouteFailsTheRun(t *testing.T) {
	s := &runState{plan: RunPlan{Namespace: "default", Routes: map[string]string{task.DefaultPool: task.DefaultQueue}}}
	if queue, err := s.queueForTask(&orkstackv1.Task{Id: "crunch", Pool: "gpu"}); err == nil {
		t.Errorf("queueForTask(pool: gpu) = %q, want the run to fail on an unplanned pool", queue)
	}

	// And a plan that routes onto the engine's own queue is refused, whatever
	// wrote it: an Orker polling ork-engine-events would take the engine's
	// projection work off the queue the engine is waiting on.
	reserved := &runState{plan: RunPlan{Namespace: "default", Routes: map[string]string{
		task.DefaultPool: task.EngineEventQueue,
	}}}
	_, err := reserved.queueForTask(&orkstackv1.Task{Id: "crunch"})
	if err == nil {
		t.Fatalf("queueForTask on a plan routed to %q succeeded; it must be refused", task.EngineEventQueue)
	}
	if !strings.Contains(err.Error(), task.EngineEventQueue) {
		t.Errorf("error %q does not name the reserved queue %q", err, task.EngineEventQueue)
	}
}
