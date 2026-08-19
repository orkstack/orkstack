package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A white-box test, because the property is about the two halves of one
// decision and both are unexported. What it holds is entirely user-facing: the
// pool a client is told a task ran on must name the Orkers the task was
// dispatched to, and the two ways of spelling the default pool must be one
// route.
//
// The defect it closes: `pool: main` mapped through task.QueueForPool to
// task.DefaultQueue while a task that named no pool went to the run's own
// orkerQueue. Under the ORK_ORKER_QUEUE isolation AGENTS.md §6 gives every
// parallel agent, that made `pool: main` an escape hatch onto the shared
// queue — from a document whose author was told the two spellings mean the
// same thing (docs/DSL.md).

// defaultChannels is the space the route decision is a function of: the
// execution channel TemporalStarter.StartRun bound into this run's input.
// These are the four configurations acceptance item 11 enumerates.
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
	// The same override written as a pool's queue — which the repository's own
	// harness does: cmd/orkctl/integration_test.go binds
	// task.QueueForPool(privateTaskPool()).
	{name: "an override that is itself pool-shaped", bound: "ork-pool-gpu", wantPool: "gpu"},
}

// TestRunState_TheDefaultPoolIsTheDeploymentsDefaultChannel is acceptance item
// 11 stated directly: `pool: main` and an omitted pool are the SAME route,
// in BOTH projections, under every configuration — by construction, not by
// coincidence.
//
// `main` names the deployment's default pool. Whatever channel an operator
// bound as the default, that channel is what `main` resolves to; it is not a
// synonym for task.DefaultQueue, which is only what the default pool resolves
// to when nothing was configured.
func TestRunState_TheDefaultPoolIsTheDeploymentsDefaultChannel(t *testing.T) {
	for _, channel := range defaultChannels {
		t.Run(channel.name, func(t *testing.T) {
			s := &runState{orkerQueue: channel.bound}

			omitted, err := s.routeForTask(&orkstackv1.Task{Id: "silent"})
			if err != nil {
				t.Fatalf("routeForTask(no pool): %v", err)
			}
			named, err := s.routeForTask(&orkstackv1.Task{Id: "named", Pool: task.DefaultPool})
			if err != nil {
				t.Fatalf("routeForTask(pool: %s): %v", task.DefaultPool, err)
			}
			if named != omitted {
				t.Errorf("`pool: %s` routes to %+v and no pool routes to %+v under the bound "+
					"channel %q; the two spellings must reach the same Orkers under every "+
					"configuration", task.DefaultPool, named, omitted, channel.bound)
			}

			// The two projections separately, because they are what the
			// workflow calls: a divergence in either is a divergence a client
			// or an Orker sees.
			omittedQueue, err := s.queueForTask(&orkstackv1.Task{Id: "silent"})
			if err != nil {
				t.Fatalf("queueForTask(no pool): %v", err)
			}
			namedQueue, err := s.queueForTask(&orkstackv1.Task{Id: "named", Pool: task.DefaultPool})
			if err != nil {
				t.Fatalf("queueForTask(pool: %s): %v", task.DefaultPool, err)
			}
			if namedQueue != omittedQueue {
				t.Errorf("`pool: %s` dispatches to %q and no pool dispatches to %q: one reported "+
					"pool, two sets of Orkers", task.DefaultPool, namedQueue, omittedQueue)
			}
			omittedPool := s.poolForTask(&orkstackv1.Task{Id: "silent"})
			namedPool := s.poolForTask(&orkstackv1.Task{Id: "named", Pool: task.DefaultPool})
			if namedPool != omittedPool {
				t.Errorf("`pool: %s` is reported on pool %q and no pool on %q: the same Orkers "+
					"under two names", task.DefaultPool, namedPool, omittedPool)
			}

			// And the route is the operator's channel, under its own name.
			wantQueue := channel.bound
			if wantQueue == "" {
				wantQueue = task.DefaultQueue
			}
			if namedQueue != wantQueue {
				t.Errorf("`pool: %s` dispatches to %q, want the bound channel %q",
					task.DefaultPool, namedQueue, wantQueue)
			}
			if namedPool != channel.wantPool {
				t.Errorf("`pool: %s` is reported on pool %q, want %q — the name the bound "+
					"channel %q answers to", task.DefaultPool, namedPool, channel.wantPool, wantQueue)
			}
		})
	}
}

// TestRunState_OnePoolNameMeansOneSetOfOrkers is the other half of the same
// invariant: within one run, the reported pool and the dispatch queue
// determine each other. Two tasks reported on the same pool go to the same
// queue; two tasks on different queues are reported on different pools.
func TestRunState_OnePoolNameMeansOneSetOfOrkers(t *testing.T) {
	pooled := func(id, pool string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Pool: pool}
	}
	for _, channel := range defaultChannels {
		t.Run(channel.name, func(t *testing.T) {
			s := &runState{orkerQueue: channel.bound}
			tasks := []*orkstackv1.Task{
				pooled("silent", ""),
				pooled("named", task.DefaultPool),
				pooled("gpu", "gpu"),
				pooled("etl", "etl"),
			}
			queueOf := map[string]string{} // pool -> queue
			poolOf := map[string]string{}  // queue -> pool
			for _, tk := range tasks {
				queue, err := s.queueForTask(tk)
				if err != nil {
					t.Fatalf("queueForTask(%q): %v", tk.GetId(), err)
				}
				pool := s.poolForTask(tk)
				if seen, ok := queueOf[pool]; ok && seen != queue {
					t.Errorf("tasks reported on pool %q were dispatched to %q and %q: "+
						"one pool name, two sets of Orkers", pool, seen, queue)
				}
				if seen, ok := poolOf[queue]; ok && seen != pool {
					t.Errorf("tasks dispatched to %q were reported on pools %q and %q: "+
						"one set of Orkers, two names", queue, seen, pool)
				}
				queueOf[pool] = queue
				poolOf[queue] = pool
			}
		})
	}
}

// TestRunState_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute is the
// isolation half, stated on its own because it is the one that bites: an agent
// running under ORK_ORKER_QUEUE (AGENTS.md §6) must not be able to reach the
// shared `ork-tasks` by writing `pool: main`, however the override is spelled.
func TestRunState_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute(t *testing.T) {
	for _, channel := range defaultChannels {
		if channel.bound == task.DefaultQueue || channel.bound == "" {
			continue // no isolation was asked for
		}
		t.Run(channel.name, func(t *testing.T) {
			s := &runState{orkerQueue: channel.bound}
			queue, err := s.queueForTask(&orkstackv1.Task{Id: "named", Pool: task.DefaultPool})
			if err != nil {
				t.Fatalf("queueForTask(pool: %s): %v", task.DefaultPool, err)
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

// TestRunState_AnUnmappablePoolFailsTheRun keeps the other half of the
// contract: a pool nothing can route to is an error, never a fallback onto
// whatever queue happens to be at hand.
func TestRunState_AnUnmappablePoolFailsTheRun(t *testing.T) {
	s := &runState{orkerQueue: task.DefaultQueue}
	for _, pool := range []string{"GPU", "gpu.1", "1gpu", "ork pool"} {
		queue, err := s.queueForTask(&orkstackv1.Task{Id: "crunch", Pool: pool})
		if err == nil {
			t.Errorf("queueForTask(pool: %q) = %q, want the run to fail on an unroutable pool", pool, queue)
		}
	}
}
