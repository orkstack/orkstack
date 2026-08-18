package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A white-box test, because the property is about the two halves of one
// decision and both are unexported. What it holds is entirely user-facing: the
// pool a client is told a task ran on must name the Orkers the task was
// dispatched to.
//
// The defect it closes: `pool: main` mapped through task.QueueForPool to
// task.DefaultQueue while a task that named no pool went to the run's
// orkerQueue, and BOTH were reported as pool "main". Under the ORK_ORKER_QUEUE
// isolation AGENTS.md §6 gives every parallel agent, that made `pool: main` an
// escape hatch onto the shared queue — from a document whose author was told
// the two spellings mean the same thing (docs/DSL.md).

// TestRunState_OnePoolNameMeansOneSetOfOrkers is the invariant: within one
// run, the reported pool and the dispatch queue determine each other. Two
// tasks reported on the same pool go to the same queue; two tasks on different
// queues are reported on different pools.
func TestRunState_OnePoolNameMeansOneSetOfOrkers(t *testing.T) {
	pooled := func(id, pool string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Pool: pool}
	}
	for _, tc := range []struct {
		name       string
		orkerQueue string
		tasks      []*orkstackv1.Task
	}{
		{
			// The zero-configuration stack: `make run-ork` + `make run-orker`.
			name:       "the default route",
			orkerQueue: task.DefaultQueue,
			tasks: []*orkstackv1.Task{
				pooled("silent", ""), pooled("named", task.DefaultPool), pooled("gpu", "gpu"),
			},
		},
		{
			// A raw isolation queue: nobody's pool, so a document that named
			// none is reported on the built-in default — and a document that
			// named that default must reach the very same Orkers.
			name:       "a raw operator isolation queue",
			orkerQueue: "ork-tasks-isolated-agent",
			tasks: []*orkstackv1.Task{
				pooled("silent", ""), pooled("named", task.DefaultPool), pooled("gpu", "gpu"),
			},
		},
		{
			// An operator who pointed the default route at a pool's own queue.
			// A task that names no pool is reported on that pool, and `main`
			// is then a genuinely different pool with its own queue.
			name:       "the default route pointed at a pool",
			orkerQueue: "ork-pool-gpu",
			tasks: []*orkstackv1.Task{
				pooled("silent", ""), pooled("gpu", "gpu"), pooled("named", task.DefaultPool),
			},
		},
		{
			// StartRun always binds a queue; a zero value must still resolve.
			name:       "no queue bound at all",
			orkerQueue: "",
			tasks: []*orkstackv1.Task{
				pooled("silent", ""), pooled("named", task.DefaultPool),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &runState{orkerQueue: tc.orkerQueue}
			queueOf := map[string]string{} // pool -> queue
			poolOf := map[string]string{}  // queue -> pool
			for _, tk := range tc.tasks {
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
// shared `ork-tasks` by writing `pool: main`.
func TestRunState_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute(t *testing.T) {
	const isolated = "ork-tasks-m7-9"
	s := &runState{orkerQueue: isolated}

	queue, err := s.queueForTask(&orkstackv1.Task{Id: "named", Pool: task.DefaultPool})
	if err != nil {
		t.Fatalf("queueForTask(pool: %s): %v", task.DefaultPool, err)
	}
	if queue != isolated {
		t.Errorf("`pool: %s` dispatched to %q under ORK_ORKER_QUEUE=%q; a document must not be able "+
			"to leave the operator's route", task.DefaultPool, queue, isolated)
	}
	if queue == task.DefaultQueue {
		t.Errorf("`pool: %s` reached the shared queue %q, which is what the isolation exists to prevent",
			task.DefaultPool, task.DefaultQueue)
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
