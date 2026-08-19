package task_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// boundChannels is the space QueueForPool's answer is a function of, beside
// the pool itself: the execution channel a deployment bound as its default.
// These are the four configurations acceptance item 11 enumerates, and the
// same four internal/engine/pool_route_test.go drives the engine with.
var boundChannels = []struct {
	name  string
	bound string
	// want is the queue the DEFAULT pool must resolve to under this channel.
	want string
}{
	// The zero-configuration stack: `make run-ork` + `make run-orker`.
	{name: "the unconfigured default channel", bound: task.DefaultQueue, want: task.DefaultQueue},
	// A caller with nothing bound must still get the zero-config answer.
	{name: "nothing bound at all", bound: "", want: task.DefaultQueue},
	// ORK_ORKER_QUEUE for one parallel agent (AGENTS.md §6).
	{name: "a plain operator override", bound: "ork-tasks-m7-9", want: "ork-tasks-m7-9"},
	// The same override written as a pool's queue, which this repository's own
	// harness does (cmd/orkctl/integration_test.go).
	{name: "an override that is itself pool-shaped", bound: "ork-pool-gpu", want: "ork-pool-gpu"},
}

// TestQueueForPool_TheDefaultPoolIsWhateverChannelTheDeploymentBound is the
// whole reason the default channel is an ARGUMENT.
//
// `main` is the name of the deployment's default pool, not a synonym for one
// fixed queue. A function that answered task.DefaultQueue for `main` whatever
// the deployment bound would give the word two meanings, and the tree would
// hold only while every caller happened to route the default somewhere else
// first. Here the caller cannot omit the channel, so it cannot get the
// unconfigured answer by accident.
func TestQueueForPool_TheDefaultPoolIsWhateverChannelTheDeploymentBound(t *testing.T) {
	for _, channel := range boundChannels {
		t.Run(channel.name, func(t *testing.T) {
			for _, pool := range []string{"", task.DefaultPool} {
				queue, err := task.QueueForPool(pool, channel.bound)
				if err != nil {
					t.Fatalf("QueueForPool(%q, %q): %v", pool, channel.bound, err)
				}
				if queue != channel.want {
					t.Errorf("QueueForPool(%q, %q) = %q, want %q — the default pool is the "+
						"channel the deployment bound", pool, channel.bound, queue, channel.want)
				}
			}
		})
	}
}

// TestQueueForPool_IsInjectiveAndCannotForgeAReservedQueue is the isolation
// claim, under the channel it is a claim about: a pool name is a bounded slug,
// so it cannot escape its prefix and land on the engine's own queues or on
// another pool's.
//
// It drives the UNCONFIGURED channel, and says so because that is the boundary
// of the property. Distinct pools get distinct queues while the bound channel
// is not itself a pool's queue; bind ork-pool-gpu and `main` and `gpu` are one
// queue, because the deployment made them one set of Orkers. That case belongs
// to RouteForPool, which keeps them one NAME as well
// (TestRouteForPool_OneSetOfOrkersHasExactlyOneName).
func TestQueueForPool_IsInjectiveAndCannotForgeAReservedQueue(t *testing.T) {
	seen := map[string]string{}
	for _, pool := range []string{"", "main", "gpu", "batch", "a", "gpu-large"} {
		queue, err := task.QueueForPool(pool, "")
		if err != nil {
			t.Fatalf("QueueForPool(%q): %v", pool, err)
		}
		if pool != "" && pool != task.DefaultPool {
			if !strings.HasPrefix(queue, "ork-pool-") {
				t.Errorf("QueueForPool(%q) = %q, want it under the pool prefix", pool, queue)
			}
			if queue == task.DefaultQueue {
				t.Errorf("pool %q reached the default queue", pool)
			}
		}
		if other, dup := seen[queue]; dup && other != pool && !(other == "" || pool == "") {
			t.Errorf("pools %q and %q share queue %q", other, pool, queue)
		}
		seen[queue] = pool
	}

	// Anything that is not a slug is refused, so no pool can contain a
	// separator, a prefix of its own, or whitespace.
	for _, pool := range []string{
		"ork-tasks", "GPU", "gpu large", "gpu/large", "-gpu", "1gpu", "gpu_large", "../gpu",
	} {
		queue, err := task.QueueForPool(pool, "")
		if pool == "ork-tasks" {
			// A legal slug: it maps under the prefix like any other and does
			// NOT collide with the engine's queue of the same spelling.
			if err != nil || queue == task.DefaultQueue {
				t.Errorf("QueueForPool(%q) = %q, %v; want a distinct prefixed queue", pool, queue, err)
			}
			continue
		}
		if !errors.Is(err, task.ErrInvalidPool) {
			t.Errorf("QueueForPool(%q) = %q, %v; want ErrInvalidPool", pool, queue, err)
		}
	}
}

// TestQueueForPool_BoundsThePoolNameWhereTheDSLDoes covers the class a pattern
// alone cannot: length. The DSL bounds every slug at dsl.MaxSlug, so a pool
// longer than that is refused by every document; mapping it to a queue here
// would make this rule the looser of two and let a hand-built message name a
// pool no author could write.
func TestQueueForPool_BoundsThePoolNameWhereTheDSLDoes(t *testing.T) {
	atBound := strings.Repeat("a", task.MaxPoolName)
	if queue, err := task.QueueForPool(atBound, ""); err != nil {
		t.Errorf("QueueForPool(%d-byte pool) = %q, %v; want it routed", task.MaxPoolName, queue, err)
	}
	over := strings.Repeat("a", task.MaxPoolName+1)
	if queue, err := task.QueueForPool(over, ""); !errors.Is(err, task.ErrInvalidPool) {
		t.Errorf("QueueForPool(%d-byte pool) = %q, %v; want ErrInvalidPool",
			task.MaxPoolName+1, queue, err)
	}
	if pool, ok := task.PoolForQueue("ork-pool-" + over); ok {
		t.Errorf("PoolForQueue of an over-long pool queue = %q, true; want false", pool)
	}
}

// TestQueueForPool_RefusesTheEnginesOwnEventQueueAsAChannel is the one queue
// the mapping must not answer for any pool. ork-engine-events carries the
// engine's projection activities; a deployment that bound it as its execution
// channel would dispatch every task naming no pool — and every `pool: main` —
// onto the engine's internals. The comment on QueueForPool used to claim no
// pool could reach it, while `QueueForPool("main", "ork-engine-events")`
// returned exactly that.
func TestQueueForPool_RefusesTheEnginesOwnEventQueueAsAChannel(t *testing.T) {
	for _, pool := range []string{"", task.DefaultPool, "gpu"} {
		queue, err := task.QueueForPool(pool, task.EngineEventQueue)
		if !errors.Is(err, task.ErrReservedQueue) {
			t.Errorf("QueueForPool(%q, %q) = %q, %v; want ErrReservedQueue",
				pool, task.EngineEventQueue, queue, err)
		}
		if queue == task.EngineEventQueue {
			t.Errorf("pool %q reached the engine's event queue %q", pool, task.EngineEventQueue)
		}
		if _, err := task.RouteForPool(pool, task.EngineEventQueue); !errors.Is(err, task.ErrReservedQueue) {
			t.Errorf("RouteForPool(%q, %q) = %v; want ErrReservedQueue",
				pool, task.EngineEventQueue, err)
		}
	}
}

// TestRouteForPool_OneSetOfOrkersHasExactlyOneName is the property the
// QueueForPool comment used to claim and did not hold: two pools CAN share a
// queue, because an operator can bind one pool's queue as the deployment's
// default channel. What must never happen is a client being told two different
// pool names for one set of Orkers — and the collapse that prevents it lived
// in internal/engine, one layer above the function that documented it, where
// the next caller (M10.2's pool resolver) would not have gone through it.
func TestRouteForPool_OneSetOfOrkersHasExactlyOneName(t *testing.T) {
	pools := []string{"", task.DefaultPool, "gpu", "etl"}
	for _, channel := range boundChannels {
		t.Run(channel.name, func(t *testing.T) {
			poolOf := map[string]string{} // queue -> the one pool name it answers to
			queueOf := map[string]string{}
			for _, pool := range pools {
				r, err := task.RouteForPool(pool, channel.bound)
				if err != nil {
					t.Fatalf("RouteForPool(%q, %q): %v", pool, channel.bound, err)
				}
				if seen, ok := poolOf[r.Queue]; ok && seen != r.Pool {
					t.Errorf("queue %q is reported as pool %q and pool %q: one set of "+
						"Orkers under two names", r.Queue, seen, r.Pool)
				}
				if seen, ok := queueOf[r.Pool]; ok && seen != r.Queue {
					t.Errorf("pool %q resolves to queues %q and %q: one pool name, two "+
						"sets of Orkers", r.Pool, seen, r.Queue)
				}
				poolOf[r.Queue] = r.Pool
				queueOf[r.Pool] = r.Queue
			}

			// The two spellings of the default pool are one route, whatever
			// the deployment bound (acceptance item 11).
			omitted, err := task.RouteForPool("", channel.bound)
			if err != nil {
				t.Fatalf("RouteForPool(no pool, %q): %v", channel.bound, err)
			}
			named, err := task.RouteForPool(task.DefaultPool, channel.bound)
			if err != nil {
				t.Fatalf("RouteForPool(%q, %q): %v", task.DefaultPool, channel.bound, err)
			}
			if named != omitted {
				t.Errorf("`pool: %s` routes to %+v and no pool to %+v under the bound "+
					"channel %q", task.DefaultPool, named, omitted, channel.bound)
			}
			if named.Queue != channel.want {
				t.Errorf("the default pool runs on %q, want the bound channel %q",
					named.Queue, channel.want)
			}
		})
	}
}

// TestRouteForPool_NamesTheChannelAPoolQueueWasBoundAs is the shape that
// forced the collapse into this package: bind the gpu pool's own queue as the
// default channel and `pool: main` runs on gpu's Orkers. It must then be
// REPORTED on gpu — saying "main" would name a set of Orkers that also
// answers to another name.
func TestRouteForPool_NamesTheChannelAPoolQueueWasBoundAs(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}
	for _, pool := range []string{"", task.DefaultPool, "gpu"} {
		r, err := task.RouteForPool(pool, gpuQueue)
		if err != nil {
			t.Fatalf("RouteForPool(%q, %q): %v", pool, gpuQueue, err)
		}
		if r.Queue != gpuQueue || r.Pool != "gpu" {
			t.Errorf("RouteForPool(%q, %q) = %+v, want pool gpu on %q",
				pool, gpuQueue, r, gpuQueue)
		}
	}
}

func TestPoolForQueue_ReversesTheMappingAndSkipsForeignQueues(t *testing.T) {
	for _, pool := range []string{"main", "gpu", "gpu-large"} {
		queue, err := task.QueueForPool(pool, "")
		if err != nil {
			t.Fatalf("QueueForPool(%q): %v", pool, err)
		}
		got, ok := task.PoolForQueue(queue)
		if !ok || got != pool {
			t.Errorf("PoolForQueue(%q) = %q, %v; want %q, true", queue, got, ok, pool)
		}
	}
	for _, queue := range []string{"", "ork-engine-events", "ork-tasks-m7-9", "ork-pool-", "ork-pool-GPU"} {
		if pool, ok := task.PoolForQueue(queue); ok {
			t.Errorf("PoolForQueue(%q) = %q, true; want false", queue, pool)
		}
	}
}
