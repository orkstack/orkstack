package task_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// TestQueueForPool_MapsTheDefaultPoolOntoTheQueueAZeroConfigStackPolls pins the
// one mapping the whole dev stack rests on. Break it and `make run-orker`
// polls a queue nothing dispatches to, while the committed replay history
// names a third one.
func TestQueueForPool_MapsTheDefaultPoolOntoTheQueueAZeroConfigStackPolls(t *testing.T) {
	for _, pool := range []string{"", task.DefaultPool} {
		queue, err := task.QueueForPool(pool)
		if err != nil {
			t.Fatalf("QueueForPool(%q): %v", pool, err)
		}
		if queue != task.DefaultQueue {
			t.Errorf("QueueForPool(%q) = %q, want %q", pool, queue, task.DefaultQueue)
		}
	}
}

// TestQueueForPool_IsInjectiveAndCannotForgeAReservedQueue is the isolation
// claim: a pool name is a slug, so it cannot escape its prefix and land on the
// engine's own queues or on another pool's.
func TestQueueForPool_IsInjectiveAndCannotForgeAReservedQueue(t *testing.T) {
	seen := map[string]string{}
	for _, pool := range []string{"", "main", "gpu", "batch", "a", "gpu-large"} {
		queue, err := task.QueueForPool(pool)
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
		queue, err := task.QueueForPool(pool)
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

func TestPoolForQueue_ReversesTheMappingAndSkipsForeignQueues(t *testing.T) {
	for _, pool := range []string{"main", "gpu", "gpu-large"} {
		queue, err := task.QueueForPool(pool)
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
