package task

import (
	"errors"
	"fmt"
)

// ErrPoolBindingConflict reports a process told to serve a pool and, at the
// same time, to poll a raw queue that is not that pool's.
var ErrPoolBindingConflict = errors.New("pool and queue disagree")

// PoolBinding is where one process's task work lives: the pool it serves, and
// the Temporal task queue that pool resolves to.
//
// Both binaries resolve it from the same two variables, because they must
// arrive at the same answer. An Orker polls Queue; the engine dispatches a
// task that names no pool of its own to Queue. Disagree and the run reaches
// RUNNING and stays there: the Orker is listening to one channel while the
// engine speaks into another, and nothing reports it (AGENTS.md §6).
type PoolBinding struct {
	// Pool is the OrkStack name for this process's work: what a task's `pool:`
	// has to say to reach it, and what a report calls it.
	Pool string

	// Queue is the Temporal task queue underneath. Nothing above the engine
	// seam ever sees it.
	Queue string

	// RawOverride reports that Queue came from the operator escape hatch
	// rather than from Pool. It is what lets a boot log say so: a raw queue
	// this package did not generate is nobody's pool, so Pool is then the
	// built-in default rather than an answer derived from the queue.
	RawOverride bool
}

// ResolvePoolBinding turns the pool a process was told to serve
// (ORK_ORKER_POOL) and the raw queue an operator overrode it with
// (ORK_ORKER_QUEUE) into the one binding both binaries use.
//
// The two variables answer different questions and are documented as such in
// docs/DEPLOY.md. pool is OrkStack's: which set of Orkers is this. rawQueue is
// the operator's escape hatch to the channel underneath, which the test
// harnesses use to isolate concurrent runs from each other.
//
// Five cases, and only one of them fails:
//
//   - neither: the built-in pool on the queue a zero-config stack polls, byte
//     for byte what `make run-orker` did before pools existed;
//   - pool only: the pool's own queue, through the single mapping in pool.go;
//   - raw only: that queue verbatim, labelled with the pool whose image it is,
//     or with the built-in default when it is nobody's image;
//   - both, agreeing: the pool, on its queue;
//   - both, disagreeing: refused, naming both variables and both values.
//
// The last one is refused rather than resolved because either winner is wrong
// half the time, and the wrong one is invisible until a run hangs.
func ResolvePoolBinding(pool, rawQueue string) (PoolBinding, error) {
	if pool == "" && rawQueue == "" {
		return PoolBinding{Pool: DefaultPool, Queue: DefaultQueue}, nil
	}

	if pool == "" {
		binding := PoolBinding{Pool: DefaultPool, Queue: rawQueue, RawOverride: true}
		if named, ok := PoolForQueue(rawQueue); ok {
			binding.Pool = named
		}
		return binding, nil
	}

	queue, err := QueueForPool(pool)
	if err != nil {
		return PoolBinding{}, fmt.Errorf("ORK_ORKER_POOL: %w", err)
	}
	if rawQueue == "" {
		return PoolBinding{Pool: pool, Queue: queue}, nil
	}
	if rawQueue != queue {
		return PoolBinding{}, fmt.Errorf(
			"%w: ORK_ORKER_POOL=%s resolves to %q but ORK_ORKER_QUEUE=%q; "+
				"set one of them, or make them agree",
			ErrPoolBindingConflict, pool, queue, rawQueue)
	}
	return PoolBinding{Pool: pool, Queue: queue, RawOverride: true}, nil
}
