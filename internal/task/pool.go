package task

import (
	"fmt"
	"regexp"
)

// A pool is the user-facing word for where a task runs: a set of Orkers. A
// Temporal task queue is how the engine makes that happen, and the translation
// between the two lives here and nowhere else. Everything above this file —
// proto, the DSL, orkctl, the UI — says pool; everything below it says queue.
const (
	// DefaultPool is the pool a task runs on when neither the task nor its
	// namespace names one.
	DefaultPool = "main"

	// poolQueuePrefix namespaces every pool's queue, so no pool can name a
	// queue the engine reserves for itself.
	poolQueuePrefix = "ork-pool-"
)

// poolPattern is the pool name rule. It is the DSL's slug rule, repeated here
// because this function is also reachable from a hand-built message that never
// went through the compiler.
var poolPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ErrInvalidPool reports a pool name that cannot be mapped to a queue.
var ErrInvalidPool = fmt.Errorf("invalid pool name")

// QueueForPool is the ONLY translation from an OrkStack pool to a Temporal
// task queue.
//
// It is injective. DefaultPool maps to DefaultQueue, the one mapping outside
// the prefix, because a zero-configuration dev stack depends on it: `make
// run-orker` polls DefaultQueue, ORK_ORKER_QUEUE defaults to it in both
// binaries, and the committed replay history's activities name it. Every other
// pool maps under poolQueuePrefix, and since a pool name may not contain the
// characters that would let it forge one, no pool can reach DefaultQueue,
// EventQueue or another pool's queue.
//
// DefaultPool -> DefaultQueue is the UNCONFIGURED answer, and it is the answer
// only for a caller with no run in hand. A run carries the default queue an
// operator bound into it, and inside the engine `main` means THAT queue —
// engine.runState.routeForTask resolves it there and never asks this function.
// Confusing the two is what once sent `pool: main` to DefaultQueue while a
// task naming no pool took the run's own queue.
func QueueForPool(pool string) (string, error) {
	if pool == "" || pool == DefaultPool {
		return DefaultQueue, nil
	}
	if !poolPattern.MatchString(pool) {
		return "", fmt.Errorf("%w: %q must match %s", ErrInvalidPool, pool, poolPattern)
	}
	return poolQueuePrefix + pool, nil
}

// PoolForQueue reverses QueueForPool for labelling and reporting. A queue this
// package did not generate reports ok=false rather than guessing a pool name.
func PoolForQueue(queue string) (string, bool) {
	if queue == DefaultQueue {
		return DefaultPool, true
	}
	if len(queue) <= len(poolQueuePrefix) || queue[:len(poolQueuePrefix)] != poolQueuePrefix {
		return "", false
	}
	pool := queue[len(poolQueuePrefix):]
	if !poolPattern.MatchString(pool) {
		return "", false
	}
	return pool, true
}
