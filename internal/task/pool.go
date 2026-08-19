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

// MaxPoolName bounds a pool name, and is the DSL's own bound (dsl.MaxSlug).
// The pattern without a bound is only half a rule: a 201-character pool is
// refused by every document and would still map to a queue here.
const MaxPoolName = 200

// ErrInvalidPool reports a pool name that cannot be mapped to a queue.
var ErrInvalidPool = fmt.Errorf("invalid pool name")

// QueueForPool is the ONLY translation from an OrkStack pool to a Temporal
// task queue.
//
// defaultQueue is the execution channel this deployment bound as its default —
// ORK_ORKER_QUEUE, or the queue TemporalStarter.StartRun bound into a run's
// input. Empty means the unconfigured task.DefaultQueue, which is what `make
// run-ork` + `make run-orker` poll.
//
// It is a parameter and not a package default because `main` is the NAME of
// the deployment's default pool, not a synonym for one fixed queue: whatever
// channel an operator bound, `main` is a word for it. Answering DefaultQueue
// for `main` regardless is how `pool: main` came to dispatch to ork-tasks
// while a task naming no pool took the run's own channel — two disjoint sets
// of Orkers under one reported pool, and, under the ORK_ORKER_QUEUE isolation
// AGENTS.md §6 gives every parallel agent, an escape from it. The signature
// carries the fix: a caller cannot ask this question without saying what the
// default channel is, so the answer cannot be wrong by omission.
//
// It is injective over pools. Every pool other than the default maps under
// poolQueuePrefix, and since a pool name may not contain the characters that
// would let it forge one, no pool can reach DefaultQueue, EventQueue or
// another pool's queue.
func QueueForPool(pool, defaultQueue string) (string, error) {
	if defaultQueue == "" {
		defaultQueue = DefaultQueue
	}
	if pool == "" || pool == DefaultPool {
		return defaultQueue, nil
	}
	if err := validPool(pool); err != nil {
		return "", err
	}
	return poolQueuePrefix + pool, nil
}

// validPool reports why a pool name cannot be mapped.
func validPool(pool string) error {
	if len(pool) > MaxPoolName {
		return fmt.Errorf("%w: %d bytes exceeds the %d-byte limit (%.32q…)",
			ErrInvalidPool, len(pool), MaxPoolName, pool)
	}
	if !poolPattern.MatchString(pool) {
		return fmt.Errorf("%w: %q must match %s", ErrInvalidPool, pool, poolPattern)
	}
	return nil
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
	if validPool(pool) != nil {
		return "", false
	}
	return pool, true
}
