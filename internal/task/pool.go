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

// ErrReservedQueue reports an execution channel the engine keeps for itself.
// It is not a bad pool name — it is a deployment pointed at a queue no task
// may run on.
var ErrReservedQueue = fmt.Errorf("reserved task queue")

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
// The queues it can answer are exactly two: the default channel, and
// poolQueuePrefix + pool. A pool name is a bounded slug (validPool), so it
// cannot forge the prefix — no NAMED pool reaches DefaultQueue, the engine's
// event queue, another pool's queue, or an operator's raw isolation channel.
// The DEFAULT pool reaches the bound channel and nothing else, because that
// channel is what the word means; the one channel that is refused rather than
// routed is EngineEventQueue, which carries the engine's own projection work
// (ErrReservedQueue, and `ork` refuses to boot on it).
//
// What it is NOT is one-to-one from pool names onto queues. The comment here
// used to say it was, and that was wrong for one shape: bind ork-pool-gpu as
// the default channel and `main` and `gpu` answer the same queue, because the
// deployment made them one set of Orkers. One set of Orkers under two names is
// a lie a client would read, so the resolution that also NAMES a pool
// collapses them — RouteForPool, below, which is what every caller that
// reports a pool must use. This function answers the queue alone.
func QueueForPool(pool, defaultQueue string) (string, error) {
	if defaultQueue == "" {
		defaultQueue = DefaultQueue
	}
	if defaultQueue == EngineEventQueue {
		return "", fmt.Errorf("%w: %q carries the engine's own projection activities, "+
			"so no pool may be routed to it", ErrReservedQueue, defaultQueue)
	}
	if pool == "" || pool == DefaultPool {
		return defaultQueue, nil
	}
	if err := validPool(pool); err != nil {
		return "", err
	}
	return poolQueuePrefix + pool, nil
}

// Route is where one task runs: the pool a client is told, and the Temporal
// task queue the activity is scheduled on. The two travel together because
// they are one decision. Computing them separately is how `pool: main` came
// to dispatch to DefaultQueue while a task that named no pool dispatched to
// the run's own channel.
type Route struct {
	Pool  string
	Queue string
}

// DefaultRoute is where a task that names no pool runs, and the name that
// route answers to.
//
// The pool is the bound channel's own name in the user's vocabulary:
// PoolForQueue names the pool when an operator pointed the default channel at
// one, and declines for a raw isolation channel (ORK_ORKER_QUEUE=ork-tasks-m7-9),
// which is nobody's pool and therefore reports the built-in default — the pool
// a document that named none asked for.
func DefaultRoute(defaultQueue string) (Route, error) {
	queue, err := QueueForPool(DefaultPool, defaultQueue)
	if err != nil {
		return Route{}, err
	}
	pool, ok := PoolForQueue(queue)
	if !ok {
		pool = DefaultPool
	}
	return Route{Pool: pool, Queue: queue}, nil
}

// RouteForPool resolves one task's pool against the channel its deployment
// bound as the default, and is the only resolution that answers both halves.
//
// It holds the property QueueForPool cannot: one set of Orkers has exactly one
// name. A pool whose queue IS the bound channel is reported under the name
// that channel answers to, whichever of the two spellings the document used.
// So `pool: main` and an omitted pool are one route under every configuration,
// and under a channel an operator pointed at the gpu pool's own queue, `pool:
// main` is reported on gpu — reporting `main` while running on gpu's Orkers
// would be a lie one rename later.
//
// It is a pure function of its arguments, so a caller inside Temporal workflow
// code keeps the determinism boundary.
func RouteForPool(pool, defaultQueue string) (Route, error) {
	def, err := DefaultRoute(defaultQueue)
	if err != nil {
		return Route{}, err
	}
	queue, err := QueueForPool(pool, def.Queue)
	if err != nil {
		return Route{}, err
	}
	if queue == def.Queue {
		return def, nil
	}
	return Route{Pool: pool, Queue: queue}, nil
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
