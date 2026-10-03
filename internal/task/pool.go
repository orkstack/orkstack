package task

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
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

	// orkerQueuePrefix names every Orker's control channel:
	// orkerQueuePrefix + orker_id. Exactly one process polls each, and it
	// carries DescribeOrker and CordonOrker to that process and no other
	// (orker-cordon-drain §4). It is internal: no user-facing surface names it.
	orkerQueuePrefix = "ork-orker-"

	// OrkersQueue is the presence channel every Orker polls and nothing
	// dispatches to. Its poller list is the deployment's Orker inventory; it
	// holds no work (TestOrkersQueue_CarriesNoDispatch keeps it so).
	OrkersQueue = "ork-orkers"

	// MaxOrkerID bounds an Orker id, the same bound a pool name has.
	MaxOrkerID = 200
)

// ErrInvalidOrkerID reports an Orker id that cannot name a control channel.
var ErrInvalidOrkerID = fmt.Errorf("invalid orker id")

// ValidOrkerID is the rule an Orker id must meet to be addressable: 1 to 200
// bytes, no byte at or below 0x20 and no 0x7F. It is the rule cmd/orker's boot
// and the API boundary both apply; the derived default (`<host>-<8 hex>`,
// internal/orker.defaultOrkerID) always passes. The id is validated, never
// sanitized.
func ValidOrkerID(orkerID string) error {
	if orkerID == "" {
		return fmt.Errorf("%w: empty", ErrInvalidOrkerID)
	}
	if len(orkerID) > MaxOrkerID {
		return fmt.Errorf("%w: %d bytes exceeds the %d-byte limit (%.32q…)",
			ErrInvalidOrkerID, len(orkerID), MaxOrkerID, orkerID)
	}
	for i := 0; i < len(orkerID); i++ {
		if b := orkerID[i]; b <= 0x20 || b == 0x7f {
			return fmt.Errorf("%w: %q contains whitespace or a control character at byte %d",
				ErrInvalidOrkerID, orkerID, i)
		}
	}
	return nil
}

// QueueForOrker is the ONLY translation from an Orker id to its control
// channel. The id is validated, never sanitized (ValidOrkerID).
func QueueForOrker(orkerID string) (string, error) {
	if err := ValidOrkerID(orkerID); err != nil {
		return "", err
	}
	return orkerQueuePrefix + orkerID, nil
}

// reservedQueue refuses the channels no pool may be routed to: the engine's
// own projection channel, the Orker presence channel, and every Orker's
// control channel. It is the one place those shapes are listed, so QueueForPool
// and ResolvePoolBinding (through DefaultRoute) refuse the same set.
func reservedQueue(queue string) error {
	switch {
	case queue == EngineEventQueue:
		return fmt.Errorf("%w: %q carries the engine's own projection activities, "+
			"so no pool may be routed to it", ErrReservedQueue, queue)
	case queue == OrkersQueue:
		return fmt.Errorf("%w: %q is the Orker presence channel, which nothing may be "+
			"dispatched to, so no pool may be routed to it", ErrReservedQueue, queue)
	case strings.HasPrefix(queue, orkerQueuePrefix):
		return fmt.Errorf("%w: %q is shaped like one Orker's own control channel, "+
			"so no pool may be routed to it", ErrReservedQueue, queue)
	}
	return nil
}

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
// AGENTS.md §11.1 gives every parallel agent, an escape from it. The signature
// carries the fix: a caller cannot ask this question without saying what the
// default channel is, so the answer cannot be wrong by omission.
//
// The queues it can answer are exactly two: the default channel, and
// poolQueuePrefix + pool. A pool name is a bounded slug (validPool), so it
// cannot forge the prefix — no NAMED pool reaches DefaultQueue, the engine's
// event queue, another pool's queue, or an operator's raw isolation channel.
// The DEFAULT pool reaches the bound channel and nothing else, because that
// channel is what the word means; the channels that are refused rather than
// routed are EngineEventQueue, which carries the engine's own projection work,
// and the Orker presence and control channels (reservedQueue; ErrReservedQueue,
// and both binaries refuse to boot on any of them).
//
// What it is NOT is one-to-one from pool names onto queues, and one shape
// shows why: bind ork-pool-gpu as the default channel and `main` and `gpu`
// answer the same queue, because the
// deployment made them one set of Orkers. One set of Orkers under two names is
// a lie a client would read, so the resolution that also NAMES a pool
// collapses them — RouteForPool, below, which is what every caller that
// reports a pool must use. This function answers the queue alone.
func QueueForPool(pool, defaultQueue string) (string, error) {
	if defaultQueue == "" {
		defaultQueue = DefaultQueue
	}
	if err := reservedQueue(defaultQueue); err != nil {
		return "", err
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
// one, and declines for a raw isolation channel (ORK_ORKER_QUEUE=ork-tasks-agent),
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

// PoolForChannel names the pool a channel carries, for a deployment whose
// default route is def (DefaultRoute of its ORK_ORKER_QUEUE): the default
// route's own channel answers def.Pool — so a raw ORK_ORKER_QUEUE channel is
// called by the name its Orkers report — and every other channel answers
// PoolForQueue. ok=false for anything else: a raw channel that is not the
// default, a pin-creation channel, another product's channel. It builds no
// channel name and adds no prefix; it composes the two readers this file
// already has (orkctl-release §4).
func PoolForChannel(queue string, def Route) (string, bool) {
	if queue != "" && queue == def.Queue {
		return def.Pool, true
	}
	return PoolForQueue(queue)
}

// MaxCordonReason bounds a cordon's reason, in bytes.
const MaxCordonReason = 256

// ValidCordonReason is the rule a cordon's reason meets, at the API boundary
// and again on the Orker: present (not blank), at most 256 bytes, valid UTF-8,
// no control character. It is what ListOrkers shows beside the state, so it
// must be one printable line.
func ValidCordonReason(reason string) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("a cordon needs a reason: it is what every reader of the fleet sees beside the state")
	}
	if len(reason) > MaxCordonReason {
		return fmt.Errorf("a cordon reason is at most %d bytes; this one is %d", MaxCordonReason, len(reason))
	}
	if !utf8.ValidString(reason) {
		return fmt.Errorf("a cordon reason must be valid UTF-8")
	}
	for _, r := range reason {
		if unicode.IsControl(r) {
			return fmt.Errorf("a cordon reason may not contain a control character (%U)", r)
		}
	}
	return nil
}
