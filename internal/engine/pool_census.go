package engine

import (
	"context"
	"sync"
)

// poolCensus is the Orker inventory (ReadFleet) grouped by the pool each Orker
// says it serves: what one Plan or Pools call asks about a pool beyond its
// channel's poller records.
//
// It exists because those records only show an Orker with a free slot. A busy
// Orker reserves no slot, so it polls nothing on its pool's channel, and once
// its last poll is older than DefaultPollerFreshness the cluster's records
// say nobody serves the pool — the same answer a pool nobody serves gives. A
// capability probe sent on that channel waits behind the busy slots for the
// same reason. An Orker's presence and control channels have workers and
// slots of their own, so the inventory lists and asks a busy Orker like an
// idle one.
//
// An Orker is matched to a pool by the name it reports (DescribeOrkerResult.
// pool), which is the name RouteForPool gives its channel. Two Orkers polling
// two raw ORK_ORKER_QUEUE channels of one Temporal namespace both report the
// default pool, so the census cannot tell them apart; the pool channel's own
// records still can, and both are asked.
//
// It is read at most once, and only when a question needs it: a submit that
// finds its pools' channels polled and carries no pinned, fan-out, cached,
// failure-handler or sized task never reads it, so it costs such a submit
// nothing. One that does pays one describe per live Orker, together, each
// bounded by orkerDescribeTimeout.
type poolCensus struct {
	fleet FleetReader

	once       sync.Once
	err        error
	byPool     map[string][]FleetOrker
	unanswered []string
}

func newPoolCensus(fleet FleetReader) *poolCensus {
	return &poolCensus{fleet: fleet}
}

// read takes the census, once. Only a failed presence read fails it; an Orker
// that does not answer on its own channel is counted as unanswered and
// attributed to no pool, because which pool it serves is what it did not say.
func (c *poolCensus) read(ctx context.Context) error {
	c.once.Do(func() {
		c.byPool = map[string][]FleetOrker{}
		if c.fleet == nil {
			return
		}
		readCtx, cancel := context.WithTimeout(ctx, poolDescribeTimeout)
		defer cancel()
		orkers, err := ReadFleet(readCtx, c.fleet)
		if err != nil {
			c.err = err
			return
		}
		for _, orker := range orkers {
			if orker.Err != nil || orker.Described == nil {
				c.unanswered = append(c.unanswered, orker.Presence.Identity)
				continue
			}
			pool := orker.Described.GetPool()
			c.byPool[pool] = append(c.byPool[pool], orker)
		}
	})
	return c.err
}

// taking is the Orkers of one pool that answered on their own channel and
// would take a new task: a cordoned Orker answers, but takes nothing new, so
// it neither keeps a pool live nor speaks for what the pool can run. In
// identity order.
func (c *poolCensus) taking(ctx context.Context, pool string) ([]FleetOrker, error) {
	if err := c.read(ctx); err != nil {
		return nil, err
	}
	var orkers []FleetOrker
	for _, orker := range c.byPool[pool] {
		if !orker.Described.GetCordoned() {
			orkers = append(orkers, orker)
		}
	}
	return orkers, nil
}

// unansweredCount is how many live Orkers did not answer on their own
// channel. Zero before the census was read.
func (c *poolCensus) unansweredCount() int {
	if c.byPool == nil {
		return 0
	}
	return len(c.unanswered)
}
