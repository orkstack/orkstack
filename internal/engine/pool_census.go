package engine

import (
	"context"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
		c.fill(orkers)
	})
	return c.err
}

// fill groups an inventory by the pool each Orker answered for. Once.
func (c *poolCensus) fill(orkers []FleetOrker) {
	c.byPool = map[string][]FleetOrker{}
	for _, orker := range orkers {
		if orker.Err != nil || orker.Described == nil {
			c.unanswered = append(c.unanswered, orker.Presence.Identity)
			continue
		}
		pool := orker.Described.GetPool()
		c.byPool[pool] = append(c.byPool[pool], orker)
	}
}

// censusOf is the census of an inventory already read (ReadFleet).
func censusOf(orkers []FleetOrker) *poolCensus {
	c := &poolCensus{}
	c.once.Do(func() { c.fill(orkers) })
	return c
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

// serving is the Orkers of one pool that answered on their own channel and
// either take a new task now or will once object storage answers them. An
// Orker waiting out a store outage has only paused: work sent to its pool waits
// and then runs, so it keeps the pool live and speaks for what the pool can
// run. An operator's cordon says the opposite, that this Orker is leaving, so
// one that holds both does not count. In identity order.
func (c *poolCensus) serving(ctx context.Context, pool string) ([]FleetOrker, error) {
	if err := c.read(ctx); err != nil {
		return nil, err
	}
	var orkers []FleetOrker
	for _, orker := range c.byPool[pool] {
		if !orker.Described.GetCordoned() || pausedForStoreOutage(orker.Described) {
			orkers = append(orkers, orker)
		}
	}
	return orkers, nil
}

// pausedForStoreOutage says a store outage alone holds this Orker's cordon: an
// operator's cordon always carries a reason.
func pausedForStoreOutage(o *orkstackv1.DescribeOrkerResult) bool {
	return o.GetStoreOutage() != nil && o.GetCordonReason() == ""
}

// wasRead says the census was taken, successfully, by an earlier question;
// it never takes one.
func (c *poolCensus) wasRead() bool { return c.byPool != nil && c.err == nil }

// unansweredCount is how many live Orkers did not answer on their own
// channel. Zero before the census was read.
func (c *poolCensus) unansweredCount() int {
	if c.byPool == nil {
		return 0
	}
	return len(c.unanswered)
}
