package engine

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

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

	// now is the replica's clock; nil is time.Now.
	now func() time.Time

	once       sync.Once
	err        error
	byPool     map[string][]FleetOrker
	unanswered []string
	// readAt is when the read began, on now: what a submit that pinned
	// profile contracts is judged fresh against (profileCensusMaxAge).
	readAt time.Time

	againOnce sync.Once
}

func newPoolCensus(fleet FleetReader) *poolCensus {
	return &poolCensus{fleet: fleet}
}

// clock is the census's now, time.Now when none was given.
func (c *poolCensus) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
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
		c.readAt = c.clock()
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
// either take a new task now or keep their pool live while they do not. An
// Orker waiting out a store outage has only paused: work sent to its pool waits
// and then runs. A stopping Orker finishes what it runs and keeps the pool live
// until its process exits. Each speaks for what the pool can run. An operator's
// cordon says the opposite, that this Orker is leaving, so one that holds it
// beside either does not count. In identity order.
func (c *poolCensus) serving(ctx context.Context, pool string) ([]FleetOrker, error) {
	if err := c.read(ctx); err != nil {
		return nil, err
	}
	var orkers []FleetOrker
	for _, orker := range c.byPool[pool] {
		if !orker.Described.GetCordoned() || keepsPoolLive(orker.Described) {
			orkers = append(orkers, orker)
		}
	}
	return orkers, nil
}

// cordonedOwners is the Orkers of one pool that answered under an
// operator's cordon and hold no profile drift: they are leaving, so they speak for nothing the pool can
// run, but a profiled submit reads the definitions they give
// (judgeTaskProfiles), so a run pins what they would run under once
// uncordoned. In identity order.
func (c *poolCensus) cordonedOwners(ctx context.Context, pool string) ([]FleetOrker, error) {
	if err := c.read(ctx); err != nil {
		return nil, err
	}
	var orkers []FleetOrker
	for _, orker := range c.byPool[pool] {
		if orker.Described.GetCordoned() && orker.Described.GetCordonReason() != "" && !profileDrifted(orker.Described) {
			orkers = append(orkers, orker)
		}
	}
	return orkers, nil
}

// profileDrifted says an Orker holds a profile drift cordon: what it reports
// is no longer what it audited, so its definitions are not read.
func profileDrifted(o *orkstackv1.DescribeOrkerResult) bool {
	for _, cause := range o.GetProfileCordons() {
		if cause.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT {
			return true
		}
	}
	return false
}

// askAgain asks the live Orkers that did not answer the census's read once
// more, together and each bounded by orkerDescribeTimeout, and files every
// answer under its pool: at most once per census, and only after a read. It
// says whether any answered. A late answer carries only its identity as
// presence, and is seen by every question asked after it, never by one asked
// before. Not for concurrent use with the census's readers: only Plan asks
// it, and Plan asks its questions one at a time.
func (c *poolCensus) askAgain(ctx context.Context) bool {
	answered := false
	c.againOnce.Do(func() {
		if c.fleet == nil || !c.wasRead() || len(c.unanswered) == 0 {
			return
		}
		ids := c.unanswered
		got := make([]FleetOrker, len(ids))
		probeTogether(len(ids), func(i int) {
			describeCtx, cancel := context.WithTimeout(ctx, orkerDescribeTimeout)
			defer cancel()
			described, err := c.fleet.DescribeOrkerByID(describeCtx, ids[i])
			got[i] = FleetOrker{Presence: OrkerPresence{Identity: ids[i]}, Described: described, Err: err}
		})
		c.unanswered = nil
		for _, orker := range got {
			if orker.Err != nil || orker.Described == nil {
				c.unanswered = append(c.unanswered, orker.Presence.Identity)
				continue
			}
			answered = true
			pool := orker.Described.GetPool()
			c.byPool[pool] = append(c.byPool[pool], orker)
			slices.SortFunc(c.byPool[pool], func(a, b FleetOrker) int {
				return strings.Compare(a.Presence.Identity, b.Presence.Identity)
			})
		}
	})
	return answered
}

// pausedForStoreOutage says a store outage alone holds this Orker's cordon, so
// it takes new tasks again once object storage answers: an operator's cordon
// always carries a reason, a stopping Orker never takes one again, and a
// profile cause has its own end.
func pausedForStoreOutage(o *orkstackv1.DescribeOrkerResult) bool {
	return o.GetStoreOutage() != nil && o.GetCordonReason() == "" && o.GetStopping() == nil &&
		len(o.GetProfileCordons()) == 0
}

// keepsPoolLive says every cause that holds this Orker's cordon keeps its pool
// live: a store outage, the Orker's own stop and an unavailable profile do; an
// operator's cordon, a profile drift and a profile cause this engine cannot
// read do not (AGENTS.md §7, Cordon).
func keepsPoolLive(o *orkstackv1.DescribeOrkerResult) bool {
	if o.GetCordonReason() != "" {
		return false
	}
	live := o.GetStoreOutage() != nil || o.GetStopping() != nil
	for _, cause := range o.GetProfileCordons() {
		if cause.GetCause() != orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
			return false
		}
		live = true
	}
	return live
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
