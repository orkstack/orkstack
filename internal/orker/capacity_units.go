package orker

import (
	"context"
	"errors"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ReasonGPUUnitsUnavailable fails an attempt whose gpu units could not be
// bound. Every polled task's units are reserved when it is polled, so a
// shortfall at bind is an invariant failure; it is otherwise reached through
// a drifted or disabled gpu profile.
const ReasonGPUUnitsUnavailable = "GPU_UNITS_UNAVAILABLE"

// recoveryPasses is how many consecutive healthy probes, at recovery_interval,
// bring a failed unit back into the usable set.
const recoveryPasses = 3

// errProbeSkipped is probeUnit's answer for a unit it may not probe this
// interval: bound to a task or a seeded container, or short of count because
// tasks hold it. A probe never waits for a bound unit.
var errProbeSkipped = errors.New("the unit is bound; it is probed next interval")

// gpuUnits is the unit side of the ledger: which of the gpu profile's units
// are usable and who holds each. It is guarded by Capacity.mu, and every
// method is safe on a nil *gpuUnits (an Orker with no gpu profile), where it
// does nothing.
//
// The counts live in Capacity's amounts: limit.gpu is the usable count,
// ceiling.gpu min(max_gpu, units) fixed at boot, and reserved.gpu every
// permit's ceiling, every admission's units, every seeded unit and every
// probe of a usable unit. Health flips only in endProbe, under the mutex, so
// the free usable units always cover what is reserved and bind never falls
// short.
type gpuUnits struct {
	profile string
	usable  []bool
	passes  []int // consecutive healthy probes of a unit outside the usable set
	// owner is who holds each unit: an *Admission, a *unitProbe or the
	// ContainerID of an unremoved container; nil is free.
	owner []any
	// probeDue counts probes waiting for unclaimed polls to drain; while it
	// is above zero no poll is granted (R11b2-H1).
	probeDue int
	// onHealth hears every change of the usable count, under the mutex, in
	// the step that flips it, so a profile's UNITS_BELOW_CEILING and
	// ALL_UNITS_UNHEALTHY are set and cleared exactly with the limit. It must
	// not call back into the ledger.
	onHealth func(usable, ceiling int)
}

// useUnits gives the ledger n units of profile, every one usable, and the
// fixed ceiling min(maxGPU, n). Called once, before the first poll.
func (c *Capacity) useUnits(profile string, n, maxGPU int, onHealth func(usable, ceiling int)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	usable := make([]bool, n)
	for i := range usable {
		usable[i] = true
	}
	c.units = &gpuUnits{profile: profile, usable: usable, passes: make([]int, n), owner: make([]any, n), onHealth: onHealth}
	c.limit.gpu = int64(n)
	c.ceiling.gpu = int64(max(0, min(maxGPU, n)))
}

// probePending reports whether a due probe holds new grants. Locked.
func (u *gpuUnits) probePending() bool { return u != nil && u.probeDue > 0 }

// unbind frees each of indices that by holds. Locked.
func (u *gpuUnits) unbind(indices []int, by any) {
	if u == nil {
		return
	}
	for _, i := range indices {
		if u.owner[i] == by {
			u.owner[i] = nil
		}
	}
}

// rebind hands each of indices that from holds to to. Locked.
func (u *gpuUnits) rebind(indices []int, from, to any) {
	if u == nil {
		return
	}
	for _, i := range indices {
		if u.owner[i] == from {
			u.owner[i] = to
		}
	}
}

func (u *gpuUnits) usableCount() int {
	n := 0
	for _, ok := range u.usable {
		if ok {
			n++
		}
	}
	return n
}

// bind gives a its units: the lowest free usable indices, all or nothing. It
// never waits. A shortfall is an invariant failure (every polled task's units
// were reserved at poll), reported as GPU_UNITS_UNAVAILABLE; the caller logs
// it as one and the attempt's Done frees the reservation.
func (c *Capacity) bind(a *Admission) error {
	want := int(a.held.gpu)
	if want == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	u := c.units
	if u == nil {
		return Fail(ReasonGPUUnitsUnavailable,
			"the task asks for %d gpu units and this Orker offers no gpu profile", want)
	}
	picked := make([]int, 0, want)
	for i := range u.owner {
		if len(picked) == want {
			break
		}
		if u.usable[i] && u.owner[i] == nil {
			picked = append(picked, i)
		}
	}
	if len(picked) < want {
		return Fail(ReasonGPUUnitsUnavailable,
			"invariant failure: the task's %d %s units were reserved when it was polled, but only %d usable units are free",
			want, u.profile, len(picked))
	}
	for _, i := range picked {
		u.owner[i] = a
	}
	a.units = picked
	return nil
}

// Units is the indices bind gave the admission, ascending.
func (a *Admission) Units() []int {
	if a == nil {
		return nil
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	return append([]int(nil), a.units...)
}

// seedUnits reserves and binds indices for a container the lock-scoped reap
// could not remove, merging them into the entry the by-id pass may already
// hold for it; a later successful removal frees the count and the indices
// together. An index out of range, or already held, is not counted: the
// caller seeds every unit of the profile for a label it cannot read.
func (c *Capacity) seedUnits(id ContainerID, indices []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	u := c.units
	if u == nil {
		return
	}
	hold := c.unremoved[id]
	for _, i := range indices {
		if i < 0 || i >= len(u.owner) || u.owner[i] != nil {
			continue
		}
		u.owner[i] = id
		hold.units = append(hold.units, i)
		hold.held.gpu++
		c.reserved.gpu++
	}
	c.unremoved[id] = hold
	c.broadcast()
}

// unremovedHold is what an unremoved container still holds: its reservation
// and its unit indices.
type unremovedHold struct {
	held  amount
	units []int
}

// unitProbe is one probe's hold on a unit.
type unitProbe struct {
	index   int
	counted bool // a probe of a usable unit holds one unit of count
}

// probeUnit takes unit i for a probe, never through an SDK slot or a permit.
// A bound unit is skipped (errProbeSkipped). A unit outside the usable set is
// taken with no count, because the limit does not include it. A usable unit
// takes one unit of count within the limit; short only because granted polls
// carry no task yet, the probe is due: no poll is granted until those drain,
// which the poll timeout bounds. Short because tasks hold units, it is
// skipped. Taking is one step under the mutex, so no grant can be promised a
// unit a probe holds. The caller must endProbe what it gets.
func (c *Capacity) probeUnit(ctx context.Context, i int) (*unitProbe, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	u := c.units
	if u == nil || i < 0 || i >= len(u.owner) {
		return nil, errProbeSkipped
	}
	due := false
	defer func() {
		if due {
			u.probeDue--
			c.broadcast()
		}
	}()
	for {
		if u.owner[i] != nil {
			return nil, errProbeSkipped
		}
		if !u.usable[i] {
			p := &unitProbe{index: i}
			u.owner[i] = p
			return p, nil
		}
		if c.reserved.gpu+1 <= c.limit.gpu {
			p := &unitProbe{index: i, counted: true}
			u.owner[i] = p
			c.reserved.gpu++
			return p, nil
		}
		unclaimed := int64(c.issued-c.claimed) * c.ceiling.gpu
		if c.reserved.gpu-unclaimed+1 > c.limit.gpu {
			return nil, errProbeSkipped
		}
		if !due {
			due = true
			u.probeDue++
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// endProbe releases the probe's unit and, in the same step under the mutex,
// flips the unit's health: a failing usable unit leaves the usable set at
// once; a unit outside it returns after recoveryPasses consecutive healthy
// probes. A task's outcome never reaches here.
func (c *Capacity) endProbe(p *unitProbe, healthy bool) {
	if p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	u := c.units
	if u.owner[p.index] == p {
		u.owner[p.index] = nil
	}
	if p.counted {
		c.reserved.gpu--
	}
	flipped := false
	switch {
	case u.usable[p.index] && !healthy:
		u.usable[p.index], u.passes[p.index] = false, 0
		c.limit.gpu--
		flipped = true
	case !u.usable[p.index] && !healthy:
		u.passes[p.index] = 0
	case !u.usable[p.index]:
		u.passes[p.index]++
		if u.passes[p.index] >= recoveryPasses {
			u.usable[p.index], u.passes[p.index] = true, 0
			c.limit.gpu++
			flipped = true
		}
	}
	if flipped && u.onHealth != nil {
		u.onHealth(u.usableCount(), int(c.ceiling.gpu))
	}
	c.broadcast()
}

// unitHealthReporter is the ledger's onHealth for profile: it holds the
// profile unavailable UNITS_BELOW_CEILING while fewer units are usable than
// the per-task cap every poll reserves, so the Orker polls for no task and
// says why, and ALL_UNITS_UNHEALTHY while none is. Both are unavailable
// causes: the pool stays live, and submit's fit check, which reads the fixed
// cap, does not refuse. It runs under the ledger's mutex and takes only the
// profile state's, never the ledger's.
func unitHealthReporter(s *profileState, profile string, now func() time.Time) func(usable, ceiling int) {
	const (
		below = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNITS_BELOW_CEILING
		none  = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY
	)
	return func(usable, ceiling int) {
		if usable == 0 {
			s.setUnavailable(profile, none, "no unit of the profile passes its probe", now())
		} else {
			s.clearUnavailable(profile, none)
		}
		if usable < ceiling {
			s.setUnavailable(profile, below, fmt.Sprintf(
				"%d of %d units usable, and every poll reserves %d: the Orker polls for no task until enough are back",
				usable, ceiling, ceiling), now())
		} else {
			s.clearUnavailable(profile, below)
		}
	}
}

// abandonProbe releases a probe that judged nothing (the Orker stopped while
// it ran): its unit and count are freed and its health is unchanged.
func (c *Capacity) abandonProbe(p *unitProbe) {
	if p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.units.owner[p.index] == p {
		c.units.owner[p.index] = nil
	}
	if p.counted {
		c.reserved.gpu--
	}
	c.broadcast()
}

// unitUsable reports whether unit i is in the usable set.
func (c *Capacity) unitUsable(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.units != nil && i >= 0 && i < len(c.units.usable) && c.units.usable[i]
}
