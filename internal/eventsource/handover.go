package eventsource

import (
	"cmp"
	"maps"
	"slices"
)

// HandoverHost is one source host as the inventory shows it: Slots is how
// many instances it runs at once, and Free how many more it would take.
type HandoverHost struct {
	Orker   string
	Pool    string
	Release string
	// Usable: live, fit and not cordoned.
	Usable      bool
	Slots, Free int
}

// HandoverActivity is an activity a describe shows: taken by an Orker, the
// last one there was, and running or between attempts.
type HandoverActivity struct {
	Orker   string
	Running bool
}

// HandoverSource is one source of a walk, as its describe and its document
// show it.
type HandoverSource struct {
	ID   string
	Pool string
	// Open: its workflow runs, on Release. One that is not asks for what it
	// would be started with.
	Open    bool
	Release string
	// Wants is how many instances it asks for, none where its document says
	// paused, held or deleted; Replacements is how many more are scheduled.
	Wants, Replacements int
	Activities          []HandoverActivity
	// Unseen: it could not be described, so what its pool is spoken for is
	// not known.
	Unseen bool
	// TaskPending: it has not read what it was last told. Blocked: its
	// execution cannot run. CannotCarry: the current release cannot take it.
	TaskPending, Blocked, CannotCarry bool
}

// HandoverOrphan is an open workflow that no document accounts for.
type HandoverOrphan struct {
	Pool      string
	Instances int
}

// HandoverView is what one walk knows, all read before it tells anybody.
type HandoverView struct {
	// Current is the release new work is sent to; Routed are it and one being
	// ramped.
	Current string
	Routed  map[string]bool
	Hosts   []HandoverHost
	Sources []HandoverSource
	Orphans []HandoverOrphan
}

// HandoverPool is one pool as a walk counts it: its instance slots on the
// current release, what they are spoken for, what the sources that wait to be
// handed over need, and what those that are blocked do.
type HandoverPool struct {
	Slots, SpokenFor, Waiting, Blocked int
	// Unknown: a source of it could not be seen.
	Unknown bool
}

// Handover is what is told, in order, and each pool as it was counted before
// anything was.
type Handover struct {
	Tell  []string
	Pools map[string]HandoverPool
}

// PlanHandover spends each pool's free slots on the sources that wait to be
// handed over, in the order of their ids: the first that does not fit ends its
// pool's turn, so two replicas that see the same tell the same. most bounds
// how many are told.
func PlanHandover(v HandoverView, most int) Handover {
	b := budget{view: v, pools: map[string]HandoverPool{}, counted: map[string]int{}, hosts: map[string]HandoverHost{}}
	b.sources = slices.Clone(v.Sources)
	slices.SortFunc(b.sources, func(x, y HandoverSource) int { return cmp.Compare(x.ID, y.ID) })
	b.count()
	out := Handover{Pools: map[string]HandoverPool{}}
	for _, pool := range slices.Sorted(maps.Keys(b.pools)) {
		p := b.pools[pool]
		left := p.Slots - p.SpokenFor
		for _, s := range b.sources {
			if s.Pool == pool && b.waits(s) {
				if s.Blocked {
					p.Blocked += b.cost(s)
				} else if !s.CannotCarry {
					p.Waiting += b.cost(s)
				}
			}
		}
		out.Pools[pool] = p
		for _, s := range b.sources {
			if s.Pool != pool || !b.waits(s) || s.Blocked || s.CannotCarry {
				continue
			}
			if p.Unknown || s.TaskPending || b.cost(s) > max(left, 0) || len(out.Tell) >= most {
				break
			}
			left -= b.cost(s)
			out.Tell = append(out.Tell, s.ID)
		}
	}
	return out
}

type budget struct {
	view    HandoverView
	sources []HandoverSource
	pools   map[string]HandoverPool
	// counted is how many of a source's own activities are counted already.
	counted map[string]int
	hosts   map[string]HandoverHost
}

// waits reports whether the source is on a release no longer routed to.
func (b *budget) waits(s HandoverSource) bool {
	return s.Open && !s.Unseen && !b.view.Routed[s.Release]
}

// cost is what handing the source over takes of the budget: what it wants,
// less what of it is counted already.
func (b *budget) cost(s HandoverSource) int { return max(s.Wants-b.counted[s.ID], 0) }

func (b *budget) speak(pool string, n int) {
	p := b.pools[pool]
	p.SpokenFor += n
	b.pools[pool] = p
}

func (b *budget) count() {
	type of struct{ release, pool string }
	free := map[of]int{}
	for _, h := range b.view.Hosts {
		b.hosts[h.Orker] = h
		if !h.Usable {
			continue
		}
		free[of{h.Release, h.Pool}] += max(h.Free, 0)
		if h.Release == b.view.Current {
			p := b.pools[h.Pool]
			p.Slots += max(h.Slots, 0)
			b.pools[h.Pool] = p
		}
	}
	for _, o := range b.view.Orphans {
		b.speak(o.Pool, o.Instances)
	}
	for _, s := range b.sources {
		p := b.pools[s.Pool]
		p.Unknown = p.Unknown || s.Unseen
		b.pools[s.Pool] = p
		switch {
		case s.Unseen:
		case !s.Open:
			b.speak(s.Pool, s.Wants)
		case s.Release == b.view.Current:
			b.speak(s.Pool, max(s.Wants+s.Replacements, b.holds(s)))
		default:
			for _, a := range s.Activities {
				host, known := b.hosts[a.Orker]
				switch {
				case known && host.Release == b.view.Current:
					// It is here, and stays while it is retried.
				case !a.Running:
					// It is on its way where its own release has no room left.
					if f := (of{s.Release, s.Pool}); free[f] > 0 {
						free[f]--
						continue
					}
				case known && host.Usable:
					continue
				}
				b.counted[s.ID]++
				b.speak(s.Pool, 1)
			}
		}
	}
}

// holds is what a source that wants nothing still holds: what it wants is
// dropped, and nothing else of it.
func (b *budget) holds(s HandoverSource) int {
	if s.Wants > 0 {
		return 0
	}
	return len(s.Activities)
}
