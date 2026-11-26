package eventsource

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

const (
	older = "v1"
	newer = "v2"
)

func view(slots int, sources ...HandoverSource) HandoverView {
	return HandoverView{Current: newer, Routed: map[string]bool{newer: true}, Sources: sources,
		Hosts: []HandoverHost{{Orker: "new", Pool: "p", Release: newer, Usable: true, Slots: slots, Free: slots},
			{Orker: "old", Pool: "p", Release: older, Usable: true, Slots: 8, Free: 0},
			{Orker: "gone", Pool: "p", Release: older, Slots: 8, Free: 8}}}
}

// on is a source of the pool whose instances run on a host of its release.
func on(id, release string, n int) HandoverSource {
	host := "old"
	if release == newer {
		host = "new"
	}
	return HandoverSource{ID: id, Pool: "p", Open: true, Release: release, Wants: n, Activities: slices.Repeat([]HandoverActivity{{Orker: host, Running: true}}, n)}
}

func edit(s HandoverSource, f func(*HandoverSource)) HandoverSource {
	f(&s)
	return s
}

func TestPlanHandover(t *testing.T) {
	ran := func(orker string, running bool) func(*HandoverSource) {
		return func(s *HandoverSource) {
			for i := range s.Activities {
				s.Activities[i] = HandoverActivity{Orker: orker, Running: running}
			}
		}
	}
	nothing := func(s *HandoverSource) { s.Wants = 0 }
	for name, tc := range map[string]struct {
		view   HandoverView
		most   int
		tell   []string
		spoken int
	}{
		"two slots and three sources": {view: view(2, on("c", older, 1), on("a", older, 1), on("b", older, 1)), tell: []string{"a", "b"}},
		"none":                        {view: view(0, on("a", older, 1))},
		"what is on the current release is spoken for, running or not": {
			view: view(3, on("a", newer, 2), on("b", older, 1), on("c", older, 1)), tell: []string{"b"}, spoken: 2},
		"its replacements too":                                   {view: view(3, edit(on("a", newer, 2), func(s *HandoverSource) { s.Replacements = 1 }), on("b", older, 1)), spoken: 3},
		"one that crashes and comes back changes nothing":        {view: view(1, edit(on("a", newer, 1), func(s *HandoverSource) { s.Activities = nil }), on("b", older, 1)), spoken: 1},
		"one that closed asks for what it would be started with": {view: view(2, HandoverSource{ID: "a", Pool: "p", Wants: 2}, on("b", older, 1)), spoken: 2},
		"the first that does not fit ends the turn":              {view: view(2, on("a", older, 3), on("b", older, 1))},
		"one it fits behind does not jump it":                    {view: view(3, on("a", older, 2), on("b", older, 2), on("c", older, 1)), tell: []string{"a"}},
		"a paused one always fits":                               {view: view(0, edit(edit(on("a", older, 2), nothing), func(s *HandoverSource) { s.Activities = nil })), tell: []string{"a"}},
		"what wants nothing holds what it has":                   {view: view(2, edit(on("a", newer, 2), nothing), on("b", older, 1)), spoken: 2},
		"a pool that is spoken for twice over tells one that asks for nothing": {
			view: view(1, on("a", newer, 3), edit(edit(on("b", older, 1), nothing), func(s *HandoverSource) { s.Activities = nil })), tell: []string{"b"}, spoken: 3},

		// What of another release claims the current one's slots.
		"one that came here, and is told":      {view: view(1, edit(on("a", older, 1), ran("new", true))), tell: []string{"a"}, spoken: 1},
		"one that came here, between attempts": {view: view(1, edit(on("a", older, 1), ran("new", false)), on("b", older, 1)), tell: []string{"a"}, spoken: 1},
		"one on its way, its release full":     {view: view(1, edit(on("a", older, 1), ran("", false)), on("b", older, 1)), tell: []string{"a"}, spoken: 1},
		"one that its own release can take": {view: func() HandoverView {
			v := view(1, edit(on("a", older, 1), ran("", false)), on("b", older, 1))
			v.Hosts[1].Free = 1
			return v
		}(), tell: []string{"a"}},
		"one free slot hides one of two": {view: func() HandoverView {
			v := view(2, edit(on("a", older, 2), ran("", false)))
			v.Hosts[1].Free = 1
			return v
		}(), tell: []string{"a"}, spoken: 1},
		"one on a host that is gone":          {view: view(1, edit(on("a", older, 1), ran("gone", true)), on("b", older, 1)), tell: []string{"a"}, spoken: 1},
		"one on a host nobody knows":          {view: view(1, edit(on("a", older, 1), ran("?", true)), on("b", older, 1)), tell: []string{"a"}, spoken: 1},
		"one on a host of its own":            {view: view(1, on("a", older, 1)), tell: []string{"a"}},
		"a workflow no document accounts for": {view: func() HandoverView { v := view(1, on("a", older, 1)); v.Orphans = []HandoverOrphan{{"p", 1}}; return v }(), spoken: 1},
		"of another pool":                     {view: func() HandoverView { v := view(1, on("a", older, 1)); v.Orphans = []HandoverOrphan{{"q", 9}}; return v }(), tell: []string{"a"}},

		// Whose turn it is not.
		"one not seen":                          {view: view(9, on("a", older, 1), HandoverSource{ID: "b", Pool: "p", Unseen: true})},
		"one with a task pending":               {view: view(9, on("a", older, 1), edit(on("b", older, 1), func(s *HandoverSource) { s.TaskPending = true }), on("c", older, 1)), tell: []string{"a"}},
		"one that is blocked":                   {view: view(9, edit(on("a", older, 1), func(s *HandoverSource) { s.Blocked, s.TaskPending = true, true }), on("b", older, 1)), tell: []string{"b"}},
		"one that cannot be taken":              {view: view(9, edit(on("a", older, 1), func(s *HandoverSource) { s.CannotCarry = true }), on("b", older, 1)), tell: []string{"b"}},
		"one a ramp's release runs":             {view: func() HandoverView { v := view(9, on("a", older, 1)); v.Routed[older] = true; return v }()},
		"more than are told at once":            {view: view(9, on("a", older, 1), on("b", older, 1), on("c", older, 1)), most: 2, tell: []string{"a", "b"}},
		"a host that is not usable has no slot": {view: func() HandoverView { v := view(9, on("a", older, 1)); v.Hosts[0].Usable = false; return v }()},
	} {
		t.Run(name, func(t *testing.T) {
			most := tc.most
			if most == 0 {
				most = 10
			}
			got := PlanHandover(tc.view, most)
			if !slices.Equal(got.Tell, tc.tell) || got.Pools["p"].SpokenFor != tc.spoken {
				t.Errorf("told %v, spoken for %d; want %v, %d (%+v)", got.Tell, got.Pools["p"].SpokenFor, tc.tell, tc.spoken, got.Pools)
			}
		})
	}
	v := view(5, on("a", older, 2), edit(on("b", older, 3), func(s *HandoverSource) { s.Blocked = true }), edit(on("c", older, 4), func(s *HandoverSource) { s.CannotCarry = true }))
	if got := PlanHandover(v, 10).Pools["p"]; got != (HandoverPool{Slots: 5, Waiting: 2, Blocked: 3}) {
		t.Errorf("%+v", got)
	}
}

// TestPlanHandover_NeverPastTheRoom: whatever the walk saw, what is told fits
// beside everything already spoken for; every replica that sees the same tells
// the same; and what one told, seen again as closed or as come over, leaves no
// more room than was left.
func TestPlanHandover_NeverPastTheRoom(t *testing.T) {
	for seed := range uint64(3000) {
		r := rand.New(rand.NewPCG(seed, 1))
		v := HandoverView{Current: newer, Routed: map[string]bool{newer: true}}
		for i := range 1 + r.IntN(5) {
			release := []string{older, newer}[r.IntN(2)]
			slots := r.IntN(6)
			v.Hosts = append(v.Hosts, HandoverHost{Orker: fmt.Sprint("o", i), Pool: []string{"p", "q"}[r.IntN(2)], Release: release, Usable: r.IntN(4) > 0, Slots: slots, Free: r.IntN(slots + 1)})
		}
		for i := range r.IntN(12) {
			s := HandoverSource{ID: fmt.Sprintf("s%02d", i), Pool: []string{"p", "q"}[r.IntN(2)], Open: r.IntN(5) > 0, Release: []string{older, newer}[r.IntN(2)], Wants: r.IntN(4),
				Replacements: r.IntN(2), TaskPending: r.IntN(8) == 0, Blocked: r.IntN(8) == 0, CannotCarry: r.IntN(8) == 0, Unseen: r.IntN(40) == 0}
			for range r.IntN(5) {
				s.Activities = append(s.Activities, HandoverActivity{Orker: fmt.Sprint("o", r.IntN(6)), Running: r.IntN(2) == 0})
			}
			v.Sources = append(v.Sources, s)
		}
		got := PlanHandover(v, 10)
		shuffled := v
		shuffled.Sources = slices.Clone(v.Sources)
		r.Shuffle(len(shuffled.Sources), func(i, j int) { shuffled.Sources[i], shuffled.Sources[j] = shuffled.Sources[j], shuffled.Sources[i] })
		if again := PlanHandover(shuffled, 10); !slices.Equal(again.Tell, got.Tell) {
			t.Fatalf("seed %d: told %v and %v of the same", seed, got.Tell, again.Tell)
		}
		if len(got.Tell) > 10 {
			t.Fatalf("seed %d: told %d", seed, len(got.Tell))
		}
		// Told, each is seen next as closed, wanting what it wants.
		after := v
		after.Sources = slices.Clone(v.Sources)
		for i, s := range after.Sources {
			if slices.Contains(got.Tell, s.ID) {
				if s.Release == newer || !s.Open || s.Blocked || s.CannotCarry || s.Unseen || s.TaskPending {
					t.Fatalf("seed %d: told %+v", seed, s)
				}
				after.Sources[i] = HandoverSource{ID: s.ID, Pool: s.Pool, Wants: s.Wants}
			}
		}
		now := PlanHandover(after, 10)
		for pool, p := range now.Pools {
			before := got.Pools[pool]
			if p.SpokenFor > max(p.Slots, before.SpokenFor) {
				t.Fatalf("seed %d: pool %s of %d slots is spoken for %d, was %d, after telling %v\n%+v", seed, pool, p.Slots, p.SpokenFor, before.SpokenFor, got.Tell, v)
			}
		}
	}
}
