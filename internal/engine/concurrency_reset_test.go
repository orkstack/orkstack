package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// resetGate is a gate holding and queueing two runs each, with one tombstone.
func resetGate() *gateState {
	t0 := gateT0
	g := &gateState{Tunables: DefaultGateTunables(), Phase: gatePhaseActive, Seq: 40, Epoch: 3}
	g.Tunables.FreezeTimeout = time.Minute
	entry := func(run string, seq int64) gateEntry {
		return gateEntry{Run: run, TaskIx: gateRunLevel, SeqBase: 1, Limit: 2, Strategy: gateStrategyQueue,
			Proto: gateProto, ArrivedAt: t0, Seq: seq}
	}
	h1, h2 := entry("r1", 1), entry("r2", 2)
	h1.GrantedAt, h2.GrantedAt = t0, t0
	q1, q2 := entry("r3", 3), entry("r4", 4)
	q1.WaitUntil, q2.WaitUntil = t0.Add(time.Hour), t0.Add(time.Hour)
	g.Holders, g.Queue = []gateEntry{h1, h2}, []gateEntry{q1, q2}
	g.Tombstones = []gateTomb{{Key: slotTombKey("gone", 1, gateRunLevel), Until: t0.Add(time.Hour)}}
	return g
}

// resetReq is a reset request issued at, with a body digest of its own.
func resetReq(id string, issued time.Time) gateReset {
	return gateReset{ID: id, IssuedAt: issued, Body: "body-" + id}
}

// TestGateFreeze_StopsTheGateUntilItsTimeOrItsRequestEndsIt: a frozen gate
// takes no acquire and grants nothing, keeps taking releases, unfreezes by
// itself at its time, and is ended early only by the request that froze it.
func TestGateFreeze_StopsTheGateUntilItsTimeOrItsRequestEndsIt(t *testing.T) {
	now := gateT0
	a, b := resetReq("a", now), resetReq("b", now)
	g := resetGate()
	must.OK(t, g.freeze(a, now))
	if !g.frozen(now) || g.Frozen.ID != "a" {
		t.Fatalf("frozen %v by %q, want by a", g.frozen(now), g.Frozen.ID)
	}
	if err := g.validateAcquire(gateAcquire{Proto: 1, Run: "r9", SeqBase: 1, TaskIx: gateRunLevel, Limit: 2, Strategy: gateStrategyQueue, IssuedAt: now, WaitUntil: now.Add(time.Hour)}, now); err == nil {
		t.Fatal("a frozen gate took an acquire")
	}
	if got := g.admitHeads(now); len(got) != 0 {
		t.Fatalf("a frozen gate granted %v", got)
	}
	if !g.release("r1", 1, gateRunLevel, 0, now) || len(g.Holders) != 1 {
		t.Fatalf("a frozen gate did not take a release: %d holders", len(g.Holders))
	}
	// The same request asks again: one freeze, begun again.
	must.OK(t, g.freeze(a, now.Add(time.Second)))
	if want := now.Add(time.Second + g.Tunables.FreezeTimeout); !g.FrozenUntil.Equal(want) {
		t.Fatalf("frozen until %s, want the gate's own bound %s", g.FrozenUntil, want)
	}
	if err := g.freeze(b, now.Add(2*time.Second)); err == nil {
		t.Fatal("another request froze a frozen gate")
	}
	// The same id with another issued_at or body is refused, never a silent repeat.
	for _, other := range []gateReset{{ID: "a", IssuedAt: now.Add(time.Second), Body: "body-a"}, {ID: "a", IssuedAt: now, Body: "another"}} {
		if err := g.freeze(other, now.Add(2*time.Second)); err == nil || errors.As(err, new(*resetKnownError)) {
			t.Fatalf("the same id with another request froze the gate: %v", err)
		}
	}
	if g.unfreeze(b, now.Add(2*time.Second)) || !g.frozen(now.Add(2*time.Second)) {
		t.Fatal("a request that did not freeze the gate unfroze it")
	}
	if !g.unfreeze(a, now.Add(3*time.Second)) || g.frozen(now.Add(3*time.Second)) || g.Frozen.ID != "" {
		t.Fatalf("the request that froze it did not unfreeze it: %+v", g.FrozenUntil)
	}
	// Left alone, it ends by itself.
	must.OK(t, g.freeze(resetReq("c", now), now.Add(4*time.Second)))
	if g.frozen(now.Add(64 * time.Second)) {
		t.Fatal("the gate is still frozen past its time")
	}
	must.OK(t, g.freeze(resetReq("d", now), now.Add(65*time.Second)))
}

// TestGateFreeze_Refuses: what a gate cannot carry, what it applied already,
// what its own clock says is too old or not yet issued, and a gate that
// cannot know its predecessor's resets.
func TestGateFreeze_Refuses(t *testing.T) {
	now := gateT0
	window := DefaultGateTunables().ResetWindow
	for name, tc := range map[string]struct {
		req   gateReset
		at    time.Time
		setup func(*gateState)
		known bool
	}{
		"no id":                               {req: gateReset{IssuedAt: now, Body: "b"}},
		"no body":                             {req: gateReset{ID: "a", IssuedAt: now}},
		"no issued_at":                        {req: gateReset{ID: "a", Body: "b"}},
		"an id too long":                      {req: resetReq(strings.Repeat("x", ResetRequestIDMax+1), now)},
		"a body too long":                     {req: gateReset{ID: "a", IssuedAt: now, Body: strings.Repeat("x", resetBodyMax+1)}},
		"draining":                            {req: resetReq("a", now), setup: func(g *gateState) { g.Draining = true }},
		"applied already":                     {req: resetReq("a", now), setup: func(g *gateState) { g.noteReset(resetReq("a", now), now) }, known: true},
		"another request under an applied id": {req: gateReset{ID: "a", IssuedAt: now, Body: "other"}, setup: func(g *gateState) { g.noteReset(resetReq("a", now), now) }},
		"too old":                             {req: resetReq("a", now.Add(-window))},
		"too old a moment ago":                {req: resetReq("a", now.Add(-window)), at: now.Add(time.Nanosecond)},
		"issued in the future":                {req: resetReq("a", now.Add(resetClockAhead+time.Second))},
		"unknown predecessor":                 {req: resetReq("a", now), setup: func(g *gateState) { g.ResetsUnknownUntil = now.Add(time.Second) }},
		"no window pinned":                    {req: resetReq("a", now), setup: func(g *gateState) { g.Tunables.ResetWindow = 0 }},
		"full": {req: resetReq("new", now), setup: func(g *gateState) {
			for i := range gateResetMemory {
				g.noteReset(resetReq(fmt.Sprint(i), now), now)
			}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			g := resetGate()
			if tc.setup != nil {
				tc.setup(g)
			}
			at := now
			if !tc.at.IsZero() {
				at = tc.at
			}
			err := g.freeze(tc.req, at)
			var known *resetKnownError
			if err == nil || g.frozen(at) || errors.As(err, &known) != tc.known {
				t.Fatalf("freeze(%+v) = %v (known %v), frozen %v", tc.req, err, errors.As(err, &known), g.frozen(at))
			}
		})
	}
}

// TestGateFreeze_AcceptsWhatIsAllowed: one just inside the window, one a
// little ahead of the gate's clock, and one after the unknown has passed.
func TestGateFreeze_AcceptsWhatIsAllowed(t *testing.T) {
	now := gateT0
	window := DefaultGateTunables().ResetWindow
	for name, tc := range map[string]struct {
		req   gateReset
		setup func(*gateState)
	}{
		"just inside the window": {req: resetReq("a", now.Add(-window+time.Second))},
		"a little ahead":         {req: resetReq("a", now.Add(resetClockAhead))},
		"the unknown has passed": {req: resetReq("a", now), setup: func(g *gateState) { g.ResetsUnknownUntil = now }},
		"memory made room": {req: resetReq("a", now), setup: func(g *gateState) {
			for i := range gateResetMemory {
				g.noteReset(resetReq(fmt.Sprint(i), now.Add(-window)), now.Add(-window))
			}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			g := resetGate()
			if tc.setup != nil {
				tc.setup(g)
			}
			must.OK(t, g.freeze(tc.req, now))
		})
	}
}

// TestResetJudge_DropsOnlyWhatHasEndedOrMovedOn: a reset never drops a live
// entry of a run's current incarnation, whatever its record says or whether it
// could be checked; a pass's own missing-record rule does not apply (§10.1).
func TestResetJudge_DropsOnlyWhatHasEndedOrMovedOn(t *testing.T) {
	g := resetGate()
	findings := []gateRunFinding{
		{Run: "r1", Open: true, MemoSeq: 1}, // live, no record of its slot: kept
		{Run: "r2", Err: "the cluster did not answer"},
		{Run: "r3", NotFound: true},
		{Run: "r4", Open: true, MemoSeq: 2, Keys: map[int]bool{gateRunLevel: true}},
	}
	seeds, dropped := g.judgeReset(findings)
	if dropped != 2 || len(seeds.Holders) != 2 || len(seeds.Queue) != 0 {
		t.Fatalf("dropped %d, kept %d holders and %d queued; want 2 dropped, both holders kept, no queue", dropped, len(seeds.Holders), len(seeds.Queue))
	}
	for _, h := range seeds.Holders {
		if !h.VerifyPending {
			t.Errorf("seed %s is not marked for verification", h.Run)
		}
	}
	if len(g.Holders) != 2 || len(g.Queue) != 2 || g.Holders[0].VerifyPending {
		t.Fatal("judging a reset changed the gate it judged")
	}
	if _, dropped := resetGate().judgeReset(nil); dropped != 0 {
		t.Fatalf("a run nobody described dropped %d entries", dropped)
	}
	if seeds.Seq != 40 || seeds.Epoch != 3 || len(seeds.Tombstones) != 1 {
		t.Fatalf("seeds carry seq %d, epoch %d, %d tombstones", seeds.Seq, seeds.Epoch, len(seeds.Tombstones))
	}
}

// TestGateSeed_StartsAnEpochAndRestoreAddsNothingTwice: a seeded gate keeps
// every entry in its order, starts the next epoch and remembers the request,
// and the rebuild from the runs' own records adds no entry a second time.
func TestGateSeed_StartsAnEpochAndRestoreAddsNothingTwice(t *testing.T) {
	now := gateT0.Add(time.Minute)
	seeds, _ := resetGate().judgeReset(nil)
	g := &gateState{Tunables: DefaultGateTunables(), Phase: gatePhaseActive, Epoch: 1}
	g.seed(seeds, resetReq("a", now), now)
	if g.Epoch != 4 || g.Seq != 40 || !g.resetKnown("a", now) {
		t.Fatalf("epoch %d, seq %d, knows a %v; want 4, 40, true", g.Epoch, g.Seq, g.resetKnown("a", now))
	}
	g.restore([]gateRunRecord{{Run: "r1", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{
		gateRunLevel: memoEntry(t, gateMemoEntry{Proto: gateProto, Held: true, Limit: 2, Strategy: gateStrategyQueue, SeqBase: 1, Seq: 1})}}}, now)
	if len(g.Holders) != 2 || len(g.Queue) != 2 {
		t.Fatalf("%d holders and %d queued after the rebuild, want 2 and 2", len(g.Holders), len(g.Queue))
	}
	if got := []string{g.Queue[0].Run, g.Queue[1].Run}; !slices.Equal(got, []string{"r3", "r4"}) {
		t.Fatalf("line %v, want r3 then r4", got)
	}
}

// TestGateResets_AreRememberedUntilTheirWindow: an applied request is known
// until issued_at plus the window, by the gate's clock, forgotten at that
// instant, and refused by its age from then on; noted twice it is one entry.
func TestGateResets_AreRememberedUntilTheirWindow(t *testing.T) {
	now := gateT0
	window := DefaultGateTunables().ResetWindow
	g := resetGate()
	a := resetReq("a", now)
	g.noteReset(a, now)
	g.noteReset(a, now)
	if len(g.Resets) != 1 {
		t.Fatalf("a reset noted twice made %d entries", len(g.Resets))
	}
	if !g.resetKnown("a", now.Add(window-time.Nanosecond)) {
		t.Fatal("forgotten before its window")
	}
	if g.resetKnown("a", now.Add(window)) {
		t.Fatal("remembered at the end of its window")
	}
	// Past the window it never acts again: forgotten, and too old.
	if err := g.freeze(a, now.Add(window)); err == nil {
		t.Fatal("an id older than the window froze the gate")
	}
	g.noteReset(resetReq("b", now.Add(window)), now.Add(window))
	if len(g.Resets) != 1 || g.Resets[0].ID != "b" {
		t.Fatalf("remembers %v, want b alone: a's window has passed", g.Resets)
	}
}

// TestResetConsequence_Texts: each code's text, filled from a verdict, is
// exactly the spec's §10.1.
func TestResetConsequence_Texts(t *testing.T) {
	v := ResetVerdict{Key: "db:prod", Epoch: 3, Holders: 2, Queued: 5, Dropped: 1, Limit: 4, AcquireTimeout: 10 * time.Minute}
	for code, want := range map[string]string{
		ResetAnswering:        "Reset key db:prod (epoch 3): 2 running and 5 queued runs keep their slots and their order; 1 entries whose runs have ended or moved on are dropped. No run is stopped, restarted or moved in line. Runs that ask for this key during the reset wait, then ask the new record; a run that refuses or skips at this key and is not answered within 10m0s fails RUN_CONCURRENCY_UNAVAILABLE.",
		ResetAbsent:           "Key db:prod has no record. The reset starts a new one from the records of the runs holding or waiting for it, which keep their slots and their order.",
		ResetUnansweredForced: "The limit is not answering, so the runs it holds cannot all be known. The reset rebuilds it from the runs' own records; until every run that holds the key is found, up to 4 more runs than the limit (4) may run at once on key db:prod.",
	} {
		v.Code = code
		if got := ResetConsequence(v); got != want {
			t.Errorf("%s:\n%s\nwant\n%s", code, got, want)
		}
	}
}
