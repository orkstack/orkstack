package engine

import (
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// acqN is acq for the slot's ordinal n, sent as the protocol an ordinal
// needs (§22.14.1).
func acqN(run string, limit int, n int64, at time.Time) gateAcquire {
	a := acq(run, limit, gateStrategyQueue, at)
	a.N = n
	if n > 0 {
		a.Proto = gateProtoOrdinal
	} else {
		a.Proto = gateProtoLegacy
	}
	return a
}

// An acquire with n > 0 needs protocol 2; protocol 1 carries none, and an
// ordinal above the cap is refused.
func TestGateOrdinal_Validate(t *testing.T) {
	g := newTestGate()
	if err := g.validateAcquire(acqN("r", 1, 1, gateT0), gateT0); err != nil {
		t.Fatalf("proto 2 with n=1 refused: %v", err)
	}
	if err := g.validateAcquire(acqN("r", 1, 0, gateT0), gateT0); err != nil {
		t.Fatalf("proto 1 with n=0 refused: %v", err)
	}
	legacy := acqN("r", 1, 1, gateT0)
	legacy.Proto = gateProtoLegacy
	if err := g.validateAcquire(legacy, gateT0); err == nil {
		t.Fatal("proto 1 carrying an ordinal accepted")
	}
	over := acqN("r", 1, g.Tunables.OrdinalModulus, gateT0)
	if err := g.validateAcquire(over, gateT0); err == nil {
		t.Fatal("an ordinal at the modulus accepted")
	}
	// A gate started before ordinals carries no modulus: it takes n = 0 only.
	old := newTestGate()
	old.Tunables.OrdinalModulus = 0
	if err := old.validateAcquire(acqN("r", 1, 1, gateT0), gateT0); err == nil {
		t.Fatal("a gate without a modulus accepted an ordinal")
	}
}

// The ordinal wraps instead of running into a cap (decision D11): past the
// modulus it starts again at 0, which is newer than the modulus less one,
// so a wrapped re-acquire replaces the entry and passes the tombstone; an
// acquire half the modulus away is neither newer nor older, and is fenced.
func TestGateOrdinal_Wraps(t *testing.T) {
	g := newTestGate()
	m := g.Tunables.OrdinalModulus
	last := m - 1
	g.decideAcquire(acqN("a", 1, last, gateT0), gateT0, false)
	if d := g.decideAcquire(acqN("a", 1, 0, gateT0), gateT0, false); d.Answer.Answer != gateAnswerGranted || len(g.Holders) != 1 || g.Holders[0].N != 0 {
		t.Fatalf("the wrapped re-acquire = %+v, holders %+v; want it granted in place of n=%d", d.Answer, g.Holders, last)
	}
	if _, fenced := g.tombFor("a", 1, gateRunLevel, 0, gateT0); fenced {
		t.Fatalf("the tombstone at n=%d fences the wrapped n=0", last)
	}
	if _, fenced := g.tombFor("a", 1, gateRunLevel, last, gateT0); !fenced {
		t.Fatalf("the tombstone does not fence n=%d itself", last)
	}
	if d := g.decideAcquire(acqN("a", 1, m/2, gateT0), gateT0, false); d.Answer.Answer != gateAnswerFenced {
		t.Fatalf("an acquire half the modulus away = %+v, want fenced", d.Answer)
	}
	if d := g.decideAcquire(acqN("a", 1, last, gateT0), gateT0, false); d.Answer.Answer != gateAnswerFenced {
		t.Fatalf("the pre-wrap ordinal again = %+v, want fenced as older", d.Answer)
	}
}

// The worst-case carry with every ordinal at the largest the wire takes
// fits the bound, at the defaults.
func TestGateOrdinal_WorstCaseCarryFits(t *testing.T) {
	size, err := worstCaseGateBytes(DefaultGateTunables())
	if err != nil {
		t.Fatal(err)
	}
	if size > gateCarryBound {
		t.Fatalf("the worst-case carry with ordinals at %d is %d bytes, above the %d-byte bound", gateOrdinalWire-1, size, gateCarryBound)
	}
	if g := worstCaseGateState(DefaultGateTunables()); g.Holders[0].N != gateOrdinalWire-1 || g.Tombstones[0].N != gateOrdinalWire-1 {
		t.Fatal("the worst case does not set the ordinals at the largest")
	}
}

// acquire(n+1) before release(n) replaces the held entry in one decision:
// the holder count never holds both, a waiter behind it is admitted, and a
// late release(n) leaves the n+1 entry in place.
func TestGateOrdinal_ReplaceThenLateRelease(t *testing.T) {
	g := newTestGate()
	if d := g.decideAcquire(acqN("a", 1, 0, gateT0), gateT0, false); d.Answer.Answer != gateAnswerGranted {
		t.Fatalf("a's first acquire = %+v", d.Answer)
	}
	if d := g.decideAcquire(acqN("b", 1, 0, gateT0), gateT0.Add(time.Second), false); d.Answer.Answer != gateAnswerQueued {
		t.Fatalf("b = %+v, want queued", d.Answer)
	}
	// a gave its slot back and asks again with n=1: its held n=0 entry is
	// replaced, b (now first) is admitted in the same decision, which hands
	// it to the gate to signal, and a joins the tail.
	d := g.decideAcquire(acqN("a", 1, 1, gateT0.Add(2*time.Second)), gateT0.Add(2*time.Second), false)
	if d.Answer.Answer != gateAnswerQueued || d.Answer.Position != 1 || d.Answer.HoldersAhead != 1 {
		t.Fatalf("a's re-acquire = %+v, want queued first behind b's grant", d.Answer)
	}
	if len(d.Admitted) != 1 || d.Admitted[0].Run != "b" || !d.Admitted[0].GrantPending {
		t.Fatalf("admitted %+v, want b, its grant pending", d.Admitted)
	}
	if len(g.Holders) != 1 || entriesOf(g, "a") != 1 {
		t.Fatalf("holders %+v, queue %+v: want b holding and one entry of a", g.Holders, g.Queue)
	}
	// The late release of a's old ordinal frees nothing.
	g.release("a", 1, gateRunLevel, 0, gateT0.Add(3*time.Second))
	if len(g.Queue) != 1 || g.Queue[0].Run != "a" || g.Queue[0].N != 1 {
		t.Fatalf("a late release(0) removed the n=1 entry: queue %+v", g.Queue)
	}
	// release(1) frees it and fences n ≤ 1.
	g.release("a", 1, gateRunLevel, 1, gateT0.Add(4*time.Second))
	if entriesOf(g, "a") != 0 {
		t.Fatal("release(1) left a's entry")
	}
	for n, want := range map[int64]string{0: gateAnswerFenced, 1: gateAnswerFenced, 2: gateAnswerQueued} {
		if got := g.decideAcquire(acqN("a", 1, n, gateT0.Add(5*time.Second)), gateT0.Add(5*time.Second), true).Answer.Answer; got != want {
			t.Errorf("after release(1), acquire n=%d = %s, want %s", n, got, want)
		}
	}
}

// Attempts of one acquire carry one n: each matches its own entry, never
// queueing behind or fencing itself; an older ordinal than the entry's is
// fenced.
func TestGateOrdinal_RetrySameNAndOlderFenced(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acqN("a", 1, 0, gateT0), gateT0, false)
	g.decideAcquire(acqN("b", 1, 2, gateT0), gateT0, false)
	again := g.decideAcquire(acqN("b", 1, 2, gateT0.Add(time.Second)), gateT0.Add(time.Second), true)
	if again.Answer.Answer != gateAnswerQueued || again.Answer.Position != 1 || entriesOf(g, "b") != 1 {
		t.Fatalf("b's retry = %+v with %d entries; want its own place", again.Answer, entriesOf(g, "b"))
	}
	if older := g.decideAcquire(acqN("b", 1, 1, gateT0.Add(time.Second)), gateT0.Add(time.Second), true); older.Answer.Answer != gateAnswerFenced {
		t.Fatalf("an older ordinal = %+v, want fenced", older.Answer)
	}
	if held := g.decideAcquire(acqN("a", 1, 0, gateT0.Add(time.Second)), gateT0.Add(time.Second), true); held.Answer.Answer != gateAnswerGranted {
		t.Fatalf("a's retry = %+v, want granted", held.Answer)
	}
}

// A late release (older than the slot's entry) frees nothing and writes no
// tombstone; a release newer than the entry frees it, since the run moved
// past it and its own release or the acquire that replaced it was lost; and
// one half the modulus away frees nothing. release says which.
func TestGateOrdinal_ReleaseByOrdinal(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acqN("a", 1, 3, gateT0), gateT0, false)
	if g.release("a", 1, gateRunLevel, 2, gateT0) || entriesOf(g, "a") != 1 || len(g.Tombstones) != 0 {
		t.Fatalf("release(2) against n=3 changed the gate: holders %+v, tombstones %+v", g.Holders, g.Tombstones)
	}
	if !g.release("a", 1, gateRunLevel, 4, gateT0) || entriesOf(g, "a") != 0 {
		t.Fatalf("release(4) left the n=3 entry it moved past: holders %+v", g.Holders)
	}
	if _, fenced := g.tombFor("a", 1, gateRunLevel, 4, gateT0); !fenced {
		t.Fatal("release(4) does not fence n=4")
	}
	half := newTestGate()
	half.decideAcquire(acqN("a", 1, 3, gateT0), gateT0, false)
	if half.release("a", 1, gateRunLevel, 3+half.Tunables.OrdinalModulus/2, gateT0) || entriesOf(half, "a") != 1 {
		t.Fatal("a release half the modulus away freed the entry")
	}
	if g.release("a", 1, gateRunLevel, 4, gateT0) {
		t.Fatal("a second release reports a free")
	}
}

// A replacement says so, so the handler decides it as a new arrival after a
// pass.
func TestGateOrdinal_ReplacementIsReported(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acqN("a", 1, 0, gateT0), gateT0, false)
	if d := g.decideAcquire(acqN("a", 1, 1, gateT0), gateT0, false); !d.Replaced {
		t.Fatalf("a replacement = %+v, not reported", d)
	}
	if d := g.decideAcquire(acqN("a", 1, 1, gateT0), gateT0, false); d.Replaced {
		t.Fatal("the same ordinal again reported a replacement")
	}
}

// A carried state with an ordinal its modulus cannot hold is refused, as is
// any ordinal on a gate without a modulus.
func TestGateOrdinal_CheckOrdinals(t *testing.T) {
	g := newTestGate()
	g.Holders = []gateEntry{{Run: "a", N: g.Tunables.OrdinalModulus - 1}}
	if err := g.checkOrdinals(); err != nil {
		t.Fatalf("an ordinal within the modulus refused: %v", err)
	}
	g.Holders[0].N = g.Tunables.OrdinalModulus
	if err := g.checkOrdinals(); err == nil {
		t.Fatal("an ordinal at the modulus accepted")
	}
	old := newTestGate()
	old.Tunables.OrdinalModulus = 0
	old.Tombstones = []gateTomb{{Key: "k", N: 1}}
	if err := old.checkOrdinals(); err == nil {
		t.Fatal("an ordinal on a gate without a modulus accepted")
	}
}

// A tombstone raises its stored ordinal and never lowers it; a legacy one
// (no ordinal) fences n=0 only.
func TestGateOrdinal_TombstoneOrdinal(t *testing.T) {
	g := newTestGate()
	g.release("a", 1, gateRunLevel, 5, gateT0)
	g.release("a", 1, gateRunLevel, 2, gateT0)
	if len(g.Tombstones) != 1 || g.Tombstones[0].N != 5 {
		t.Fatalf("tombstones %+v, want one at ordinal 5", g.Tombstones)
	}
	legacy := newTestGate()
	legacy.Tombstones = []gateTomb{{Key: slotTombKey("a", 1, gateRunLevel), Until: gateT0.Add(time.Hour)}}
	if _, fenced := legacy.tombFor("a", 1, gateRunLevel, 0, gateT0); !fenced {
		t.Fatal("a legacy tombstone does not fence n=0")
	}
	if _, fenced := legacy.tombFor("a", 1, gateRunLevel, 1, gateT0); fenced {
		t.Fatal("a legacy tombstone fences n=1")
	}
}

// The carry keeps an entry's and a tombstone's ordinal, writes the legacy
// shape when there is none (so a history recorded before ordinals carries
// the same bytes), and reads both shapes.
func TestGateOrdinal_CarryShapes(t *testing.T) {
	plain := gateEntry{Run: "r", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue, Proto: 1, ArrivedAt: gateT0, Seq: 1}
	withN := plain
	withN.N = 7
	for _, e := range []gateEntry{plain, withN} {
		b, err := stablejson.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		var back gateEntry
		if err := back.UnmarshalJSON(b); err != nil || back.N != e.N {
			t.Fatalf("entry %s read back n=%d, %v; want %d", b, back.N, err, e.N)
		}
	}
	b, _ := stablejson.Marshal(plain)
	if parts, _ := gateParts(b, "entry", 13); len(parts) != 13 {
		t.Fatalf("an entry without an ordinal is written %s, not the legacy 13 fields", b)
	}
	tomb := gateTomb{Key: "0123456789abcdef", Until: gateT0, N: 4}
	tb, _ := stablejson.Marshal(tomb)
	var tback gateTomb
	if err := tback.UnmarshalJSON(tb); err != nil || tback.N != 4 {
		t.Fatalf("tombstone %s read back %+v, %v", tb, tback, err)
	}
}

func entriesOf(g *gateState, run string) int {
	n := 0
	for _, e := range append(append([]gateEntry(nil), g.Holders...), g.Queue...) {
		if e.Run == run {
			n++
		}
	}
	return n
}

// A replacement on a full key with a check due is answered at once, its
// entry in the line, while the pass it is due is still asked for: there is
// no wait between taking the old entry and writing the new one for a timed
// out Update to fall into, and its retry finds its own place.
func TestGateOrdinal_ReplacementIsQueuedBeforeAnyPass(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acqN("h", 1, 0, gateT0), gateT0, false)
	g.decideAcquire(acqN("a", 1, 0, gateT0), gateT0, false)
	late := gateT0.Add(g.Tunables.VerifyMinAge + time.Second) // h is old enough to check
	d := g.decideAcquire(acqN("a", 1, 1, late), late, false)
	if d.Answer.Answer != gateAnswerQueued || !d.WantsPass || !d.Replaced {
		t.Fatalf("the replacement = %+v; want queued at once, a pass asked for, and reported replaced", d)
	}
	g.decideAcquire(acqN("b", 1, 0, late.Add(time.Second)), late.Add(time.Second), true)
	retry := g.decideAcquire(acqN("a", 1, 1, late.Add(2*time.Second)), late.Add(2*time.Second), true)
	if retry.Answer.Answer != gateAnswerQueued || retry.Answer.Position != 1 {
		t.Fatalf("the replacement's retry = %+v; want its own place, first, ahead of b", retry.Answer)
	}
}
