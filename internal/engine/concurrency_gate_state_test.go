package engine

import (
	"encoding/json/v2"
	"errors"
	"github.com/orkstack/orkstack/internal/stablejson"
	"reflect"
	"strings"
	"testing"
	"time"
)

var gateT0 = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func newTestGate() *gateState {
	return &gateState{Tunables: DefaultGateTunables(), Phase: gatePhaseActive}
}

func acq(run string, limit int, strategy string, at time.Time) gateAcquire {
	return gateAcquire{Proto: gateProto, Run: run, SeqBase: 1, TaskIx: gateRunLevel, Limit: limit,
		Strategy: strategy, Label: "deploy", IssuedAt: at, WaitUntil: at.Add(time.Hour)}
}

func TestGateID_IsStableAndSeparatesItsParts(t *testing.T) {
	a := GateID("payments", "2026-01-01T00:00:00Z", "deploy-prod")
	if a != GateID("payments", "2026-01-01T00:00:00Z", "deploy-prod") || len(a) != len("ork-conc-")+32 {
		t.Fatalf("GateID = %q, want a stable ork-conc-<32 hex>", a)
	}
	// The NUL separator keeps "ab"+"c" apart from "a"+"bc".
	if GateID("ab", "", "c") == GateID("a", "", "bc") {
		t.Fatal("two different (namespace, key) pairs share a gate id")
	}
	if GateID("payments", "2026-01-01T00:00:00Z", "k") == GateID("payments", "2026-02-01T00:00:00Z", "k") {
		t.Fatal("a namespace created again shares its predecessor's gate")
	}
}

func TestGateValidateAcquire(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate    func(*gateState, *gateAcquire)
		protocol  bool
		retryable bool
	}{
		"ok":                {mutate: func(*gateState, *gateAcquire) {}},
		"newer protocol":    {mutate: func(_ *gateState, a *gateAcquire) { a.Proto = gateProtoOrdinal + 1 }, protocol: true},
		"no protocol":       {mutate: func(_ *gateState, a *gateAcquire) { a.Proto = 0 }, protocol: true},
		"cancel_oldest":     {mutate: func(_ *gateState, a *gateAcquire) { a.Strategy = gateStrategyCancelOldest }, protocol: true},
		"replace":           {mutate: func(_ *gateState, a *gateAcquire) { a.Strategy = gateStrategyReplace }, protocol: true},
		"unknown strategy":  {mutate: func(_ *gateState, a *gateAcquire) { a.Strategy = "lifo" }, protocol: true},
		"no run":            {mutate: func(_ *gateState, a *gateAcquire) { a.Run = "" }, protocol: true},
		"zero limit":        {mutate: func(_ *gateState, a *gateAcquire) { a.Limit = 0 }, protocol: true},
		"no incarnation":    {mutate: func(_ *gateState, a *gateAcquire) { a.SeqBase = 0 }, protocol: true},
		"bad task index":    {mutate: func(_ *gateState, a *gateAcquire) { a.TaskIx = -2 }, protocol: true},
		"run id too long":   {mutate: func(_ *gateState, a *gateAcquire) { a.Run = strings.Repeat("r", gateWorstRunIDBytes+1) }, protocol: true},
		"template too long": {mutate: func(_ *gateState, a *gateAcquire) { a.Label = strings.Repeat("<", gateWorstLabelBytes/6+1) }, protocol: true},
		"draining":          {mutate: func(g *gateState, _ *gateAcquire) { g.Draining = true }, retryable: true},
		"frozen":            {mutate: func(g *gateState, _ *gateAcquire) { g.FrozenUntil = gateT0.Add(time.Minute) }, retryable: true},
		"freeze has passed": {mutate: func(g *gateState, _ *gateAcquire) { g.FrozenUntil = gateT0 }},
	} {
		t.Run(name, func(t *testing.T) {
			g, a := newTestGate(), acq("r1", 1, gateStrategyQueue, gateT0)
			tc.mutate(g, &a)
			err := g.validateAcquire(a, gateT0)
			var rej *gateRejection
			if !tc.protocol && !tc.retryable {
				if err != nil {
					t.Fatalf("rejected %v, want accepted", err)
				}
				return
			}
			if !errors.As(err, &rej) || rej.Protocol != tc.protocol || rej.Retryable != tc.retryable {
				t.Fatalf("validateAcquire = %#v, want protocol=%v retryable=%v", err, tc.protocol, tc.retryable)
			}
		})
	}
}

func TestGateDecideAcquire_GrantsQueuesAndRefuses(t *testing.T) {
	g := newTestGate()
	if d := g.decideAcquire(acq("r1", 1, gateStrategyQueue, gateT0), gateT0, false); d.Answer.Answer != gateAnswerGranted || !d.Granted {
		t.Fatalf("first acquire = %+v, want granted", d)
	}
	// The same acquire again is answered from the holders.
	if d := g.decideAcquire(acq("r1", 1, gateStrategyQueue, gateT0), gateT0, false); d.Answer.Answer != gateAnswerGranted || d.Granted {
		t.Fatalf("repeated acquire = %+v, want granted without a new grant", d)
	}
	d := g.decideAcquire(acq("r2", 1, gateStrategyQueue, gateT0), gateT0, false)
	if d.Answer.Answer != gateAnswerQueued || d.Answer.Position != 1 || d.Answer.HoldersAhead != 1 {
		t.Fatalf("second acquire = %+v, want queued first behind one holder", d)
	}
	for _, s := range []string{gateStrategyRefuse, gateStrategySkip} {
		if d := g.decideAcquire(acq("r-"+s, 1, s, gateT0), gateT0, false); d.Answer.Answer != gateAnswerFull {
			t.Fatalf("%s while full = %+v, want full and never queued", s, d)
		}
	}
	if len(g.Queue) != 1 {
		t.Fatalf("queue %v, want r2 alone", g.Queue)
	}
}

func TestGateDecideAcquire_StaleAndTombstoned(t *testing.T) {
	g := newTestGate()
	old := acq("r1", 1, gateStrategyQueue, gateT0.Add(-g.Tunables.AcquireTimeout-g.Tunables.ClockGrace-time.Second))
	if d := g.decideAcquire(old, gateT0, false); d.Answer.Answer != gateAnswerFenced {
		t.Fatalf("stale acquire = %+v, want fenced", d)
	}
	g.decideAcquire(acq("r1", 1, gateStrategyQueue, gateT0), gateT0, false)
	g.release("r1", 1, gateRunLevel, 0, gateT0)
	if d := g.decideAcquire(acq("r1", 1, gateStrategyQueue, gateT0), gateT0.Add(time.Second), false); d.Answer.Answer != gateAnswerFenced {
		t.Fatalf("acquire after its release = %+v, want fenced by the slot tombstone", d)
	}
	// Past the tombstone's life the same slot is welcome again.
	later := gateT0.Add(g.Tunables.tombstoneLife() + time.Second)
	if d := g.decideAcquire(acq("r1", 1, gateStrategyQueue, later), later, false); d.Answer.Answer != gateAnswerGranted {
		t.Fatalf("acquire past the tombstone = %+v, want granted", d)
	}
}

func TestGateReleaseAll_FencesOnlyEarlierIncarnations(t *testing.T) {
	g := newTestGate()
	a := acq("r1", 3, gateStrategyQueue, gateT0)
	g.decideAcquire(a, gateT0, false)
	g.releaseAll("r1", 2, gateT0)
	if !g.empty() {
		t.Fatalf("holders %v after release_all below 2, want none", g.Holders)
	}
	if d := g.decideAcquire(a, gateT0, false); d.Answer.Answer != gateAnswerFenced {
		t.Fatalf("seq_base 1 after release_all{below: 2} = %+v, want fenced", d)
	}
	next := a
	next.SeqBase = 2
	if d := g.decideAcquire(next, gateT0, false); d.Answer.Answer != gateAnswerGranted {
		t.Fatalf("seq_base 2 after release_all{below: 2} = %+v, want granted", d)
	}
}

func TestGateAdmitHeads_InLineOrderAndStrictestLimit(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acq("r1", 2, gateStrategyQueue, gateT0), gateT0, false)
	// r2 declares limit 1, and one holder already holds the key: it queues.
	g.decideAcquire(acq("r2", 1, gateStrategyQueue, gateT0), gateT0, false)
	g.decideAcquire(acq("r3", 2, gateStrategyQueue, gateT0), gateT0, false)
	if len(g.Holders) != 1 || len(g.Queue) != 2 {
		t.Fatalf("holders %d queue %d, want 1 and 2: r2's limit of 1 is full", len(g.Holders), len(g.Queue))
	}
	g.release("r1", 1, gateRunLevel, 0, gateT0)
	granted := g.admitHeads(gateT0)
	if len(granted) != 1 || granted[0].Run != "r2" {
		t.Fatalf("granted %v, want r2 alone: it holds the key at limit 1, so r3 waits", granted)
	}
	g.release("r2", 1, gateRunLevel, 0, gateT0)
	if granted := g.admitHeads(gateT0); len(granted) != 1 || granted[0].Run != "r3" {
		t.Fatalf("granted %v, want r3", granted)
	}
}

func TestGateDecideAcquire_RebuildAndSettle(t *testing.T) {
	g := newTestGate()
	g.Rebuild = gateRebuild{State: gateRebuildListing, Since: gateT0}
	if d := g.decideAcquire(acq("r1", 1, gateStrategyRefuse, gateT0), gateT0, false); d.Answer.Answer != gateAnswerUnavailable {
		t.Fatalf("refuse while rebuilding = %+v, want unavailable", d)
	}
	if d := g.decideAcquire(acq("r2", 1, gateStrategyQueue, gateT0), gateT0, false); d.Answer.Answer != gateAnswerQueued {
		t.Fatalf("queue while rebuilding = %+v, want queued, nothing granted", d)
	}
	g.Rebuild = gateRebuild{State: gateRebuildSettling, Since: gateT0}
	if granted := g.admitHeads(gateT0.Add(time.Second)); len(granted) != 0 {
		t.Fatalf("granted %v during the settle", granted)
	}
	if granted := g.admitHeads(gateT0.Add(g.Tunables.RebuildSettle)); len(granted) != 1 {
		t.Fatalf("granted %v after the settle, want r2", granted)
	}
}

func TestGateDecideAcquire_ReassertKeepsItsPlaceAndIgnoresTheBound(t *testing.T) {
	g := newTestGate()
	g.Tunables.MaxEntries = 2
	g.decideAcquire(acq("holder", 1, gateStrategyQueue, gateT0), gateT0, false)
	first := g.decideAcquire(acq("early", 1, gateStrategyQueue, gateT0), gateT0, false).Answer
	if d := g.decideAcquire(acq("late", 1, gateStrategyQueue, gateT0), gateT0.Add(time.Second), false); d.Answer.Answer != gateAnswerFullBound {
		t.Fatalf("a new arrival at max_entries = %+v, want full_bound", d)
	}
	// The gate lost its line; "early" re-asserts with its first answer.
	g.Queue = nil
	g.decideAcquire(acq("later", 1, gateStrategyQueue, gateT0), gateT0.Add(2*time.Second), false)
	re := acq("early", 1, gateStrategyQueue, gateT0.Add(3*time.Second))
	re.ArrivedAt, re.Seq = first.ArrivedAt, first.Seq
	d := g.decideAcquire(re, gateT0.Add(3*time.Second), false)
	if d.Answer.Answer != gateAnswerQueued || d.Answer.Position != 1 || len(g.Holders)+len(g.Queue) != 3 {
		t.Fatalf("re-assert = %+v (%d entries), want queued ahead of the later arrival, past max_entries", d, len(g.Holders)+len(g.Queue))
	}
}

func TestGateDecideAcquire_FullWithACheckDueAsksForAPass(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acq("r1", 1, gateStrategyQueue, gateT0), gateT0, false)
	now := gateT0.Add(g.Tunables.VerifyMinAge + time.Second)
	req := acq("r2", 1, gateStrategyRefuse, now)
	before := len(g.Queue)
	if d := g.decideAcquire(req, now, false); !d.WantsPass || len(g.Queue) != before {
		t.Fatalf("full with an old holder = %+v, want a pass requested and nothing changed", d)
	}
	if d := g.decideAcquire(req, now, true); d.WantsPass || d.Answer.Answer != gateAnswerFull {
		t.Fatalf("after the pass = %+v, want full", d)
	}
	g.LastPassAt = now
	if d := g.decideAcquire(req, now.Add(time.Second), false); d.WantsPass {
		t.Fatal("a pass was requested within verify_debounce of the last one")
	}
}

func TestGateTombstones_EvictionDegradesTheFence(t *testing.T) {
	g := newTestGate()
	g.Tunables.MaxTombstones = 2
	for i, run := range []string{"a", "b", "c"} {
		g.release(run, 1, gateRunLevel, 0, gateT0.Add(time.Duration(i)*time.Second))
	}
	if len(g.Tombstones) != 2 || !g.FenceDegradedUntil.Equal(gateT0.Add(g.Tunables.tombstoneLife())) {
		t.Fatalf("tombstones %d, degraded until %v; want 2 and a's expiry", len(g.Tombstones), g.FenceDegradedUntil)
	}
	d := g.decideAcquire(acq("d", 1, gateStrategyQueue, gateT0.Add(5*time.Second)), gateT0.Add(5*time.Second), false)
	if !d.Granted || !g.Holders[0].VerifyPending {
		t.Fatalf("a grant while fence-degraded = %+v, holder %+v; want it verify_pending", d, g.Holders)
	}
}

func TestGateDropExpiredQueued(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acq("h", 1, gateStrategyQueue, gateT0), gateT0, false)
	short := acq("q", 1, gateStrategyQueue, gateT0)
	short.WaitUntil = gateT0.Add(time.Minute)
	g.decideAcquire(short, gateT0, false)
	g.dropExpiredQueued(gateT0.Add(time.Minute))
	if len(g.Queue) != 0 {
		t.Fatalf("queue %v past its wait_until, want empty", g.Queue)
	}
}

// The default tunables pass every boot check, and each check refuses the
// value it guards, naming the setting.
func TestGateTunables_Check(t *testing.T) {
	if err := DefaultGateTunables().Check(); err != nil {
		t.Fatalf("the defaults are refused: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(*GateTunables)
		names  string
	}{
		"continue events below four passes": {func(g *GateTunables) { g.ContinueEvents = 4*gatePassEvents - 1 }, "gate_continue_events"},
		"continue updates below four":       {func(g *GateTunables) { g.ContinueUpdates = 3 }, "gate_continue_updates"},
		"linger within a tombstone's life":  {func(g *GateTunables) { g.Linger = g.tombstoneLife() }, "gate_linger"},
		"a worst-case carry over the bound": {func(g *GateTunables) { g.MaxEntries = 5000 }, "max_entries"},
		"a zero bound":                      {func(g *GateTunables) { g.VerifyBatch = 0 }, "verify_batch"},
		"a zero duration":                   {func(g *GateTunables) { g.VerifyAwait = 0 }, "verify_await"},
	} {
		t.Run(name, func(t *testing.T) {
			g := DefaultGateTunables()
			tc.mutate(&g)
			err := g.Check()
			if err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("Check = %v, want a refusal naming %s", err, tc.names)
			}
		})
	}
}

// The worst case the boot measures is a gate full at every bound, and at the
// defaults it fits the carry bound. Its size is pinned: the margin is thin
// (§5.1.9), so a change that grows the carry must change this number on
// purpose, and the boot check still holds it to the bound.
func TestWorstCaseGateState_FitsAtTheDefaults(t *testing.T) {
	const pinned = 243059
	size, err := worstCaseGateBytes(DefaultGateTunables())
	if err != nil {
		t.Fatal(err)
	}
	if size != pinned {
		t.Fatalf("the worst-case carry at the defaults is %d bytes, pinned at %d (bound %d): "+
			"if a field was added on purpose, update the pin and keep it within the bound", size, pinned, gateCarryBound)
	}
	if size > gateCarryBound {
		t.Fatalf("worst-case carry %d bytes is over the %d-byte bound", size, gateCarryBound)
	}
}

// The worst case sets every field of the state, its entries and its
// tombstones, so a field added later is measured too or this fails.
func TestWorstCaseGateState_SetsEveryField(t *testing.T) {
	g := worstCaseGateState(DefaultGateTunables())
	for _, v := range []any{g, g.Queue[0], g.Tombstones[0], g.Rebuild, g.Tunables} {
		rv := reflect.ValueOf(v)
		for i := range rv.NumField() {
			if rv.Field(i).IsZero() {
				t.Errorf("the worst-case %s leaves %s unset", rv.Type().Name(), rv.Type().Field(i).Name)
			}
		}
	}
}

// Two arrivals under a millisecond apart keep their order across a carry:
// the gate records arrivals to the millisecond its carry keeps, so a
// re-assert after a continue-as-new finds the same line.
func TestGateCarry_KeepsTheLinesOrder(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acq("h", 1, gateStrategyQueue, gateT0), gateT0, false)
	first := gateT0.Add(300 * time.Microsecond)
	g.decideAcquire(acq("a", 1, gateStrategyQueue, first), first, false)
	second := gateT0.Add(700 * time.Microsecond)
	g.decideAcquire(acq("b", 1, gateStrategyQueue, second), second, false)
	data, err := stablejson.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	var back gateState
	if err := json.Unmarshal(data, &back, stablejson.DurationAsNano); err != nil {
		t.Fatal(err)
	}
	for i := range g.Queue {
		if !back.Queue[i].ArrivedAt.Equal(g.Queue[i].ArrivedAt) || back.Queue[i].Seq != g.Queue[i].Seq {
			t.Fatalf("entry %d changed across the carry: %+v then %+v", i, g.Queue[i], back.Queue[i])
		}
	}
	if back.Queue[0].Run != "a" || back.Queue[1].Run != "b" {
		t.Fatalf("line after the carry %v, want a then b", []string{back.Queue[0].Run, back.Queue[1].Run})
	}
}
