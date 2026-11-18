package engine

import (
	"encoding/json/v2"
	"fmt"
	"github.com/orkstack/orkstack/internal/stablejson"
	"reflect"
	"slices"
	"testing"
	"time"
)

// FuzzGateState drives a gate with acquires, re-asserts, releases, time and
// grants, and after every step checks the properties E88 §13 states for it:
//
//   - R1: no key ever has more holders than any holder's own limit;
//   - FIFO: the line stays in (arrived_at, seq) order;
//   - refuse and skip never queue;
//   - an entry is a holder or queued, never both, never twice;
//   - nothing is granted while frozen or settling;
//   - a released slot is fenced: the same acquire is never granted again
//     while its tombstone lives, unless the tombstone was evicted, and then
//     the grant is verify_pending;
//   - release_all{below} removes exactly the run's entries below it;
//   - a re-assert is never answered full_bound;
//   - the tombstones stay within max_tombstones;
//   - the state survives an encode and decode unchanged (it is the carry);
//   - ordinals (§22.14.1): a slot holds at most one entry; acquire(n+1)
//     arriving before release(n) never raises the holder count past R1; a
//     late release(n) leaves the n+1 entry in place; a release never removes
//     an entry of another ordinal.
func FuzzGateState(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{0, 0, 0, 0, 1, 1, 1, 3, 3, 4})
	f.Add([]byte{0, 16, 32, 48, 2, 0, 5, 64, 7, 0, 0, 0})
	f.Add([]byte{6, 6, 0, 0, 0, 2, 2, 8, 8, 0, 3})
	f.Add([]byte{0, 0, 0, 1, 8, 0, 9, 0, 8, 1, 3, 0, 9, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		g := newTestGate()
		g.Tunables.MaxEntries = 6
		g.Tunables.MaxTombstones = 5
		// A small modulus, so a short run of ops wraps the ordinal.
		g.Tunables.OrdinalModulus = 6
		now := gateT0
		strategies := []string{gateStrategyQueue, gateStrategyRefuse, gateStrategySkip}
		answers := map[string]gateAnswer{}
		released := map[string]time.Time{}
		ords := map[string]int64{} // each slot's latest ordinal asked for
		for i := 0; i+1 < len(ops); i += 2 {
			op, arg := ops[i]%10, int(ops[i+1])
			run := fmt.Sprintf("r%d", arg%5)
			seqBase := int64(1 + (arg>>3)%2)
			switch op {
			case 0, 1: // acquire, or retry it, at the slot's current ordinal
				req := acq(run, 1+(arg>>4)%3, strategies[(arg>>2)%3], now)
				req.SeqBase = seqBase
				if n := ords[slotTombKey(run, seqBase, gateRunLevel)]; n > 0 {
					req.N, req.Proto = n, gateProtoOrdinal
				}
				d := g.decideAcquire(req, now, false)
				if d.WantsPass {
					d = g.decideAcquire(req, now, true)
				}
				key := slotTombKey(run, seqBase, gateRunLevel)
				// A released slot is fenced while its tombstone lives. One
				// evicted early (past max_tombstones) degrades the fence
				// instead: the grant is then verify_pending, checked at the
				// next pass, never silently trusted.
				if until, ok := released[key]; ok && now.Before(until) && d.Granted {
					if h := g.holderIx(run, seqBase, gateRunLevel); h < 0 || !g.Holders[h].VerifyPending {
						t.Fatalf("step %d: %s granted while its tombstone lives, and not verify_pending", i, run)
					}
				}
				if d.Answer.Answer == gateAnswerQueued || d.Answer.Answer == gateAnswerGranted {
					answers[key] = d.Answer
				}
				g.admitHeads(now)
			case 2: // re-assert after a lost line
				key := slotTombKey(run, seqBase, gateRunLevel)
				first, ok := answers[key]
				if !ok || g.holderIx(run, seqBase, gateRunLevel) >= 0 || g.queueIx(run, seqBase, gateRunLevel) >= 0 {
					continue
				}
				req := acq(run, 1, gateStrategyQueue, now)
				req.SeqBase, req.ArrivedAt, req.Seq = seqBase, first.ArrivedAt, first.Seq
				if d := g.decideAcquire(req, now, true); d.Answer.Answer == gateAnswerFullBound {
					t.Fatalf("step %d: a re-assert was answered full_bound", i)
				}
			case 3: // release, at the slot's current ordinal
				g.release(run, seqBase, gateRunLevel, ords[slotTombKey(run, seqBase, gateRunLevel)], now)
				released[slotTombKey(run, seqBase, gateRunLevel)] = now.Add(g.Tunables.tombstoneLife())
				g.admitHeads(now)
			case 4: // release_all
				below := seqBase + 1
				g.releaseAll(run, below, now)
				for _, e := range slices.Concat(g.Holders, g.Queue) {
					if e.Run == run && e.SeqBase < below {
						t.Fatalf("step %d: release_all{%s, below %d} left %+v", i, run, below, e)
					}
				}
				g.admitHeads(now)
			case 5: // time passes
				now = now.Add(time.Duration(1+arg) * 7 * time.Second)
				g.dropExpiredQueued(now)
				g.admitHeads(now)
			case 6: // freeze, or a settle, then try to grant
				if arg%2 == 0 {
					g.FrozenUntil = now.Add(time.Minute)
				} else {
					g.Rebuild = gateRebuild{State: gateRebuildSettling, Since: now}
				}
				if granted := g.admitHeads(now); len(granted) > 0 {
					t.Fatalf("step %d: granted %v while frozen or settling", i, granted)
				}
				g.FrozenUntil, g.Rebuild = time.Time{}, gateRebuild{}
			case 7:
				g.admitHeads(now)
			case 8: // a give-back's re-acquire, arriving before its release
				key := slotTombKey(run, seqBase, gateRunLevel)
				// The ordinal wraps (decision D11); the run's latest
				// ordinal moves on only when the gate took it.
				next := (ords[key] + 1) % g.Tunables.OrdinalModulus
				req := acq(run, 1+(arg>>4)%3, gateStrategyQueue, now)
				req.SeqBase, req.N, req.Proto = seqBase, next, gateProtoOrdinal
				d := g.decideAcquire(req, now, true)
				if d.Answer.Answer == gateAnswerQueued || d.Answer.Answer == gateAnswerGranted {
					answers[key], ords[key] = d.Answer, next
				}
				g.admitHeads(now)
			case 9: // the late release of the ordinal before the latest
				key := slotTombKey(run, seqBase, gateRunLevel)
				if ords[key] == 0 {
					continue
				}
				late := (ords[key] - 1 + g.Tunables.OrdinalModulus) % g.Tunables.OrdinalModulus
				var current *gateEntry
				for _, e := range slices.Concat(g.Holders, g.Queue) {
					if e.same(run, seqBase, gateRunLevel) {
						current = &e
					}
				}
				g.release(run, seqBase, gateRunLevel, late, now)
				if current != nil && current.N == ords[key] {
					h, q := g.holderIx(run, seqBase, gateRunLevel), g.queueIx(run, seqBase, gateRunLevel)
					if (h < 0 || g.Holders[h].N != ords[key]) && (q < 0 || g.Queue[q].N != ords[key]) {
						t.Fatalf("step %d: a late release(%d) removed %s's entry of ordinal %d", i, late, run, ords[key])
					}
				}
				g.admitHeads(now)
			}
			checkGateInvariants(t, g, i)
		}
		data, err := stablejson.Marshal(g)
		if err != nil {
			t.Fatalf("encode the carry: %v", err)
		}
		var back gateState
		if err := json.Unmarshal(data, &back, stablejson.DurationAsNano); err != nil {
			t.Fatalf("decode the carry: %v", err)
		}
		again, _ := stablejson.Marshal(&back)
		if string(again) != string(data) {
			t.Fatalf("the carry changed across an encode and decode:\n%s\n%s", data, again)
		}
	})
}

func checkGateInvariants(t *testing.T, g *gateState, step int) {
	t.Helper()
	for _, h := range g.Holders {
		if len(g.Holders) > h.Limit {
			t.Fatalf("step %d: %d holders, but %s's limit is %d", step, len(g.Holders), h.Run, h.Limit)
		}
	}
	for i := 1; i < len(g.Queue); i++ {
		if !g.Queue[i-1].before(g.Queue[i].ArrivedAt, g.Queue[i].Seq) {
			t.Fatalf("step %d: line out of order at %d: %+v then %+v", step, i, g.Queue[i-1], g.Queue[i])
		}
	}
	seen := map[string]bool{}
	for _, e := range slices.Concat(g.Holders, g.Queue) {
		key := slotTombKey(e.Run, e.SeqBase, e.TaskIx)
		if seen[key] {
			t.Fatalf("step %d: %s seq_base %d is held or queued twice", step, e.Run, e.SeqBase)
		}
		seen[key] = true
	}
	for _, q := range g.Queue {
		if q.Strategy == gateStrategyRefuse || q.Strategy == gateStrategySkip {
			t.Fatalf("step %d: a %s entry queued", step, q.Strategy)
		}
	}
	if len(g.Tombstones) > g.Tunables.MaxTombstones {
		t.Fatalf("step %d: %d tombstones, bound %d", step, len(g.Tombstones), g.Tunables.MaxTombstones)
	}
}

// FuzzDecodeGateMemoEntry: a run's memo entry is read back from the cluster.
// Whatever the bytes, decoding never panics, an accepted entry has a
// protocol this gate speaks, a limit of at least one, a strategy it decides
// and an ordinal within the cap that only protocol 2 carries, and it
// re-encodes to bytes that decode to the same entry.
func FuzzDecodeGateMemoEntry(f *testing.F) {
	f.Add([]byte(`{"proto":1,"held":true,"limit":2,"strategy":"queue","arrived_at":"2026-11-17T12:00:00Z","seq":4,"seq_base":1}`))
	f.Add([]byte(`{"proto":1,"limit":"two"}`))
	f.Add([]byte(`{"proto":3,"limit":1,"strategy":"queue","arrived_at":"2026-11-17T12:00:00Z","seq":1,"seq_base":1}`))
	f.Add([]byte(`{"proto":1,"limit":1,"strategy":"replace","arrived_at":"2026-11-17T12:00:00Z","seq":1,"seq_base":1,"extra":1}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"proto":2,"limit":1,"strategy":"queue","arrived_at":"2026-11-17T12:00:00Z","seq":1,"seq_base":1,"n":3}`))
	f.Add([]byte(`{"proto":1,"limit":1,"strategy":"queue","arrived_at":"2026-11-17T12:00:00Z","seq":1,"seq_base":1,"n":3}`))
	f.Add([]byte(`{"proto":2,"limit":1,"strategy":"queue","arrived_at":"2026-11-17T12:00:00Z","seq":1,"seq_base":1,"n":65536}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var m gateMemoEntry
		if err := decodeGateMemoEntry(raw, &m); err != nil {
			return
		}
		if m.Proto < 1 || m.Proto > gateProtoOrdinal || m.Limit < 1 || !gateStrategyDecided(m.Strategy) ||
			m.N < 0 || m.N >= gateOrdinalWire || (m.N > 0 && m.Proto < gateProtoOrdinal) {
			t.Fatalf("accepted %+v", m)
		}
		again, err := stablejson.Marshal(m)
		if err != nil {
			t.Fatalf("re-encode %+v: %v", m, err)
		}
		var back gateMemoEntry
		if err := decodeGateMemoEntry(again, &back); err != nil || back != m {
			t.Fatalf("round trip %+v → %s → %+v, %v", m, again, back, err)
		}
	})
}

// FuzzParseGateMemoKey: a memo key read back is one of the gate's only if
// it is exactly the key gateMemoKey writes for the index it parses to.
func FuzzParseGateMemoKey(f *testing.F) {
	id := GateID("payments", "", "deploy")
	f.Add(gateMemoKey(id, gateRunLevel))
	f.Add(gateMemoKey(id, 3))
	f.Add("orkflow_conc_" + id + "_03")
	f.Add("orkflow_conc_" + id + "_-1")
	f.Add("orkflow_conc_seq")
	f.Fuzz(func(t *testing.T, key string) {
		ix, ok := parseGateMemoKey(id, key)
		if ok && gateMemoKey(id, ix) != key {
			t.Fatalf("parsed %q as %d, which writes %q", key, ix, gateMemoKey(id, ix))
		}
	})
}

// FuzzGateCarry: a gate's carry is read back from its history. Whatever the
// bytes, decoding an entry or a tombstone never panics, and one it accepts
// re-encodes to bytes that decode to the same value, with a run, a strategy,
// a key and known flags.
func FuzzGateCarry(f *testing.F) {
	e, _ := stablejson.Marshal(gateEntry{Run: "r1", TaskIx: gateRunLevel, SeqBase: 1, Limit: 2, Strategy: gateStrategyQueue,
		Proto: 1, ArrivedAt: gateT0, Seq: 3, GrantPending: true})
	tb, _ := stablejson.Marshal(gateTomb{Key: "0123456789abcdef", RunLevel: true, Below: 2, Until: gateT0})
	f.Add(e)
	f.Add(tb)
	f.Add([]byte(`["r1",-1,1,2,"queue",1,0,0,3,0,0,0,99]`))
	f.Add([]byte(`["k",7,1,0,""]`))
	f.Add([]byte(`[null,-1,1,2,"queue",1,0,0,3,0,0,0,0]`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var entry gateEntry
		if err := json.Unmarshal(raw, &entry, stablejson.DurationAsNano); err == nil {
			if entry.Run == "" || entry.Strategy == "" || entry.Limit < 1 {
				t.Fatalf("accepted entry %+v", entry)
			}
			again, err := stablejson.Marshal(entry)
			var back gateEntry
			if err != nil || json.Unmarshal(again, &back, stablejson.DurationAsNano) != nil || !reflect.DeepEqual(back, entry) {
				t.Fatalf("entry round trip %+v → %s → %+v", entry, again, back)
			}
		}
		var tomb gateTomb
		if err := json.Unmarshal(raw, &tomb, stablejson.DurationAsNano); err == nil {
			if tomb.Key == "" {
				t.Fatalf("accepted tombstone %+v", tomb)
			}
			again, err := stablejson.Marshal(tomb)
			var back gateTomb
			if err != nil || json.Unmarshal(again, &back, stablejson.DurationAsNano) != nil || !reflect.DeepEqual(back, tomb) {
				t.Fatalf("tombstone round trip %+v → %s → %+v", tomb, again, back)
			}
		}
	})
}
