package engine

import (
	"github.com/orkstack/orkstack/internal/stablejson"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"testing"
	"time"
)

func heldGate(t *testing.T, runs ...string) *gateState {
	t.Helper()
	g := newTestGate()
	for _, r := range runs {
		if d := g.decideAcquire(acq(r, len(runs), gateStrategyQueue, gateT0), gateT0, false); !d.Granted {
			t.Fatalf("setup: %s not granted: %+v", r, d)
		}
	}
	return g
}

func alive(run string, seqBase int64) gateRunFinding {
	return gateRunFinding{Run: run, Open: true, MemoSeq: seqBase, Keys: map[int]bool{gateRunLevel: true}}
}

func TestGateApplyFindings(t *testing.T) {
	later := gateT0.Add(DefaultGateTunables().TaskAckGrace + time.Second)
	for name, tc := range map[string]struct {
		finding  gateRunFinding
		at       time.Time
		released bool
		marked   bool
		stalled  bool
	}{
		"alive":                   {finding: alive("r1", 1), at: later},
		"not found":               {finding: gateRunFinding{Run: "r1", NotFound: true}, at: later, released: true},
		"closed":                  {finding: gateRunFinding{Run: "r1"}, at: later, released: true},
		"a newer incarnation":     {finding: gateRunFinding{Run: "r1", Open: true, MemoSeq: 2, Keys: map[int]bool{gateRunLevel: true}}, at: later, released: true},
		"an older incarnation":    {finding: gateRunFinding{Run: "r1", Open: true, MemoSeq: 0, Keys: map[int]bool{}}, at: later, marked: true},
		"key missing, in grace":   {finding: gateRunFinding{Run: "r1", Open: true, MemoSeq: 1, Keys: map[int]bool{}}, at: gateT0.Add(time.Second)},
		"key missing, past grace": {finding: gateRunFinding{Run: "r1", Open: true, MemoSeq: 1, Keys: map[int]bool{}}, at: later, marked: true},
		"stalled":                 {finding: gateRunFinding{Run: "r1", Open: true, MemoSeq: 1, Keys: map[int]bool{gateRunLevel: true}, StuckSince: gateT0}, at: gateT0.Add(DefaultGateTunables().StalledAfter + time.Second), stalled: true},
		"an error keeps it":       {finding: gateRunFinding{Run: "r1", Err: "deadline exceeded"}, at: later},
	} {
		t.Run(name, func(t *testing.T) {
			g := heldGate(t, "r1")
			out := g.applyFindings([]gateRunFinding{tc.finding}, tc.at)
			gone := g.holderIx("r1", 1, gateRunLevel) < 0
			if gone != tc.released {
				t.Fatalf("released = %v, want %v", gone, tc.released)
			}
			if tc.released {
				if d := g.decideAcquire(acq("r1", 1, gateStrategyQueue, tc.at), tc.at, true); d.Answer.Answer != gateAnswerFenced {
					t.Fatalf("a released holder's acquire = %+v, want fenced", d)
				}
				return
			}
			h := g.Holders[0]
			if !h.MissingSince.IsZero() != tc.marked || h.Stalled != tc.stalled {
				t.Fatalf("holder %+v, want marked=%v stalled=%v", h, tc.marked, tc.stalled)
			}
			if tc.finding.Err != "" && out.Errors != 1 {
				t.Fatalf("errors counted %d, want 1", out.Errors)
			}
		})
	}
}

// A missing key is released only after one confirming pass, and at most
// task_confirm_batch per pass.
func TestGateApplyFindings_MissingKeyNeedsAConfirmingPass(t *testing.T) {
	g := newTestGate()
	g.Tunables.TaskConfirmBatch = 2
	runs := []string{"a", "b", "c"}
	for _, r := range runs {
		g.decideAcquire(acq(r, 3, gateStrategyQueue, gateT0), gateT0, false)
	}
	missing := func() []gateRunFinding {
		var out []gateRunFinding
		for _, r := range runs {
			out = append(out, gateRunFinding{Run: r, Open: true, MemoSeq: 1, Keys: map[int]bool{}})
		}
		return out
	}
	at := gateT0.Add(g.Tunables.TaskAckGrace + time.Second)
	g.applyFindings(missing(), at)
	if len(g.Holders) != 3 {
		t.Fatalf("the first pass released %d, want none: it only marks", 3-len(g.Holders))
	}
	g.applyFindings(missing(), at.Add(time.Minute))
	if len(g.Holders) != 1 {
		t.Fatalf("the confirming pass left %d holders, want 1: at most 2 released per pass", len(g.Holders))
	}
	// A key that reappears clears the mark.
	g.applyFindings([]gateRunFinding{alive(g.Holders[0].Run, 1)}, at.Add(2*time.Minute))
	if !g.Holders[0].MissingSince.IsZero() {
		t.Fatal("a key seen again kept its mark")
	}
}

// An adopted entry is marked at once, with no grace.
func TestGateApplyFindings_AdoptedIsMarkedAtOnce(t *testing.T) {
	g := heldGate(t, "r1")
	g.Holders[0].Adopted = true
	g.applyFindings([]gateRunFinding{{Run: "r1", Open: true, MemoSeq: 1, Keys: map[int]bool{}}}, gateT0.Add(time.Second))
	if g.Holders[0].MissingSince.IsZero() {
		t.Fatal("an adopted entry with no key was not marked at once")
	}
}

func memoEntry(t *testing.T, e gateMemoEntry) stablejson.RawJSON {
	t.Helper()
	b, err := stablejson.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGateRestore(t *testing.T) {
	g := newTestGate()
	records := []gateRunRecord{
		{Run: "holder", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Held: true, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0, Seq: 4, SeqBase: 1})}},
		{Run: "second", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0.Add(2 * time.Second), Seq: 9, SeqBase: 1})}},
		{Run: "first", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0.Add(time.Second), Seq: 7, SeqBase: 1})}},
		// A stale incarnation: its seq_base is not the run's.
		{Run: "stale", MemoSeq: 2, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Held: true, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0, Seq: 2, SeqBase: 1})}},
		// Undecodable: adopted at limit 1.
		{Run: "odd", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{3: stablejson.RawJSON(`{"proto":1,"limit":"two"}`)}},
	}
	g.restore(records, gateT0)
	if len(g.Holders) != 2 || g.Holders[0].Run != "holder" || g.Holders[1].Run != "odd" || !g.Holders[1].Adopted || g.Holders[1].Limit != 1 {
		t.Fatalf("holders %+v, want holder and odd (adopted, limit 1)", g.Holders)
	}
	if len(g.Queue) != 2 || g.Queue[0].Run != "first" || g.Queue[1].Run != "second" {
		t.Fatalf("queue %+v, want first then second, in line order", g.Queue)
	}
	if g.Seq != 9 {
		t.Fatalf("seq = %d, want it resumed at 9, the largest seen", g.Seq)
	}
}

// An entry its run re-asserted during the rebuild is not restored twice,
// and one queued that the run recorded as held becomes a holder.
func TestGateRestore_MergesWithEntriesAlreadyThere(t *testing.T) {
	g := newTestGate()
	g.Rebuild = gateRebuild{State: gateRebuildListing}
	re := acq("h", 2, gateStrategyQueue, gateT0)
	re.ArrivedAt, re.Seq = gateT0, 4
	g.decideAcquire(re, gateT0, false) // queued during the rebuild
	g.decideAcquire(acq("q", 2, gateStrategyQueue, gateT0), gateT0.Add(time.Second), false)
	g.restore([]gateRunRecord{
		{Run: "h", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Held: true, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0, Seq: 4, SeqBase: 1})}},
		{Run: "q", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memoEntry(t, gateMemoEntry{
			Proto: 1, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: gateT0.Add(time.Second), Seq: 5, SeqBase: 1})}},
	}, gateT0)
	if len(g.Holders) != 1 || g.Holders[0].Run != "h" || len(g.Queue) != 1 || g.Queue[0].Run != "q" {
		t.Fatalf("holders %+v queue %+v, want h holding and q queued once", g.Holders, g.Queue)
	}
}

// A release that reached the gate while it was listing is not undone by the
// restore, however stale the listing; a run whose incarnation cannot be read
// has its entries adopted, never dropped.
func TestGateRestore_KeepsReleasesAndAdoptsUnreadableIncarnations(t *testing.T) {
	g := newTestGate()
	g.Rebuild = gateRebuild{State: gateRebuildListing}
	g.release("released", 1, gateRunLevel, 0, gateT0)
	held := memoEntry(t, gateMemoEntry{Proto: 1, Held: true, Limit: 3, Strategy: gateStrategyQueue, ArrivedAt: gateT0, Seq: 2, SeqBase: 1})
	out := g.restore([]gateRunRecord{
		{Run: "released", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}},
		{Run: "unreadable", SeqUnreadable: true, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}},
		{Run: "twice", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}},
		{Run: "twice", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}},
	}, gateT0.Add(time.Second))
	if g.holderIx("released", 1, gateRunLevel) >= 0 || out.Fenced != 1 {
		t.Fatalf("holders %+v, outcome %+v; want the released run left released", g.Holders, out)
	}
	i := g.holderIx("unreadable", 1, gateRunLevel)
	if i < 0 || !g.Holders[i].Adopted || g.Holders[i].Limit != 1 {
		t.Fatalf("holders %+v, want the unreadable run's entry adopted at limit 1", g.Holders)
	}
	n := 0
	for _, h := range g.Holders {
		if h.Run == "twice" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("a run listed twice was restored %d times", n)
	}
}

// Refuse and skip cannot be told the key is full while the gate settles
// after a rebuild: they are answered unavailable, and retried.
func TestGateDecideAcquire_RefuseDuringTheSettleIsUnavailable(t *testing.T) {
	g := newTestGate()
	g.Rebuild = gateRebuild{State: gateRebuildSettling, Since: gateT0}
	if d := g.decideAcquire(acq("r", 1, gateStrategyRefuse, gateT0), gateT0.Add(time.Second), false); d.Answer.Answer != gateAnswerUnavailable {
		t.Fatalf("refuse during the settle = %+v, want unavailable", d.Answer)
	}
}

// An incarnation present in a run's memo but unreadable is an error, never
// a zero that would make a live holder look like an old incarnation.
func TestReadGateMemo_AnUnreadableIncarnationIsAnError(t *testing.T) {
	id := GateID("default", "", "deploy")
	bad, _ := converter.GetDefaultDataConverter().ToPayload("seven")
	good, _ := converter.GetDefaultDataConverter().ToPayload(int64(3))
	entry, _ := converter.GetDefaultDataConverter().ToPayload(map[string]any{"proto": 1})
	if _, _, err := readGateMemo(id, &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoSeqKey: bad}}); err == nil {
		t.Fatal("an unreadable incarnation was read without an error")
	}
	seq, keys, err := readGateMemo(id, &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoSeqKey: good, gateMemoKey(id, 2): entry}})
	if err != nil || seq != 3 || !keys[2] {
		t.Fatalf("readGateMemo = %d, %v, %v; want 3 and key 2", seq, keys, err)
	}
}
