package engine

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestLiveQueued_ReadsThePlaceInLineNowFromTheGate is GetRun's live read of
// Run.queued (concurrency design §9.1): the run's place counted in the
// gate's line as it stands, the holders now rather than at arrival, at most
// shown of them named, and the note the gate's state calls for, in
// precedence; the snapshot's arrival fields are kept as they were.
func TestLiveQueued_ReadsThePlaceInLineNowFromTheGate(t *testing.T) {
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	since := timestamppb.New(now.Add(-time.Hour))
	snapshot := func() *orkstackv1.ConcurrencyQueued {
		return &orkstackv1.ConcurrencyQueued{Key: "db", Limit: 1, PositionAtArrival: 3, Position: 3, PositionKnown: true,
			HoldersAhead: []string{"ork-old"}, HoldersCount: 1, HoldersAsOfArrival: true, QueuedSince: since}
	}
	line := func(mut func(*GateLine)) GateLine {
		l := GateLine{Present: true, Holders: 3, Queued: 3,
			Line: []GateLineEntry{
				{Run: "ork-a", RunLevel: true},
				{Run: "ork-task", RunLevel: false},
				{Run: "ork-me", RunLevel: true},
			},
			HolderRuns: []string{"ork-h1", "ork-h2", "ork-h3"}}
		if mut != nil {
			mut(&l)
		}
		return l
	}
	cases := []struct {
		name     string
		line     GateLine
		position int32
		known    bool
		holders  []string
		noteCode string
		note     string
	}{
		{name: "third in line behind a task", line: line(nil), position: 3, known: true, holders: []string{"ork-h1", "ork-h2"}},
		{name: "not in the line any more", line: line(func(l *GateLine) { l.Line = l.Line[:1] }), known: false, holders: []string{"ork-h1", "ork-h2"}},
		{name: "a holder stalled", line: line(func(l *GateLine) { l.StalledRuns = []string{"ork-h2"} }), position: 3, known: true,
			holders: []string{"ork-h1", "ork-h2"}, noteCode: "HOLDER_STALLED",
			note: "waiting behind run ork-h2, which is not making progress; an operator must end it"},
		{name: "rebuilding outranks a stall", line: line(func(l *GateLine) { l.StalledRuns = []string{"ork-h2"}; l.Rebuilding = true }),
			position: 3, known: true, holders: []string{"ork-h1", "ork-h2"}, noteCode: "REBUILDING",
			note: "waiting while the limit on key db reads which runs hold it (a first run at a key, or after its record was lost)"},
		{name: "a failing rebuild outranks rebuilding", line: line(func(l *GateLine) {
			l.Rebuilding = true
			l.RebuildFailingSince = now.Add(-25 * time.Minute)
		}), position: 3, known: true, holders: []string{"ork-h1", "ork-h2"}, noteCode: "REBUILD_FAILING",
			note: "the limit has failed to rebuild for 25m0s; it keeps trying"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := LiveQueued(snapshot(), "ork-me", c.line, 2, now)
			if got.GetPosition() != c.position || got.GetPositionKnown() != c.known {
				t.Errorf("position %d (known %v), want %d (known %v)", got.GetPosition(), got.GetPositionKnown(), c.position, c.known)
			}
			if !slices.Equal(got.GetHoldersAhead(), c.holders) || got.GetHoldersCount() != 3 || got.GetHoldersAsOfArrival() {
				t.Errorf("holders %v of %d (as of arrival %v), want %v of 3, live", got.GetHoldersAhead(), got.GetHoldersCount(), got.GetHoldersAsOfArrival(), c.holders)
			}
			if got.GetNoteCode() != c.noteCode || got.GetNote() != c.note {
				t.Errorf("note %s %q, want %s %q", got.GetNoteCode(), got.GetNote(), c.noteCode, c.note)
			}
			if got.GetPositionAtArrival() != 3 || !got.GetQueuedSince().AsTime().Equal(since.AsTime()) || got.GetKey() != "db" {
				t.Errorf("the arrival fields changed: %v", got)
			}
		})
	}
}

// TestNotAnsweringQueued_SaysTheRunStaysQueued: a gate that cannot be read
// leaves the place unknown, the holders as the run found them, and says when
// the run asks again.
func TestNotAnsweringQueued_SaysTheRunStaysQueued(t *testing.T) {
	askBy := time.Date(2026, 11, 19, 10, 15, 0, 0, time.UTC)
	snapshot := &orkstackv1.ConcurrencyQueued{Key: "db", Position: 2, PositionKnown: true, HoldersAhead: []string{"ork-old"},
		HoldersCount: 1, HoldersAsOfArrival: true}
	got := NotAnsweringQueued(snapshot, askBy)
	if got.GetPositionKnown() || got.GetNoteCode() != "NOT_ANSWERING" ||
		got.GetNote() != "the limit is not answering; this run stays queued and asks again by 2026-11-19T10:15:00Z" {
		t.Errorf("got %v", got)
	}
	if !got.GetHoldersAsOfArrival() || !slices.Equal(got.GetHoldersAhead(), []string{"ork-old"}) {
		t.Errorf("the holders were not kept as of arrival: %v", got)
	}
	if !snapshot.GetPositionKnown() {
		t.Error("the snapshot was changed in place")
	}
}

// TestGateLineOf_OrdersTheLineAndTheHolders: the line by arrival, then the
// gate's sequence; the holders by when they were granted, each run once
// however many slots it holds; and a rebuild's
// state as rebuilding, or failing since it began.
func TestGateLineOf_OrdersTheLineAndTheHolders(t *testing.T) {
	t0 := time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)
	g := gateState{
		Queue: []gateEntry{
			{Run: "c", TaskIx: gateRunLevel, ArrivedAt: t0.Add(time.Minute), Seq: 1},
			{Run: "b", TaskIx: 2, ArrivedAt: t0, Seq: 9},
			{Run: "a", TaskIx: gateRunLevel, ArrivedAt: t0, Seq: 4},
		},
		Holders: []gateEntry{
			{Run: "h2", GrantedAt: t0.Add(2 * time.Minute), Stalled: true},
			{Run: "h1", GrantedAt: t0.Add(time.Minute)},
			{Run: "h2", TaskIx: 3, GrantedAt: t0.Add(3 * time.Minute), Stalled: true}, // a second slot of h2's
		},
		Rebuild: gateRebuild{State: gateRebuildFailing, Since: t0},
	}
	line := gateLineOf(g)
	want := []GateLineEntry{{Run: "a", RunLevel: true}, {Run: "b"}, {Run: "c", RunLevel: true}}
	if !slices.Equal(line.Line, want) {
		t.Errorf("line %v, want %v", line.Line, want)
	}
	if !slices.Equal(line.HolderRuns, []string{"h1", "h2"}) || !slices.Equal(line.StalledRuns, []string{"h2"}) {
		t.Errorf("holders %v stalled %v", line.HolderRuns, line.StalledRuns)
	}
	if !line.Rebuilding || !line.RebuildFailingSince.Equal(t0) || line.Holders != 3 || line.Queued != 3 {
		t.Errorf("got %+v", line)
	}
	for state, rebuilding := range map[string]bool{gateRebuildNone: false, gateRebuildListing: true, gateRebuildSettling: true} {
		got := gateLineOf(gateState{Rebuild: gateRebuild{State: state, Since: t0}})
		if got.Rebuilding != rebuilding || !got.RebuildFailingSince.IsZero() {
			t.Errorf("rebuild %q: rebuilding %v, failing since %v", state, got.Rebuilding, got.RebuildFailingSince)
		}
	}
}
