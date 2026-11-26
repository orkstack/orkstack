package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// frozenOutcome is one freeze's result.
type frozenOutcome struct {
	frozen GateFrozen
	failed error
}

type freezeRecorder struct{ out *frozenOutcome }

func (r freezeRecorder) Accept()          {}
func (r freezeRecorder) Reject(err error) { r.out.failed = err }
func (r freezeRecorder) Complete(success any, err error) {
	if err != nil {
		r.out.failed = err
		return
	}
	switch v := success.(type) {
	case GateFrozen:
		r.out.frozen = v
	case converter.EncodedValue:
		r.out.failed = v.Get(&r.out.frozen)
	}
}

// freezeAt sends a freeze for r at offset, issued then unless r says when.
func (h *gateHarness) freezeAt(offset time.Duration, r gateReset) *frozenOutcome {
	out := &frozenOutcome{}
	h.env.RegisterDelayedCallback(func() {
		if r.IssuedAt.IsZero() {
			r.IssuedAt = h.env.Now()
		}
		h.env.UpdateWorkflow(GateUpdateFreeze, "", freezeRecorder{out: out}, GateFreeze{Request: r})
	}, offset)
	return out
}

func errType(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Type()
	}
	return ""
}

// TestGateWorkflow_FreezeStopsItUntilItsTime: a freeze answers the state it
// froze; until its time the gate refuses an acquire as busy, keeps taking
// releases and grants nothing; after it the head of the line is granted.
func TestGateWorkflow_FreezeStopsItUntilItsTime(t *testing.T) {
	h := newGateHarness(t)
	tun := DefaultGateTunables()
	tun.FreezeTimeout = 30 * time.Second
	h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	h.acquireAt(2*time.Second, runAcquire("r2", 1, gateStrategyQueue))
	out := h.freezeAt(3*time.Second, gateReset{ID: "a", Body: "b"})
	late := h.acquireAt(4*time.Second, runAcquire("r3", 1, gateStrategyQueue))
	h.signalAt(5*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	var frozeAt time.Time
	h.env.RegisterDelayedCallback(func() { frozeAt = h.env.Now() }, 3*time.Second)
	h.env.RegisterDelayedCallback(func() {
		if len(h.granted) != 0 {
			t.Errorf("a frozen gate granted %v", h.granted)
		}
	}, 30*time.Second)
	h.signalAt(5*time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, tun)
	h.closedEmpty()
	if h.memo == nil || !h.memo.FrozenUntil.Equal(frozeAt.Add(30*time.Second)) {
		t.Fatalf("memo %+v; want it to say a reset holds the gate", h.memo)
	}
	if out.failed != nil || len(out.frozen.State.Holders) != 1 || len(out.frozen.State.Queue) != 1 || !out.frozen.Until.Equal(frozeAt.Add(30*time.Second)) {
		t.Fatalf("frozen = %+v, %v; want r1 holding, r2 queued, until 30s later", out.frozen, out.failed)
	}
	if errType(late.rejected) != GateErrBusy {
		t.Fatalf("an acquire of a frozen gate = %v; want %s", late.rejected, GateErrBusy)
	}
	if len(h.granted) != 1 || h.granted[0].Run != "r2" || h.grantAt[0].Before(frozeAt.Add(30*time.Second)) {
		t.Fatalf("grants %v at %v; want r2's, once the freeze ended", h.granted, h.grantAt)
	}
}

// TestGateWorkflow_UnfreezeEndsItEarly: the request that froze it ends the
// freeze; another's unfreeze, or one of another time, does not.
func TestGateWorkflow_UnfreezeEndsItEarly(t *testing.T) {
	h := newGateHarness(t)
	h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	h.acquireAt(2*time.Second, runAcquire("r2", 1, gateStrategyQueue))
	var r gateReset
	h.env.RegisterDelayedCallback(func() {
		r = gateReset{ID: "a", Body: "b", IssuedAt: h.env.Now()}
		h.env.UpdateWorkflow(GateUpdateFreeze, "", freezeRecorder{out: &frozenOutcome{}}, GateFreeze{Request: r})
	}, 3*time.Second)
	h.signalAt(4*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.env.RegisterDelayedCallback(func() {
		other := r
		other.IssuedAt = r.IssuedAt.Add(time.Second)
		h.env.SignalWorkflow(GateSignalUnfreeze, other)
		h.env.SignalWorkflow(GateSignalUnfreeze, gateReset{ID: "z", Body: "b", IssuedAt: r.IssuedAt})
	}, 5*time.Second)
	var unfroze time.Time
	h.env.RegisterDelayedCallback(func() {
		if len(h.granted) != 0 {
			t.Errorf("another request's unfreeze ended the freeze: granted %v", h.granted)
		}
		unfroze = h.env.Now()
		h.env.SignalWorkflow(GateSignalUnfreeze, r)
	}, 10*time.Second)
	h.signalAt(5*time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	if len(h.granted) != 1 || !h.grantAt[0].Equal(unfroze) {
		t.Fatalf("grants %v at %v; want r2's at the unfreeze, %v", h.granted, h.grantAt, unfroze)
	}
}

// TestGateWorkflow_AFrozenEmptyGateDoesNotClose: a gate frozen as its linger
// ends stays until the freeze does, so no acquire starts another beside the
// reset it is frozen for.
func TestGateWorkflow_AFrozenEmptyGateDoesNotClose(t *testing.T) {
	h := newGateHarness(t)
	tun := DefaultGateTunables()
	var frozeAt time.Time
	h.env.RegisterDelayedCallback(func() { frozeAt = h.env.Now() }, tun.Linger-10*time.Second)
	out := h.freezeAt(tun.Linger-10*time.Second, gateReset{ID: "a", Body: "b"})
	h.run(GatePreviousClean, tun)
	h.closedEmpty()
	if closed := h.env.Now(); out.failed != nil || frozeAt.IsZero() || closed.Before(frozeAt.Add(time.Minute)) {
		t.Fatalf("the gate, frozen at %v (%v), closed at %v; want not before its freeze ended", frozeAt, out.failed, closed)
	}
}

// resetStart is a start that resets a key holding r1 and queueing r2, read at
// epoch 3 under a freeze that ends well after the start.
func resetStart(now time.Time) GateStart {
	entry := gateEntry{Run: "r1", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue, Proto: gateProto,
		ArrivedAt: now, Seq: 7, GrantedAt: now, VerifyPending: true}
	queued := gateEntry{Run: "r2", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue, Proto: gateProto,
		ArrivedAt: now, Seq: 8, WaitUntil: now.Add(50 * time.Minute)}
	return GateStart{Tunables: DefaultGateTunables(), Previous: GatePreviousUnclean, Reset: &GateResetStart{
		Request:     gateReset{ID: "a", IssuedAt: now, Body: "b"},
		FrozenUntil: now.Add(time.Minute),
		Seeds: gateSeeds{Labels: []string{"deploy"}, Holders: []gateEntry{entry}, Queue: []gateEntry{queued}, Seq: 8, Epoch: 3,
			Resets: []gateReset{{ID: "earlier", IssuedAt: now.Add(-time.Hour), Body: "c", Done: true}}},
	}}
}

// TestGateWorkflow_AResetStartTakesOverAndRecords: the new gate holds what it
// was seeded with beside what the runs' records add, each once and in line
// order, in the epoch after the one it was read at; it records the reset
// applied, then completed once its record is rebuilt; and it remembers the
// request, so the same one never acts again and one that asks otherwise under
// its id is refused.
func TestGateWorkflow_AResetStartTakesOverAndRecords(t *testing.T) {
	h := newGateHarness(t)
	now := h.env.Now()
	h.list = func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		held, _ := stablejson.Marshal(gateMemoEntry{Proto: 1, Held: true, Limit: 1, Strategy: gateStrategyQueue, ArrivedAt: gateArrival(now.UTC()), Seq: 7, SeqBase: 1})
		return ListConcurrencyEntriesResult{Records: []gateRunRecord{{Run: "r1", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}}}}, nil
	}
	in := resetStart(now)
	again := h.freezeAt(2*time.Minute, in.Reset.Request)
	other := in.Reset.Request
	other.Body = "another"
	changed := h.freezeAt(3*time.Minute, other)
	var state GateStateView
	h.env.RegisterDelayedCallback(func() {
		v, err := h.env.QueryWorkflow(GateQueryState)
		must.OK(t, err)
		must.OK(t, v.Get(&state))
	}, 4*time.Minute)
	h.signalAt(5*time.Minute, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(6*time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.start(in)
	r := h.closedEmpty()
	g := state.State
	if len(g.Holders) != 1 || g.Holders[0].Run != "r1" || len(g.Queue) != 1 || g.Queue[0].Run != "r2" || g.Epoch != 4 || r.Epoch != 4 || g.ResetID != "a" {
		t.Fatalf("holders %v, queue %v, epoch %d, reset %q; want r1, r2, 4 and a", g.Holders, g.Queue, g.Epoch, g.ResetID)
	}
	if len(h.records) != 2 || h.records[0].Kind != ResetRecordApplied || h.records[1].Kind != ResetRecordCompleted ||
		h.records[0].RequestID != "a" || h.records[0].Epoch != 4 || h.records[0].Key != "deploy" || h.records[0].StartedAt.IsZero() ||
		!h.records[1].StartedAt.Equal(h.records[0].StartedAt) {
		t.Fatalf("records %+v; want a applied then completed, at epoch 4", h.records)
	}
	if errType(again.failed) != GateErrResetKnown || errType(changed.failed) != GateErrResetRefused {
		t.Fatalf("the same request again = %v, another under its id = %v; want %s and %s", again.failed, changed.failed, GateErrResetKnown, GateErrResetRefused)
	}
	if h.memo == nil || h.memo.Current != "a" || len(h.memo.Resets) != 2 || !h.memo.Resets[1].Done || !h.memo.UnknownUntil.IsZero() {
		t.Fatalf("memo %+v; want a and the earlier reset, a done", h.memo)
	}
	if len(h.granted) != 1 || h.granted[0].Run != "r2" {
		t.Fatalf("grants %v; want r2's alone, after r1 released", h.granted)
	}
}

// TestGateWorkflow_AResetThatCannotRebuildIsRecordedFailed, once, when its
// rebuild is past its timeout; and completed if a later attempt succeeds.
func TestGateWorkflow_AResetThatCannotRebuildIsRecordedFailed(t *testing.T) {
	h := newGateHarness(t)
	started := h.env.Now()
	h.list = func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		if h.env.Now().Sub(started) < 20*time.Minute {
			return ListConcurrencyEntriesResult{}, errors.New("visibility down")
		}
		return ListConcurrencyEntriesResult{}, nil
	}
	in := resetStart(started)
	in.Reset.Seeds.Holders, in.Reset.Seeds.Queue = nil, nil
	h.start(in)
	h.closedEmpty()
	var kinds []string
	for _, r := range h.records {
		kinds = append(kinds, r.Kind)
	}
	if len(kinds) != 3 || kinds[0] != ResetRecordApplied || kinds[1] != ResetRecordFailed || kinds[2] != ResetRecordCompleted {
		t.Fatalf("records %v; want applied, failed, completed", kinds)
	}
}

// TestGateWorkflow_AResetStartedLate, after the freeze its state was read under
// ended or past the time a start without one had, keeps the state it was given
// and takes no other reset until any that came between is past its window,
// which it also writes where the next gate reads it. One started in time takes
// another reset.
func TestGateWorkflow_AResetStartedLate(t *testing.T) {
	for name, c := range map[string]struct {
		frozenUntil, startBy time.Duration // from the start; 0 is unset
		late                 bool
	}{
		"its freeze lapsed":      {frozenUntil: -time.Second, late: true},
		"a stray without one":    {startBy: -time.Second, late: true},
		"in time without one":    {startBy: time.Second},
		"in time under a freeze": {frozenUntil: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			h := newGateHarness(t)
			now := h.env.Now()
			in := resetStart(now)
			in.Reset.FrozenUntil, in.Reset.StartBy = time.Time{}, time.Time{}
			if c.frozenUntil != 0 {
				in.Reset.FrozenUntil = now.Add(c.frozenUntil)
			}
			if c.startBy != 0 {
				in.Reset.StartBy = now.Add(c.startBy)
			}
			soon := h.freezeAt(30*time.Second, gateReset{ID: "y", Body: "b"})
			out := h.freezeAt(2*time.Minute, gateReset{ID: "z", Body: "b"})
			var holders int
			h.env.RegisterDelayedCallback(func() {
				v, err := h.env.QueryWorkflow(GateQueryState)
				must.OK(t, err)
				var state GateStateView
				must.OK(t, v.Get(&state))
				holders = len(state.State.Holders)
			}, 3*time.Minute)
			h.signalAt(4*time.Minute, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
			h.signalAt(5*time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
			h.start(in)
			h.closedEmpty()
			if holders != 1 {
				t.Fatalf("%d holders; want r1 still holding", holders)
			}
			// While its start may be repeated, no other reset is taken.
			if want := map[bool]string{true: GateErrResetRefused, false: GateErrBusy}[c.late]; errType(soon.failed) != want {
				t.Fatalf("a reset 30s after it = %v; want %s", soon.failed, want)
			}
			if !c.late {
				if out.failed != nil || h.memo == nil || !h.memo.UnknownUntil.IsZero() {
					t.Fatalf("a later reset = %v, memo %+v; want it taken", out.failed, h.memo)
				}
				return
			}
			if errType(out.failed) != GateErrResetRefused || h.memo == nil || h.memo.UnknownUntil.Before(now.Add(in.Tunables.ResetWindow)) {
				t.Fatalf("a later reset = %v, memo %+v; want %s and resets unknown for the window", out.failed, h.memo, GateErrResetRefused)
			}
		})
	}
}

// TestGateWorkflow_ResetsAreInherited: a gate that follows another, whether it
// closed cleanly or not, remembers the resets that one applied, refuses one it
// cannot know was applied, and grants acquires either way (CR-1).
func TestGateWorkflow_ResetsAreInherited(t *testing.T) {
	for _, previous := range []string{GatePreviousClean, GatePreviousUnclean} {
		t.Run(previous, func(t *testing.T) {
			h := newGateHarness(t)
			now := h.env.Now()
			past := gateReset{ID: "a", IssuedAt: now.Add(-time.Hour), Body: "b", Done: true}
			old := gateReset{ID: "o", IssuedAt: now.Add(-25 * time.Hour), Body: "b", Done: true}
			again := h.freezeAt(2*time.Minute, gateReset{ID: "a", IssuedAt: past.IssuedAt, Body: "b"})
			changed := h.freezeAt(3*time.Minute, gateReset{ID: "a", Body: "b"})
			aged := h.freezeAt(4*time.Minute, gateReset{ID: "o", IssuedAt: old.IssuedAt, Body: "b"})
			fresh := h.freezeAt(4*time.Minute+30*time.Second, gateReset{ID: "n", Body: "b"})
			h.start(GateStart{Tunables: DefaultGateTunables(), Previous: previous, Resets: &gateResetMemo{Resets: []gateReset{past, old}}})
			h.closedEmpty()
			if errType(again.failed) != GateErrResetKnown || errType(changed.failed) != GateErrResetRefused ||
				errType(aged.failed) != GateErrResetRefused || fresh.failed != nil || fresh.frozen.Until.IsZero() {
				t.Fatalf("again %v, under another time %v, past its window %v, new %v", again.failed, changed.failed, aged.failed, fresh.failed)
			}
		})
		t.Run(previous+", unreadable", func(t *testing.T) {
			h := newGateHarness(t)
			now := h.env.Now()
			acquired := h.acquireAt(2*time.Minute, runAcquire("r1", 1, gateStrategyQueue))
			out := h.freezeAt(3*time.Minute, gateReset{ID: "n", Body: "b"})
			h.signalAt(4*time.Minute, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
			h.start(GateStart{Tunables: DefaultGateTunables(), Previous: previous, Resets: &gateResetMemo{Unreadable: true}})
			h.closedEmpty()
			if errType(out.failed) != GateErrResetRefused || acquired.answer.Answer != gateAnswerGranted {
				t.Fatalf("a reset = %v, an acquire = %+v; want refused, and granted", out.failed, acquired)
			}
			if h.memo == nil || h.memo.Unreadable || h.memo.UnknownUntil.Before(now.Add(24*time.Hour)) {
				t.Fatalf("memo %+v; want when its resets are known again", h.memo)
			}
		})
	}
}

// TestGateWorkflow_AContinuedGateIsNotResetAgain: the carry holds the reset's
// memory, and the start that began it is not carried beside it.
func TestGateWorkflow_AContinuedGateIsNotResetAgain(t *testing.T) {
	h := newGateHarness(t)
	in := resetStart(h.env.Now())
	in.Tunables.ContinueUpdates = 4
	started := h.env.Now()
	for i := range 6 {
		h.acquireAt(time.Duration(i+1)*time.Second, runAcquire("r1", 1, gateStrategyQueue))
	}
	// Not before the start that began it could last be repeated.
	h.env.RegisterDelayedCallback(func() {
		v, err := h.env.QueryWorkflow(GateQueryState)
		must.OK(t, err)
		var state GateStateView
		must.OK(t, v.Get(&state))
		if h.env.IsWorkflowCompleted() || state.State.Draining {
			t.Error("the gate carried its state on while its own start could be repeated")
		}
	}, in.Tunables.FreezeTimeout-time.Second)
	h.start(in)
	if past := h.env.Now().Sub(started); past < in.Tunables.FreezeTimeout {
		t.Fatalf("the gate continued after %s; want not within %s of its start", past, in.Tunables.FreezeTimeout)
	}
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the gate ended %v, want it continued", h.env.GetWorkflowError())
	}
	var next GateStart
	must.OK(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
	if next.Reset != nil || next.Resets != nil || next.Carry == nil || next.Carry.ResetID != "a" || len(next.Carry.Resets) != 2 || !next.Carry.Resets[1].Done {
		t.Fatalf("continued with %+v, carry %+v; want the memory carried and no reset start", next, next.Carry)
	}
}

// TestResetMemo_ReadsWhatItWroteAndNothingElse: a memo this release cannot
// read makes the next gate refuse resets rather than forget them; one that
// says nothing says nothing.
func TestResetMemo_ReadsWhatItWroteAndNothingElse(t *testing.T) {
	now := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	memo := func(v any) *commonpb.Memo {
		p, err := DataConverter().ToPayload(v)
		must.OK(t, err)
		return &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateResetsMemoKey: p}}
	}
	if got := resetMemoOf(nil); got != nil {
		t.Fatalf("no memo = %+v", got)
	}
	if got := resetMemoOf(memo(gateResetMemo{})); got != nil {
		t.Fatalf("an empty memo = %+v", got)
	}
	full := gateResetMemo{Current: "a", Resets: []gateReset{{ID: "a", IssuedAt: now, Body: "b"}}}
	got := resetMemoOf(memo(full))
	if got == nil || got.Current != "a" || len(got.Resets) != 1 || !got.Resets[0].same(full.Resets[0]) {
		t.Fatalf("read back %+v", got)
	}
	for name, v := range map[string]any{"text": "x", "a request with no time": gateResetMemo{Resets: []gateReset{{ID: "a", Body: "b"}}}} {
		if got := resetMemoOf(memo(v)); got == nil || !got.Unreadable {
			t.Errorf("%s = %+v; want unreadable", name, got)
		}
	}
	// A value the memo holds that no converter knows.
	if got := resetMemoOf(&commonpb.Memo{Fields: map[string]*commonpb.Payload{gateResetsMemoKey: {Data: []byte("?")}}}); got == nil || !got.Unreadable {
		t.Errorf("an unknown encoding = %+v; want unreadable", got)
	}
	// What the next gate takes: never Current, and no request every clock
	// agrees is past its window.
	in := gateResetMemo{Current: "a", Resets: []gateReset{{ID: "a", IssuedAt: now.Add(-25 * time.Hour), Body: "b"}, {ID: "c", IssuedAt: now.Add(-24 * time.Hour), Body: "b"}}}
	next := in.inherited(24*time.Hour, now)
	if next == nil || next.Current != "" || len(next.Resets) != 1 || next.Resets[0].ID != "c" || len(in.Resets) != 2 || in.Resets[0].ID != "a" {
		t.Fatalf("inherited %+v of %+v; want c alone", next, in)
	}
	if next := (&gateResetMemo{Unreadable: true}).inherited(time.Hour, now); next == nil || !next.Unreadable {
		t.Fatalf("inherited %+v; want unreadable kept", next)
	}
	if next := (&gateResetMemo{UnknownUntil: now.Add(-time.Hour)}).inherited(time.Hour, now); next != nil {
		t.Fatalf("inherited %+v; want nothing", next)
	}
}

// TestRecordConcurrencyReset_WritesEachOnce: a record run again is the one
// written, a failed reset's record says why, and a store that cannot be
// written is an error the gate retries.
func TestRecordConcurrencyReset_WritesEachOnce(t *testing.T) {
	store := artifacts.NewMemoryAuditStore()
	seen := &resetCounts{}
	a := (&GateActivities{}).WithResetRecords(store, func() bool { return true }, seen)
	at := time.Date(2026, 11, 25, 9, 0, 0, 5, time.UTC)
	rec := func(kind string) ConcurrencyResetRecord {
		return ConcurrencyResetRecord{Kind: kind, Namespace: "payments", GateID: "ork-conc-x", Key: "db:prod", RequestID: "a", StartedAt: at, Epoch: 4, Holders: 2, Queued: 1}
	}
	for _, kind := range []string{ResetRecordApplied, ResetRecordApplied, ResetRecordFailed, ResetRecordCompleted} {
		must.OK(t, a.RecordConcurrencyReset(context.Background(), rec(kind)))
	}
	page, err := store.ListAuditRecords(context.Background(), artifacts.AuditQuery{Since: at.Add(-time.Hour), Until: at.Add(time.Hour), Limit: 10})
	must.OK(t, err)
	if len(page.Records) != 3 || len(seen.outcomes) != 3 {
		t.Fatalf("%d records, %d counted; want 3 of each", len(page.Records), len(seen.outcomes))
	}
	for _, r := range page.Records {
		if r.GetTarget() != "db:prod" || r.GetSchemaVersion() != artifacts.AuditSchemaVersionVerified || r.GetDetail() != "request a: epoch=4 holders=2 queued=1" ||
			(r.GetAction() == "concurrency.reset.failed") == r.GetSucceeded() || (r.GetErrorCode() == ResetFailedCode) == r.GetSucceeded() {
			t.Errorf("record %v", r)
		}
	}
	if err := (&GateActivities{}).RecordConcurrencyReset(context.Background(), rec(ResetRecordApplied)); err == nil {
		t.Fatal("a record with no audit store was written nowhere and reported written")
	}
}

type resetCounts struct{ outcomes []string }

func (c *resetCounts) ObserveConcurrencyReset(_ context.Context, _, outcome string, _ bool) {
	c.outcomes = append(c.outcomes, outcome)
}

// TestGateWorkflow_AResetHoldsTheGateAgainstAnother: while the start that
// began a gate may still be repeated, the gate takes no other reset, says so
// in its memo, and takes one after.
func TestGateWorkflow_AResetHoldsTheGateAgainstAnother(t *testing.T) {
	h := newGateHarness(t)
	in := resetStart(h.env.Now())
	started := h.env.Now()
	within := h.freezeAt(in.Tunables.FreezeTimeout-time.Second, gateReset{ID: "z", Body: "b"})
	after := h.freezeAt(in.Tunables.FreezeTimeout+time.Second, gateReset{ID: "z", Body: "b"})
	h.env.RegisterDelayedCallback(func() {
		if h.memo == nil || !h.memo.FrozenUntil.Equal(started.Add(in.Tunables.FreezeTimeout)) {
			t.Errorf("memo %+v; want it held for %s", h.memo, in.Tunables.FreezeTimeout)
		}
	}, 45*time.Second)
	h.signalAt(5*time.Minute, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(6*time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.start(in)
	h.closedEmpty()
	if errType(within.failed) != GateErrBusy || after.failed != nil || after.frozen.Until.IsZero() {
		t.Fatalf("another reset within it = %v, after it = %v; want busy, then taken", within.failed, after.failed)
	}
}
