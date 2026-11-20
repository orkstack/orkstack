package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// gateHarness runs ConcurrencyGateWorkflow in the test environment, with
// its two local activities answered by the test.
type gateHarness struct {
	t        *testing.T
	env      *testsuite.TestWorkflowEnvironment
	list     func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error)
	describe func(DescribeConcurrencyEntriesInput) ([]gateRunFinding, error)
	passes   int
	granted  []ConcurrencyGrant
	grantAt  []time.Time
	grantErr func(ConcurrencyGrant) error
}

func newGateHarness(t *testing.T) *gateHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &gateHarness{t: t, env: generousDeadlock(suite.NewTestWorkflowEnvironment())}
	h.env.SetTestTimeout(30 * time.Second)
	// A gate that never closes ends at this virtual bound, a failure the
	// test sees, rather than running its timers forever.
	h.env.SetWorkflowRunTimeout(time.Hour)
	h.env.RegisterWorkflowWithOptions(ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: ConcurrencyGateWorkflowType})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		if h.list == nil {
			return ListConcurrencyEntriesResult{}, nil
		}
		return h.list(in)
	}, activity.RegisterOptions{Name: ActivityListConcurrencyEntries})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
		h.passes++
		if h.describe == nil {
			var out []gateRunFinding
			for _, r := range in.Runs {
				out = append(out, gateRunFinding{Run: r, Open: true, MemoSeq: 1, Keys: map[int]bool{gateRunLevel: true}})
			}
			return out, nil
		}
		return h.describe(in)
	}, activity.RegisterOptions{Name: ActivityDescribeConcurrencyEntries})
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", ConcurrencyGrantSignal, mock.Anything).Return(
		func(_, _, _, _ string, arg any) error {
			g := arg.(ConcurrencyGrant)
			h.granted = append(h.granted, g)
			h.grantAt = append(h.grantAt, h.env.Now())
			if h.grantErr != nil {
				return h.grantErr(g)
			}
			return nil
		})
	return h
}

// gateOutcome is one acquire's result.
type gateOutcome struct {
	answer   gateAnswer
	rejected error
	failed   error
}

type gateRecorder struct{ out *gateOutcome }

func (r gateRecorder) Accept()          {}
func (r gateRecorder) Reject(err error) { r.out.rejected = err }
func (r gateRecorder) Complete(success any, err error) {
	if err != nil {
		r.out.failed = err
		return
	}
	switch v := success.(type) {
	case gateAnswer:
		r.out.answer = v
	case converter.EncodedValue:
		r.out.failed = v.Get(&r.out.answer)
	}
}

// acquireAt sends an acquire at offset from the start, issued then.
func (h *gateHarness) acquireAt(offset time.Duration, req gateAcquire) *gateOutcome {
	out := &gateOutcome{}
	h.env.RegisterDelayedCallback(func() {
		req.IssuedAt = h.env.Now()
		req.WaitUntil = h.env.Now().Add(time.Hour)
		h.env.UpdateWorkflow(GateUpdateAcquire, "", gateRecorder{out: out}, req)
	}, offset)
	return out
}

func (h *gateHarness) signalAt(offset time.Duration, name string, arg any) {
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(name, arg) }, offset)
}

func (h *gateHarness) run(previous string, tunables GateTunables) {
	h.env.ExecuteWorkflow(ConcurrencyGateWorkflowType, GateStart{
		GateID: GateID("default", "", "deploy"), Namespace: "default", Key: "deploy",
		Tunables: tunables, Previous: previous,
	})
	if !h.env.IsWorkflowCompleted() {
		h.t.Fatal("the gate did not complete")
	}
}

func (h *gateHarness) closedEmpty() GateResult {
	h.t.Helper()
	if err := h.env.GetWorkflowError(); err != nil {
		h.t.Fatalf("the gate ended %v, want it closed empty", err)
	}
	var r GateResult
	if err := h.env.GetWorkflowResult(&r); err != nil || r.Closed != "empty" {
		h.t.Fatalf("the gate's result %+v, %v; want closed empty", r, err)
	}
	return r
}

func runAcquire(run string, limit int, strategy string) gateAcquire {
	return gateAcquire{Proto: gateProto, Run: run, SeqBase: 1, TaskIx: gateRunLevel, Limit: limit, Strategy: strategy, Label: "deploy"}
}

// A clean gate grants at once, queues a second run, grants it by signal
// when the first releases, and completes empty once nothing is left and the
// linger has passed.
func TestGateWorkflow_GrantsQueuesAndClosesEmpty(t *testing.T) {
	h := newGateHarness(t)
	first := h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	second := h.acquireAt(2*time.Second, runAcquire("r2", 1, gateStrategyQueue))
	h.signalAt(3*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(4*time.Second, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, DefaultGateTunables())
	r := h.closedEmpty()
	if first.answer.Answer != gateAnswerGranted || second.answer.Answer != gateAnswerQueued {
		t.Fatalf("answers %+v, %+v; want granted then queued", first, second)
	}
	if len(h.granted) != 1 || h.granted[0].Run != "r2" || h.granted[0].Seq != second.answer.Seq {
		t.Fatalf("grant signals %+v, want r2's at its seq", h.granted)
	}
	if r.Epoch != 0 || r.Seq != 2 {
		t.Fatalf("closed at epoch %d seq %d, want 0 and 2: a clean start starts no epoch", r.Epoch, r.Seq)
	}
}

// The validator refuses a newer protocol before anything is written, as a
// non-retryable CONCURRENCY_UNSUPPORTED.
func TestGateWorkflow_RefusesANewerProtocol(t *testing.T) {
	h := newGateHarness(t)
	req := runAcquire("r1", 1, gateStrategyQueue)
	req.Proto = gateProtoOrdinal + 1
	out := h.acquireAt(time.Second, req)
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	var app *temporal.ApplicationError
	if !errors.As(out.rejected, &app) || app.Type() != GateErrUnsupported || !app.NonRetryable() {
		t.Fatalf("rejection %v, want a non-retryable %s", out.rejected, GateErrUnsupported)
	}
}

// An absent predecessor rebuilds from the open runs' records before it
// decides, with no settle: a holder it restored keeps the key.
func TestGateWorkflow_AbsentStartRebuildsWithoutSettle(t *testing.T) {
	h := newGateHarness(t)
	held, _ := stablejson.Marshal(gateMemoEntry{Proto: 1, Held: true, Limit: 1, Strategy: gateStrategyQueue, ArrivedAt: time.Unix(0, 0).UTC(), Seq: 7, SeqBase: 1})
	h.list = func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		return ListConcurrencyEntriesResult{Records: []gateRunRecord{{Run: "old", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: held}}}}, nil
	}
	out := h.acquireAt(time.Second, runAcquire("new", 1, gateStrategyQueue))
	h.signalAt(time.Minute, GateSignalRelease, GateRelease{Run: "old", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(2*time.Minute, GateSignalRelease, GateRelease{Run: "new", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousAbsent, DefaultGateTunables())
	r := h.closedEmpty()
	if out.answer.Answer != gateAnswerQueued || out.answer.Seq != 8 {
		t.Fatalf("acquire = %+v, want queued behind the restored holder, at seq 8", out.answer)
	}
	if len(h.granted) != 1 || h.granted[0].Run != "new" {
		t.Fatalf("grants %+v, want new's once old released", h.granted)
	}
	if r.Epoch != 1 {
		t.Fatalf("epoch %d, want 1: a rebuild starts an epoch", r.Epoch)
	}
}

// An unclean predecessor rebuilds and then settles: nothing is granted
// until rebuild_settle has passed.
func TestGateWorkflow_UncleanStartSettlesBeforeGranting(t *testing.T) {
	h := newGateHarness(t)
	tunables := DefaultGateTunables()
	out := h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	h.signalAt(10*time.Minute, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	start := h.env.Now()
	h.run(GatePreviousUnclean, tunables)
	h.closedEmpty()
	if out.answer.Answer != gateAnswerQueued {
		t.Fatalf("acquire during the settle = %+v, want queued", out.answer)
	}
	if len(h.grantAt) != 1 || h.grantAt[0].Before(start.Add(tunables.RebuildSettle)) {
		t.Fatalf("granted at %v, want once, after the %s settle", h.grantAt, tunables.RebuildSettle)
	}
}

// A full key with an old holder runs a check pass before refusing; a holder
// the cluster no longer has is released, and the acquire is granted.
func TestGateWorkflow_APassReleasesAGoneHolder(t *testing.T) {
	h := newGateHarness(t)
	h.describe = func(in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
		var out []gateRunFinding
		for _, r := range in.Runs {
			out = append(out, gateRunFinding{Run: r, NotFound: r == "gone"})
		}
		return out, nil
	}
	h.acquireAt(time.Second, runAcquire("gone", 1, gateStrategyQueue))
	out := h.acquireAt(time.Minute, runAcquire("next", 1, gateStrategyRefuse))
	h.signalAt(2*time.Minute, GateSignalRelease, GateRelease{Run: "next", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	if out.answer.Answer != gateAnswerGranted || h.passes == 0 {
		t.Fatalf("refuse acquire = %+v after %d passes, want granted after a pass", out.answer, h.passes)
	}
}

// A grant to a run the cluster does not know releases that entry, so it
// never holds the key.
func TestGateWorkflow_AGrantToAnUnknownRunReleasesIt(t *testing.T) {
	h := newGateHarness(t)
	h.grantErr = func(ConcurrencyGrant) error { return &temporal.UnknownExternalWorkflowExecutionError{} }
	h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	h.acquireAt(2*time.Second, runAcquire("ghost", 1, gateStrategyQueue))
	h.signalAt(3*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	if len(h.granted) != 1 || h.granted[0].Run != "ghost" {
		t.Fatalf("grants %+v, want the one to ghost", h.granted)
	}
}

// Past gate_continue_updates the gate carries its state on, holders and
// sequence included, and the next execution is not told to rebuild.
func TestGateWorkflow_ContinuesAsNewCarryingItsState(t *testing.T) {
	h := newGateHarness(t)
	tunables := DefaultGateTunables()
	tunables.ContinueUpdates = 2
	for i, run := range []string{"a", "b", "c"} {
		h.acquireAt(time.Duration(i+1)*time.Second, runAcquire(run, 3, gateStrategyQueue))
	}
	h.run(GatePreviousClean, tunables)
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the gate ended %v, want it carried on", h.env.GetWorkflowError())
	}
	var next GateStart
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if next.Carry == nil || len(next.Carry.Holders) != 3 || next.Carry.Seq != 3 || next.Carry.Draining || next.Previous != "" {
		t.Fatalf("carried %+v (previous %q), want three holders at seq 3, not draining, no rebuild", next.Carry, next.Previous)
	}
}

// A grant whose signal keeps failing never holds the gate open: at the
// continuation the drain stops its retries, and the next execution is
// handed the holder still marked grant_pending, to send it again.
func TestGateWorkflow_AFailingGrantIsCarriedNotWaitedOn(t *testing.T) {
	h := newGateHarness(t)
	h.grantErr = func(ConcurrencyGrant) error { return errors.New("frontend unavailable") }
	tunables := DefaultGateTunables()
	tunables.ContinueUpdates = 3
	h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	h.acquireAt(2*time.Second, runAcquire("r2", 1, gateStrategyQueue))
	h.signalAt(3*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.acquireAt(time.Minute, runAcquire("r3", 1, gateStrategyQueue))
	h.acquireAt(2*time.Minute, runAcquire("r4", 1, gateStrategyQueue))
	h.run(GatePreviousClean, tunables)
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the gate ended %v, want it carried on", h.env.GetWorkflowError())
	}
	var next GateStart
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if len(next.Carry.Holders) != 1 || next.Carry.Holders[0].Run != "r2" || !next.Carry.Holders[0].GrantPending {
		t.Fatalf("carried holders %+v, want r2 still grant_pending", next.Carry.Holders)
	}
	if len(h.granted) < 2 {
		t.Fatalf("%d grant attempts, want the failure retried before the drain", len(h.granted))
	}
}

// A continuation that carries a grant_pending holder sends its grant first.
func TestGateWorkflow_SendsACarriedPendingGrant(t *testing.T) {
	h := newGateHarness(t)
	carry := &gateState{Tunables: DefaultGateTunables(), Phase: gatePhaseActive, Seq: 5,
		Holders: []gateEntry{{Run: "r2", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue,
			Proto: gateProto, Seq: 5, GrantPending: true}}}
	h.signalAt(time.Minute, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.env.ExecuteWorkflow(ConcurrencyGateWorkflowType, GateStart{GateID: GateID("default", "", "deploy"), Tunables: carry.Tunables, Carry: carry})
	h.closedEmpty()
	if len(h.granted) != 1 || h.granted[0].Run != "r2" || h.granted[0].Seq != 5 {
		t.Fatalf("grants %+v, want r2's carried grant sent once", h.granted)
	}
}

// A pass checks every entry, verify_batch runs per call, however many runs
// hold or queue at the key.
func TestGateWorkflow_APassChecksEveryEntry(t *testing.T) {
	h := newGateHarness(t)
	tunables := DefaultGateTunables()
	tunables.VerifyBatch = 2
	seen := map[string]bool{}
	h.describe = func(in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
		if len(in.Runs) > 2 {
			t.Errorf("one describe of %d runs, want at most verify_batch", len(in.Runs))
		}
		var out []gateRunFinding
		for _, r := range in.Runs {
			seen[r] = true
			out = append(out, gateRunFinding{Run: r, Open: true, MemoSeq: 1, Keys: map[int]bool{gateRunLevel: true}})
		}
		return out, nil
	}
	runs := []string{"a", "b", "c", "d", "e"}
	for i, r := range runs {
		h.acquireAt(time.Duration(i+1)*time.Second, runAcquire(r, 5, gateStrategyQueue))
		h.signalAt(20*time.Minute+time.Duration(i)*time.Second, GateSignalRelease, GateRelease{Run: r, SeqBase: 1, TaskIx: gateRunLevel})
	}
	h.run(GatePreviousClean, tunables)
	h.closedEmpty()
	for _, r := range runs {
		if !seen[r] {
			t.Fatalf("the sweep never checked %s (checked %v)", r, seen)
		}
	}
}

// One release delivered in the workflow task that carries the gate on is
// applied before the carry, never left behind. The test environment runs
// coroutines in an order that cannot reproduce the SDK handing a signal to
// a blocked Receive, the case drainSignals and the buffered receivers
// close; that one is proven against a real cluster
// (TestIntegration_GateReleaseAtContinueIsNotLost).
func TestGateWorkflow_AReleaseInTheContinuationTaskIsCarried(t *testing.T) {
	h := newGateHarness(t)
	tunables := DefaultGateTunables()
	tunables.ContinueUpdates = 2
	h.acquireAt(time.Second, runAcquire("a", 3, gateStrategyQueue))
	h.acquireAt(2*time.Second, runAcquire("b", 3, gateStrategyQueue))
	out := &gateOutcome{}
	h.env.RegisterDelayedCallback(func() {
		req := runAcquire("c", 3, gateStrategyQueue)
		req.IssuedAt, req.WaitUntil = h.env.Now(), h.env.Now().Add(time.Hour)
		h.env.SignalWorkflow(GateSignalRelease, GateRelease{Run: "a", SeqBase: 1, TaskIx: gateRunLevel})
		h.env.UpdateWorkflow(GateUpdateAcquire, "", gateRecorder{out: out}, req)
	}, 3*time.Second)
	h.run(GatePreviousClean, tunables)
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the gate ended %v, want it carried on", h.env.GetWorkflowError())
	}
	var next GateStart
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	for _, e := range next.Carry.Holders {
		if e.Run == "a" {
			t.Fatalf("carried holders %+v still hold a, released in the continuation's task", next.Carry.Holders)
		}
	}
	if _, fenced := next.Carry.tombFor("a", 1, gateRunLevel, 0, h.env.Now()); !fenced {
		t.Fatal("a's release left no tombstone in the carry")
	}
}

// A run that gave its slot back asks again with n=1 before its release(0)
// arrives: the gate replaces its held entry, grants the run waiting behind
// it by signal in the same decision, queues the re-acquire at the tail, and
// the late release(0) frees nothing.
func TestGateWorkflow_AnOrdinalReplacementGrantsTheWaiter(t *testing.T) {
	h := newGateHarness(t)
	first := h.acquireAt(time.Second, runAcquire("r1", 1, gateStrategyQueue))
	waiter := h.acquireAt(2*time.Second, runAcquire("r2", 1, gateStrategyQueue))
	again := runAcquire("r1", 1, gateStrategyQueue)
	again.N, again.Proto, again.Requeue = 1, gateProtoOrdinal, true
	reacquired := h.acquireAt(3*time.Second, again)
	h.signalAt(4*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(5*time.Second, GateSignalRelease, GateRelease{Run: "r2", SeqBase: 1, TaskIx: gateRunLevel})
	h.signalAt(6*time.Second, GateSignalRelease, GateRelease{Run: "r1", SeqBase: 1, TaskIx: gateRunLevel, N: 1})
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	if first.answer.Answer != gateAnswerGranted || waiter.answer.Answer != gateAnswerQueued {
		t.Fatalf("answers %+v, %+v; want granted then queued", first.answer, waiter.answer)
	}
	if reacquired.answer.Answer != gateAnswerQueued || reacquired.answer.Position != 1 || reacquired.answer.HoldersAhead != 1 {
		t.Fatalf("the re-acquire = %+v, want first in line behind r2", reacquired.answer)
	}
	if len(h.granted) != 2 || h.granted[0].Run != "r2" || h.granted[0].N != 0 || h.granted[1].Run != "r1" || h.granted[1].N != 1 {
		t.Fatalf("grant signals %+v; want r2's, then r1's at ordinal 1 once r2 released", h.granted)
	}
}

// An acquire that arrives with the gate's start (an Update-with-Start
// beside a holder the rebuild is about to restore) is never granted past
// that holder: the gate is rebuilding from its first instant, so the acquire
// queues and is granted only once the holder releases.
func TestGateWorkflow_AnAcquireWithTheStartWaitsForTheRebuild(t *testing.T) {
	for _, previous := range []string{GatePreviousAbsent, GatePreviousUnclean} {
		t.Run(previous, func(t *testing.T) {
			h := newGateHarness(t)
			held, _ := stablejson.Marshal(gateMemoEntry{Proto: 1, Held: true, Limit: 1, Strategy: gateStrategyQueue,
				ArrivedAt: time.Unix(0, 0).UTC(), Seq: 7, SeqBase: 1})
			h.list = func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
				return ListConcurrencyEntriesResult{Records: []gateRunRecord{{Run: "old", MemoSeq: 1,
					Entries: map[int]stablejson.RawJSON{gateRunLevel: held}}}}, nil
			}
			out := h.acquireAt(0, runAcquire("new", 1, gateStrategyQueue))
			h.signalAt(time.Hour/2, GateSignalRelease, GateRelease{Run: "old", SeqBase: 1, TaskIx: gateRunLevel})
			h.signalAt(time.Hour/2+time.Minute, GateSignalRelease, GateRelease{Run: "new", SeqBase: 1, TaskIx: gateRunLevel})
			h.run(previous, DefaultGateTunables())
			h.closedEmpty()
			if out.answer.Answer != gateAnswerQueued {
				t.Fatalf("the acquire sent with the start = %+v, want queued behind the holder the rebuild restores", out.answer)
			}
			if len(h.granted) != 1 || h.granted[0].Run != "new" {
				t.Fatalf("grants %+v, want new's once old released", h.granted)
			}
		})
	}
}
