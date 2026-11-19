package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The pause side of a run-level limit (concurrency design §22.5): a paused,
// quiet run gives its slot back with the single-slot release, and on unpause
// asks for it again with the next ordinal, queue strategy, at the back of
// the line.

// pausedConcHarness is a concRunHarness whose plan can be paused, whose first
// task takes ten seconds, and which records every single-slot release.
type pausedConcHarness struct {
	*concRunHarness
	single []GateRelease
	// queuedSeen is every Run.queued a snapshot carried, in order.
	queuedSeen []*orkstackv1.ConcurrencyQueued
	// fail names the tasks whose activity fails; ranIDs is every task
	// that ran, in order.
	fail   map[string]bool
	ranIDs []string
}

func newPausedConcHarness(t *testing.T, answers ...AcquireConcurrencyResult) *pausedConcHarness {
	t.Helper()
	h := &pausedConcHarness{concRunHarness: newConcRunHarness(t, answers...)}
	h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			h.mu.Lock()
			h.ran++
			h.ranIDs = append(h.ranIDs, in.GetTaskId())
			failing := h.fail[in.GetTaskId()]
			h.mu.Unlock()
			if failing {
				return nil, temporal.NewNonRetryableApplicationError("exit 1", "ActivityFailed", nil)
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, ValuesJson: []byte(`{}`)}, nil
		}).After(10 * time.Second)
	h.env.OnActivity(ActivityWriteSnapshot, mock.Anything, mock.Anything, mock.Anything).Return(
		func(_ context.Context, run *orkstackv1.Run, _ int64) (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.last = run
			if q := run.GetQueued(); q != nil {
				h.queuedSeen = append(h.queuedSeen, q)
			}
			return "memory://x", nil
		})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-test", "", GateSignalRelease, mock.Anything).Return(
		func(_, _, _, _ string, arg any) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.single = append(h.single, arg.(GateRelease))
			return nil
		})
	return h
}

func pausedConcPlan(t *testing.T) RunPlan {
	plan := concPlan()
	plan.Pause = pausePlan(t).Pause
	return plan
}

func twoStepConcOrkflow() *orkstackv1.Orkflow {
	wf := concOrkflow()
	wf.Tasks = []*orkstackv1.Task{
		{Id: "build", Image: &orkstackv1.ImageRef{Reference: "alpine"}},
		{Id: "ship", Image: &orkstackv1.ImageRef{Reference: "alpine"}, Needs: []string{"build"}},
	}
	return wf
}

func (h *pausedConcHarness) pauseAt(d time.Duration, pause bool) {
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(pause, "0")) }, d)
}

func (h *pausedConcHarness) grantAt(d time.Duration, seq, n int64) {
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: seq, N: n})
	}, d)
}

func (h *pausedConcHarness) runTwoStep(t *testing.T) {
	t.Helper()
	h.runOrkflow(pausedConcPlan(t), twoStepConcOrkflow())
}

func (h *pausedConcHarness) withType(eventType orkstackv1.EventType) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range h.events {
		if e.GetType() == eventType {
			out = append(out, e)
		}
	}
	return out
}

// TestConcurrencyPause_AQuietPausedRunGivesItsSlotBackAndAsksAgain: paused
// while build runs, the run gives the slot back once build ends, with the
// single-slot release of ordinal 0, never release_all; unpaused, it asks
// again with ordinal 1, queue, marked requeue, and ship runs on the grant.
func TestConcurrencyPause_AQuietPausedRunGivesItsSlotBackAndAsksAgain(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), granted(5))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if len(h.single) != 1 || h.single[0] != (GateRelease{Run: concRunID, SeqBase: 1, TaskIx: gateRunLevel}) {
		t.Fatalf("single releases %+v, want one of ordinal 0 at the give-back", h.single)
	}
	if len(h.acquires) != 2 {
		t.Fatalf("%d acquires, want the arrival's and the re-acquire", len(h.acquires))
	}
	re := h.acquires[1].Request
	if re.N != 1 || re.Proto != gateProtoOrdinal || !re.Requeue || re.Strategy != gateStrategyQueue || re.TaskIx != gateRunLevel {
		t.Fatalf("the re-acquire = %+v, want ordinal 1, protocol 2, queue, requeue", re)
	}
	if h.ran != 2 || len(h.releases) != 1 {
		t.Fatalf("ran %d, release_all %d; want 2 and 1 (on the way out)", h.ran, len(h.releases))
	}
	if len(h.withType(orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED)) != 0 {
		t.Fatal("a granted re-acquire emitted RUN_QUEUED")
	}
}

// TestConcurrencyPause_ARunUnpausedBeforeItIsQuietKeepsItsSlot: a pause
// lifted while build still runs gives nothing back and asks for nothing.
func TestConcurrencyPause_ARunUnpausedBeforeItIsQuietKeepsItsSlot(t *testing.T) {
	h := newPausedConcHarness(t, granted(1))
	h.pauseAt(2*time.Second, true)
	h.pauseAt(4*time.Second, false)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if len(h.single) != 0 || len(h.acquires) != 1 || h.ran != 2 {
		t.Fatalf("single releases %v, acquires %d, ran %d; want none, 1, 2", h.single, len(h.acquires), h.ran)
	}
}

// TestConcurrencyPause_AReQueuedRunWaitsRunningAndStartsOnItsGrant: a
// re-acquire answered queued emits RUN_QUEUED RUNNING, holds ship, ignores
// a grant of the given-back entry (ordinal 0), and starts ship on its own.
func TestConcurrencyPause_AReQueuedRunWaitsRunningAndStartsOnItsGrant(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), queued(7, 2))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.grantAt(time.Hour+10*time.Second, 1, 0) // a late grant of the old entry
	h.grantAt(time.Hour+20*time.Second, 7, 1)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	q := h.withType(orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED)
	if len(q) != 1 || q[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING || !strings.Contains(q[0].GetMessage(), "after an unpause") {
		t.Fatalf("RUN_QUEUED events %v, want one RUNNING, after an unpause", q)
	}
	if len(h.queuedSeen) == 0 || h.queuedSeen[0].GetCause() != orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_UNPAUSE {
		t.Fatalf("the queued snapshots = %v, want cause UNPAUSE", h.queuedSeen)
	}
	if h.ran != 2 || h.last.GetQueued() != nil {
		t.Fatalf("ran %d, final queued %v; want 2 and none", h.ran, h.last.GetQueued())
	}
}

// TestConcurrencyPause_APauseReachingTheWaitLeavesTheLine: paused again
// while it waits, the run releases its entry in line (ordinal 1) and stays
// given back; the next unpause asks with ordinal 2.
func TestConcurrencyPause_APauseReachingTheWaitLeavesTheLine(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), queued(7, 2), granted(9))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.pauseAt(time.Hour+30*time.Second, true)
	h.pauseAt(2*time.Hour, false)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if len(h.single) != 2 || h.single[1].N != 1 {
		t.Fatalf("single releases %+v, want the give-back (0) then the line entry (1)", h.single)
	}
	if len(h.acquires) != 3 || h.acquires[2].Request.N != 2 {
		t.Fatalf("acquires %d, the last with ordinal %d; want 3, ordinal 2", len(h.acquires), h.acquires[len(h.acquires)-1].Request.N)
	}
	if h.ran != 2 {
		t.Fatalf("ran %d, want 2", h.ran)
	}
}

// TestConcurrencyPause_AReQueueThatTimesOutFailsTheRun: a re-queue nobody
// grants ends FAILED RUN_QUEUE_TIMEOUT, saying it was after an unpause, and
// ship never runs.
func TestConcurrencyPause_AReQueueThatTimesOutFailsTheRun(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), queued(7, 2), queued(7, 2), queued(7, 2), queued(7, 2), queued(7, 2),
		queued(7, 2), queued(7, 2), queued(7, 2), queued(7, 2), queued(7, 2))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.runTwoStep(t)
	err := h.env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "after an unpause without continuing") {
		t.Fatalf("the run ended %v, want the re-queue's timeout", err)
	}
	if h.last.GetError().GetCode() != RunQueueTimeoutCode || h.ran != 1 {
		t.Fatalf("error %v, ran %d; want RUN_QUEUE_TIMEOUT and only build", h.last.GetError(), h.ran)
	}
}

// TestConcurrencyPause_AFullLineIsAskedAgainUntilItsDeadline: a line at its
// bound keeps the run waiting (LINE_FULL), asked again with the acquire
// back-off, until it is granted.
func TestConcurrencyPause_AFullLineIsAskedAgainUntilItsDeadline(t *testing.T) {
	full := AcquireConcurrencyResult{Answer: gateAnswer{Proto: 2, Answer: gateAnswerFullBound}}
	h := newPausedConcHarness(t, granted(1), full, full, granted(9))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	q := h.withType(orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED)
	if len(q) != 1 || len(h.queuedSeen) == 0 || h.queuedSeen[0].GetNoteCode() != queuedNoteLineFull || h.queuedSeen[0].GetPositionKnown() {
		t.Fatalf("RUN_QUEUED %v, queued snapshots %v; want one saying the line is full", q, h.queuedSeen)
	}
	if len(h.acquires) != 4 || h.ran != 2 {
		t.Fatalf("acquires %d, ran %d; want 4 and 2", len(h.acquires), h.ran)
	}
}

// TestConcurrencyPause_TheOrdinalAndTheGiveBackCrossAContinuation: a run
// carried on while its slot is given back is still given back after the
// switch, and asks with the carried ordinal's next.
func TestConcurrencyPause_TheOrdinalAndTheGiveBackCrossAContinuation(t *testing.T) {
	plan := concPlan()
	c := newConcRunState(plan.Concurrency)
	c.pausable = true
	c.noteAttempted("ork-conc-test")
	c.ordinal, c.givenBack = 3, true
	carried := c.carry()
	next := newConcRunState(plan.Concurrency)
	next.pausable = true
	if err := next.adoptCarry(carried); err != nil {
		t.Fatalf("adoptCarry: %v", err)
	}
	if next.ordinal != 3 || !next.givenBack || next.held != nil {
		t.Fatalf("after the switch: ordinal %d, given back %v, held %v; want 3, true, none", next.ordinal, next.givenBack, next.held)
	}
	holding := newConcRunState(plan.Concurrency)
	holding.held = &heldSlot{gate: "ork-conc-test", seq: 4}
	again := newConcRunState(plan.Concurrency)
	if err := again.adoptCarry(holding.carry()); err != nil || again.givenBack || again.held == nil || again.ordinal != 0 {
		t.Fatalf("a holder carried on: %v, given back %v, held %v, ordinal %d", err, again.givenBack, again.held, again.ordinal)
	}
}

// awaitWithCleanup is build, an await open from the start, and a cleanup
// failure handler on the await: the await expires while the run is paused
// with its slot given back.
func awaitWithCleanup() *orkstackv1.Orkflow {
	wf := concOrkflow()
	wf.Tasks = []*orkstackv1.Task{
		{Id: "build", Image: &orkstackv1.ImageRef{Reference: "alpine"}},
		{Id: "approve", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 120}},
		{Id: "cleanup", Image: &orkstackv1.ImageRef{Reference: "alpine"},
			Needs: []string{"approve"}, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED},
	}
	return wf
}

// TestConcurrencyPause_AFailingRunAsksForItsSlotAgainToRunItsHandlers: a
// run whose open await expires while it is paused with its slot given back
// asks for the slot again once unpaused (cause FAILING), runs its failure
// handler inside the limit, and ends FAILED with its own error (D22).
func TestConcurrencyPause_AFailingRunAsksForItsSlotAgainToRunItsHandlers(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), queued(7, 1))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.grantAt(time.Hour+30*time.Second, 7, 1)
	h.runOrkflow(pausedConcPlan(t), awaitWithCleanup())
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded; want its await to fail it")
	}
	if len(h.acquires) != 2 || h.acquires[1].Request.N != 1 || !h.acquires[1].Request.Requeue {
		t.Fatalf("acquires %d; want the arrival and the failing run's ask with ordinal 1", len(h.acquires))
	}
	if len(h.queuedSeen) == 0 || h.queuedSeen[len(h.queuedSeen)-1].GetCause() != orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_FAILING {
		t.Fatalf("queued snapshots %v, want the failing run's wait, cause FAILING", h.queuedSeen)
	}
	if len(h.ranIDs) != 2 || h.ranIDs[1] != "cleanup" {
		t.Fatalf("ran %v, want build then the cleanup handler", h.ranIDs)
	}
	if h.last.GetStatus() != orkstackv1.Status_STATUS_FAILED || strings.HasPrefix(h.last.GetError().GetCode(), "RUN_CONCURRENCY") ||
		strings.Contains(h.last.GetError().GetMessage(), "did not run") {
		t.Fatalf("the run ended %s %v, want FAILED with its own error", h.last.GetStatus(), h.last.GetError())
	}
}

// TestConcurrencyPause_AFailingRunThatDoesNotRegainItsSlotSaysItsHandlersDidNotRun:
// the same run, whose wait for the slot times out, ends FAILED with its own
// error, and both the error and the handler say the handlers did not run,
// and why.
func TestConcurrencyPause_AFailingRunThatDoesNotRegainItsSlotSaysItsHandlersDidNotRun(t *testing.T) {
	answers := []AcquireConcurrencyResult{granted(1)}
	for range 12 {
		answers = append(answers, queued(7, 1))
	}
	h := newPausedConcHarness(t, answers...)
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.runOrkflow(pausedConcPlan(t), awaitWithCleanup())
	if len(h.ranIDs) != 1 {
		t.Fatalf("ran %v, want build alone", h.ranIDs)
	}
	const why = "failure handlers did not run: the concurrency slot on key deploy was not regained within 1h0m0s (RUN_QUEUE_TIMEOUT)"
	if h.last.GetStatus() != orkstackv1.Status_STATUS_FAILED || strings.HasPrefix(h.last.GetError().GetCode(), "RUN_CONCURRENCY") ||
		h.last.GetError().GetCode() == RunQueueTimeoutCode || !strings.Contains(h.last.GetError().GetMessage(), why) {
		t.Fatalf("the run ended %s %v, want FAILED with its own error and %q", h.last.GetStatus(), h.last.GetError(), why)
	}
	if cleanup := rowOf(h.last, "cleanup"); cleanup.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("cleanup %s, want CANCELED", cleanup.GetStatus())
	}
	said := false
	for _, e := range h.withType(orkstackv1.EventType_EVENT_TYPE_TASK_LOG) {
		said = said || (e.GetTaskId() == "cleanup" && strings.Contains(e.GetMessage(), why))
	}
	if !said {
		t.Fatalf("no event says why cleanup did not run (%q)", why)
	}
	if h.last.GetQueued() != nil {
		t.Fatalf("the ended run still reads as queued: %v", h.last.GetQueued())
	}
}

// TestConcurrencyPause_ARunFailingWhilePausedKeepsItsSlotForItsHandlers: a
// run that fails while paused, before it is quiet, keeps its slot through
// its failing phase; its handler runs after the unpause, with no release
// and no second acquire.
func TestConcurrencyPause_ARunFailingWhilePausedKeepsItsSlotForItsHandlers(t *testing.T) {
	h := newPausedConcHarness(t, granted(1))
	h.fail = map[string]bool{"build": true}
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	wf := concOrkflow()
	wf.Tasks = []*orkstackv1.Task{
		{Id: "build", Image: &orkstackv1.ImageRef{Reference: "alpine"}},
		{Id: "cleanup", Image: &orkstackv1.ImageRef{Reference: "alpine"},
			Needs: []string{"build"}, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED},
	}
	h.runOrkflow(pausedConcPlan(t), wf)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded; want build to fail it")
	}
	if len(h.single) != 0 || len(h.acquires) != 1 {
		t.Fatalf("single releases %v, acquires %d; want none and 1", h.single, len(h.acquires))
	}
	if len(h.ranIDs) != 2 || h.ranIDs[1] != "cleanup" {
		t.Fatalf("ran %v, want build then cleanup", h.ranIDs)
	}
}

// TestConcurrencyPause_ALimitThatDoesNotAnswerTheReacquireKeepsTheRunWaiting:
// a run that has done work is never ended at once by a limit that does not
// answer: it waits, saying so (NOT_ANSWERING), asks again, and runs on.
func TestConcurrencyPause_ALimitThatDoesNotAnswerTheReacquireKeepsTheRunWaiting(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), AcquireConcurrencyResult{Unavailable: "frontend unavailable"}, granted(9))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if len(h.queuedSeen) == 0 || h.queuedSeen[0].GetNoteCode() != QueuedNoteNotAnswering || h.queuedSeen[0].GetPositionKnown() {
		t.Fatalf("queued snapshots %v, want one saying the limit is not answering", h.queuedSeen)
	}
	if len(h.acquires) != 3 || h.ran != 2 {
		t.Fatalf("acquires %d, ran %d; want 3 and 2", len(h.acquires), h.ran)
	}
}

// TestConcurrencyPause_AGrantUnderAnotherSeqAsksTheGateAtOnce: a grant of
// the run's own slot under a seq it does not know (a rebuild renumbered the
// line) brings the re-assert forward rather than waiting for it.
func TestConcurrencyPause_AGrantUnderAnotherSeqAsksTheGateAtOnce(t *testing.T) {
	h := newPausedConcHarness(t, granted(1), queued(7, 2), granted(12))
	h.pauseAt(5*time.Second, true)
	h.pauseAt(time.Hour, false)
	h.grantAt(time.Hour+10*time.Second, 12, 1)
	h.runTwoStep(t)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if len(h.acquires) != 3 {
		t.Fatalf("acquires %d, want the re-assert the grant brought forward", len(h.acquires))
	}
	if at := h.acquires[2].Request.IssuedAt; at.After(h.acquires[1].Request.IssuedAt.Add(time.Minute)) {
		t.Fatalf("the re-assert was sent at %s, after the re-queue interval; want at the grant", at)
	}
}

// TestConcurrencyPause_ARecordedReacquireCarriesItsOrdinal: the memo entry
// of a slot asked for again names its ordinal, so a rebuilding gate restores
// it as the run will release it.
func TestConcurrencyPause_ARecordedReacquireCarriesItsOrdinal(t *testing.T) {
	plan := concPlan()
	c := newConcRunState(plan.Concurrency)
	c.held, c.ordinal = &heldSlot{gate: "ork-conc-test", seq: 4}, 2
	e := c.heldMemoEntry(plan.Concurrency.runSlot())
	if e.Proto != gateProtoOrdinal || e.N != 2 || e.Strategy != gateStrategyQueue {
		t.Fatalf("memo entry %+v, want protocol 2, ordinal 2, queue", e)
	}
	c.ordinal = 0
	if e := c.heldMemoEntry(plan.Concurrency.runSlot()); e.Proto != gateProtoLegacy || e.N != 0 {
		t.Fatalf("memo entry %+v, want the legacy entry at ordinal 0", e)
	}
}

// TestConcurrencyPause_ACarryThatCannotBeAGiveBackIsRefused: a carry with
// two ordinals, an ordinal of 0, or no slot for a run that cannot be paused
// is refused, never adopted as a given-back slot.
func TestConcurrencyPause_ACarryThatCannotBeAGiveBackIsRefused(t *testing.T) {
	plan := concPlan()
	ord := func(n int64) *orkstackv1.SlotOrdinal {
		return &orkstackv1.SlotOrdinal{GateId: "ork-conc-test", TaskIx: gateRunLevel, N: n}
	}
	for name, tc := range map[string]struct {
		carry    *orkstackv1.RunConcurrencyCarry
		pausable bool
	}{
		"two ordinals": {&orkstackv1.RunConcurrencyCarry{Ordinals: []*orkstackv1.SlotOrdinal{ord(1), ord(2)}}, true},
		"ordinal zero": {&orkstackv1.RunConcurrencyCarry{Ordinals: []*orkstackv1.SlotOrdinal{ord(0)}}, true},
		"not pausable": {&orkstackv1.RunConcurrencyCarry{}, false},
		"another gate": {&orkstackv1.RunConcurrencyCarry{Ordinals: []*orkstackv1.SlotOrdinal{{GateId: "x", TaskIx: gateRunLevel, N: 1}}}, true},
	} {
		c := newConcRunState(plan.Concurrency)
		c.pausable = tc.pausable
		if err := c.adoptCarry(tc.carry); err == nil {
			t.Errorf("%s: adopted %v", name, tc.carry)
		}
	}
}
