package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumedConcHarness is a run held to a limit, executed as the reset run of a
// resume at sequence 42 that runs its one task again.
type resumedConcHarness struct {
	*concRunHarness
	// steps is what the resume issues, in order.
	steps []string
}

func newResumedConcHarness(t *testing.T, answers ...AcquireConcurrencyResult) *resumedConcHarness {
	t.Helper()
	h := &resumedConcHarness{concRunHarness: newConcRunHarness(t, answers...)}
	wf := concOrkflow()
	block := &orkstackv1.ResumeBlock{SeqBase: 42, Scope: 42, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME}
	must.OK(t, EncodeResumeUnits(wf, block, []ResumeUnitPlan{{Task: "apply", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN}}, false))
	record := &orkstackv1.ResumePlanRecord{RunId: concRunID, Scope: 42, BaseTerminalSequence: 41, Block: block}
	h.env.RegisterActivityWithOptions(func(context.Context, ReadResetFactsInput) (ResetFacts, error) {
		return ResetFacts{Reason: ResumePlanKey{Scope: 42, PlanDigest: strings.Repeat("ab", 32)}.String()}, nil
	}, activity.RegisterOptions{Name: ActivityReadResetFacts})
	h.env.RegisterActivityWithOptions(func(context.Context, LoadResumePlanInput) (*orkstackv1.ResumePlanRecord, error) {
		return record, nil
	}, activity.RegisterOptions{Name: ActivityLoadResumePlan})
	h.env.RegisterActivityWithOptions(NewResumeActivities(artifacts.NewMemoryCommitStore(), nil).AdoptRecorded,
		activity.RegisterOptions{Name: ActivityAdoptRecorded})
	h.env.OnUpsertMemo(mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		for k, v := range args.Get(0).(map[string]any) {
			v = payloadcrypt.Unplain(v)
			if k == gateMemoSeqKey {
				h.steps = append(h.steps, fmt.Sprintf("seq %v", v))
			} else if v == nil {
				h.steps = append(h.steps, "cleared")
			}
		}
	})
	return h
}

// resumedConcWorkflow is the run of concOrkflow as a resume's reset run
// executes it.
func resumedConcWorkflow(ctx workflow.Context, plan RunPlan) (*orkstackv1.Run, error) {
	deps := workflowDeps{isResetRun: func(*workflow.Info) bool { return true }, limitUnbuilt: ResumeLimitUnbuilt}
	return runWorkflowReleasing(ctx, deps, concRunID, concOrkflow(), &orkstackv1.Payload{}, plan, nil)
}

func (h *resumedConcHarness) resume(plan RunPlan) {
	h.env.ExecuteWorkflow("resumed", plan)
	if !h.env.IsWorkflowCompleted() {
		h.t.Fatal("the run did not end")
	}
}

func (h *resumedConcHarness) eventOf(t orkstackv1.EventType) *orkstackv1.Event {
	for _, e := range h.events {
		if e.GetType() == t {
			return e
		}
	}
	return nil
}

// TestResumedRun_RetakesItsSlotAnew: after RUN_RESULTS_KEPT a resumed run
// gives up its earlier incarnations' slots at every gate of its plan, records
// its own, and only then asks for its slot, at the resume's sequence; its tasks
// run under it, and it releases above it as it ends.
func TestResumedRun_RetakesItsSlotAnew(t *testing.T) {
	h := newResumedConcHarness(t, granted(1), granted(7))
	plan := concPlan()
	// A second gate the prefix never asked: a task-level one (E88 P7).
	plan.Concurrency.Slots = append(plan.Concurrency.Slots, ConcurrencySlotPlan{GateID: "ork-conc-task", Key: "t", Label: "t", Limit: 1, Strategy: gateStrategyQueue})
	var other []GateReleaseAll
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-task", "", GateSignalReleaseAll, mock.Anything).Return(
		func(_, _, _, _ string, arg any) error {
			if len(h.acquires) != 1 {
				t.Errorf("an earlier slot was released after %d acquires; want before the resume's", len(h.acquires))
			}
			other = append(other, payloadcrypt.Unplain(arg).(GateReleaseAll))
			return nil
		})
	h.resume(plan)
	must.OK(t, h.env.GetWorkflowError())
	if len(h.acquires) != 2 || h.acquires[0].Request.SeqBase != 1 || h.acquires[1].Request.SeqBase != 42 ||
		h.acquires[1].Request.N != 0 || h.acquires[1].Request.Strategy != gateStrategyQueue || h.acquires[1].UpdateID == h.acquires[0].UpdateID {
		t.Fatalf("acquires %+v; want the prefix's, then the resume's at 42", h.acquires)
	}
	want := []GateReleaseAll{{Run: concRunID, Below: 42}, {Run: concRunID, Below: 43}}
	if len(h.releases) != 2 || h.releases[0] != want[0] || h.releases[1] != want[1] || len(other) != 1 || other[0] != want[0] {
		t.Fatalf("releases %+v, at the gate it never asked %+v; want below 42 at both, then below 43", h.releases, other)
	}
	if h.ran != 1 {
		t.Fatalf("%d tasks ran, want the one run again", h.ran)
	}
	if got := strings.Join(h.steps, ", "); !strings.Contains(got, "seq 1") || !strings.Contains(got, "cleared") || !strings.HasSuffix(strings.ReplaceAll(got, ", cleared", ""), "seq 42") {
		t.Fatalf("memo writes: %s; want the resume's incarnation recorded last, and the old entry dropped", got)
	}
}

// TestResumedRun_WaitsInLineRunning: a resumed run whose key is full waits
// RUNNING, says it was resumed, and goes on when its own grant arrives; a grant
// of its earlier incarnation is not one.
func TestResumedRun_WaitsInLineRunning(t *testing.T) {
	h := newResumedConcHarness(t, granted(1), queued(7, 2))
	grant := func(seqBase int64) func() {
		return func() {
			h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: seqBase, Seq: 7})
		}
	}
	h.env.RegisterDelayedCallback(grant(1), 10*time.Second)
	h.env.RegisterDelayedCallback(func() {
		if h.ran != 0 {
			t.Error("a grant of an earlier incarnation started the resumed run")
		}
		grant(42)()
	}, 20*time.Second)
	h.resume(concPlan())
	must.OK(t, h.env.GetWorkflowError())
	q := h.eventOf(orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED)
	if q == nil || q.GetStatus() != orkstackv1.Status_STATUS_RUNNING || !strings.HasPrefix(q.GetMessage(), "resumed, and waiting for key deploy") {
		t.Fatalf("RUN_QUEUED = %v; want it RUNNING and resumed", q)
	}
	if h.ran != 1 || len(h.releases) != 2 || h.releases[1].Below != 43 {
		t.Fatalf("ran %d, releases %+v", h.ran, h.releases)
	}
	var ended *orkstackv1.Event
	for _, e := range h.events {
		if e.GetWaitEnd() != nil {
			ended = e
			break
		}
	}
	if ended == nil || ended.GetWaitEnd().GetOutcome() != orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED || h.last.GetQueued() != nil {
		t.Fatalf("the wait ended %v, run queued %v; want started, and not queued", ended.GetWaitEnd(), h.last.GetQueued())
	}
}

// TestResumedRun_TheLimitsEndings: what the limit refuses a resumed run ends
// it FAILED with that reason, running nothing,
// and releasing; a cancel while it waits ends it canceled.
func TestResumedRun_TheLimitsEndings(t *testing.T) {
	full := AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFull}}
	for name, c := range map[string]struct {
		answer AcquireConcurrencyResult
		cancel bool
		code   string
		status orkstackv1.Status
	}{
		"refused":      {answer: full, code: RunConcurrencyLimitCode, status: orkstackv1.Status_STATUS_FAILED},
		"not answered": {answer: AcquireConcurrencyResult{Unavailable: "down"}, code: RunConcurrencyUnavailableCode, status: orkstackv1.Status_STATUS_FAILED},
		"timed out":    {answer: queued(7, 2), code: RunQueueTimeoutCode, status: orkstackv1.Status_STATUS_FAILED},
		"canceled":     {answer: queued(7, 2), cancel: true, code: "RUN_CANCELED", status: orkstackv1.Status_STATUS_CANCELED},
	} {
		t.Run(name, func(t *testing.T) {
			answers := []AcquireConcurrencyResult{granted(1), c.answer}
			for range 10 { // the wait's re-asserts
				answers = append(answers, queued(7, 2))
			}
			h := newResumedConcHarness(t, answers...)
			if c.cancel {
				h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 5*time.Second)
			}
			h.resume(concPlan())
			if h.env.GetWorkflowError() == nil {
				t.Fatal("the run succeeded without its slot")
			}
			last := h.events[len(h.events)-1]
			if h.last.GetError().GetCode() != c.code || last.GetStatus() != c.status || h.ran != 0 {
				t.Fatalf("ended %v %s after %d tasks; want %v %s and none", last.GetStatus(), h.last.GetError().GetCode(), h.ran, c.status, c.code)
			}
			if n := len(h.releases); n != 2 || h.releases[1].Below != 43 {
				t.Fatalf("releases %+v; want the resume's, then below 43", h.releases)
			}
			if h.last.GetQueued() != nil {
				t.Fatalf("the ended run still shows its place: %v", h.last.GetQueued())
			}
		})
	}
}

// TestResumedRun_AFailedReleaseOfAnEarlierSlotIsNoEnding: a gate that cannot
// take the release is left to its own check pass, and the run goes on.
func TestResumedRun_AFailedReleaseOfAnEarlierSlotIsNoEnding(t *testing.T) {
	h := newResumedConcHarness(t, granted(1), granted(7))
	plan := concPlan()
	plan.Concurrency.Slots = append(plan.Concurrency.Slots, ConcurrencySlotPlan{GateID: "ork-conc-task", Key: "t", Label: "t", Limit: 1, Strategy: gateStrategyQueue})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-task", "", GateSignalReleaseAll, mock.Anything).Return(
		func(_, _, _, _ string, _ any) error { return temporal.NewApplicationError("no", "SIGNAL_LIMIT") })
	h.resume(plan)
	must.OK(t, h.env.GetWorkflowError())
	if h.ran != 1 {
		t.Fatalf("ran %d", h.ran)
	}
}

// TestSeedAttempted_ACarriedResumeKeepsItsIncarnation: a generation after a
// resumed one releases and asks under the resume's sequence, never the plan's.
func TestSeedAttempted_ACarriedResumeKeepsItsIncarnation(t *testing.T) {
	c := newConcRunState(concPlan().Concurrency)
	c.seedAttempted(&orkstackv1.RunContinuation{Resume: &orkstackv1.RunResumeCarry{SeqBase: 42},
		Concurrency: &orkstackv1.RunConcurrencyCarry{Attempted: []string{"ork-conc-test"}}})
	if c.seqBase != 42 || len(c.attempted) != 1 {
		t.Fatalf("seq_base %d, attempted %v; want 42 and the gate", c.seqBase, c.attempted)
	}
	plain := newConcRunState(concPlan().Concurrency)
	plain.seedAttempted(&orkstackv1.RunContinuation{Concurrency: &orkstackv1.RunConcurrencyCarry{}})
	if plain.seqBase != 1 {
		t.Fatalf("seq_base %d of a run never resumed; want its plan's", plain.seqBase)
	}
}

// TestResumeSlotCost: a resume's history preflight counts the longest wait in
// line its limit allows, at the interval that grows to its maximum, and
// nothing for a run with no run-level limit.
func TestResumeSlotCost(t *testing.T) {
	if e, b := resumeSlotCost(nil); e != 0 || b != 0 {
		t.Fatalf("no limit costs %d events, %d bytes", e, b)
	}
	p := concPlan().Concurrency
	p.Slots[0].QueueTimeoutSeconds = int64((7 * 24 * time.Hour).Seconds())
	week, _ := resumeSlotCost(p)
	// 1, 3, 7 and 15 minutes in, then each 15: 675 ticks.
	if ticks := (week - 23) / reassertEvents; ticks < 670 || ticks > 680 {
		t.Fatalf("a week's wait costs %d events, %d ticks; want about 675", week, ticks)
	}
	p.Slots[0].Strategy = gateStrategyRefuse
	if refused, _ := resumeSlotCost(p); refused >= 30 {
		t.Fatalf("a limit that never queues costs %d events", refused)
	}
	p.Slots[0].Strategy, p.Slots[0].QueueTimeoutSeconds = gateStrategyQueue, 30
	if short, bytes := resumeSlotCost(p); short >= 30 || bytes < p.PerSlotBytes {
		t.Fatalf("a wait shorter than one tick costs %d events, %d bytes", short, bytes)
	}
}

// TestResumedRun_CancelWhileItReleases: an operator's cancel ends the wait for
// the earlier slots' releases; the run asks for nothing and releases at its
// end all the same.
func TestResumedRun_CancelWhileItReleases(t *testing.T) {
	h := newResumedConcHarness(t, granted(1))
	plan := concPlan()
	plan.Concurrency.Slots = append(plan.Concurrency.Slots, ConcurrencySlotPlan{GateID: "ork-conc-task", Key: "t", Label: "t", Limit: 1, Strategy: gateStrategyQueue})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-task", "", GateSignalReleaseAll, mock.Anything).After(time.Hour).Return(nil)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 5*time.Second)
	started := h.env.Now()
	h.resume(plan)
	if h.env.GetWorkflowError() == nil || h.last.GetError().GetCode() != "RUN_CANCELED" || h.ran != 0 || len(h.acquires) != 1 {
		t.Fatalf("ended %v after %d tasks and %d acquires; want canceled, none run and no acquire but the prefix's", h.last.GetError(), h.ran, len(h.acquires))
	}
	if past := h.env.Now().Sub(started); past > time.Minute {
		t.Fatalf("the cancel took %s", past)
	}
	if n := len(h.releases); n != 2 || h.releases[1].Below != 43 {
		t.Fatalf("releases %+v; want the resume's, then below 43", h.releases)
	}
}

// TestResumeConcurrency_ClearsWhatThePrefixHeld: nothing of an earlier
// incarnation is taken for this one's, in the run's state or its carry.
func TestResumeConcurrency_ClearsWhatThePrefixHeld(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (string, error) {
		s := &runState{runID: concRunID, plan: concPlan(), conc: newConcRunState(concPlan().Concurrency)}
		s.conc.runID = concRunID
		s.conc.held, s.conc.givenBack, s.conc.ordinal = &heldSlot{gate: "ork-conc-test", seq: 1}, true, 3
		s.conc.queued, s.conc.requeue = &orkstackv1.ConcurrencyQueued{}, &requeueWait{}
		err := s.resumeConcurrency(ctx, 42)
		c := s.conc
		return fmt.Sprint(c.held, c.givenBack, c.ordinal, c.queued, c.requeue, c.seqBase, err != nil), nil
	}, workflow.RegisterOptions{Name: "clears"})
	env.RegisterActivityWithOptions(func(_ context.Context, in AcquireConcurrencyInput) (AcquireConcurrencyResult, error) {
		return AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFull}}, nil
	}, activity.RegisterOptions{Name: ActivityAcquireConcurrency})
	env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-test", "", GateSignalReleaseAll, mock.Anything).Return(nil)
	env.ExecuteWorkflow("clears")
	var got string
	must.OK(t, env.GetWorkflowResult(&got))
	if want := fmt.Sprint((*heldSlot)(nil), false, int64(0), (*orkstackv1.ConcurrencyQueued)(nil), (*requeueWait)(nil), int64(42), true); got != want {
		t.Fatalf("after a resume whose key was full: %s; want %s", got, want)
	}
}
