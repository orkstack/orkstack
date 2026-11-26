package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// taskConcHarness is a concRunHarness whose tasks are played by body, and which
// records every single-slot release.
type taskConcHarness struct {
	*concRunHarness
	ranIDs []string
	// ranAt is when each task's activity ended, from the run's start.
	ranAt map[string]time.Duration
	// queued is every TaskRun.queued a snapshot carried, by task.
	queued map[string]*orkstackv1.ConcurrencyQueued
	// script is each task's errors, in order, before it succeeds.
	script map[string][]error
	// pausedQueued: a paused run's snapshot showed a task waiting.
	pausedQueued bool
}

func newTaskConcHarness(t *testing.T, answers ...AcquireConcurrencyResult) *taskConcHarness {
	t.Helper()
	h := &taskConcHarness{concRunHarness: newConcRunHarness(t, answers...), queued: map[string]*orkstackv1.ConcurrencyQueued{},
		ranAt: map[string]time.Duration{}}
	started := h.env.Now()
	h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.ran++
			h.ranIDs = append(h.ranIDs, in.GetTaskId())
			h.ranAt[in.GetTaskId()] = h.env.Now().Sub(started)
			if errs := h.script[in.GetTaskId()]; len(errs) > 0 {
				h.script[in.GetTaskId()] = errs[1:]
				return nil, errs[0]
			}
			if strings.HasPrefix(in.GetTaskId(), "bad") {
				return nil, temporal.NewNonRetryableApplicationError("exit 1", "ActivityFailed", nil)
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, ValuesJson: []byte(`{}`)}, nil
		}).After(10 * time.Second)
	h.env.OnActivity(ActivityWriteSnapshot, mock.Anything, mock.Anything, mock.Anything).Return(
		func(_ context.Context, run *orkstackv1.Run, _ int64) (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.last = run
			for _, r := range run.GetTaskRuns() {
				if r.GetQueued() != nil {
					h.queued[r.GetTaskId()] = r.GetQueued()
					h.pausedQueued = h.pausedQueued || run.GetStatus() == statusPaused
				}
			}
			return "memory://x", nil
		})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-test", "", GateSignalRelease, mock.Anything).Return(
		func(_, _, _, _ string, _ any) error { return nil }) // recorded in workflow code (concRunHarness.single)
	return h
}

// taskLimits is a plan whose named tasks share one slot of key "deploy", and
// the run has none.
func taskLimits(ids ...string) RunPlan {
	plan := concPlan()
	plan.Concurrency.Run = nil
	plan.Concurrency.Tasks = map[string]int{}
	for _, id := range ids {
		plan.Concurrency.Tasks[id] = 0
	}
	return plan
}

// tasksOf is an Orkflow of independent tasks; "x<y" is x needing y.
func tasksOf(ids ...string) *orkstackv1.Orkflow {
	wf := concOrkflow()
	wf.Tasks = nil
	for _, id := range ids {
		name, need, _ := strings.Cut(id, "<")
		task := &orkstackv1.Task{Id: name, Image: &orkstackv1.ImageRef{Reference: "alpine"}}
		if need != "" {
			task.Needs = []string{need}
		}
		wf.Tasks = append(wf.Tasks, task)
	}
	return wf
}

func (h *taskConcHarness) grantTask(d time.Duration, ix int, seq, n int64) {
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: ix, SeqBase: 1, Seq: seq, N: n})
	}, d)
}

func (h *taskConcHarness) count(typ orkstackv1.EventType) (n int) {
	for _, e := range h.events {
		if e.GetType() == typ {
			n++
		}
	}
	return n
}

func (h *taskConcHarness) row(id string) *orkstackv1.TaskRun {
	h.t.Helper()
	for _, r := range h.last.GetTaskRuns() {
		if r.GetTaskId() == id {
			return r
		}
	}
	h.t.Fatalf("no row %s", id)
	return nil
}

// TestTaskSlot_AskedBeforeDispatchAndReleasedAtItsEnd: a task with a limit asks
// for its slot once its needs are done, under its own index, runs once granted,
// and releases it at its end; the run asks for nothing of its own, a task with
// no limit asks nothing, and the end releases every gate asked.
func TestTaskSlot_AskedBeforeDispatchAndReleasedAtItsEnd(t *testing.T) {
	h := newTaskConcHarness(t, granted(4))
	h.runOrkflow(taskLimits("push"), tasksOf("build", "push<build", "after<push"))
	must.OK(t, h.env.GetWorkflowError())
	if !slices.Equal(h.ranIDs, []string{"build", "push", "after"}) || len(h.acquires) != 1 {
		t.Fatalf("ran %v after %d acquires", h.ranIDs, len(h.acquires))
	}
	a := h.acquires[0]
	if a.GateID != "ork-conc-test" || a.Request.TaskIx != 1 || a.Request.Strategy != gateStrategyQueue || a.Request.Limit != 1 ||
		a.Request.N != 0 || a.Request.Proto != gateProtoLegacy || a.Request.WaitUntil.Sub(a.Request.IssuedAt) != time.Hour {
		t.Fatalf("asked %+v", a.Request)
	}
	if !slices.Equal(h.single, []GateRelease{{Run: concRunID, SeqBase: 1, TaskIx: 1}}) ||
		!slices.Equal(h.releases, []GateReleaseAll{{Run: concRunID, Below: 2}}) {
		t.Fatalf("released %v then %v", h.single, h.releases)
	}
	if h.count(orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED) != 0 || h.row("push").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("events %v, push %v", h.eventTypes(), h.row("push"))
	}
}

// TestTaskSlot_WaitsInLine: a task whose key is full is said queued once, shows
// its place, runs nothing until its grant, and lets the run's other tasks run
// meanwhile; a grant that is not its entry's changes nothing.
func TestTaskSlot_WaitsInLine(t *testing.T) {
	h := newTaskConcHarness(t, queued(6, 2))
	h.grantTask(15*time.Second, 1, 6, 0) // another task's
	h.grantTask(16*time.Second, 0, 5, 0) // another entry's, answered below
	h.answers = append(h.answers, queued(6, 1))
	h.grantTask(30*time.Second, 0, 6, 0)
	h.runOrkflow(taskLimits("push"), tasksOf("push", "other"))
	must.OK(t, h.env.GetWorkflowError())
	if !slices.Equal(h.ranIDs, []string{"other", "push"}) {
		t.Fatalf("ran %v; want the waiting task last", h.ranIDs)
	}
	q := h.queued["push"]
	if h.count(orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED) != 1 || q.GetKey() != "deploy" || !q.GetPositionKnown() || q.GetPositionAtArrival() != 2 ||
		h.row("push").GetQueued() != nil || h.row("push").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("queued %v, row %v, events %v", q, h.row("push"), h.eventTypes())
	}
	// The grant under another seq asked the gate again; it granted nothing.
	if len(h.single) != 1 || len(h.acquires) != 2 || h.ranAt["push"] < 30*time.Second {
		t.Fatalf("released %v, %d acquires, push ran at %v", h.single, len(h.acquires), h.ranAt["push"])
	}
	// Its wait's end rides its TASK_SCHEDULED, task-level, and no other event.
	ends := h.taskWaitEnds()
	end := ends["push "+orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED.String()]
	if len(ends) != 1 || end.GetOutcome() != orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED || !end.GetTaskLevel() ||
		end.GetLimitLabel() != CensusLimitLabel("deploy") || end.GetStrategy() != orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE {
		t.Fatalf("wait ends %v; want push's start, task-level, on its TASK_SCHEDULED", ends)
	}
}

// taskWaitEnds is every event's wait end, by "task type".
func (h *concRunHarness) taskWaitEnds() map[string]*orkstackv1.ConcurrencyWaitEnd {
	out := map[string]*orkstackv1.ConcurrencyWaitEnd{}
	for _, e := range h.events {
		if e.GetWaitEnd() != nil {
			out[e.GetTaskId()+" "+e.GetType().String()] = e.GetWaitEnd()
		}
	}
	return out
}

// TestTaskSlot_AGrantBeforeItsAnswer: a grant the run reads while the acquire
// it answers is still on its way is kept for it.
func TestTaskSlot_AGrantBeforeItsAnswer(t *testing.T) {
	s, w := &runState{runID: concRunID, conc: &concRunState{plan: taskLimits("push").Concurrency}}, &taskWait{}
	s.conc.addTasks(tasksOf("push"))
	slot := s.conc.tasks[0]
	w.req = gateAcquire{SeqBase: 1}
	slot.wait = w
	if got := s.grantedTask(ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: 0, SeqBase: 1, Seq: 9}); got != slot {
		t.Fatalf("a grant for a waiting task = %v", got)
	}
	for _, g := range []ConcurrencyGrant{
		{Gate: "ork-conc-test", Run: concRunID, TaskIx: 0, SeqBase: 1, N: 1}, {Gate: "ork-conc-test", Run: concRunID, TaskIx: 0, SeqBase: 2},
		{Gate: "ork-conc-test", Run: concRunID, TaskIx: 1, SeqBase: 1}, {Gate: "other", Run: concRunID, TaskIx: 0, SeqBase: 1},
		{Gate: "ork-conc-test", Run: "ork-other", TaskIx: 0, SeqBase: 1}} {
		if got := s.grantedTask(g); got != nil {
			t.Errorf("grant %+v = %s's", g, got.id)
		}
	}
}

// TestTaskSlot_OneEntryAKey: of a run's ready tasks on one key one waits in its
// line; the next asks once that one has its slot, in definition order.
func TestTaskSlot_OneEntryAKey(t *testing.T) {
	h := newTaskConcHarness(t, queued(1, 1), granted(2), granted(3))
	h.grantTask(30*time.Second, 0, 1, 0)
	h.runOrkflow(taskLimits("a", "b", "c"), tasksOf("a", "b", "c"))
	must.OK(t, h.env.GetWorkflowError())
	var order []int
	for _, a := range h.acquires {
		order = append(order, a.Request.TaskIx)
	}
	if !slices.Equal(order, []int{0, 1, 2}) || h.count(orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED) != 1 || len(h.single) != 3 {
		t.Fatalf("asked for %v, released %v, events %v", order, h.single, h.eventTypes())
	}
}

// TestTaskSlot_AsksAgain: a task in line asks again at its place, and a limit
// that does not answer keeps it waiting, with its place unknown.
func TestTaskSlot_AsksAgain(t *testing.T) {
	h := newTaskConcHarness(t, queued(6, 2), AcquireConcurrencyResult{Unavailable: "down"}, granted(6))
	h.runOrkflow(taskLimits("push"), tasksOf("push"))
	must.OK(t, h.env.GetWorkflowError())
	if len(h.acquires) != 3 || h.acquires[1].Request.Seq != 6 || h.acquires[2].Request.Seq != 6 || h.acquires[2].Request.ArrivedAt.IsZero() ||
		h.acquires[0].UpdateID == h.acquires[1].UpdateID {
		t.Fatalf("asked %+v", h.acquires)
	}
	if h.ran != 1 || h.count(orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED) != 1 {
		t.Fatalf("ran %d, events %v", h.ran, h.eventTypes())
	}
}

// TestTaskSlot_Endings: a wait that ends without a slot fails its task, with
// its own reason, releases what it asked for and fails the run; nothing runs.
func TestTaskSlot_Endings(t *testing.T) {
	answer := func(a string) AcquireConcurrencyResult {
		return AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: a}}
	}
	infra, timedOut := orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	for name, c := range map[string]struct {
		answers  []AcquireConcurrencyResult
		code     string
		category orkstackv1.FailureCategory
		// waited: it was in line, and its TASK_FAILED says how that ended.
		waited orkstackv1.ConcurrencyWaitOutcome
	}{
		"timed out": {[]AcquireConcurrencyResult{queued(1, 1), queued(1, 1)}, TaskQueueTimeoutCode, timedOut,
			orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_TIMED_OUT},
		"unanswered":   {[]AcquireConcurrencyResult{{Unavailable: "down"}, {Unavailable: "down"}}, TaskConcurrencyUnavailableCode, infra, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNAVAILABLE},
		"a full line":  {[]AcquireConcurrencyResult{answer(gateAnswerFullBound)}, TaskConcurrencyFullCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED, 0},
		"fenced":       {[]AcquireConcurrencyResult{answer(gateAnswerFenced)}, TaskConcurrencyFencedCode, infra, 0},
		"unsupported":  {[]AcquireConcurrencyResult{{Unsupported: "protocol 9"}}, TaskConcurrencyUnsupportedCode, infra, 0},
		"unknown":      {[]AcquireConcurrencyResult{answer("maybe")}, TaskConcurrencyUnsupportedCode, infra, 0},
		"never a slot": {[]AcquireConcurrencyResult{answer(gateAnswerUnavailable), answer(gateAnswerUnavailable)}, TaskConcurrencyUnavailableCode, infra, orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNAVAILABLE},
	} {
		t.Run(name, func(t *testing.T) {
			h := newTaskConcHarness(t, c.answers...)
			plan := taskLimits("push")
			plan.Concurrency.Slots[0].QueueTimeoutSeconds = 90
			plan.Concurrency.RequeueInterval = time.Minute
			h.runOrkflow(plan, tasksOf("push", "after<push"))
			if h.env.GetWorkflowError() == nil || h.ran != 0 {
				t.Fatalf("ended %v after %d tasks", h.env.GetWorkflowError(), h.ran)
			}
			row := h.row("push")
			if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != c.code || row.GetError().GetCategory() != c.category ||
				!strings.Contains(row.GetError().GetMessage(), `task "push"`) || row.GetQueued() != nil {
				t.Fatalf("row %v; want %s", row, c.code)
			}
			if len(h.single) == 0 || len(h.releases) != 1 || h.last.GetStatus() != orkstackv1.Status_STATUS_FAILED {
				t.Fatalf("released %v then %v, run %v", h.single, h.releases, h.last.GetStatus())
			}
			ends := h.taskWaitEnds()
			if c.waited == 0 && len(ends) != 0 {
				t.Fatalf("wait ends %v of a task never in line", ends)
			}
			if end := ends["push "+orkstackv1.EventType_EVENT_TYPE_TASK_FAILED.String()]; c.waited != 0 &&
				(len(ends) != 1 || end.GetOutcome() != c.waited || !end.GetTaskLevel()) {
				t.Fatalf("wait ends %v; want %v on push's TASK_FAILED", ends, c.waited)
			}
		})
	}
}

// TestTaskSlot_AnEndingRunLeavesTheLine: a sibling's failure and an operator's
// cancel give the waiting task's place back before anything else of the run
// ends, and the task never runs.
func TestTaskSlot_AnEndingRunLeavesTheLine(t *testing.T) {
	for name, cancel := range map[string]bool{"a sibling fails": false, "a cancel": true} {
		t.Run(name, func(t *testing.T) {
			h := newTaskConcHarness(t, queued(1, 1))
			ids := []string{"push", "bad"}
			if cancel {
				ids = []string{"push", "other"}
				h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 3*time.Second)
			}
			h.runOrkflow(taskLimits("push"), tasksOf(ids...))
			err := h.env.GetWorkflowError()
			var canceled *temporal.CanceledError
			if err == nil || cancel != errors.As(err, &canceled) {
				t.Fatalf("ended %v", err)
			}
			if slices.Contains(h.ranIDs, "push") || h.row("push").GetStatus() != orkstackv1.Status_STATUS_CANCELED || h.row("push").GetQueued() != nil ||
				len(h.single) != 1 || h.single[0] != (GateRelease{Run: concRunID, SeqBase: 1, TaskIx: 0}) {
				t.Fatalf("ran %v, push %v, released %v", h.ranIDs, h.row("push"), h.single)
			}
		})
	}
}

// TestTaskSlot_APauseGivesThePlaceBack: a pause gives a waiting task's place
// back, its RUN_PAUSED snapshot shows none, and the unpause asks again with the
// next ordinal; a running task keeps its slot through a pause.
func TestTaskSlot_APauseGivesThePlaceBack(t *testing.T) {
	h := newTaskConcHarness(t, queued(1, 1), granted(2))
	plan := taskLimits("push")
	plan.Pause = pausePlan(t).Pause
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 2*time.Second)
	h.grantTask(5*time.Second, 0, 1, 0) // for the place given back
	h.env.RegisterDelayedCallback(func() {
		if h.row("push").GetQueued() != nil || h.ran != 0 {
			t.Errorf("paused: push %v, ran %d", h.row("push"), h.ran)
		}
		h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "1"))
	}, time.Minute)
	h.runOrkflow(plan, tasksOf("push"))
	must.OK(t, h.env.GetWorkflowError())
	if h.pausedQueued {
		t.Error("the paused run's snapshot shows its task waiting")
	}
	if len(h.acquires) != 2 || h.acquires[1].Request.N != 1 || h.acquires[1].Request.Proto != gateProtoOrdinal || !h.acquires[1].Request.Requeue {
		t.Fatalf("asked %+v", h.acquires)
	}
	if !slices.Equal(h.single, []GateRelease{{Run: concRunID, SeqBase: 1, TaskIx: 0}, {Run: concRunID, SeqBase: 1, TaskIx: 0, N: 1}}) || h.ran != 1 {
		t.Fatalf("released %v, ran %d", h.single, h.ran)
	}

	h = newTaskConcHarness(t, granted(1))
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 2*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "1")) }, time.Minute)
	h.runOrkflow(plan, tasksOf("push", "after<push"))
	must.OK(t, h.env.GetWorkflowError())
	if len(h.acquires) != 1 || len(h.single) != 1 || h.single[0].N != 0 {
		t.Fatalf("a running task's slot: %d acquires, released %v", len(h.acquires), h.single)
	}
}

// TestTaskSlot_BesideTheRunsOwn: a run with a limit of its own holds that slot
// from its start, and its task asks for its key's beside it.
func TestTaskSlot_BesideTheRunsOwn(t *testing.T) {
	h := newTaskConcHarness(t, granted(1), granted(2))
	plan := taskLimits()
	run := 0
	plan.Concurrency.Run = &run
	own := plan.Concurrency.Slots[0]
	own.GateID, own.Key = "ork-conc-push", "push"
	plan.Concurrency.Slots = append(plan.Concurrency.Slots, own)
	plan.Concurrency.Tasks = map[string]int{"push": 1}
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-push", "", GateSignalRelease, mock.Anything).Return(nil)
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-push", "", GateSignalReleaseAll, mock.Anything).Return(nil)
	h.runOrkflow(plan, tasksOf("push"))
	must.OK(t, h.env.GetWorkflowError())
	pushed := h.singleAt["ork-conc-push"]
	if len(h.acquires) != 2 || h.acquires[0].Request.TaskIx != gateRunLevel || h.acquires[1].GateID != "ork-conc-push" || len(pushed) != 1 ||
		len(h.single) != 0 || len(h.releases) != 1 {
		t.Fatalf("acquires %+v, released %v, %v, %v", h.acquires, pushed, h.single, h.releases)
	}
}

// stopped is a task its stopping Orker handed back, before it started or not.
func stopped(started bool) error {
	return temporal.NewApplicationErrorWithOptions("ORKER_STOPPED", interruptionTypePrefix+"ORKER_STOPPED",
		temporal.ApplicationErrorOptions{Details: []any{&orkstackv1.ActivityFailureDetail{
			ReasonCode: "ORKER_STOPPED", Started: started, NeverStarted: !started, OrkerId: "orker-a"}}})
}

// TestTaskSlot_KeptAcrossAnInterruption: a task run again keeps its slot, asks
// for nothing, and gives it back once, at its end; and so does one whose run
// fails while it waits to run again.
func TestTaskSlot_KeptAcrossAnInterruption(t *testing.T) {
	plan := taskLimits("push")
	budgets := DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	h := newTaskConcHarness(t, granted(1))
	h.script = map[string][]error{"push": {stopped(true)}}
	h.runOrkflow(plan, tasksOf("push"))
	must.OK(t, h.env.GetWorkflowError())
	if len(h.acquires) != 1 || len(h.single) != 1 || !slices.Equal(h.ranIDs, []string{"push", "push"}) {
		t.Fatalf("%d acquires, released %v, ran %v", len(h.acquires), h.single, h.ranIDs)
	}

	budgets.NeverStarted, budgets.NeverStartedBackoffMillis = 0, 60000
	h = newTaskConcHarness(t, granted(1))
	h.script = map[string][]error{"push": {stopped(false)}}
	h.runOrkflow(plan, tasksOf("push", "slow", "bad<slow"))
	if h.env.GetWorkflowError() == nil || len(h.single) != 1 || h.single[0].TaskIx != 0 {
		t.Fatalf("ended %v, released %v; want the waiting task's slot once", h.env.GetWorkflowError(), h.single)
	}
}

// TestTaskSlot_WaitsForTheRunsOwnSlot: a task that keeps its slot through an
// interruption does not run before its unpaused run has its own slot again.
func TestTaskSlot_WaitsForTheRunsOwnSlot(t *testing.T) {
	h := newTaskConcHarness(t, granted(1), granted(2), queued(7, 1), queued(7, 1)) // the last a re-assert
	plan := taskLimits("push")
	run := 0
	plan.Concurrency.Run, plan.Pause = &run, pausePlan(t).Pause
	own := plan.Concurrency.Slots[0]
	own.GateID, own.Key = "ork-conc-push", "push"
	plan.Concurrency.Slots = append(plan.Concurrency.Slots, own)
	plan.Concurrency.Tasks["push"] = 1
	budgets := DefaultInterruptionBudgets
	budgets.NeverStarted, budgets.NeverStartedBackoffMillis = 0, 60000
	plan.Interruptions = &budgets
	h.script = map[string][]error{"push": {stopped(false)}}
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-push", "", mock.Anything, mock.Anything).Return(nil)
	at := func(d time.Duration, f func()) { h.env.RegisterDelayedCallback(f, d) }
	at(15*time.Second, func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) })
	at(30*time.Second, func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "1")) })
	at(150*time.Second, func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 7, N: 1})
	})
	h.runOrkflow(plan, tasksOf("push"))
	must.OK(t, h.env.GetWorkflowError())
	if len(h.acquires) != 4 || h.acquires[2].Request.TaskIx != gateRunLevel || h.acquires[3].Request.TaskIx != gateRunLevel || h.ranAt["push"] < 150*time.Second {
		t.Fatalf("%d acquires, push ran at %v; want after the run had its slot again", len(h.acquires), h.ranAt["push"])
	}
}

// TestTaskSlot_IsReserved: what giving a slot back appends is part of what a
// run keeps its history for, from the ask to the release.
func TestTaskSlot_IsReserved(t *testing.T) {
	s := &runState{conc: &concRunState{plan: taskLimits("a", "b").Concurrency}, plan: taskLimits("a", "b")}
	s.conc.plan.PerSlotEvents = TaskSlotReserveEvents
	s.conc.addTasks(tasksOf("a", "b"))
	events, bytes := s.failReserve(nil, DefaultHistoryReserve)
	s.conc.tasks[0].held, s.conc.tasks[1].wait = true, &taskWait{}
	withEvents, withBytes := s.failReserve(nil, DefaultHistoryReserve)
	if withEvents-events != 2*TaskSlotReserveEvents || withBytes-bytes != 2*s.conc.plan.PerSlotBytes {
		t.Fatalf("two slots reserve %d events and %d bytes", withEvents-events, withBytes-bytes)
	}
}

// TestTaskSlot_ADispatchThatFailsGivesItsSlotBack: a task granted its slot
// whose dispatch then fails (here its pool has no route) never reaches the
// in-flight set, and its slot is released at once, not left to the gate's
// check pass.
func TestTaskSlot_ADispatchThatFailsGivesItsSlotBack(t *testing.T) {
	h := newTaskConcHarness(t, granted(4))
	wf := tasksOf("push")
	wf.Tasks[0].Pool = "nowhere"
	h.runOrkflow(taskLimits("push"), wf)
	if h.env.GetWorkflowError() == nil || h.ran != 0 {
		t.Fatalf("ended %v after %d tasks; want the dispatch's failure and none run", h.env.GetWorkflowError(), h.ran)
	}
	if !slices.Equal(h.single, []GateRelease{{Run: concRunID, SeqBase: 1, TaskIx: 0}}) {
		t.Fatalf("task releases %v; want push's slot given back", h.single)
	}
}

// TestTaskSlot_AnUnansweredWaitSaysWhy: a wait that ends while its limit does
// not answer names the last acquire's cause.
func TestTaskSlot_AnUnansweredWaitSaysWhy(t *testing.T) {
	h := newTaskConcHarness(t, AcquireConcurrencyResult{Unavailable: "frontend refused"}, AcquireConcurrencyResult{Unavailable: "frontend refused"})
	plan := taskLimits("push")
	plan.Concurrency.Slots[0].QueueTimeoutSeconds = 90
	h.runOrkflow(plan, tasksOf("push"))
	if msg := h.row("push").GetError().GetMessage(); h.row("push").GetError().GetCode() != TaskConcurrencyUnavailableCode ||
		!strings.HasSuffix(msg, "; its last acquire: frontend refused") {
		t.Fatalf("push ended %v; want %s naming the last cause", h.row("push").GetError(), TaskConcurrencyUnavailableCode)
	}
}

// TestTaskSlot_AReleaseIsRecordedBeforeItsSignalIsDelivered: a run that ends
// as it gives its task's slot back (a full line) has the release recorded
// when the run ends, however late the test environment delivers the signal
// (its mock runs on a goroutine of its own; here it is held until the end).
func TestTaskSlot_AReleaseIsRecordedBeforeItsSignalIsDelivered(t *testing.T) {
	h := newTaskConcHarness(t, AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFullBound}})
	plan := taskLimits("push")
	plan.Concurrency.Slots[0].GateID = "ork-conc-slow"
	hold := make(chan struct{})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-slow", "", GateSignalRelease, mock.Anything).Return(
		func(_, _, _, _ string, _ any) error { <-hold; return nil })
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-slow", "", GateSignalReleaseAll, mock.Anything).Return(nil)
	h.runOrkflow(plan, tasksOf("push"))
	h.mu.Lock()
	single := slices.Clone(h.singleAt["ork-conc-slow"])
	h.mu.Unlock()
	close(hold)
	if h.row("push").GetError().GetCode() != TaskConcurrencyFullCode {
		t.Fatalf("push ended %v; want %s", h.row("push").GetError(), TaskConcurrencyFullCode)
	}
	if !slices.Equal(single, []GateRelease{{Run: concRunID, SeqBase: 1, TaskIx: 0}}) {
		t.Fatalf("released %v when the run ended; want push's slot, recorded as the run sent it", single)
	}
}
