package engine

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// reservePausePlan is pausePlan with an apply reserve no request fits under:
// applying any pause request would take the run's history past its limit.
func reservePausePlan(t *testing.T) RunPlan {
	plan := pausePlan(t)
	plan.Pause.ApplyReserveEvents = plan.History.MaxEvents
	return plan
}

// In the main loop a pause request that would take the run's history past
// its limit is not applied: the run stops RUN_HISTORY_LIMIT, explicitly,
// naming the request, and is never shown paused.
func TestPause_MainLoopReserveStopsTheRun(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	err := h.run(t, twoStepDocument, reservePausePlan(t))
	requireCode(t, err, RunHistoryLimitCode)
	if !strings.Contains(err.Error(), "pause request req-1") {
		t.Fatalf("refusal %q does not name the pause request", err)
	}
	if paused := h.sawEvent(eventRunPaused); len(paused) != 0 {
		t.Fatalf("RUN_PAUSED emitted %d times; the request was never applied", len(paused))
	}
}

// In the handler loop, which never carries on into a new history, a pause
// request that would take the history past its limit is dropped: the run
// keeps failing with its own cause, never RUN_HISTORY_LIMIT over a pause
// (M7), and its handlers run to their end.
func TestPause_HandlerLoopReserveDropsTheRequest(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}, "rollback": {takes: 10 * time.Minute}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 2*time.Minute)
	err := h.run(t, handlerDocument, reservePausePlan(t))
	requireCode(t, err, "TASK_FAILED")
	_, run := h.last(t)
	if run.GetError().GetCode() == RunHistoryLimitCode || run.GetPause() != nil || run.GetLastPause() != nil {
		t.Fatalf("run %v, pause %v, last %v; want build's cause and no pause", run.GetError(), run.GetPause(), run.GetLastPause())
	}
	if rollback := rowOf(run, "rollback"); rollback.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback %s, want SUCCEEDED: the dropped request held nothing", rollback.GetStatus())
	}
	if paused := h.sawEvent(eventRunPaused); len(paused) != 0 {
		t.Fatalf("RUN_PAUSED emitted %d times; the request was dropped", len(paused))
	}
}
