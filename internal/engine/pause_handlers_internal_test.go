package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// step is one task's behaviour under behave: how long it takes, and whether
// it then fails.
type step struct {
	takes time.Duration
	fails bool
}

// behave answers each named task's container calls after its time; every
// other task succeeds at once.
func (h *timeoutHarness) behave(steps map[string]step) {
	answer := func(fails bool) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			h.mu.Lock()
			h.tasks = append(h.tasks, in.GetTaskId())
			h.mu.Unlock()
			if fails {
				return nil, temporal.NewNonRetryableApplicationError(in.GetTaskId()+" failed", "TASK_FAILED", nil)
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, ValuesJson: []byte(`{}`)}, nil
		}
	}
	for id, s := range steps {
		h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.MatchedBy(func(in *orkstackv1.ExecuteTaskInput) bool {
			return in.GetTaskId() == id
		})).Return(answer(s.fails)).After(s.takes)
	}
	h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.MatchedBy(func(in *orkstackv1.ExecuteTaskInput) bool {
		_, named := steps[in.GetTaskId()]
		return !named
	})).Return(answer(false))
}

// failingNote is the failing phase's one TASK_LOG with no task id.
func (h *timeoutHarness) failingNote(t *testing.T) *orkstackv1.Event {
	t.Helper()
	for _, e := range h.sawEvent(orkstackv1.EventType_EVENT_TYPE_TASK_LOG) {
		if e.GetTaskId() == "" && strings.Contains(e.GetMessage(), "failure handler") {
			return e
		}
	}
	t.Fatal("no failing note was emitted")
	return nil
}

const handlerChainDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: handler-chain
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [build]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [rollback]
    join: all_done
    with: {op: exec}
`

// A paused run that starts failing stays PAUSED and holds its failure
// handlers; after the unpause they run and the run ends with its own cause.
func TestPause_FailingWhilePausedHoldsHandlers(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	err := h.run(t, handlerDocument, pausePlan(t))
	if err == nil {
		t.Fatal("the run succeeded; build failed")
	}
	if note := h.failingNote(t); note.GetStatus() != statusPaused {
		t.Fatalf("failing note status %s, want PAUSED", note.GetStatus())
	}
	_, run := h.last(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() == RunPauseTimeoutCode {
		t.Fatalf("run %s %v; want FAILED with build's cause", run.GetStatus(), run.GetError())
	}
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback %s, want SUCCEEDED after the unpause", rollback.GetStatus())
	}
	unpaused := h.sawEvent(eventRunUnpaused)
	if len(unpaused) != 1 || rollback.GetScheduledAt().AsTime().Before(unpaused[0].GetTimestamp().AsTime()) {
		t.Fatalf("rollback was dispatched at %s, before the unpause %v", rollback.GetScheduledAt().AsTime(), unpaused)
	}
}

// A failing paused run whose pause expires keeps its own cause, with the
// pause's end as detail; a held handler never starts and ends CANCELED.
func TestPause_FailingPausedExpiryKeepsCause(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "600")) }, 5*time.Second)
	err := h.run(t, handlerDocument, pausePlan(t))
	requireCode(t, err, "TASK_FAILED")
	_, run := h.last(t)
	if run.GetError().GetCode() != "RUN_FAILED" ||
		!strings.Contains(run.GetError().GetMessage(), "(RUN_PAUSE_TIMEOUT)") ||
		!strings.Contains(run.GetError().GetMessage(), "rollback did not start") {
		t.Fatalf("run error %v; want build's cause with the pause's end as detail", run.GetError())
	}
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil ||
		!strings.Contains(eventsFor(h, "rollback"), "did not start: the run's pause ended at") {
		t.Fatalf("rollback %s scheduled=%v %q; want CANCELED, never dispatched, did not start",
			rollback.GetStatus(), rollback.GetScheduledAt(), eventsFor(h, "rollback"))
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took < 605*time.Second || took > 606*time.Second {
		t.Fatalf("the run ended %s after it started, want at the pause's expiry, 10m5s", took)
	}
}

// A pause of a failing run that expires stops the handler still running and
// starts none of those it held.
func TestPause_FailingPausedExpiryStopsARunningHandler(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: 10 * time.Second, fails: true}, "rollback": {takes: 2 * time.Hour}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "600")) }, 20*time.Second)
	err := h.run(t, handlerChainDocument, pausePlan(t))
	requireCode(t, err, "TASK_FAILED")
	_, run := h.last(t)
	rollback, notify := rowOf(run, "rollback"), rowOf(run, "notify")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		!strings.Contains(rollback.GetError().GetMessage(), "pause ended") {
		t.Fatalf("rollback %s %v; want CANCELED, stopped when the pause ended", rollback.GetStatus(), rollback.GetError())
	}
	if notify.GetStatus() != orkstackv1.Status_STATUS_CANCELED || notify.GetScheduledAt() != nil {
		t.Fatalf("notify %s scheduled=%v; want CANCELED, never dispatched", notify.GetStatus(), notify.GetScheduledAt())
	}
	message := run.GetError().GetMessage()
	if !strings.Contains(message, "rollback was stopped") || !strings.Contains(message, "notify did not start") {
		t.Fatalf("run message %q; want each handler's state", message)
	}
}

// An operator's cancel of a failing paused run ends it at once, as today's
// cancel of a failing run does: FAILED with its own cause, no held handler
// started.
func TestPause_CancelWhileFailingPaused(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 30*time.Minute)
	requireCode(t, h.run(t, handlerDocument, pausePlan(t)), "TASK_FAILED")
	_, run := h.last(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" {
		t.Fatalf("run %s %v, want FAILED with build's cause", run.GetStatus(), run.GetError())
	}
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil ||
		!strings.Contains(eventsFor(h, "rollback"), cancelReasonHandlerStopped) {
		t.Fatalf("rollback %s scheduled=%v %q; want CANCELED, never dispatched, as today's cancel leaves it",
			rollback.GetStatus(), rollback.GetScheduledAt(), eventsFor(h, "rollback"))
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took > 31*time.Minute {
		t.Fatalf("the run ended %s after it started, want at the cancel", took)
	}
}

// A paused run that reaches its run timeout fails RUN_TIMED_OUT, stays
// PAUSED and holds its handlers; its hard end then ends it, the held handler
// CANCELED and never dispatched.
func TestPause_RunTimeoutOnAPausedRunHoldsHandlers(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: 2 * time.Hour}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	plan := pausePlan(t)
	plan.RunTimeout = &RunTimeoutPlan{HandlerGraceSeconds: 600, ResolutionsPerPass: DefaultResolutionsPerPass}
	err := h.run(t, timed(handlerDocument, "60s"), plan)
	requireCode(t, err, RunTimedOutCode)
	if note := h.failingNote(t); note.GetStatus() != statusPaused {
		t.Fatalf("failing note status %s, want PAUSED", note.GetStatus())
	}
	_, run := h.last(t)
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil ||
		!strings.Contains(rollback.GetError().GetMessage()+" "+eventsFor(h, "rollback"), "passed before this handler started") {
		t.Fatalf("rollback %s scheduled=%v %v; want CANCELED at the hard end, never dispatched",
			rollback.GetStatus(), rollback.GetScheduledAt(), rollback.GetError())
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took < 660*time.Second || took > 661*time.Second {
		t.Fatalf("the run ended %s after it started, want at its hard end, 11m", took)
	}
}

// An operator's cancel of a failing paused run with a handler running stops
// that handler and ends the run, never spinning on the closed cancel.
func TestPause_CancelWhileFailingPausedWithAHandlerRunning(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: 10 * time.Second, fails: true}, "rollback": {takes: 2 * time.Hour}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 20*time.Second)
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 30*time.Minute)
	requireCode(t, h.run(t, handlerChainDocument, pausePlan(t)), "TASK_FAILED")
	_, run := h.last(t)
	if rollback := rowOf(run, "rollback"); rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("rollback %s, want CANCELED by the cancel", rollback.GetStatus())
	}
	if notify := rowOf(run, "notify"); notify.GetStatus() != orkstackv1.Status_STATUS_CANCELED || notify.GetScheduledAt() != nil {
		t.Fatalf("notify %s scheduled=%v; want CANCELED, never dispatched", notify.GetStatus(), notify.GetScheduledAt())
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took > 31*time.Minute {
		t.Fatalf("the run ended %s after it started, want at the cancel", took)
	}
}

// The first of a pause expiry and the run timeout's hard end to stop the
// handlers names them.
func TestHandlersStoppedReason_FirstStopNames(t *testing.T) {
	hardEnded := &runTimeout{hardEnded: true, hardEnd: time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)}
	stop := &pauseStop{detail: "pause ended at 2026-11-15T09:00:00Z (RUN_PAUSE_TIMEOUT)"}
	tests := []struct {
		name string
		s    runState
		want string
	}{
		{"neither", runState{}, ""},
		{"hard end", runState{runTimeout: hardEnded}, "hard end"},
		{"pause expiry", runState{pauseStop: stop}, "pause ended"},
		{"pause expiry, then the hard end", runState{runTimeout: hardEnded, pauseStop: stop}, "pause ended"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, stopped := tt.s.handlersStoppedReason()
			if stopped != (tt.want != "") || !strings.Contains(got, tt.want) {
				t.Fatalf("handlersStoppedReason() = %q, %v; want %q", got, stopped, tt.want)
			}
		})
	}
}

// The handler loop registers a pause case only while its pass top would take
// it: never after the pause or the hard end has stopped the handlers, so a
// ready timer it no longer takes cannot spin the loop.
func TestPauseActsOnHandlers(t *testing.T) {
	var channel workflow.ReceiveChannel = &fakeReceiveChannel{}
	tests := []struct {
		name string
		s    runState
		want bool
	}{
		{"no pause plan", runState{}, false},
		{"pausable", runState{pauses: channel}, true},
		{"pause stopped the handlers", runState{pauses: channel, pauseStop: &pauseStop{}}, false},
		{"hard end stopped them", runState{pauses: channel, runTimeout: &runTimeout{hardEnded: true}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.pauseActsOnHandlers(); got != tt.want {
				t.Fatalf("pauseActsOnHandlers() = %v, want %v", got, tt.want)
			}
		})
	}
}

// fakeReceiveChannel stands in for the request channel; it is never read.
type fakeReceiveChannel struct{ workflow.ReceiveChannel }
