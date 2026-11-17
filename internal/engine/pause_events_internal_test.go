package engine

import (
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// requestSignal is pauseSignal with its own request id.
func requestSignal(pause bool, requestID string) map[string]any {
	sig := pauseSignal(pause, "0")
	sig["requestId"] = requestID
	return sig
}

// pauseFields is what one event says of a pause request.
type pauseFields struct {
	eventType  orkstackv1.EventType
	requestID  string
	transition orkstackv1.PauseTransition
}

// pauseEvents is every RUN_PAUSED and RUN_UNPAUSED the run emitted, in
// order, then its terminal event.
func (h *timeoutHarness) pauseEvents(t *testing.T) []pauseFields {
	t.Helper()
	var out []pauseFields
	h.mu.Lock()
	for _, e := range h.events {
		if t := e.GetType(); t == eventRunPaused || t == eventRunUnpaused || isTerminalRunEventType(t) {
			out = append(out, pauseFields{e.GetType(), e.GetRequestId(), e.GetPauseTransition()})
		}
	}
	h.mu.Unlock()
	return out
}

func requirePauseEvents(t *testing.T, got []pauseFields, want ...pauseFields) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("pause events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pause event %d is %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}

var (
	terminalSucceeded = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	terminalFailed    = orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	terminalCanceled  = orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
)

// Each pause event says what it did to the run's pause and, for an API
// request, which request it answers; the terminal event says how it ended
// a pause the run still held, and nothing when it held none.
func TestPause_EventsCarryTheTransition(t *testing.T) {
	t.Run("pause, unpause, unpause again", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, requestSignal(true, "req-1")) }, 5*time.Second)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, requestSignal(false, "req-2")) }, time.Hour)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, requestSignal(false, "req-3")) }, time.Hour+time.Second)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		requirePauseEvents(t, h.pauseEvents(t),
			pauseFields{eventRunPaused, "req-1", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED},
			pauseFields{eventRunUnpaused, "req-2", orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED},
			pauseFields{eventRunUnpaused, "req-3", orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED},
			pauseFields{terminalSucceeded, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_UNSPECIFIED})
	})
	t.Run("canceled while paused", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, requestSignal(true, "req-1")) }, 5*time.Second)
		h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Hour)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		requirePauseEvents(t, h.pauseEvents(t),
			pauseFields{eventRunPaused, "req-1", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED},
			pauseFields{terminalCanceled, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED})
	})
	t.Run("expired", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		requirePauseEvents(t, h.pauseEvents(t),
			pauseFields{eventRunPaused, "req-1", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED},
			pauseFields{terminalFailed, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT})
	})
	t.Run("a failing run's pause expired", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "600")) }, 5*time.Second)
		_ = h.run(t, handlerDocument, pausePlan(t))
		requirePauseEvents(t, h.pauseEvents(t),
			pauseFields{eventRunPaused, "req-1", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED},
			pauseFields{terminalFailed, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT})
	})
	t.Run("inherited, then the run's own", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() {
			h.env.SignalWorkflow(PauseSignal, h.inheritedSignal(true, 1, time.Hour))
		}, 5*time.Second)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, requestSignal(true, "req-1")) }, time.Minute)
		h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 10*time.Minute)
		_ = h.run(t, twoStepDocument, childPausePlan(t))
		requirePauseEvents(t, h.pauseEvents(t),
			pauseFields{eventRunPaused, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED},
			pauseFields{eventRunPaused, "req-1", orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED},
			pauseFields{terminalCanceled, "", orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED})
	})
}
