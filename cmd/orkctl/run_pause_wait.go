package main

import (
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/runstatus"
)

// pauseOutcome is what a waited pause or unpause request came to, decided
// by the request's own id (design §5.6). Its JSON spelling is the --json
// "outcome" field.
type pauseOutcome string

const (
	pauseApplied      pauseOutcome = "applied"
	pauseOtherRequest pauseOutcome = "other_request"
	// pauseStillPaused: an unpause the run applied by lifting its own pause
	// while its parent run's pause still holds it (CAUSES_CHANGED).
	pauseStillPaused pauseOutcome = "still_paused"
	pauseRunEnded    pauseOutcome = "run_ended"
	pauseTimeout     pauseOutcome = "timeout"
)

// Exit codes of run pause and run unpause --wait. 2 and 3 are not used: they
// mean failed and canceled to run get --watch.
const (
	exitPauseRunEnded     = 4
	exitPauseTimeout      = 5
	exitPauseOtherRequest = 6
)

// pauseWaiter decides a waited request from the run's events after the
// request's after_sequence.
type pauseWaiter struct {
	runID     string
	requestID string
	pause     bool
	// event is the event that decided the outcome; ended is a terminal
	// event read after it.
	event *orkstackv1.Event
	ended *orkstackv1.Event
	// endedCode is the ended run's error code, read from the run once it
	// ended; empty when it did not fail.
	endedCode string
}

// observe takes one event and reports the outcome once one is decided.
func (w *pauseWaiter) observe(ev *orkstackv1.Event) (pauseOutcome, bool) {
	switch {
	case ev.GetRequestId() == w.requestID && w.requestID != "" &&
		(ev.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED || ev.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED):
		w.event = ev
		if !w.pause && ev.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED {
			// The own pause was lifted, but the run stays paused by its
			// parent run's pause.
			return pauseStillPaused, true
		}
		return pauseApplied, true
	case isTerminalRunEvent(ev):
		w.event = ev
		return pauseRunEnded, true
	case w.pause && ev.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED &&
		ev.GetPauseTransition() == orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED:
		w.event = ev
		return pauseOtherRequest, true
	}
	return "", false
}

// fromSnapshot decides a request after a STREAM_GAP from the run as GetRun
// reads it: the requested state, or the run's end.
func (w *pauseWaiter) fromSnapshot(run *orkstackv1.Run) (pauseOutcome, bool) {
	switch {
	case runstatus.IsTerminal(run.GetStatus()):
		return pauseRunEnded, true
	case w.pause && run.GetPause().GetRequestId() == w.requestID:
		return pauseApplied, true
	case w.pause && run.GetStatus() == orkstackv1.Status_STATUS_PAUSED:
		return pauseOtherRequest, true
	}
	// An unpause is decided only by its event: a run that reads unpaused may
	// be one whose read model lags, or whose pause expired and which is
	// ending, neither of which is this request applied.
	return "", false
}

// isTerminalRunEvent reports a run's terminal event, as the stream ends on
// it (api.IsTerminalEvent).
func isTerminalRunEvent(ev *orkstackv1.Event) bool {
	switch ev.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	}
	return false
}

// summary is the human line for outcome; expires is the pause's end when
// known, children whether its running child runs are paused with it.
func (w *pauseWaiter) summary(outcome pauseOutcome, expires string, children bool) string {
	switch outcome {
	case pauseApplied:
		var line string
		switch {
		case w.event != nil && w.event.GetPauseTransition() == orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED:
			line = fmt.Sprintf("run %s was not paused", w.runID)
		case w.pause:
			line = fmt.Sprintf("run %s paused until %s", w.runID, expires)
			if children {
				line += "; its running child runs are paused too"
			}
		default:
			line = fmt.Sprintf("run %s unpaused", w.runID)
		}
		if w.ended != nil {
			line += fmt.Sprintf("; the run then finished (%s)", statusLabel(w.ended.GetStatus()))
		}
		return line
	case pauseRunEnded:
		if w.event != nil {
			return fmt.Sprintf("run %s ended (%s); the request had no effect", w.runID, w.endedLabel())
		}
		return fmt.Sprintf("run %s ended; the request had no effect", w.runID)
	case pauseOtherRequest:
		return fmt.Sprintf("run %s is paused, by another request", w.runID)
	case pauseStillPaused:
		return fmt.Sprintf("run %s's own pause is lifted, but it is still paused because its parent run is paused", w.runID)
	case pauseTimeout:
		return fmt.Sprintf("the run has not applied request %s yet", w.requestID)
	}
	return ""
}

// endedLabel is the ended run's status and, when it failed, its reason.
func (w *pauseWaiter) endedLabel() string {
	label := statusLabel(w.event.GetStatus())
	if w.endedCode != "" {
		label += " " + w.endedCode
	}
	return label
}

// exitCode is the command's exit status for outcome.
func (outcome pauseOutcome) exitCode() int {
	switch outcome {
	case pauseRunEnded:
		return exitPauseRunEnded
	case pauseTimeout:
		return exitPauseTimeout
	case pauseOtherRequest, pauseStillPaused:
		return exitPauseOtherRequest
	}
	return 0
}
