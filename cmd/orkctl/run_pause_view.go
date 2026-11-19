package main

import (
	"fmt"
	"io"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// pauseLines are run get's header lines for a run's pause: the current one
// (Paused) and, once a pause ended, how (Last pause). A run never paused has
// none. Each line is "Label:\tvalue", as the header table takes it.
func pauseLines(run *orkstackv1.Run) []string {
	var lines []string
	if own := run.GetPause(); own != nil {
		line := fmt.Sprintf("Paused:\tsince %s by %s", formatTime(own.GetPausedAt()), pausePrincipal(own.GetPausedBy(), own.GetPausedByVerified()))
		if reason := own.GetReason(); reason != "" {
			line += ": " + assertedName(reason)
		}
		if run.GetError() != nil {
			line += fmt.Sprintf("; ends at %s unless unpaused; failure handlers wait until it is unpaused",
				formatTime(own.GetExpiresAt()))
		} else {
			line += fmt.Sprintf("; ends as failed at %s unless unpaused", formatTime(own.GetExpiresAt()))
		}
		if n := runningTasks(run); n > 0 {
			line += fmt.Sprintf("; %s still running", plural(int64(n), "task"))
		}
		lines = append(lines, line)
	} else if parent := run.GetPausedByParent(); parent != nil {
		lines = append(lines, fmt.Sprintf("Paused:\tbecause parent run %s is paused (since %s)",
			parent.GetParentRunId(), formatTime(parent.GetSince())))
	}
	for _, w := range run.GetPauseWarnings() {
		lines = append(lines, "Not paused:\t"+pauseWarningText(w))
	}
	if last := run.GetLastPause(); last != nil && run.GetPause() == nil {
		line := fmt.Sprintf("Last pause:\t%s by %s", formatTime(last.GetPausedAt()), pausePrincipal(last.GetPausedBy(), last.GetPausedByVerified()))
		if reason := last.GetReason(); reason != "" {
			line += ": " + assertedName(reason)
		}
		switch last.GetEnd() {
		case orkstackv1.PauseEnd_PAUSE_END_UNPAUSED:
			line += "; unpaused at " + formatTime(last.GetEndedAt())
			if by := last.GetEndedBy(); by != "" {
				line += " by " + pausePrincipal(by, last.GetEndedByVerified())
			}
		case orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT:
			line += "; timed out at " + formatTime(last.GetEndedAt())
		case orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED:
			line += "; ended with the run at " + formatTime(last.GetEndedAt())
		}
		lines = append(lines, line)
	}
	return lines
}

// pausePrincipal is who asked for a pause or an unpause, marked verified
// when OrkStack authenticated them and asserted otherwise.
func pausePrincipal(principal string, verified bool) string {
	if principal == "" {
		principal = "an unnamed caller"
	}
	if verified {
		return assertedName(principal) + " (verified)"
	}
	return assertedName(principal) + " (asserted)"
}

// runningTasks counts the run's tasks still running: the work a pause lets
// finish.
func runningTasks(run *orkstackv1.Run) int {
	n := 0
	for _, task := range run.GetTaskRuns() {
		if task.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			n++
		}
	}
	return n
}

// printPauseWarnings prints the ways the run's pause did not reach a child
// run, once a waited pause applied (Run.pause_warnings).
func printPauseWarnings(out io.Writer, run *orkstackv1.Run) {
	for _, w := range run.GetPauseWarnings() {
		fmt.Fprintln(out, "warning: "+pauseWarningText(w))
	}
}

// pauseWarningText is one warning in words: the composed task, then the
// engine's message, which names the child run. Both are engine-written text,
// so a control character is shown as a space, never sent to the terminal.
func pauseWarningText(w *orkstackv1.PauseWarning) string {
	return fmt.Sprintf("task %s: %s", assertedName(w.GetTaskId()), assertedName(w.GetMessage()))
}
