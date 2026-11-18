package main

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/runstatus"
)

// runTimeoutLine is `run get`'s Timeout value (docs/DSL.md "Run timeout"):
// the declared limit, then where the run's clock stands — not started yet,
// its deadline and the time left, the instant it timed out — and, once the
// run is failing under a hard end, when its failure handlers stop. It is
// empty for a run whose Orkflow declares no timeout. now measures the time
// left; nothing in a finished run's line depends on it.
func runTimeoutLine(run *orkstackv1.Run, now time.Time) string {
	timeout := run.GetTimeout()
	// A malformed answer that still says the timeout acted is shown, never
	// dropped.
	if timeout == nil || (timeout.GetSeconds() <= 0 && !timeout.GetTimedOut() && !timeout.GetHardEndReached()) {
		return ""
	}
	limit := timeoutLabel(timeout.GetSeconds())
	live := runstatus.IsLive(run.GetStatus())
	deadline := timeout.GetDeadline()
	if !setTime(deadline) {
		if timeout.GetTimedOut() {
			return limit + " (timed out, deadline unknown)"
		}
		if live {
			return limit + " (starts when the run starts)"
		}
		return limit + " (the run never started)"
	}

	var state string
	switch {
	case timeout.GetTimedOut():
		state = "timed out at " + formatTime(deadline)
	case !live:
		state = "deadline " + formatTime(deadline)
		// A run that ended at or past its deadline without the timeout as its
		// cause (it was already failing) says the deadline passed.
		if finished := run.GetFinishedAt(); setTime(finished) && !finished.AsTime().Before(deadline.AsTime()) {
			state += ", passed"
		}
	default:
		state = "deadline " + formatTime(deadline) + ", " + timeLeft(deadline.AsTime().Sub(now))
	}
	if hardEnd := timeout.GetHardEnd(); setTime(hardEnd) {
		switch {
		case timeout.GetHardEndReached():
			state += "; failure handlers stopped at " + formatTime(hardEnd)
		case live:
			state += "; failure handlers stop at " + formatTime(hardEnd)
		}
	}
	return limit + " (" + state + ")"
}

// timeLeft is the time before a live run's deadline in whole minutes, or
// whole seconds under a minute, and "passed" once none is left: the engine
// records the timeout at its next workflow task, so a read can land between.
func timeLeft(left time.Duration) string {
	switch {
	case left <= 0:
		return "passed"
	case left < time.Minute:
		// Whole seconds rounded up, so a live run short of its deadline
		// never reads "0s left".
		return shortDuration((left + time.Second - 1).Truncate(time.Second)) + " left"
	default:
		return shortDuration(left.Truncate(time.Minute)) + " left"
	}
}

// timeoutLabel is a timeout's seconds as an operator writes the duration
// (6h, 1h30m), or plain seconds for a value no time.Duration holds, which
// the DSL's bounds never produce but a malformed answer could.
func timeoutLabel(seconds int64) string {
	if seconds > math.MaxInt64/int64(time.Second) || seconds < 0 {
		return fmt.Sprintf("%ds", seconds)
	}
	return shortDuration(time.Duration(seconds) * time.Second)
}

// setTime reports whether a timestamp carries an instant, the test
// formatTime applies before it prints one.
func setTime(ts *timestamppb.Timestamp) bool {
	return ts != nil && ts.IsValid() && !ts.AsTime().IsZero()
}
