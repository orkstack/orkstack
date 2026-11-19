package api

import (
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/enumname"
	"github.com/orkstack/orkstack/internal/runstatus"
)

// The checks PauseRun and UnpauseRun make against the read model before
// anything is sent (design §5.1, checks 3–9). They are best effort: the read
// model can lag and clocks can differ, and in every race the run's own
// decision is final; the request's event says what it did.

// pauseRefused is a FAILED_PRECONDITION whose message begins with its reason,
// as an admission refusal's does.
func pauseRefused(code, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	if code != "" {
		message = code + ": " + message
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s", message))
}

// pauseRequestRefusal is the refusal of a pause (pause true) or an unpause of
// run at now, or nil when the request may be sent.
func pauseRequestRefusal(run *orkstackv1.Run, pause bool, now time.Time) error {
	id := run.GetId()
	status := run.GetStatus()
	if runstatus.IsTerminal(status) {
		return pauseRefused("", "run %q has ended (%s)", id, enumname.Of(status))
	}
	if !run.GetPausable() {
		return pauseRefused(engine.RunPauseUnsupportedCode,
			"run %q was started without support for pausing (it began before this release, or under one without it); "+
				"it can be cancelled, or left to finish", id)
	}
	if run.GetQueued() != nil {
		return pauseRefused(engine.RunPauseNotRunningCode,
			"run %q is waiting for its concurrency slot; cancel it if it should not continue", id)
	}
	if status == orkstackv1.Status_STATUS_PENDING {
		return pauseRefused(engine.RunPauseNotRunningCode, "run %q has not started yet", id)
	}
	if pause {
		if status == orkstackv1.Status_STATUS_PAUSED || run.GetPause() != nil || run.GetPausedByParent() != nil {
			if own := run.GetPause(); own != nil {
				return pauseRefused(engine.RunAlreadyPausedCode,
					"run %q is already paused, since %s by %s, until %s; unpause it to pause it again",
					id, instant(own.GetPausedAt()), pausedByMark(own), instant(own.GetExpiresAt()))
			}
			return pauseRefused(engine.RunAlreadyPausedCode,
				"run %q is already paused because its parent run %q is paused", id, run.GetPausedByParent().GetParentRunId())
		}
		if run.GetError() != nil && !hasHandlerToStart(run) {
			return pauseRefused(engine.RunEndingCode,
				"run %q is failing and all its failure handlers have started; a pause would hold nothing", id)
		}
		return nil
	}
	if parent := run.GetPausedByParent(); parent != nil {
		return pauseRefused(engine.RunPauseInheritedCode,
			"run %q is paused because its parent run %q is paused; unpause %q", id, parent.GetParentRunId(), parent.GetParentRunId())
	}
	if own := run.GetPause(); own != nil && own.GetExpiresAt() != nil && !own.GetExpiresAt().AsTime().After(now) {
		return pauseRefused(engine.RunEndingCode, "run %q's pause expired at %s; it is ending", id, instant(own.GetExpiresAt()))
	}
	return nil
}

// hasHandlerToStart reports whether a failure handler of run has not
// started: one a pause would hold.
func hasHandlerToStart(run *orkstackv1.Run) bool {
	for _, task := range run.GetTaskRuns() {
		join := task.GetJoin()
		if (join == orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED || join == orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE) &&
			task.GetStatus() == orkstackv1.Status_STATUS_PENDING {
			return true
		}
	}
	return false
}

// pauseTimeoutOf is a pause's length: the request's, or the deployment's
// default when it names none, refused below one second, off whole seconds,
// or above the deployment's maximum.
func pauseTimeoutOf(asked time.Duration, plan engine.PausePlan) (time.Duration, error) {
	maximum := time.Duration(plan.MaxTimeoutSeconds) * time.Second
	if asked == 0 {
		return time.Duration(plan.DefaultTimeoutSeconds) * time.Second, nil
	}
	switch {
	case asked < time.Second:
		return 0, fmt.Errorf("timeout %s is below the minimum of 1s", asked)
	case asked%time.Second != 0:
		return 0, fmt.Errorf("timeout %s is not a whole number of seconds", asked)
	case asked > maximum:
		return 0, fmt.Errorf("timeout %s is above this deployment's maximum of %s (engine.run_pause_timeout_max)", asked, maximum)
	}
	return asked, nil
}

// pauseWarnings are what pausing run until expires will not hold back, said
// before the pause and in its response: a failing run kept open, deadlines
// that end first (every clock keeps running while paused), the child runs
// paused with it, and a schedule that cancels it at its next fire.
func pauseWarnings(run *orkstackv1.Run, length time.Duration, expires time.Time, cancelOther bool) []string {
	var warnings []string
	before := func(at *timestamppb.Timestamp) bool { return at != nil && at.AsTime().Before(expires) }
	if run.GetError() != nil {
		warnings = append(warnings, fmt.Sprintf(
			"the run is failing; pausing holds its failure handlers and keeps it open, counted toward max_running, for up to %s",
			length))
	}
	if timeout := run.GetTimeout(); timeout.GetDeadline() != nil && before(timeout.GetDeadline()) && !timeout.GetTimedOut() {
		warnings = append(warnings, fmt.Sprintf(
			"this run's timeout ends at %s, before this pause does; it will then fail, and its failure handlers will wait for the unpause until %s",
			instant(timeout.GetDeadline()), instant(timeout.GetHardEnd())))
	}
	children := 0
	for _, task := range run.GetTaskRuns() {
		if deadline := task.GetAwait().GetDeadline(); deadline != nil && task.GetStatus() == orkstackv1.Status_STATUS_AWAITING && before(deadline) {
			warnings = append(warnings, fmt.Sprintf(
				"awaited task %s's deadline is %s, before this pause ends; it will then time out", task.GetTaskId(), instant(deadline)))
		}
		if task.GetChild().GetRunId() != "" && runstatus.IsLive(task.GetStatus()) {
			children++
		}
	}
	switch children {
	case 0:
	case 1:
		warnings = append(warnings, "1 running child run is paused too")
	default:
		warnings = append(warnings, fmt.Sprintf("%d running child runs are paused too", children))
	}
	if cancelOther {
		warnings = append(warnings,
			"the schedule that started this run cancels it at its next fire while it is paused (overlap policy CANCEL_OTHER)")
	}
	return warnings
}

// pausedByMark is who paused a run, marked as the record says: verified
// when OrkStack authenticated them, asserted otherwise.
func pausedByMark(p *orkstackv1.RunPause) string {
	return principalMark(principalOrUnknown(p.GetPausedBy()), p.GetPausedByVerified())
}

// principalMark marks a principal (verified) or (asserted) (AGENTS.md §7,
// Principal).
func principalMark(principal string, verified bool) string {
	if verified {
		return principal + " (verified)"
	}
	return principal + " (asserted)"
}

func principalOrUnknown(principal string) string {
	if principal == "" {
		return "an unnamed caller"
	}
	return principal
}

// instant renders a timestamp in RFC 3339 UTC; unset reads as "an unknown
// time".
func instant(at *timestamppb.Timestamp) string {
	if at == nil {
		return "an unknown time"
	}
	return at.AsTime().UTC().Format(time.RFC3339)
}
