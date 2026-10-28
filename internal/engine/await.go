package engine

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ResolveAwaitSignal is the Temporal Signal name that resolves an awaited
// task. It is recorded in history and chosen once; it is internal and never
// appears on a user-facing surface (AGENTS.md §7).
const ResolveAwaitSignal = "ResolveAwait"

// Terminal reason codes, as TaskRun.error.code.
const (
	AwaitTimedOutCode = "AWAIT_TIMED_OUT"
	AwaitRejectedCode = "AWAIT_REJECTED"
)

// AwaitRejectedMessage is the recorded error message of a rejected await. It
// is exported because the doors compare it: a rejection that repeats the one
// already applied — same reason, so the same message — is answered as a
// repeat rather than refused.
func AwaitRejectedMessage(taskID, reason string) string {
	return fmt.Sprintf("await %q was rejected: %s", taskID, reason)
}

// awaitState is one awaited task's life in this run. Like pinState, every
// field is a pure function of the workflow input and recorded history.
type awaitState struct {
	spec *orkstackv1.AwaitSpec
	// open is true from the moment the task starts awaiting until a
	// resolution, its deadline, or the run's end closes it.
	open bool
	// deadline settles when the timer fires, or with a cancellation when the
	// run ends or the await is resolved first.
	deadline       workflow.Future
	cancelDeadline workflow.CancelFunc
	// deadlineAt is the instant the timer fires, on the workflow's clock.
	deadlineAt time.Time
	timeout    time.Duration
}

// awaitVia is the word a resolution's door is rendered with in an event.
func awaitVia(via orkstackv1.AwaitResolvedVia) string {
	switch via {
	case orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API:
		return "api"
	case orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK:
		return "callback"
	default:
		return "unknown"
	}
}

// awaitResolver is "via <door>[ by <principal>]".
func awaitResolver(via orkstackv1.AwaitResolvedVia, principal string) string {
	out := "via " + awaitVia(via)
	if principal != "" {
		out += " by " + principal
	}
	return out
}

// statusWord is a Status as the clients print it: "SUCCEEDED", not
// "STATUS_SUCCEEDED".
func statusWord(status orkstackv1.Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}

// openAwaits counts the awaits that are waiting now, over s.order and never
// over the map.
func (s *runState) openAwaits() int {
	open := 0
	for _, id := range s.order {
		if a := s.awaits[id]; a != nil && a.open {
			open++
		}
	}
	return open
}

// openAwait starts one awaited task waiting: its deadline timer, its snapshot,
// its TASK_AWAITING event — and no activity. It is called from the main loop
// for a ready task classify decided to run.
//
// The timer is started on runCtx, so fail()'s cancelRun() and an operator's
// run cancellation both settle it: that is the await's cancellation path, as
// the session context is a pin's. It is started BEFORE the event is emitted,
// and that order is part of the replay-pinned command stream
// (TestAwaitedHistory_CatchesAMovedTimer).
func (s *runState) openAwait(ctx, runCtx workflow.Context, id string) error {
	a := s.awaits[id]
	now := workflow.Now(ctx)
	timeout := time.Duration(a.spec.GetTimeoutSeconds()) * time.Second

	a.open = true
	s.chargeAwaitOpen()
	a.timeout = timeout
	a.deadlineAt = now.Add(timeout)
	startDeadline := func() {
		timerCtx, cancel := workflow.WithCancel(runCtx)
		a.deadline = workflow.NewTimer(timerCtx, timeout)
		a.cancelDeadline = cancel
	}
	if !awaitDeadlineAfterEmit {
		startDeadline()
	}

	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_AWAITING
	// Both: the task was never queued, and a pre-contract snapshot is
	// recognised by scheduled_at unset with started_at set — a combination an
	// await must never produce.
	taskRun.ScheduledAt = timestamppb.New(now)
	taskRun.StartedAt = timestamppb.New(now)
	taskRun.Await.Deadline = timestamppb.New(a.deadlineAt)

	message := "task awaiting external completion until " + a.deadlineAt.UTC().Format(time.RFC3339)
	if a.spec.GetCallback() {
		message += "; accepts a callback"
	}
	err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, id,
		orkstackv1.Status_STATUS_AWAITING, message)
	if awaitDeadlineAfterEmit {
		startDeadline()
	}
	return err
}

// awaitDeadlineAfterEmit reorders openAwait's two commands — the deadline
// timer AFTER the TASK_AWAITING emit instead of before it. It is false in
// every build and is never set outside one test,
// TestAwaitedHistory_CatchesAMovedTimer, which flips it to prove the
// committed awaited fixture can see a reordered command. A package variable
// rather than a second copy of RunWorkflow, so the body that test replays is
// the real one with exactly one thing moved.
var awaitDeadlineAfterEmit = false

// ignoreResolution records a resolution the run did not apply as a TASK_LOG
// carrying the task's CURRENT status, so it is never silently dropped and a
// client that applies only SKIPPED/CANCELED from a TASK_LOG changes nothing.
// An emit error is logged, not returned: the task's outcome is already
// recorded and an ignored message must not end a run.
func (s *runState) ignoreResolution(ctx workflow.Context, sig *orkstackv1.AwaitSignal, why string) {
	id := sig.GetTaskId()
	taskRun := s.taskRuns[id]
	message := fmt.Sprintf("resolution ignored: %s (%s)", why,
		awaitResolver(sig.GetVia(), sig.GetPrincipal()))
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id,
		taskRun.GetStatus(), message); err != nil {
		workflow.GetLogger(ctx).Error("emit ignored resolution failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
}

// resolve applies one resolution taken off the signal channel.
func (s *runState) resolve(ctx workflow.Context, sig *orkstackv1.AwaitSignal) error {
	id := sig.GetTaskId()
	a := s.awaits[id]
	if a == nil {
		// No such await in this run: there is no task to attach an event to,
		// and only a direct cluster client can send one (both doors refuse it).
		workflow.GetLogger(ctx).Warn("resolution for a task that is not an await of this run",
			"run_id", s.runID, "task_id", id)
		return nil
	}
	taskRun := s.taskRuns[id]
	if taskRun.GetStatus() != orkstackv1.Status_STATUS_AWAITING {
		why := "task is " + statusWord(taskRun.GetStatus())
		if code := taskRun.GetError().GetCode(); code != "" {
			why += " (" + code + ")"
		}
		s.ignoreResolution(ctx, sig, why)
		return nil
	}
	if !sig.GetReject() && sig.GetValues() == nil {
		s.ignoreResolution(ctx, sig, "the completion carries no values")
		return nil
	}

	a.open = false
	a.cancelDeadline()
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Await.ResolvedVia = sig.GetVia()
	taskRun.Await.ResolvedBy = sig.GetPrincipal()
	resolver := awaitResolver(sig.GetVia(), sig.GetPrincipal())

	if sig.GetReject() {
		message := AwaitRejectedMessage(id, sig.GetReason())
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.Error = &orkstackv1.Error{
			Code:      AwaitRejectedCode,
			Message:   message,
			Details:   fmt.Sprintf("task %q; rejected %s", id, resolver),
			Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
			Retryable: false,
		}
		if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED, id,
			orkstackv1.Status_STATUS_FAILED, fmt.Sprintf("await rejected %s: %s", resolver, sig.GetReason())); err != nil {
			workflow.GetLogger(ctx).Error("emit await rejection failed",
				"run_id", s.runID, "task_id", id, "error", err)
		}
		if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, message); err != nil {
			workflow.GetLogger(ctx).Error("emit task failure failed",
				"run_id", s.runID, "task_id", id, "error", err)
		}
		return temporal.NewNonRetryableApplicationError(message, AwaitRejectedCode, nil)
	}

	// A completion. The values were stored by the door BEFORE the signal was
	// sent; what arrives here is the reference and, at 4 KiB or less, the
	// exact bytes — decoded for conditions and display only, exactly as a
	// container's are in collect.
	values := decodeValues(sig.GetValuesJson())
	output, convErr := payloadOf(values)
	if convErr != nil {
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.Error = &orkstackv1.Error{
			Code:     "TASK_OUTPUT_INVALID",
			Message:  convErr.Error(),
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		}
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, convErr.Error()); emitErr != nil {
			workflow.GetLogger(ctx).Error("emit task failure failed",
				"run_id", s.runID, "task_id", id, "error", emitErr)
		}
		return convErr
	}
	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.ValuesRef = sig.GetValues()
	taskRun.Output = output
	// A local value only: upstreamFor projects it into UpstreamRef{task_id,
	// values} with an empty commit_uri — an await has no commit.json, the
	// Signal in history is its commit.
	s.results[id] = &orkstackv1.ExecuteTaskResult{
		TaskId:        id,
		ValuesRef:     sig.GetValues(),
		ValuesJson:    sig.GetValuesJson(),
		ValuesInlined: len(sig.GetValuesJson()) > 0,
	}
	s.outputs[id] = values
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "await resolved "+resolver); err != nil {
		return err
	}
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task succeeded")
}

// expire takes one await's settled deadline off the Selector.
//
// A timer that settled with an error was cancelled because the run is ending —
// an operator's CancelRun, or cancelRun() in fail() — and the error is handed
// back unchanged so fail() records RUN_CANCELED. A timer that fired is the
// deadline: the task fails AWAIT_TIMED_OUT and the run fails with it. It
// therefore always returns an error, never nil.
func (s *runState) expire(ctx workflow.Context, id string) error {
	a := s.awaits[id]
	err := a.deadline.Get(ctx, nil)
	a.open = false
	if err != nil {
		return err
	}
	taskRun := s.taskRuns[id]
	message := fmt.Sprintf("await %q was not resolved within %s (deadline %s)",
		id, a.timeout, a.deadlineAt.UTC().Format(time.RFC3339))
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:      AwaitTimedOutCode,
		Message:   message,
		Details:   fmt.Sprintf("task %q", id),
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
		Retryable: false,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, AwaitTimedOutCode, nil)
}

// cancelAwaiting closes one await the run is ending under: its timer is
// cancelled and the task recorded CANCELED. Reached only for a task whose
// status is AWAITING, which no history recorded before awaits existed holds.
func (s *runState) cancelAwaiting(ctx workflow.Context, id string) error {
	if a := s.awaits[id]; a != nil {
		a.open = false
		if a.cancelDeadline != nil {
			a.cancelDeadline()
		}
	}
	return s.markCanceled(ctx, id, "run ended while this task was awaiting external completion")
}

// drainResolutions records every resolution still queued on the channel when
// the run ends, so one that reached history after its await closed is an
// event rather than dropped. ReceiveAsync never blocks.
func (s *runState) drainResolutions(ctx workflow.Context) {
	if s.resolutions == nil {
		return
	}
	for {
		var sig *orkstackv1.AwaitSignal
		if !s.resolutions.ReceiveAsync(&sig) {
			return
		}
		id := sig.GetTaskId()
		if s.awaits[id] == nil {
			workflow.GetLogger(ctx).Warn("resolution for a task that is not an await of this run",
				"run_id", s.runID, "task_id", id)
			continue
		}
		why := "task is " + statusWord(s.taskRuns[id].GetStatus())
		if code := s.taskRuns[id].GetError().GetCode(); code != "" {
			why += " (" + code + ")"
		}
		s.ignoreResolution(ctx, sig, why)
	}
}
