package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// Join policies and failure handlers (docs/DSL.md "Joins and failure
// handlers"). Everything here is either a pure helper shared by the
// interpreter and the plan, or a branch of the interpreter entered ONLY by a
// definition that declares a join — which no history recorded before join
// policies existed holds — so no workflow.GetVersion marker is needed.

// needOutcome is a need's outcome as a join reads it. The interpreter maps
// TaskRun.status onto it; Simulate maps PlanOutcome onto it.
type needOutcome int

const (
	needSucceeded needOutcome = iota
	needFailed
	needSkipped
	needCanceled
)

// word is the status word a sentence names for an outcome.
func (o needOutcome) word() string {
	switch o {
	case needSucceeded:
		return "SUCCEEDED"
	case needFailed:
		return "FAILED"
	case needSkipped:
		return "SKIPPED"
	default:
		return "CANCELED"
	}
}

// isFailureHandler reports whether t's join is any_failed or all_done: the
// only kind of task that starts in a failing run.
func isFailureHandler(t *orkstackv1.Task) bool { return dsl.IsFailureHandler(t) }

// joinAdmits is the join table (docs/DSL.md "Joins and failure handlers"): whether a task whose needs are all terminal is
// admitted by its join, and when not, the reason recorded after "task
// skipped: ". Pure; shared by classify and Simulate so a plan and a run say
// one sentence. UNSPECIFIED (all_succeeded) is classify's own loop; it is
// answered here too, with that loop's sentence, for completeness.
func joinAdmits(policy orkstackv1.JoinPolicy, needs []string,
	outcome func(id string) needOutcome,
) (admitted bool, reason string) {
	switch policy {
	case orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED:
		succeeded := false
		for _, need := range needs {
			switch o := outcome(need); o {
			case needFailed, needCanceled:
				return false, skipReasonNeedDidNotSucceed(need, o.word())
			case needSucceeded:
				succeeded = true
			}
		}
		if !succeeded {
			return false, skipReasonEveryNeedSkipped(needs)
		}
		return true, ""
	case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED:
		for _, need := range needs {
			if outcome(need) == needFailed {
				return true, ""
			}
		}
		return false, skipReasonNoNeedFailed(needs)
	case orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
		return true, ""
	default:
		for _, need := range needs {
			if outcome(need) == needSkipped {
				return false, skipReasonUpstream(need)
			}
		}
		return true, ""
	}
}

// skipReasonEveryNeedSkipped is none_failed's sentence when every need was
// skipped.
func skipReasonEveryNeedSkipped(needs []string) string {
	return fmt.Sprintf("join none_failed: every need was skipped (%s)", strings.Join(needs, ", "))
}

// skipReasonNeedDidNotSucceed is none_failed's sentence for a need that
// failed or was canceled — unreachable through the interpreter (an ordinary
// task is never classified in a failing run), defined for completeness.
func skipReasonNeedDidNotSucceed(id, status string) string {
	return fmt.Sprintf("join none_failed: need %q did not succeed (%s)", id, status)
}

// skipReasonNoNeedFailed is any_failed's sentence. It names no status on
// purpose: the plan prints the same sentence, and a plan never says
// SUCCEEDED.
func skipReasonNoNeedFailed(needs []string) string {
	return fmt.Sprintf("join any_failed: none of its needs failed (%s); it runs only when one of them fails",
		strings.Join(needs, ", "))
}

// cancelReasonNotAHandler is what fail-fast records on a never-started task
// that is not a failure handler, in a run with failure handlers.
func cancelReasonNotAHandler(cause string) string {
	return fmt.Sprintf("the run is failing (task %q failed) and this task is not a failure handler", cause)
}

const (
	// cancelReasonRunCanceled is a pending failure handler's record when an
	// operator cancels the run before anything failed.
	cancelReasonRunCanceled = "the run was canceled; a canceled run starts no failure handler"
	// cancelReasonHandlerStopped is a pending (or mid-dispatch) failure
	// handler's record when an operator cancels a failing run.
	cancelReasonHandlerStopped = "the run was canceled while its failure handlers ran"
	// cancelReasonNeedsUnfinished and cancelReasonStartUnrecorded are F9's
	// defensive sentences; F4, F6 and the failing phase leave nothing for
	// them, so neither ever appears (a test asserts it).
	cancelReasonNeedsUnfinished = "the run ended before this task's needs all finished"
	cancelReasonStartUnrecorded = "the run ended before this task's start was recorded"
)

// failingMessage is the failing note: the one TASK_LOG, with no task id, a
// run with failure handlers emits when it starts failing.
func failingMessage(cause string, handlers []string) string {
	return fmt.Sprintf("run failing after task %q failed; waiting for its failure handlers: %s",
		cause, strings.Join(handlers, ", "))
}

// The bounds of one NeedOutcome.error, each on the length of that field's
// JSON-escaped encoding (encoding/json with HTML escaping off — an upper bound
// on what protojson writes for the same string), and of the whole context.
const (
	needErrorCodeBytes    = 128
	needErrorMessageBytes = 1024
	needErrorDetailsBytes = 256
	// joinContextMaxBytes bounds task.PayloadSize(JoinContext) for any handler
	// rule 42 admits: 32 needs, each with a 200-byte task id (the name rule's
	// ceiling) and an error at every bound. TestJoinContextFor_WorstCase
	// builds exactly that context and asserts it.
	joinContextMaxBytes = 64 << 10
)

// ellipsis ends a cut text. It escapes to itself (3 bytes).
const ellipsis = "…"

// escapedLen is the length of s's JSON string encoding without its quotes.
func escapedLen(s string) int {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)    // a string always encodes
	return buf.Len() - 3 // the two quotes and Encode's newline
}

// boundEscaped returns s unchanged when its escaped length is within limit,
// and otherwise its longest rune-aligned prefix whose escaped length plus
// that of "…" is within limit, followed by "…". Pure and deterministic.
func boundEscaped(s string, limit int) string {
	if escapedLen(s) <= limit {
		return s
	}
	budget := limit - escapedLen(ellipsis)
	used, cut := 0, 0
	for i := 0; i < len(s); {
		_, size := utf8.DecodeRuneInString(s[i:])
		n := escapedLen(s[i : i+size])
		if used+n > budget {
			break
		}
		used += n
		i += size
		cut = i
	}
	return s[:cut] + ellipsis
}

// boundedError copies a need's error with code, message and details each
// bounded on its own escaped length; category and retryable as they are.
func boundedError(e *orkstackv1.Error) *orkstackv1.Error {
	if e == nil {
		return nil
	}
	return &orkstackv1.Error{
		Code:      boundEscaped(e.GetCode(), needErrorCodeBytes),
		Message:   boundEscaped(e.GetMessage(), needErrorMessageBytes),
		Details:   boundEscaped(e.GetDetails(), needErrorDetailsBytes),
		Retryable: e.GetRetryable(),
		Category:  e.GetCategory(),
	}
}

// joinContextFor builds a failure handler's JoinContext from its needs'
// records, in written order, each error bounded field by field. Pure: shared
// by schedule() and LocalTaskInput.
func joinContextFor(t *orkstackv1.Task, record func(id string) *orkstackv1.TaskRun) *orkstackv1.JoinContext {
	out := &orkstackv1.JoinContext{Policy: t.GetJoin()}
	for _, need := range t.GetNeeds() {
		tr := record(need)
		out.Needs = append(out.Needs, &orkstackv1.NeedOutcome{
			TaskId: need,
			Status: tr.GetStatus(),
			Error:  boundedError(tr.GetError()),
		})
	}
	return out
}

// failedNeedRef is the UpstreamRef a failure handler gets for a direct need
// that FAILED with output artifacts — task_id and artifacts only — or nil: for
// a need with no artifacts, and for a fan-out, composed or awaited need, whose
// own rows carry none. Pure; shared by schedule() and LocalTaskInput.
func failedNeedRef(need *orkstackv1.TaskRun) *orkstackv1.UpstreamRef {
	if need.GetStatus() != orkstackv1.Status_STATUS_FAILED || len(need.GetArtifacts()) == 0 ||
		need.GetEach() != nil || need.GetChild() != nil || need.GetAwait() != nil {
		return nil
	}
	return &orkstackv1.UpstreamRef{TaskId: need.GetTaskId(), Artifacts: need.GetArtifacts()}
}

// needOutcomeOf maps a need's recorded status onto a join's outcome. Only a
// terminal status reaches it: a task is classified once every need is.
func (s *runState) needOutcomeOf(id string) needOutcome {
	switch s.taskRuns[id].GetStatus() {
	case orkstackv1.Status_STATUS_SUCCEEDED:
		return needSucceeded
	case orkstackv1.Status_STATUS_FAILED:
		return needFailed
	case orkstackv1.Status_STATUS_SKIPPED:
		return needSkipped
	default:
		return needCanceled
	}
}

// isFailureHandlerID is isFailureHandler by task id.
func (s *runState) isFailureHandlerID(id string) bool {
	return isFailureHandler(s.tasks[id])
}

// joinHandlersOnRunContextForTest is a TEST-ONLY switch, the precedent of
// awaitDeadlineAfterEmit: with it set, dispatchContext returns runCtx for a
// failure handler, which is how TestJoinHistory_CatchesAHandlerOnTheRunContext
// proves the committed join fixture can see a handler scheduled on the wrong
// context. Nothing but that test sets it.
var joinHandlersOnRunContextForTest = false

// dispatchContext is the context a ready task is dispatched on: the handler
// context for a failure handler — a sibling of runCtx, so fail-fast's
// cancelRun() does not reach it and an operator's cancel does — and runCtx
// for every other task, which is today's path for every definition without a
// failure handler.
func (s *runState) dispatchContext(runCtx workflow.Context, id string) workflow.Context {
	if s.handlerCtx != nil && s.isFailureHandlerID(id) && !joinHandlersOnRunContextForTest {
		return s.handlerCtx
	}
	return runCtx
}

// eventContext is ctx while it is not done, and otherwise one disconnected
// context created once and kept — the substitution today's fail makes,
// made lazily because an operator may cancel during F6 or F8.
func (s *runState) eventContext(ctx workflow.Context) workflow.Context {
	if ctx.Err() == nil {
		return ctx
	}
	if s.detached == nil {
		s.detached, s.cancelDetached = workflow.NewDisconnectedContext(ctx)
	}
	return s.detached
}

// failCondition records the task whose condition could not be evaluated
// FAILED when the definition declares a failure handler, then returns the
// evaluation error unchanged, so fail() records the run's error exactly as
// before (RUN_FAILED, the error's text, task "<id>").
func (s *runState) failCondition(ctx workflow.Context, id string, err error) error {
	if !s.hasHandlers {
		return err // today's path, byte for byte: the task is CANCELED by fail()
	}
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:     "CONDITION_FAILED",
		Message:  err.Error(),
		Details:  fmt.Sprintf("task %q", id),
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
	}
	if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, err.Error()); emitErr != nil {
		workflow.GetLogger(ctx).Error("emit condition failure failed",
			"run_id", s.runID, "task_id", id, "error", emitErr)
	}
	// Terminal, so the pin may be done with — as markSkipped, markCanceled
	// and collect do. A pinned task whose condition broke and that was its
	// pin's last non-terminal member releases the host slot and the shared
	// directory now, not at F11 after every handler has run.
	s.closePinFor(ctx, id)
	return err
}

// recordRefusedDispatch is recordFailure(ctx, id, err) followed by a
// TASK_FAILED emit carrying err's text; an emit error is logged, never
// returned. It ends with s.closePinFor(ctx, id), as markSkipped, markCanceled
// and collect do on every terminal transition: a pinned cause that was its
// pin's last non-terminal member releases the host slot and the shared
// directory now, not at F11 after every handler. An await left open by an
// emit that failed after its timer started is closed first, so nothing waits
// on its deadline. Shared by F4 and settleHandlerDispatch.
func (s *runState) recordRefusedDispatch(ctx workflow.Context, id string, err error) {
	if a := s.awaits[id]; a != nil && a.open {
		a.open = false
		if a.cancelDeadline != nil {
			a.cancelDeadline()
		}
	}
	s.recordFailure(ctx, id, err)
	if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		s.taskRuns[id].GetStatus(), err.Error()); emitErr != nil {
		workflow.GetLogger(ctx).Error("emit refused dispatch failed",
			"run_id", s.runID, "task_id", id, "error", emitErr)
	}
	s.closePinFor(ctx, id)
}

// settleHandlerDispatch records a failure handler whose dispatch returned an
// error. Nothing to do when the dispatch already recorded a terminal outcome
// (startChild's failComposed and cancelled-start paths do). Otherwise the
// task is RUNNING with nothing tracking it — schedule() and startChild()
// set RUNNING before the emits that can fail — and is recorded now, so no
// task is left non-terminal:
//   - the operator's cancel reached handlerCtx between the loop's check and
//     an emit (ctx done, or a cancelled error): CANCELED with
//     cancelReasonHandlerStopped, and scheduled_at cleared when started_at
//     is unset — no activity or child was started, and a client reads
//     "dispatched" from scheduled_at (ui/src/lib/status.ts taskDispatched);
//   - anything else (INPUT_TOO_LARGE, INVALID_POOL, a snapshot write that
//     failed): FAILED through recordRefusedDispatch, as F4 records the cause.
func (s *runState) settleHandlerDispatch(ctx workflow.Context, id string, err error) {
	taskRun := s.taskRuns[id]
	if isTerminal(taskRun.GetStatus()) {
		return
	}
	ectx := s.eventContext(ctx)
	if ctx.Err() != nil || temporal.IsCanceledError(err) {
		if taskRun.GetStartedAt() == nil {
			taskRun.ScheduledAt = nil
		}
		if markErr := s.markCanceled(ectx, id, cancelReasonHandlerStopped); markErr != nil {
			workflow.GetLogger(ctx).Error("mark canceled failed",
				"run_id", s.runID, "task_id", id, "error", markErr)
		}
		return
	}
	s.recordRefusedDispatch(ectx, id, err)
}

// failWithHandlers is fail() for a definition that declares a failure
// handler (docs/DSL.md "Joins and failure handlers"). The order is replay-visible.
func (s *runState) failWithHandlers(
	ctx workflow.Context,
	cancelRun workflow.CancelFunc,
	inFlight map[string]workflow.Future,
	taskID string,
	cause error,
) (*orkstackv1.Run, error) {
	// F1: the run's error, exactly as today's fail computes it.
	failure := s.runFailureFor(taskID, cause)

	// F2: canceled — today's predicate. No failure handler starts, and every
	// task still running, failure handlers included, is stopped.
	if ctx.Err() != nil || (temporal.IsCanceledError(cause) && s.pinLossOf(taskID) == nil) {
		return s.cancelWithHandlers(ctx, cancelRun, inFlight, taskID, cause, failure)
	}

	// F3: failing. The run's outcome is decided; it stays RUNNING, with its
	// error set, until its failure handlers have run.
	s.runError = failure

	// F4: a cause that is not terminal — a dispatch the engine refused after
	// schedule marked it RUNNING, or an emit that failed after it did — is
	// recorded, so its handlers see it FAILED.
	if !isTerminal(s.taskRuns[taskID].GetStatus()) {
		s.recordRefusedDispatch(s.eventContext(ctx), taskID, cause)
	}

	// F5: the version marker exactly where today's fail takes it — it is 1
	// for every run that can reach this path — then fail-fast.
	if v := workflow.GetVersion(ctx, "cancel-siblings-on-run-end", workflow.DefaultVersion, 1); v >= 1 {
		cancelRun()
	}

	// F6: fail-fast, minus the handlers. Every in-flight task, item and open
	// await that is not a failure handler is drained and recorded; every
	// never-started one becomes CANCELED with the not-a-handler sentence.
	s.cancelInFlight(s.eventContext(ctx), inFlight, s.isFailureHandlerID,
		func(string) string { return cancelReasonNotAHandler(taskID) })

	// F7: the failing note, once, when any handler is still to finish. No
	// task id: no client may attribute it to the cause's card.
	var outstanding []string
	for _, id := range s.order {
		if s.isFailureHandlerID(id) && !isTerminal(s.taskRuns[id].GetStatus()) {
			outstanding = append(outstanding, id)
		}
	}
	if len(outstanding) > 0 {
		if err := s.emit(s.eventContext(ctx), orkstackv1.EventType_EVENT_TYPE_TASK_LOG, "",
			orkstackv1.Status_STATUS_RUNNING, failingMessage(taskID, outstanding)); err != nil {
			// The run is already failing for a recorded reason, and the
			// handlers must still run.
			workflow.GetLogger(ctx).Error("emit failing note failed",
				"run_id", s.runID, "error", err)
		}
	}

	// F8: the failure handlers run to their own outcomes.
	s.runFailureHandlers(ctx, inFlight)

	// F9: a defensive sweep — no task is left non-terminal.
	ectx := s.eventContext(ctx)
	for _, id := range s.order {
		taskRun := s.taskRuns[id]
		status := taskRun.GetStatus()
		if isTerminal(status) {
			continue
		}
		reason := cancelReasonStartUnrecorded
		switch {
		case ctx.Err() != nil:
			reason = cancelReasonHandlerStopped
		case status == orkstackv1.Status_STATUS_PENDING:
			reason = cancelReasonNeedsUnfinished
		}
		if a := s.awaits[id]; a != nil && a.open {
			a.open = false
			if a.cancelDeadline != nil {
				a.cancelDeadline()
			}
		}
		if err := s.markCanceled(ectx, id, reason); err != nil {
			workflow.GetLogger(ctx).Error("mark canceled failed",
				"run_id", s.runID, "task_id", id, "error", err)
		}
	}

	// F10.
	s.status = orkstackv1.Status_STATUS_FAILED
	s.finished = timestamppb.New(workflow.Now(ctx))
	// F11.
	s.awaitPinsClosed(ectx)
	s.drainResolutions(ectx)
	// F12.
	if err := s.emit(ectx, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, taskID, s.status,
		s.runError.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("emit terminal run event failed",
			"run_id", s.runID, "event", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED.String(), "error", err)
	}
	if s.cancelDetached != nil {
		s.cancelDetached()
	}
	// F13: the workflow's error is the original cause.
	return s.snapshot(), cause
}

// cancelWithHandlers is F2: today's canceled end, with the handler context
// cancelled too and a pending handler's own sentence.
func (s *runState) cancelWithHandlers(
	ctx workflow.Context,
	cancelRun workflow.CancelFunc,
	inFlight map[string]workflow.Future,
	taskID string,
	cause error,
	failure *orkstackv1.Error,
) (*orkstackv1.Run, error) {
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError = failure
	disconnected, cancelDisconnected := workflow.NewDisconnectedContext(ctx)
	defer cancelDisconnected()
	eventCtx := disconnected
	s.status = orkstackv1.Status_STATUS_CANCELED
	s.runError.Code = "RUN_CANCELED"
	if taskRun, ok := s.taskRuns[taskID]; ok &&
		taskRun.GetStatus() != orkstackv1.Status_STATUS_AWAITING {
		taskRun.Status = orkstackv1.Status_STATUS_CANCELED
	}
	if v := workflow.GetVersion(ctx, "cancel-siblings-on-run-end", workflow.DefaultVersion, 1); v >= 1 {
		cancelRun()
		// A no-op when the operator's cancel already reached handlerCtx
		// through ctx; required when the cause is a cancellation that did not
		// cancel ctx, such as a child run cancelled from outside.
		s.cancelHandlers()
		s.cancelInFlight(eventCtx, inFlight, nil, func(id string) string {
			if s.isFailureHandlerID(id) {
				return cancelReasonRunCanceled
			}
			return runEndedBeforeStart
		})
	}
	s.awaitPinsClosed(eventCtx)
	s.drainResolutions(eventCtx)
	if err := s.emit(eventCtx, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, taskID, s.status,
		s.runError.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("emit terminal run event failed",
			"run_id", s.runID, "event", orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED.String(), "error", err)
	}
	return s.snapshot(), cause
}

// runFailureHandlers is the failing phase: every pending failure
// handler is classified as its needs settle and dispatched on handlerCtx;
// every outcome is recorded and none stops anything else. It returns when
// nothing is in flight, no await is open and nothing is ready.
func (s *runState) runFailureHandlers(ctx workflow.Context, inFlight map[string]workflow.Future) {
	for {
		ectx := s.eventContext(ctx)
		stopping := ctx.Err() != nil
		// Only failure handlers can be PENDING here (F6). Declaration order.
		for _, id := range s.readyTasks() {
			if stopping {
				if err := s.markCanceled(ectx, id, cancelReasonHandlerStopped); err != nil {
					workflow.GetLogger(ctx).Error("mark canceled failed",
						"run_id", s.runID, "task_id", id, "error", err)
				}
				continue
			}
			run, err := s.classify(ectx, id) // a condition error records CONDITION_FAILED itself
			if err != nil || !run {
				continue
			}
			switch {
			case s.awaits[id] != nil:
				// openAwait starts the deadline timer BEFORE its emit, so on an
				// emit error the await is open and its timer settles it; the
				// error is logged, never fatal to the failing phase.
				if err := s.openAwait(ectx, s.dispatchContext(s.runCtx, id), id); err != nil {
					workflow.GetLogger(ctx).Error("emit await opening failed",
						"run_id", s.runID, "task_id", id, "error", err)
				}
			case s.children[id] != nil:
				future, err := s.startChild(ectx, s.dispatchContext(s.runCtx, id), id)
				if err != nil {
					s.settleHandlerDispatch(ctx, id, err) // a no-op when startChild recorded its own outcome
					continue
				}
				inFlight[id] = future
			default:
				// schedule emits on the context it is given — handlerCtx — so
				// an operator's cancel landing after `stopping` was read makes
				// its emit fail with a cancellation: settleHandlerDispatch
				// records that CANCELED, never FAILED.
				future, err := s.schedule(s.dispatchContext(s.runCtx, id), id)
				if err != nil {
					s.settleHandlerDispatch(ctx, id, err)
					continue
				}
				inFlight[id] = future
			}
		}
		if len(inFlight) == 0 && s.openAwaits() == 0 {
			if len(s.readyTasks()) == 0 {
				return
			}
			continue
		}

		// Fixed registration order, as the main loop's: in-flight futures in
		// s.order; then the resolution channel when the run has one; then
		// every open await's deadline in s.order.
		selector := workflow.NewSelector(ctx)
		selectedID := ""
		for _, id := range s.order {
			if future, ok := inFlight[id]; ok {
				taskID := id
				selector.AddFuture(future, func(workflow.Future) { selectedID = taskID })
			}
		}
		var resolution *orkstackv1.AwaitSignal
		resolved := false
		expired := ""
		if s.resolutions != nil {
			selector.AddReceive(s.resolutions, func(c workflow.ReceiveChannel, _ bool) {
				c.Receive(ctx, &resolution)
				resolved = true
			})
			for _, id := range s.order {
				if a := s.awaits[id]; a != nil && a.open {
					awaitID := id
					selector.AddFuture(a.deadline, func(workflow.Future) { expired = awaitID })
				}
			}
		}
		selector.Select(ctx)
		ectx = s.eventContext(ctx)
		switch {
		case resolved:
			_ = s.resolve(ectx, resolution) // the outcome is recorded on the task
		case expired != "":
			if err := s.expire(ectx, expired); err != nil && temporal.IsCanceledError(err) &&
				s.taskRuns[expired].GetStatus() == orkstackv1.Status_STATUS_AWAITING {
				if cancelErr := s.cancelAwaiting(ectx, expired); cancelErr != nil {
					workflow.GetLogger(ctx).Error("mark canceled failed",
						"run_id", s.runID, "task_id", expired, "error", cancelErr)
				}
			}
		default:
			future := inFlight[selectedID]
			delete(inFlight, selectedID)
			_ = s.collect(ectx, selectedID, future) // FAILED or SUCCEEDED, recorded; never fail-fast
		}
	}
}
