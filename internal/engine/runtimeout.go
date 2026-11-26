package engine

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The run-level timeout (docs/DSL.md "Run timeout"): a top-level `timeout:`
// bounds a run's wall clock from its start. RunWorkflow enforces it with one
// Timer per generation and a check at the top of every pass; at the deadline
// D a run that is not failing fails RUN_TIMED_OUT through its ordinary fail
// path, so it has a terminal event, a snapshot, an archive, closed pins and
// cancelled containers. Failure handlers may run until the hard end
// H = D + the deployment's grace.
//
// Every branch here is entered only when the definition declares `timeout:`
// (s.runTimeout != nil): no recorded history holds such a definition, so every
// recorded history replays unchanged without a version marker (AGENTS.md
// §7.2, definition-gated).

// Codes of the run timeout.
const (
	// RunTimedOutCode is the run's error when its deadline ended it.
	RunTimedOutCode = "RUN_TIMED_OUT"
	// TaskStoppedByRunTimeoutCode is the row of work running at the deadline.
	TaskStoppedByRunTimeoutCode = "TASK_STOPPED_BY_RUN_TIMEOUT"
	// RunDefinitionUnsupportedCode ends a run whose definition declares a
	// feature its plan does not carry.
	RunDefinitionUnsupportedCode = "RUN_DEFINITION_UNSUPPORTED"
	// RunTimeoutStateInvalidCode ends a started run that has no deadline.
	RunTimeoutStateInvalidCode = "RUN_TIMEOUT_STATE_INVALID"
)

// RunTimeoutPlan is what a run with a timeout carries in its plan, decided at
// submit and pinned (AGENTS.md §7.2).
type RunTimeoutPlan struct {
	// HandlerGraceSeconds is how long after D failure handlers may run
	// (engine.run_timeout_handler_grace).
	HandlerGraceSeconds int64 `json:"handler_grace_seconds"`
	// ResolutionsPerPass bounds the resolutions one pass applies
	// (DefaultResolutionsPerPass).
	ResolutionsPerPass int32 `json:"resolutions_per_pass"`
}

// DefaultResolutionsPerPass is the one default of the bounded resolution
// drain, shared with the identity design's plan (pinned per run).
const DefaultResolutionsPerPass = 32

// runTimeout is the run's own time limit. Nil when the definition declares no
// timeout, and then no command or row of the run changes.
type runTimeout struct {
	seconds, graceSeconds int64 // derived: definition, plan

	deadline time.Time       // D; zero until the clock starts (restored)
	taken    bool            // D acted on
	timer    workflow.Future // D's timer this generation (rebuilt)

	hardEnd   time.Time       // H; set when the failing phase starts
	hardTimer workflow.Future // failing phase only
	hardTaken bool            // H's timer consumed
	hardEnded bool            // H reached: handlers stopped

	// timeoutDrain is true only during D's fail-fast drain: interrupted
	// work is recorded FAILED TASK_STOPPED_BY_RUN_TIMEOUT, not CANCELED.
	timeoutDrain bool
}

// newRunTimeout builds the state for a definition that declares a timeout,
// and nil otherwise.
func newRunTimeout(wf *orkstackv1.Orkflow, plan RunPlan) *runTimeout {
	seconds := wf.GetTimeoutSeconds()
	if seconds <= 0 {
		return nil
	}
	t := &runTimeout{seconds: seconds}
	if plan.RunTimeout != nil {
		t.graceSeconds = plan.RunTimeout.HandlerGraceSeconds
	}
	return t
}

func (t *runTimeout) limit() time.Duration { return time.Duration(t.seconds) * time.Second }

// runTimeoutProto is the run's Run.timeout: unset when its definition
// declares none, and otherwise the limit with whatever of the deadline, the
// hard end and the ending is known.
func (s *runState) runTimeoutProto() *orkstackv1.RunTimeout {
	t := s.runTimeout
	if t == nil {
		return nil
	}
	out := &orkstackv1.RunTimeout{
		Seconds:        t.seconds,
		TimedOut:       s.isTimedOutRun(),
		HardEndReached: t.hardEnded,
	}
	if !t.deadline.IsZero() {
		out.Deadline = timestamppb.New(t.deadline)
	}
	if !t.hardEnd.IsZero() {
		out.HardEnd = timestamppb.New(t.hardEnd)
	}
	return out
}

// adoptRunTimeout takes the deadline a saved state carries. A switch happens
// only while a run is running and before D, so a saved state of a definition
// declaring `timeout:` always holds one; one without it is corrupt, never a
// fresh clock. It returns what is wrong, or "".
func (s *runState) adoptRunTimeout(run *orkstackv1.Run) string {
	t := s.runTimeout
	if t == nil {
		return ""
	}
	deadline := run.GetTimeout().GetDeadline()
	if deadline == nil {
		return "it has no run deadline"
	}
	if err := deadline.CheckValid(); err != nil {
		return fmt.Sprintf("its run deadline is invalid: %v", err)
	}
	if got := run.GetTimeout().GetSeconds(); got != t.seconds {
		return fmt.Sprintf("its run timeout is %ds, the run declares %ds", got, t.seconds)
	}
	// A deadline is never before the run's start (a resumed run's fresh clock
	// is later still), so one that is was not written by this engine.
	at := deadline.AsTime()
	if started := s.startedAt; started != nil && at.Before(started.AsTime()) {
		return fmt.Sprintf("its run deadline %s is before the run started", formatInstant(at))
	}
	t.deadline = at
	return ""
}

// clearRunClock stops the run timeout's clock as a resume sets the ending
// aside (E107 §7.1): the main loop's entry starts it again, in full.
func (s *runState) clearRunClock() {
	if t := s.runTimeout; t != nil {
		t.deadline, t.taken, t.timer = time.Time{}, false, nil
	}
}

// runLevelError is the error of a run-level ending: one no task owns.
func runLevelError(code, message string) error {
	return temporal.NewNonRetryableApplicationError(message, code, nil)
}

// isRunTimeoutCode reports a run-level ending of the run timeout's family:
// the deadline, and the two invariants that end a run explicitly.
func isRunTimeoutCode(code string) bool {
	switch code {
	case RunTimedOutCode, RunDefinitionUnsupportedCode, RunTimeoutStateInvalidCode, RunPauseTimeoutCode:
		return true
	}
	return false
}

// runTimeoutCauseCode is the code of a run-level cause of this family, or "".
// A task's or a child's error that happens to carry the type is not one: as
// isHistoryLimit, only an error raised by this workflow's own code is.
func runTimeoutCauseCode(cause error) string {
	failure := categoryOf(cause)
	if !failure.isApplication() || !isRunTimeoutCode(failure.appType()) {
		return ""
	}
	if failure.viaActivity || failure.viaChild {
		return ""
	}
	return failure.appType()
}

// isRunLevelCause reports a cause that belongs to the run, never to the task
// or fan-out where it was noticed.
func isRunLevelCause(cause error) bool {
	return isHistoryLimit(cause) || runTimeoutCauseCode(cause) != "" || concCauseCode(cause) != ""
}

// runTimeoutFailure is the run's error for a cause of this family.
func (s *runState) runTimeoutFailure(cause error) *orkstackv1.Error {
	read := categoryOf(cause)
	failure := &orkstackv1.Error{
		Code:    read.appType(),
		Message: read.appMessage(),
	}
	switch failure.Code {
	case RunPauseTimeoutCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	case RunTimedOutCode:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
		if t := s.runTimeout; t != nil {
			failure.Details = fmt.Sprintf("run timeout %s; deadline %s", t.limit(), formatInstant(t.deadline))
		}
	default:
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	}
	return failure
}

func formatInstant(at time.Time) string { return at.UTC().Format(time.RFC3339) }

// armRunTimer creates D's timer once per generation, at the main loop's
// entry. A past D gives a ready timer and no command; the first pass takes it.
func (s *runState) armRunTimer(ctx workflow.Context) error {
	t := s.runTimeout
	if t == nil || t.taken {
		return nil
	}
	if t.deadline.IsZero() {
		// Only a resumed run arrives here without a deadline, and gets a fresh
		// clock: its read-back and its wait for a slot are not counted. Any
		// other is an impossible state, made explicit, never a silent one.
		if !s.resumed {
			return runLevelError(RunTimeoutStateInvalidCode,
				"this run declares a run timeout but its clock was never started")
		}
		t.deadline = workflow.Now(ctx).Add(t.limit())
	}
	d := t.deadline.Sub(workflow.Now(ctx))
	if d > 0 {
		if limit := s.historyLimit(); limit != nil {
			s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
		}
	} else {
		d = 0
	}
	// On ctx, never runCtx: fail-fast does not cancel it, and an operator's
	// cancel settles it.
	t.timer = workflow.NewTimer(ctx, d)
	return nil
}

// runTimeoutDue reports that D has passed and must be taken now: its timer
// fired cleanly, or the recorded clock is past D (the timer only wakes an
// idle run). A run whose work is all done in the same workflow task succeeds.
// canceled reports that the timer was settled by the operator's cancel.
func (s *runState) runTimeoutDue(ctx workflow.Context, inFlight map[string]workflow.Future) (due, canceled bool) {
	t := s.runTimeout
	if t == nil || t.taken || t.timer == nil {
		return false, false
	}
	if t.timer.IsReady() {
		if err := t.timer.Get(ctx, nil); err != nil || ctx.Err() != nil {
			return false, true
		}
	} else if workflow.Now(ctx).Before(t.deadline) {
		return false, false
	}
	if s.idleAndDone(inFlight) {
		return false, false
	}
	return true, false
}

// idleAndDone is the main loop's break test: nothing in flight, opening,
// open, closing, ready or held. A run that is idle and done succeeds.
func (s *runState) idleAndDone(inFlight map[string]workflow.Future) bool {
	return len(inFlight) == 0 && s.openingPins() == 0 && s.openAwaits() == 0 && s.openSleeps() == 0 &&
		len(s.waits) == 0 && !(s.quiescing && s.closingPins() > 0) && len(s.readyTasks()) == 0 && !s.heldFanOuts
}

// takeRunTimeout marks D taken and returns the cause the run fails with. The
// message is provisional; the fail path rebuilds it from the final rows.
func (s *runState) takeRunTimeout() error {
	t := s.runTimeout
	t.taken = true
	return runLevelError(RunTimedOutCode, fmt.Sprintf("run timed out at %s; stopping running work",
		formatInstant(t.deadline)))
}

// isTimedOutRun reports that this run is failing because its deadline passed.
func (s *runState) isTimedOutRun() bool {
	return s.runTimeout != nil && s.runError.GetCode() == RunTimedOutCode
}

// interruptedReason is the row message of work stopped at D.
func (s *runState) interruptedReason() string {
	return "stopped when the run timed out at " + formatInstant(s.runTimeout.deadline)
}

// notStartedReason is the row message of work D ended before it started.
const notStartedReason = "run timed out before this task started"

// recordInterrupted records work running when D passed: FAILED,
// TASK_STOPPED_BY_RUN_TIMEOUT, category TIMED_OUT. A result that committed
// first keeps its own row; this is reached only for a cancellation.
func (s *runState) recordInterrupted(ctx workflow.Context, taskRun *orkstackv1.TaskRun, label string) {
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	if taskRun.GetFinishedAt() == nil {
		taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	}
	taskRun.Error = &orkstackv1.Error{
		Code:     TaskStoppedByRunTimeoutCode,
		Message:  s.interruptedReason(),
		Details:  label,
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
	}
}

// interrupting reports that the current drain records interrupted work as
// stopped by the run timeout.
func (s *runState) interrupting() bool {
	return s.runTimeout != nil && s.runTimeout.timeoutDrain
}

// finalRunTimeoutMessage is the run's message once the drain is over, built
// from the final rows.
func (s *runState) finalRunTimeoutMessage() string {
	t := s.runTimeout
	var stopped []string
	notStarted := 0
	composed := false
	for _, id := range s.order {
		row := s.taskRuns[id]
		if row.GetError().GetCode() == TaskStoppedByRunTimeoutCode {
			stopped = append(stopped, id)
			if s.children[id] != nil {
				composed = true
			}
		}
		if row.GetStatus() == orkstackv1.Status_STATUS_CANCELED && row.GetStartedAt() == nil &&
			!s.isFailureHandlerID(id) {
			notStarted++
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "run did not finish within its timeout of %s (deadline %s)", t.limit(), formatInstant(t.deadline))
	if len(stopped) > 0 {
		b.WriteString("; stopped: " + strings.Join(stopped, ", "))
	}
	if notStarted > 0 {
		fmt.Fprintf(&b, "; not started: %d %s", notStarted, plural(notStarted, "task", "tasks"))
	}
	if composed {
		b.WriteString("; start it again with orkctl run again, or raise timeout: in the Orkflow")
	} else {
		b.WriteString("; resume it with orkctl run resume if it can be resumed; to give it more time, raise timeout: and use orkctl run again")
	}
	return boundEscaped(b.String(), needErrorMessageBytes)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// failTimedOut is fail() for a run without failure handlers whose deadline
// passed: fail-fast, with work running recorded as interrupted and work never
// started CANCELED; then the message rebuilt from the final rows. Every emit
// uses the lazy event context, so an operator's cancel during the drain still
// yields the terminal event and the archive, and the run still ends
// RUN_TIMED_OUT: the cause was decided first.
func (s *runState) failTimedOut(
	ctx workflow.Context,
	cancelRun workflow.CancelFunc,
	inFlight map[string]workflow.Future,
	cause error,
) (*orkstackv1.Run, error) {
	s.runError = s.runFailureFor("", cause)
	s.status = orkstackv1.Status_STATUS_FAILED
	if v := s.version(ctx, cancelSiblingsChange, 1); v >= 1 {
		cancelRun()
		s.runTimeout.timeoutDrain = true
		s.cancelInFlight(s.eventContext(ctx), inFlight, nil, func(string) string { return notStartedReason })
		s.runTimeout.timeoutDrain = false
	}
	ectx := s.eventContext(ctx)
	s.awaitPinsClosed(ectx)
	s.drainResolutions(ectx)
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError.Message = s.finalRunTimeoutMessage()
	if err := s.emit(ectx, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "", s.status,
		s.runError.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("emit terminal run event failed",
			"run_id", s.runID, "event", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED.String(), "error", err)
	}
	if s.cancelDetached != nil {
		s.cancelDetached()
	}
	// A fresh error carrying the final message, so the closing event and the
	// terminal snapshot agree.
	return s.snapshot(), runLevelError(RunTimedOutCode, s.runError.GetMessage())
}

// interruptOpen records an open await or sleep the deadline stopped, with
// its TASK_FAILED event. ok reports that it did.
func (s *runState) interruptOpen(ctx workflow.Context, id string) (bool, error) {
	if !s.interrupting() {
		return false, nil
	}
	taskRun := s.taskRuns[id]
	s.recordInterrupted(ctx, taskRun, fmt.Sprintf("task %q", id))
	return true, s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, taskRun.GetError().GetMessage())
}

// runCauseText names a run-level cause other than the history guard's, for
// the failing note and a never-started task's row; "" for the guard's, whose
// texts stay as they were.
func (s *runState) runCauseText() string {
	switch s.runError.GetCode() {
	case RunTimedOutCode:
		if s.runTimeout == nil {
			return ""
		}
		return fmt.Sprintf("it reached its timeout of %s at %s", s.runTimeout.limit(), formatInstant(s.runTimeout.deadline))
	case RunDefinitionUnsupportedCode:
		return "its definition declares a run timeout its plan does not carry"
	case RunTimeoutStateInvalidCode:
		return "its run timeout's clock was never started"
	case RunPauseTimeoutCode:
		return "its pause was not lifted before the pause timed out"
	}
	return ""
}

// startHardEnd sets H when a run with a timeout starts failing: D + grace,
// or now + grace when the clock has not started. A hard end already passed
// is taken at once, so the failing phase dispatches nothing; otherwise one
// timer, on ctx, so an operator's cancel settles it.
func (s *runState) startHardEnd(ctx workflow.Context) {
	t := s.runTimeout
	if t == nil {
		return
	}
	now := workflow.Now(ctx)
	base := t.deadline
	if base.IsZero() {
		base = now
	}
	t.hardEnd = base.Add(time.Duration(t.graceSeconds) * time.Second)
	if !now.Before(t.hardEnd) {
		s.takeHardEnd()
		return
	}
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
	}
	t.hardTimer = workflow.NewTimer(ctx, t.hardEnd.Sub(now))
}

// takeHardEnd stops the failure handlers: running ones are cancelled, and
// none starts after it. Idempotent.
func (s *runState) takeHardEnd() {
	t := s.runTimeout
	if t.hardEnded {
		return
	}
	t.hardEnded = true
	if s.cancelHandlers != nil {
		s.cancelHandlers()
	}
}

// hardEnded reports that H has stopped the failure handlers.
func (s *runState) hardEnded() bool { return s.runTimeout != nil && s.runTimeout.hardEnded }

// hardEndStoppedReason is a handler's row when H stopped it running.
func (s *runState) hardEndStoppedReason() string {
	return "stopped at the run timeout's hard end (" + formatInstant(s.runTimeout.hardEnd) + ")"
}

// hardEndNotStartedReason is a handler's row when H passed before it started.
func (s *runState) hardEndNotStartedReason() string {
	return "the run timeout's hard end (" + formatInstant(s.runTimeout.hardEnd) + ") passed before this handler started"
}

// handlersStoppedReason is a running handler's row when the run stopped its
// failure handlers itself: when a failing run's pause expired, or at the run
// timeout's hard end. The first to stop them names it: a pause expiry is
// never taken after the hard end (pauseHandlerPass).
func (s *runState) handlersStoppedReason() (string, bool) {
	switch {
	case s.pauseStop != nil:
		return s.pauseStopStoppedReason(), true
	case s.hardEnded():
		return s.hardEndStoppedReason(), true
	}
	return "", false
}

// hardEndNote is what the run's message gains when H stopped its handlers:
// each handler's final state.
func (s *runState) hardEndNote() string {
	return fmt.Sprintf("; its timeout's hard end passed at %s: %s",
		formatInstant(s.runTimeout.hardEnd), s.handlerStates())
}

// handlerStates is each failure handler's final state, in declaration order.
func (s *runState) handlerStates() string {
	var parts []string
	for _, id := range s.order {
		if !s.isFailureHandlerID(id) {
			continue
		}
		row := s.taskRuns[id]
		switch {
		case row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED:
			parts = append(parts, id+" finished")
		case row.GetStatus() == orkstackv1.Status_STATUS_CANCELED && row.GetScheduledAt() == nil && row.GetStartedAt() == nil:
			parts = append(parts, id+" did not start")
		case row.GetStatus() == orkstackv1.Status_STATUS_CANCELED:
			parts = append(parts, id+" was stopped")
		default:
			parts = append(parts, id+" "+statusWord(row.GetStatus()))
		}
	}
	return strings.Join(parts, ", ")
}

// runTimeoutReserveEvents is what a run timeout can still add to a run's
// history: D's timer started and fired (or cancelled), and in the failing
// phase H's timer started and fired (or cancelled), with a workflow task to
// take each firing.
const runTimeoutReserveEvents = 6

// fixedReserve is the reserve's fixed part, with the run timeout's events
// for a run that declares one, and the write of a child run's outputs' record;
// unchanged for every other run.
func (s *runState) fixedReserve(r HistoryReserve) int64 {
	fixed := r.Fixed
	if s.isChildRun() {
		fixed += s.readEvents()
	}
	if s.runTimeout != nil {
		return fixed + runTimeoutReserveEvents
	}
	return fixed
}

// resolutionsPerPass is the plan's bound on resolutions one drain applies.
// It is pinned at submit: a plan whose RunTimeout lacks it never starts
// (planCarriesRunTimeout), so no code constant decides it here.
func (s *runState) resolutionsPerPass() int {
	return int(s.plan.RunTimeout.ResolutionsPerPass)
}

// planCarriesRunTimeout reports that the plan carries every run-timeout
// number a run with a timeout decides by.
func planCarriesRunTimeout(plan RunPlan) bool {
	return plan.RunTimeout != nil && plan.RunTimeout.ResolutionsPerPass > 0 && plan.RunTimeout.HandlerGraceSeconds >= 0
}

// drainQueuedResolutions applies the resolutions already queued when it
// starts, for a run with a timeout: a bounded snapshot (the queue's length
// at the start, at most resolutionsPerPass), never a loop until empty, and
// it stops once D is due, so a pass never applies past its deadline. changed
// reports that it applied any, or that some are left queued; a resolve error
// is the task's.
func (s *runState) drainQueuedResolutions(ctx workflow.Context, inFlight map[string]workflow.Future) (changed bool, taskID string, err error) {
	if s.resolutions == nil {
		return false, "", nil
	}
	if s.recorded() {
		taskID, err = s.drainRecordedAtPass(ctx, inFlight)
		return false, taskID, err
	}
	take := min(s.resolutions.Len(), s.resolutionsPerPass())
	for i := 0; i < take; i++ {
		if t := s.runTimeout; !t.deadline.IsZero() && !workflow.Now(ctx).Before(t.deadline) {
			break
		}
		var raw converter.RawValue
		if !s.resolutions.ReceiveAsync(&raw) {
			break
		}
		changed = true
		sig, ok := s.openResolution(ctx, raw)
		if !ok {
			continue
		}
		if err := s.resolve(ctx, sig); err != nil {
			return true, sig.GetTaskId(), err
		}
	}
	return changed || s.resolutions.Len() > 0, "", nil
}

// switchResolutions is the switch's resolution step: today's drain until
// empty for a run with no timeout, and for one with a timeout the bounded
// drain, with a ready deadline abandoning the switch (the pass top takes
// it). changed means the switch must not proceed in this attempt.
func (s *runState) switchResolutions(ctx workflow.Context, inFlight map[string]workflow.Future) (changed bool, taskID string, err error) {
	if s.recorded() {
		if due, canceled := s.runTimeoutDue(ctx, inFlight); due || canceled {
			return true, "", nil
		}
		changed, taskID, err = s.switchRecorded(ctx, inFlight)
		if due, canceled := s.runTimeoutDue(ctx, inFlight); due || canceled {
			changed = true
		}
		return changed, taskID, err
	}
	if s.runTimeout == nil {
		return s.applyQueuedResolutions(ctx)
	}
	if due, canceled := s.runTimeoutDue(ctx, inFlight); due || canceled {
		return true, "", nil
	}
	changed, taskID, err = s.drainQueuedResolutions(ctx, inFlight)
	if err != nil || changed {
		return true, taskID, err
	}
	if due, canceled := s.runTimeoutDue(ctx, inFlight); due || canceled {
		return true, "", nil
	}
	return false, "", nil
}
