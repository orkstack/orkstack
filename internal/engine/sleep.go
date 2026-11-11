package engine

import (
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// sleepState is one sleeping task's life in this run. Like awaitState, every
// field is a pure function of the workflow input and recorded history.
//
// A sleep is an await with no door: one timer, started on runCtx, and no
// signal. Its timer firing is the task's success; its timer settling with a
// cancellation is the run ending under it.
type sleepState struct {
	spec *orkstackv1.SleepSpec
	// open is true from the moment the task starts sleeping until its timer
	// settles or the run's end closes it.
	open bool
	// wake settles when the timer fires, or with a cancellation when the run
	// ends first.
	wake       workflow.Future
	cancelWake workflow.CancelFunc
	// wakeAt is the instant the timer fires, on the workflow's clock. It is
	// absolute, so a sleep that crosses a switch fires at its original instant.
	wakeAt   time.Time
	duration time.Duration
}

// openSleeps counts the sleeps that are sleeping now, over s.order and never
// over the map.
func (s *runState) openSleeps() int {
	open := 0
	for _, id := range s.order {
		if sl := s.sleeps[id]; sl != nil && sl.open {
			open++
		}
	}
	return open
}

// openSleep starts one sleeping task: its timer, its snapshot, its
// TASK_SLEEPING event — and no activity. It is called from the main loop, and
// from the failing phase for a sleeping failure handler, for a ready task
// classify decided to run.
//
// The timer is started on runCtx (a handler's, through dispatchContext), so
// fail()'s cancelRun() and an operator's run cancellation both settle it: that
// is the sleep's cancellation path, as it is an await's. It is started BEFORE
// the event is emitted — openAwait's order — so an emit error leaves a sleep
// whose timer still settles it.
func (s *runState) openSleep(ctx, runCtx workflow.Context, id string) error {
	sl := s.sleeps[id]
	now := workflow.Now(ctx)
	duration := time.Duration(sl.spec.GetSeconds()) * time.Second

	sl.open = true
	// An open sleep's failure path is an open await's, which bounds it: a
	// timer cancel and one task event.
	s.chargeAwaitOpen()
	sl.duration = duration
	sl.wakeAt = now.Add(duration)
	startTimer := func() {
		timerCtx, cancel := workflow.WithCancel(runCtx)
		sl.wake = workflow.NewTimer(timerCtx, duration)
		sl.cancelWake = cancel
	}
	if !sleepTimerAfterEmit {
		startTimer()
	}

	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_SLEEPING
	// Both, as for an await: the task is never queued and no Orker reports
	// on it.
	taskRun.ScheduledAt = timestamppb.New(now)
	taskRun.StartedAt = timestamppb.New(now)
	taskRun.Sleep.WakeAt = timestamppb.New(sl.wakeAt)

	err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING, id,
		orkstackv1.Status_STATUS_SLEEPING, sleepingMessage(sl.wakeAt, duration))
	if sleepTimerAfterEmit {
		startTimer()
	}
	return err
}

// sleepTimerAfterEmit reorders openSleep's two commands — the timer AFTER the
// TASK_SLEEPING emit instead of before it. It is false in every build and is
// never set outside one test, TestSleepingHistory_CatchesAMovedTimer, which
// flips it to prove the committed sleeping fixture can see a reordered
// command: awaitDeadlineAfterEmit's precedent.
var sleepTimerAfterEmit = false

// sleepingMessage is TASK_SLEEPING's message: the wake-up time first, as
// TASK_AWAITING names its deadline.
func sleepingMessage(wakeAt time.Time, duration time.Duration) string {
	return "task sleeping until " + wakeAt.UTC().Format(time.RFC3339) + " (" + duration.String() + ")"
}

// wake takes one sleep's settled timer off the Selector.
//
// A timer that fired is the sleep's end: the task SUCCEEDED, with no values
// and no artifacts — nothing is stored and nothing references an object, so
// AGENTS.md §4.1's order has nothing to order — and nil is returned. A timer
// that settled with an error was cancelled because the run is ending (an
// operator's cancel, or cancelRun() in fail()); the error is handed back
// unchanged so the caller records the run CANCELED and the task with it.
func (s *runState) wake(ctx workflow.Context, id string) error {
	sl := s.sleeps[id]
	err := sl.wake.Get(ctx, nil)
	sl.open = false
	if err != nil {
		return err
	}
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task woke after "+sl.duration.String())
}

// closeSleep cancels one open sleep's timer and marks it closed, without
// recording anything: the caller records the task's outcome.
func (s *runState) closeSleep(id string) {
	if sl := s.sleeps[id]; sl != nil && sl.open {
		sl.open = false
		if sl.cancelWake != nil {
			sl.cancelWake()
		}
	}
}

// cancelSleeping closes one sleep the run is ending under: its timer is
// cancelled and the task recorded CANCELED. Reached only for a task whose
// status is SLEEPING, which no history recorded before sleeps existed holds.
func (s *runState) cancelSleeping(ctx workflow.Context, id string) error {
	s.closeSleep(id)
	return s.markCanceled(ctx, id, "run ended while this task was sleeping")
}
