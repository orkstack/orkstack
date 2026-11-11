package engine

import (
	"sync/atomic"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// TaskPublishChange is the workflow.GetVersion change id of the detached
// publish path for a plain task's dispatch.
//
// Before it, schedule awaited the PublishEvent of a plain task's
// TASK_SCHEDULED and then of its TASK_STARTED, and each await ended the
// workflow task: two round trips through the cluster per task, one task at a
// time. A stage of 300 tasks that start together took about 61 s to
// dispatch (continuation chaos C25: 607 awaited publishes over 609 workflow
// tasks before its fan-out reached the in-flight cap). From version 1 both
// publishes are scheduled without the loop waiting for them, on the
// disconnected context and in the outstanding set a fan-out's item publishes
// already use (lean_events.go), at most the plan's InFlight.PublishWindow at
// a time; and one workflow task schedules at most InFlight.DispatchBytes of
// plain tasks' activity input (counted apart from a fan-out's item batch,
// whose own budget is eachDispatchBytes). Every publish is still awaited
// before the run's terminal event and before a switch, so the stream, the
// bus and the archive hold exactly what they did.
//
// The path is entered only by a plan whose InFlight pins both numbers — no
// plan built before it does — and the version is asked LAZILY, at the first
// plain task such a run dispatches, before that task's TASK_SCHEDULED, so
// its marker precedes the first command that differs. A history recorded
// before the change has neither, and replays the awaited path. A carried
// generation answers from its carry (version).
const TaskPublishChange = "task-publish-window"

// taskPublishGateDisabledForTest is a TEST-ONLY switch, the precedent of
// retryMaxIntervalGateDisabledForTest: when set, a plain task takes the
// awaited path WITHOUT asking the version, so a recorder can write a history
// in the shape of the builds before the gate (which ignore the plan's
// window), and a negative replay test proves the committed fixture can only
// replay because of it. Never set in production code.
var taskPublishGateDisabledForTest atomic.Bool

// detachTaskPublishes reports whether this plain-task dispatch publishes its
// two events detached. Only a plan that pins the window and the byte budget,
// on the lean event path (whose TASK_STARTED reuses its TASK_SCHEDULED's
// snapshot), ever asks.
func (s *runState) detachTaskPublishes(ctx workflow.Context) bool {
	lim := s.plan.InFlight
	if lim == nil || lim.PublishWindow <= 0 || lim.DispatchBytes <= 0 {
		return false
	}
	if taskPublishGateDisabledForTest.Load() || !s.leanEvents(ctx) {
		return false
	}
	return s.version(ctx, TaskPublishChange, 1) >= 1
}

// taskPublishesDetached reports whether this run has already taken the
// detached publish path: its version was asked and answered 1. It asks
// nothing.
func (s *runState) taskPublishesDetached() bool {
	v, ok := s.versions[TaskPublishChange]
	return ok && v >= 1
}

// paceTaskDispatch keeps one workflow task's plain-task activity input
// within the plan's InFlight.DispatchBytes. bytes is the dispatch's input.
// A workflow task is told apart by the history length it started at, which
// moves at every workflow task (syncHistory). When the dispatch would take
// this workflow task past its budget, the loop waits for the newest
// outstanding publish: that publish was scheduled in this workflow task —
// a task was dispatched in it, and the window only ever lets go of the
// oldest — so waiting for it ends the workflow task, and its commands are
// sent. Nothing is waited for while the budget holds.
func (s *runState) paceTaskDispatch(ctx workflow.Context, bytes int64) {
	length := workflow.GetInfo(ctx).GetCurrentHistoryLength()
	if length != s.dispatchTaskLength {
		s.dispatchTaskLength, s.dispatchTaskBytes = length, 0
	}
	if s.dispatchTaskBytes > 0 && s.dispatchTaskBytes+bytes > s.plan.InFlight.DispatchBytes &&
		len(s.pendingItemPublishes) > 0 {
		s.awaitItemPublish(ctx, s.pendingItemPublishes[len(s.pendingItemPublishes)-1])
		// The wait ended the workflow task in production; a test
		// environment never moves the length, so the count restarts here.
		s.dispatchTaskLength, s.dispatchTaskBytes = workflow.GetInfo(ctx).GetCurrentHistoryLength(), 0
	}
	s.dispatchTaskBytes += bytes
}

// taskInputBytes is what a plain task's ExecuteTask command carries: the
// deterministic upper bound on its input's payload as Temporal records it
// (task.PayloadSize, the measure CheckInputSize bounds), a pure function of
// the definition and the run's state. An input it cannot measure counts as
// the whole budget, so it is sent in a workflow task of its own; it never
// reaches here, because CheckInputSize refused it first.
func (s *runState) taskInputBytes(input *orkstackv1.ExecuteTaskInput) int64 {
	size, err := task.PayloadSize(input)
	if err != nil {
		return s.plan.InFlight.DispatchBytes
	}
	return size
}

// settleDetachedPublishes lets go of every outstanding publish that has
// already completed, on the detached publish path only, at the top of a
// pass, before anything counts the set. A completed publish's events are in
// history already, so the failure path's reserve (failReserve, quickReserve)
// and the cluster's pending count (pendingActivitiesOver) must not count it
// again. Without this, a chain of plain tasks — two publishes each, none
// evicted until the window fills — would carry up to the window's
// PerInFlight with nothing running. A future is ready only once the workflow
// has processed its completion event, so the answer replays. Get on a ready
// future issues no command; it is called so a failed publish is logged, as
// the window's own eviction does. Before the path, a fan-out's item
// publishes stay in the set until the window evicts them, which is what the
// recorded histories' guard decisions were made with.
func (s *runState) settleDetachedPublishes(ctx workflow.Context) {
	if len(s.pendingItemPublishes) == 0 || !s.taskPublishesDetached() {
		return
	}
	kept := make([]detachedPublish, 0, len(s.pendingItemPublishes))
	for _, publish := range s.pendingItemPublishes {
		if publish.future.IsReady() {
			s.awaitItemPublish(ctx, publish)
			continue
		}
		kept = append(kept, publish)
	}
	s.pendingItemPublishes = kept
}

// publishReserve is what the history guard reserves for one outstanding
// detached publish. On the detached publish path it is the plan's
// InFlight.PublishEvents, the history the publish can still append (its
// start and close, and a workflow task to collect it). Before the path it is
// PerInFlight, an in-flight task's whole failure path, which is what a
// recorded history's guard crossings were decided by.
func (s *runState) publishReserve(r HistoryReserve) int64 {
	if lim := s.plan.InFlight; lim != nil && lim.PublishEvents > 0 && s.taskPublishesDetached() {
		return lim.PublishEvents
	}
	return r.PerInFlight
}
