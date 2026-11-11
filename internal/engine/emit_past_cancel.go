package engine

import (
	"sync/atomic"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// EmitPastCancelChange is the workflow.GetVersion change id of an event
// that an operator's cancel can no longer lose.
//
// Before it, an emit wrote its snapshot (a local activity) and published on
// the caller's context. When the run's context was canceled before or during
// the emit — a task's completion and the cancel delivered in one workflow
// task, say — the write or the publish was canceled with it: the emit had
// taken its sequence number already, the number was never published, and
// the archive recorded a hole (continuation chaos C4: the TASK_SUCCEEDED of a
// task that had committed was missing from the stream and the archive). From
// version 1:
//
//   - an event that records what already happened — a task's or an item's
//     outcome, a log line, the run's own events: every event but a dispatch's
//     TASK_SCHEDULED and TASK_STARTED — writes its snapshot on a
//     disconnected copy of its context, which carries the same values and
//     which a cancel does not reach;
//   - a dispatch's announcement is not made once the run is canceled: it
//     takes no number and announces no task the run will not dispatch. One
//     the cancel reaches while its snapshot is being written is stopped as
//     before, and its number stays used, an unpublished gap the archive
//     records (noteSkipped). A number is never given to a second event: the
//     stopped write can still finish its object afterwards, which would
//     overwrite the second event's snapshot under the same key (the reason
//     run-created-first writes its terminal event under a key of its own);
//     and
//   - every publish of an event the run numbered and recorded goes out on a
//     disconnected copy, so a cancel never leaves a recorded number
//     unpublished.
//
// Each step is still bounded by its own timeouts (snapshotLocalActivityOptions,
// eventActivityOptions), so a canceled run never waits on one unboundedly.
//
// The version is asked LAZILY, at the run's first emit, the precedent of
// lean-event-path: a history recorded before the change has no marker and
// replays its old commands; a carried generation answers from its carry.
const EmitPastCancelChange = "emit-past-cancel"

// emitPastCancelGateDisabledForTest is a TEST-ONLY switch, the precedent of
// taskPublishGateDisabledForTest: when set, an emit takes the old path
// WITHOUT asking the version, so a recorder can write a history in the shape
// of the builds before the gate and a negative replay test proves the
// committed fixture replays only because of it. Never set in production code.
var emitPastCancelGateDisabledForTest atomic.Bool

// emitsPastCancel reports whether this run takes the change's path, asking
// the version the first time.
func (s *runState) emitsPastCancel(ctx workflow.Context) bool {
	if emitPastCancelGateDisabledForTest.Load() {
		return false
	}
	return s.version(ctx, EmitPastCancelChange, 1) >= 1
}

// announcesDispatch reports whether a task event announces a dispatch the
// run is about to make, rather than something that already happened.
func announcesDispatch(eventType orkstackv1.EventType) bool {
	return eventType == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED ||
		eventType == orkstackv1.EventType_EVENT_TYPE_TASK_STARTED
}

// emitContexts are the contexts one emit writes its snapshot on and
// publishes on. Before the change both are ctx. On its path the publish is a
// disconnected copy of ctx, and so is the write unless the event announces a
// dispatch. Creating a disconnected context issues no command.
func (s *runState) emitContexts(ctx workflow.Context, announces bool) (write, publish workflow.Context) {
	if !s.emitsPastCancel(ctx) {
		return ctx, ctx
	}
	disconnected, _ := workflow.NewDisconnectedContext(ctx)
	if announces {
		return ctx, disconnected
	}
	return disconnected, disconnected
}

// unannounced reports, for a dispatch's announcement, whether the run is
// canceled already on the change's path, so the announcement is not made.
func (s *runState) unannounced(ctx workflow.Context, announces bool) bool {
	return announces && ctx.Err() != nil && s.emitsPastCancel(ctx)
}
