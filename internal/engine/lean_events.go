package engine

import (
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// leanEventPathChange is the workflow.GetVersion change id of the lean event
// path. A run that recorded its marker (version 1) publishes events in two
// cheaper ways than a run that did not:
//
//   - the TASK_STARTED a plain task's or a fan-out's schedule emits directly
//     after its TASK_SCHEDULED, with no state change between the two, reuses
//     the snapshot URI just written for TASK_SCHEDULED instead of writing a
//     byte-identical object of its own (one local activity and one PUT fewer
//     per task); and
//   - a fan-out item event without a snapshot of its own is published without
//     the main loop waiting for it, at most itemPublishWindow at a time, and
//     every one of them is awaited before the run's terminal event.
//
// A history recorded before this change carries no marker, so a replay of it
// takes DefaultVersion: the old command stream, every event awaited and every
// event with its own snapshot (AGENTS.md §2.1, §7.2). The precedent is
// "cancel-siblings-on-run-end".
const leanEventPathChange = "lean-event-path"

// itemPublishWindow bounds the item publishes a run leaves outstanding. When
// it is full, the oldest is awaited before another is scheduled, which keeps a
// run far below Temporal's pending-activity limit (2,000 by default) even
// beside a fan-out's own in-flight items.
const itemPublishWindow = 32

// leanEventPathForTest is a TEST-ONLY switch, the precedent of
// eachDispatchDescending. leanEventPathForcedForTest takes version 1 WITHOUT
// asking workflow.GetVersion, which is how a negative replay test proves the
// committed v1 fixtures can only replay because of the gate.
// leanEventPathWithoutReuseForTest asks GetVersion but never reuses a
// snapshot URI, which is how a negative replay test proves a lean-path
// fixture sees the reuse. Never set in production code.
var leanEventPathForTest = leanEventPathAsk

const (
	leanEventPathAsk = iota
	leanEventPathForcedForTest
	leanEventPathWithoutReuseForTest
)

// leanEvents reports whether this run takes the lean event path. The version
// is asked once, at the first event that needs the answer, and kept: the SDK
// would return the same answer to a second call, and asking once keeps the
// marker's position obvious.
func (s *runState) leanEvents(ctx workflow.Context) bool {
	if s.leanDecided {
		return s.lean
	}
	s.leanDecided = true
	if leanEventPathForTest == leanEventPathForcedForTest {
		s.lean = true
		if s.versions == nil {
			s.versions = map[string]workflow.Version{}
		}
		s.versions[leanEventPathChange] = 1
		return s.lean
	}
	// Through the run's own memory of its decisions, so a carried generation
	// never asks again (continuation.go).
	s.lean = s.version(ctx, leanEventPathChange, 1) >= 1
	return s.lean
}

// startedSnapshotURI is the snapshot URI a TASK_STARTED emitted directly after
// its TASK_SCHEDULED publishes with: the one TASK_SCHEDULED just wrote, on the
// lean path, or empty — write a fresh one — on the old one. The two events
// carry byte-identical snapshots by construction (nothing in the run's state
// changes between them), so the reuse loses nothing a reader or a webhook
// body could see. The composed pair (compose.go) never comes here: its child
// run id and start time change between its two events.
func (s *runState) startedSnapshotURI(ctx workflow.Context, scheduledURI string) string {
	if !s.leanEvents(ctx) || leanEventPathForTest == leanEventPathWithoutReuseForTest {
		return ""
	}
	return scheduledURI
}

// publishItemDetached schedules one item event's PublishEvent without waiting
// for it, and keeps the future so drainItemPublishes can. It is scheduled on
// itemPublishContext, so an operator's cancel — which cancels the run's own
// context — never cancels a publish already on its way: the event stays in
// history, on the bus and in the archive like every other.
func (s *runState) publishItemDetached(ctx workflow.Context, event *orkstackv1.Event, snapshotURI, dedupeKey string) {
	s.publishDetached(ctx, itemPublishWindow, event, snapshotURI, dedupeKey)
}

// detachedPublish is one publish scheduled without the loop waiting for it:
// its future, and the event it publishes, so a failure names that event as
// an awaited publish's does (logPublishFailed).
type detachedPublish struct {
	future workflow.Future
	event  *orkstackv1.Event
}

// publishDetached is publishItemDetached with its window: itemPublishWindow
// for an item event, the plan's InFlight.PublishWindow for a plain task's
// (task_publish.go). Both share one outstanding set, oldest first.
//
// A publish that takes the place of the one let go leaves the outstanding
// set, and so the reserve failReserve counts for it, unchanged. On the
// detached publish path such a publish charges nothing; before it every
// publish charged its in-flight term, which is the formula a recorded
// history's guard crossings were decided by, so it stays for those runs.
func (s *runState) publishDetached(ctx workflow.Context, window int, event *orkstackv1.Event, snapshotURI, dedupeKey string) {
	replaced := false
	if len(s.pendingItemPublishes) >= window {
		oldest := s.pendingItemPublishes[0]
		s.pendingItemPublishes = s.pendingItemPublishes[1:]
		s.awaitItemPublish(ctx, oldest)
		replaced = true
	}
	activityCtx := workflow.WithActivityOptions(s.itemPublishContext(ctx), eventActivityOptions())
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, limit.Reserve.PublishBytes)
		if !replaced || !s.taskPublishesDetached() {
			s.chargeDetachedPublish()
		}
	}
	s.pendingItemPublishes = append(s.pendingItemPublishes, detachedPublish{
		future: workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI, dedupeKey),
		event:  event,
	})
}

// itemPublishContext is the disconnected context every item publish of a
// lean-path run is scheduled on, created once. Creating it issues no command.
func (s *runState) itemPublishContext(ctx workflow.Context) workflow.Context {
	if s.publishCtx == nil {
		s.publishCtx, _ = workflow.NewDisconnectedContext(ctx)
	}
	return s.publishCtx
}

// drainItemPublishes waits for every item publish still outstanding, oldest
// first. It runs before the run's terminal event, so a projection without a
// history reader — whose archive body is its own bus log — has every item
// event before it archives, and no publish is left running past the run's
// end. It waits on the event context, so a canceled run still drains.
func (s *runState) drainItemPublishes(ctx workflow.Context) {
	pending := s.pendingItemPublishes
	s.pendingItemPublishes = nil
	for _, publish := range pending {
		s.awaitItemPublish(ctx, publish)
	}
}

func (s *runState) awaitItemPublish(ctx workflow.Context, publish detachedPublish) {
	if err := publish.future.Get(s.eventContext(ctx), nil); err != nil {
		// Projection is observability, not orchestration, exactly as for an
		// awaited publish: logged, and the run continues.
		s.logPublishFailed(ctx, publish.event, err)
	}
}

// logPublishFailed logs a publish whose every attempt failed, naming its
// event: an item's by its task and item, any other by its type. The awaited
// and the detached path log the same line for the same event, so which path
// published it never changes what an operator reads. A log line is no
// command, so this replays as it ran.
func (s *runState) logPublishFailed(ctx workflow.Context, event *orkstackv1.Event, err error) {
	if event.Item != nil {
		workflow.GetLogger(ctx).Error("publish item event failed",
			"run_id", s.runID, "task_id", event.GetTaskId(), "item", event.GetItem(),
			"sequence", event.GetSequence(), "error", err)
		return
	}
	workflow.GetLogger(ctx).Error("publish event failed",
		"run_id", s.runID, "namespace", s.wf.GetNamespace(), "sequence", event.GetSequence(),
		"event", event.GetType().String(), "error", err)
}
