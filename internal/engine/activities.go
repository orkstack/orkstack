package engine

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RunNotifier is the seam between a run's subscribable events and whoever is
// listening outside the process: the run's own state transitions, and its
// task outcomes (IsSubscribableEvent). The engine owns *when* an event
// happened; it deliberately does not know that the listener is a webhook, what
// a delivery looks like, or how a Run is spelled on the wire.
//
// Implementations must not block: this is called from the projection activity,
// which a run's progress waits on. Task outcome events arrive too, so an
// implementation sees up to four more calls per task than per run.
type RunNotifier interface {
	NotifyRunEvent(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run)
}

// EventObserver sees every lifecycle event the FIRST time this replica
// publishes it, with the run snapshot it belongs to. It is the seam telemetry
// hangs on: counters and durations, never orchestration. It must not block.
//
// Counts are at-least-once across replicas: firstPublication is decided by
// this process's own EventBus (AGENTS.md §7), so an activity retry that lands
// on another replica announces the event again. Durations are taken from the
// snapshot's own timestamps, so their VALUE is the same wherever they are
// observed even when their COUNT is not.
type EventObserver interface {
	ObserveEvent(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run)
}

// RunEventSource reads a run's whole event stream from the system of record.
// It is the seam that keeps an archive write complete under more than one
// engine replica; engine.TemporalRunReader is the production implementation.
type RunEventSource interface {
	Events(ctx context.Context, runID string, fromSequence int64) ([]*orkstackv1.Event, error)
	// EventsForArchive is Events for the archive write: a segment that is
	// permanently gone (missing, or failing verification) becomes an
	// EventGap instead of an error; a transient failure is still an error,
	// so the activity retries exactly as today.
	EventsForArchive(ctx context.Context, runID string) ([]*orkstackv1.Event, []EventGap, error)
}

// EventActivities projects workflow-produced state into the in-process read
// model and, when a run ends, into the durable archive. RunWorkflow must stay
// deterministic, so every mutation of the store, every event publication and
// every archive write happens here, inside an activity.
type EventActivities struct {
	bus       *EventBus
	store     *RunStore
	archive   *RunArchiver
	notifier  RunNotifier
	observer  EventObserver
	events    RunEventSource
	snapshots SnapshotStore
	// recent holds the last few snapshots this process read, by URI. A
	// snapshot object is immutable, and on the lean event path a TASK_STARTED
	// publishes with the URI its TASK_SCHEDULED just did, so the second
	// publish on this replica costs no read.
	recent snapshotCache
	// bases is the process's base cache (WithBaseCache): the full snapshots
	// the overlays it resolves are written against. Nil caches none.
	bases *SnapshotBaseCache
}

// snapshotCacheSize bounds snapshotCache: two publishes of one URI are
// adjacent in a run's stream, so a handful of entries per concurrently
// projecting run is all the reuse needs.
const snapshotCacheSize = 32

// snapshotCache is a bounded URI → decoded Run cache, oldest entry evicted
// first. It is a disposable process-local cache (AGENTS.md §13): a miss reads
// the object store exactly as before, and nothing reads the cache but
// readSnapshot. Entries are cloned in and out, so no caller shares a message.
type snapshotCache struct {
	mu    sync.Mutex
	runs  map[string]*orkstackv1.Run
	order []string
}

func (c *snapshotCache) get(uri string) (*orkstackv1.Run, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	run, ok := c.runs[uri]
	if !ok {
		return nil, false
	}
	return proto.Clone(run).(*orkstackv1.Run), true
}

func (c *snapshotCache) put(uri string, run *orkstackv1.Run) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runs == nil {
		c.runs = make(map[string]*orkstackv1.Run, snapshotCacheSize)
	}
	if _, ok := c.runs[uri]; ok {
		return
	}
	if len(c.order) >= snapshotCacheSize {
		delete(c.runs, c.order[0])
		c.order = c.order[1:]
	}
	c.runs[uri] = proto.Clone(run).(*orkstackv1.Run)
	c.order = append(c.order, uri)
}

// NewEventActivities wires the projection activity to a bus and run store.
// Runs are then held in process only: nothing survives a restart.
func NewEventActivities(bus *EventBus, store *RunStore) *EventActivities {
	return &EventActivities{bus: bus, store: store}
}

// NewArchivingEventActivities also writes each finished run to the archive, so
// run history outlives the engine process.
func NewArchivingEventActivities(bus *EventBus, store *RunStore, archive *RunArchiver) *EventActivities {
	return &EventActivities{bus: bus, store: store, archive: archive}
}

// WithRunNotifier announces subscribable events — run state transitions and
// task outcomes — to notifier as well, and returns the same activities so it
// can be chained onto a constructor.
func (a *EventActivities) WithRunNotifier(notifier RunNotifier) *EventActivities {
	a.notifier = notifier
	return a
}

// WithEventObserver hands every published event, of any type, to o as well.
// Unlike WithRunNotifier this is not limited to subscribable events: a
// telemetry observer wants every event, TASK_LOG included. It returns the
// same activities so it can be chained onto a constructor.
func (a *EventActivities) WithEventObserver(o EventObserver) *EventActivities {
	a.observer = o
	return a
}

// WithEventSource makes the archive body come from the system of record rather
// than from this process's own bus.
//
// It has to. Projection activities are dispatched to whichever replica polls
// ork-engine-events first, so with more than one engine each process executes
// an arbitrary subset of a run's events and its bus log holds only that subset.
// The replica that happens to execute the terminal event would then archive a
// stream with gaps — a corruption no test of a single process can see.
func (a *EventActivities) WithEventSource(events RunEventSource) *EventActivities {
	a.events = events
	return a
}

// WithSnapshotStore resolves the immutable snapshot references carried by
// PublishEvent. The same object store is shared by every engine replica.
func (a *EventActivities) WithSnapshotStore(store SnapshotStore) *EventActivities {
	a.snapshots = store
	return a
}

// WithBaseCache resolves snapshot overlays through cache, the process's one
// base cache, shared with the activity that writes them.
func (a *EventActivities) WithBaseCache(cache *SnapshotBaseCache) *EventActivities {
	a.bases = cache
	return a
}

// PublishEvent resolves and records a run snapshot, then publishes one
// lifecycle event. dedupeKey makes the call idempotent across activity retries
// and workflow replays.
// A terminal event also archives the run: this is the write-through seam, and
// it is the last thing the activity does, so the archived body already contains
// the terminal event itself. The write is idempotent (the archive body is
// immutable), which is what lets this activity be retried at all.
//
// A subscribable event (IsSubscribableEvent: a run state transition or a task
// outcome) is announced to the RunNotifier, and every event —
// including task events and TASK_LOG — is announced to the EventObserver, but
// only when the bus reports that this publication was the first for its
// dedupe key. An activity retry or a workflow replay therefore re-runs the
// projection without re-announcing anything, which is what keeps an external
// subscriber from seeing the same run finish twice and a telemetry counter
// from double-counting it.
//
// Arguments are passed separately (rather than in one struct) because the
// Temporal data converter encodes top-level protobuf messages with protojson.
//
// The second argument is a snapshot URI, or empty for an event that carries
// none of its own — a fan-out's item event between its stride snapshots.
// Such an event reads no snapshot, applies nothing to the
// RunStore, is published to the bus, is announced to the EventObserver with
// the store's copy of the run (skipped when there is none), notifies nothing
// and archives nothing. The projection worker on
// ork-engine-events is unversioned (AGENTS.md §7.2), so the shape of this
// activity's input is part of the RunWorkflow replay contract.
func (a *EventActivities) PublishEvent(
	ctx context.Context,
	event *orkstackv1.Event,
	snapshotURI string,
	dedupeKey string,
) error {
	var run *orkstackv1.Run
	if snapshotURI != "" {
		read, err := a.readSnapshot(ctx, snapshotURI)
		if err != nil {
			return err
		}
		run = read
		a.store.Apply(run)
	}
	firstPublication := false
	if event != nil {
		_, firstPublication = a.bus.Publish(event, dedupeKey)
	}

	// Computed once and shared by every branch below: all three want the
	// same run, resolved the same way (mergedSnapshot prefers the store's
	// own copy, which still carries CreatedAt and Input), so there is
	// nothing for a second or third call to recompute.
	snapshot := a.mergedSnapshot(event.GetRunId(), run)

	if a.notifier != nil && firstPublication && run != nil && IsSubscribableEvent(event) && snapshot != nil {
		a.notifier.NotifyRunEvent(ctx, event, snapshot)
	}

	if a.observer != nil && firstPublication && snapshot != nil {
		a.observer.ObserveEvent(ctx, event, snapshot)
	}

	if a.archive == nil || !isTerminalRunEvent(event) {
		return nil
	}
	if snapshot == nil {
		return nil
	}
	archived, gaps, err := a.archivedEvents(ctx, event.GetRunId())
	if err != nil {
		return err
	}
	// Returning the error makes Temporal retry the activity, so a transient
	// storage failure heals itself instead of silently dropping the run. A
	// permanent loss cannot heal, so it is archived as an explicit gap: a run
	// whose earlier events are gone still reaches the archive (AGENTS.md
	// §15), and every read of the lost range says so.
	return a.archive.Archive(ctx, snapshot, archived, gaps...)
}

func (a *EventActivities) readSnapshot(ctx context.Context, uri string) (*orkstackv1.Run, error) {
	if a.snapshots == nil {
		return nil, fmt.Errorf("engine: publish event: snapshot store is not configured")
	}
	if run, ok := a.recent.get(uri); ok {
		return run, nil
	}
	// A {seq}.ovl is assembled with the full snapshot it names
	// (materializeSnapshot): at most two reads, none on a cache hit.
	run, err := materializeSnapshot(ctx, a.snapshots, a.bases, uri)
	if err != nil {
		return nil, fmt.Errorf("engine: publish event: read snapshot: %w", err)
	}
	a.recent.put(uri, run)
	return run, nil
}

// archivedEvents is the event log that goes into the archive body.
//
// It comes from the system of record when one is wired, and a failure to read
// it is returned rather than papered over with the local log: an archive
// written from a partial view is worse than a retried activity, because the
// gap is permanent and silent. The one exception is a loss retrying cannot
// heal — an earlier generation's event segment gone or altered — which the
// source reports as explicit gaps, recorded in the body rather than hidden.
// Without a source — a single-process engine, or a unit test — the local bus
// saw every event of the run and is exact.
func (a *EventActivities) archivedEvents(
	ctx context.Context,
	runID string,
) ([]*orkstackv1.Event, []EventGap, error) {
	if a.events == nil {
		return a.bus.Events(runID, 0), nil, nil
	}
	events, gaps, err := a.events.EventsForArchive(ctx, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("read events of run %s for the archive: %w", runID, err)
	}
	return events, gaps, nil
}

// mergedSnapshot prefers the store's copy of a run: it still carries the
// CreatedAt and Input that the workflow's own snapshot does not own.
func (a *EventActivities) mergedSnapshot(runID string, fallback *orkstackv1.Run) *orkstackv1.Run {
	if snapshot, ok := a.store.Get(runID); ok {
		return snapshot
	}
	return fallback
}

// IsSubscribableEvent reports whether an event is one an external subscriber
// may ask for: a run transition, or a task outcome (TASK_SCHEDULED,
// TASK_STARTED, TASK_SUCCEEDED, and TASK_FAILED with status FAILED).
//
// Two things the interpreter emits are excluded on purpose. TASK_LOG is never
// one, whatever status it carries: a skipped task and a task that never
// started before its run ended are recorded as log lines (workflow.go
// markSkipped, markCanceled). And a TASK_FAILED whose status is CANCELED is not
// one either: it is how collect records a task that was RUNNING when its run
// ended — an operator cancel, a run timeout, or a sibling canceled because
// another task failed (recordFailure's IsCanceledError branch). That task did
// not fail; delivering it under X-Ork-Event: TASK_FAILED would say it did. The
// run's terminal delivery carries its CANCELED status, which is where a
// subscriber reads it.
//
// The gate lives here, in the projection activity, and nowhere upstream:
// RunWorkflow keeps writing the task's status into history exactly as it
// always has, and only the outbound announcement filters it.
func IsSubscribableEvent(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		orkstackv1.EventType_EVENT_TYPE_TASK_STARTED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		// An await's two events, whatever their status: a TASK_RESOLVED
		// carrying FAILED is a real rejection, not a run-ending cancellation.
		orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING,
		orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED:
		return true
	case orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
		return event.GetStatus() == orkstackv1.Status_STATUS_FAILED
	}
	return IsRunTransitionEvent(event)
}

// IsRunTransitionEvent reports whether an event moves the run itself between
// states — PENDING → RUNNING, then RUNNING → SUCCEEDED, FAILED or CANCELED.
// Task events describe work inside a run and are not transitions of it.
func IsRunTransitionEvent(event *orkstackv1.Event) bool {
	if event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		return true
	}
	return isTerminalRunEvent(event)
}
