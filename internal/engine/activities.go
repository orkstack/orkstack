package engine

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RunNotifier is the seam between a run's state transitions and whoever is
// listening outside the process. The engine owns *when* a transition happened;
// it deliberately does not know that the listener is a webhook, what a
// delivery looks like, or how a Run is spelled on the wire.
//
// Implementations must not block: this is called from the projection activity,
// which a run's progress waits on.
type RunNotifier interface {
	NotifyRunTransition(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run)
}

// RunEventSource reads a run's whole event stream from the system of record.
// It is the seam that keeps an archive write complete under more than one
// engine replica; engine.TemporalRunReader is the production implementation.
type RunEventSource interface {
	Events(ctx context.Context, runID string, fromSequence int64) ([]*orkstackv1.Event, error)
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
	events    RunEventSource
	snapshots SnapshotStore
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

// WithRunNotifier announces run state transitions to notifier as well, and
// returns the same activities so it can be chained onto a constructor.
func (a *EventActivities) WithRunNotifier(notifier RunNotifier) *EventActivities {
	a.notifier = notifier
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

// PublishEvent resolves and records a run snapshot, then publishes one
// lifecycle event. dedupeKey makes the call idempotent across activity retries
// and workflow replays.
// A terminal event also archives the run: this is the write-through seam, and
// it is the last thing the activity does, so the archived body already contains
// the terminal event itself. The write is idempotent (the archive body is
// immutable), which is what lets this activity be retried at all.
//
// A run state transition is announced to the RunNotifier, but only when the
// bus reports that this publication was the first for its dedupe key. An
// activity retry or a workflow replay therefore re-runs the projection without
// re-announcing the transition, which is what keeps an external subscriber
// from seeing the same run finish twice.
//
// Arguments are passed separately (rather than in one struct) because the
// Temporal data converter encodes top-level protobuf messages with protojson.
//
// The second argument is always a snapshot URI; the projection worker on
// ork-engine-events is unversioned (AGENTS.md §7.2), so the shape of this
// activity's input is part of the RunWorkflow replay contract.
func (a *EventActivities) PublishEvent(
	ctx context.Context,
	event *orkstackv1.Event,
	snapshotURI string,
	dedupeKey string,
) error {
	run, err := a.readSnapshot(ctx, snapshotURI)
	if err != nil {
		return err
	}
	a.store.Apply(run)
	firstPublication := false
	if event != nil {
		_, firstPublication = a.bus.Publish(event, dedupeKey)
	}

	if a.notifier != nil && firstPublication && IsRunTransitionEvent(event) {
		if snapshot := a.mergedSnapshot(event.GetRunId(), run); snapshot != nil {
			a.notifier.NotifyRunTransition(ctx, event, snapshot)
		}
	}

	if a.archive == nil || !isTerminalRunEvent(event) {
		return nil
	}
	snapshot := a.mergedSnapshot(event.GetRunId(), run)
	if snapshot == nil {
		return nil
	}
	archived, err := a.archivedEvents(ctx, event.GetRunId())
	if err != nil {
		return err
	}
	// Returning the error makes Temporal retry the activity, so a transient
	// storage failure heals itself instead of silently dropping the run.
	return a.archive.Archive(ctx, snapshot, archived)
}

func (a *EventActivities) readSnapshot(ctx context.Context, uri string) (*orkstackv1.Run, error) {
	if a.snapshots == nil {
		return nil, fmt.Errorf("engine: publish event: snapshot store is not configured")
	}
	data, err := a.snapshots.ReadSnapshot(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("engine: publish event: read snapshot: %w", err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		return nil, fmt.Errorf("engine: publish event: decode snapshot %s: %w", uri, err)
	}
	return run, nil
}

// archivedEvents is the event log that goes into the archive body.
//
// It comes from the system of record when one is wired, and a failure to read
// it is returned rather than papered over with the local log: an archive
// written from a partial view is worse than a retried activity, because the
// gap is permanent and silent. Without a source — a single-process engine, or
// a unit test — the local bus saw every event of the run and is exact.
func (a *EventActivities) archivedEvents(
	ctx context.Context,
	runID string,
) ([]*orkstackv1.Event, error) {
	if a.events == nil {
		return a.bus.Events(runID, 0), nil
	}
	events, err := a.events.Events(ctx, runID, 0)
	if err != nil {
		return nil, fmt.Errorf("read events of run %s for the archive: %w", runID, err)
	}
	return events, nil
}

// mergedSnapshot prefers the store's copy of a run: it still carries the
// CreatedAt and Input that the workflow's own snapshot does not own.
func (a *EventActivities) mergedSnapshot(runID string, fallback *orkstackv1.Run) *orkstackv1.Run {
	if snapshot, ok := a.store.Get(runID); ok {
		return snapshot
	}
	return fallback
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
