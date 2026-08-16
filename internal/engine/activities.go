package engine

import (
	"context"

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

// EventActivities projects workflow-produced state into the in-process read
// model and, when a run ends, into the durable archive. RunWorkflow must stay
// deterministic, so every mutation of the store, every event publication and
// every archive write happens here, inside an activity.
type EventActivities struct {
	bus      *EventBus
	store    *RunStore
	archive  *RunArchiver
	notifier RunNotifier
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

// PublishEvent records a run snapshot and publishes one lifecycle event.
// dedupeKey makes the call idempotent across activity retries and workflow
// replays.
//
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
// Temporal data converter encodes top-level protobuf messages with protojson;
// protobuf messages nested inside a plain Go struct would be encoded with
// encoding/json, which cannot round-trip google.protobuf.Struct.
func (a *EventActivities) PublishEvent(
	ctx context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
	dedupeKey string,
) error {
	if run != nil {
		a.store.Apply(run)
	}
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
	// Returning the error makes Temporal retry the activity, so a transient
	// storage failure heals itself instead of silently dropping the run.
	return a.archive.Archive(ctx, snapshot, a.bus.Events(event.GetRunId(), 0))
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
