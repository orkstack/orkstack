package engine

import (
	"context"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// EventActivities projects workflow-produced state into the in-process read
// model and, when a run ends, into the durable archive. RunWorkflow must stay
// deterministic, so every mutation of the store, every event publication and
// every archive write happens here, inside an activity.
type EventActivities struct {
	bus     *EventBus
	store   *RunStore
	archive *RunArchiver
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

// PublishEvent records a run snapshot and publishes one lifecycle event.
// dedupeKey makes the call idempotent across activity retries and workflow
// replays.
//
// A terminal event also archives the run: this is the write-through seam, and
// it is the last thing the activity does, so the archived body already contains
// the terminal event itself. The write is idempotent (the archive body is
// immutable), which is what lets this activity be retried at all.
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
	if event != nil {
		a.bus.Publish(event, dedupeKey)
	}
	if a.archive == nil || !isTerminalRunEvent(event) {
		return nil
	}

	runID := event.GetRunId()
	// The store's snapshot is the merged one: it still carries the CreatedAt
	// and Input that the workflow's own snapshot does not own.
	snapshot, ok := a.store.Get(runID)
	if !ok {
		snapshot = run
	}
	if snapshot == nil {
		return nil
	}
	// Returning the error makes Temporal retry the activity, so a transient
	// storage failure heals itself instead of silently dropping the run.
	return a.archive.Archive(ctx, snapshot, a.bus.Events(runID, 0))
}
