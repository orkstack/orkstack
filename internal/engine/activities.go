package engine

import (
	"context"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// EventActivities projects workflow-produced state into the in-process read
// model. RunWorkflow must stay deterministic, so every mutation of the store
// and every event publication happens here, inside an activity.
type EventActivities struct {
	bus   *EventBus
	store *RunStore
}

// NewEventActivities wires the projection activity to a bus and run store.
func NewEventActivities(bus *EventBus, store *RunStore) *EventActivities {
	return &EventActivities{bus: bus, store: store}
}

// PublishEvent records a run snapshot and publishes one lifecycle event.
// dedupeKey makes the call idempotent across activity retries and workflow
// replays.
//
// Arguments are passed separately (rather than in one struct) because the
// Temporal data converter encodes top-level protobuf messages with protojson;
// protobuf messages nested inside a plain Go struct would be encoded with
// encoding/json, which cannot round-trip google.protobuf.Struct.
func (a *EventActivities) PublishEvent(
	_ context.Context,
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
	return nil
}
