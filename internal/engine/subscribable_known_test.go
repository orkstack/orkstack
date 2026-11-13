package engine_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestEngine_SubscribableMatchesKnownEvents: the engine's announce gate and
// the subscription registry agree. Every event a subscription can name passes
// IsSubscribableEvent, and none of the event types no subscription can name
// does, so a new event cannot be subscribable yet silently dropped.
func TestEngine_SubscribableMatchesKnownEvents(t *testing.T) {
	status := map[string]orkstackv1.Status{
		"RUN_STARTED":   orkstackv1.Status_STATUS_RUNNING,
		"RUN_SUCCEEDED": orkstackv1.Status_STATUS_SUCCEEDED,
		"RUN_FAILED":    orkstackv1.Status_STATUS_FAILED,
		"RUN_CANCELED":  orkstackv1.Status_STATUS_CANCELED,
		"TASK_FAILED":   orkstackv1.Status_STATUS_FAILED,
	}
	for _, known := range webhooks.KnownEvents() {
		value, ok := orkstackv1.EventType_value["EVENT_TYPE_"+string(known)]
		if !ok {
			t.Fatalf("%s is no proto event type", known)
		}
		event := &orkstackv1.Event{Type: orkstackv1.EventType(value), Status: status[string(known)]}
		if !engine.IsSubscribableEvent(event) {
			t.Errorf("IsSubscribableEvent drops %s, which a subscription can name", known)
		}
	}
	// Every other event type is one no subscription can name: derived from
	// the enum, so a type added later is held here too.
	known := map[string]bool{}
	for _, e := range webhooks.KnownEvents() {
		known["EVENT_TYPE_"+string(e)] = true
	}
	for name, value := range orkstackv1.EventType_value {
		if known[name] || value == int32(orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED) {
			continue
		}
		for _, st := range orkstackv1.Status_value {
			event := &orkstackv1.Event{Type: orkstackv1.EventType(value), Status: orkstackv1.Status(st)}
			if engine.IsSubscribableEvent(event) {
				t.Errorf("IsSubscribableEvent passes %s with status %d, which no subscription can name", name, st)
			}
		}
	}
}
