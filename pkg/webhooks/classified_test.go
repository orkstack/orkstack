package webhooks

import (
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// notSubscribable is every event type a subscription can never name.
var notSubscribable = []Event{"RUN_CREATED", "TASK_LOG", "STREAM_GAP"}

// TestWebhooks_EveryEventTypeIsClassified: a new proto event type fails here
// until it is placed in KnownEvents or in notSubscribable, so no event is
// silently dropped.
func TestWebhooks_EveryEventTypeIsClassified(t *testing.T) {
	for number, name := range orkstackv1.EventType_name {
		short := Event(strings.TrimPrefix(name, "EVENT_TYPE_"))
		if orkstackv1.EventType(number) == orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED {
			continue
		}
		known := slices.Contains(KnownEvents(), short)
		refused := slices.Contains(notSubscribable, short)
		if known == refused {
			t.Errorf("%s: in KnownEvents = %v, in notSubscribable = %v; it must be in exactly one", short, known, refused)
		}
		if want := known && strings.HasPrefix(string(short), "TASK_"); isTaskEvent(short) != want {
			t.Errorf("isTaskEvent(%s) = %v, want %v", short, isTaskEvent(short), want)
		}
	}
	for _, event := range KnownEvents() {
		if _, ok := orkstackv1.EventType_value["EVENT_TYPE_"+string(event)]; !ok {
			t.Errorf("KnownEvents names %s, which is no proto event type", event)
		}
	}
	want := []Event{"RUN_STARTED", "RUN_SUCCEEDED", "RUN_FAILED", "RUN_CANCELED"}
	if got := RunEvents(); !slices.Equal(got, want) {
		t.Errorf("RunEvents() = %v, want %v", got, want)
	}
}

// TestValidateScope_MessageNamesEveryTaskEvent: the refusal lists the task
// events from the one slice, so it cannot omit one.
func TestValidateScope_MessageNamesEveryTaskEvent(t *testing.T) {
	err := ValidateScope("", "", "extract", nil)
	if err == nil {
		t.Fatal("a task scope with no events was accepted")
	}
	for _, event := range TaskEvents() {
		if !strings.Contains(err.Error(), string(event)) {
			t.Errorf("error %q does not name %s", err, event)
		}
	}
}
