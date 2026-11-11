package engine

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func orderEvent(runID string, seq int64, eventType orkstackv1.EventType) *orkstackv1.Event {
	return &orkstackv1.Event{RunId: runID, Sequence: seq, Type: eventType}
}

// receiveAll reads a subscription until its terminal event or its close,
// within a deadline, as the API's stream does.
func receiveAll(t *testing.T, events <-chan *orkstackv1.Event) []*orkstackv1.Event {
	t.Helper()
	var got []*orkstackv1.Event
	deadline := time.After(10 * time.Second)
	for {
		select {
		case event, open := <-events:
			if !open {
				return got
			}
			got = append(got, event)
			if isTerminalRunEvent(event) {
				return got
			}
		case <-deadline:
			t.Fatalf("the stream did not end; received %d events", len(got))
		}
	}
}

// TestEventBus_PublishesInReverseOrderReachASubscriberInOrder: a run's
// publishes complete in reverse order — 3, then 2, then 1, then its terminal
// event — as detached publishes can. A live subscriber is handed 1, 2, 3, 4,
// each once, and the stream ends cleanly; the bus's own log is in order too.
func TestEventBus_PublishesInReverseOrderReachASubscriberInOrder(t *testing.T) {
	const runID = "ork-reverse"
	bus := NewEventBus()
	events, cancel, terminal, streamErr := bus.Subscribe(runID, 0)
	defer cancel()
	if terminal {
		t.Fatal("a new run reported its terminal event reached")
	}
	for _, seq := range []int64{3, 2, 1} {
		bus.Publish(orderEvent(runID, seq, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	}
	bus.Publish(orderEvent(runID, 4, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED), "")
	got := receiveAll(t, events)
	if want := []int64{1, 2, 3, 4}; !equalSequences(sequencesOf(got), want) {
		t.Fatalf("the subscriber was handed %v, want %v", sequencesOf(got), want)
	}
	if err := streamErr(); err != nil {
		t.Fatalf("the stream ended with %v, want a clean end", err)
	}
	if logged := sequencesOf(bus.Events(runID, 0)); !equalSequences(logged, []int64{1, 2, 3, 4}) {
		t.Fatalf("the bus logged %v, want 1–4 in order", logged)
	}
}

// TestEventBus_ABacklogOutOfOrderIsHandedOverInOrder: a subscriber that
// arrives while 3 and 1 are on the bus and 2 is not is handed 1, then waits,
// and is handed 2 and 3 once 2 arrives.
func TestEventBus_ABacklogOutOfOrderIsHandedOverInOrder(t *testing.T) {
	const runID = "ork-backlog"
	bus := NewEventBus()
	bus.Publish(orderEvent(runID, 3, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	bus.Publish(orderEvent(runID, 1, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	events, cancel, _, streamErr := bus.Subscribe(runID, 0)
	defer cancel()
	if first := <-events; first.GetSequence() != 1 {
		t.Fatalf("the first event handed over is %d, want 1", first.GetSequence())
	}
	bus.Publish(orderEvent(runID, 2, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	bus.Publish(orderEvent(runID, 4, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED), "")
	rest := receiveAll(t, events)
	if want := []int64{2, 3, 4}; !equalSequences(sequencesOf(rest), want) {
		t.Fatalf("then %v, want %v", sequencesOf(rest), want)
	}
	if err := streamErr(); err != nil {
		t.Fatalf("the stream ended with %v", err)
	}
}

// TestEventBus_AMissingPredecessorEndsTheStreamNamingIt: 1 and 3 arrive and
// 2 never does. The subscriber is handed 1, holds 3, and once the order hold
// has passed the stream ends with an error naming 2 — never a silent stall.
func TestEventBus_AMissingPredecessorEndsTheStreamNamingIt(t *testing.T) {
	const runID = "ork-missing"
	bus := NewEventBus()
	bus.orderHold = 20 * time.Millisecond
	events, cancel, _, streamErr := bus.Subscribe(runID, 0)
	defer cancel()
	bus.Publish(orderEvent(runID, 1, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	bus.Publish(orderEvent(runID, 3, orkstackv1.EventType_EVENT_TYPE_TASK_LOG), "")
	got := receiveAll(t, events)
	if !equalSequences(sequencesOf(got), []int64{1}) {
		t.Fatalf("the subscriber was handed %v, want only 1", sequencesOf(got))
	}
	err := streamErr()
	if err == nil {
		t.Fatal("the stream ended without an error, though 2 never arrived")
	}
	for _, want := range []string{"event 2 of run ork-missing", "resumed from sequence 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the stream's error %q does not say %q", err, want)
		}
	}
}

// TestEventBus_ATerminalEventReleasesAGapAsAStreamGap: a run publishes its
// terminal event only after every detached publish of it completed, so a
// number still missing then was never published. The subscriber is handed
// the STREAM_GAP event the history reader serves for it, then the terminal
// event, and the stream ends cleanly.
func TestEventBus_ATerminalEventReleasesAGapAsAStreamGap(t *testing.T) {
	const runID = "ork-gap"
	bus := NewEventBus()
	events, cancel, _, streamErr := bus.Subscribe(runID, 0)
	defer cancel()
	bus.Publish(orderEvent(runID, 1, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED), "")
	bus.Publish(orderEvent(runID, 4, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED), "")
	got := receiveAll(t, events)
	if !equalSequences(sequencesOf(got), []int64{1, 3, 4}) {
		t.Fatalf("the subscriber was handed %v, want 1, the gap's STREAM_GAP (numbered 3), 4", sequencesOf(got))
	}
	gap := got[1]
	if gap.GetType() != orkstackv1.EventType_EVENT_TYPE_STREAM_GAP || !strings.Contains(gap.GetMessage(), "events 2–3") {
		t.Fatalf("the event between is %v %q, want a STREAM_GAP for 2–3", gap.GetType(), gap.GetMessage())
	}
	if err := streamErr(); err != nil {
		t.Fatalf("the stream ended with %v", err)
	}
}

func equalSequences(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
