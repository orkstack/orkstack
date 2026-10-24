package engine_test

import (
	"context"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// recordingObserver stands in for the telemetry seam: it records every event
// PublishEvent hands to it, so a test can assert both what fired and how many
// times.
type recordingObserver struct {
	mu     sync.Mutex
	events []*orkstackv1.Event
	runs   []*orkstackv1.Run
}

func (o *recordingObserver) ObserveEvent(
	_ context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	o.runs = append(o.runs, run)
}

func (o *recordingObserver) types() []orkstackv1.EventType {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]orkstackv1.EventType, 0, len(o.events))
	for _, event := range o.events {
		out = append(out, event.GetType())
	}
	return out
}

func TestPublishEvent_ObservesEveryEventTypeIncludingTasksAndLogs(t *testing.T) {
	observer := &recordingObserver{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithEventObserver(observer))

	const runID = "ork-1755302400123456789-10"
	sequence := []struct {
		eventType orkstackv1.EventType
		status    orkstackv1.Status
	}{
		{orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING},
		{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
		{orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for i, step := range sequence {
		run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: step.status}
		if err := activities.PublishEvent(context.Background(),
			runEvent(runID, step.eventType, step.status), snapshots.putRun(t, run), dedupe(runID, i)); err != nil {
			t.Fatalf("PublishEvent %v: %v", step.eventType, err)
		}
	}

	want := make([]orkstackv1.EventType, len(sequence))
	for i, step := range sequence {
		want[i] = step.eventType
	}
	got := observer.types()
	if len(got) != len(want) {
		t.Fatalf("observed %v, want every event %v", got, want)
	}
	for i, eventType := range want {
		if got[i] != eventType {
			t.Errorf("observation %d = %v, want %v", i, got[i], eventType)
		}
	}
}

func TestPublishEvent_ObservesATransitionOnceAcrossActivityRetries(t *testing.T) {
	observer := &recordingObserver{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithEventObserver(observer))

	const runID = "ork-1755302400123456789-11"
	run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	event := runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED)

	// The same dedupe key three times: a Temporal activity retry, then a
	// workflow replay. The observer's counts must not move on the retries,
	// exactly like the notifier's.
	for range 3 {
		if err := activities.PublishEvent(context.Background(), event, snapshots.putRun(t, run), runID+"#7"); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
	}
	if got := observer.types(); len(got) != 1 {
		t.Errorf("observed the same event %d times (%v); a retry must not re-observe it", len(got), got)
	}
}

func TestPublishEvent_ObservesTheMergedRunSnapshot(t *testing.T) {
	// Same guarantee as the notifier: the workflow's own snapshot does not
	// carry CreatedAt or Input, so the observer must see the store's merged
	// copy, not the partial one PublishEvent decoded off the wire.
	store := engine.NewRunStore()
	const runID = "ork-1755302400123456789-12"
	store.Save(&orkstackv1.Run{
		Id:          runID,
		OrkflowName: "hello-ork",
		Status:      orkstackv1.Status_STATUS_PENDING,
	})

	observer := &recordingObserver{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), store).WithEventObserver(observer))
	finished := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if err := activities.PublishEvent(context.Background(),
		runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED),
		snapshots.putRun(t, finished), runID+"#1"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.runs) != 1 {
		t.Fatalf("observed %d runs, want 1", len(observer.runs))
	}
	if observer.runs[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("observed run status = %v, want the transition's", observer.runs[0].GetStatus())
	}
}

func TestPublishEvent_ProjectsNormallyWithoutAnObserver(t *testing.T) {
	bus := engine.NewEventBus()
	store := engine.NewRunStore()
	activities, snapshots := eventActivitiesWithSnapshots(engine.NewEventActivities(bus, store))

	const runID = "ork-1755302400123456789-13"
	run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if err := activities.PublishEvent(context.Background(),
		runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED),
		snapshots.putRun(t, run), runID+"#1"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if events := bus.Events(runID, 0); len(events) != 1 {
		t.Errorf("bus holds %d events, want 1", len(events))
	}
}
