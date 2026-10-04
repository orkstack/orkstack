package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// recordingNotifier stands in for whatever listens outside the process.
type recordingNotifier struct {
	mu     sync.Mutex
	events []*orkstackv1.Event
	runs   []*orkstackv1.Run
}

func (n *recordingNotifier) NotifyRunEvent(
	_ context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, event)
	n.runs = append(n.runs, run)
}

func (n *recordingNotifier) types() []orkstackv1.EventType {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]orkstackv1.EventType, 0, len(n.events))
	for _, event := range n.events {
		out = append(out, event.GetType())
	}
	return out
}

// runEvent builds one lifecycle event for a run.
func runEvent(runID string, eventType orkstackv1.EventType, status orkstackv1.Status) *orkstackv1.Event {
	return &orkstackv1.Event{
		RunId:     runID,
		Type:      eventType,
		Status:    status,
		Timestamp: timestamppb.New(time.Unix(1755302400, 0).UTC()),
	}
}

func TestPublishEvent_AnnouncesRunTransitionsAndTaskOutcomesButNeverLogs(t *testing.T) {
	notifier := &recordingNotifier{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithRunNotifier(notifier))

	const runID = "ork-1755302400123456789-1"
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
		{orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_FAILED},
		// A skipped task and a task that never started before its run ended
		// are recorded as log lines; neither is ever announced.
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_SKIPPED},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_CANCELED},
		{orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for i, step := range sequence {
		run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: step.status}
		if err := activities.PublishEvent(context.Background(),
			runEvent(runID, step.eventType, step.status), snapshots.putRun(t, run), dedupe(runID, i)); err != nil {
			t.Fatalf("PublishEvent %v: %v", step.eventType, err)
		}
	}

	want := []orkstackv1.EventType{
		orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		orkstackv1.EventType_EVENT_TYPE_TASK_STARTED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_TASK_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
	}
	got := notifier.types()
	if len(got) != len(want) {
		t.Fatalf("announced %v, want the run transitions and task outcomes %v", got, want)
	}
	for i, eventType := range want {
		if got[i] != eventType {
			t.Errorf("announcement %d = %v, want %v", i, got[i], eventType)
		}
	}
}

func TestPublishEvent_AnnouncesATaskOutcomeOnceAcrossActivityRetries(t *testing.T) {
	notifier := &recordingNotifier{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithRunNotifier(notifier))

	const runID = "ork-1755302400123456789-6"
	run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_RUNNING}
	event := runEvent(runID, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED)
	event.TaskId = "extract"
	for range 3 {
		if err := activities.PublishEvent(context.Background(), event, snapshots.putRun(t, run), runID+"#9"); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
	}
	if got := notifier.types(); len(got) != 1 {
		t.Errorf("announced the same task outcome %d times (%v); a retry must not re-announce it", len(got), got)
	}
}

func TestPublishEvent_NeverAnnouncesATaskFailedEventCarryingCanceled(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    orkstackv1.Status
		announced bool
	}{
		{"a task running when its run was canceled", orkstackv1.Status_STATUS_CANCELED, false},
		{"a task that failed on its own terms", orkstackv1.Status_STATUS_FAILED, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			notifier := &recordingNotifier{}
			activities, snapshots := eventActivitiesWithSnapshots(
				engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithRunNotifier(notifier))
			const runID = "ork-1755302400123456789-7"
			sequence := []struct {
				eventType orkstackv1.EventType
				status    orkstackv1.Status
				taskID    string
			}{
				{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, ""},
				{orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, orkstackv1.Status_STATUS_RUNNING, "sibling"},
				{orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, orkstackv1.Status_STATUS_RUNNING, "sibling"},
				{orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, tc.status, "sibling"},
				{orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, orkstackv1.Status_STATUS_CANCELED, ""},
			}
			for i, step := range sequence {
				event := runEvent(runID, step.eventType, step.status)
				event.TaskId = step.taskID
				run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_RUNNING}
				if err := activities.PublishEvent(context.Background(), event, snapshots.putRun(t, run), dedupe(runID, i)); err != nil {
					t.Fatalf("PublishEvent %v: %v", step.eventType, err)
				}
			}
			announcedFailure := false
			for _, eventType := range notifier.types() {
				if eventType == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
					announcedFailure = true
				}
			}
			if announcedFailure != tc.announced {
				t.Errorf("TASK_FAILED carrying %v announced = %v, want %v (announcements %v)",
					tc.status, announcedFailure, tc.announced, notifier.types())
			}
			wantCount := 4
			if tc.announced {
				wantCount = 5
			}
			if got := notifier.types(); len(got) != wantCount {
				t.Errorf("announced %v, want %d announcements", got, wantCount)
			}
		})
	}
}

func TestIsSubscribableEvent_SelectsRunTransitionsAndTaskOutcomes(t *testing.T) {
	for typeValue := range orkstackv1.EventType_name {
		eventType := orkstackv1.EventType(typeValue)
		for statusValue := range orkstackv1.Status_name {
			status := orkstackv1.Status(statusValue)
			var want bool
			switch eventType {
			case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED,
				orkstackv1.EventType_EVENT_TYPE_TASK_STARTED,
				orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
				// An await's two events, whatever their status.
				orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING,
				orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED:
				want = true
			case orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
				want = status == orkstackv1.Status_STATUS_FAILED
			case orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
				want = false
			default:
				want = engine.IsRunTransitionEvent(&orkstackv1.Event{Type: eventType, Status: status})
			}
			if got := engine.IsSubscribableEvent(&orkstackv1.Event{Type: eventType, Status: status}); got != want {
				t.Errorf("IsSubscribableEvent(%v, %v) = %v, want %v", eventType, status, got, want)
			}
		}
	}
	// The rows that carry the contract, stated outright.
	rows := []struct {
		eventType orkstackv1.EventType
		status    orkstackv1.Status
		want      bool
	}{
		{orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_FAILED, true},
		{orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_CANCELED, false},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_SKIPPED, false},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_CANCELED, false},
		{orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING, false},
		{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, true},
		{orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, orkstackv1.Status_STATUS_CANCELED, true},
		{orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, orkstackv1.Status_STATUS_AWAITING, true},
		{orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED, orkstackv1.Status_STATUS_SUCCEEDED, true},
		{orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED, orkstackv1.Status_STATUS_FAILED, true},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_SUCCEEDED, false},
		{orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_AWAITING, false},
	}
	for _, row := range rows {
		if got := engine.IsSubscribableEvent(&orkstackv1.Event{Type: row.eventType, Status: row.status}); got != row.want {
			t.Errorf("IsSubscribableEvent(%v, %v) = %v, want %v", row.eventType, row.status, got, row.want)
		}
	}
	if engine.IsSubscribableEvent(nil) {
		t.Error("IsSubscribableEvent(nil) = true")
	}
}

func TestPublishEvent_AnnouncesFailureAndCancellationToo(t *testing.T) {
	for _, terminal := range []struct {
		eventType orkstackv1.EventType
		status    orkstackv1.Status
	}{
		{orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED},
		{orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, orkstackv1.Status_STATUS_CANCELED},
	} {
		notifier := &recordingNotifier{}
		activities, snapshots := eventActivitiesWithSnapshots(
			engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithRunNotifier(notifier))

		const runID = "ork-1755302400123456789-2"
		run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: terminal.status}
		if err := activities.PublishEvent(context.Background(),
			runEvent(runID, terminal.eventType, terminal.status), snapshots.putRun(t, run), runID+"#1"); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
		if got := notifier.types(); len(got) != 1 || got[0] != terminal.eventType {
			t.Errorf("announced %v, want [%v]", got, terminal.eventType)
		}
	}
}

func TestPublishEvent_AnnouncesATransitionOnceAcrossActivityRetries(t *testing.T) {
	notifier := &recordingNotifier{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithRunNotifier(notifier))

	const runID = "ork-1755302400123456789-3"
	run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	event := runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.Status_STATUS_SUCCEEDED)

	// The same dedupe key three times: a Temporal activity retry, then a
	// workflow replay.
	for range 3 {
		if err := activities.PublishEvent(context.Background(), event, snapshots.putRun(t, run), runID+"#7"); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
	}
	if got := notifier.types(); len(got) != 1 {
		t.Errorf("announced the same transition %d times (%v); a retry must not re-announce it",
			len(got), got)
	}
}

func TestPublishEvent_AnnouncesTheMergedRunSnapshot(t *testing.T) {
	// The workflow's snapshot does not own CreatedAt or the run Input; the
	// store does. A subscriber must receive the merged run, not the partial
	// one the workflow emitted.
	store := engine.NewRunStore()
	const runID = "ork-1755302400123456789-4"
	created := timestamppb.New(time.Unix(1755302399, 0).UTC())
	store.Save(&orkstackv1.Run{
		Id:          runID,
		OrkflowName: "hello-ork",
		Status:      orkstackv1.Status_STATUS_PENDING,
		CreatedAt:   created,
	})

	notifier := &recordingNotifier{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewEventActivities(engine.NewEventBus(), store).WithRunNotifier(notifier))
	finished := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if err := activities.PublishEvent(context.Background(),
		runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED),
		snapshots.putRun(t, finished), runID+"#1"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	if len(notifier.runs) != 1 {
		t.Fatalf("announced %d runs, want 1", len(notifier.runs))
	}
	announced := notifier.runs[0]
	if !announced.GetCreatedAt().AsTime().Equal(created.AsTime()) {
		t.Errorf("announced run createdAt = %v, want the store's %v",
			announced.GetCreatedAt().AsTime(), created.AsTime())
	}
	if announced.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("announced run status = %v, want the transition's", announced.GetStatus())
	}
}

func TestPublishEvent_ProjectsNormallyWithoutANotifier(t *testing.T) {
	bus := engine.NewEventBus()
	store := engine.NewRunStore()
	activities, snapshots := eventActivitiesWithSnapshots(engine.NewEventActivities(bus, store))

	const runID = "ork-1755302400123456789-5"
	run := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if err := activities.PublishEvent(context.Background(),
		runEvent(runID, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED),
		snapshots.putRun(t, run), runID+"#1"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if events := bus.Events(runID, 0); len(events) != 1 {
		t.Errorf("bus holds %d events, want 1", len(events))
	}
	if _, ok := store.Get(runID); !ok {
		t.Error("the run was not recorded in the read model")
	}
}

func TestIsRunTransitionEvent_SelectsTheRunsOwnStateChanges(t *testing.T) {
	transitions := map[orkstackv1.EventType]bool{
		orkstackv1.EventType_EVENT_TYPE_RUN_STARTED:    true,
		orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED:  true,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED:     true,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:   true,
		orkstackv1.EventType_EVENT_TYPE_RUN_CREATED:    false,
		orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED: false,
		orkstackv1.EventType_EVENT_TYPE_TASK_STARTED:   false,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED: false,
		orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:    false,
		orkstackv1.EventType_EVENT_TYPE_TASK_LOG:       false,
		orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED:    false,
	}
	for eventType, want := range transitions {
		if got := engine.IsRunTransitionEvent(&orkstackv1.Event{Type: eventType}); got != want {
			t.Errorf("IsRunTransitionEvent(%v) = %v, want %v", eventType, got, want)
		}
	}
	if engine.IsRunTransitionEvent(nil) {
		t.Error("IsRunTransitionEvent(nil) = true")
	}
}

func dedupe(runID string, i int) string {
	return runID + "#" + time.Duration(i).String()
}
