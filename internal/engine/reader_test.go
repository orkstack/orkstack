package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The reader is tested against synthesized history rather than a cluster,
// because what has to be right is the decoding: a run's history *is* its event
// log, and every replica has to read the same run and the same ordered stream
// out of it. The two-replica integration test proves the same claim end to end
// against a real Temporal (cmd/orkctl/tworeplica_read_integration_test.go).

var readerEpoch = time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC)

// fakeHistory is a HistoryEventIterator over a fixed slice, optionally ending
// in an error — which is how the server reports a run it does not hold.
type fakeHistory struct {
	events []*historypb.HistoryEvent
	at     int
	err    error
}

func (f *fakeHistory) HasNext() bool {
	return f.at < len(f.events) || f.err != nil
}

func (f *fakeHistory) Next() (*historypb.HistoryEvent, error) {
	if f.at < len(f.events) {
		event := f.events[f.at]
		f.at++
		return event, nil
	}
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	return nil, fmt.Errorf("fake history: Next past the end")
}

// fakeReadClient serves one history and one visibility page.
type fakeReadClient struct {
	history    map[string]*fakeHistory
	executions []*workflowpb.WorkflowExecutionInfo
	listQuery  string
	longPolled bool
}

func (f *fakeReadClient) GetWorkflowHistory(
	_ context.Context,
	workflowID, _ string,
	isLongPoll bool,
	_ enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	f.longPolled = isLongPoll
	history, ok := f.history[workflowID]
	if !ok {
		return &fakeHistory{err: serviceerror.NewNotFound("workflow execution not found")}
	}
	return &fakeHistory{events: history.events, err: history.err}
}

func (f *fakeReadClient) ListWorkflow(
	_ context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.listQuery = request.GetQuery()
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: f.executions}, nil
}

func payloadsOf(t *testing.T, values ...any) *commonpb.Payloads {
	t.Helper()
	encoded, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	if err != nil {
		t.Fatalf("encode payloads: %v", err)
	}
	return encoded
}

// startedEvent is the WorkflowExecutionStarted history event RunWorkflow is
// launched with: (runID, orkflow, input).
func startedEvent(t *testing.T, runID string, wf *orkstackv1.Orkflow) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(readerEpoch),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input:        payloadsOf(t, runID, wf, &orkstackv1.Payload{}),
			},
		},
	}
}

// publishEventScheduled is what the engine records for every lifecycle event:
// the scheduling of the projection activity, carrying the event and the run
// snapshot as its input.
func publishEventScheduled(
	t *testing.T,
	eventID int64,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(readerEpoch.Add(time.Duration(eventID) * time.Second)),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent},
				Input: payloadsOf(t, event, run,
					fmt.Sprintf("%s#%d", run.GetId(), event.GetSequence())),
			},
		},
	}
}

// helloRun is one run's history: created, started, task succeeded, succeeded —
// the shape every OrkStack run has.
func helloRun(t *testing.T, runID string) ([]*historypb.HistoryEvent, *orkstackv1.Orkflow) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "hello-ork", Version: "0.1.0"}
	events := []*historypb.HistoryEvent{startedEvent(t, runID, wf)}

	kinds := []struct {
		eventType orkstackv1.EventType
		status    orkstackv1.Status
	}{
		{orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING},
		{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
		{orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for i, kind := range kinds {
		sequence := int64(i + 1)
		runStatus := orkstackv1.Status_STATUS_RUNNING
		if kind.eventType == orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
			runStatus = orkstackv1.Status_STATUS_PENDING
		}
		if kind.eventType == orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
			runStatus = orkstackv1.Status_STATUS_SUCCEEDED
		}
		snapshot := &orkstackv1.Run{
			Id:             runID,
			OrkflowName:    wf.GetName(),
			OrkflowVersion: wf.GetVersion(),
			Status:         runStatus,
			CreatedAt:      timestamppb.New(readerEpoch),
		}
		if runStatus == orkstackv1.Status_STATUS_SUCCEEDED {
			snapshot.FinishedAt = timestamppb.New(readerEpoch.Add(10 * time.Second))
		}
		events = append(events, publishEventScheduled(t, sequence+1, &orkstackv1.Event{
			RunId:     runID,
			Sequence:  sequence,
			Type:      kind.eventType,
			Status:    kind.status,
			Message:   kind.eventType.String(),
			Timestamp: timestamppb.New(readerEpoch.Add(time.Duration(sequence) * time.Second)),
		}, snapshot))
	}
	return events, wf
}

func closedEvent(eventID int64, eventType enumspb.EventType, at time.Time) *historypb.HistoryEvent {
	event := &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(at),
		EventType: eventType,
	}
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason: "operator terminated the run",
			},
		}
	}
	return event
}

func readerOver(events []*historypb.HistoryEvent, runID string) (*TemporalRunReader, *fakeReadClient) {
	fake := &fakeReadClient{history: map[string]*fakeHistory{runID: {events: events}}}
	return newRunReader(fake, converter.GetDefaultDataConverter()), fake
}

type countingSnapshotStore struct {
	objects map[string][]byte
	reads   int
}

func (s *countingSnapshotStore) WriteSnapshot(context.Context, string, string, int64, []byte) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (s *countingSnapshotStore) ReadSnapshot(_ context.Context, uri string) ([]byte, error) {
	s.reads++
	data, ok := s.objects[uri]
	if !ok {
		return nil, artifacts.ErrNotFound
	}
	return data, nil
}

func publishEventWithURI(t *testing.T, eventID int64, event *orkstackv1.Event, uri string) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent},
				Input:        payloadsOf(t, event, uri, event.GetRunId()+"#"+fmt.Sprint(event.GetSequence())),
			},
		},
	}
}

func TestTemporalRunReader_FetchesOnlyTheLastSnapshotURI(t *testing.T) {
	const runID = "ork-uri-snapshot"
	wf := &orkstackv1.Orkflow{Name: "snapshot-ork", Version: "1.0.0"}
	first := &orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_RUNNING}
	last := &orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "done", Status: orkstackv1.Status_STATUS_SUCCEEDED}}}
	firstBytes, _ := proto.Marshal(first)
	lastBytes, _ := proto.Marshal(last)
	store := &countingSnapshotStore{objects: map[string][]byte{"memory://first": firstBytes, "memory://last": lastBytes}}
	history := []*historypb.HistoryEvent{
		startedEvent(t, runID, wf),
		publishEventWithURI(t, 2, &orkstackv1.Event{RunId: runID, Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED}, "memory://first"),
		publishEventWithURI(t, 3, &orkstackv1.Event{RunId: runID, Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED}, "memory://last"),
	}
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if store.reads != 1 {
		t.Fatalf("snapshot reads = %d, want exactly 1", store.reads)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetTaskRuns()) != 1 {
		t.Fatalf("rebuilt run = %+v, want complete last snapshot", run)
	}
}

// uriRunHistory is a run in the externalized shape: every lifecycle event
// carries a snapshot URI, the run publishes its own terminal event, and the
// execution closes afterwards when withClose is set.
func uriRunHistory(t *testing.T, runID string, withClose bool) []*historypb.HistoryEvent {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "uri-ork", Version: "1.0.0"}
	history := []*historypb.HistoryEvent{
		startedEvent(t, runID, wf),
		publishEventWithURI(t, 2, &orkstackv1.Event{RunId: runID, Sequence: 1,
			Type:   orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
			Status: orkstackv1.Status_STATUS_RUNNING}, "memory://first"),
		publishEventWithURI(t, 3, &orkstackv1.Event{RunId: runID, Sequence: 2,
			Type:   orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			Status: orkstackv1.Status_STATUS_SUCCEEDED}, "memory://last"),
	}
	if withClose {
		history = append(history, closedEvent(4, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			readerEpoch.Add(10*time.Second)))
	}
	return history
}

// TestTemporalRunReader_EventReadsSurviveAnUnreadableSnapshot is the
// regression for the live-run inversion: the events are complete in Temporal
// history, so an event read must succeed — and fetch nothing from object
// storage — even when every referenced snapshot object is gone and even when
// no snapshot store is wired at all. Before the fix both reads returned
// "artifact not found"/"snapshot store is not configured", which the API layer
// translated into CodeNotFound for a run Temporal was actively executing.
func TestTemporalRunReader_EventReadsSurviveAnUnreadableSnapshot(t *testing.T) {
	const runID = "ork-evrev-live"
	for name, history := range map[string][]*historypb.HistoryEvent{
		"live":   uriRunHistory(t, runID, false),
		"closed": uriRunHistory(t, runID, true),
	} {
		t.Run(name, func(t *testing.T) {
			// Every snapshot object is missing: reads would fail if attempted.
			store := &countingSnapshotStore{objects: map[string][]byte{}}
			reader, _ := readerOver(history, runID)
			reader.WithSnapshotStore(store)

			events, err := reader.Events(context.Background(), runID, 0)
			if err != nil {
				t.Fatalf("Events: %v", err)
			}
			if len(events) != 2 || events[1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
				t.Fatalf("events = %+v, want the 2 recorded events ending in RUN_SUCCEEDED", events)
			}
			if store.reads != 0 {
				t.Fatalf("event read fetched %d snapshots, want 0", store.reads)
			}

			// No snapshot store at all must read the same.
			bare, _ := readerOver(history, runID)
			events, err = bare.Events(context.Background(), runID, 0)
			if err != nil {
				t.Fatalf("Events without a snapshot store: %v", err)
			}
			if len(events) != 2 {
				t.Fatalf("events without a snapshot store = %+v, want 2", events)
			}

			var streamed []*orkstackv1.Event
			if err := bare.StreamEvents(context.Background(), runID, 0, func(event *orkstackv1.Event) error {
				streamed = append(streamed, event)
				return nil
			}); err != nil {
				t.Fatalf("StreamEvents: %v", err)
			}
			if len(streamed) != 2 {
				t.Fatalf("streamed = %+v, want 2 events", streamed)
			}
		})
	}
}

// TestTemporalRunReader_EventReadsDoNotSynthesizeASecondTerminal pins the
// closeRun decision an event read now makes from history alone: a closed run
// that published its own terminal event gets no synthetic duplicate, while a
// terminated run — which published nothing — still gets its synthesized end.
func TestTemporalRunReader_EventReadsDoNotSynthesizeASecondTerminal(t *testing.T) {
	const runID = "ork-evrev-close"
	terminated := []*historypb.HistoryEvent{
		startedEvent(t, runID, &orkstackv1.Orkflow{Name: "uri-ork", Version: "1.0.0"}),
		publishEventWithURI(t, 2, &orkstackv1.Event{RunId: runID, Sequence: 1,
			Type:   orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
			Status: orkstackv1.Status_STATUS_RUNNING}, "memory://first"),
		closedEvent(3, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			readerEpoch.Add(10*time.Second)),
	}
	reader, _ := readerOver(terminated, runID)
	events, err := reader.Events(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("Events(terminated): %v", err)
	}
	if len(events) != 2 || events[1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("terminated events = %+v, want RUN_STARTED then a synthesized RUN_CANCELED", events)
	}

	closed := uriRunHistory(t, runID, true)
	reader, _ = readerOver(closed, runID)
	events, err = reader.Events(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("Events(closed): %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("closed run events = %+v, want exactly the 2 published events", events)
	}
}

// TestTemporalRunReader_RebuildsTheOrderedStreamFromHistory is the core claim:
// the events Temporal recorded when each projection activity was *scheduled*
// are the run's stream, in order, on any replica.
func TestTemporalRunReader_RebuildsTheOrderedStreamFromHistory(t *testing.T) {
	const runID = "ork-read-1"
	history, _ := helloRun(t, runID)
	history = append(history, closedEvent(7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		readerEpoch.Add(10*time.Second)))
	reader, _ := readerOver(history, runID)
	ctx := context.Background()

	events, err := reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("decoded %d events, want 4", len(events))
	}
	for i, event := range events {
		if got, want := event.GetSequence(), int64(i+1); got != want {
			t.Fatalf("event %d carries sequence %d, want %d", i, got, want)
		}
	}
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
		t.Errorf("first event = %s, want RUN_CREATED", events[0].GetType())
	}
	if events[3].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Errorf("last event = %s, want RUN_SUCCEEDED", events[3].GetType())
	}

	after, err := reader.Events(ctx, runID, 2)
	if err != nil {
		t.Fatalf("Events(from=2): %v", err)
	}
	if len(after) != 2 || after[0].GetSequence() != 3 || after[1].GetSequence() != 4 {
		t.Fatalf("resume after 2 returned %v, want sequences 3 and 4", after)
	}
}

func TestTemporalRunReader_GetRunReturnsTheLatestPublishedSnapshot(t *testing.T) {
	const runID = "ork-read-2"
	history, _ := helloRun(t, runID)
	history = append(history, closedEvent(7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		readerEpoch.Add(10*time.Second)))
	reader, _ := readerOver(history, runID)

	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetId() != runID || run.GetOrkflowName() != "hello-ork" || run.GetOrkflowVersion() != "0.1.0" {
		t.Fatalf("GetRun returned %s / %s@%s", run.GetId(), run.GetOrkflowName(), run.GetOrkflowVersion())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("GetRun status = %s, want SUCCEEDED", run.GetStatus())
	}
	if run.GetCreatedAt().AsTime() != readerEpoch {
		t.Errorf("GetRun created_at = %v, want %v", run.GetCreatedAt().AsTime(), readerEpoch)
	}
}

// TestTemporalRunReader_StreamEndsAtTheTerminalEvent holds the stream contract
// and proves the long poll is used: a live run's stream is the iterator.
func TestTemporalRunReader_StreamEndsAtTheTerminalEvent(t *testing.T) {
	const runID = "ork-read-3"
	history, _ := helloRun(t, runID)
	reader, fake := readerOver(history, runID)

	var delivered []*orkstackv1.Event
	err := reader.StreamEvents(context.Background(), runID, 0, func(event *orkstackv1.Event) error {
		delivered = append(delivered, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if !fake.longPolled {
		t.Error("StreamEvents did not long-poll; a live run's stream would end early")
	}
	if len(delivered) != 4 {
		t.Fatalf("delivered %d events, want 4", len(delivered))
	}
	if delivered[3].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("stream ended on %s, want RUN_SUCCEEDED", delivered[3].GetType())
	}
}

// TestTemporalRunReader_ReportsATerminatedRunAsEnded covers the run that emits
// nothing: termination kills the workflow, so the only record of the end is the
// execution's own closing event. Without the overlay such a run reads RUNNING
// forever and its stream never terminates.
func TestTemporalRunReader_ReportsATerminatedRunAsEnded(t *testing.T) {
	const runID = "ork-read-4"
	history, _ := helloRun(t, runID)
	// Keep only created + started, then terminate.
	history = append(history[:3],
		closedEvent(9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			readerEpoch.Add(30*time.Second)))
	reader, _ := readerOver(history, runID)
	ctx := context.Background()

	run, err := reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("a terminated run reads %s, want STATUS_CANCELED", run.GetStatus())
	}
	if run.GetFinishedAt() == nil {
		t.Error("a terminated run has no finished_at")
	}
	if run.GetError().GetCode() != "RUN_TERMINATED" {
		t.Errorf("run error = %v, want RUN_TERMINATED", run.GetError())
	}

	var delivered []*orkstackv1.Event
	if err := reader.StreamEvents(ctx, runID, 0, func(event *orkstackv1.Event) error {
		delivered = append(delivered, event)
		return nil
	}); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(delivered) != 3 {
		t.Fatalf("delivered %v, want the two published events and a terminal one",
			delivered)
	}
	last := delivered[2]
	if last.GetSequence() != 3 {
		t.Errorf("the synthesized terminal event carries sequence %d, want 3", last.GetSequence())
	}
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Errorf("the stream ended on %s, want RUN_CANCELED", last.GetType())
	}
}

// TestTemporalRunReader_AnswersBeforeTheFirstEventIsPublished closes the window
// between SubmitRun returning and the first projection activity reaching
// history: the run is already readable, from the arguments it was started with.
func TestTemporalRunReader_AnswersBeforeTheFirstEventIsPublished(t *testing.T) {
	const runID = "ork-read-5"
	wf := &orkstackv1.Orkflow{Name: "hello-ork", Version: "0.1.0"}
	reader, _ := readerOver([]*historypb.HistoryEvent{startedEvent(t, runID, wf)}, runID)

	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetOrkflowName() != "hello-ork" || run.GetOrkflowVersion() != "0.1.0" {
		t.Fatalf("GetRun returned %s@%s, want hello-ork@0.1.0",
			run.GetOrkflowName(), run.GetOrkflowVersion())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Errorf("a run with no published event reads %s, want PENDING", run.GetStatus())
	}
	if run.GetCreatedAt() == nil {
		t.Error("a started run has no created_at")
	}
}

// TestTemporalRunReader_ReportsAMissingRunAsNotFound is what lets the API fall
// back to the archive: the error has to be recognisable, and it has to arrive
// before anything was sent.
func TestTemporalRunReader_ReportsAMissingRunAsNotFound(t *testing.T) {
	reader, _ := readerOver(nil, "ork-present-1")
	ctx := context.Background()

	_, err := reader.GetRun(ctx, "ork-absent-1")
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("GetRun error = %v, want ErrRunNotFound", err)
	}
	if !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetRun error = %v, want it to match artifacts.ErrNotFound", err)
	}

	sent := 0
	err = reader.StreamEvents(ctx, "ork-absent-1", 0, func(*orkstackv1.Event) error {
		sent++
		return nil
	})
	if !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("StreamEvents error = %v, want a not-found", err)
	}
	if sent != 0 {
		t.Fatalf("StreamEvents sent %d events before reporting not-found; "+
			"the archive fallback would duplicate them", sent)
	}
}

// TestTemporalRunReader_ListsRunsFromVisibility proves the list half reads the
// cluster's index, and names the orkflow from the memo StartRun writes.
func TestTemporalRunReader_ListsRunsFromVisibility(t *testing.T) {
	fake := &fakeReadClient{
		executions: []*workflowpb.WorkflowExecutionInfo{
			visibilityRecord(t, "ork-list-1", "hello-ork", "0.1.0",
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch),
			visibilityRecord(t, "ork-list-2", "other-ork", "2.0.0",
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, readerEpoch.Add(time.Minute)),
		},
	}
	reader := newRunReader(fake, converter.GetDefaultDataConverter())

	all, err := reader.ListRuns(context.Background(), RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListRuns returned %d runs, want 2", len(all))
	}
	if fake.listQuery != fmt.Sprintf("WorkflowType = %q", RunWorkflowType) {
		t.Errorf("ListRuns queried %q, want only run workflows", fake.listQuery)
	}
	if all[0].GetOrkflowName() != "hello-ork" || all[0].GetOrkflowVersion() != "0.1.0" {
		t.Errorf("listed run 1 = %s@%s, want hello-ork@0.1.0",
			all[0].GetOrkflowName(), all[0].GetOrkflowVersion())
	}
	if all[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("a running execution lists as %s, want RUNNING", all[0].GetStatus())
	}
	if all[1].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("a completed execution lists as %s, want SUCCEEDED", all[1].GetStatus())
	}

	byName, err := reader.ListRuns(context.Background(), RunQuery{OrkflowName: "other-ork"})
	if err != nil {
		t.Fatalf("ListRuns(name): %v", err)
	}
	if len(byName) != 1 || byName[0].GetId() != "ork-list-2" {
		t.Fatalf("filtering by orkflow returned %v, want only ork-list-2", byName)
	}

	byStatus, err := reader.ListRuns(context.Background(), RunQuery{
		Status: orkstackv1.Status_STATUS_RUNNING,
	})
	if err != nil {
		t.Fatalf("ListRuns(status): %v", err)
	}
	if len(byStatus) != 1 || byStatus[0].GetId() != "ork-list-1" {
		t.Fatalf("filtering by status returned %v, want only ork-list-1", byStatus)
	}
}

// TestTemporalRunReader_ExhaustiveListWalksPastTheFirstPage proves the
// safety-gate query keeps paging, so an older active run behind a full page of
// newer runs is still found, and that asking for everything is what unlocks
// the unbounded answer.
func TestTemporalRunReader_ExhaustiveListWalksPastTheFirstPage(t *testing.T) {
	pages := [][]*workflowpb.WorkflowExecutionInfo{
		{visibilityRecord(t, "ork-newer", "hello-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, readerEpoch.Add(time.Hour))},
		{visibilityRecord(t, "ork-older-active", "hello-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch)},
	}
	bounded, err := newRunReader(&pagedReadClient{pages: pages},
		converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{OrkflowName: "hello-ork", Limit: 1})
	if err != nil {
		t.Fatalf("bounded ListRuns: %v", err)
	}
	if len(bounded) != 1 || bounded[0].GetId() != "ork-newer" {
		t.Fatalf("bounded list = %v, want only the newest run", bounded)
	}

	// Limit 0 under Exhaustive is the deletion gate's query: every match, so
	// the refusal can count honestly.
	all, err := newRunReader(&pagedReadClient{pages: pages},
		converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{OrkflowName: "hello-ork", Exhaustive: true})
	if err != nil {
		t.Fatalf("exhaustive ListRuns: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("exhaustive list returned %d runs, want both pages", len(all))
	}
	if all[1].GetId() != "ork-older-active" || all[1].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("exhaustive list missed the older active run: %v", all)
	}
}

// TestTemporalRunReader_ExhaustiveListStillHonoursLimit separates the two
// things Exhaustive could have meant. It makes the walk complete; it does not
// widen the answer. A caller that asks for one run gets one run, and the run
// it gets is found by walking past the page it was not on.
func TestTemporalRunReader_ExhaustiveListStillHonoursLimit(t *testing.T) {
	pages := [][]*workflowpb.WorkflowExecutionInfo{
		{visibilityRecord(t, "ork-newer", "hello-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, readerEpoch.Add(time.Hour))},
		{visibilityRecord(t, "ork-older-active", "hello-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch)},
	}
	capped, err := newRunReader(&pagedReadClient{pages: pages},
		converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{OrkflowName: "hello-ork", Limit: 1, Exhaustive: true})
	if err != nil {
		t.Fatalf("exhaustive ListRuns(Limit=1): %v", err)
	}
	if len(capped) != 1 {
		t.Fatalf("exhaustive list with Limit 1 returned %d runs, want 1; Exhaustive controls "+
			"how far the walk goes, not how much comes back", len(capped))
	}

	// The same bound, asked the question the UI asks: only the older run on
	// the second page matches, so a reader that stopped at page one would
	// report no live run.
	live, err := newRunReader(&pagedReadClient{pages: pages},
		converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{
			OrkflowName: "hello-ork",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			Limit:       1,
			Exhaustive:  true,
		})
	if err != nil {
		t.Fatalf("exhaustive ListRuns(status=RUNNING, Limit=1): %v", err)
	}
	if len(live) != 1 || live[0].GetId() != "ork-older-active" {
		t.Fatalf("exhaustive status query = %v, want the older active run from the second page", live)
	}
}

// pagedReadClient serves one visibility page per call, so a walk that stops at
// the first page is observable.
type pagedReadClient struct {
	pages [][]*workflowpb.WorkflowExecutionInfo
	calls int
}

func (p *pagedReadClient) GetWorkflowHistory(
	context.Context, string, string, bool, enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	return &fakeHistory{err: serviceerror.NewNotFound("workflow execution not found")}
}

func (p *pagedReadClient) ListWorkflow(
	_ context.Context,
	_ *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	index := p.calls
	p.calls++
	if index >= len(p.pages) {
		return &workflowservice.ListWorkflowExecutionsResponse{}, nil
	}
	response := &workflowservice.ListWorkflowExecutionsResponse{Executions: p.pages[index]}
	if index < len(p.pages)-1 {
		response.NextPageToken = []byte("next")
	}
	return response, nil
}

func visibilityRecord(
	t *testing.T,
	runID, name, version string,
	status enumspb.WorkflowExecutionStatus,
	startedAt time.Time,
) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	namePayload, err := dc.ToPayload(name)
	if err != nil {
		t.Fatalf("encode memo name: %v", err)
	}
	versionPayload, err := dc.ToPayload(version)
	if err != nil {
		t.Fatalf("encode memo version: %v", err)
	}
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: runID},
		Type:      &commonpb.WorkflowType{Name: RunWorkflowType},
		Status:    status,
		StartTime: timestamppb.New(startedAt),
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			MemoOrkflowName:    namePayload,
			MemoOrkflowVersion: versionPayload,
		}},
	}
	if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		info.CloseTime = timestamppb.New(startedAt.Add(time.Minute))
	}
	return info
}

// endlessReadClient always returns a full page and always offers another one,
// so a walk that does not stop on its own budget never stops at all. It also
// records every query string it was handed.
type endlessReadClient struct {
	page    []*workflowpb.WorkflowExecutionInfo
	calls   int
	queries []string
}

func (e *endlessReadClient) GetWorkflowHistory(
	context.Context, string, string, bool, enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	return &fakeHistory{err: serviceerror.NewNotFound("workflow execution not found")}
}

func (e *endlessReadClient) ListWorkflow(
	_ context.Context,
	req *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	e.calls++
	e.queries = append(e.queries, req.GetQuery())
	return &workflowservice.ListWorkflowExecutionsResponse{
		Executions:    e.page,
		NextPageToken: []byte("next"),
	}, nil
}

// TestTemporalRunReader_OrdinaryListStaysOnTheBoundedBudget pins the half of
// the walk a wire caller gets. An ordinary list — any list that did not ASK to
// be exhaustive — stops at maxRunListPages and returns what it found. It must
// not spend the 2000-page scan budget, and it must not report the
// deletion-safety failure, which is a sentence about an internal gate that no
// `orkctl run list` should ever be able to print.
func TestTemporalRunReader_OrdinaryListStaysOnTheBoundedBudget(t *testing.T) {
	// A page of runs belonging to a DIFFERENT orkflow: every record is read
	// and discarded, so nothing ever satisfies the limit and only the budget
	// can end the walk.
	client := &endlessReadClient{page: []*workflowpb.WorkflowExecutionInfo{
		visibilityRecord(t, "ork-other", "other-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch),
	}}
	runs, err := newRunReader(client, converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{
			OrkflowName: "hello-ork",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			Limit:       1,
		})
	if err != nil {
		t.Fatalf("ordinary ListRuns returned an error: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("ordinary ListRuns = %v, want none: no record matches the orkflow", runs)
	}
	if client.calls != maxRunListPages {
		t.Fatalf("ordinary ListRuns read %d pages, want the bounded %d; a wire request "+
			"must not reach the %d-page scan budget", client.calls, maxRunListPages, maxRunScanPages)
	}
}

// TestTemporalRunReader_PushesTheStatusPredicateIntoVisibility is why the
// bounded budget above is ENOUGH for the delete control's question. With the
// status in the query, visibility returns live executions only, so "is
// anything still live?" is answered over matches instead of over every run the
// orkflow ever had.
func TestTemporalRunReader_PushesTheStatusPredicateIntoVisibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status orkstackv1.Status
		want   string
	}{
		{
			name:   "running is the complement of the terminal set",
			status: orkstackv1.Status_STATUS_RUNNING,
			want: `ExecutionStatus NOT IN ("Completed", "Failed", "TimedOut", ` +
				`"Canceled", "Terminated")`,
		},
		{
			name:   "failed covers the timeout Temporal reports separately",
			status: orkstackv1.Status_STATUS_FAILED,
			want:   `ExecutionStatus IN ("Failed", "TimedOut")`,
		},
		{
			name:   "canceled covers termination",
			status: orkstackv1.Status_STATUS_CANCELED,
			want:   `ExecutionStatus IN ("Canceled", "Terminated")`,
		},
		{
			name:   "succeeded is exactly completed",
			status: orkstackv1.Status_STATUS_SUCCEEDED,
			want:   `ExecutionStatus IN ("Completed")`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &endlessReadClient{}
			_, err := newRunReader(client, converter.GetDefaultDataConverter()).
				ListRuns(context.Background(), RunQuery{
					OrkflowName: "hello-ork", Status: tc.status, Limit: 1,
				})
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			if len(client.queries) == 0 {
				t.Fatal("ListRuns issued no visibility query")
			}
			if !strings.Contains(client.queries[0], tc.want) {
				t.Fatalf("visibility query = %q, want it to contain %q",
					client.queries[0], tc.want)
			}
		})
	}
}

// An unfiltered list pushes nothing down and still walks: STATUS_UNSPECIFIED
// means "no status filter", not "a filter visibility cannot express".
func TestTemporalRunReader_PushesNoStatusPredicateForAnUnfilteredList(t *testing.T) {
	client := &endlessReadClient{}
	if _, err := newRunReader(client, converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{
			OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_UNSPECIFIED, Limit: 1,
		}); err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(client.queries) == 0 {
		t.Fatal("an unfiltered list issued no visibility query")
	}
	if strings.Contains(client.queries[0], "ExecutionStatus") {
		t.Fatalf("visibility query = %q, want no ExecutionStatus predicate",
			client.queries[0])
	}
}

// TestTemporalRunReader_AsksNothingForAStatusVisibilityCannotDecode pins the
// difference between "found nothing" and "asked nothing".
//
// statusFromExecution can only ever return SUCCEEDED, FAILED, CANCELED or
// RUNNING, so matchesRunQuery rejects EVERY record a PENDING or SKIPPED walk
// reads. The old behaviour spent the whole page budget proving that — 20
// visibility round-trips per call, and the UI's Versions tab asks on every
// render. The answer is provably empty before the first request, so the walk
// is skipped rather than performed.
//
// The call count is the assertion, deliberately: an empty result alone cannot
// tell a skipped walk from an exhausted one.
func TestTemporalRunReader_AsksNothingForAStatusVisibilityCannotDecode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status orkstackv1.Status
	}{
		{"pending is never decoded from visibility", orkstackv1.Status_STATUS_PENDING},
		{"skipped is never decoded from visibility", orkstackv1.Status_STATUS_SKIPPED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &endlessReadClient{}
			runs, err := newRunReader(client, converter.GetDefaultDataConverter()).
				ListRuns(context.Background(), RunQuery{
					OrkflowName: "hello-ork", Status: tc.status, Limit: 1,
				})
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			if len(runs) != 0 {
				t.Fatalf("returned %d runs, want none: visibility cannot decode %s",
					len(runs), tc.status)
			}
			if client.calls != 0 {
				t.Fatalf("issued %d visibility calls, want 0: the answer is provably "+
					"empty before the first request, so asking is pure cost",
					client.calls)
			}
		})
	}
}

// The exhaustive walk must take the same shortcut. An empty answer that is
// provably COMPLETE is not the short answer the exhaustive error guards
// against, so the deletion-safety caller may not be handed a failure for a
// question visibility was never going to answer.
func TestTemporalRunReader_ExhaustiveAlsoAsksNothingForAnImpossibleStatus(t *testing.T) {
	client := &endlessReadClient{}
	runs, err := newRunReader(client, converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{
			Namespace: "payments", OrkflowName: "hello-ork",
			Status: orkstackv1.Status_STATUS_PENDING, Exhaustive: true,
		})
	if err != nil {
		t.Fatalf("exhaustive ListRuns: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("returned %d runs, want none", len(runs))
	}
	if client.calls != 0 {
		t.Fatalf("issued %d visibility calls, want 0", client.calls)
	}
}

// TestReaderStatuses_TracksStatusFromExecution is the anti-drift guard for the
// shortcut. The skipped set is the COMPLEMENT of what statusFromExecution can
// return, so it is derived from that function here rather than restated:
// iterating the SDK's own enum means a status added by an upgrade fails this
// test instead of quietly making a reachable status unaskable.
func TestReaderStatuses_TracksStatusFromExecution(t *testing.T) {
	for value := range enumspb.WorkflowExecutionStatus_name {
		decoded := statusFromExecution(enumspb.WorkflowExecutionStatus(value))
		if !readerDecodableStatuses[decoded] {
			t.Errorf("statusFromExecution(%s) = %s, but the reader treats %s as a "+
				"status it can never decode and skips the walk for it",
				enumspb.WorkflowExecutionStatus(value), decoded, decoded)
		}
	}
	// The two archive-only outcomes must stay outside the decodable set, or
	// the shortcut silently stops applying and the wasted walk returns.
	for _, status := range []orkstackv1.Status{
		orkstackv1.Status_STATUS_PENDING,
		orkstackv1.Status_STATUS_SKIPPED,
	} {
		if readerDecodableStatuses[status] {
			t.Errorf("%s is marked decodable from visibility, but no Temporal "+
				"execution status maps onto it", status)
		}
	}
}

// TestExecutionStatusPredicate_TracksStatusFromExecution is the anti-drift
// guard. The pushdown and the DECODE must describe the same partition: a
// status listed as terminal here that statusFromExecution decodes as RUNNING
// would be filtered out of the live set and let deletion proceed over a live
// run. Iterating the SDK's own enum means a status added by an upgrade fails
// this test rather than silently joining the wrong side.
func TestExecutionStatusPredicate_TracksStatusFromExecution(t *testing.T) {
	terminal := map[enumspb.WorkflowExecutionStatus]bool{}
	for _, status := range terminalExecutionStatuses {
		terminal[status] = true
	}
	for value := range enumspb.WorkflowExecutionStatus_name {
		status := enumspb.WorkflowExecutionStatus(value)
		decodedLive := statusFromExecution(status) == orkstackv1.Status_STATUS_RUNNING
		if decodedLive == terminal[status] {
			t.Errorf("%s: statusFromExecution decodes it as live=%t, but the pushdown "+
				"treats it as terminal=%t; the two must agree or the live set is wrong",
				status, decodedLive, terminal[status])
		}
	}
}
