package engine

import (
	"context"
	"errors"
	"fmt"
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
