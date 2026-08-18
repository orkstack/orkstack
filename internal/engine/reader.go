package engine

import (
	"context"
	"errors"
	"fmt"

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

// ErrRunNotFound reports that the cluster holds no execution under a run id.
//
// It wraps artifacts.ErrNotFound so one "not found" test covers both halves of
// the read model: Temporal answers for live runs, the archive for finished ones
// whose history has aged out.
var ErrRunNotFound = fmt.Errorf("engine: run not found: %w", artifacts.ErrNotFound)

const (
	// MemoOrkflowName, MemoOrkflowVersion and MemoNamespace are the memo keys
	// StartRun writes onto an execution. Visibility returns them with the
	// execution, so a list can scope by namespace without reading history.
	//
	// All three carry the orkflow_ prefix, and the third one needs it most: an
	// unqualified `namespace` sitting beside a Temporal client would read as
	// the Temporal namespace, which is exactly the ambiguity AGENTS.md §1.9
	// forbids. The key is written into history, so it is chosen once.
	MemoOrkflowName    = "orkflow_name"
	MemoOrkflowVersion = "orkflow_version"
	MemoNamespace      = "orkflow_namespace"
	// DefaultRunPageSize bounds an unlimited ListRuns.
	DefaultRunPageSize = 200

	// maxRunListPages bounds how far ListRuns pages through visibility looking
	// for matches, so a narrow filter over a large namespace cannot turn one
	// request into an unbounded scan.
	maxRunListPages = 20
)

// RunQuery selects runs. Zero values mean "no filter".
type RunQuery struct {
	// Namespace narrows the list to one OrkStack namespace. Empty means no
	// namespace filter; callers that mean the built-in default pass "default".
	Namespace   string
	OrkflowName string
	Status      orkstackv1.Status
	// Limit caps the runs returned, newest first. 0 means DefaultRunPageSize.
	Limit int
}

// runReadClient is the slice of client.Client the reader uses. Narrowing it
// keeps the decoding testable against synthesized history, which is the part
// worth testing without a cluster.
type runReadClient interface {
	GetWorkflowHistory(
		ctx context.Context,
		workflowID, runID string,
		isLongPoll bool,
		filterType enumspb.HistoryEventFilterType,
	) client.HistoryEventIterator
	ListWorkflow(
		ctx context.Context,
		request *workflowservice.ListWorkflowExecutionsRequest,
	) (*workflowservice.ListWorkflowExecutionsResponse, error)
}

// TemporalRunReader answers run reads from Temporal, so any replica can answer
// for any run.
//
// The mechanism is that a run's history already *is* its event log. RunWorkflow
// emits each lifecycle event through the PublishEvent activity, and Temporal
// records the event and the run snapshot as that activity's input when it is
// **scheduled** — before, and independently of, whichever replica later
// executes it. Reading history therefore needs no process to have seen the run:
// not the one that started it, and not the one that projected its events.
//
//	eventId 5  ACTIVITY_TASK_SCHEDULED  PublishEvent  queue ork-engine-events
//	  json/protobuf orkstack.v1.Event  {"runId":"ork-…","sequence":"2",…}
//	  json/protobuf orkstack.v1.Run    {"id":"ork-…","status":"STATUS_RUNNING",…}
//	  json/plain                       "ork-…#2"
//
// The in-process RunStore and EventBus are not consulted here and are not a
// cache in front of this: they are write-side helpers that assemble the archive
// body and dedupe webhook notifications.
type TemporalRunReader struct {
	client    runReadClient
	converter converter.DataConverter
}

// The reader is the archive writer's event source too: PublishEvent assembles
// an archive body from the record rather than from the executing process's own
// bus, which is the only way that body is whole under more than one replica.
var _ RunEventSource = (*TemporalRunReader)(nil)

// NewTemporalRunReader returns a reader over a Temporal client. It reads the
// namespace the client is bound to, which is the hard partition every other
// part of the engine already respects.
func NewTemporalRunReader(c client.Client) *TemporalRunReader {
	return newRunReader(c, converter.GetDefaultDataConverter())
}

func newRunReader(c runReadClient, dc converter.DataConverter) *TemporalRunReader {
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	return &TemporalRunReader{client: c, converter: dc}
}

// GetRun returns a run rebuilt from its history: the latest snapshot the
// workflow published, with a terminal status overlaid from the execution's own
// closing event.
//
// The overlay is what covers a **terminated** run. Termination emits no
// lifecycle event at all — the workflow never runs again — so a reader that
// trusted only the last published snapshot would report that run RUNNING
// forever.
func (r *TemporalRunReader) GetRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, fmt.Errorf("engine: get run: run id is required")
	}
	projection, err := r.scan(ctx, runID, false, 0, nil)
	if err != nil {
		return nil, err
	}
	if projection.run == nil {
		return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
	}
	return projection.run, nil
}

// Events returns a run's whole event stream after fromSequence, in sequence
// order, as Temporal recorded it.
func (r *TemporalRunReader) Events(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	projection, err := r.scan(ctx, runID, false, fromSequence, nil)
	if err != nil {
		return nil, err
	}
	return projection.events, nil
}

// StreamEvents delivers a run's events after fromSequence to send, and ends at
// the run's terminal event.
//
// A live run blocks in the iterator rather than polling: GetWorkflowHistory
// with isLongPoll=true does not finish until the workflow does (SDK v1.47.0
// internal/client.go:186-188), so the iterator *is* the stream.
func (r *TemporalRunReader) StreamEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if send == nil {
		return fmt.Errorf("engine: stream run %s: no consumer", runID)
	}
	_, err := r.scan(ctx, runID, true, fromSequence, send)
	return err
}

// ListRuns returns runs from Temporal's visibility index, newest first.
//
// A listed run is the shallow projection visibility can give: id, orkflow
// (from the memo StartRun writes), status and times. Task runs, input and
// output live in history and in the archive, and GetRun reads them from there.
//
// Filters are applied over the page rather than pushed into the query, because
// the orkflow name lives in a memo and memos are not searchable; making them
// searchable means registering custom search attributes, which is a cluster
// provisioning step this milestone deliberately does not require.
func (r *TemporalRunReader) ListRuns(ctx context.Context, q RunQuery) ([]*orkstackv1.Run, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultRunPageSize
	}
	out := make([]*orkstackv1.Run, 0, limit)
	var token []byte
	for page := 0; page < maxRunListPages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         fmt.Sprintf("WorkflowType = %q", RunWorkflowType),
			PageSize:      int32(limit),
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("engine: list runs from temporal: %w", err)
		}
		for _, execution := range response.GetExecutions() {
			run := r.listedRun(execution)
			if !matchesRunQuery(run, q) {
				continue
			}
			out = append(out, run)
			if len(out) >= limit {
				return out, nil
			}
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
	return out, nil
}

// runProjection is one run's history, decoded.
type runProjection struct {
	run    *orkstackv1.Run
	events []*orkstackv1.Event
}

// scan walks a run's history once and decodes it.
//
// With send set the scan is a stream: each event after fromSequence is handed
// over as it is decoded and the walk stops at the run's terminal event. With
// send nil the whole projection is collected, and the walk continues past the
// terminal event to the execution's closing event, which is the only place a
// terminated run's outcome is recorded.
func (r *TemporalRunReader) scan(
	ctx context.Context,
	runID string,
	follow bool,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) (*runProjection, error) {
	iter := r.client.GetWorkflowHistory(ctx, runID, "", follow,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	projection := &runProjection{}
	var (
		seen         = map[int64]struct{}{}
		lastSequence int64
		read         int
	)
	for iter.HasNext() {
		historyEvent, err := iter.Next()
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) && read == 0 {
				// Nothing has been decoded or delivered yet, so the caller may
				// still fall back to the archive without duplicating anything.
				return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
			}
			return nil, fmt.Errorf("engine: read history of run %s: %w", runID, err)
		}
		read++

		if attrs := historyEvent.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			projection.run = mergeRunSnapshot(projection.run, r.startedRun(runID, historyEvent, attrs))
			continue
		}
		if r.isPublishEvent(historyEvent) {
			event, run, err := r.decodePublishEvent(runID, historyEvent)
			if err != nil {
				return nil, err
			}
			projection.run = mergeRunSnapshot(projection.run, run)
			if event == nil {
				continue
			}
			if _, duplicate := seen[event.GetSequence()]; duplicate {
				continue
			}
			seen[event.GetSequence()] = struct{}{}
			if event.GetSequence() > lastSequence {
				lastSequence = event.GetSequence()
			}
			if event.GetSequence() <= fromSequence {
				// The cursor is already past this event. A terminal one still
				// ends the stream: the caller has seen it.
				if send != nil && isTerminalRunEvent(event) {
					return projection, nil
				}
				continue
			}
			if send != nil {
				if err := send(event); err != nil {
					return nil, err
				}
			} else {
				projection.events = append(projection.events, event)
			}
			if isTerminalRunEvent(event) && send != nil {
				return projection, nil
			}
			continue
		}
		if status, cause, closed := closedExecution(historyEvent); closed {
			terminal := r.closeRun(projection, runID, historyEvent, status, cause, lastSequence)
			if terminal == nil || terminal.GetSequence() <= fromSequence {
				continue
			}
			if send != nil {
				if err := send(terminal); err != nil {
					return nil, err
				}
				return projection, nil
			}
			projection.events = append(projection.events, terminal)
		}
	}
	if projection.run == nil && len(projection.events) == 0 {
		return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
	}
	return projection, nil
}

// closeRun overlays the outcome the execution itself reports, and returns a
// terminal event when the run produced none of its own.
//
// A run that failed or was canceled emits its terminal event first and closes
// afterwards, so the published snapshot is already terminal and richer than
// anything the closing event carries: it is kept. A run that was **terminated**
// emits nothing, and this is the only record of its end — including the
// terminal event, synthesized here so the stream contract still holds.
func (r *TemporalRunReader) closeRun(
	projection *runProjection,
	runID string,
	historyEvent *historypb.HistoryEvent,
	status orkstackv1.Status,
	cause *orkstackv1.Error,
	lastSequence int64,
) *orkstackv1.Event {
	closedAt := cloneTime(historyEvent.GetEventTime())
	if projection.run == nil {
		projection.run = &orkstackv1.Run{Id: runID}
	}
	if isTerminal(projection.run.GetStatus()) {
		if projection.run.GetFinishedAt() == nil {
			projection.run.FinishedAt = closedAt
		}
		return nil
	}
	projection.run.Status = status
	projection.run.FinishedAt = closedAt
	if projection.run.GetError() == nil {
		projection.run.Error = cause
	}
	message := "run ended without a lifecycle event"
	if cause.GetMessage() != "" {
		message = cause.GetMessage()
	}
	return &orkstackv1.Event{
		RunId:     runID,
		Sequence:  lastSequence + 1,
		Type:      terminalEventFor(status),
		Status:    status,
		Message:   message,
		Timestamp: closedAt,
	}
}

// isPublishEvent reports whether a history event is one of the run's own
// lifecycle events, recorded as the scheduling of the projection activity.
func (r *TemporalRunReader) isPublishEvent(historyEvent *historypb.HistoryEvent) bool {
	attrs := historyEvent.GetActivityTaskScheduledEventAttributes()
	return attrs != nil && attrs.GetActivityType().GetName() == ActivityPublishEvent
}

// decodePublishEvent decodes the (Event, Run, dedupeKey) arguments RunWorkflow
// passed to the projection activity.
func (r *TemporalRunReader) decodePublishEvent(
	runID string,
	historyEvent *historypb.HistoryEvent,
) (*orkstackv1.Event, *orkstackv1.Run, error) {
	payloads := historyEvent.GetActivityTaskScheduledEventAttributes().GetInput().GetPayloads()
	var (
		event *orkstackv1.Event
		run   *orkstackv1.Run
	)
	if len(payloads) > 0 {
		if err := r.converter.FromPayload(payloads[0], &event); err != nil {
			return nil, nil, fmt.Errorf("engine: decode event of run %s at history event %d: %w",
				runID, historyEvent.GetEventId(), err)
		}
	}
	if len(payloads) > 1 {
		if err := r.converter.FromPayload(payloads[1], &run); err != nil {
			return nil, nil, fmt.Errorf("engine: decode run snapshot of run %s at history event %d: %w",
				runID, historyEvent.GetEventId(), err)
		}
	}
	return event, run, nil
}

// startedRun is the run as it exists before its first lifecycle event: the
// arguments the workflow was started with, dated by the execution's own start
// time. It closes the window between a submission and the first PublishEvent
// reaching history, so a read taken immediately after SubmitRun still answers.
func (r *TemporalRunReader) startedRun(
	runID string,
	historyEvent *historypb.HistoryEvent,
	attrs *historypb.WorkflowExecutionStartedEventAttributes,
) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:        runID,
		Status:    orkstackv1.Status_STATUS_PENDING,
		CreatedAt: cloneTime(historyEvent.GetEventTime()),
	}
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) > 1 {
		var wf *orkstackv1.Orkflow
		if err := r.converter.FromPayload(payloads[1], &wf); err == nil && wf != nil {
			run.OrkflowName = wf.GetName()
			run.OrkflowVersion = wf.GetVersion()
			run.Namespace = wf.GetNamespace()
		}
	}
	if len(payloads) > 2 {
		var input *orkstackv1.Payload
		if err := r.converter.FromPayload(payloads[2], &input); err == nil {
			run.Input = input
		}
	}
	return run
}

// listedRun projects one visibility record onto a Run.
func (r *TemporalRunReader) listedRun(info *workflowpb.WorkflowExecutionInfo) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:        info.GetExecution().GetWorkflowId(),
		Status:    statusFromExecution(info.GetStatus()),
		CreatedAt: cloneTime(info.GetStartTime()),
	}
	if info.GetCloseTime() != nil {
		run.FinishedAt = cloneTime(info.GetCloseTime())
	}
	run.OrkflowName = r.memoString(info.GetMemo(), MemoOrkflowName)
	run.OrkflowVersion = r.memoString(info.GetMemo(), MemoOrkflowVersion)
	run.Namespace = r.memoString(info.GetMemo(), MemoNamespace)
	return run
}

// memoString reads one memo field. A run started by an engine that predates
// the memo simply has none, which is a missing name rather than an error.
func (r *TemporalRunReader) memoString(memo *commonpb.Memo, key string) string {
	payload, ok := memo.GetFields()[key]
	if !ok {
		return ""
	}
	var value string
	if err := r.converter.FromPayload(payload, &value); err != nil {
		return ""
	}
	return value
}

// matchesRunQuery applies the client-side half of a list query.
func matchesRunQuery(run *orkstackv1.Run, q RunQuery) bool {
	if q.Namespace != "" && artifacts.ResolveNamespace(run.GetNamespace()) != artifacts.ResolveNamespace(q.Namespace) {
		return false
	}
	if q.OrkflowName != "" && run.GetOrkflowName() != q.OrkflowName {
		return false
	}
	if q.Status != orkstackv1.Status_STATUS_UNSPECIFIED && run.GetStatus() != q.Status {
		return false
	}
	return true
}

// closedExecution maps a history event that closes an execution onto the run
// status it means.
func closedExecution(
	historyEvent *historypb.HistoryEvent,
) (orkstackv1.Status, *orkstackv1.Error, bool) {
	switch historyEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return orkstackv1.Status_STATUS_SUCCEEDED, nil, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := historyEvent.GetWorkflowExecutionFailedEventAttributes()
		return orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{
			Code:    "RUN_FAILED",
			Message: attrs.GetFailure().GetMessage(),
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{
			Code:    "RUN_CANCELED",
			Message: "run canceled",
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		attrs := historyEvent.GetWorkflowExecutionTerminatedEventAttributes()
		message := attrs.GetReason()
		if message == "" {
			message = "run terminated"
		}
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{
			Code:    "RUN_TERMINATED",
			Message: message,
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{
			Code:    "RUN_TIMED_OUT",
			Message: "run timed out",
		}, true
	default:
		return orkstackv1.Status_STATUS_UNSPECIFIED, nil, false
	}
}

// statusFromExecution maps a visibility status onto a run status.
func statusFromExecution(status enumspb.WorkflowExecutionStatus) orkstackv1.Status {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return orkstackv1.Status_STATUS_SUCCEEDED
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return orkstackv1.Status_STATUS_FAILED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return orkstackv1.Status_STATUS_CANCELED
	default:
		return orkstackv1.Status_STATUS_RUNNING
	}
}

// terminalEventFor is the event type that ends a stream for a given outcome.
func terminalEventFor(status orkstackv1.Status) orkstackv1.EventType {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED:
		return orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	case orkstackv1.Status_STATUS_CANCELED:
		return orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
	default:
		return orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	}
}

// mergeRunSnapshot folds a newer snapshot over an older one. The workflow owns
// everything it publishes; the fields it can omit are filled from what came
// before, so a late snapshot never erases the run's identity.
func mergeRunSnapshot(base, next *orkstackv1.Run) *orkstackv1.Run {
	if next == nil {
		return base
	}
	if base == nil {
		return next
	}
	if next.GetCreatedAt() == nil {
		next.CreatedAt = base.GetCreatedAt()
	}
	if next.GetInput() == nil {
		next.Input = base.GetInput()
	}
	if next.GetOrkflowName() == "" {
		next.OrkflowName = base.GetOrkflowName()
	}
	if next.GetOrkflowVersion() == "" {
		next.OrkflowVersion = base.GetOrkflowVersion()
	}
	return next
}

// cloneTime copies a timestamp out of a history event, so nothing the reader
// returns aliases the protobuf it decoded.
func cloneTime(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(ts.AsTime())
}
