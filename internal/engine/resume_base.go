package engine

// The resume planner's history scan (E85, PR 3): what a base execution's own
// history says about where a resume restarts it and what it dispatched.
//
// ScanResumeBase is pure: it reads a slice of history events in order and
// decodes the activity inputs the workflow scheduled, with the engine's data
// converter. It reads no store and no cluster; the caller fetches the
// execution's history once (a closed base's history is immutable).

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// runStartedSequence is RUN_STARTED's sequence in generation 0: event 1 is
// RUN_CREATED (workflow.go).
const runStartedSequence = 2

// resumeDispatchTypes are the activity types a task's attempt is scheduled
// as. ExecuteResumedTask, a rerun's attempt on the pool's rerun channel,
// lands with the resume engine (PR 2); a base resumed before holds it.
var resumeDispatchTypes = map[string]bool{
	task.ActivityExecuteTask: true,
	"ExecuteResumedTask":     true,
}

// ResumeDispatch is one element of D: a task attempt the base scheduled.
type ResumeDispatch struct {
	Unit ResumeUnit
	// ScheduledEventID is the scheduling event's id.
	ScheduledEventID int64
	// HeartbeatTimeout is the scheduled event's own; Grace is the task's
	// cancellation grace from the same input.
	HeartbeatTimeout time.Duration
	Grace            time.Duration
	// Open: no completion, failure, timeout or cancellation follows it.
	Open bool
}

// ResumeHistory is what ScanResumeBase found.
type ResumeHistory struct {
	ResetPoint ResetPoint
	// Continued: the execution started from a carry, so it is a continued
	// generation.
	Continued bool
	// Dispatches is D in scheduling order; Dispatched indexes it by unit.
	Dispatches []ResumeDispatch
	Dispatched map[ResumeUnit]bool
	// Published names the tasks this execution published a task event for.
	Published map[string]bool
	// CloseTime is the closing event's time. A base is always closed: a
	// history without a closing event is refused as truncated.
	CloseTime time.Time
}

// ScanResumeBase scans one base execution's history, oldest event first.
// It finds R (design §3.2): in generation 0 the first WorkflowTaskCompleted
// after the RUN_STARTED publish completed; in a continued generation its
// first WorkflowTaskCompleted. Failed and timed-out workflow tasks, a
// reset's included, are never R.
func ScanResumeBase(events []*historypb.HistoryEvent, dc converter.DataConverter) (*ResumeHistory, error) {
	if dc == nil {
		dc = DataConverter()
	}
	if len(events) == 0 {
		return nil, errors.New("engine: scan resume base: the history is empty")
	}
	started := events[0].GetWorkflowExecutionStartedEventAttributes()
	if started == nil {
		return nil, fmt.Errorf("engine: scan resume base: event %d is not the execution's start", events[0].GetEventId())
	}
	continued, err := startCarries(dc, started)
	if err != nil {
		return nil, err
	}
	if continued && started.GetContinuedExecutionRunId() == "" {
		return nil, errors.New("engine: scan resume base: the start carries saved state but names no predecessor")
	}
	h := &ResumeHistory{
		Continued:  continued,
		Dispatched: map[ResumeUnit]bool{},
		Published:  map[string]bool{},
	}
	// Generation 0 waits for its RUN_STARTED publish: the activity whose
	// input is that event, then its completion.
	var runStartedScheduled int64
	runStartedDone := h.Continued
	open := map[int64]int{} // scheduled event id -> index in Dispatches
	last := int64(0)
	for _, ev := range events {
		if id := ev.GetEventId(); id != last+1 {
			return nil, fmt.Errorf("engine: scan resume base: event %d after event %d: the history is not whole", id, last)
		}
		last = ev.GetEventId()
		if !h.CloseTime.IsZero() {
			return nil, fmt.Errorf("engine: scan resume base: event %d follows the execution's close", last)
		}
		switch ev.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attrs := ev.GetActivityTaskScheduledEventAttributes()
			name := attrs.GetActivityType().GetName()
			switch {
			case name == ActivityPublishEvent:
				event, err := decodePublished(dc, ev)
				if err != nil {
					return nil, err
				}
				if event.GetType() == orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED {
					return nil, fmt.Errorf("engine: scan resume base: the event published at event %d has no type", ev.GetEventId())
				}
				if event.GetTaskId() != "" {
					h.Published[event.GetTaskId()] = true
				}
				if !h.Continued && runStartedScheduled == 0 &&
					event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_STARTED &&
					event.GetSequence() == runStartedSequence {
					runStartedScheduled = ev.GetEventId()
				}
			case resumeDispatchTypes[name]:
				d, err := decodeDispatch(dc, ev)
				if err != nil {
					return nil, err
				}
				open[ev.GetEventId()] = len(h.Dispatches)
				h.Dispatches = append(h.Dispatches, d)
				h.Dispatched[d.Unit] = true
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			sched := activityScheduledID(ev)
			if sched <= 0 || sched >= ev.GetEventId() {
				return nil, fmt.Errorf("engine: scan resume base: activity close at event %d names scheduling event %d",
					ev.GetEventId(), sched)
			}
			if i, ok := open[sched]; ok {
				h.Dispatches[i].Open = false
				delete(open, sched)
			}
			if runStartedScheduled != 0 && sched == runStartedScheduled &&
				ev.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
				runStartedDone = true
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			if runStartedDone && !h.ResetPoint.Found() {
				attrs := ev.GetWorkflowTaskCompletedEventAttributes()
				h.ResetPoint = ResetPoint{
					CompletedEventID: ev.GetEventId(),
					ScheduledEventID: attrs.GetScheduledEventId(),
					StartedEventID:   attrs.GetStartedEventId(),
				}
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			if ev.GetEventTime() == nil {
				return nil, fmt.Errorf("engine: scan resume base: the closing event %d has no time", ev.GetEventId())
			}
			h.CloseTime = ev.GetEventTime().AsTime()
		}
	}
	if h.CloseTime.IsZero() {
		return nil, fmt.Errorf("engine: scan resume base: the history ends at event %d without the execution's close", last)
	}
	return h, nil
}

// startCarries reports whether an execution's start carries a continuation:
// its fifth argument decodes to a RunContinuation, exactly as the reader's
// decodeCarry reads it (reader_chain.go).
func startCarries(dc converter.DataConverter, attrs *historypb.WorkflowExecutionStartedEventAttributes) (bool, error) {
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 5 {
		return false, nil
	}
	var carry *orkstackv1.RunContinuation
	if err := dc.FromPayload(payloads[4], &carry); err != nil {
		return false, fmt.Errorf("engine: scan resume base: decode the saved-state reference of its start: %w", err)
	}
	return carry != nil, nil
}

func activityScheduledID(ev *historypb.HistoryEvent) int64 {
	switch {
	case ev.GetActivityTaskCompletedEventAttributes() != nil:
		return ev.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
	case ev.GetActivityTaskFailedEventAttributes() != nil:
		return ev.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
	case ev.GetActivityTaskTimedOutEventAttributes() != nil:
		return ev.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
	case ev.GetActivityTaskCanceledEventAttributes() != nil:
		return ev.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()
	}
	return 0
}

func decodePublished(dc converter.DataConverter, ev *historypb.HistoryEvent) (*orkstackv1.Event, error) {
	payloads := ev.GetActivityTaskScheduledEventAttributes().GetInput().GetPayloads()
	if len(payloads) == 0 {
		return nil, fmt.Errorf("engine: scan resume base: publish at event %d has no input", ev.GetEventId())
	}
	var event *orkstackv1.Event
	if err := dc.FromPayload(payloads[0], &event); err != nil {
		return nil, fmt.Errorf("engine: scan resume base: decode the event published at event %d: %w", ev.GetEventId(), err)
	}
	return event, nil
}

func decodeDispatch(dc converter.DataConverter, ev *historypb.HistoryEvent) (ResumeDispatch, error) {
	attrs := ev.GetActivityTaskScheduledEventAttributes()
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) == 0 {
		return ResumeDispatch{}, fmt.Errorf("engine: scan resume base: task attempt at event %d has no input", ev.GetEventId())
	}
	var in *orkstackv1.ExecuteTaskInput
	if err := dc.FromPayload(payloads[0], &in); err != nil {
		return ResumeDispatch{}, fmt.Errorf("engine: scan resume base: decode the task input at event %d: %w", ev.GetEventId(), err)
	}
	if in.GetTaskId() == "" {
		return ResumeDispatch{}, fmt.Errorf("engine: scan resume base: the task input at event %d names no task", ev.GetEventId())
	}
	u := nodeUnit(in.GetTaskId())
	if each := in.GetEach(); each != nil {
		if each.GetIndex() < 0 {
			return ResumeDispatch{}, fmt.Errorf("engine: scan resume base: the task input at event %d has item %d", ev.GetEventId(), each.GetIndex())
		}
		u.Item = int(each.GetIndex())
	}
	return ResumeDispatch{
		Unit:             u,
		ScheduledEventID: ev.GetEventId(),
		HeartbeatTimeout: attrs.GetHeartbeatTimeout().AsDuration(),
		Grace:            time.Duration(in.GetCancellationGracePeriodSeconds()) * time.Second,
		Open:             true,
	}, nil
}

// Facts turns a scan into the planner's history facts for a base whose
// terminal state is run. It leaves OpenPins unset: a force-cancelled base's
// caller sets it from OpenPinsAt, and the planner refuses to plan without. In a continued generation a task that ended but
// published nothing in it ended in an earlier generation.
//
// A task's ending is published by the generation that ended it (a canceled
// row by its TASK_LOG), so a terminal row with no publish here was carried
// in from the checkpoint.
func (h *ResumeHistory) Facts(run *orkstackv1.Run) (ResumeBaseFacts, error) {
	if run == nil {
		return ResumeBaseFacts{}, errors.New("engine: resume facts: no base state")
	}
	f := ResumeBaseFacts{
		ResetPoint: h.ResetPoint,
		Continued:  h.Continued,
		Dispatched: h.Dispatched,
		Earlier:    map[string]bool{},
	}
	if h.Continued {
		for _, r := range run.GetTaskRuns() {
			if isTerminalStatus(r.GetStatus()) && !h.Published[r.GetTaskId()] {
				f.Earlier[r.GetTaskId()] = true
			}
		}
	}
	return f, nil
}

// OpenPinsAt names the pins a force cancel caught open: a pin with any
// member whose row was not terminal, or whose attempt is still open in D
// (design §23.6, G-d). A pin whose members had all ended needs no rerun:
// their results are committed, so a session still closing changes nothing.
func OpenPinsAt(def *orkstackv1.Orkflow, run *orkstackv1.Run, h *ResumeHistory) map[string]bool {
	rows := map[string]*orkstackv1.TaskRun{}
	for _, r := range run.GetTaskRuns() {
		rows[r.GetTaskId()] = r
	}
	openD := map[string]bool{}
	for _, d := range h.Dispatches {
		if d.Open {
			openD[d.Unit.Task] = true
		}
	}
	out := map[string]bool{}
	for _, t := range def.GetTasks() {
		pin := t.GetPin()
		if pin == "" {
			continue
		}
		r := rows[t.GetId()]
		if r == nil || !isTerminalStatus(r.GetStatus()) || openD[t.GetId()] {
			out[pin] = true
		}
	}
	return out
}

func isTerminalStatus(s orkstackv1.Status) bool {
	switch s {
	case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED, orkstackv1.Status_STATUS_CANCELED:
		return true
	}
	return false
}
