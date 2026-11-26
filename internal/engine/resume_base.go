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
	"maps"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// runStartedSequence is RUN_STARTED's sequence in generation 0 of a run that
// started at once: event 1 is RUN_CREATED (workflow.go), and a run that waited
// in line first published RUN_QUEUED between them.
const runStartedSequence = 2

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
	// Completed: its Orker reported a result, so its container has exited.
	Completed bool
	// ClosedAt is when it closed; zero while Open.
	ClosedAt time.Time
}

// ResumeChild is a child run the base asked to start.
type ResumeChild struct {
	RunID            string
	InitiatedEventID int64
	// Closed: the base saw it end, or saw that it never started.
	Closed bool
}

// ResumeHistory is what ScanResumeBase found.
type ResumeHistory struct {
	ResetPoint ResetPoint
	// Continued: the execution started from a carry, so it is a continued
	// generation.
	Continued bool
	// Carry is the saved state a continued generation started from; nil for
	// generation 0. EndedBefore names the tasks that had ended in it
	// (ReadSavedState).
	Carry       *orkstackv1.RunContinuation
	EndedBefore map[string]bool
	// Dispatches is D in scheduling order; Dispatched indexes it by unit.
	Dispatches []ResumeDispatch
	Dispatched map[ResumeUnit]bool
	// Published names the tasks this execution published a task event for.
	Published map[string]bool
	// MaxPublishedSequence is the highest sequence of an event this execution
	// scheduled a publish of.
	MaxPublishedSequence int64
	// Children is the child runs it asked to start, in order.
	Children []ResumeChild
	// PrefixBytes is the size of the events a reset to R keeps, through R's
	// completion; zero without an R.
	PrefixBytes int64
	// WorkflowTaskTimeout is the run's own, from its start.
	WorkflowTaskTimeout time.Duration
	// CloseTime is the closing event's time. A base is always closed: a
	// history without a closing event is refused as truncated.
	CloseTime time.Time
	// CloseEvent is the closing event: a force-cancelled base's carries the
	// record that ended it.
	CloseEvent *historypb.HistoryEvent
	// NeverRan: it ended as a resume whose plan could not be run, so it
	// dispatched nothing past its reset point, which ResetFrom, the execution
	// it was reset from, did (AddEarlier).
	NeverRan  bool
	ResetFrom string
	// DispatchedBefore is what the executions it was reset from dispatched.
	DispatchedBefore map[ResumeUnit]bool
	// resolve is what the resolve scan reads of the history after R
	// (ResolutionsAfterR).
	resolve *resumeResolveTrace
}

// AddEarlier takes what earlier, the history h's execution was reset from,
// dispatched: a resume whose plan could not be run dispatched nothing itself.
func (h *ResumeHistory) AddEarlier(earlier *ResumeHistory) {
	if h.DispatchedBefore == nil {
		h.DispatchedBefore = map[ResumeUnit]bool{}
	}
	maps.Copy(h.DispatchedBefore, earlier.Dispatched)
	maps.Copy(h.DispatchedBefore, earlier.DispatchedBefore)
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
	carry, err := startCarry(dc, started)
	if err != nil {
		return nil, err
	}
	continued := carry != nil
	if continued && started.GetContinuedExecutionRunId() == "" {
		return nil, errors.New("engine: scan resume base: the start carries saved state but names no predecessor")
	}
	h := &ResumeHistory{
		Continued:  continued,
		Carry:      carry,
		Dispatched: map[ResumeUnit]bool{},
		Published:  map[string]bool{},

		WorkflowTaskTimeout: started.GetWorkflowTaskTimeout().AsDuration(),

		resolve: &resumeResolveTrace{awaits: map[string]*resumeAwaitTrace{}, started: map[int64]int{}, signals: map[string][]resumeSignal{},
			taskStarted: map[int64]time.Time{}, batchStarted: map[int64]int64{}},
	}
	// Generation 0 waits for its RUN_STARTED publish: the activity whose
	// input is that event, then its completion.
	var runStartedScheduled int64
	startedAt := int64(runStartedSequence)
	runStartedDone := h.Continued
	open := map[int64]int{}     // scheduled event id -> index in Dispatches
	children := map[int64]int{} // initiated event id -> index in Children
	last := int64(0)
	var closeCode string
	for _, ev := range events {
		if !h.ResetPoint.Found() {
			h.PrefixBytes += int64(proto.Size(ev))
		}
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
				h.MaxPublishedSequence = max(h.MaxPublishedSequence, event.GetSequence())
				if event.GetTaskId() != "" {
					h.Published[event.GetTaskId()] = true
				}
				if h.ResetPoint.Found() {
					h.resolve.published(event, attrs.GetWorkflowTaskCompletedEventId())
				}
				switch {
				case h.Continued || runStartedScheduled != 0:
				case event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED && event.GetSequence() == startedAt:
					// It waited in line for its concurrency slot first.
					startedAt++
				case event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_STARTED && event.GetSequence() == startedAt:
					runStartedScheduled = ev.GetEventId()
				}
			case name == task.ActivityExecuteTask:
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
				if ev.GetEventTime() == nil {
					return nil, fmt.Errorf("engine: scan resume base: the activity close at event %d has no time", ev.GetEventId())
				}
				d := &h.Dispatches[i]
				d.Open, d.ClosedAt = false, ev.GetEventTime().AsTime()
				d.Completed = ev.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
				delete(open, sched)
			}
			if runStartedScheduled != 0 && sched == runStartedScheduled &&
				ev.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
				runStartedDone = true
			}
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			id := ev.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId()
			if id == "" {
				return nil, fmt.Errorf("engine: scan resume base: the child run started at event %d has no id", ev.GetEventId())
			}
			children[ev.GetEventId()] = len(h.Children)
			h.Children = append(h.Children, ResumeChild{RunID: id, InitiatedEventID: ev.GetEventId()})
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			i, ok := children[childInitiatedID(ev)]
			if !ok {
				return nil, fmt.Errorf("engine: scan resume base: the child run's end at event %d names no start", ev.GetEventId())
			}
			h.Children[i].Closed = true
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			h.resolve.workflowTask(ev)
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
			h.CloseEvent = ev
			failure := ev.GetWorkflowExecutionFailedEventAttributes().GetFailure()
			closeCode = failure.GetApplicationFailureInfo().GetType()
		case enumspb.EVENT_TYPE_TIMER_STARTED, enumspb.EVENT_TYPE_TIMER_FIRED, enumspb.EVENT_TYPE_TIMER_CANCELED:
			if h.ResetPoint.Found() {
				h.resolve.timer(ev)
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			h.resolve.workflowTask(ev)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			h.resolve.cancel(ev)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			if h.ResetPoint.Found() {
				h.resolve.signal(dc, ev)
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			// The newest reset is the one that made this execution.
			if a := ev.GetWorkflowTaskFailedEventAttributes(); a.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
				h.ResetFrom = a.GetBaseRunId()
			}
		}
	}
	h.NeverRan = ResumePlanNeverRan(closeCode)
	if !h.ResetPoint.Found() {
		h.PrefixBytes = 0
	}
	if h.CloseTime.IsZero() {
		return nil, fmt.Errorf("engine: scan resume base: the history ends at event %d without the execution's close", last)
	}
	return h, nil
}

// startCarry is the continuation an execution's start carries, nil for none:
// its fifth argument decodes to a RunContinuation, exactly as the reader's
// decodeCarry reads it (reader_chain.go).
func startCarry(dc converter.DataConverter, attrs *historypb.WorkflowExecutionStartedEventAttributes) (*orkstackv1.RunContinuation, error) {
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 5 {
		return nil, nil
	}
	var carry *orkstackv1.RunContinuation
	if err := dc.FromPayload(payloads[4], &carry); err != nil {
		return nil, fmt.Errorf("engine: scan resume base: decode the saved-state reference of its start: %w", err)
	}
	return carry, nil
}

// ReadSavedState records which tasks had ended in saved, the saved state h's
// execution started from.
func (h *ResumeHistory) ReadSavedState(saved *orkstackv1.Run) {
	h.EndedBefore = map[string]bool{}
	for _, r := range saved.GetTaskRuns() {
		if isTerminalStatus(r.GetStatus()) {
			h.EndedBefore[r.GetTaskId()] = true
		}
	}
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

func childInitiatedID(ev *historypb.HistoryEvent) int64 {
	switch {
	case ev.GetStartChildWorkflowExecutionFailedEventAttributes() != nil:
		return ev.GetStartChildWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()
	case ev.GetChildWorkflowExecutionCompletedEventAttributes() != nil:
		return ev.GetChildWorkflowExecutionCompletedEventAttributes().GetInitiatedEventId()
	case ev.GetChildWorkflowExecutionFailedEventAttributes() != nil:
		return ev.GetChildWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()
	case ev.GetChildWorkflowExecutionCanceledEventAttributes() != nil:
		return ev.GetChildWorkflowExecutionCanceledEventAttributes().GetInitiatedEventId()
	case ev.GetChildWorkflowExecutionTimedOutEventAttributes() != nil:
		return ev.GetChildWorkflowExecutionTimedOutEventAttributes().GetInitiatedEventId()
	case ev.GetChildWorkflowExecutionTerminatedEventAttributes() != nil:
		return ev.GetChildWorkflowExecutionTerminatedEventAttributes().GetInitiatedEventId()
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
// caller sets it from OpenPinsAt, and the planner refuses to plan without. In a
// continued generation, what had ended in the saved state it started from
// (ReadSavedState) ended in an earlier one: never what it published, because
// a task a resume keeps publishes nothing.
func (h *ResumeHistory) Facts(run *orkstackv1.Run) (ResumeBaseFacts, error) {
	if run == nil {
		return ResumeBaseFacts{}, errors.New("engine: resume facts: no base state")
	}
	f := ResumeBaseFacts{
		ResetPoint: h.ResetPoint,
		Continued:  h.Continued,
		Dispatched: h.Dispatched,
		Earlier:    map[string]bool{},
		Resolved:   h.ResolutionsAfterR(run),
	}
	if h.NeverRan || ResumePlanNeverRan(run.GetError().GetCode()) {
		// This execution dispatched nothing: its rows are those of the run it
		// was to resume, whose own execution dispatched them (AddEarlier). A
		// row with an attempt was dispatched there too, but a failed task's
		// row holds none, so the rows alone never say what was.
		f.Dispatched = maps.Clone(h.Dispatched)
		maps.Copy(f.Dispatched, h.DispatchedBefore)
		for _, r := range run.GetTaskRuns() {
			if r.GetAttempt() > 0 {
				f.Dispatched[nodeUnit(r.GetTaskId())] = true
			}
			for _, it := range r.GetItems() {
				if it.GetAttempt() > 0 {
					f.Dispatched[ResumeUnit{Task: r.GetTaskId(), Item: int(it.GetItem())}] = true
				}
			}
		}
	}
	if h.Continued {
		if h.EndedBefore == nil {
			return ResumeBaseFacts{}, errors.New("engine: resume facts: the saved state the run carried on from was not read")
		}
		f.Earlier = maps.Clone(h.EndedBefore)
	}
	return f, nil
}

// resumeStopMargin is added to when the base's containers must have stopped.
const resumeStopMargin = 30 * time.Second

// ContainersStoppedBy is the instant after which no live Orker still runs a
// container of the base (design §25.2): each attempt's Orker learns that its
// activity is over within a heartbeat timeout and stops the container within
// its grace. An attempt that reported its result had already exited. It says
// nothing of an Orker that died, whose attempts ResumeRun asks about.
func (h *ResumeHistory) ContainersStoppedBy() time.Time {
	bound := h.CloseTime
	for _, d := range h.Dispatches {
		if d.Completed {
			continue
		}
		ref := d.ClosedAt
		if d.Open {
			ref = h.CloseTime
		}
		if t := ref.Add(d.HeartbeatTimeout + d.Grace); t.After(bound) {
			bound = t
		}
	}
	return bound.Add(resumeStopMargin)
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
