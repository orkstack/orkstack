package engine

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// resumeAwaitTrace is what a base's history after R says of one await it
// opened: the workflow task whose batch published its TASK_AWAITING, and
// whether a later publish closed it (design §26.5).
type resumeAwaitTrace struct {
	opened int64
	closed bool
}

// resumeTimer is one timer a base started after R: the batch it was issued
// in, its started event, the event that ended it (fired or cancelled; 0
// while it stands), and how long it was to run.
type resumeTimer struct {
	batch, started, ended int64
	timeout               time.Duration
}

// resumeSignal is one await-resolution Signal a base received after R.
type resumeSignal struct {
	event int64
	at    time.Time
	sig   *orkstackv1.AwaitSignal
}

// resumeResolveTrace is the part of a base's history after R that the resolve
// scan reads: the awaits it opened, its timers and its resolution Signals.
type resumeResolveTrace struct {
	awaits  map[string]*resumeAwaitTrace
	timers  []resumeTimer
	started map[int64]int // a timer's started event -> its index
	signals map[string][]resumeSignal
	// taskStarted is each workflow task's start time, by its started event,
	// and batchStarted each completed workflow task's started event: an
	// await's deadline is its opening task's start (workflow.Now) plus its
	// timeout.
	taskStarted  map[int64]time.Time
	batchStarted map[int64]int64
	// cancelRequested is the first cancel request's event: no Signal after
	// it is one the run applied, since it ends its awaits first (0: none).
	cancelRequested int64
	// unreadable is the first resolution Signal that did not decode (0:
	// none): which await it was for, and whether the run read it, is not
	// known, so no Signal after it settles any await.
	unreadable int64
}

// workflowTask notes a workflow task's start, or which start a completed one
// was of.
func (r *resumeResolveTrace) workflowTask(ev *historypb.HistoryEvent) {
	if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
		r.taskStarted[ev.GetEventId()] = ev.GetEventTime().AsTime()
		return
	}
	if a := ev.GetWorkflowTaskCompletedEventAttributes(); a != nil {
		r.batchStarted[ev.GetEventId()] = a.GetStartedEventId()
	}
}

// cancel notes the run's first cancel request.
func (r *resumeResolveTrace) cancel(ev *historypb.HistoryEvent) {
	if r.cancelRequested == 0 {
		r.cancelRequested = ev.GetEventId()
	}
}

// published notes a publish after R: an await opened by its TASK_AWAITING,
// and closed by any later event of its task that ends it.
func (r *resumeResolveTrace) published(event *orkstackv1.Event, batch int64) {
	id := event.GetTaskId()
	if id == "" {
		return
	}
	if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING {
		r.awaits[id] = &resumeAwaitTrace{opened: batch}
		return
	}
	if a := r.awaits[id]; a != nil && isEndedStatus(event.GetStatus()) {
		a.closed = true
	}
}

// isEndedStatus is a status a task ends in.
func isEndedStatus(s orkstackv1.Status) bool {
	switch s {
	case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_SKIPPED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	}
	return false
}

// timer notes a timer started or fired after R.
func (r *resumeResolveTrace) timer(ev *historypb.HistoryEvent) {
	if a := ev.GetTimerStartedEventAttributes(); a != nil {
		r.started[ev.GetEventId()] = len(r.timers)
		r.timers = append(r.timers, resumeTimer{batch: a.GetWorkflowTaskCompletedEventId(), started: ev.GetEventId(),
			timeout: a.GetStartToFireTimeout().AsDuration()})
		return
	}
	started := ev.GetTimerFiredEventAttributes().GetStartedEventId()
	if a := ev.GetTimerCanceledEventAttributes(); a != nil {
		started = a.GetStartedEventId()
	}
	if i, ok := r.started[started]; ok && r.timers[i].ended == 0 {
		r.timers[i].ended = ev.GetEventId()
	}
}

// signal notes a resolution Signal received after R. One that does not decode,
// as every Signal of a run whose payloads are sealed does (the scan has no key
// to open them), is noted as unreadable: no Signal after it settles an await.
func (r *resumeResolveTrace) signal(dc converter.DataConverter, ev *historypb.HistoryEvent) {
	a := ev.GetWorkflowExecutionSignaledEventAttributes()
	if a.GetSignalName() != ResolveAwaitSignal {
		return
	}
	payloads := a.GetInput().GetPayloads()
	var sig *orkstackv1.AwaitSignal
	if len(payloads) > 0 && (dc.FromPayload(payloads[0], &sig) != nil || sig.GetTaskId() == "") {
		sig = nil
	}
	if sig == nil {
		if r.unreadable == 0 {
			r.unreadable = ev.GetEventId()
		}
		return
	}
	r.signals[sig.GetTaskId()] = append(r.signals[sig.GetTaskId()], resumeSignal{event: ev.GetEventId(), at: ev.GetEventTime().AsTime(), sig: sig})
}

// ResumeResolved is a resolution of an await the base left awaiting, which
// the base received after R and never applied, as resolve() would have
// applied it (design §24.2, §25.7, §26.5).
type ResumeResolved struct {
	// Answer is what the planner is told: a completion, or a rejection. A
	// completion whose values do not decode fails the await, so it is told as
	// a rejection, and Invalid says why.
	Answer ResolutionAnswer
	// Row is a completion's row as it would have succeeded: its values, who
	// resolved it (asserted: a Signal says it, nothing verified it) and when.
	Row *orkstackv1.TaskRun
	// Invalid is why a completion's values would have failed the await.
	Invalid string
	// By, At and Reason say who answered, when, and a rejection's reason.
	By     string
	At     time.Time
	Reason string
}

// ResolutionsAfterR is, for each await the base's history leaves awaiting,
// the first resolution after R that resolve() would have applied: a Signal
// received after the batch that opened the await, before its deadline's timer
// fired or was cancelled, and before any cancel request of the run (design
// §26.5). The deadline's timer is the one timer of the opening batch whose
// timeout, from that workflow task's start, ends at the deadline the base's
// row holds, to the second (openAwait's workflow.Now plus its timeout). An
// await whose timer cannot be found that way, and one whose opening lies in an
// earlier generation, has none: it waits again. A resolution's values object
// is not read here; verification reads it.
func (h *ResumeHistory) ResolutionsAfterR(base *orkstackv1.Run) map[string]ResumeResolved {
	r := h.resolve
	if r == nil || len(r.signals) == 0 {
		return nil
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, row := range base.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	// Each await's deadline timer: the one timer of its batch that fires at
	// its deadline, to the second, and could be no other open await's, whether
	// or not that one was answered.
	claims := map[int64]int{}
	paired := map[string]resumeTimer{}
	for id, a := range r.awaits {
		deadline := rows[id].GetAwait().GetDeadline()
		// A row that names what resolved it was resolved, whatever the
		// history shows of it.
		applied := rows[id].GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_UNSPECIFIED
		if a.closed || applied || deadline == nil {
			continue
		}
		start, ok := r.taskStarted[r.batchStarted[a.opened]]
		if !ok {
			continue
		}
		var found []resumeTimer
		for _, t := range r.timers {
			if t.batch == a.opened && start.Add(t.timeout).Unix() == deadline.AsTime().Unix() {
				found = append(found, t)
			}
		}
		for _, t := range found {
			claims[t.started]++
		}
		if len(found) == 1 && len(r.signals[id]) > 0 {
			paired[id] = found[0]
		}
	}
	out := map[string]ResumeResolved{}
	for id, t := range paired {
		if claims[t.started] != 1 {
			continue
		}
		opened := r.awaits[id].opened
		for _, s := range r.signals[id] {
			if s.event <= opened || t.ended != 0 && s.event > t.ended || r.cancelRequested != 0 && s.event > r.cancelRequested ||
				r.unreadable != 0 && s.event > r.unreadable {
				continue
			}
			if got, taken := resolvedBy(rows[id], s); taken {
				if got.Answer.Completed || got.Answer.Rejected {
					out[id] = got
				}
				break
			}
		}
	}
	return out
}

// resolvedBy is what resolve() makes of s for an await that is open, and
// whether it takes it: a completion without values it passes over. A
// completion it takes whose row could not be settled (it names nothing that
// resolved it, or a time of it is missing) is taken with no answer: the await
// waits again.
func resolvedBy(row *orkstackv1.TaskRun, s resumeSignal) (ResumeResolved, bool) {
	sig := s.sig
	got := ResumeResolved{By: awaitResolver(sig.GetVia(), sig.GetPrincipal()), At: s.at, Reason: sig.GetReason()}
	if sig.GetReject() {
		got.Answer.Rejected = true
		return got, true
	}
	if sig.GetValues() == nil {
		return ResumeResolved{}, false
	}
	if sig.GetValues().GetSha256() == "" || sig.GetValues().GetSizeBytes() <= 0 {
		// A door names the object it stored by its digest and size; a ref
		// that names neither is no answer the resume can check whole.
		return ResumeResolved{}, true
	}
	if _, err := payloadOf(decodeValues(sig.GetValuesJson())); err != nil {
		got.Answer.Rejected, got.Invalid = true, err.Error()
		return got, true
	}
	v := sig.GetValues()
	got.Answer.Completed = true
	started := row.GetStartedAt()
	if started == nil {
		started = row.GetScheduledAt()
	}
	got.Row = &orkstackv1.TaskRun{
		TaskId: row.GetTaskId(), Status: orkstackv1.Status_STATUS_SUCCEEDED, ResultEpoch: row.GetResultEpoch(),
		ScheduledAt: row.GetScheduledAt(), StartedAt: started, FinishedAt: timestamppb.New(s.at),
		ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: v.GetUri(), Sha256: v.GetSha256(), SizeBytes: v.GetSizeBytes()},
		Await: &orkstackv1.TaskAwait{Deadline: row.GetAwait().GetDeadline(), Callback: row.GetAwait().GetCallback(),
			ResolvedVia: sig.GetVia(), ResolvedBy: sig.GetPrincipal()},
	}
	if recordedRowFault(kindAwait, cutRecordedRow(kindAwait, got.Row)) != "" {
		return ResumeResolved{}, true
	}
	return got, true
}
