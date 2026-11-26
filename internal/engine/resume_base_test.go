package engine

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// historyBuilder writes a base execution's history the way the cluster
// records it, with the engine's data converter for activity inputs.
type historyBuilder struct {
	t      testing.TB
	events []*historypb.HistoryEvent
	wftSch int64
	wftSt  int64
}

func (b *historyBuilder) add(typ enumspb.EventType, set func(*historypb.HistoryEvent)) int64 {
	ev := &historypb.HistoryEvent{
		EventId:   int64(len(b.events) + 1),
		EventType: typ,
		EventTime: timestamppb.New(time.Unix(1_800_000_000+int64(len(b.events)), 0)),
	}
	if set != nil {
		set(ev)
	}
	b.events = append(b.events, ev)
	return ev.EventId
}

func (b *historyBuilder) payloads(args ...any) *commonpb.Payloads {
	p, err := DataConverter().ToPayloads(args...)
	if err != nil {
		b.t.Fatalf("encode: %v", err)
	}
	return p
}

func (b *historyBuilder) start(carry *orkstackv1.RunContinuation) {
	args := []any{"ork-x", &orkstackv1.Orkflow{}, &orkstackv1.Payload{}, &orkstackv1.Payload{}}
	if carry != nil {
		args = append(args, carry)
	}
	in := b.payloads(args...)
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(ev *historypb.HistoryEvent) {
		attrs := &historypb.WorkflowExecutionStartedEventAttributes{Input: in}
		if carry != nil {
			attrs.ContinuedExecutionRunId = "prev"
		}
		ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: attrs,
		}
	})
}

func (b *historyBuilder) closed() {
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
}

func (b *historyBuilder) wft() {
	b.wftSch = b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	b.wftSt = b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nil)
}

func (b *historyBuilder) wftCompleted() int64 {
	sch, st := b.wftSch, b.wftSt
	return b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: sch, StartedEventId: st,
			},
		}
	})
}

func (b *historyBuilder) wftFailed() {
	b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
			},
		}
	})
}

func (b *historyBuilder) schedule(name string, hb time.Duration, args ...any) int64 {
	in := b.payloads(args...)
	return b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType:     &commonpb.ActivityType{Name: name},
				Input:            in,
				HeartbeatTimeout: durationpb.New(hb),
			},
		}
	})
}

func (b *historyBuilder) completed(sched int64) {
	b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: sched},
		}
	})
}

func (b *historyBuilder) failed(sched int64) {
	b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{ScheduledEventId: sched},
		}
	})
}

func (b *historyBuilder) publish(typ orkstackv1.EventType, seq int64, taskID string) int64 {
	return b.schedule(ActivityPublishEvent, 0, &orkstackv1.Event{Type: typ, Sequence: seq, TaskId: taskID}, "snap")
}

func (b *historyBuilder) task(id string, item int32) int64 {
	in := &orkstackv1.ExecuteTaskInput{TaskId: id, CancellationGracePeriodSeconds: 30}
	if item >= 0 {
		in.Each = &orkstackv1.EachContext{Index: item}
	}
	return b.schedule(task.ActivityExecuteTask, 20*time.Second, in)
}

// gen0Prefix writes a generation 0 up to its RUN_STARTED publish's
// completion, with a failed workflow task in between, and returns R.
func gen0Prefix(b *historyBuilder) ResetPoint {
	b.start(nil)
	b.wft()
	b.wftCompleted()
	b.completed(b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, 1, ""))
	b.wft()
	b.wftCompleted()
	b.completed(b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, runStartedSequence, ""))
	b.wft()
	b.wftFailed()
	b.wft()
	sch, st := b.wftSch, b.wftSt
	return ResetPoint{CompletedEventID: b.wftCompleted(), ScheduledEventID: sch, StartedEventID: st}
}

func TestScanResumeBase_Generation0(t *testing.T) {
	b := &historyBuilder{t: t}
	want := gen0Prefix(b)
	a := b.task("a", -1)
	b.publish(orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, 3, "a")
	b.completed(a)
	i0, i1 := b.task("fan", 0), b.task("fan", 1)
	b.failed(i0)
	_ = i1
	b.closed()

	h, err := ScanResumeBase(b.events, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h.ResetPoint != want {
		t.Errorf("R = %+v, want %+v (the failed workflow task is never R)", h.ResetPoint, want)
	}
	if h.Continued {
		t.Error("generation 0 read as continued")
	}
	if len(h.Dispatches) != 3 {
		t.Fatalf("D = %+v", h.Dispatches)
	}
	open := map[ResumeUnit]bool{}
	for _, d := range h.Dispatches {
		open[d.Unit] = d.Open
		if d.HeartbeatTimeout != 20*time.Second || d.Grace != 30*time.Second {
			t.Errorf("%s: HT %v grace %v", d.Unit, d.HeartbeatTimeout, d.Grace)
		}
	}
	if open[nodeUnit("a")] || open[ResumeUnit{"fan", 0}] || !open[ResumeUnit{"fan", 1}] {
		t.Errorf("open = %v, want only fan[1] open", open)
	}
	if !h.Published["a"] || h.CloseTime.IsZero() {
		t.Errorf("published %v, close %v", h.Published, h.CloseTime)
	}
}

// TestScanResumeBase_WhatMayStillRun: when each attempt ended and how, the
// children started and not ended, the highest sequence published, and how
// much history the reset keeps.
func TestScanResumeBase_WhatMayStillRun(t *testing.T) {
	b := &historyBuilder{t: t}
	r := gen0Prefix(b)
	prefix := len(b.events)
	done, timedOut := b.task("a", -1), b.task("b", -1)
	b.completed(done)
	lost := b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{ScheduledEventId: timedOut}}
	})
	b.task("c", -1)
	initiated := func(id string) int64 {
		return b.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, func(ev *historypb.HistoryEvent) {
			ev.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
				StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: id}}
		})
	}
	ended, _, refused := initiated("ork-ended"), initiated("ork-open"), initiated("ork-refused")
	b.add(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: ended}}
	})
	b.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
			StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: refused}}
	})
	b.publish(orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, 41, "b")
	b.publish(orkstackv1.EventType_EVENT_TYPE_TASK_LOG, 40, "b")
	b.closed()
	h, err := ScanResumeBase(b.events, nil)
	if err != nil {
		t.Fatal(err)
	}
	at := func(id int64) time.Time { return b.events[id-1].GetEventTime().AsTime() }
	if d := h.Dispatches[0]; !d.Completed || d.Open {
		t.Errorf("a = %+v, want completed", d)
	}
	if d := h.Dispatches[1]; d.Completed || d.Open || !d.ClosedAt.Equal(at(lost)) {
		t.Errorf("b = %+v, want closed at its timeout, not completed", d)
	}
	if d := h.Dispatches[2]; !d.Open || d.Completed || !d.ClosedAt.IsZero() {
		t.Errorf("c = %+v, want open", d)
	}
	if len(h.Children) != 3 || !h.Children[0].Closed || h.Children[1].Closed || h.Children[1].RunID != "ork-open" || !h.Children[2].Closed {
		t.Errorf("children = %+v, want ork-open alone open", h.Children)
	}
	if h.MaxPublishedSequence != 41 {
		t.Errorf("highest published = %d, want 41", h.MaxPublishedSequence)
	}
	var bytes int64
	for _, ev := range b.events[:r.CompletedEventID] {
		bytes += int64(proto.Size(ev))
	}
	if int64(prefix) != r.CompletedEventID || h.PrefixBytes != bytes {
		t.Errorf("prefix = %d bytes, want the %d of the first %d events", h.PrefixBytes, bytes, r.CompletedEventID)
	}
}

func TestScanResumeBase_NeverStarted(t *testing.T) {
	b := &historyBuilder{t: t}
	b.start(nil)
	b.wft()
	b.wftCompleted()
	b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, runStartedSequence, "")
	b.closed()
	h, err := ScanResumeBase(b.events, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h.ResetPoint.Found() {
		t.Errorf("R = %+v for a run whose RUN_STARTED publish never completed", h.ResetPoint)
	}
}

// TestScanResumeBase_AQueuedRunStarted: a run that waited in line for its
// concurrency slot started at event 3, and its R is found; one that ended in
// line has none, and neither has a RUN_STARTED at a sequence no run gives it.
func TestScanResumeBase_AQueuedRunStarted(t *testing.T) {
	for name, c := range map[string]struct {
		started int64 // RUN_STARTED's sequence; 0 is none
		found   bool
	}{"started": {3, true}, "ended in line": {0, false}, "another sequence": {4, false}} {
		b := &historyBuilder{t: t}
		b.start(nil)
		b.wft()
		b.wftCompleted()
		b.completed(b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, 1, ""))
		b.completed(b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, 2, ""))
		b.wft()
		b.wftCompleted()
		if c.started > 0 {
			b.completed(b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, c.started, ""))
		}
		b.wft()
		r := b.wftCompleted()
		b.closed()
		h, err := ScanResumeBase(b.events, nil)
		if err != nil {
			t.Fatal(err)
		}
		if h.ResetPoint.Found() != c.found || c.found && h.ResetPoint.CompletedEventID != r {
			t.Errorf("%s: R = %+v; want found %v at event %d", name, h.ResetPoint, c.found, r)
		}
	}
}

func TestScanResumeBase_ContinuedGeneration(t *testing.T) {
	b := &historyBuilder{t: t}
	b.start(&orkstackv1.RunContinuation{})
	b.wft()
	b.wftFailed()
	b.wft()
	sch, st := b.wftSch, b.wftSt
	want := ResetPoint{CompletedEventID: b.wftCompleted(), ScheduledEventID: sch, StartedEventID: st}
	b.publish(orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, 40, "late")
	b.closed()
	h, err := ScanResumeBase(b.events, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !h.Continued || h.ResetPoint != want {
		t.Fatalf("continued %v, R %+v, want %+v", h.Continued, h.ResetPoint, want)
	}
	f, err := h.Facts(&orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{prow("old", sOK), prow("late", sFail), prow("next", sPend)}})
	if err != nil {
		t.Fatal(err)
	}
	if !f.Earlier["old"] || f.Earlier["late"] || f.Earlier["next"] {
		t.Errorf("earlier = %v, want only old", f.Earlier)
	}
}

func TestScanResumeBase_RefusesMalformedHistory(t *testing.T) {
	b := &historyBuilder{t: t}
	if _, err := ScanResumeBase(nil, nil); err == nil {
		t.Error("an empty history scanned")
	}
	b.wft()
	if _, err := ScanResumeBase(b.events, nil); err == nil {
		t.Error("a history that does not begin with the start scanned")
	}
	b = &historyBuilder{t: t}
	b.start(nil)
	b.schedule(task.ActivityExecuteTask, 0, &orkstackv1.ExecuteTaskInput{})
	b.closed()
	if _, err := ScanResumeBase(b.events, nil); err == nil {
		t.Error("a task input naming no task scanned")
	}
	cases := map[string]func(*historyBuilder){
		"no close": func(b *historyBuilder) { b.wft() },
		"gap": func(b *historyBuilder) {
			b.wft()
			b.closed()
			b.events[2].EventId = 9
		},
		"after close":     func(b *historyBuilder) { b.closed(); b.wft() },
		"close no time":   func(b *historyBuilder) { b.closed(); b.events[len(b.events)-1].EventTime = nil },
		"untyped publish": func(b *historyBuilder) { b.publish(orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED, 1, ""); b.closed() },
		"orphan close":    func(b *historyBuilder) { b.completed(0); b.closed() },
		"carry, no parent": func(b *historyBuilder) {
			b.events[0].GetWorkflowExecutionStartedEventAttributes().ContinuedExecutionRunId = ""
			b.closed()
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			b := &historyBuilder{t: t}
			b.start(&orkstackv1.RunContinuation{})
			if name != "carry, no parent" {
				b.events[0].GetWorkflowExecutionStartedEventAttributes().ContinuedExecutionRunId = "prev"
			}
			edit(b)
			if _, err := ScanResumeBase(b.events, nil); err == nil {
				t.Error("scanned")
			}
		})
	}
	if _, err := (&ResumeHistory{Continued: true}).Facts(nil); err == nil {
		t.Error("facts of no state")
	}
}

func TestOpenPinsAt(t *testing.T) {
	a, b, c := ptask("a"), ptask("b"), ptask("c")
	a.Pin, b.Pin, c.Pin = "p", "p", "q"
	def := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{a, b, c}}
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{prow("a", sOK), prow("b", sRun), prow("c", sOK)}}
	got := OpenPinsAt(def, run, &ResumeHistory{})
	if !got["p"] || got["q"] {
		t.Errorf("open pins = %v, want only p", got)
	}
	got = OpenPinsAt(def, run, &ResumeHistory{Dispatches: []ResumeDispatch{{Unit: nodeUnit("c"), Open: true}}})
	if !got["q"] {
		t.Errorf("open pins = %v, want q too: c's attempt is open", got)
	}
}

// FuzzScanResumeBase scans generated generation-0 histories: workflow
// tasks that complete, fail or time out in any order around the RUN_STARTED
// publish, and task attempts that close or stay open. R is always a
// WorkflowTaskCompleted after the publish completed, with no completed
// workflow task between the two, and its scheduled and started ids are the
// ones that event names; D holds exactly the attempts scheduled.
func FuzzScanResumeBase(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 3, 1})
	f.Add([]byte{1, 1, 1, 0, 0, 2, 3, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		g := &fuzzBytes{b: data}
		b := &historyBuilder{t: t}
		b.start(nil)
		pub := int64(0)
		scheduled := 0
		var open []int64
		for range 4 + g.n(20) {
			switch g.n(6) {
			case 0:
				b.wft()
				b.wftCompleted()
			case 1:
				b.wft()
				b.wftFailed()
			case 2:
				b.wft()
				b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, nil)
			case 3:
				if pub == 0 {
					pub = b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, runStartedSequence, "")
					b.completed(pub)
				}
			case 4:
				open = append(open, b.task("t", int32(g.n(3))-1))
				scheduled++
			case 5:
				if len(open) > 0 {
					b.completed(open[0])
					open = open[1:]
				}
			}
		}
		b.closed()
		h, err := ScanResumeBase(b.events, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(h.Dispatches) != scheduled {
			t.Fatalf("D has %d attempts, %d were scheduled", len(h.Dispatches), scheduled)
		}
		openN := 0
		for _, d := range h.Dispatches {
			if d.Open {
				openN++
			}
		}
		if openN != len(open) {
			t.Fatalf("%d open in D, %d left open", openN, len(open))
		}
		if !h.ResetPoint.Found() {
			for _, ev := range b.events {
				if pub != 0 && ev.GetEventId() > pub && ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
					t.Fatalf("no R, but event %d completes a workflow task after the publish", ev.GetEventId())
				}
			}
			return
		}
		r := b.events[h.ResetPoint.CompletedEventID-1]
		if r.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED || pub == 0 || r.GetEventId() < pub {
			t.Fatalf("R %d is not a completed workflow task after the publish %d", r.GetEventId(), pub)
		}
		attrs := r.GetWorkflowTaskCompletedEventAttributes()
		if attrs.GetScheduledEventId() != h.ResetPoint.ScheduledEventID || attrs.GetStartedEventId() != h.ResetPoint.StartedEventID {
			t.Fatalf("R's ids %+v differ from its event's", h.ResetPoint)
		}
		for _, ev := range b.events[pub : r.GetEventId()-1] {
			if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				t.Fatalf("event %d completes a workflow task before R %d", ev.GetEventId(), r.GetEventId())
			}
		}
	})
}

// TestResumeHistory_ContainersStoppedBy: no container of the base runs past
// its close plus 30 s, nor past 30 s after the heartbeat timeout and grace
// of an attempt that did not report its result, counted from that attempt's
// end, or from the base's close when it was still open.
func TestResumeHistory_ContainersStoppedBy(t *testing.T) {
	closed := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	ht, grace := 20*time.Second, 30*time.Second
	for name, tc := range map[string]struct {
		d    []ResumeDispatch
		want time.Duration
	}{
		"nothing dispatched":                    {nil, 30 * time.Second},
		"an open attempt":                       {[]ResumeDispatch{{Open: true, HeartbeatTimeout: ht, Grace: grace}}, ht + grace + 30*time.Second},
		"a reported result counts nothing":      {[]ResumeDispatch{{Completed: true, ClosedAt: closed, HeartbeatTimeout: ht, Grace: time.Hour}}, 30 * time.Second},
		"an attempt that ended long before":     {[]ResumeDispatch{{ClosedAt: closed.Add(-time.Hour), HeartbeatTimeout: ht, Grace: grace}}, 30 * time.Second},
		"an attempt that ended just before":     {[]ResumeDispatch{{ClosedAt: closed.Add(-10 * time.Second), HeartbeatTimeout: ht, Grace: grace}}, ht + grace + 20*time.Second},
		"the latest of several":                 {[]ResumeDispatch{{Open: true, HeartbeatTimeout: ht}, {Open: true, HeartbeatTimeout: ht, Grace: time.Minute}}, ht + time.Minute + 30*time.Second},
		"a long grace of an attempt that ended": {[]ResumeDispatch{{ClosedAt: closed.Add(-time.Minute), HeartbeatTimeout: ht, Grace: 10 * time.Minute}}, ht + 9*time.Minute + 30*time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			h := &ResumeHistory{CloseTime: closed, Dispatches: tc.d}
			if got := h.ContainersStoppedBy(); !got.Equal(closed.Add(tc.want)) {
				t.Fatalf("stopped by close+%s, want close+%s", got.Sub(closed), tc.want)
			}
		})
	}
}
