package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// forceCanceledClose is the outcome of a verified force cancel of sampleRecord.
func forceCanceledClose() ForceCancelClose {
	return ForceCancelClose{Verification: ForceCancelVerified, Record: sampleRecord()}
}

// TestForceCanceledTerminalEvent: the one function every surface builds a
// force-canceled run's ending with. The run reads CANCELED RUN_FORCE_CANCELED
// whatever it said before; its task rows stay as of its last event, but for
// a row still reading back a resumed run's result, which no longer can; the
// event is the run's next, says why, and names the force cancel's request.
// The same inputs give the same bytes.
func TestForceCanceledTerminalEvent(t *testing.T) {
	closedAt := time.Date(2026, 11, 20, 12, 5, 0, 0, time.UTC)
	last := &orkstackv1.Run{Id: "ork-r", Namespace: "payments", Status: orkstackv1.Status_STATUS_RUNNING,
		TraceId: "0af7651916cd43dd8448eb211c80319c", Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Error: &orkstackv1.Error{Code: "TASK_FAILED", Message: "a task failed"},
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "done", Status: orkstackv1.Status_STATUS_SUCCEEDED},
			{TaskId: "stuck", Status: orkstackv1.Status_STATUS_RUNNING},
			{TaskId: "kept", Status: orkstackv1.Status_STATUS_SUCCEEDED, ReadingBack: true},
		}}
	last.Pause, last.PausedByParent = &orkstackv1.RunPause{PausedBy: "ops", ExpiresAt: timestamppb.New(closedAt.Add(time.Hour))}, &orkstackv1.InheritedPause{}
	last.Queued, last.DispatchHold = &orkstackv1.ConcurrencyQueued{}, orkstackv1.RunDispatchHold(1)
	before := proto.Clone(last)
	ev, run := ForceCanceledTerminalEvent(last, forceCanceledClose(), 42, closedAt)
	if !proto.Equal(last, before) {
		t.Fatal("the run's last recorded state was changed in place")
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != RunForceCanceledCode ||
		!run.GetFinishedAt().AsTime().Equal(closedAt) {
		t.Fatalf("run = %v %v at %v, want CANCELED %s at the close", run.GetStatus(), run.GetError(), run.GetFinishedAt(), RunForceCanceledCode)
	}
	if run.GetCanceledBy() != "user:corp:alice" || !run.GetCanceledByVerified() {
		t.Errorf("canceled by %q verified %v, want the record's user:corp:alice, verified", run.GetCanceledBy(), run.GetCanceledByVerified())
	}
	if run.GetTraceId() == "" || run.GetTraceId() != last.GetTraceId() || run.GetTraceparent() != last.GetTraceparent() {
		t.Errorf("trace %q %q, want the run's own: its span is made from them", run.GetTraceId(), run.GetTraceparent())
	}
	if run.Pause != nil || run.PausedByParent != nil || run.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED || !run.GetLastPause().GetEndedAt().AsTime().Equal(closedAt) {
		t.Errorf("an ended run is paused: %v, %v, last %v", run.GetPause(), run.GetPausedByParent(), run.GetLastPause())
	}
	if run.Queued != nil || run.GetDispatchHold() != 0 {
		t.Errorf("an ended run waits in line: %v, %v", run.GetQueued(), run.GetDispatchHold())
	}
	unpaused := proto.Clone(last).(*orkstackv1.Run)
	unpaused.Pause, unpaused.LastPause = nil, &orkstackv1.RunPause{End: orkstackv1.PauseEnd_PAUSE_END_UNPAUSED}
	if _, r := ForceCanceledTerminalEvent(unpaused, forceCanceledClose(), 42, closedAt); r.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_UNPAUSED {
		t.Errorf("a pause that had ended before reads %v", r.GetLastPause())
	}
	lapsed := proto.Clone(last).(*orkstackv1.Run)
	lapsed.Pause.ExpiresAt = timestamppb.New(closedAt.Add(-time.Hour))
	if _, r := ForceCanceledTerminalEvent(lapsed, forceCanceledClose(), 42, closedAt); r.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT || !r.GetLastPause().GetEndedAt().AsTime().Equal(closedAt.Add(-time.Hour)) {
		t.Errorf("a pause that had lapsed ended %v", r.GetLastPause())
	}
	asserted := forceCanceledClose()
	asserted.Verification = ForceCancelAsserted // a record nobody signed names nobody as verified, whatever it says
	if _, r := ForceCanceledTerminalEvent(last, asserted, 42, closedAt); r.GetCanceledBy() != "user:corp:alice" || r.GetCanceledByVerified() {
		t.Errorf("asserted: canceled by %q verified %v, want user:corp:alice, asserted", r.GetCanceledBy(), r.GetCanceledByVerified())
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, row := range run.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	if rows["done"].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || rows["stuck"].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("task rows changed: done %v, stuck %v; want them as of the last event", rows["done"].GetStatus(), rows["stuck"].GetStatus())
	}
	if r := rows["kept"]; r.GetStatus() != orkstackv1.Status_STATUS_CANCELED || r.GetError().GetCode() != ReasonResumeNotReadBack || r.GetReadingBack() {
		t.Errorf("a row reading back = %v %v reading_back=%v, want CANCELED %s", r.GetStatus(), r.GetError(), r.GetReadingBack(), ReasonResumeNotReadBack)
	}
	want := &orkstackv1.Event{RunId: "ork-r", Sequence: 42, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED,
		Status: orkstackv1.Status_STATUS_CANCELED, Message: run.GetError().GetMessage(),
		Timestamp: timestamppb.New(closedAt), RequestId: "fc-1"}
	if !proto.Equal(ev, want) {
		t.Fatalf("event = %v, want %v", ev, want)
	}
	again, _ := ForceCanceledTerminalEvent(before.(*orkstackv1.Run), forceCanceledClose(), 42, closedAt)
	a, err := proto.MarshalOptions{Deterministic: true}.Marshal(ev)
	must.OK(t, err)
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(again)
	must.OK(t, err)
	if string(a) != string(b) {
		t.Fatal("the same inputs built different bytes")
	}
}
