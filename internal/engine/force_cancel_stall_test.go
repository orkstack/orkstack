package engine

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ev(id int64, typ enumspb.EventType) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: id, EventType: typ}
}

// TestForceCancelProgress: whether a run made progress since its cancel was
// requested (event l0), read newest first. A completed workflow task after
// l0 is progress; so is a window too long to read to l0 within the budget,
// and a task that timed out (its worker died) with none that failed. A run
// whose every task since l0 failed, the loop a force cancel exists for, did
// not progress.
func TestForceCancelProgress(t *testing.T) {
	const (
		sched     = enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
		started   = enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
		completed = enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
		failed    = enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
		timedOut  = enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
		cancelReq = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	)
	tests := []struct {
		name        string
		newestFirst []*historypb.HistoryEvent
		progressing bool
		newest      int64
	}{
		{"a loop of failed tasks since the cancel", []*historypb.HistoryEvent{
			ev(16, failed), ev(15, started), ev(14, sched), ev(13, failed), ev(12, started), ev(11, sched), ev(10, cancelReq),
		}, false, 16},
		{"a task completed after the cancel", []*historypb.HistoryEvent{
			ev(14, completed), ev(13, started), ev(12, sched), ev(11, failed), ev(10, cancelReq),
		}, true, 14},
		{"a completion at or before l0 is not progress since", []*historypb.HistoryEvent{
			ev(12, failed), ev(11, started), ev(10, completed),
		}, false, 12},
		{"not read down to l0", []*historypb.HistoryEvent{
			ev(20, failed), ev(19, started), ev(18, sched),
		}, true, 20},
		{"a timed-out task and none failed: its worker went away", []*historypb.HistoryEvent{
			ev(13, timedOut), ev(12, started), ev(11, sched), ev(10, cancelReq),
		}, true, 13},
		{"a timed-out task beside failed ones", []*historypb.HistoryEvent{
			ev(16, failed), ev(15, started), ev(14, timedOut), ev(13, started), ev(12, sched), ev(10, cancelReq),
		}, false, 16},
		{"read down to the event after l0, all failed", []*historypb.HistoryEvent{
			ev(13, failed), ev(12, started), ev(11, sched),
		}, false, 13},
		{"nothing read", nil, false, 10},
		{"nothing since l0", []*historypb.HistoryEvent{ev(10, cancelReq)}, false, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ForceCancelProgress(tt.newestFirst, 10)
			if p.Progressing != tt.progressing || p.Newest != tt.newest {
				t.Fatalf("progress = %+v, want progressing %v newest %d", p, tt.progressing, tt.newest)
			}
		})
	}
}

// TestForceCancelStalled: the record says the run did nothing for its grace.
// That is signed only when the window the stall test read still shows no
// progress, and is as old as it says by the cluster's times; a clock that says
// otherwise is told apart from a run that moved.
func TestForceCancelStalled(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	const grace = time.Minute
	at := func(id int64, typ enumspb.EventType, ago time.Duration) *historypb.HistoryEvent {
		e := ev(id, typ)
		e.EventTime = timestamppb.New(now.Add(-ago))
		return e
	}
	const completed, failed, cancelReq = enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	tests := []struct {
		name         string
		events       []*historypb.HistoryEvent
		moved, early bool
	}{
		{"only failures since", []*historypb.HistoryEvent{at(12, failed, 10*time.Second), at(11, failed, 50*time.Second), at(10, cancelReq, 61*time.Second)}, false, false},
		{"nothing since", []*historypb.HistoryEvent{at(10, cancelReq, 61*time.Second)}, false, false},
		{"it moved after the stall test", []*historypb.HistoryEvent{at(12, completed, time.Second), at(11, failed, 50*time.Second), at(10, cancelReq, 61*time.Second)}, true, false},
		{"the window was not read through", []*historypb.HistoryEvent{at(14, failed, 10*time.Second), at(13, failed, 20*time.Second)}, true, false},
		{"all but the event it is dated by", []*historypb.HistoryEvent{at(11, failed, 61*time.Second)}, true, false},
		{"within the clocks' allowance", []*historypb.HistoryEvent{at(10, cancelReq, 56*time.Second)}, false, false},
		{"a clock that runs behind", []*historypb.HistoryEvent{at(10, cancelReq, 30*time.Second)}, false, true},
		{"nothing was read", nil, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			moved, early := forceCancelStalled(tt.events, 10, grace, now)
			if moved != tt.moved || early != tt.early {
				t.Fatalf("moved %v early %v, want %v %v", moved, early, tt.moved, tt.early)
			}
		})
	}
}
