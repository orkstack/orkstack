package api_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The task-timing derivation (docs/UI_CLI_PARITY.md "Task timing"), row by
// row. Every quantity is one of three states, and each case pins which.

var timingT0 = time.Date(2026, 9, 22, 22, 0, 0, 0, time.UTC)

func timingAt(offset time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(timingT0.Add(offset))
}

func value(d time.Duration) *orkstackv1.TimingQuantity {
	return &orkstackv1.TimingQuantity{Duration: durationpb.New(d)}
}

func because(reason orkstackv1.TimingReason) *orkstackv1.TimingQuantity {
	return &orkstackv1.TimingQuantity{Reason: reason}
}

const (
	noStart     = orkstackv1.TimingReason_TIMING_REASON_NO_ORKER_START
	skew        = orkstackv1.TimingReason_TIMING_REASON_CLOCK_SKEW
	preContract = orkstackv1.TimingReason_TIMING_REASON_PRE_CONTRACT
	reclaimed   = orkstackv1.TimingReason_TIMING_REASON_RECLAIMED
	noRecord    = orkstackv1.TimingReason_TIMING_REASON_NO_RECORD
	noPhases    = orkstackv1.TimingReason_TIMING_REASON_NO_PHASES
)

func requireProto(t *testing.T, what string, got, want proto.Message) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Errorf("%s = %v, want %v", what, prototext.Format(got), prototext.Format(want))
	}
}

func TestSetRunTiming_DerivesEachKindOfRow(t *testing.T) {
	succeeded, running := orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_RUNNING
	now := timingT0.Add(time.Hour)
	for _, tc := range []struct {
		name string
		task *orkstackv1.TaskRun
		want *orkstackv1.TaskTiming
	}{{
		name: "never scheduled: nothing applies",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_CANCELED},
		want: &orkstackv1.TaskTiming{},
	}, {
		name: "finished: wait, run and total are measured",
		task: &orkstackv1.TaskRun{Status: succeeded, ScheduledAt: timingAt(0), StartedAt: timingAt(4 * time.Second), FinishedAt: timingAt(10 * time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Wait: value(4 * time.Second), Run: value(6 * time.Second), Total: value(10 * time.Second)},
	}, {
		name: "finished with no Orker start: wait and run are unavailable, total is measured",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_FAILED, ScheduledAt: timingAt(0), FinishedAt: timingAt(time.Minute)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Wait: because(noStart), Run: because(noStart), Total: value(time.Minute)},
	}, {
		name: "recorded before scheduled time existed: total is dispatch to finish",
		task: &orkstackv1.TaskRun{Status: succeeded, StartedAt: timingAt(0), FinishedAt: timingAt(3 * time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, PreContract: true, Wait: because(preContract), Run: because(preContract), Total: value(3 * time.Second)},
	}, {
		name: "an Orker start before scheduling is clock skew, never a negative",
		task: &orkstackv1.TaskRun{Status: succeeded, ScheduledAt: timingAt(5 * time.Second), StartedAt: timingAt(time.Second), FinishedAt: timingAt(9 * time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Wait: because(skew), Run: value(8 * time.Second), Total: value(4 * time.Second)},
	}, {
		name: "running: only since scheduled, against now",
		task: &orkstackv1.TaskRun{Status: running, ScheduledAt: timingAt(0)},
		want: &orkstackv1.TaskTiming{Scheduled: true, SinceScheduled: value(time.Hour)},
	}, {
		name: "pending and scheduled: nothing applies yet",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_PENDING, ScheduledAt: timingAt(0)},
		want: &orkstackv1.TaskTiming{Scheduled: true},
	}, {
		name: "awaited: run and total are the time it awaited, wait does not apply",
		task: &orkstackv1.TaskRun{Status: succeeded, Await: &orkstackv1.TaskAwait{}, ScheduledAt: timingAt(0), StartedAt: timingAt(0), FinishedAt: timingAt(90 * time.Minute)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Run: value(90 * time.Minute), Total: value(90 * time.Minute)},
	}, {
		name: "an await that never opened was never scheduled",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_SKIPPED, Await: &orkstackv1.TaskAwait{}},
		want: &orkstackv1.TaskTiming{},
	}, {
		name: "a fan-out whose items never reported a start",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_FAILED, Each: &orkstackv1.TaskEach{}, ScheduledAt: timingAt(0), FinishedAt: timingAt(time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Wait: because(noStart), Run: because(noStart), Total: value(time.Second)},
	}, {
		name: "a composed task whose child never started: neither wait nor run applies",
		task: &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_FAILED, Child: &orkstackv1.TaskChild{}, ScheduledAt: timingAt(0), FinishedAt: timingAt(time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, Total: value(time.Second)},
	}, {
		name: "a running composed task: since scheduled",
		task: &orkstackv1.TaskRun{Status: running, Child: &orkstackv1.TaskChild{}, ScheduledAt: timingAt(0), StartedAt: timingAt(time.Second)},
		want: &orkstackv1.TaskTiming{Scheduled: true, SinceScheduled: value(time.Hour)},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{tc.task}}
			api.SetRunTiming(run, now)
			requireProto(t, "timing", run.GetTaskRuns()[0].GetTiming(), tc.want)
		})
	}
}

func TestSetRunTiming_SumsTheRunsWaitAndTimesEachItem(t *testing.T) {
	now := timingT0.Add(time.Hour)
	succeeded := orkstackv1.Status_STATUS_SUCCEEDED
	run := &orkstackv1.Run{
		StartedAt:  timingAt(0),
		FinishedAt: timingAt(time.Minute),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "a", Status: succeeded, ScheduledAt: timingAt(0), StartedAt: timingAt(2 * time.Second), FinishedAt: timingAt(3 * time.Second)},
			{TaskId: "b", Status: succeeded, ScheduledAt: timingAt(0), StartedAt: timingAt(5 * time.Second), FinishedAt: timingAt(6 * time.Second)},
			{TaskId: "c", Status: orkstackv1.Status_STATUS_FAILED, ScheduledAt: timingAt(0), FinishedAt: timingAt(time.Second)},
			{TaskId: "d", Status: orkstackv1.Status_STATUS_SKIPPED},
			{
				TaskId: "fan", Status: succeeded, Each: &orkstackv1.TaskEach{Expanded: true, Count: 1},
				ScheduledAt: timingAt(0), StartedAt: timingAt(time.Second), FinishedAt: timingAt(4 * time.Second),
				Items: []*orkstackv1.TaskRun{{
					TaskId: "fan", Item: proto.Int32(0), Status: succeeded,
					ScheduledAt: timingAt(0), StartedAt: timingAt(3 * time.Second), FinishedAt: timingAt(4 * time.Second),
				}},
			},
		},
	}
	api.SetRunTiming(run, now)
	// The fan-out's own row is summed (1s), its item is not.
	requireProto(t, "run timing", run.GetTiming(), &orkstackv1.RunTiming{
		Elapsed:              value(time.Minute),
		WaitSum:              value(8 * time.Second),
		WaitSumTasks:         3,
		WaitUnavailableTasks: 1,
	})
	requireProto(t, "item timing", run.GetTaskRuns()[4].GetItems()[0].GetTiming(), &orkstackv1.TaskTiming{
		Scheduled: true, Wait: value(3 * time.Second), Run: value(time.Second), Total: value(4 * time.Second),
	})
}

func TestSetRunTiming_EveryWaitUnavailableIsAnUnavailableSum(t *testing.T) {
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "a", Status: orkstackv1.Status_STATUS_FAILED, ScheduledAt: timingAt(0), FinishedAt: timingAt(time.Second)},
	}}
	api.SetRunTiming(run, timingT0)
	requireProto(t, "run timing", run.GetTiming(), &orkstackv1.RunTiming{
		WaitSum:              &orkstackv1.TimingQuantity{},
		WaitUnavailableTasks: 1,
	})
}

// A run terminated from outside leaves the rows that were running RUNNING;
// they are not measured against now, so two reads of the finished run agree.
func TestSetRunTiming_AFinishedRunIsNeverMeasuredAgainstNow(t *testing.T) {
	read := func(now time.Time) *orkstackv1.Run {
		run := &orkstackv1.Run{
			Status:    orkstackv1.Status_STATUS_CANCELED,
			StartedAt: timingAt(0), FinishedAt: timingAt(time.Second),
			TaskRuns: []*orkstackv1.TaskRun{{TaskId: "left", Status: orkstackv1.Status_STATUS_RUNNING, ScheduledAt: timingAt(0)}},
		}
		api.SetRunTiming(run, now)
		return run
	}
	first, later := read(timingT0.Add(time.Minute)), read(timingT0.Add(time.Hour))
	requireProto(t, "the row left RUNNING", first.GetTaskRuns()[0].GetTiming(), &orkstackv1.TaskTiming{Scheduled: true})
	requireProto(t, "a later read", later, first)
}

func TestSetRunTiming_OnlyARunningTaskIsMeasuredAgainstNow(t *testing.T) {
	now := timingT0.Add(time.Minute)
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING, Each: &orkstackv1.TaskEach{Expanded: true, Count: 1}, ScheduledAt: timingAt(0),
			Items: []*orkstackv1.TaskRun{{TaskId: "fan", Item: proto.Int32(0), Status: orkstackv1.Status_STATUS_RUNNING, ScheduledAt: timingAt(time.Second)}}},
	}}
	api.SetRunTiming(run, now)
	requireProto(t, "run timing", run.GetTiming(), &orkstackv1.RunTiming{ComputedAt: timestamppb.New(now)})
	requireProto(t, "item since scheduled", run.GetTaskRuns()[0].GetItems()[0].GetTiming().GetSinceScheduled(), value(59*time.Second))
}

func timingRow(attempt int32, outcome orkstackv1.AttemptOutcome, start time.Duration, phases ...*orkstackv1.PhaseSpan) *orkstackv1.TaskAttempt {
	row := &orkstackv1.TaskAttempt{TaskId: "t", Attempt: attempt, Outcome: outcome, Phases: phases}
	if outcome != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
		row.StartedAt = timingAt(start)
	}
	return row
}

func TestAttemptTimings_DerivesTheWaitForAnOrkerAndEachAttempt(t *testing.T) {
	succeeded, failed := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	lost := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED
	finished := &orkstackv1.TaskRun{TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2, ScheduledAt: timingAt(0), StartedAt: timingAt(20 * time.Second), FinishedAt: timingAt(30 * time.Second)}
	running := &orkstackv1.TaskRun{TaskId: "t", Status: orkstackv1.Status_STATUS_RUNNING, Attempt: 2, ScheduledAt: timingAt(0)}
	for _, tc := range []struct {
		name         string
		task         *orkstackv1.TaskRun
		rows         []*orkstackv1.TaskAttempt
		want         *orkstackv1.TaskAttemptsTiming
		wantAttempts []*orkstackv1.AttemptTiming
	}{{
		name:         "attempt 1's start less scheduled",
		task:         finished,
		rows:         []*orkstackv1.TaskAttempt{timingRow(1, failed, 7*time.Second), timingRow(2, succeeded, 20*time.Second)},
		want:         &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: value(7 * time.Second)},
		wantAttempts: []*orkstackv1.AttemptTiming{{PhasesReason: noPhases}, {PhasesReason: noPhases}},
	}, {
		name:         "attempt 1 has no record",
		task:         finished,
		rows:         []*orkstackv1.TaskAttempt{timingRow(1, lost, 0), timingRow(2, succeeded, 20*time.Second)},
		want:         &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: because(noRecord)},
		wantAttempts: []*orkstackv1.AttemptTiming{{PhasesReason: noRecord}, {PhasesReason: noPhases}},
	}, {
		name:         "every record of a task an Orker reported on is gone",
		task:         finished,
		rows:         []*orkstackv1.TaskAttempt{timingRow(1, lost, 0), timingRow(2, lost, 0)},
		want:         &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: because(reclaimed), Reclaimed: true},
		wantAttempts: []*orkstackv1.AttemptTiming{{PhasesReason: reclaimed}, {PhasesReason: reclaimed}},
	}, {
		name: "a finished task with no rows at all",
		task: finished,
		want: &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: because(reclaimed), Reclaimed: true},
	}, {
		name:         "the newest attempt of a running task is in progress, not unreported",
		task:         running,
		rows:         []*orkstackv1.TaskAttempt{timingRow(1, failed, 3*time.Second), timingRow(2, lost, 0)},
		want:         &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: value(3 * time.Second)},
		wantAttempts: []*orkstackv1.AttemptTiming{{PhasesReason: noPhases}, {InProgress: true}},
	}, {
		name:         "recorded before scheduled time existed",
		task:         &orkstackv1.TaskRun{TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, StartedAt: timingAt(0), FinishedAt: timingAt(time.Second)},
		rows:         []*orkstackv1.TaskAttempt{timingRow(1, succeeded, 0, &orkstackv1.PhaseSpan{Phase: "run", StartedAt: timingAt(0), FinishedAt: timingAt(time.Second)})},
		want:         &orkstackv1.TaskAttemptsTiming{TaskId: "t", QueueWait: because(preContract)},
		wantAttempts: []*orkstackv1.AttemptTiming{{}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{tc.task}}
			timings := api.AttemptTimings(run, "", nil, tc.rows)
			if len(timings) != 1 {
				t.Fatalf("timings = %v, want one", timings)
			}
			requireProto(t, "attempts timing", timings[0], tc.want)
			for i, row := range tc.rows {
				requireProto(t, fmt.Sprintf("attempt %d timing", row.GetAttempt()), row.GetTiming(), tc.wantAttempts[i])
			}
		})
	}
}

func TestAttemptTimings_TimesEachPhaseAndNamesAMissingEnd(t *testing.T) {
	task := &orkstackv1.TaskRun{TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, ScheduledAt: timingAt(0), StartedAt: timingAt(time.Second), FinishedAt: timingAt(4 * time.Second)}
	row := timingRow(1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, time.Second,
		&orkstackv1.PhaseSpan{Phase: "image", StartedAt: timingAt(time.Second), FinishedAt: timingAt(1500 * time.Millisecond)},
		&orkstackv1.PhaseSpan{Phase: "run", StartedAt: timingAt(1500 * time.Millisecond), FinishedAt: timingAt(time.Second)},
		&orkstackv1.PhaseSpan{Phase: "commit", StartedAt: timingAt(4 * time.Second)},
	)
	api.AttemptTimings(&orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{task}}, "", nil, []*orkstackv1.TaskAttempt{row})
	for i, want := range []*orkstackv1.TimingQuantity{value(500 * time.Millisecond), because(skew), because(noRecord)} {
		requireProto(t, row.GetPhases()[i].GetPhase(), row.GetPhases()[i].GetDuration(), want)
	}
	requireProto(t, "attempt timing", row.GetTiming(), &orkstackv1.AttemptTiming{})
}

func TestAttemptTimings_OnlyScheduledContainerTimelinesAndTheNamedItem(t *testing.T) {
	succeeded := orkstackv1.Status_STATUS_SUCCEEDED
	item := func(i int32) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: "fan", Item: proto.Int32(i), Status: succeeded, ScheduledAt: timingAt(0), StartedAt: timingAt(time.Second), FinishedAt: timingAt(2 * time.Second)}
	}
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "gate", Status: succeeded, Await: &orkstackv1.TaskAwait{}, ScheduledAt: timingAt(0), StartedAt: timingAt(0), FinishedAt: timingAt(time.Second)},
		{TaskId: "child", Status: succeeded, Child: &orkstackv1.TaskChild{}, ScheduledAt: timingAt(0)},
		{TaskId: "never", Status: orkstackv1.Status_STATUS_SKIPPED},
		{TaskId: "fan", Status: succeeded, Each: &orkstackv1.TaskEach{Expanded: true, Count: 2}, ScheduledAt: timingAt(0), Items: []*orkstackv1.TaskRun{item(0), item(1)}},
	}}
	scopes := func(timings []*orkstackv1.TaskAttemptsTiming) []string {
		var out []string
		for _, timing := range timings {
			out = append(out, fmt.Sprintf("%s[%d]", timing.GetTaskId(), timing.GetItem()))
		}
		return out
	}
	if got := scopes(api.AttemptTimings(run, "", nil, nil)); fmt.Sprint(got) != "[fan[0] fan[1]]" {
		t.Errorf("every task: timelines = %v, want the fan-out's two items only", got)
	}
	if got := scopes(api.AttemptTimings(run, "fan", proto.Int32(1), nil)); fmt.Sprint(got) != "[fan[1]]" {
		t.Errorf("item 1: timelines = %v, want item 1 only", got)
	}
}

// GetRun and ListTaskAttempts answer the derivation over the wire.
func TestGetRunAndListTaskAttempts_CarryTheTiming(t *testing.T) {
	run := retriedRun("ork-timing-wire", "flaky", 2)
	start := run.GetStartedAt().AsTime()
	run.TaskRuns[0].ScheduledAt = timestamppb.New(start)
	run.TaskRuns[0].StartedAt = timestamppb.New(start.Add(3 * time.Second))
	run.TaskRuns[0].FinishedAt = timestamppb.New(start.Add(4 * time.Second))
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := fixture.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.GetId()}))
	must.OKf(t, err, "GetRun")
	requireProto(t, "GetRun wait", got.Msg.GetRun().GetTaskRuns()[0].GetTiming().GetWait(), value(3*time.Second))
	requireProto(t, "GetRun elapsed", got.Msg.GetRun().GetTiming().GetElapsed(), value(10*time.Second))

	listed, err := fixture.runClient().ListTaskAttempts(ctx, connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()}))
	must.OKf(t, err, "ListTaskAttempts")
	// attemptRecord starts attempt 1 at 17:01:01, a minute and a second after
	// the task was scheduled at 17:00:00.
	requireProto(t, "timings",
		&orkstackv1.ListTaskAttemptsResponse{Timings: listed.Msg.GetTimings()},
		&orkstackv1.ListTaskAttemptsResponse{Timings: []*orkstackv1.TaskAttemptsTiming{{TaskId: "flaky", QueueWait: value(61 * time.Second)}}})
	for _, attempt := range listed.Msg.GetAttempts() {
		requireProto(t, fmt.Sprintf("attempt %d timing", attempt.GetAttempt()), attempt.GetTiming(),
			&orkstackv1.AttemptTiming{PhasesReason: noPhases})
	}
}
