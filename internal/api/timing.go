package api

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// WHERE A TASK'S TIME WENT, DERIVED IN ONE PLACE.
//
// This file is the task-timing derivation (docs/UI_CLI_PARITY.md "Task
// timing") and the only place that subtracts a task's timestamps: GetRun
// sets Run.timing and every TaskRun.timing; ListTaskAttempts sets each
// attempt's timing, each phase's duration and one TaskAttemptsTiming per
// container timeline. `orkctl run timing` and the run page render those
// fields and derive nothing.
//
// Every quantity is one of three states (TimingQuantity): nil does not apply,
// a reason without a duration is unavailable, a duration is a value. A zero is
// only ever a value somebody measured.

func measuredQuantity(d time.Duration) *orkstackv1.TimingQuantity {
	return &orkstackv1.TimingQuantity{Duration: durationpb.New(d)}
}

func unavailableQuantity(reason orkstackv1.TimingReason) *orkstackv1.TimingQuantity {
	return &orkstackv1.TimingQuantity{Reason: reason}
}

// spanQuantity is to − from, or unavailable with CLOCK_SKEW when the two ends
// are inverted: two clocks, one span, and a negative is never answered.
func spanQuantity(from, to time.Time) *orkstackv1.TimingQuantity {
	if to.Before(from) {
		return unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_CLOCK_SKEW)
	}
	return measuredQuantity(to.Sub(from))
}

// instantOf is a timestamp as an instant, or nil when it is unset, invalid or
// zero: unset is "not known", never the epoch.
func instantOf(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || !ts.IsValid() || ts.AsTime().IsZero() {
		return nil
	}
	at := ts.AsTime()
	return &at
}

func statusIsTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_SKIPPED:
		return true
	}
	return false
}

// isContainerRow is a row whose time an Orker spends: not an awaited or
// sleeping task, a fan-out's own row or a composed task. Only such a row has
// attempts, phases and a wait for an Orker.
func isContainerRow(taskRun *orkstackv1.TaskRun) bool {
	return taskRun.GetAwait() == nil && taskRun.GetSleep() == nil && taskRun.GetEach() == nil && taskRun.GetChild() == nil
}

// SetRunTiming sets Run.timing and every TaskRun.timing, a fan-out's items
// included, measuring each running task's since_scheduled against now. GetRun
// answers with it; it is exported for the clients' rendering tests.
func SetRunTiming(run *orkstackv1.Run, now time.Time) {
	timing := &orkstackv1.RunTiming{}
	if from, to := instantOf(run.GetStartedAt()), instantOf(run.GetFinishedAt()); from != nil && to != nil {
		timing.Elapsed = spanQuantity(*from, *to)
	}
	// Only a live run's tasks are measured against now: a run that ended —
	// terminated from outside, say — can leave a row RUNNING that no longer
	// runs, and a finished run must read the same bytes every time.
	live := !statusIsTerminal(run.GetStatus())
	var waitSum time.Duration
	for _, taskRun := range run.GetTaskRuns() {
		task := taskTimingOf(taskRun, now, live)
		taskRun.Timing = task
		measuredNow := task.GetSinceScheduled() != nil
		for _, item := range taskRun.GetItems() {
			item.Timing = taskTimingOf(item, now, live)
			measuredNow = measuredNow || item.GetTiming().GetSinceScheduled() != nil
		}
		if measuredNow {
			timing.ComputedAt = timestamppb.New(now)
		}
		switch {
		case task.GetWait() == nil:
		case task.GetWait().GetDuration() != nil:
			waitSum += task.GetWait().GetDuration().AsDuration()
			timing.WaitSumTasks++
		default:
			timing.WaitUnavailableTasks++
		}
	}
	switch {
	case timing.WaitSumTasks > 0:
		timing.WaitSum = measuredQuantity(waitSum)
	case timing.WaitUnavailableTasks > 0:
		timing.WaitSum = &orkstackv1.TimingQuantity{}
	}
	run.Timing = timing
}

// taskTimingOf derives one row: wait = B − S, run = F − B, total = F − S, with
// S = scheduled_at, B = started_at and F = finished_at. since_scheduled is
// measured against now only when the run is live.
func taskTimingOf(taskRun *orkstackv1.TaskRun, now time.Time, live bool) *orkstackv1.TaskTiming {
	S, B, F := instantOf(taskRun.GetScheduledAt()), instantOf(taskRun.GetStartedAt()), instantOf(taskRun.GetFinishedAt())
	status := taskRun.GetStatus()
	terminal := statusIsTerminal(status)
	noStart := unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_NO_ORKER_START)
	timing := &orkstackv1.TaskTiming{}
	running := func() {
		if live && status == orkstackv1.Status_STATUS_RUNNING && S != nil {
			timing.SinceScheduled = spanQuantity(*S, now)
		}
	}

	switch {
	case taskRun.GetAwait() != nil, taskRun.GetSleep() != nil:
		// scheduled_at and started_at are both the instant it started
		// awaiting (or sleeping); one that never opened has neither. It is
		// never queued, so wait does not apply, and run is the time it
		// awaited (or slept).
		timing.Scheduled = B != nil
		if timing.Scheduled && terminal && F != nil {
			timing.Run = spanQuantity(*B, *F)
			timing.Total = timing.Run
		}
		return timing
	case !isContainerRow(taskRun):
		// A fan-out's own row (S its expansion, B its earliest item's start)
		// or a composed task (S its child's request, B its confirmed start).
		timing.Scheduled = S != nil
		if !timing.Scheduled {
			return timing
		}
		switch {
		case !terminal:
			running()
		case B == nil && taskRun.GetEach() != nil:
			timing.Wait, timing.Run = noStart, noStart
		case B == nil:
			// No child run was started: there is no start latency and no
			// child duration to measure, so neither applies.
		default:
			timing.Wait = spanQuantity(*S, *B)
			if F != nil {
				timing.Run = spanQuantity(*B, *F)
			}
		}
		if terminal && F != nil {
			timing.Total = spanQuantity(*S, *F)
		}
		return timing
	}

	timing.Scheduled = S != nil || B != nil
	timing.PreContract = S == nil && B != nil
	switch {
	case !timing.Scheduled:
		// Never scheduled: no quantity applies.
	case timing.PreContract:
		// B still holds the dispatch instant: there is no Orker start to
		// measure wait or run against, and total is the old dispatch-to-
		// finish span.
		preContract := unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_PRE_CONTRACT)
		timing.Wait, timing.Run = preContract, preContract
		if terminal && F != nil {
			timing.Total = spanQuantity(*B, *F)
		}
	case !terminal:
		running()
	default:
		if B == nil {
			timing.Wait, timing.Run = noStart, noStart
		} else {
			timing.Wait = spanQuantity(*S, *B)
			if F != nil {
				timing.Run = spanQuantity(*B, *F)
			}
		}
		if F != nil {
			timing.Total = spanQuantity(*S, *F)
		}
	}
	return timing
}

// recordsReclaimed says a timeline holds no record an Orker filed even though
// the snapshot proves an Orker reported on it. An Orker that reports a start
// (started_at) has always written a record for that attempt; a timeline with
// none left is one the cleanup worker reclaimed, not one whose every Orker
// crashed.
func recordsReclaimed(taskRun *orkstackv1.TaskRun, rows []*orkstackv1.TaskAttempt) bool {
	if len(rows) == 0 {
		return statusIsTerminal(taskRun.GetStatus())
	}
	if instantOf(taskRun.GetStartedAt()) == nil {
		return false
	}
	for _, row := range rows {
		if row.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
			return false
		}
	}
	return true
}

// timelineKey is one timeline of a ListTaskAttempts answer: a task, or one
// item of a fan-out.
type timelineKey struct {
	taskID string
	item   artifacts.Item
}

// AttemptTimings derives the attempt half over the rows ListTaskAttempts
// answers for run: every row's timing and phase durations, and one
// TaskAttemptsTiming per scheduled container timeline the request selected
// (every task's when taskID is empty, a fan-out's items, or the one item
// named), in snapshot order. It is exported for the clients' rendering tests.
func AttemptTimings(
	run *orkstackv1.Run,
	taskID string,
	item *int32,
	attempts []*orkstackv1.TaskAttempt,
) []*orkstackv1.TaskAttemptsTiming {
	rowsOf := make(map[timelineKey][]*orkstackv1.TaskAttempt)
	for _, attempt := range attempts {
		key := timelineKey{attempt.GetTaskId(), artifacts.ItemOf(attempt.Item)}
		rowsOf[key] = append(rowsOf[key], attempt)
	}
	var timings []*orkstackv1.TaskAttemptsTiming
	timeline := func(taskRun *orkstackv1.TaskRun) {
		rows := rowsOf[timelineKey{taskRun.GetTaskId(), artifacts.ItemOf(taskRun.Item)}]
		reclaimed := recordsReclaimed(taskRun, rows)
		setAttemptRowsTiming(taskRun, rows, reclaimed)
		if !isContainerRow(taskRun) || (instantOf(taskRun.GetScheduledAt()) == nil && instantOf(taskRun.GetStartedAt()) == nil) {
			return
		}
		timings = append(timings, &orkstackv1.TaskAttemptsTiming{
			TaskId:    taskRun.GetTaskId(),
			Item:      taskRun.Item,
			QueueWait: queueWaitOf(taskRun, rows, reclaimed),
			Reclaimed: reclaimed,
		})
	}
	for _, task := range run.GetTaskRuns() {
		switch {
		case taskID != "" && task.GetTaskId() != taskID:
		case task.GetEach() == nil:
			timeline(task)
		default:
			for _, row := range task.GetItems() {
				if item == nil || row.GetItem() == *item {
					timeline(row)
				}
			}
		}
	}
	return timings
}

// queueWaitOf is attempt 1's started_at − scheduled_at: how long the task
// waited for an Orker of its pool before anything ran.
func queueWaitOf(taskRun *orkstackv1.TaskRun, rows []*orkstackv1.TaskAttempt, reclaimed bool) *orkstackv1.TimingQuantity {
	scheduled := instantOf(taskRun.GetScheduledAt())
	switch {
	case scheduled == nil:
		return unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_PRE_CONTRACT)
	case reclaimed:
		return unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_RECLAIMED)
	}
	for _, row := range rows {
		if row.GetAttempt() != 1 {
			continue
		}
		start := instantOf(row.GetStartedAt())
		if row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED || start == nil {
			break
		}
		return spanQuantity(*scheduled, *start)
	}
	return unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_NO_RECORD)
}

// setAttemptRowsTiming sets each row's timing and each of its phases'
// duration. rows are one timeline, ascending.
func setAttemptRowsTiming(taskRun *orkstackv1.TaskRun, rows []*orkstackv1.TaskAttempt, reclaimed bool) {
	terminal := statusIsTerminal(taskRun.GetStatus())
	for i, row := range rows {
		timing := &orkstackv1.AttemptTiming{}
		unreported := row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED
		switch {
		case unreported && !terminal && i == len(rows)-1:
			// Records are written when an attempt ENDS: the newest attempt of
			// a task still going has none yet, and its Orker did not crash.
			timing.InProgress = true
		case unreported && reclaimed:
			timing.PhasesReason = orkstackv1.TimingReason_TIMING_REASON_RECLAIMED
		case unreported:
			timing.PhasesReason = orkstackv1.TimingReason_TIMING_REASON_NO_RECORD
		case len(row.GetPhases()) == 0:
			timing.PhasesReason = orkstackv1.TimingReason_TIMING_REASON_NO_PHASES
		default:
			for _, phase := range row.GetPhases() {
				from, to := instantOf(phase.GetStartedAt()), instantOf(phase.GetFinishedAt())
				if from != nil && to != nil {
					phase.Duration = spanQuantity(*from, *to)
				} else {
					phase.Duration = unavailableQuantity(orkstackv1.TimingReason_TIMING_REASON_NO_RECORD)
				}
			}
		}
		row.Timing = timing
	}
}
