package main

import (
	"fmt"
	"io"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// sleepRemaining is REMAINING: wake_at − now for a SLEEPING task, "waking"
// once it has passed (the run has yet to record the wake), and "-"
// otherwise. ui/src/lib/sleeps.ts sleepRemaining renders the same words.
func sleepRemaining(taskRun *orkstackv1.TaskRun, now time.Time) string {
	wakeAt := taskRun.GetSleep().GetWakeAt()
	if taskRun.GetStatus() != orkstackv1.Status_STATUS_SLEEPING || wakeAt == nil {
		return dash
	}
	left := wakeAt.AsTime().Sub(now)
	if left < 0 {
		return "waking"
	}
	return formatRemaining(left)
}

// renderSleepTable prints the sleep table under `run get`'s task table, only
// when the run has a sleeping task: how long each sleeps, when it wakes and
// how long is left.
func renderSleepTable(out io.Writer, run *orkstackv1.Run, now time.Time) error {
	hasSleep := false
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetSleep() != nil {
			hasSleep = true
		}
	}
	if !hasSleep {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "SLEEP\tSTATUS\tFOR\tWAKES\tREMAINING")
	for _, taskRun := range run.GetTaskRuns() {
		sleep := taskRun.GetSleep()
		if sleep == nil {
			continue
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n",
			taskRun.GetTaskId(), taskStatusLabel(taskRun),
			(time.Duration(sleep.GetSeconds()) * time.Second).String(),
			formatTime(sleep.GetWakeAt()), sleepRemaining(taskRun, now))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
