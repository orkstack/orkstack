package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Fan-out in the CLI (docs/UI_CLI_PARITY.md "Fan-out"). An item is addressed by its
// 0-based index with --item N, read only when the flag was given, and a human
// rendering spells one item convert[97] — a rendering only, never sent and
// never parsed. --json output is always the unchanged response envelope, which
// carries `item` and `items` as protojson names them.

// itemFlag is one command's --item.
type itemFlag struct {
	value int32
}

// register adds --item to cmd.
func (f *itemFlag) register(cmd *cobra.Command, usage string) {
	cmd.Flags().Int32Var(&f.value, "item", 0, usage)
}

// resolve returns the item to send, or nil when --item was not given. A
// negative value, or --item without --task, is refused before any request,
// in the server's own sentences.
func (f *itemFlag) resolve(cmd *cobra.Command, taskID string) (*int32, error) {
	if !cmd.Flags().Changed("item") {
		return nil, nil
	}
	if taskID == "" {
		return nil, fmt.Errorf("item requires task_id: name the fan-out with --task")
	}
	if f.value < 0 {
		return nil, fmt.Errorf("item %d is negative; items are numbered from 0", f.value)
	}
	value := f.value
	return &value, nil
}

// itemLabel renders one item as convert[97]; a task's own row is its id.
func itemLabel(taskID string, item *int32) string {
	if item == nil {
		return taskID
	}
	return fmt.Sprintf("%s[%d]", taskID, *item)
}

// itemCell renders the ITEM column: the index, or a dash for a task's own.
func itemCell(item *int32) string {
	if item == nil {
		return dash
	}
	return fmt.Sprintf("%d", *item)
}

// eachProgress tallies a fan-out's items by status, and how many retried.
type eachProgress struct {
	count, succeeded, failed, canceled, running, pending, retried, cached int32
}

func progressOf(task *orkstackv1.TaskRun) eachProgress {
	p := eachProgress{count: task.GetEach().GetCount()}
	for _, item := range task.GetItems() {
		switch item.GetStatus() {
		case orkstackv1.Status_STATUS_SUCCEEDED:
			p.succeeded++
		case orkstackv1.Status_STATUS_FAILED:
			p.failed++
		case orkstackv1.Status_STATUS_CANCELED:
			p.canceled++
		case orkstackv1.Status_STATUS_RUNNING:
			p.running++
		default:
			p.pending++
		}
		if item.GetAttempt() > 1 {
			p.retried++
		}
		if item.GetCache().GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			p.cached++
		}
	}
	return p
}

// fanOutStatusLabel is a fan-out row's STATUS: its status, then the settled
// fraction succeeded/count once expanded (RUNNING 97/120, SUCCEEDED 0/0).
func fanOutStatusLabel(task *orkstackv1.TaskRun) string {
	label := taskStatusLabel(task)
	if !task.GetEach().GetExpanded() {
		return label
	}
	p := progressOf(task)
	return fmt.Sprintf("%s %d/%d", label, p.succeeded, p.count)
}

// fanOutSummaryLine is the line under the task table for one expanded
// fan-out: "convert: 120 items — 120 succeeded, 2 retried. Expand with: …".
func fanOutSummaryLine(namespace, runID string, task *orkstackv1.TaskRun) string {
	p := progressOf(task)
	parts := []string{}
	for _, part := range []struct {
		n    int32
		word string
	}{
		{p.succeeded, "succeeded"}, {p.failed, "failed"}, {p.canceled, "canceled"},
		{p.running, "running"}, {p.pending, "pending"},
	} {
		if part.n > 0 || part.word == "succeeded" {
			parts = append(parts, fmt.Sprintf("%d %s", part.n, part.word))
		}
	}
	parts = append(parts, fmt.Sprintf("%d retried", p.retried))
	if p.cached > 0 {
		parts = append(parts, fmt.Sprintf("%d cached", p.cached))
	}
	noun := "items"
	if p.count == 1 {
		noun = "item"
	}
	return fmt.Sprintf("%s: %d %s — %s. Expand with: %s", task.GetTaskId(), p.count, noun,
		strings.Join(parts, ", "), orkctlCommand(namespace, "run", "get", runID, "--expand", task.GetTaskId()))
}

// renderFanOutItems is --expand's table for one fan-out.
func renderFanOutItems(out io.Writer, task *orkstackv1.TaskRun) error {
	fmt.Fprintln(out)
	fmt.Fprintf(out, "%s — %d items\n", task.GetTaskId(), task.GetEach().GetCount())
	table := newTable(out)
	fmt.Fprintln(table, "ITEM\tSTATUS\tORKER\tATTEMPT\tEXIT\tSCHEDULED\tSTARTED\tFINISHED\tERROR")
	for _, item := range task.GetItems() {
		attempt, exit := dash, dash
		if item.GetAttempt() > 0 {
			attempt = fmt.Sprintf("%d", item.GetAttempt())
		}
		if item.GetOrkerId() != "" {
			exit = fmt.Sprintf("%d", item.GetExitCode())
		}
		fmt.Fprintf(table, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			item.GetItem(), taskStatusLabel(item), orDash(item.GetOrkerId()), attempt, exit,
			formatTime(item.GetScheduledAt()), formatTime(item.GetStartedAt()),
			formatTime(item.GetFinishedAt()), errorLine(item.GetError()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// fanOutRow finds a fan-out's row in a run, or nil.
func fanOutRow(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == taskID && task.GetEach() != nil {
			return task
		}
	}
	return nil
}

// eachDetail is a plan's DETAIL for a fan-out: "each over <over>:
// 3 items (≤ 50, 2 at a time)" when counted, "each over <over> (≤ 500 items;
// counted when the run reaches it)" otherwise.
func eachDetail(task *orkstackv1.PlanTask) string {
	spec := task.GetEach()
	if spec == nil {
		return ""
	}
	parallel := ""
	if spec.GetMaxParallel() > 0 {
		parallel = fmt.Sprintf(", %d at a time", spec.GetMaxParallel())
	}
	if task.EachCount != nil {
		noun := "items"
		if task.GetEachCount() == 1 {
			noun = "item"
		}
		return fmt.Sprintf("each over %s: %d %s (≤ %d%s)", spec.GetOver(), task.GetEachCount(), noun, spec.GetMaxItems(), parallel)
	}
	return fmt.Sprintf("each over %s (≤ %d items%s; counted when the run reaches it)", spec.GetOver(), spec.GetMaxItems(), parallel)
}
