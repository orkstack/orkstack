package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The seven reasons a timing quantity can be unavailable. They are the whole
// vocabulary — the UI's ui/src/lib/timing.ts spells the same seven — and each
// names a cause an operator can act on, never a generic "unknown".
const (
	reasonNoOrkerStart   = "no orker reported a start"
	reasonClockSkew      = "clock skew"
	reasonPreContract    = "recorded before scheduled time existed"
	reasonReclaimed      = "attempt records reclaimed"
	reasonNoRecord       = "no attempt record"
	reasonNoPhases       = "record predates phase spans"
	reasonNoAttemptStore = "no task attempt store"
)

// timingReasons lists the seven, for the test that proves every one of them is
// printed by some scenario.
var timingReasons = []string{
	reasonNoOrkerStart, reasonClockSkew, reasonPreContract, reasonReclaimed,
	reasonNoRecord, reasonNoPhases, reasonNoAttemptStore,
}

// quantity is one derived duration in one of three states, and the three are
// never collapsed into each other:
//
//   - does not apply (the task was never scheduled, or is not terminal yet):
//     rendered "-" and ABSENT from JSON;
//   - unavailable (it happened and the evidence is gone or inconsistent):
//     rendered "unavailable" and null in JSON beside a <name>Reason;
//   - a value.
//
// A zero is only ever a value somebody measured. It is never how "not known"
// is spelled.
type quantity struct {
	applies bool
	known   bool
	value   time.Duration
	reason  string
}

func unknownBecause(reason string) quantity {
	return quantity{applies: true, reason: reason}
}

func measured(value time.Duration) quantity {
	return quantity{applies: true, known: true, value: value}
}

// span is to − from, or unknownBecause(clock skew) when the two ends are
// inverted: two clocks, one span, and a negative is never printed.
func span(from, to time.Time) quantity {
	if to.Before(from) {
		return unknownBecause(reasonClockSkew)
	}
	return measured(to.Sub(from))
}

// runTiming is the one derived document both renderings print: the human
// table and the --json object are two views of this, never two computations.
type runTiming struct {
	RunID          string
	Namespace      string
	OrkflowName    string
	OrkflowVersion string
	Status         string
	RunStartedAt   *time.Time
	RunFinishedAt  *time.Time
	RunElapsed     quantity

	WaitSum              quantity
	WaitSumTasks         int
	WaitUnavailableTasks int

	AttemptRecordsAvailable bool
	Tasks                   []*taskTiming
}

type taskTiming struct {
	TaskID  string
	Status  string
	Attempt int32
	Pool    string
	// FanOut marks a fan-out's own row: S is its expansion, B its earliest
	// item's start, and ATTEMPT reads "fan-out" — attempts belong to items.
	FanOut bool
	// Item is set on one item's row of a fan-out (listed under --task).
	Item *int32
	// Child marks a composed task: S is when the child run was requested, B
	// when its start was confirmed, F its end — WAIT is the child's start
	// latency, RUN its duration, and ATTEMPT reads "child": the attempts are
	// the child's tasks'.
	Child bool

	ScheduledAt *time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time

	Scheduled   bool // reached schedule(); false renders "-" across the row
	PreContract bool // scheduled_at unset with started_at set
	// Awaited marks an awaited task: it is never queued and no Orker reports
	// on it, so wait, queue wait and phases do not apply and RUN is the time
	// it spent awaiting (the run-timing derivation's await row).
	Awaited bool

	Wait           quantity
	Run            quantity
	Total          quantity
	SinceScheduled quantity // RUNNING only

	QueueWait      quantity
	QueueWaitStart *time.Time // attempt 1's start, when QueueWait is known

	// Attempts is newest first, the order the attempts block prints. When
	// AttemptsReason is set the list is unavailable as a whole.
	Attempts       []*attemptTiming
	AttemptsReason string
	// ReclaimedEmpty marks a task the snapshot says ran whose timeline came
	// back with no rows at all.
	ReclaimedEmpty bool
}

type attemptTiming struct {
	Attempt    int32
	Outcome    string
	StartedAt  *time.Time
	FinishedAt *time.Time
	Phases     []*phaseTiming
	// PhasesReason is set when the phases are unavailable: an UNREPORTED row,
	// a record written before phase spans existed, or reclaimed records.
	PhasesReason string
	// InProgress marks the newest attempt of a task that has not finished:
	// it has no record yet because records are written when an attempt ENDS,
	// and calling it unreported would say its Orker crashed.
	InProgress bool
}

type phaseTiming struct {
	Phase      string
	StartedAt  *time.Time
	FinishedAt *time.Time
	Duration   quantity
}

// timingOptions are computeRunTiming's inputs besides the two responses.
type timingOptions struct {
	// Task narrows the document to one task; empty means every task. A
	// fan-out named here also lists its items, one row each.
	Task string
	// Item narrows a fan-out Task to one item, with its attempts.
	Item *int32
	// AttemptStore is false when the engine answered ListTaskAttempts with
	// FailedPrecondition: there are no attempt records to read at all.
	AttemptStore bool
	// Now is the clock "since scheduled" is measured against.
	Now time.Time
}

func timeOf(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || !ts.IsValid() || ts.AsTime().IsZero() {
		return nil
	}
	at := ts.AsTime().UTC()
	return &at
}

func taskIsTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_SKIPPED:
		return true
	}
	return false
}

// computeRunTiming implements the task-timing derivation (docs/UI_CLI_PARITY.md
// "Task timing") over one GetRun snapshot and one ListTaskAttempts response.
// It is the only place in the CLI that subtracts a task's timestamps.
func computeRunTiming(
	run *orkstackv1.Run,
	attempts []*orkstackv1.TaskAttempt,
	opts timingOptions,
) *runTiming {
	namespace := run.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	timing := &runTiming{
		RunID:                   run.GetId(),
		Namespace:               namespace,
		OrkflowName:             run.GetOrkflowName(),
		OrkflowVersion:          run.GetOrkflowVersion(),
		Status:                  statusLabel(run.GetStatus()),
		RunStartedAt:            timeOf(run.GetStartedAt()),
		RunFinishedAt:           timeOf(run.GetFinishedAt()),
		AttemptRecordsAvailable: opts.AttemptStore,
	}
	if timing.RunStartedAt != nil && timing.RunFinishedAt != nil {
		timing.RunElapsed = span(*timing.RunStartedAt, *timing.RunFinishedAt)
	}

	byTask := map[string][]*orkstackv1.TaskAttempt{}
	for _, attempt := range attempts {
		key := itemLabel(attempt.GetTaskId(), attempt.Item)
		byTask[key] = append(byTask[key], attempt)
	}

	var waitSum time.Duration
	for _, taskRun := range run.GetTaskRuns() {
		if opts.Task != "" && taskRun.GetTaskId() != opts.Task {
			continue
		}
		task := computeTaskTiming(taskRun, byTask[taskRun.GetTaskId()], opts)
		timing.Tasks = append(timing.Tasks, task)
		// A fan-out named by --task lists its items, one row each, with the
		// per-item derivation; --item narrows to one.
		if taskRun.GetEach() != nil && opts.Task == taskRun.GetTaskId() {
			for _, itemRun := range taskRun.GetItems() {
				if opts.Item != nil && itemRun.GetItem() != *opts.Item {
					continue
				}
				itemTiming := computeTaskTiming(itemRun, byTask[itemLabel(itemRun.GetTaskId(), itemRun.Item)], opts)
				index := itemRun.GetItem()
				itemTiming.Item = &index
				timing.Tasks = append(timing.Tasks, itemTiming)
			}
		}
		switch {
		case !task.Wait.applies:
		case task.Wait.known:
			waitSum += task.Wait.value
			timing.WaitSumTasks++
		default:
			timing.WaitUnavailableTasks++
		}
	}
	switch {
	case timing.WaitSumTasks > 0:
		timing.WaitSum = measured(waitSum)
	case timing.WaitUnavailableTasks > 0:
		timing.WaitSum = quantity{applies: true}
	}
	return timing
}

func computeTaskTiming(
	taskRun *orkstackv1.TaskRun,
	rows []*orkstackv1.TaskAttempt,
	opts timingOptions,
) *taskTiming {
	task := &taskTiming{
		TaskID:      taskRun.GetTaskId(),
		Status:      statusLabel(taskRun.GetStatus()),
		Attempt:     taskRun.GetAttempt(),
		Pool:        taskRun.GetPool(),
		ScheduledAt: timeOf(taskRun.GetScheduledAt()),
		StartedAt:   timeOf(taskRun.GetStartedAt()),
		FinishedAt:  timeOf(taskRun.GetFinishedAt()),
	}
	S, B, F := task.ScheduledAt, task.StartedAt, task.FinishedAt
	terminal := taskIsTerminal(taskRun.GetStatus())
	if taskRun.GetEach() != nil {
		// A fan-out's own row: S its expansion, B
		// its earliest item's start, F its end. No attempts of its own.
		task.FanOut = true
		task.Scheduled = S != nil
		if !task.Scheduled {
			return task
		}
		switch {
		case !terminal:
			if taskRun.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
				task.SinceScheduled = span(*S, opts.Now)
			}
		case B == nil:
			task.Wait = unknownBecause(reasonNoOrkerStart)
			task.Run = unknownBecause(reasonNoOrkerStart)
		default:
			task.Wait = span(*S, *B)
			if F != nil {
				task.Run = span(*B, *F)
			}
		}
		if terminal && F != nil {
			task.Total = span(*S, *F)
		}
		return task
	}
	if taskRun.GetChild() != nil {
		task.Child = true
		task.Scheduled = S != nil
		if !task.Scheduled {
			return task
		}
		switch {
		case !terminal:
			if taskRun.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
				task.SinceScheduled = span(*S, opts.Now)
			}
		case B == nil:
			// No child run was started: there is no start latency and no
			// child duration to measure, so neither applies.
		default:
			task.Wait = span(*S, *B)
			if F != nil {
				task.Run = span(*B, *F)
			}
		}
		if terminal && F != nil {
			task.Total = span(*S, *F)
		}
		return task
	}
	if taskRun.GetAwait() != nil {
		// scheduled_at and started_at are both the instant it started
		// awaiting; an await that never opened has neither.
		task.Awaited = true
		task.Scheduled = B != nil
		if task.Scheduled && terminal && F != nil {
			task.Run = span(*B, *F)
			task.Total = task.Run
		}
		return task
	}
	task.Scheduled = S != nil || B != nil
	task.PreContract = S == nil && B != nil
	if !task.Scheduled {
		// Never scheduled: every quantity is "-", and there are no attempts.
		return task
	}

	switch {
	case task.PreContract:
		// B still holds the dispatch instant: there is no Orker start to
		// measure wait or run against, and total is the old dispatch-to-
		// finish span.
		task.Wait = unknownBecause(reasonPreContract)
		task.Run = unknownBecause(reasonPreContract)
		if terminal && F != nil {
			task.Total = span(*B, *F)
		}
	case !terminal:
		if taskRun.GetStatus() == orkstackv1.Status_STATUS_RUNNING && S != nil {
			task.SinceScheduled = span(*S, opts.Now)
		}
	default:
		if B == nil {
			task.Wait = unknownBecause(reasonNoOrkerStart)
			task.Run = unknownBecause(reasonNoOrkerStart)
		} else {
			task.Wait = span(*S, *B)
			if F != nil {
				task.Run = span(*B, *F)
			}
		}
		if F != nil {
			task.Total = span(*S, *F)
		}
	}

	task.QueueWait, task.QueueWaitStart = queueWaitOf(task, rows, terminal, opts)
	task.Attempts, task.AttemptsReason, task.ReclaimedEmpty = attemptsOf(task, rows, opts)
	return task
}

// recordsReclaimed says a task's timeline holds no record the Orker filed
// even though the snapshot proves an Orker reported on it. An Orker that
// reports a start (started_at) has always written a record for that attempt;
// a timeline with none left is one the cleanup worker reclaimed, not one whose
// every Orker crashed.
func recordsReclaimed(task *taskTiming, rows []*orkstackv1.TaskAttempt, terminal bool) bool {
	if len(rows) == 0 {
		return terminal
	}
	if task.StartedAt == nil {
		return false
	}
	for _, row := range rows {
		if row.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
			return false
		}
	}
	return true
}

func queueWaitOf(
	task *taskTiming,
	rows []*orkstackv1.TaskAttempt,
	terminal bool,
	opts timingOptions,
) (quantity, *time.Time) {
	switch {
	case !opts.AttemptStore:
		return unknownBecause(reasonNoAttemptStore), nil
	case task.PreContract:
		return unknownBecause(reasonPreContract), nil
	case recordsReclaimed(task, rows, terminal):
		return unknownBecause(reasonReclaimed), nil
	}
	for _, row := range rows {
		if row.GetAttempt() != 1 {
			continue
		}
		start := timeOf(row.GetStartedAt())
		if row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED || start == nil {
			return unknownBecause(reasonNoRecord), nil
		}
		wait := span(*task.ScheduledAt, *start)
		if !wait.known {
			return wait, nil
		}
		return wait, start
	}
	return unknownBecause(reasonNoRecord), nil
}

func attemptsOf(
	task *taskTiming,
	rows []*orkstackv1.TaskAttempt,
	opts timingOptions,
) ([]*attemptTiming, string, bool) {
	if !opts.AttemptStore {
		return nil, reasonNoAttemptStore, false
	}
	terminal := taskIsTerminal(statusFromLabel(task.Status))
	if len(rows) == 0 {
		return nil, "", terminal
	}
	reclaimed := recordsReclaimed(task, rows, terminal)
	out := make([]*attemptTiming, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		attempt := &attemptTiming{
			Attempt:    row.GetAttempt(),
			Outcome:    attemptOutcomeLabel(row.GetOutcome()),
			StartedAt:  timeOf(row.GetStartedAt()),
			FinishedAt: timeOf(row.GetFinishedAt()),
		}
		switch {
		case row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED &&
			!terminal && i == len(rows)-1:
			attempt.InProgress = true
		case row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED && reclaimed:
			attempt.PhasesReason = reasonReclaimed
		case row.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED:
			attempt.PhasesReason = reasonNoRecord
		case len(row.GetPhases()) == 0:
			attempt.PhasesReason = reasonNoPhases
		default:
			for _, phase := range row.GetPhases() {
				p := &phaseTiming{
					Phase:      phase.GetPhase(),
					StartedAt:  timeOf(phase.GetStartedAt()),
					FinishedAt: timeOf(phase.GetFinishedAt()),
				}
				if p.StartedAt != nil && p.FinishedAt != nil {
					p.Duration = span(*p.StartedAt, *p.FinishedAt)
				} else {
					p.Duration = unknownBecause(reasonNoRecord)
				}
				attempt.Phases = append(attempt.Phases, p)
			}
		}
		out = append(out, attempt)
	}
	return out, "", false
}

// statusFromLabel is statusLabel's inverse, for the one place a derived task
// needs its status back.
func statusFromLabel(label string) orkstackv1.Status {
	return orkstackv1.Status(orkstackv1.Status_value["STATUS_"+label])
}

// ---------------------------------------------------------------- human

// formatDuration is attemptDuration's spelling: Round(ms).String().
func formatDuration(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}

// cell renders a quantity for one table cell.
func (q quantity) cell() string {
	switch {
	case !q.applies:
		return dash
	case !q.known:
		return unavailable
	default:
		return formatDuration(q.value)
	}
}

const (
	retriedWaitNote = "WAIT is scheduling to the start of the attempt that produced the outcome; for a\n" +
		"retried task the earlier attempts are inside it. See the attempts below."
	preContractFooter = "this run was recorded before scheduled time existed; TOTAL is dispatch to finish"
	noStoreBlock      = "attempt records: not available on this engine (no task attempt store)"
	unreportedLine    = "unavailable (no attempt record: the orker crashed or lost its heartbeat)"
)

// renderRunTiming prints the timing table, the run summary (unless narrowed to
// one task) and the per-attempt block.
func renderRunTiming(out io.Writer, timing *runTiming, oneTask bool) error {
	table := newTable(out)
	fmt.Fprintln(table, "TASK\tSTATUS\tPOOL\tATTEMPT\tSCHEDULED\tWAIT\tRUN\tTOTAL")
	retried, preContract := false, false
	for _, task := range timing.Tasks {
		if task.Attempt > 1 {
			retried = true
		}
		if task.PreContract {
			preContract = true
		}
		total := task.Total.cell()
		if task.SinceScheduled.applies {
			total = "since scheduled " + task.SinceScheduled.cell()
		}
		run := task.Run.cell()
		if task.Awaited && task.Run.applies {
			run = "awaited " + run
		}
		attempt := fmt.Sprintf("%d", task.Attempt)
		if task.FanOut {
			attempt = "fan-out"
		}
		if task.Child {
			attempt = "child"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			itemLabel(task.TaskID, task.Item), task.statusCell(), orDash(task.Pool), attempt,
			formatTimePtr(task.ScheduledAt), task.Wait.cell(), run, total)
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	var b strings.Builder
	if !oneTask {
		b.WriteString("\n")
		b.WriteString(timing.summaryLine())
		b.WriteString("\n")
	}
	if retried {
		b.WriteString(retriedWaitNote + "\n")
	}
	if preContract {
		b.WriteString(preContractFooter + "\n")
	}
	block := timing.attemptsBlock()
	if block != "" {
		b.WriteString("\n")
		b.WriteString(block)
	}
	if _, err := io.WriteString(out, b.String()); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func (task *taskTiming) statusCell() string {
	// The same "(never started)" rule taskStatusLabel applies to `run get`.
	if task.Status == "CANCELED" && !task.Scheduled {
		return task.Status + " (never started)"
	}
	return task.Status
}

func formatTimePtr(at *time.Time) string {
	if at == nil {
		return dash
	}
	return at.UTC().Format(time.RFC3339)
}

func (timing *runTiming) summaryLine() string {
	line := "run " + formatTimePtr(timing.RunStartedAt)
	switch {
	case timing.RunFinishedAt == nil:
		line += " → running"
	default:
		line += " → " + formatTimePtr(timing.RunFinishedAt)
		if timing.RunElapsed.applies {
			line += " (" + timing.RunElapsed.cell() + ")"
		}
	}
	switch {
	case timing.WaitSumTasks > 0:
		line += " · task wait, summed: " + formatDuration(timing.WaitSum.value) +
			" over " + plural(int64(timing.WaitSumTasks), "task")
		if timing.WaitUnavailableTasks > 0 {
			line += fmt.Sprintf(" (%d unavailable)", timing.WaitUnavailableTasks)
		}
	case timing.WaitUnavailableTasks > 0:
		line += " · task wait, summed: unavailable"
	}
	return line
}

// attemptsBlock renders every scheduled task's attempts, newest first, each
// retried task's line preceded by its queue wait.
func (timing *runTiming) attemptsBlock() string {
	if !timing.AttemptRecordsAvailable {
		return noStoreBlock + "\n"
	}
	width := 0
	for _, task := range timing.Tasks {
		if label := itemLabel(task.TaskID, task.Item); task.Scheduled && !task.Awaited && !task.FanOut && !task.Child && len(label) > width {
			width = len(label)
		}
	}
	if width == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("attempts:\n")
	for _, task := range timing.Tasks {
		// An awaited task has no attempts and no phases, and neither does a
		// fan-out's own row (its items do): nothing to list.
		if !task.Scheduled || task.Awaited || task.FanOut || task.Child {
			continue
		}
		prefix := "  " + padRight(itemLabel(task.TaskID, task.Item), width) + "  "
		if task.Attempt > 1 {
			pool := orDash(task.Pool)
			if task.QueueWait.known {
				fmt.Fprintf(&b, "%squeue wait on pool %s %s (attempt 1 started %s)\n",
					prefix, pool, formatDuration(task.QueueWait.value), formatTimePtr(task.QueueWaitStart))
			} else {
				fmt.Fprintf(&b, "%squeue wait on pool %s: unavailable (%s)\n", prefix, pool, task.QueueWait.reason)
			}
		}
		if task.ReclaimedEmpty {
			attempt := task.Attempt
			if attempt < 1 {
				attempt = 1
			}
			fmt.Fprintf(&b, "%sattempt %d  unavailable (%s)\n", prefix, attempt, reasonReclaimed)
			continue
		}
		if len(task.Attempts) == 0 {
			// Not finished and nothing recorded yet: an attempt's record is
			// written when it ends.
			fmt.Fprintf(&b, "%sattempt %d  in progress\n", prefix, max(task.Attempt, 1))
			continue
		}
		for _, attempt := range task.Attempts {
			fmt.Fprintf(&b, "%sattempt %d  %s\n", prefix, attempt.Attempt, attempt.line())
		}
	}
	return b.String()
}

func padRight(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(value))
}

// outcomeWidth is the widest outcome label an attempt line carries
// ("UNREPORTED"), so the phases start in one column.
const outcomeWidth = len("UNREPORTED")

func (attempt *attemptTiming) line() string {
	if attempt.InProgress {
		return "in progress"
	}
	switch attempt.PhasesReason {
	case reasonNoRecord:
		return unreportedLine
	case reasonReclaimed:
		return "unavailable (" + reasonReclaimed + ")"
	}
	head := padRight(attempt.Outcome, outcomeWidth) + "  "
	if attempt.PhasesReason != "" {
		return head + "phases unavailable (" + attempt.PhasesReason + ")"
	}
	parts := make([]string, 0, len(attempt.Phases))
	for _, phase := range attempt.Phases {
		parts = append(parts, phase.Phase+" "+phase.Duration.cell())
	}
	return head + strings.Join(parts, "  ")
}

// ---------------------------------------------------------------- JSON

// jsonObject is an ordered JSON object: the document's key order is part of
// what its golden pins, and a map would lose it.
type jsonObject []jsonField

type jsonField struct {
	key   string
	value any
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, field := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(field.key)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		value, err := json.Marshal(field.value)
		if err != nil {
			return nil, err
		}
		b.Write(value)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func (o *jsonObject) add(key string, value any) { *o = append(*o, jsonField{key, value}) }

func (o *jsonObject) addTime(key string, at *time.Time) {
	if at != nil {
		o.add(key, at.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
	}
}

// addQuantity writes <name>Seconds (and <name>Reason when unavailable), or
// nothing at all when the quantity does not apply.
func (o *jsonObject) addQuantity(name string, q quantity) {
	if !q.applies {
		return
	}
	if q.known {
		o.add(name+"Seconds", q.value.Seconds())
		return
	}
	o.add(name+"Seconds", nil)
	if q.reason != "" {
		o.add(name+"Reason", q.reason)
	}
}

func (timing *runTiming) document() jsonObject {
	doc := jsonObject{}
	doc.add("runId", timing.RunID)
	doc.add("namespace", timing.Namespace)
	doc.add("orkflowName", timing.OrkflowName)
	doc.add("orkflowVersion", timing.OrkflowVersion)
	doc.add("status", timing.Status)
	doc.addTime("runStartedAt", timing.RunStartedAt)
	doc.addTime("runFinishedAt", timing.RunFinishedAt)
	doc.addQuantity("runElapsed", timing.RunElapsed)
	doc.addQuantity("waitSum", timing.WaitSum)
	doc.add("waitSumTasks", timing.WaitSumTasks)
	doc.add("waitUnavailableTasks", timing.WaitUnavailableTasks)
	doc.add("attemptRecordsAvailable", timing.AttemptRecordsAvailable)
	tasks := make([]jsonObject, 0, len(timing.Tasks))
	for _, task := range timing.Tasks {
		tasks = append(tasks, task.document())
	}
	doc.add("tasks", tasks)
	return doc
}

func (task *taskTiming) document() jsonObject {
	doc := jsonObject{}
	doc.add("taskId", task.TaskID)
	if task.Item != nil {
		doc.add("item", *task.Item)
	}
	doc.add("status", task.Status)
	doc.add("attempt", task.Attempt)
	if task.Pool != "" {
		doc.add("pool", task.Pool)
	}
	doc.addTime("scheduledAt", task.ScheduledAt)
	doc.addTime("startedAt", task.StartedAt)
	doc.addTime("finishedAt", task.FinishedAt)
	if task.PreContract {
		// The footer's machine form: this total is dispatch to finish.
		doc.add("preContract", true)
	}
	if task.Awaited {
		doc.add("awaited", true)
	}
	if task.FanOut {
		doc.add("fanOut", true)
	}
	if task.Child {
		doc.add("child", true)
	}
	doc.addQuantity("wait", task.Wait)
	doc.addQuantity("run", task.Run)
	doc.addQuantity("total", task.Total)
	doc.addQuantity("sinceScheduled", task.SinceScheduled)
	doc.addQuantity("queueWait", task.QueueWait)
	if task.AttemptsReason != "" {
		doc.add("attempts", nil)
		doc.add("attemptsReason", task.AttemptsReason)
		return doc
	}
	attempts := make([]jsonObject, 0, len(task.Attempts))
	for _, attempt := range task.Attempts {
		attempts = append(attempts, attempt.document())
	}
	doc.add("attempts", attempts)
	return doc
}

func (attempt *attemptTiming) document() jsonObject {
	doc := jsonObject{}
	doc.add("attempt", attempt.Attempt)
	doc.add("outcome", attempt.Outcome)
	doc.addTime("startedAt", attempt.StartedAt)
	doc.addTime("finishedAt", attempt.FinishedAt)
	if attempt.InProgress {
		doc.add("inProgress", true)
		return doc
	}
	if attempt.PhasesReason != "" {
		doc.add("phases", nil)
		doc.add("phasesReason", attempt.PhasesReason)
		return doc
	}
	phases := make([]jsonObject, 0, len(attempt.Phases))
	for _, phase := range attempt.Phases {
		p := jsonObject{}
		p.add("phase", phase.Phase)
		p.addTime("startedAt", phase.StartedAt)
		p.addTime("finishedAt", phase.FinishedAt)
		if phase.Duration.known {
			p.add("seconds", phase.Duration.value.Seconds())
		} else {
			p.add("seconds", nil)
			p.add("secondsReason", phase.Duration.reason)
		}
		phases = append(phases, p)
	}
	doc.add("phases", phases)
	return doc
}

func writeTimingJSON(out io.Writer, timing *runTiming) error {
	encoded, err := json.MarshalIndent(timing.document(), "", "  ")
	if err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	if _, err := out.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- command

// newRunTimingCommand builds `orkctl run timing`.
func newRunTimingCommand(g *globalFlags) *cobra.Command {
	var task string
	var item itemFlag
	cmd := &cobra.Command{
		Use:   "timing <run-id>",
		Short: "Show where each task's time went: waiting, running, and each phase",
		Long: "timing splits each task's time into WAIT (scheduling to the start of the\n" +
			"attempt that produced its outcome), RUN (that start to its finish) and\n" +
			"TOTAL, and lists the phases every attempt passed through.\n\n" +
			"A value that cannot be known — a host that died before reporting, a run\n" +
			"recorded before scheduled time existed, attempt records that were\n" +
			"reclaimed, two clocks that disagree — prints `unavailable`, never 0s. A\n" +
			"dash means the quantity does not apply: the task was never scheduled or\n" +
			"has not finished yet.\n\n" +
			"It reads the run snapshot and the per-attempt records; nothing is\n" +
			"computed on the server.\n\n" +
			"--json prints this command's own timing document (the table's data), not a\n" +
			"response envelope.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			selected, err := item.resolve(cmd, task)
			if err != nil {
				return err
			}
			timing, err := fetchRunTimingItem(cmd, g, args[0], task, selected)
			if err != nil {
				return err
			}
			if g.json {
				return writeTimingJSON(cmd.OutOrStdout(), timing)
			}
			return renderRunTiming(cmd.OutOrStdout(), timing, task != "")
		},
	}
	cmd.Flags().StringVar(&task, "task", "", "only this task, with its phases (a fan-out also lists its items)")
	item.register(cmd, "only this item of the fan-out --task names, with its attempts")
	return cmd
}

// fetchRunTiming composes GetRun and ListTaskAttempts, in that order, into
// the derived document. An engine without an attempt store still answers the
// snapshot half.
func fetchRunTiming(cmd *cobra.Command, g *globalFlags, runID, task string) (*runTiming, error) {
	return fetchRunTimingItem(cmd, g, runID, task, nil)
}

// fetchRunTimingItem is fetchRunTiming narrowed to one item of a fan-out.
func fetchRunTimingItem(cmd *cobra.Command, g *globalFlags, runID, task string, item *int32) (*runTiming, error) {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()

	runResp, err := g.runClient().GetRun(ctx, connect.NewRequest(
		&orkstackv1.GetRunRequest{RunId: runID, Namespace: g.namespace},
	))
	if err != nil {
		return nil, rpcError("get run", err)
	}
	run := runResp.Msg.GetRun()
	if task != "" && !runHasTask(run, task) {
		return nil, fmt.Errorf("task %q not found in run %q", task, run.GetId())
	}

	store := true
	var attempts []*orkstackv1.TaskAttempt
	attemptsResp, err := g.runClient().ListTaskAttempts(ctx, connect.NewRequest(
		&orkstackv1.ListTaskAttemptsRequest{RunId: runID, Namespace: g.namespace, TaskId: task, Item: item},
	))
	var connectErr *connect.Error
	switch {
	case err == nil:
		attempts = attemptsResp.Msg.GetAttempts()
	case errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition:
		store = false
	default:
		return nil, attemptsError(err)
	}
	return computeRunTiming(run, attempts, timingOptions{
		Task:         task,
		Item:         item,
		AttemptStore: store,
		Now:          time.Now().UTC(),
	}), nil
}

// appendRunTiming prints the timing block under a rendered `run get`.
func appendRunTiming(cmd *cobra.Command, g *globalFlags, runID string) error {
	timing, err := fetchRunTiming(cmd, g, runID, "")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout())
	return renderRunTiming(cmd.OutOrStdout(), timing, false)
}
