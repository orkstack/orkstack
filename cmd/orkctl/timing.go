package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// The seven reasons a timing quantity can be unavailable, in the words the
// CLI prints for orkstackv1.TimingReason. The engine names the reason; the UI
// (ui/src/lib/timing.ts) spells the same seven.
const (
	reasonNoOrkerStart   = "no orker reported a start"
	reasonClockSkew      = "clock skew"
	reasonPreContract    = "recorded before scheduled time existed"
	reasonReclaimed      = "attempt records reclaimed"
	reasonNoRecord       = "no attempt record"
	reasonNoPhases       = "record predates phase spans"
	reasonNoAttemptStore = "no task attempt store"
)

var timingReasonWords = map[orkstackv1.TimingReason]string{
	orkstackv1.TimingReason_TIMING_REASON_NO_ORKER_START:   reasonNoOrkerStart,
	orkstackv1.TimingReason_TIMING_REASON_CLOCK_SKEW:       reasonClockSkew,
	orkstackv1.TimingReason_TIMING_REASON_PRE_CONTRACT:     reasonPreContract,
	orkstackv1.TimingReason_TIMING_REASON_RECLAIMED:        reasonReclaimed,
	orkstackv1.TimingReason_TIMING_REASON_NO_RECORD:        reasonNoRecord,
	orkstackv1.TimingReason_TIMING_REASON_NO_PHASES:        reasonNoPhases,
	orkstackv1.TimingReason_TIMING_REASON_NO_ATTEMPT_STORE: reasonNoAttemptStore,
}

// timingReasons lists the seven, for the test that proves every one of them is
// printed by some scenario.
var timingReasons = []string{
	reasonNoOrkerStart, reasonClockSkew, reasonPreContract, reasonReclaimed,
	reasonNoRecord, reasonNoPhases, reasonNoAttemptStore,
}

// quantity is one derived duration (orkstackv1.TimingQuantity) in one of
// three states, never collapsed into each other:
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

// quantityOf reads the engine's quantity: unset does not apply, a duration is
// a value, and a reason alone is unavailable.
func quantityOf(q *orkstackv1.TimingQuantity) quantity {
	switch {
	case q == nil:
		return quantity{}
	case q.GetDuration() != nil:
		return quantity{applies: true, known: true, value: q.GetDuration().AsDuration()}
	default:
		return unknownBecause(timingReasonWords[q.GetReason()])
	}
}

// runTiming is the one document both renderings print: the human table and
// the --json object are two views of the timing GetRun and ListTaskAttempts
// answered, which the engine derived (internal/api/timing.go).
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
	// on it, so wait, the wait for an Orker and phases do not apply and RUN is the time
	// it spent awaiting (the run-timing derivation's await row).
	Awaited bool
	// Slept marks a sleeping task, on the await's terms: RUN is the time it
	// slept.
	Slept bool

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

// timingOptions narrow the document.
type timingOptions struct {
	// Task narrows the document to one task; empty means every task. A
	// fan-out named here also lists its items, one row each.
	Task string
	// Item narrows a fan-out Task to one item, with its attempts.
	Item *int32
}

func timeOf(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || !ts.IsValid() || ts.AsTime().IsZero() {
		return nil
	}
	at := ts.AsTime().UTC()
	return &at
}

// timelineKey names one timeline: a task, or one item of a fan-out.
func timelineKey(taskID string, item *int32) string { return itemLabel(taskID, item) }

// buildRunTiming lays out the timing GetRun (run) and ListTaskAttempts
// (attempts; nil when the engine has no attempt store) answered as the
// document the CLI prints. It subtracts nothing: every quantity is the
// engine's.
func buildRunTiming(run *orkstackv1.Run, attempts *orkstackv1.ListTaskAttemptsResponse, opts timingOptions) *runTiming {
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
		RunElapsed:              quantityOf(run.GetTiming().GetElapsed()),
		AttemptRecordsAvailable: attempts != nil,
	}
	rowsOf := map[string][]*orkstackv1.TaskAttempt{}
	for _, attempt := range attempts.GetAttempts() {
		key := timelineKey(attempt.GetTaskId(), attempt.Item)
		rowsOf[key] = append(rowsOf[key], attempt)
	}
	timingsOf := map[string]*orkstackv1.TaskAttemptsTiming{}
	for _, t := range attempts.GetTimings() {
		timingsOf[timelineKey(t.GetTaskId(), t.Item)] = t
	}
	row := func(taskRun *orkstackv1.TaskRun) *taskTiming {
		key := timelineKey(taskRun.GetTaskId(), taskRun.Item)
		return taskTimingOf(taskRun, rowsOf[key], timingsOf[key], attempts != nil)
	}

	for _, taskRun := range run.GetTaskRuns() {
		if opts.Task != "" && taskRun.GetTaskId() != opts.Task {
			continue
		}
		task := row(taskRun)
		timing.Tasks = append(timing.Tasks, task)
		// A fan-out named by --task lists its items, one row each; --item
		// narrows to one.
		if taskRun.GetEach() != nil && opts.Task == taskRun.GetTaskId() {
			for _, itemRun := range taskRun.GetItems() {
				if opts.Item != nil && itemRun.GetItem() != *opts.Item {
					continue
				}
				itemTiming := row(itemRun)
				index := itemRun.GetItem()
				itemTiming.Item = &index
				timing.Tasks = append(timing.Tasks, itemTiming)
			}
		}
	}

	// The run's summed wait is the engine's; narrowed to one task it is that
	// task's own wait.
	if opts.Task == "" {
		runTiming := run.GetTiming()
		timing.WaitSum = quantityOf(runTiming.GetWaitSum())
		timing.WaitSumTasks = int(runTiming.GetWaitSumTasks())
		timing.WaitUnavailableTasks = int(runTiming.GetWaitUnavailableTasks())
	} else if len(timing.Tasks) > 0 {
		switch wait := timing.Tasks[0].Wait; {
		case !wait.applies:
		case wait.known:
			timing.WaitSum, timing.WaitSumTasks = wait, 1
		default:
			timing.WaitSum, timing.WaitUnavailableTasks = quantity{applies: true}, 1
		}
	}
	return timing
}

// taskTimingOf lays out one row from its snapshot (with the engine's
// TaskTiming), its attempt rows and its TaskAttemptsTiming.
func taskTimingOf(
	taskRun *orkstackv1.TaskRun,
	rows []*orkstackv1.TaskAttempt,
	attemptsTiming *orkstackv1.TaskAttemptsTiming,
	store bool,
) *taskTiming {
	derived := taskRun.GetTiming()
	task := &taskTiming{
		TaskID:         taskRun.GetTaskId(),
		Status:         statusLabel(taskRun.GetStatus()),
		Attempt:        taskRun.GetAttempt(),
		Pool:           taskRun.GetPool(),
		FanOut:         taskRun.GetEach() != nil,
		Child:          taskRun.GetChild() != nil,
		Awaited:        taskRun.GetAwait() != nil,
		Slept:          taskRun.GetSleep() != nil,
		ScheduledAt:    timeOf(taskRun.GetScheduledAt()),
		StartedAt:      timeOf(taskRun.GetStartedAt()),
		FinishedAt:     timeOf(taskRun.GetFinishedAt()),
		Scheduled:      derived.GetScheduled(),
		PreContract:    derived.GetPreContract(),
		Wait:           quantityOf(derived.GetWait()),
		Run:            quantityOf(derived.GetRun()),
		Total:          quantityOf(derived.GetTotal()),
		SinceScheduled: quantityOf(derived.GetSinceScheduled()),
	}
	// Only a scheduled container task has attempts, phases and a wait for an
	// Orker.
	if !task.Scheduled || task.FanOut || task.Child || task.Awaited || task.Slept {
		return task
	}
	if !store {
		task.QueueWait = unknownBecause(reasonNoAttemptStore)
		task.AttemptsReason = reasonNoAttemptStore
		return task
	}
	task.QueueWait = quantityOf(attemptsTiming.GetQueueWait())
	task.ReclaimedEmpty = attemptsTiming.GetReclaimed() && len(rows) == 0
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		if row.GetAttempt() == 1 && task.QueueWait.known {
			task.QueueWaitStart = timeOf(row.GetStartedAt())
		}
		attempt := &attemptTiming{
			Attempt:      row.GetAttempt(),
			Outcome:      attemptOutcomeLabel(row.GetOutcome()),
			StartedAt:    timeOf(row.GetStartedAt()),
			FinishedAt:   timeOf(row.GetFinishedAt()),
			InProgress:   row.GetTiming().GetInProgress(),
			PhasesReason: timingReasonWords[row.GetTiming().GetPhasesReason()],
		}
		if !attempt.InProgress && attempt.PhasesReason == "" {
			for _, phase := range row.GetPhases() {
				attempt.Phases = append(attempt.Phases, &phaseTiming{
					Phase:      phase.GetPhase(),
					StartedAt:  timeOf(phase.GetStartedAt()),
					FinishedAt: timeOf(phase.GetFinishedAt()),
					Duration:   quantityOf(phase.GetDuration()),
				})
			}
		}
		task.Attempts = append(task.Attempts, attempt)
	}
	return task
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
		if task.Slept && task.Run.applies {
			run = "slept " + run
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
// retried task's line preceded by how long attempt 1 waited for an Orker.
func (timing *runTiming) attemptsBlock() string {
	if !timing.AttemptRecordsAvailable {
		return noStoreBlock + "\n"
	}
	width := 0
	for _, task := range timing.Tasks {
		if label := itemLabel(task.TaskID, task.Item); task.Scheduled && !task.Awaited && !task.Slept && !task.FanOut && !task.Child && len(label) > width {
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
		if !task.Scheduled || task.Awaited || task.Slept || task.FanOut || task.Child {
			continue
		}
		prefix := "  " + padRight(itemLabel(task.TaskID, task.Item), width) + "  "
		if task.Attempt > 1 {
			pool := orDash(task.Pool)
			if task.QueueWait.known {
				fmt.Fprintf(&b, "%swaited for an Orker of pool %s %s (attempt 1 started %s)\n",
					prefix, pool, formatDuration(task.QueueWait.value), formatTimePtr(task.QueueWaitStart))
			} else {
				fmt.Fprintf(&b, "%swait for an Orker of pool %s: unavailable (%s)\n", prefix, pool, task.QueueWait.reason)
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
		key, err := stablejson.Marshal(field.key)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		value, err := stablejson.Marshal(field.value)
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
	if task.Slept {
		doc.add("slept", true)
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
	return writeGoJSON(out, timing.document())
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
			"Every quantity is the engine's: it derives them from the run snapshot\n" +
			"and the per-attempt records, and this command prints them.\n\n" +
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
	cmd.Flags().StringVarP(&task, "task", "t", "", "only this task, with its phases (a fan-out also lists its items)")
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

	var attempts *orkstackv1.ListTaskAttemptsResponse
	attemptsResp, err := g.runClient().ListTaskAttempts(ctx, connect.NewRequest(
		&orkstackv1.ListTaskAttemptsRequest{RunId: runID, Namespace: g.namespace, TaskId: task, Item: item},
	))
	var connectErr *connect.Error
	switch {
	case err == nil:
		attempts = attemptsResp.Msg
	case errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition:
		// No attempt store: the snapshot half still prints.
	default:
		return nil, attemptsError(err)
	}
	return buildRunTiming(run, attempts, timingOptions{Task: task, Item: item}), nil
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
