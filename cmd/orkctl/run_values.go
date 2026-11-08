package main

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// taskValuesInlineMax is the most of a task's values `run get` prints, in
// characters: the run page's own limit (INLINE_VALUES_MAX in
// ui/src/lib/runDetail.ts).
const taskValuesInlineMax = 64

// taskValuesRowsMax is the most tasks the values table lists, so a run of
// thousands of tasks keeps its other tables and footers on screen. The line
// after the table names the command that prints the rest.
const taskValuesRowsMax = 50

// emptyValuesBytes is the most an empty values.json takes: `{}` and a line end,
// which is what `echo '{}'` writes. A task with no more has no row, and an item
// no read.
const emptyValuesBytes = 4

// itemValuesReadMax is the largest item value --expand reads: the 4 KiB a run
// carries inline for a task. A larger one is treated as a task's is, and named.
const itemValuesReadMax = 4096

// plainShellWord is what a shell reads as itself.
var plainShellWord = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// renderTaskValues is one row per task that returned something, awaited or not,
// in run order. A run with none prints nothing new. It is a table of its own
// because a column in the task table would widen every row by a value.
func renderTaskValues(out io.Writer, run *orkstackv1.Run) error {
	var rows [][2]string
	left := 0
	for _, taskRun := range run.GetTaskRuns() {
		cell := taskValuesCell(run, taskRun)
		switch {
		case cell == "":
		case len(rows) == taskValuesRowsMax:
			left++
		default:
			rows = append(rows, [2]string{taskRun.GetTaskId(), cell})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "TASK\tVALUES")
	for _, row := range rows {
		fmt.Fprintf(table, "%s\t%s\n", row[0], row[1])
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if left > 0 {
		noun := "tasks hold"
		if left == 1 {
			noun = "task holds"
		}
		fmt.Fprintf(out, "%d more %s values; %s prints them\n", left, noun,
			orkctlCommand(run.GetNamespace(), "run", "get", run.GetId(), "--json"))
	}
	return nil
}

// taskValuesCell is what the values table says of one task, or nothing when it
// has no values to show. The run carries a value of at most 4 KiB inline; a
// larger one is only a reference, and run get names the command rather than
// fetching it. What a task wrote reaches the terminal only as an escape.
func taskValuesCell(run *orkstackv1.Run, taskRun *orkstackv1.TaskRun) string {
	ref := taskRun.GetValuesRef()
	data := taskRun.GetOutput().GetData()
	if len(data.GetFields()) == 0 {
		if ref.GetSizeBytes() <= emptyValuesBytes {
			return ""
		}
		return valuesNotShown(run, taskRun)
	}
	line, err := compactValuesLine(data)
	if err != nil {
		return ""
	}
	return valuesLineCell(run, taskRun, line)
}

// valuesLineCell is a line of values as a row shows it: whole when it fits, or
// cut and followed by the command that gets the rest.
func valuesLineCell(run *orkstackv1.Run, taskRun *orkstackv1.TaskRun, line string) string {
	if utf8.RuneCountInString(line) <= taskValuesInlineMax {
		return line
	}
	cut := cutJSONLine(line, taskValuesInlineMax)
	ref := taskRun.GetValuesRef()
	if ref.GetName() == "" {
		return fmt.Sprintf("%s… (%d bytes; %s prints them whole)", cut, len(line),
			orkctlCommand(run.GetNamespace(), "run", "get", run.GetId(), "--json"))
	}
	return fmt.Sprintf("%s… (%d bytes; %s saves them whole)", cut, ref.GetSizeBytes(), taskValuesDownload(run, taskRun))
}

// valuesNotShown is the row of a value run get does not fetch.
func valuesNotShown(run *orkstackv1.Run, taskRun *orkstackv1.TaskRun) string {
	return fmt.Sprintf("not shown (%d bytes; %s saves them)", taskRun.GetValuesRef().GetSizeBytes(), taskValuesDownload(run, taskRun))
}

// itemValuesReader reads one fan-out item's stored values.
type itemValuesReader func(item *orkstackv1.TaskRun) ([]byte, error)

// renderFanOutItemValues is --expand's table of what each item returned. The
// run holds only a reference to an item's values, so it reads them, one request
// per row: at most taskValuesRowsMax, and none once a read has failed, so a store
// that is down costs one timeout rather than one per item.
func renderFanOutItemValues(out io.Writer, run *orkstackv1.Run, task *orkstackv1.TaskRun, read itemValuesReader) error {
	var rows [][2]string
	left := 0
	var firstLeft *orkstackv1.TaskRun
	storeAnswers := true
	for _, item := range task.GetItems() {
		ref := item.GetValuesRef()
		switch {
		case ref.GetSizeBytes() <= emptyValuesBytes:
			continue
		case len(rows) == taskValuesRowsMax:
			if left++; firstLeft == nil {
				firstLeft = item
			}
			continue
		}
		cell := valuesNotShown(run, item)
		if storeAnswers && ref.GetSizeBytes() <= itemValuesReadMax {
			raw, err := read(item)
			if err != nil {
				storeAnswers = false
				cell = fmt.Sprintf("not read (%s; %s saves them)", terminalSafe(err.Error()), taskValuesDownload(run, item))
			} else if line, err := compactStoredValues(raw); err != nil {
				cell = fmt.Sprintf("not read (the stored values are not JSON; %s saves them)", taskValuesDownload(run, item))
			} else {
				cell = valuesLineCell(run, item, line)
			}
		}
		rows = append(rows, [2]string{strconv.Itoa(int(item.GetItem())), cell})
	}
	if len(rows) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "ITEM\tVALUES")
	for _, row := range rows {
		fmt.Fprintf(table, "%s\t%s\n", row[0], row[1])
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if left > 0 {
		noun := "items hold"
		if left == 1 {
			noun = "item holds"
		}
		fmt.Fprintf(out, "%d more %s values; %s saves one\n", left, noun, taskValuesDownload(run, firstLeft))
	}
	return nil
}

// taskValuesDownload is the command that saves one task's or item's values.
func taskValuesDownload(run *orkstackv1.Run, taskRun *orkstackv1.TaskRun) string {
	name := terminalSafe(taskRun.GetValuesRef().GetName())
	if !plainShellWord.MatchString(name) {
		name = shellQuote(name)
	}
	args := []string{"run", "artifact", "download", run.GetId(), "--task", taskRun.GetTaskId()}
	output := taskRun.GetTaskId() + "-values.json"
	if taskRun.Item != nil {
		index := strconv.Itoa(int(taskRun.GetItem()))
		args = append(args, "--item", index)
		output = taskRun.GetTaskId() + "-" + index + "-values.json"
	}
	return orkctlCommand(run.GetNamespace(), append(args, "--kind", "values", "--name", name, "--output", output)...)
}

// cutJSONLine is the start of a line of encoded JSON, at most limit characters,
// stopping before an escape that would not fit whole: half of one reads as
// text, and half of a surrogate pair as a character that is not in the value.
func cutJSONLine(line string, limit int) string {
	count := 0
	for at := 0; at < len(line); {
		width, size := jsonCharWidth(line[at:])
		if count+width > limit {
			return line[:at]
		}
		count += width
		at += size
	}
	return line
}

// jsonCharWidth is the first character of encoded JSON as a reader sees it: the
// characters it prints as and the bytes it takes.
func jsonCharWidth(rest string) (width, size int) {
	if rest[0] != '\\' {
		_, size = utf8.DecodeRuneInString(rest)
		return 1, size
	}
	const short, unicodeEscape = 2, 6
	if !strings.HasPrefix(rest, `\u`) || len(rest) < unicodeEscape {
		return short, min(short, len(rest))
	}
	if isHighSurrogateEscape(rest) && len(rest) >= 2*unicodeEscape && strings.HasPrefix(rest[unicodeEscape:], `\u`) {
		return 2 * unicodeEscape, 2 * unicodeEscape
	}
	return unicodeEscape, unicodeEscape
}

// isHighSurrogateEscape reports whether a \u escape names the first half of a
// surrogate pair, \ud800 to \udbff.
func isHighSurrogateEscape(escape string) bool {
	return (escape[2] == 'd' || escape[2] == 'D') && strings.ContainsRune("89abAB", rune(escape[3]))
}
