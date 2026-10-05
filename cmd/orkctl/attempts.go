package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// newRunAttemptsCommand builds `orkctl run attempts`.
//
// Temporal owns retries, so a run snapshot only ever carries a task's FINAL
// outcome: the try that failed and was retried away leaves no trace in
// `run get`. This command reads the per-attempt records the Orker filed and
// prints the timeline they make, including the attempts nothing filed a record
// for (§2.7).
func newRunAttemptsCommand(g *globalFlags) *cobra.Command {
	var task string
	var item itemFlag
	cmd := &cobra.Command{
		Use:   "attempts <run-id>",
		Short: "List every attempt of a run's tasks, including retried-away ones",
		Long: "attempts prints one row per try of each task of a run, oldest first.\n\n" +
			"A retry is invisible in `run get`: Temporal keeps only the final outcome\n" +
			"of a task, so the try that failed and was retried away is not in the run\n" +
			"snapshot. This reads the record the Orker filed for each attempt instead,\n" +
			"which is where the failed try's error, phase, exit code and Orker live.\n\n" +
			"An attempt whose Orker died before it could file one is still listed, as\n" +
			"UNREPORTED with dashes: the timeline never has a silent hole, and an\n" +
			"unreported attempt is an absent record rather than a failure.\n\n" +
			"With --task the full error of every failed attempt is printed under the\n" +
			"table, each with the command that prints that attempt's output.\n\n" +
			"Records are read from the artifact store, never from the engine's memory,\n" +
			"so any replica answers — including one that never started the run.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			selected, err := item.resolve(cmd, task)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.runClient().ListTaskAttempts(ctx, connect.NewRequest(
				&orkstackv1.ListTaskAttemptsRequest{
					RunId:     args[0],
					Namespace: g.namespace,
					TaskId:    task,
					Item:      selected,
				}))
			if err != nil {
				return attemptsError(err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			attempts := response.Msg.GetAttempts()
			if err := renderAttemptList(cmd.OutOrStdout(), attempts); err != nil {
				return err
			}
			if task == "" {
				return nil
			}
			return renderAttemptFailures(cmd.OutOrStdout(), g.namespace, args[0], attempts)
		},
	}
	cmd.Flags().StringVar(&task, "task", "",
		"only this task's attempts, with the full error of every failed one")
	item.register(cmd, "only this item of the fan-out --task names (0-based)")
	return cmd
}

// attemptsError renders a failed ListTaskAttempts as a CLI error.
//
// FailedPrecondition is the one code that is not about this request: it says
// the engine was built without an attempt store, so no run has a retry history
// to show and no argument the caller could have typed would change that.
// Reported through rpcError it would read as "list task attempts:
// failed_precondition: …", which sends an operator looking for the run id they
// got wrong. Everything else — an unknown run or task (NotFound), an
// unreachable store (Unavailable) — is about this request and keeps the shared
// rendering.
func attemptsError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition {
		return fmt.Errorf(
			"attempt history is not available: this engine has no task attempt store " +
				"configured, so no per-attempt records are kept")
	}
	return rpcError("list task attempts", err)
}

// renderAttemptList prints the attempt timeline as a table.
//
// The order is the server's contract (tasks in run order, then attempt
// ascending, gapless from 1) and is rendered as given: re-sorting here would
// break the one property that makes the table readable as a history.
func renderAttemptList(out io.Writer, attempts []*orkstackv1.TaskAttempt) error {
	if len(attempts) == 0 {
		// Not an error: a task that was never dispatched has no attempts, and
		// so does a run whose records the cleanup worker has reclaimed.
		_, err := fmt.Fprintln(out, "No attempts.")
		return err
	}
	table := newTable(out)
	// The ITEM column appears whenever any record is an item's (fan-out); a
	// run with no fan-out keeps the table it always had.
	withItems := false
	// The CACHE column appears whenever any record carries a memo decision
	// (a cached task); with none the table is unchanged.
	withCache := false
	for _, attempt := range attempts {
		withItems = withItems || attempt.Item != nil
		withCache = withCache || attempt.GetCache() != nil
	}
	cacheHeader := ""
	if withCache {
		cacheHeader = "\tCACHE"
	}
	if withItems {
		fmt.Fprintln(table, "TASK\tITEM\tATTEMPT\tOUTCOME\tPHASE\tEXIT\tSTARTED\tDURATION\tORKER"+cacheHeader+"\tERROR")
	} else {
		fmt.Fprintln(table, "TASK\tATTEMPT\tOUTCOME\tPHASE\tEXIT\tSTARTED\tDURATION\tORKER"+cacheHeader+"\tERROR")
	}
	unreported := false
	for _, attempt := range attempts {
		if attempt.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
			unreported = true
		}
		task := attempt.GetTaskId()
		if withItems {
			task += "\t" + itemCell(attempt.Item)
		}
		orker := orDash(attempt.GetOrkerId())
		if withCache {
			orker += "\t" + attemptCacheCell(attempt.GetCache())
		}
		fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			task,
			attempt.GetAttempt(),
			attemptOutcomeLabel(attempt.GetOutcome()),
			orDash(attempt.GetPhase()),
			attemptExitCell(attempt),
			formatTime(attempt.GetStartedAt()),
			attemptDuration(attempt),
			orker,
			attemptErrorLine(attempt.GetError()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if unreported {
		// The legend is printed once, under the table, rather than in the ERROR
		// cell of each unreported row: an UNREPORTED attempt is a MISSING
		// record, and text in the error column would read as the failure the
		// attempt is not known to have had.
		if _, err := fmt.Fprintln(out,
			"\nUNREPORTED: no record from the orker — it crashed or lost its heartbeat."); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}

// renderAttemptFailures prints the full error of every failed attempt, with the
// command that prints that attempt's output.
//
// The table's ERROR column is one line, and a failure's message and scan notes
// are routinely longer than that. This is the half a truncated cell cannot
// carry — and the reason the block only appears under --task: over a whole run
// it would bury the table it belongs to.
func renderAttemptFailures(
	out io.Writer,
	namespace, runID string,
	attempts []*orkstackv1.TaskAttempt,
) error {
	for _, attempt := range attempts {
		if attempt.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
			continue
		}
		attemptErr := attempt.GetError()
		heading := fmt.Sprintf("Attempt %d", attempt.GetAttempt())
		if attempt.Item != nil {
			heading = fmt.Sprintf("%s attempt %d", itemLabel(attempt.GetTaskId(), attempt.Item), attempt.GetAttempt())
		}
		fmt.Fprintf(out, "\n%s (%s)\n", heading, attemptOutcomeLabel(attempt.GetOutcome()))
		fmt.Fprintf(out, "%sCategory:  %s\n", attemptDetailIndent,
			failureCategoryLabel(attemptErr.GetCategory()))
		fmt.Fprintf(out, "%sCode:      %s\n", attemptDetailIndent, orDash(attemptErr.GetCode()))
		fmt.Fprintf(out, "%sRetryable: %t\n", attemptDetailIndent, attemptErr.GetRetryable())
		fmt.Fprintf(out, "%sMessage:   %s\n", attemptDetailIndent,
			indentJSONBlock(orDash(attemptErr.GetMessage()), attemptDetailContinuation))
		if details := attemptErr.GetDetails(); details != "" {
			fmt.Fprintf(out, "%sDetails:   %s\n", attemptDetailIndent,
				indentJSONBlock(details, attemptDetailContinuation))
		}
		logArgs := []string{"run", "logs", runID, "--task", attempt.GetTaskId()}
		if attempt.Item != nil {
			logArgs = append(logArgs, "--item", strconv.Itoa(int(attempt.GetItem())))
		}
		logArgs = append(logArgs, "--attempt", strconv.Itoa(int(attempt.GetAttempt())))
		fmt.Fprintf(out, "%sLogs:      %s\n", attemptDetailIndent, orkctlCommand(namespace, logArgs...))
	}
	return nil
}

const (
	// attemptDetailIndent sets a detail block's fields off from its heading.
	attemptDetailIndent = "  "
	// attemptDetailContinuation is where a wrapped field's second line starts:
	// the indent plus the width of the widest label ("Retryable: "), so a
	// multi-line message or details block stays under its own value.
	attemptDetailContinuation = attemptDetailIndent + "           "
)

// orkctlCommand renders a copy-pasteable orkctl invocation for this namespace.
//
// The namespace is only spelled when one was resolved: an empty namespace means
// "the deployment's own default", and an empty -n is not a command anybody can
// paste.
func orkctlCommand(namespace string, args ...string) string {
	parts := make([]string, 0, len(args)+3)
	parts = append(parts, "orkctl")
	if namespace != "" {
		parts = append(parts, "-n", namespace)
	}
	parts = append(parts, args...)
	return strings.Join(parts, " ")
}

// attemptOutcomeLabel renders an AttemptOutcome without its enum prefix.
func attemptOutcomeLabel(outcome orkstackv1.AttemptOutcome) string {
	return strings.TrimPrefix(outcome.String(), "ATTEMPT_OUTCOME_")
}

// failureCategoryLabel renders a FailureCategory without its enum prefix.
func failureCategoryLabel(category orkstackv1.FailureCategory) string {
	if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return dash
	}
	return strings.TrimPrefix(category.String(), "FAILURE_CATEGORY_")
}

// attemptErrorLine renders an attempt's error for one table cell, category
// first.
//
// The category is what decides whether the attempt could be retried at all, so
// it is the first thing to read: CONTRACT_VIOLATION/VALUES_MISSING is a bug in
// the task and INFRASTRUCTURE_ERROR/COMMIT_READ_FAILED is a bug in the
// deployment, and the code alone does not separate them.
func attemptErrorLine(attemptErr *orkstackv1.Error) string {
	line := errorLine(attemptErr)
	category := attemptErr.GetCategory()
	if line == dash || category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return line
	}
	return failureCategoryLabel(category) + "/" + line
}

// attemptDuration is how long the attempt took, or a dash when it cannot be
// known. Both ends are needed: an attempt whose Orker vanished has a start and
// no end, and reporting "now minus started" would invent a duration that grows
// every time the command is run.
func attemptDuration(attempt *orkstackv1.TaskAttempt) string {
	started, finished := attempt.GetStartedAt(), attempt.GetFinishedAt()
	if started == nil || finished == nil || !started.IsValid() || !finished.IsValid() {
		return dash
	}
	elapsed := finished.AsTime().Sub(started.AsTime())
	if elapsed < 0 {
		// Two clocks, one record: a negative span is a skewed pair, not a
		// duration, and printing "-1.2s" would look like data.
		return dash
	}
	return elapsed.Round(time.Millisecond).String()
}

// attemptExitCell renders the container's exit status.
//
// Zero is only an exit CODE when the attempt succeeded. Every other outcome
// zeroes the field for an attempt whose container never ran or never reported
// — a pull that failed, an Orker that died mid-run — and printing that 0 would
// say the process exited cleanly, which is the opposite of what happened.
func attemptExitCell(attempt *orkstackv1.TaskAttempt) string {
	if code := attempt.GetExitCode(); code != 0 {
		return strconv.Itoa(int(code))
	}
	if attempt.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
		return "0"
	}
	return dash
}
