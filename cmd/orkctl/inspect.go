package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/task"
)

const (
	// defaultInspectInterval is the Orker's default heartbeat tick: a faster
	// poll cannot see a fresher detail than the cluster holds.
	defaultInspectInterval = 5 * time.Second
	// minInspectInterval is the Orker's own floor on a heartbeat tick
	// (orker.MinHeartbeatInterval): below it no beat can be fresher.
	minInspectInterval = time.Second
	// containerShortLen is the runtime's own short form of a container id.
	containerShortLen = 12
)

// newRunInspectCommand builds `orkctl run inspect`.
//
// It answers "what is this task doing right now": the phase the attempt is
// in, the bytes the workload has written, the Orker that is running it and
// how long ago it last heartbeated — read from the heartbeat detail the
// cluster already holds for the pending activity. Nothing is stored; the
// answer is only ever as fresh as the last beat.
func newRunInspectCommand(g *globalFlags) *cobra.Command {
	var (
		taskID   string
		watch    bool
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "inspect <run-id>",
		Short: "Show what each in-flight task of a run is doing right now",
		Long: "inspect prints, for every task of a run, what its in-flight attempt is\n" +
			"doing now: its state, the phase the Orker reports, the container, the\n" +
			"bytes the workload has written to stdout and stderr, how long the\n" +
			"attempt has been running and how long ago its Orker last heartbeated.\n\n" +
			"The detail is the Orker's heartbeat, read from the cluster. A heartbeat\n" +
			"that is not there is reported as unavailable with its reason, never as\n" +
			"zero. A heartbeat older than two ceiling ticks is STALE: the Orker is gone\n" +
			"or partitioned, and the footer names the instant Temporal will time the\n" +
			"attempt out.\n\n" +
			"With --watch the table is printed again every --interval until the run is\n" +
			"terminal, and the exit code is the run's (0 succeeded, 2 failed,\n" +
			"3 canceled). --json prints the raw response; with --watch, one compact\n" +
			"response per line.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("interval") && !watch {
				return fmt.Errorf("--interval needs --watch")
			}
			if interval < minInspectInterval {
				return fmt.Errorf("--interval must be at least %s", minInspectInterval)
			}
			inspector := &runInspector{g: g, cmd: cmd, runID: args[0], taskID: taskID}
			if !watch {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				if _, err := inspector.poll(ctx, false); err != nil {
					var failed pollError
					if errors.As(err, &failed) {
						return failed.rendered()
					}
					return err
				}
				return nil
			}
			return inspector.watch(interval)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "only this task")
	cmd.Flags().BoolVar(&watch, "watch", false, "poll until the run is terminal")
	cmd.Flags().DurationVar(&interval, "interval", defaultInspectInterval,
		"poll period under --watch (at least 1s)")
	return cmd
}

// runInspector is one invocation: which run, which task, where to print.
type runInspector struct {
	g      *globalFlags
	cmd    *cobra.Command
	runID  string
	taskID string
}

// poll makes one round of reads and prints them. It returns the run status
// the inspect answer reported.
func (i *runInspector) poll(ctx context.Context, watching bool) (orkstackv1.Status, error) {
	out := i.cmd.OutOrStdout()
	var run *orkstackv1.Run
	if !i.g.json {
		// The task table's rows come from the snapshot; --json prints the
		// inspect response alone, so it makes no GetRun call.
		resp, err := i.g.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId:     i.runID,
			Namespace: i.g.namespace,
		}))
		if err != nil {
			return orkstackv1.Status_STATUS_UNSPECIFIED, pollError{err: err, op: "get run"}
		}
		run = resp.Msg.GetRun()
	}
	resp, err := i.g.runClient().InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{
		RunId:     i.runID,
		Namespace: i.g.namespace,
		TaskId:    i.taskID,
	}))
	if err != nil {
		return orkstackv1.Status_STATUS_UNSPECIFIED, pollError{err: err, op: "inspect run"}
	}
	inspected := resp.Msg
	if i.g.json {
		if !watching {
			return inspected.GetRunStatus(), writeJSON(out, inspected)
		}
		// One COMPACT object per line, as `run logs -f --json` prints.
		encoded, err := api.MarshalJSON(inspected)
		if err != nil {
			return inspected.GetRunStatus(), err
		}
		if _, err := out.Write(append(encoded, '\n')); err != nil {
			return inspected.GetRunStatus(), fmt.Errorf("write output: %w", err)
		}
		return inspected.GetRunStatus(), nil
	}
	if watching {
		if _, err := fmt.Fprintf(out, "--- %s ---\n", formatTime(inspected.GetComputedAt())); err != nil {
			return inspected.GetRunStatus(), fmt.Errorf("write output: %w", err)
		}
	}
	if err := renderInspect(out, i.g.namespace, run, i.taskID, inspected); err != nil {
		return inspected.GetRunStatus(), err
	}
	if watching {
		if _, err := fmt.Fprintln(out); err != nil {
			return inspected.GetRunStatus(), fmt.Errorf("write output: %w", err)
		}
	}
	return inspected.GetRunStatus(), nil
}

// pollError carries which of the two reads failed, so the watch loop can
// tell a transient Unavailable from an answer that ends it.
type pollError struct {
	err error
	op  string
}

func (p pollError) Error() string { return p.err.Error() }
func (p pollError) Unwrap() error { return p.err }

// rendered is the CLI error an operator reads.
func (p pollError) rendered() error {
	if p.op == "inspect run" {
		return inspectError(p.err)
	}
	return rpcError(p.op, p.err)
}

// watch polls until a poll reports a terminal run.
func (i *runInspector) watch(interval time.Duration) error {
	ctx, cancel := i.g.streamContext(i.cmd)
	defer cancel()
	for {
		status, err := i.poll(ctx, true)
		if err != nil {
			var failed pollError
			if !errors.As(err, &failed) {
				return err
			}
			if connect.CodeOf(failed.err) != connect.CodeUnavailable {
				return failed.rendered()
			}
			var connectErr *connect.Error
			message := failed.err.Error()
			if errors.As(failed.err, &connectErr) {
				message = connectErr.Message()
			}
			fmt.Fprintf(i.cmd.ErrOrStderr(), "inspect unavailable: %s\n", message)
		} else if isTerminal(status) {
			return runExitError(&orkstackv1.Run{Id: i.runID, Status: status})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// isTerminal is the run statuses a watch ends on.
func isTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED,
		orkstackv1.Status_STATUS_SKIPPED:
		return true
	default:
		return false
	}
}

// inspectError renders a failed InspectRun as a CLI error. FailedPrecondition
// is about the engine, not the request: no argument could change it, so it is
// worded for an operator rather than as a code.
func inspectError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition {
		return fmt.Errorf("live inspection is not available: this engine does not read runs from the cluster")
	}
	return rpcError("inspect run", err)
}

// renderInspect prints the task table and its footers.
//
// Rows are the snapshot's tasks in run order (or the one task), with the live
// columns filled from the matching LiveAttempt. A live attempt whose task the
// snapshot does not name yet is appended, so nothing live is hidden.
func renderInspect(
	out io.Writer,
	namespace string,
	run *orkstackv1.Run,
	taskID string,
	inspected *orkstackv1.InspectRunResponse,
) error {
	now := inspected.GetComputedAt().AsTime()
	liveByTask := make(map[string]*orkstackv1.LiveAttempt, len(inspected.GetAttempts()))
	for _, attempt := range inspected.GetAttempts() {
		liveByTask[attempt.GetTaskId()] = attempt
	}
	type row struct {
		task   *orkstackv1.TaskRun
		status string
		live   *orkstackv1.LiveAttempt
	}
	var rows []row
	named := map[string]bool{}
	for _, taskRun := range run.GetTaskRuns() {
		if taskID != "" && taskRun.GetTaskId() != taskID {
			continue
		}
		named[taskRun.GetTaskId()] = true
		rows = append(rows, row{task: taskRun, status: taskStatusLabel(taskRun), live: liveByTask[taskRun.GetTaskId()]})
	}
	for _, attempt := range inspected.GetAttempts() {
		if !named[attempt.GetTaskId()] {
			rows = append(rows, row{task: &orkstackv1.TaskRun{TaskId: attempt.GetTaskId()}, status: dash, live: attempt})
		}
	}

	table := newTable(out)
	fmt.Fprintln(table, "TASK\tSTATUS\tATTEMPT\tSTATE\tPHASE\tORKER\tCONTAINER\tLOGGED\tRUNNING\tHEARTBEAT")
	for _, r := range rows {
		cells := []string{r.task.GetTaskId(), r.status, dash, dash, dash, dash, dash, dash, dash, dash}
		if live := r.live; live != nil {
			cells[2] = fmt.Sprint(live.GetAttempt())
			cells[3] = liveStateLabel(live.GetState())
			if live.GetHeartbeatAvailable() {
				cells[4] = orDash(live.GetPhase())
				cells[5] = orDash(live.GetOrkerId())
				cells[6] = orDash(shortContainer(live.GetContainerId()))
				cells[7] = formatBytes(live.GetBytesLogged())
				cells[9] = heartbeatCell(live, now)
			} else {
				cells[4] = "unavailable"
			}
			if started := live.GetAttemptStartedAt(); started != nil {
				cells[8] = roundedSince(started, now).String()
			}
		}
		fmt.Fprintln(table, strings.Join(cells, "\t"))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	var footers []string
	for _, r := range rows {
		if r.live != nil {
			footers = append(footers, liveFooters(namespace, run.GetId(), r.task, r.live, now)...)
		}
	}
	switch {
	case isTerminal(inspected.GetRunStatus()):
		footers = append(footers, fmt.Sprintf("run %s; nothing is live", statusLabel(inspected.GetRunStatus())))
	case len(inspected.GetAttempts()) == 0:
		footers = append(footers, "nothing is in flight: the run is between tasks")
	}
	if len(footers) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	for _, line := range footers {
		if _, err := fmt.Fprintln(out, line); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}

// liveFooters are the sentences one live attempt earns under the table.
func liveFooters(
	namespace, runID string,
	taskRun *orkstackv1.TaskRun,
	live *orkstackv1.LiveAttempt,
	now time.Time,
) []string {
	id := live.GetTaskId()
	var lines []string
	if !live.GetHeartbeatAvailable() {
		lines = append(lines, fmt.Sprintf("%s: %s", id, unavailableReasonSentence(live.GetHeartbeatUnavailableReason())))
	}
	switch live.GetState() {
	case orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED:
		if !live.GetHeartbeatAvailable() {
			break
		}
		if live.GetHeartbeatStale() {
			lines = append(lines, fmt.Sprintf(
				"%s: no heartbeat for %s (a live orker beats at least every %s) — the orker is gone or partitioned; "+
					"Temporal times this attempt out at %s unless a beat arrives, and it will show as UNREPORTED in run attempts",
				id, roundedSince(live.GetLastHeartbeatAt(), now), task.MaxHeartbeatInterval,
				formatTime(live.GetHeartbeatDeadlineAt())))
		}
		lines = append(lines, timeoutFooter(live, now))
	case orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED:
		if next := live.GetNextAttemptAt(); next != nil {
			lines = append(lines, fmt.Sprintf(
				"%s: attempt %d ended; attempt %d is scheduled for %s (%s) · the earlier attempt's error: %s",
				id, live.GetAttempt()-1, live.GetAttempt(), formatTime(next), untilLabel(next, now),
				orkctlCommand(namespace, "run", "attempts", runID, "--task", id)))
			break
		}
		lines = append(lines, fmt.Sprintf("%s: waiting for an orker on pool %s for %s (scheduled %s)",
			id, orDash(taskRun.GetPool()), roundedSince(live.GetAttemptScheduledAt(), now),
			formatTime(live.GetAttemptScheduledAt())))
	case orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED:
		phase := "phase unavailable"
		if live.GetHeartbeatAvailable() {
			phase = "phase " + orDash(live.GetPhase())
		}
		lines = append(lines, fmt.Sprintf("%s: cancellation requested · %s · grace %s",
			id, phase, seconds(live.GetCancellationGracePeriodSeconds())))
	}
	return lines
}

// timeoutFooter is the STARTED attempt's bounds. The remaining time is a
// LOWER bound: the Orker arms the task's clock when the container starts,
// after staging and the image pull, so the parenthetical says so verbatim.
func timeoutFooter(live *orkstackv1.LiveAttempt, now time.Time) string {
	timeout := seconds(live.GetTimeoutSeconds())
	left := timeout - roundedSince(live.GetAttemptStartedAt(), now)
	if left < 0 {
		left = 0
	}
	return fmt.Sprintf(
		"%s: timeout %s, at least %s left (the container clock starts after setup) · cancellation grace %s · backstop %s",
		live.GetTaskId(), timeout, left, seconds(live.GetCancellationGracePeriodSeconds()),
		formatTime(live.GetExpiresAt()))
}

// unavailableReasonSentence is the fixed sentence for each reason.
func unavailableReasonSentence(reason orkstackv1.HeartbeatUnavailableReason) string {
	switch reason {
	case orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NOT_STARTED:
		return "not started"
	case orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NO_HEARTBEAT_YET:
		return "no heartbeat yet"
	case orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNDECODABLE:
		return "heartbeat detail undecodable"
	default:
		return "heartbeat unavailable"
	}
}

// liveStateLabel renders a LiveAttemptState without its prefix.
func liveStateLabel(state orkstackv1.LiveAttemptState) string {
	return strings.TrimPrefix(state.String(), "LIVE_ATTEMPT_STATE_")
}

// heartbeatCell is the age of the last beat against the engine's clock.
func heartbeatCell(live *orkstackv1.LiveAttempt, now time.Time) string {
	age := roundedSince(live.GetLastHeartbeatAt(), now)
	label := fmt.Sprintf("%s ago", age)
	if age <= 0 {
		label = "just now"
	}
	if live.GetHeartbeatStale() {
		label += " STALE"
	}
	return label
}

// untilLabel is how far ahead an instant is, against the engine's clock.
func untilLabel(at *timestamppb.Timestamp, now time.Time) string {
	d := at.AsTime().Sub(now).Round(time.Second)
	if d <= 0 {
		return "due now"
	}
	return "in " + d.String()
}

// roundedSince is now − at to the second.
func roundedSince(at *timestamppb.Timestamp, now time.Time) time.Duration {
	if at == nil {
		return 0
	}
	return now.Sub(at.AsTime()).Round(time.Second)
}

func seconds(n int32) time.Duration { return time.Duration(n) * time.Second }

// shortContainer is a container id's first twelve characters.
func shortContainer(id string) string {
	if len(id) > containerShortLen {
		return id[:containerShortLen]
	}
	return id
}

// formatBytes renders a byte count: exact below 1 KiB, then KiB, MiB and GiB
// with one decimal. Zero prints — a live container that has written nothing
// is a fact.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n) / unit
	for _, suffix := range []string{"KiB", "MiB"} {
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
		value /= unit
	}
	return fmt.Sprintf("%.1f GiB", value)
}
