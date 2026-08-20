package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// newRunLogsCommand builds `orkctl run logs`.
func newRunLogsCommand(g *globalFlags) *cobra.Command {
	var (
		taskID      string
		follow      bool
		attempt     int32
		fromSeq     int64
		fromAttempt int32
	)
	cmd := &cobra.Command{
		Use:   "logs <run-id>",
		Short: "Print a run's task output, and follow it live with -f",
		Long: "logs prints what a run's tasks wrote, in order.\n\n" +
			"With -f it follows the output as it is produced and returns when the run\n" +
			"reaches a terminal state. Without -f it prints what has been stored and\n" +
			"returns, even while the run is still going: a replay never turns into a\n" +
			"follow. Naming one task with --task prints that task's output verbatim;\n" +
			"without it every task of the run is printed, each line prefixed with the\n" +
			"task that wrote it.\n\n" +
			"Output is read from the artifact store, never from the engine's memory, so\n" +
			"any replica answers — including one that never started the run, one whose\n" +
			"Orker has exited, and a run that finished before this command was typed.\n\n" +
			"--from-seq resumes after a chunk sequence already seen, which is what a\n" +
			"reconnecting client hands back. A sequence is dense PER ATTEMPT and starts\n" +
			"again at 1 on a retry, so a cursor has to say which attempt it counted:\n" +
			"pin the try with --attempt, or name it with --from-attempt. A cursor with\n" +
			"no attempt is refused rather than applied to whichever try happens to be\n" +
			"latest, which would silently skip that try's first chunks.\n\n" +
			"The exit code reports whether the log could be read, not how the run\n" +
			"ended; `orkctl run watch` is the command whose exit code is the outcome.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogs(cmd, g, logsRequest{
				runID:       args[0],
				taskID:      taskID,
				follow:      follow,
				attempt:     attempt,
				fromSeq:     fromSeq,
				fromAttempt: fromAttempt,
			})
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "print only this task's output")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false,
		"follow the output live, returning when the run reaches a terminal state")
	cmd.Flags().Int32Var(&attempt, "attempt", 0,
		"print this attempt of the task (0 = the latest attempt that produced output)")
	cmd.Flags().Int64Var(&fromSeq, "from-seq", 0,
		"resume after this chunk sequence (0 = from the start of the task's output)")
	cmd.Flags().Int32Var(&fromAttempt, "from-attempt", 0,
		"the attempt --from-seq counts chunks of (0 = the attempt --attempt pins)")
	return cmd
}

// logsRequest is what the command was asked for.
type logsRequest struct {
	runID   string
	taskID  string
	follow  bool
	attempt int32
	fromSeq int64
	// fromAttempt is the other half of the cursor: which attempt fromSeq
	// counted chunks of. The engine drops a cursor from a different attempt
	// and delivers the resolved one from its first chunk; it can only do that
	// if it is told (proto StreamTaskLogsRequest.from_attempt).
	fromAttempt int32
}

// cursorAttempt is the attempt this command's --from-seq belongs to, or an
// error when nothing says.
//
// A sequence is dense PER ATTEMPT and restarts at 1 on a retry, so a cursor
// without an attempt is not a resume — it is a number applied to whichever try
// happens to be latest when the request lands. Against a task that retried
// since the caller last read it, that skips the new try's first chunks, and
// since a retry is usually shorter than the try that failed it usually
// delivers nothing at all while reporting a live stream. The CLI cannot
// recover the missing half — the attempt the run reports NOW is not the one
// the caller read — so it asks for it instead of guessing.
func (r logsRequest) cursorAttempt() (int32, error) {
	switch {
	case r.fromSeq == 0:
		// No cursor, nothing to misapply: 0 means "the attempt this request
		// resolves to", which is what a first read means.
		return r.fromAttempt, nil
	case r.fromAttempt > 0:
		return r.fromAttempt, nil
	case r.attempt > 0:
		// A pinned attempt is the attempt, so the cursor's half is known.
		return r.attempt, nil
	default:
		return 0, fmt.Errorf(
			"--from-seq %d is a cursor into one attempt's chunks, and a chunk sequence "+
				"starts again at 1 on every retry: say which attempt it counted with "+
				"--from-attempt N, or pin one try with --attempt N", r.fromSeq)
	}
}

// runLogs prints one task's output, or every task's.
func runLogs(cmd *cobra.Command, g *globalFlags, req logsRequest) error {
	if _, err := req.cursorAttempt(); err != nil {
		return err
	}
	client := g.runClient()

	// The run is resolved once, before anything streams, so a bad id or a run
	// of another namespace fails immediately with one clear error rather than
	// a stream that opens and dies.
	lookupCtx, cancelLookup := g.unaryContext(cmd)
	resp, err := client.GetRun(lookupCtx, connect.NewRequest(
		&orkstackv1.GetRunRequest{RunId: req.runID, Namespace: g.namespace}))
	cancelLookup()
	if err != nil {
		return rpcError("read run", err)
	}
	run := resp.Msg.GetRun()

	writer := &logWriter{out: cmd.OutOrStdout(), json: g.json}
	ctx, cancel := g.streamContext(cmd)
	defer cancel()

	if req.taskID != "" {
		if !runHasTask(run, req.taskID) {
			return fmt.Errorf("run %s has no task %q (tasks: %s)",
				req.runID, req.taskID, joinTaskIDs(run))
		}
		return streamOneTaskLog(ctx, g, req, req.taskID, writer, false)
	}
	if !req.follow {
		// A replay of a whole run is sequential, so the output is the same on
		// every invocation and readable as a transcript.
		for _, taskID := range taskIDsOf(run) {
			if err := streamOneTaskLog(ctx, g, req, taskID, writer, true); err != nil {
				return err
			}
		}
		return nil
	}
	return followAllTaskLogs(ctx, cmd, g, req, run, writer)
}

// streamOneTaskLog follows or replays one task, writing through writer.
func streamOneTaskLog(
	ctx context.Context,
	g *globalFlags,
	req logsRequest,
	taskID string,
	writer *logWriter,
	prefixed bool,
) error {
	fromAttempt, err := req.cursorAttempt()
	if err != nil {
		return err
	}
	stream, err := g.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{
			RunId:       req.runID,
			TaskId:      taskID,
			Attempt:     req.attempt,
			FromSeq:     req.fromSeq,
			FromAttempt: fromAttempt,
			Follow:      req.follow,
			Namespace:   g.namespace,
		}))
	if err != nil {
		return rpcError("read task logs", err)
	}
	defer func() { _ = stream.Close() }()

	for stream.Receive() {
		if err := writer.chunk(stream.Msg(), prefixed); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return rpcError("read task logs", err)
	}
	return nil
}

// followAllTaskLogs follows every task of a live run, including the ones that
// have not been scheduled yet.
//
// A run's tasks appear over time, so the set is discovered rather than fixed:
// the loop re-reads the run while it is live and starts a follower for each
// task it has not seen. It returns when the run is terminal and every follower
// has ended, which is what makes `-f` exit on its own.
func followAllTaskLogs(
	ctx context.Context,
	cmd *cobra.Command,
	g *globalFlags,
	req logsRequest,
	run *orkstackv1.Run,
	writer *logWriter,
) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		seen     = map[string]struct{}{}
	)
	start := func(taskID string) {
		if _, already := seen[taskID]; already {
			return
		}
		seen[taskID] = struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := streamOneTaskLog(ctx, g, req, taskID, writer, true); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	for _, taskID := range taskIDsOf(run) {
		start(taskID)
	}

	for !isRunTerminal(run) {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-time.After(logDiscoveryInterval):
		}
		lookupCtx, cancelLookup := g.unaryContext(cmd)
		resp, err := g.runClient().GetRun(lookupCtx, connect.NewRequest(
			&orkstackv1.GetRunRequest{RunId: req.runID, Namespace: g.namespace}))
		cancelLookup()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = rpcError("read run", err)
			}
			mu.Unlock()
			break
		}
		run = resp.Msg.GetRun()
		for _, taskID := range taskIDsOf(run) {
			start(taskID)
		}
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

// logDiscoveryInterval is how often a whole-run follower looks for a task that
// has since been scheduled.
const logDiscoveryInterval = 500 * time.Millisecond

// logWriter renders chunks. Writes are serialized because a whole-run follow
// has one goroutine per task, and two half-lines interleaved are worse than
// either line late.
type logWriter struct {
	out  io.Writer
	json bool
	mu   sync.Mutex
}

func (w *logWriter) chunk(chunk *orkstackv1.TaskLogChunk, prefixed bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.json {
		// One COMPACT object per line: a follower can be piped into a
		// line-oriented processor without waiting for the stream to end,
		// which both a single enclosing document and an indented object
		// would require.
		encoded, err := api.MarshalJSON(chunk)
		if err != nil {
			return err
		}
		return w.write(string(encoded) + "\n")
	}
	if chunk.GetLast() {
		return nil
	}
	body := string(chunk.GetData())
	if !prefixed {
		return w.write(body)
	}
	// Prefixing is per line, not per chunk: a chunk holds many lines and the
	// task that wrote them belongs on each.
	for _, line := range splitLines(body) {
		if err := w.write("[" + chunk.GetTaskId() + "] " + line); err != nil {
			return err
		}
	}
	return nil
}

func (w *logWriter) write(text string) error {
	if _, err := io.WriteString(w.out, text); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// splitLines splits a chunk into lines, each keeping its newline. A trailing
// fragment (the last line of a split over-long line) is returned as it is.
func splitLines(body string) []string {
	var lines []string
	for len(body) > 0 {
		end := len(body)
		for i := 0; i < len(body); i++ {
			if body[i] == '\n' {
				end = i + 1
				break
			}
		}
		lines = append(lines, body[:end])
		body = body[end:]
	}
	return lines
}

// taskIDsOf names the run's tasks in a stable order, so a whole-run replay
// reads the same way twice.
func taskIDsOf(run *orkstackv1.Run) []string {
	ids := make([]string, 0, len(run.GetTaskRuns()))
	for _, taskRun := range run.GetTaskRuns() {
		ids = append(ids, taskRun.GetTaskId())
	}
	sort.Strings(ids)
	return ids
}

// joinTaskIDs renders the run's tasks for an error naming the one that is not
// there.
func joinTaskIDs(run *orkstackv1.Run) string {
	ids := taskIDsOf(run)
	if len(ids) == 0 {
		return "none yet"
	}
	out := ids[0]
	for _, id := range ids[1:] {
		out += ", " + id
	}
	return out
}

// runHasTask reports whether the run snapshot carries this task.
func runHasTask(run *orkstackv1.Run, taskID string) bool {
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetTaskId() == taskID {
			return true
		}
	}
	return false
}

// isRunTerminal reports that no task of this run will produce more output.
func isRunTerminal(run *orkstackv1.Run) bool {
	switch run.GetStatus() {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	default:
		return false
	}
}
