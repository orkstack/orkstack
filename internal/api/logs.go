package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Serving a task's live log: the read half of the log-chunk contract (the
// Orker writes, this file reads).
//
// The Orker writes numbered chunks into the artifact store while the task runs
// (internal/orker/logsink.go). This file reads them back — replay what exists,
// then follow what lands — and ends the stream when the task reaches a terminal
// state.
//
// It reads TWO shared things and no process-local state: the store, for the
// chunks, and the run READER, for the task's state. Both are shared by every
// replica, so any replica serves any run's logs: one it never started, one
// whose Orker has exited, one that finished before the reader connected
// (AGENTS.md §7).
//
// Nothing here touches Temporal payloads or the Event stream. Bytes live in the
// bucket; the run's history says only that a task ran.

// LogPollOptions tunes how often a follower re-lists the log prefix. It backs
// off while a task is quiet, so a long silent task costs a listing every
// LogPollMax rather than one every LogPollMin.
type LogPollOptions struct {
	// Min is the wait after a poll that found chunks: the task is talking, so
	// ask again promptly.
	Min time.Duration
	// Max is the ceiling the wait backs off to while a task is silent.
	Max time.Duration
}

// Default poll bounds. Min is what decides how late a line can be: a chunk
// that lands the instant after a poll is delivered at most Min later, on top
// of the Orker's own flush interval.
const (
	DefaultLogPollMin = 200 * time.Millisecond
	DefaultLogPollMax = 2 * time.Second
)

func (o LogPollOptions) withDefaults() LogPollOptions {
	if o.Min <= 0 {
		o.Min = DefaultLogPollMin
	}
	if o.Max < o.Min {
		o.Max = max(o.Min, DefaultLogPollMax)
	}
	return o
}

// What one follower costs, per poll:
//
//	a LIST of the attempt's chunk prefix        every tick
//	a read of the run (its whole history)       only on a tick that swept
//	                                            nothing, and then at most one
//	                                            per backoff step
//	a LIST of the task's attempt index          the same ticks
//
// The two cluster-side reads are on the QUIET path deliberately. A task that
// is writing is a task that is not finished and has not retried, so neither
// question needs asking while chunks are arriving — and while they are
// arriving the poll is pinned at Min, which is exactly when asking is most
// expensive. Reading the run means replaying its history
// (engine.TemporalRunReader.GetRun), so a follower that asked on every tick
// cost five whole-history reads a second per open pane: ten panes over two
// tabs on one run were a hundred history reads a second, for a run nobody had
// touched. Now a chatty task costs none at all, and a silent one costs one per
// backoff step, ending at one per Max.
//
// What it costs in latency is one poll interval: a task that finishes while
// talking is noticed on the first tick that finds nothing new, which is the
// tick after its last chunk. Nothing is lost — the sweep that follows the
// terminal observation is what delivers the tail (see StreamRunTaskLogs).

// logChunkPage and logChunkPageBytes bound one read, and the ENGINE'S LOG
// MEMORY IS THE PAIR: a subscriber holds one page, and a page is at most
// logChunkPageBytes of chunk data plus the one chunk that crossed the budget.
//
// The count alone is not a bound and must not be read as one. A chunk is
// whatever the Orker flushed, which is ORK_ORKER_LOG_FLUSH_BYTES (64 KiB by
// default, and an operator's to raise) plus one over-long line of up to 1 MiB
// (internal/orker/logsink.go). At 32 chunks that is ~34 MiB per open
// subscriber on the defaults alone, and a run detail page opens one subscriber
// per expanded task. The byte budget is what makes the number the engine
// states a number the engine controls; the count still caps the LIST, which
// is what keeps a quiet poll cheap.
//
// A page that comes back short of either bound is not the end of the log: the
// drain reads again until a page is short of BOTH, which is what keeps a
// replay of a 16 MiB attempt complete while costing one page at a time.
const (
	logChunkPage      = 32
	logChunkPageBytes = 4 << 20
)

// StreamTaskLogs server-streams one task attempt's output.
func (s *Service) StreamTaskLogs(
	ctx context.Context,
	req *connect.Request[orkstackv1.StreamTaskLogsRequest],
	stream *connect.ServerStream[orkstackv1.TaskLogChunk],
) error {
	return s.StreamRunTaskLogs(ctx, TaskLogQuery{
		RunID:       req.Msg.GetRunId(),
		TaskID:      req.Msg.GetTaskId(),
		Attempt:     req.Msg.GetAttempt(),
		FromSeq:     req.Msg.GetFromSeq(),
		FromAttempt: req.Msg.GetFromAttempt(),
		Follow:      req.Msg.GetFollow(),
		Namespace:   req.Msg.GetNamespace(),
	}, stream.Send)
}

// TaskLogQuery is what both entry points — the Connect handler and the
// WebSocket bridge — hand to the one streaming implementation.
type TaskLogQuery struct {
	RunID  string
	TaskID string
	// Attempt pins one try, or 0 for "whichever is latest", re-read for as
	// long as a follow is open.
	Attempt int32
	FromSeq int64
	// FromAttempt is the attempt FromSeq counts chunks of. A cursor from
	// another attempt is dropped rather than applied: seq is dense per
	// attempt, so applying it would skip the new attempt's first chunks.
	FromAttempt int32
	Follow      bool
	Namespace   string
}

// StreamRunTaskLogs delivers a task attempt's chunks after FromSeq to send,
// and returns when the task is terminal (or, without Follow, as soon as the
// stored chunks have been delivered).
//
// It is the one streaming path, for the same reason StreamRunEvents is: the
// RPC and the socket must not be able to drift into two different contracts.
//
// The two modes are a FOLLOW and a REPLAY, and the difference is the flag,
// never the task's behaviour:
//
//   - Follow waits for chunks that do not exist yet and ends on the task's
//     terminal state.
//   - Without it the stream is a replay: one sweep of what the store holds,
//     then the terminator. It must not end merely when the task goes quiet,
//     or `orkctl run logs` with no -f would block for as long as a chatty
//     task keeps talking — a replay would silently have become a follow.
//
// The termination rule is what makes the follow gap-free. The Orker closes its
// sink BEFORE ExecuteTask returns, so every chunk is durable before the task's
// terminal state exists. This loop therefore reads the state first, sweeps the
// prefix after, and only then ends: anything written in the last instant of the
// task is picked up by that final sweep.
//
// A FOLLOW OF ATTEMPT 0 FOLLOWS THE RETRY. The Orker keys chunks by the
// Temporal activity attempt (internal/orker/activity.go), and a retry is
// invisible in the task's state — the task stays RUNNING while try 2 starts
// under a new prefix. A stream that resolved "the latest attempt" once would
// therefore sit on the dead attempt, deliver nothing and stay open until the
// task ended, reporting itself live the whole time. That is the failure on
// exactly the tasks whose output a reader most wants. So while the current
// attempt is silent the latest is re-read, and a later one is switched to and
// delivered from its first chunk.
func (s *Service) StreamRunTaskLogs(
	ctx context.Context,
	q TaskLogQuery,
	send func(*orkstackv1.TaskLogChunk) error,
) error {
	if q.RunID == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	if q.TaskID == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("task_id is required"))
	}
	// The namespace check is the same one every read carrying a run id makes:
	// a run of another namespace is NotFound before a single byte is sent.
	run, err := s.LookupRunIn(ctx, q.RunID, q.Namespace)
	if err != nil {
		return err
	}
	if s.logs == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"run %q: this engine has no task log store configured; "+
				"set ORK_ARTIFACT_BACKEND (and the ORK_S3_* settings for s3)", q.RunID))
	}
	taskRun, ok := taskRunOf(run, q.TaskID)
	if !ok {
		// A typo must not look like a quiet task.
		return connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"run %q has no task %q (tasks: %s)", q.RunID, q.TaskID, taskIDsOf(run)))
	}
	if taskRun.GetAwait() != nil {
		// A follower would otherwise wait on chunks nobody will ever write.
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"task %q of run %q is an awaited task: it runs no container and writes no logs", q.TaskID, q.RunID))
	}

	attempt, err := s.resolveLogAttempt(ctx, q)
	if err != nil {
		return err
	}
	ref := artifacts.LogRef{RunID: q.RunID, TaskID: q.TaskID, Attempt: attempt}

	poll := s.logPoll.withDefaults()
	wait := poll.Min
	cursor := q.FromSeq
	if q.FromAttempt > 0 && q.FromAttempt != ref.Attempt {
		// The cursor belongs to another attempt. seq is dense PER ATTEMPT, so
		// honouring it here would skip this attempt's first chunks — and since
		// a retry is usually shorter than the try that failed, would usually
		// deliver nothing at all while the client showed a live stream.
		cursor = 0
	}
	// consult says whether this pass asks the two shared records anything
	// before it sweeps: is the task finished, and has it started a later
	// attempt? It is true on the first pass and then only after a sweep that
	// found nothing, which is what keeps a chatty task from costing a
	// whole-history read five times a second (see the cost note above).
	consult, first := true, true
	for {
		if consult && !first && q.Follow && q.Attempt == 0 {
			// The attempt is re-read BEFORE the state and the sweep, so a task
			// that retried and then finished is switched to its last attempt
			// and swept there — not ended on the attempt it abandoned.
			latest, err := s.resolveLogAttempt(ctx, q)
			if err != nil {
				return err
			}
			if latest > ref.Attempt {
				ref.Attempt, cursor, wait = latest, 0, poll.Min
			}
		}
		terminal := false
		if consult {
			// The state is read BEFORE the sweep, so a task that finishes
			// between the two is still swept once more with the terminal
			// answer in hand.
			if terminal, err = s.taskIsTerminal(ctx, q); err != nil {
				return err
			}
		}
		delivered, err := s.drainLogChunks(ctx, ref, &cursor, send)
		if err != nil {
			return err
		}
		if terminal || !q.Follow {
			// terminal: everything the task wrote is durable (the sink closes
			// before the activity returns), and the sweep above read all of
			// it.
			//
			// !Follow: a replay was asked for, and that sweep delivered every
			// chunk the store held when it ran. Ending here bounds the replay
			// to what existed, whether or not the task is still writing.
			return sendTerminator(send, ref, cursor)
		}
		first = false
		// A sweep that delivered is a task that is running and has not
		// retried: both questions are answered by the chunks themselves, so
		// the next pass asks nobody and comes back promptly.
		consult = !delivered
		if delivered {
			wait = poll.Min
		} else if wait = wait * 2; wait > poll.Max {
			wait = poll.Max
		}
		select {
		case <-ctx.Done():
			// The caller went away. Not a failure of the run, and the event
			// stream reports the same thing the same way.
			return nil
		case <-time.After(wait):
		}
	}
}

// drainLogChunks sends every chunk the store holds after *cursor, advancing it
// as it goes, one bounded page at a time. It returns when a page comes back
// short of BOTH bounds — the store had no more at that instant — so one call
// is the whole of what a replay owes its caller, at one page of memory rather
// than a whole attempt's. A writer cannot hold it open forever: an attempt's
// chunks are capped (internal/orker/logsink.go), and every page moves the
// cursor forward.
func (s *Service) drainLogChunks(
	ctx context.Context,
	ref artifacts.LogRef,
	cursor *int64,
	send func(*orkstackv1.TaskLogChunk) error,
) (bool, error) {
	delivered := false
	for {
		chunks, err := s.logs.ReadLogChunks(ctx, ref, *cursor, logChunkPage, logChunkPageBytes)
		if err != nil {
			if ctx.Err() != nil {
				return delivered, nil
			}
			return delivered, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read logs of task %q in run %q: %w", ref.TaskID, ref.RunID, err))
		}
		if len(chunks) == 0 {
			return delivered, nil
		}
		read := 0
		for _, chunk := range chunks {
			if err := send(&orkstackv1.TaskLogChunk{
				RunId:     chunk.RunID,
				TaskId:    chunk.TaskID,
				Attempt:   chunk.Attempt,
				Seq:       chunk.Seq,
				Data:      chunk.Data,
				Truncated: chunk.Truncated,
			}); err != nil {
				return delivered, err
			}
			*cursor = chunk.Seq
			read += len(chunk.Data)
			delivered = true
		}
		// Short of both bounds is the only proof the store is exhausted. A
		// page cut by the BYTE budget is short in count while the log goes on,
		// and stopping there would end a replay in the middle of a chatty
		// task's output — the same silent truncation the budget exists to
		// avoid paying for in memory.
		if len(chunks) < logChunkPage && int64(read) < logChunkPageBytes {
			return delivered, nil
		}
	}
}

// sendTerminator ends the stream with the empty `last` chunk, so a client
// knows the difference between "the task is done" and "the connection died".
func sendTerminator(
	send func(*orkstackv1.TaskLogChunk) error,
	ref artifacts.LogRef,
	cursor int64,
) error {
	return send(&orkstackv1.TaskLogChunk{
		RunId:   ref.RunID,
		TaskId:  ref.TaskID,
		Attempt: ref.Attempt,
		Seq:     cursor,
		Last:    true,
	})
}

// resolveLogAttempt turns the request's attempt into a stored one. 0 means the
// latest attempt that wrote anything, answered by the store — the only thing
// that knows which tries produced output.
//
// A task that wrote nothing at all resolves to attempt 1 rather than failing:
// a silent task is ordinary, and a follower attaching before the first chunk
// exists is the normal case, not an error.
func (s *Service) resolveLogAttempt(ctx context.Context, q TaskLogQuery) (int32, error) {
	if q.Attempt > 0 {
		return q.Attempt, nil
	}
	if q.Attempt < 0 {
		return 0, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("attempt %d is negative; 0 means the latest attempt", q.Attempt))
	}
	switch attempt, err := s.logs.LatestLogAttempt(ctx, q.RunID, q.TaskID, artifacts.NoItem); {
	case err == nil:
		return attempt, nil
	case errors.Is(err, artifacts.ErrNotFound):
		return 1, nil
	default:
		return 0, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("resolve latest attempt of task %q in run %q: %w", q.TaskID, q.RunID, err))
	}
}

// taskIsTerminal reports whether the task will write anything more.
//
// It is read from the run READER (the cluster), never from process memory: a
// replica that never started the run has nothing in memory to consult, and
// that replica must be able to end a stream correctly.
//
// A run that reached a terminal state ends every one of its tasks' streams,
// including a task that never started — otherwise a follower of a skipped task
// would wait forever for a chunk nobody will write.
func (s *Service) taskIsTerminal(ctx context.Context, q TaskLogQuery) (bool, error) {
	run, err := s.LookupRunIn(ctx, q.RunID, q.Namespace)
	if err != nil {
		if ctx.Err() != nil {
			return true, nil
		}
		return false, err
	}
	if isTerminalStatus(run.GetStatus()) {
		return true, nil
	}
	taskRun, ok := taskRunOf(run, q.TaskID)
	if !ok {
		return false, nil
	}
	return isTerminalStatus(taskRun.GetStatus()), nil
}

func isTerminalStatus(status orkstackv1.Status) bool {
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

// taskRunOf finds one task's execution record inside a run snapshot.
func taskRunOf(run *orkstackv1.Run, taskID string) (*orkstackv1.TaskRun, bool) {
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetTaskId() == taskID {
			return taskRun, true
		}
	}
	return nil, false
}

// taskIDsOf names the tasks a run has, so a mistyped task id is answered with
// the ones that exist.
func taskIDsOf(run *orkstackv1.Run) string {
	ids := make([]string, 0, len(run.GetTaskRuns()))
	for _, taskRun := range run.GetTaskRuns() {
		ids = append(ids, taskRun.GetTaskId())
	}
	if len(ids) == 0 {
		return "none yet"
	}
	return strings.Join(ids, ", ")
}
