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

// Serving a task's live log: the read half of M10.5.
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
// (AGENTS.md §1.7).
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

// logChunkPage bounds how many chunks one read pulls into memory. The engine's
// log memory is therefore one page per subscriber, whatever the task wrote.
const logChunkPage = 32

// StreamTaskLogs server-streams one task attempt's output.
func (s *Service) StreamTaskLogs(
	ctx context.Context,
	req *connect.Request[orkstackv1.StreamTaskLogsRequest],
	stream *connect.ServerStream[orkstackv1.TaskLogChunk],
) error {
	return s.StreamRunTaskLogs(ctx, TaskLogQuery{
		RunID:     req.Msg.GetRunId(),
		TaskID:    req.Msg.GetTaskId(),
		Attempt:   req.Msg.GetAttempt(),
		FromSeq:   req.Msg.GetFromSeq(),
		Follow:    req.Msg.GetFollow(),
		Namespace: req.Msg.GetNamespace(),
	}, stream.Send)
}

// TaskLogQuery is what both entry points — the Connect handler and the
// WebSocket bridge — hand to the one streaming implementation.
type TaskLogQuery struct {
	RunID     string
	TaskID    string
	Attempt   int32
	FromSeq   int64
	Follow    bool
	Namespace string
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
	if _, ok := taskRunOf(run, q.TaskID); !ok {
		// A typo must not look like a quiet task.
		return connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"run %q has no task %q (tasks: %s)", q.RunID, q.TaskID, taskIDsOf(run)))
	}

	attempt, err := s.resolveLogAttempt(ctx, q)
	if err != nil {
		return err
	}
	ref := artifacts.LogRef{RunID: q.RunID, TaskID: q.TaskID, Attempt: attempt}

	poll := s.logPoll.withDefaults()
	wait := poll.Min
	cursor := q.FromSeq
	for {
		// The state is read BEFORE the sweep, so a task that finishes between
		// the two is still swept once more with the terminal answer in hand.
		terminal, err := s.taskIsTerminal(ctx, q)
		if err != nil {
			return err
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
// short — the store had no more at that instant — so one call is the whole of
// what a replay owes its caller. A writer cannot hold it open forever: an
// attempt's chunks are capped (internal/orker/logsink.go), and every page
// moves the cursor forward.
func (s *Service) drainLogChunks(
	ctx context.Context,
	ref artifacts.LogRef,
	cursor *int64,
	send func(*orkstackv1.TaskLogChunk) error,
) (bool, error) {
	delivered := false
	for {
		chunks, err := s.logs.ReadLogChunks(ctx, ref, *cursor, logChunkPage)
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
			delivered = true
		}
		if len(chunks) < logChunkPage {
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
	switch attempt, err := s.logs.LatestLogAttempt(ctx, q.RunID, q.TaskID); {
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
