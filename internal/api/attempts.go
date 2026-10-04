package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ListTaskAttempts serves the retry history of a run's tasks.
//
// It is ListRunArtifacts' shape — namespace-scoped lookup, an optional task
// selector, references out of the record rather than anything a caller named —
// with one difference: an artifact list can only report what exists, while an
// attempt list has to report what is MISSING. A retried task whose first Orker
// died leaves no record for that attempt, and returning only the records would
// quietly renumber the timeline, so every attempt number up to the highest one
// anything knows about is present, the ones with no record marked UNREPORTED.
//
// Nothing here reads process memory: the run comes from the reader (the
// cluster) and the records from the shared store, so the replica that never
// started the run answers it identically.
func (s *Service) ListTaskAttempts(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListTaskAttemptsRequest],
) (*connect.Response[orkstackv1.ListTaskAttemptsResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	if err := checkItemRequiresTask(req.Msg.GetTaskId(), req.Msg.Item); err != nil {
		return nil, err
	}
	if s.attempts == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("task attempt store is not configured"))
	}
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	taskID := req.Msg.GetTaskId()
	if taskID != "" && !hasTask(run, taskID) {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("task %q not found in run %q", taskID, run.GetId()))
	}
	selected, err := itemRunOf(run, taskID, req.Msg.Item)
	if err != nil {
		return nil, err
	}
	// Task order is the snapshot's order, never the store's: the run says what
	// the tasks are and in which order an operator already sees them, and a
	// listing sorted by key would put "build" after "apply" for no reason a
	// reader could see.
	attempts := make([]*orkstackv1.TaskAttempt, 0, len(run.GetTaskRuns()))
	for _, task := range run.GetTaskRuns() {
		if taskID != "" && task.GetTaskId() != taskID {
			continue
		}
		// A fan-out answers one timeline per item, items ascending: its own
		// row has no attempts (spec each-fan-out §7.2).
		if task.GetEach() != nil {
			items := task.GetItems()
			if selected != nil {
				items = []*orkstackv1.TaskRun{selected}
			}
			timelines, err := s.itemTimelines(ctx, run, items)
			if err != nil {
				return nil, err
			}
			attempts = append(attempts, timelines...)
			continue
		}
		records, err := s.attempts.ListAttempts(ctx, run.GetId(), task.GetTaskId(), artifacts.NoItem)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of task %q in run %q: %w",
					task.GetTaskId(), run.GetId(), err))
		}
		attempts = append(attempts, s.taskAttemptTimeline(ctx, run, task, records)...)
	}
	return connect.NewResponse(&orkstackv1.ListTaskAttemptsResponse{Attempts: attempts}), nil
}

// taskAttemptTimeline merges one task's stored records with the attempts that
// left no record, ascending and gapless.
//
// The highest attempt number is whatever the three sources that could know it
// agree is highest:
//
//   - TaskRun.attempt, the final attempt the engine saw. It is 0 for a task
//     Temporal timed out (the error carries no attempt) and, for an adopted
//     commit, the COMMITTING attempt rather than the last one — which is
//     exactly why it is not trusted alone.
//   - the records themselves, which is the only source for an attempt that
//     failed and was retried.
//   - the log store, which knows an attempt happened whenever it wrote a byte,
//     including one whose Orker died before it could file a record.
//
// A task nothing knows anything about (never dispatched) yields an EMPTY
// timeline, not a synthesized attempt 1: no attempt has happened, and claiming
// an unreported one would invent a failure.
func (s *Service) taskAttemptTimeline(
	ctx context.Context,
	run *orkstackv1.Run,
	task *orkstackv1.TaskRun,
	records []*orkstackv1.TaskAttempt,
) []*orkstackv1.TaskAttempt {
	highest := task.GetAttempt()
	byAttempt := make(map[int32]*orkstackv1.TaskAttempt, len(records))
	for _, record := range records {
		if record.GetAttempt() < 1 {
			// Not addressable as a key, so it cannot have been written by this
			// build. Skipped rather than refused: a foreign or corrupt object
			// must not cost an operator the rest of the timeline.
			continue
		}
		if _, seen := byAttempt[record.GetAttempt()]; seen {
			continue
		}
		byAttempt[record.GetAttempt()] = record
		if record.GetAttempt() > highest {
			highest = record.GetAttempt()
		}
	}
	if logged := s.latestLoggedAttempt(ctx, run.GetId(), task.GetTaskId(), artifacts.ItemOf(task.Item)); logged > highest {
		highest = logged
	}
	if highest < 1 {
		return nil
	}
	timeline := make([]*orkstackv1.TaskAttempt, 0, highest)
	for attempt := int32(1); attempt <= highest; attempt++ {
		if record, ok := byAttempt[attempt]; ok {
			timeline = append(timeline, record)
			continue
		}
		timeline = append(timeline, &orkstackv1.TaskAttempt{
			RunId:   run.GetId(),
			TaskId:  task.GetTaskId(),
			Attempt: attempt,
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED,
			Item:    artifacts.ItemOf(task.Item).Proto(),
		})
	}
	return timeline
}

// latestLoggedAttempt asks the log store how far a task's attempts got, which
// is the one durable attempt count that predates attempt records.
//
// It never fails the RPC. The records are the answer; this only widens the
// range they are laid out over, and refusing to show a retry history because
// the log index was briefly unreachable would trade the whole answer for a
// footnote. No store wired, nothing logged, or an unreachable store all read
// the same here: zero, meaning "no opinion".
func (s *Service) latestLoggedAttempt(ctx context.Context, runID, taskID string, item artifacts.Item) int32 {
	if s.logs == nil {
		return 0
	}
	attempt, err := s.logs.LatestLogAttempt(ctx, runID, taskID, item)
	switch {
	case err == nil:
		return attempt
	case errors.Is(err, artifacts.ErrNotFound):
		return 0
	default:
		s.logger.Warn("latest logged attempt unavailable; attempt timeline may be short",
			"run_id", runID, "task_id", taskID, "error", err)
		return 0
	}
}

// itemTimelines reads one timeline per item of a fan-out, at most storeFanout
// reads in flight, reassembled in index order, each record carrying its item.
func (s *Service) itemTimelines(ctx context.Context, run *orkstackv1.Run, items []*orkstackv1.TaskRun) ([]*orkstackv1.TaskAttempt, error) {
	timelines := make([][]*orkstackv1.TaskAttempt, len(items))
	errs := make([]error, len(items))
	inParallel(len(items), func(i int) {
		item := items[i]
		records, err := s.attempts.ListAttempts(ctx, run.GetId(), item.GetTaskId(), artifacts.ItemOf(item.Item))
		if err != nil {
			errs[i] = connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of task %q item %d in run %q: %w",
					item.GetTaskId(), item.GetItem(), run.GetId(), err))
			return
		}
		timelines[i] = s.taskAttemptTimeline(ctx, run, item, records)
	})
	var out []*orkstackv1.TaskAttempt
	for i := range items {
		if errs[i] != nil {
			return nil, errs[i]
		}
		out = append(out, timelines[i]...)
	}
	return out, nil
}
