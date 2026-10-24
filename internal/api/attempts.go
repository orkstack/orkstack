package api

import (
	"context"
	"errors"
	"fmt"
	"sort"

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
	// The whole run at once — the run page's seed, `run get --attempts`, `run
	// timing` — is read run-wide when the stores can: one listing of the
	// run's records and one of its log attempts, where the scope-by-scope
	// path below costs a listing of each (and a listing of every log chunk of
	// each) per task and per fan-out item. The answer is identical.
	if taskID == "" {
		if reader, ok := s.attempts.(artifacts.RunAttemptReader); ok {
			attempts, err := s.runAttemptTimelines(ctx, run, reader)
			if err != nil {
				return nil, err
			}
			return connect.NewResponse(&orkstackv1.ListTaskAttemptsResponse{Attempts: attempts}), nil
		}
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
		// row has no attempts.
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
		logged := s.latestLoggedAttempt(ctx, run.GetId(), task.GetTaskId(), artifacts.NoItem)
		attempts = append(attempts, taskAttemptTimeline(run, task, records, logged)...)
	}
	return connect.NewResponse(&orkstackv1.ListTaskAttemptsResponse{Attempts: attempts}), nil
}

// runAttemptTimelines answers ListTaskAttempts for a whole run from two
// run-wide reads: every record key in one listing (then the records, at most
// storeFanout in flight), and — when the log store can — every scope's
// highest logged attempt in one more. The timelines are then laid out exactly
// as the scope-by-scope path lays them out: snapshot task order, a fan-out's
// items in their order, each by taskAttemptTimeline.
func (s *Service) runAttemptTimelines(
	ctx context.Context,
	run *orkstackv1.Run,
	reader artifacts.RunAttemptReader,
) ([]*orkstackv1.TaskAttempt, error) {
	refs, err := reader.ListRunAttemptRefs(ctx, run.GetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list the attempt records of run %q: %w", run.GetId(), err))
	}
	records := make([]*orkstackv1.TaskAttempt, len(refs))
	errs := make([]error, len(refs))
	inParallel(len(refs), func(i int) {
		record, err := reader.GetAttempt(ctx, refs[i])
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
			// Reclaimed between the listing and this read: a missing record
			// reads as a missing record, as it does scope by scope.
		case err != nil:
			errs[i] = connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt %d of task %q in run %q: %w",
					refs[i].Attempt, refs[i].TaskID, run.GetId(), err))
		default:
			records[i] = record
		}
	})
	byScope := make(map[artifacts.LogScope][]*orkstackv1.TaskAttempt)
	for i, ref := range refs {
		if errs[i] != nil {
			return nil, errs[i]
		}
		if records[i] == nil {
			continue
		}
		scope := artifacts.LogScope{TaskID: ref.TaskID, Item: ref.Item}
		byScope[scope] = append(byScope[scope], records[i])
	}
	logged := s.latestLoggedAttempts(ctx, run.GetId())

	timeline := func(row *orkstackv1.TaskRun) []*orkstackv1.TaskAttempt {
		scope := artifacts.LogScope{TaskID: row.GetTaskId(), Item: artifacts.ItemOf(row.Item)}
		scoped := byScope[scope]
		// The listing is in key order, which is attempt order; the stable
		// sort by the record's own attempt is what ListAttempts does too.
		sort.SliceStable(scoped, func(i, j int) bool { return scoped[i].GetAttempt() < scoped[j].GetAttempt() })
		mark := int32(0)
		if logged != nil {
			mark = logged[scope]
		} else {
			mark = s.latestLoggedAttempt(ctx, run.GetId(), row.GetTaskId(), scope.Item)
		}
		return taskAttemptTimeline(run, row, scoped, mark)
	}
	attempts := make([]*orkstackv1.TaskAttempt, 0, len(run.GetTaskRuns()))
	for _, task := range run.GetTaskRuns() {
		if task.GetEach() != nil {
			for _, item := range task.GetItems() {
				attempts = append(attempts, timeline(item)...)
			}
			continue
		}
		attempts = append(attempts, timeline(task)...)
	}
	return attempts, nil
}

// latestLoggedAttempts is latestLoggedAttempt for a whole run, in one call,
// when the log store can answer run-wide. nil means it cannot (no store, or a
// store without the run-wide read), and the caller asks scope by scope. Like
// the per-scope read it never fails the RPC: an unreachable store is "no
// opinion" for every scope.
func (s *Service) latestLoggedAttempts(ctx context.Context, runID string) map[artifacts.LogScope]int32 {
	if s.logs == nil {
		return map[artifacts.LogScope]int32{}
	}
	lister, ok := s.logs.(artifacts.RunLogAttemptLister)
	if !ok {
		return nil
	}
	latest, err := lister.LatestLogAttempts(ctx, runID)
	if err != nil {
		s.logger.Warn("latest logged attempts unavailable; attempt timelines may be short",
			"run_id", runID, "error", err)
		return map[artifacts.LogScope]int32{}
	}
	return latest
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
//
// logged is the log store's answer for this scope (latestLoggedAttempt or the
// run-wide latestLoggedAttempts): 0 when it has none.
func taskAttemptTimeline(
	run *orkstackv1.Run,
	task *orkstackv1.TaskRun,
	records []*orkstackv1.TaskAttempt,
	logged int32,
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
	if logged > highest {
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
		logged := s.latestLoggedAttempt(ctx, run.GetId(), item.GetTaskId(), artifacts.ItemOf(item.Item))
		timelines[i] = taskAttemptTimeline(run, item, records, logged)
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
