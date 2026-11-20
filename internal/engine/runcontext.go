package engine

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// DefaultRunContextMaxBytes is the bound a run pins on each task's run
// context (RunPlan.RunContextMaxBytes): the largest RunContext workflow code
// can build, measured as PayloadSize measures an input, with room to spare
// (TestRunContext_MaximalMessageWithinPinnedBound).
const DefaultRunContextMaxBytes = 512

// runContextFor is the run context the task t sends from its row (the task's,
// or a fan-out item's), or nil for a run whose plan carries none. Pure: every
// value is the plan's or the run's recorded state. A run that declares a run
// timeout dispatches only once its clock has started, so a zero deadline here
// is an impossible state, ended explicitly.
func (s *runState) runContextFor(t *orkstackv1.Task, row *orkstackv1.TaskRun) (*orkstackv1.RunContext, error) {
	if s.plan.RunContextSHA256 == "" {
		return nil, nil
	}
	rc := &orkstackv1.RunContext{
		ObjectSha256:        s.plan.RunContextSHA256,
		TreeIndex:           s.plan.RunContextIndex,
		CreatedAt:           copyTimestamp(s.createdAt),
		StartedAt:           copyTimestamp(s.startedAt),
		TaskScheduledAt:     copyTimestamp(row.GetScheduledAt()),
		DeclaredMaxAttempts: max(t.GetRetry().GetMaximumAttempts(), 1),
		OwnAttemptsBefore:   row.GetInterruptions().GetOwnAttemptsUsed(),
	}
	if o := s.plan.Origin; o != nil && o.Kind == RunOriginKindParentRun {
		rc.ParentRunId = o.ID
		rc.RootRunId = o.RootRunID
	}
	if rt := s.runTimeout; rt != nil {
		if rt.deadline.IsZero() {
			return nil, runLevelError(RunTimeoutStateInvalidCode,
				"this run declares a run timeout but dispatched a task before its clock started")
		}
		rc.RunDeadline = timestamppb.New(rt.deadline)
	}
	return rc, nil
}

func copyTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: ts.GetSeconds(), Nanos: ts.GetNanos()}
}

// runContextBound is what a task's run context may add to its input: the
// plan's pinned bound for a run that carries one, else nothing.
func (s *runState) runContextBound() int64 {
	if s.plan.RunContextSHA256 == "" {
		return 0
	}
	return s.plan.RunContextMaxBytes
}

// runContextBytes is a run context's size as PayloadSize measures a message.
func runContextBytes(rc *orkstackv1.RunContext) (int64, error) {
	return task.PayloadSize(rc)
}

// userInputSize is the size of in without its run context, for an input
// already measured at size: size itself when it is within limit (the input
// without its context can only be smaller), otherwise in re-measured with
// the context removed. It first refuses a run context over the plan's bound,
// so the bytes a run context may add are never more than the guard charged.
// Deterministic: both measures are PayloadSize's.
func (s *runState) userInputSize(in *orkstackv1.ExecuteTaskInput, size, limit int64) (int64, error) {
	rc := in.GetRunContext()
	if rc == nil {
		return size, nil
	}
	rcSize, err := runContextBytes(rc)
	if err != nil {
		return 0, contractViolation(fmt.Errorf("%s: task %q: encode its run context: %w",
			task.ReasonInputTooLarge, in.GetTaskId(), err))
	}
	if rcSize > s.plan.RunContextMaxBytes {
		return 0, contractViolation(fmt.Errorf("%s: task %q: its run context serializes to %d bytes, over the run's %d-byte bound",
			task.ReasonInputTooLarge, in.GetTaskId(), rcSize, s.plan.RunContextMaxBytes))
	}
	if size <= limit {
		return size, nil
	}
	without := proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
	without.RunContext = nil
	n, err := task.PayloadSize(without)
	if err != nil {
		return 0, contractViolation(fmt.Errorf("%s: task %q: encode the activity input: %w",
			task.ReasonInputTooLarge, in.GetTaskId(), err))
	}
	return n, nil
}

func contractViolation(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), task.CategoryContractViolation, err)
}

// runContextState is Run.run_context: AVAILABLE for a run whose tasks are
// given a run context (a reference to the run tree's object, written before
// the run started), nil for one whose plan carries none. It says what the run
// sends, not that the object is still readable: the Orker reads and verifies
// it for each attempt that needs it, and a missing or invalid object fails
// that attempt explicitly.
func (s *runState) runContextState() *orkstackv1.RunContextState {
	if s.plan.RunContextSHA256 == "" {
		return nil
	}
	return &orkstackv1.RunContextState{Status: orkstackv1.RunContextStatus_RUN_CONTEXT_STATUS_AVAILABLE}
}
