package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// maxLineage bounds the executions one run's resets are followed through; a
// run is resumed at most 20 times.
const maxLineage = 64

// executionStart is how one execution of a run began.
type executionStart struct {
	// carry is what it carried on from, nil for a first history.
	carry *orkstackv1.RunContinuation
	// reset is the reset that made it; its BaseRunID is empty when none did.
	reset ResetFacts
	// scope is the newest resume scope among that reset and those before it.
	scope uint64
	// gone: the cluster no longer holds it.
	gone bool
}

// startOf reads how execution began. One no reset made names itself as its
// original in its first event, and no more of it is read; of another, the
// events up to its own reset.
func (r *TemporalRunReader) startOf(ctx context.Context, workflowID, execution string) (executionStart, error) {
	if r.client == nil {
		return executionStart{}, fmt.Errorf("engine: workflow deletion reader has no Temporal client")
	}
	iter := r.client.GetWorkflowHistory(ctx, workflowID, execution, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var out executionStart
	for first := true; iter.HasNext(); first = false {
		event, err := iter.Next()
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return executionStart{gone: true}, nil
			}
			return executionStart{}, fmt.Errorf("engine: read the start of run %s: %w", workflowID, err)
		}
		if !first {
			if failed := event.GetWorkflowTaskFailedEventAttributes(); failed.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
				if key, ok := ParseResumePlanKey(failed.GetFailure().GetMessage()); ok {
					out.scope = max(out.scope, key.Scope)
				}
			}
			if facts, ok := findResetEvent([]*historypb.HistoryEvent{event}, execution); ok {
				out.reset = facts
				return out, nil
			}
			continue
		}
		attrs := event.GetWorkflowExecutionStartedEventAttributes()
		if attrs == nil {
			return out, nil
		}
		dc := r.runConverter(ctx, workflowID)
		if err := dc.holdStart(attrs); err != nil {
			return executionStart{}, err
		}
		if out.carry, err = decodeCarry(dc, attrs); err != nil {
			return executionStart{}, fmt.Errorf("engine: run %s: %w", workflowID, err)
		}
		if original := attrs.GetOriginalExecutionRunId(); original == "" || original == execution {
			return out, nil
		}
	}
	return out, nil
}

// lineage is every execution of a run up to current, oldest first: what its
// resumes set aside, and the histories each carried on from.
func (r *TemporalRunReader) lineage(ctx context.Context, workflowID, current string) ([]string, error) {
	var newestFirst []string
	add := func(execution string) bool {
		if execution == "" || slices.Contains(newestFirst, execution) {
			return false
		}
		newestFirst = append(newestFirst, execution)
		return true
	}
	var follow func(execution string, resets int) error
	follow = func(execution string, resets int) error {
		if !add(execution) {
			return nil
		}
		if resets > maxLineage {
			return fmt.Errorf("engine: run %s has more executions than its resumes can have made", workflowID)
		}
		start, err := r.startOf(ctx, workflowID, execution)
		if err != nil {
			return err
		}
		segments := start.carry.GetSegments()
		for i := len(segments) - 1; i >= 0; i-- {
			earlier := segments[i].GetExecutionId()
			if !add(earlier) || start.carry.GetResume() == nil {
				continue
			}
			// One of them is a resume's, whose own reset names its base; what it
			// carried on from is among these.
			was, err := r.startOf(ctx, workflowID, earlier)
			if err != nil {
				return err
			}
			if err := follow(was.reset.BaseRunID, resets+1); err != nil {
				return err
			}
		}
		return follow(start.reset.BaseRunID, resets+1)
	}
	if err := follow(current, 0); err != nil {
		return nil, err
	}
	slices.Reverse(newestFirst)
	return newestFirst, nil
}

// NewestEnding is the resume scope a run's current execution ends under, as
// the reader has it, 0 for one no resume made, and when it closed, zero while
// it runs. found is false for a run the cluster does not hold. namespace is the
// deployment's Temporal partition.
func (r *TemporalRunReader) NewestEnding(ctx context.Context, namespace, workflowID string) (scope uint64, closedAt time.Time, found bool, err error) {
	if r.admin == nil {
		return 0, time.Time{}, false, fmt.Errorf("engine: workflow lookup reader has no Temporal client")
	}
	described, err := r.admin.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return 0, time.Time{}, false, nil
		}
		return 0, time.Time{}, false, err
	}
	info := described.GetWorkflowExecutionInfo()
	if at := info.GetCloseTime(); at != nil {
		closedAt = at.AsTime()
	}
	start, err := r.startOf(ctx, workflowID, info.GetExecution().GetRunId())
	switch {
	case err != nil:
		return 0, time.Time{}, false, err
	case start.gone:
		return 0, time.Time{}, false, nil
	}
	// A history that carried on and was not reset since holds no resume's reset:
	// its carry says which resume it is of.
	if scope = start.scope; scope == 0 {
		scope = start.carry.GetResume().GetSeqBase()
	}
	return scope, closedAt, true, nil
}
