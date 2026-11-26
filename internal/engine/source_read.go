package engine

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/held"
)

// SourceWorkflowType is the workflow that owns one event source's
// instances. Its id is the source's own.
const SourceWorkflowType = "EventSourceWorkflow"

// The two blocks of a source workflow's memo: all that anything outside the
// workflow goes by.
const (
	sourceMemoCarry = "carry"
	sourceMemoNow   = "now"
)

// workflowDescriber is the one call SourceReads makes.
type workflowDescriber interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// SourceReads reads event sources' workflows.
type SourceReads struct {
	Client workflowDescriber
	// Converter decodes a memo; nil is the engine's.
	Converter converter.DataConverter
}

// Describe is one describe of the workflow of the source of that id, in the
// fields the held check reads. No such workflow, and one that is another
// kind's, is one that is not open; a memo that cannot be decoded is an error,
// never a workflow with no slots.
func (r SourceReads) Describe(ctx context.Context, sourceID string) (held.Workflow, error) {
	resp, err := r.Client.DescribeWorkflowExecution(ctx, sourceID, "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return held.Workflow{}, nil
	}
	if err != nil {
		return held.Workflow{}, fmt.Errorf("engine: describe event source %s: %w", sourceID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || info.GetType().GetName() != SourceWorkflowType {
		return held.Workflow{}, nil
	}
	wf := held.Workflow{Open: true}
	for _, pa := range resp.GetPendingActivities() {
		p := held.Pending{ActivityID: pa.GetActivityId(), Attempt: pa.GetAttempt(), LastWorker: pa.GetLastWorkerIdentity()}
		switch pa.GetState() {
		case enumspb.PENDING_ACTIVITY_STATE_STARTED:
			p.State = held.Started
		case enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
			p.State = held.CancelRequested
		default:
			p.State = held.Scheduled
		}
		wf.Pending = append(wf.Pending, p)
	}
	var now struct {
		Slots map[int32]held.Slot `json:"slot"`
	}
	var carry struct {
		Revoke *held.Revoke `json:"revoke"`
	}
	dc := r.Converter
	if dc == nil {
		dc = DataConverter()
	}
	for name, into := range map[string]any{sourceMemoNow: &now, sourceMemoCarry: &carry} {
		payload, ok := info.GetMemo().GetFields()[name]
		if !ok {
			continue
		}
		if err := dc.FromPayload(payload, into); err != nil {
			return held.Workflow{}, fmt.Errorf("engine: event source %s's memo %q: %w", sourceID, name, err)
		}
	}
	wf.Memo = held.Memo{Slots: now.Slots, Revoke: carry.Revoke}
	return wf, nil
}
