package engine

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// FirstStartRead is what the cluster says of a run id: whether it holds an
// execution under it, and, when it does, the idempotency fingerprint the
// plan of the chain's FIRST start carried ("" for a run started with no key,
// or by something other than an engine).
type FirstStartRead struct {
	Held        bool
	Fingerprint string
}

// FirstStart reads a run id as a retry under an idempotency key must read it
// (workload-identity.md §3.1 rule 7): a describe, then the first event of the
// first execution of the id's chain.
//
// Only one answer is "not held": the cluster saying it has no execution under
// the id. Every other failure is an error and never a "no" — a describe that
// fails, an empty first-execution id, a first history the cluster no longer
// has, a read that times out, a first event that is not a start or does not
// decode. A caller that read an error as "not held" would start a second run
// beside a live one.
//
// The first execution's start input is read by its execution id, never the
// current generation's: a start's input is fixed by the client that made it,
// while a later generation's is chosen by whoever answered the run's workflow
// tasks.
func (r *TemporalRunReader) FirstStart(ctx context.Context, runID string) (FirstStartRead, error) {
	if r.client == nil {
		return FirstStartRead{}, fmt.Errorf("engine: first-start reader has no Temporal client")
	}
	resp, err := r.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return FirstStartRead{}, nil
		}
		return FirstStartRead{}, fmt.Errorf("describe run %s: %w", runID, err)
	}
	first := resp.GetWorkflowExecutionInfo().GetFirstRunId()
	if first == "" {
		return FirstStartRead{}, fmt.Errorf("describe run %s: the cluster named no first execution", runID)
	}
	history := r.client.GetWorkflowHistory(ctx, runID, first, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !history.HasNext() {
		return FirstStartRead{}, fmt.Errorf("read the first start of run %s: no history", runID)
	}
	event, err := history.Next()
	if err != nil {
		return FirstStartRead{}, fmt.Errorf("read the first start of run %s: %w", runID, err)
	}
	attrs := event.GetWorkflowExecutionStartedEventAttributes()
	if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED || attrs == nil {
		return FirstStartRead{}, fmt.Errorf("read the first start of run %s: its first event is %s",
			runID, event.GetEventType())
	}
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 4 {
		// A run's start always carries (run id, orkflow, input, plan); one
		// that does not was not started by an engine and carries no key.
		return FirstStartRead{Held: true}, nil
	}
	var plan RunPlan
	if err := r.converter.FromPayload(payloads[3], &plan); err != nil {
		return FirstStartRead{}, fmt.Errorf("decode the first start's plan of run %s: %w", runID, err)
	}
	return FirstStartRead{Held: true, Fingerprint: plan.IdempotencyFingerprint}, nil
}
