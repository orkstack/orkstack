package engine

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// TemporalStarter starts and cancels runs on a Temporal cluster. It is the
// production implementation of the API's run starter seam.
type TemporalStarter struct {
	client client.Client
	queue  string
}

// NewTemporalStarter returns a starter that dispatches RunWorkflow onto queue.
// An empty queue means task.DefaultQueue.
func NewTemporalStarter(c client.Client, queue string) *TemporalStarter {
	if queue == "" {
		queue = task.DefaultQueue
	}
	return &TemporalStarter{client: c, queue: queue}
}

// StartRun starts RunWorkflow with the run id as the Temporal workflow id.
func (s *TemporalStarter) StartRun(
	ctx context.Context,
	runID string,
	wf *orkstackv1.Workflow,
	input *orkstackv1.Payload,
) error {
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        runID,
		TaskQueue: s.queue,
	}, WorkflowName, runID, wf, input)
	if err != nil {
		return fmt.Errorf("start run %s: %w", runID, err)
	}
	return nil
}

// CancelRun requests cancellation of a running workflow.
func (s *TemporalStarter) CancelRun(ctx context.Context, runID, _ string) error {
	if err := s.client.CancelWorkflow(ctx, runID, ""); err != nil {
		return fmt.Errorf("cancel run %s: %w", runID, err)
	}
	return nil
}
