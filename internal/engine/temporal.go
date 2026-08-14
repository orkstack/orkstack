package engine

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// TemporalStarter starts and cancels runs on a Temporal cluster and discovers
// Orker plugins. It is the production implementation of the API's run starter
// and plugin-lister seams.
type TemporalStarter struct {
	client client.Client
	queue  string
	// orkerQueue is the queue Orkers poll for task activities. It defaults to
	// queue (normally task.DefaultQueue) and can be overridden so discovery
	// follows Orkers on a custom queue.
	orkerQueue string
}

// NewTemporalStarter returns a starter that dispatches RunWorkflow onto queue.
// An empty queue means task.DefaultQueue.
func NewTemporalStarter(c client.Client, queue string) *TemporalStarter {
	if queue == "" {
		queue = task.DefaultQueue
	}
	return &TemporalStarter{client: c, queue: queue, orkerQueue: queue}
}

// NewTemporalStarterWithOrkerQueue is NewTemporalStarter with an explicit
// orker queue for plugin discovery, used when Orkers poll a custom queue.
func NewTemporalStarterWithOrkerQueue(c client.Client, runQueue, orkerQueue string) *TemporalStarter {
	s := NewTemporalStarter(c, runQueue)
	if orkerQueue != "" {
		s.orkerQueue = orkerQueue
	}
	return s
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

// ListPlugins executes the Orker's ListPlugins activity and returns the
// descriptors of every plugin the connected Orker can run.
func (s *TemporalStarter) ListPlugins(ctx context.Context) ([]plugin.Descriptor, error) {
	var descriptors []plugin.Descriptor
	handle, err := s.client.ExecuteActivity(ctx, client.StartActivityOptions{
		ID:                     "list-plugins-" + fmt.Sprint(time.Now().UnixNano()),
		TaskQueue:              s.orkerQueue,
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}, task.ActivityListPlugins)
	if err != nil {
		return nil, fmt.Errorf("list plugins: %w", err)
	}
	if err := handle.Get(ctx, &descriptors); err != nil {
		return nil, fmt.Errorf("list plugins: %w", err)
	}
	return descriptors, nil
}
