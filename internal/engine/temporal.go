package engine

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

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
	// queue (normally task.DefaultQueue) and can be overridden so both plugin
	// discovery and task dispatch follow Orkers on a custom queue.
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
//
// The run's tasks are bound to this engine's orker queue here, before the
// workflow starts, because this is the last place that may read process
// configuration: workflow code must be deterministic, so it cannot consult the
// environment for a queue name. Binding it into the workflow input records the
// queue in the run's history, which makes every replay dispatch where the
// original run did — and lets a second engine execute a workflow this one
// started without redirecting its tasks.
func (s *TemporalStarter) StartRun(
	ctx context.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) error {
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        runID,
		TaskQueue: s.queue,
	}, WorkflowName, runID, pinTaskQueues(wf, s.orkerQueue), input)
	if err != nil {
		return fmt.Errorf("start run %s: %w", runID, err)
	}
	return nil
}

// pinTaskQueues returns wf with every task that names no queue of its own
// bound to queue. A task that names one keeps it: an explicit `queue:` in the
// DSL routes to a dedicated Orker pool and outranks the engine's default.
//
// The result is a copy. The stored definition is immutable and shared by every
// engine pointed at the same bucket, so the queue one process happens to be
// configured with belongs to that run's input, never to the orkflow.
func pinTaskQueues(wf *orkstackv1.Orkflow, queue string) *orkstackv1.Orkflow {
	if wf == nil || queue == "" {
		return wf
	}
	unpinned := false
	for _, t := range wf.GetTasks() {
		if t.GetQueue() == "" {
			unpinned = true
			break
		}
	}
	if !unpinned {
		return wf
	}
	pinned := proto.Clone(wf).(*orkstackv1.Orkflow)
	for _, t := range pinned.GetTasks() {
		if t.GetQueue() == "" {
			t.Queue = queue
		}
	}
	return pinned
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
