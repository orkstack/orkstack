package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// ErrRunIDTaken reports that a run id is already the workflow id of an
// execution on this cluster, running or finished. It is the server's answer,
// not a guess, and it is what lets a caller reissue the id instead of
// treating the collision as a failed submission.
var ErrRunIDTaken = errors.New("run id already taken")

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
// This engine's orker queue is bound into the workflow input here, before the
// workflow starts, because this is the last place that may read process
// configuration: workflow code must be deterministic, so it cannot consult the
// environment for a queue name. Binding it records the queue in the run's
// history, which makes every replay dispatch where the original run did — and
// lets a second engine execute a workflow this one started without redirecting
// its tasks.
//
// The orkflow travels verbatim. It used to be cloned so a queue name could be
// written into each task; since M10.1 a definition carries no queue at all,
// and the stored document is passed exactly as the bucket holds it.
//
// The three start policies make the run id single use for all time, which is
// the half of run identity that entropy cannot provide (see RunIDGenerator):
//
//   - REJECT_DUPLICATE refuses an id a finished run already used;
//   - CONFLICT_POLICY_FAIL refuses an id a running run already holds;
//   - WorkflowExecutionErrorWhenAlreadyStarted turns both into an error.
//
// That last one defaults to false, and its default is a live hazard: the SDK
// then swallows the server's already-started error and hands back a handle to
// whichever execution holds the id (v1.47.0
// internal/internal_workflow_client.go:2201), so a replica whose id collided
// would report someone else's run as its own.
func (s *TemporalStarter) StartRun(
	ctx context.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) error {
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       runID,
		TaskQueue:                                s.queue,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		// The memo is how `run list` names an orkflow without reading a
		// history per run: visibility returns it with the execution, on every
		// replica. It is a copy of the definition's identity, never a second
		// source of truth for it.
		Memo: map[string]any{
			MemoOrkflowName:    wf.GetName(),
			MemoOrkflowVersion: wf.GetVersion(),
			MemoNamespace:      wf.GetNamespace(),
		},
	}, RunWorkflowType, runID, wf, input, s.orkerQueue)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			// Joined rather than wrapped: the caller matches ErrRunIDTaken to
			// decide whether to reissue, and the server's error stays readable
			// because it names the execution that holds the id.
			return fmt.Errorf("start run %s: %w", runID, errors.Join(ErrRunIDTaken, err))
		}
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
