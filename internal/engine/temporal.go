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
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrRunIDTaken reports that a run id is already the workflow id of an
// execution on this cluster, running or finished. It is the server's answer,
// not a guess, and it is what lets a caller reissue the id instead of
// treating the collision as a failed submission.
var ErrRunIDTaken = errors.New("run id already taken")

// TemporalStarter starts and cancels runs on a Temporal cluster and probes
// pool liveness. It is the production implementation of the API's run starter
// and Orker-describer seams.
type TemporalStarter struct {
	client client.Client
	queue  string
	// orkerQueue is the queue Orkers poll for task activities. It defaults to
	// queue (normally task.DefaultQueue) and can be overridden so both the
	// liveness probe and task dispatch follow Orkers on a custom queue.
	orkerQueue string

	// namespaceIndexed reports that this cluster has the OrkflowNamespace
	// search attribute, so a run may be stamped with the namespace it belongs
	// to and listed by it. Stamping an attribute a cluster does not have fails
	// the start, so it is off until a boot says otherwise.
	namespaceIndexed bool

	// originIndexed reports that this cluster has the OrkflowOriginKind and
	// OrkflowOriginId search attributes (task.EnsureRunOriginSearchAttributes).
	// It is a separate answer from namespaceIndexed: a cluster may have
	// accepted the namespace attribute in an earlier release and refuse the
	// two origin attributes now, and stamping one it does not have fails
	// every submit.
	originIndexed bool
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
// orker queue for the liveness probe, used when Orkers poll a custom queue.
func NewTemporalStarterWithOrkerQueue(c client.Client, runQueue, orkerQueue string) *TemporalStarter {
	s := NewTemporalStarter(c, runQueue)
	if orkerQueue != "" {
		s.orkerQueue = orkerQueue
	}
	return s
}

// WithNamespaceIndex records whether the cluster indexes a run's OrkStack
// namespace (task.EnsureRunSearchAttributes). The engine's boot is what knows,
// because it is what registered it.
func (s *TemporalStarter) WithNamespaceIndex(indexed bool) *TemporalStarter {
	s.namespaceIndexed = indexed
	return s
}

// WithOriginIndex records whether the cluster indexes a run's origin
// (task.EnsureRunOriginSearchAttributes), on the terms WithNamespaceIndex
// records the namespace answer.
func (s *TemporalStarter) WithOriginIndex(indexed bool) *TemporalStarter {
	s.originIndexed = indexed
	return s
}

// StartRun starts RunWorkflow with the run id as the Temporal workflow id.
//
// The run's routing is bound into the workflow input here, before the workflow
// starts, because this is the last place that may read process configuration:
// workflow code must be deterministic, so it cannot consult a namespace
// document, the environment, or the cluster for where a task should run. The
// plan carries the answer — every pool this run uses, mapped to the channel
// that carries it — so the routing is recorded in the run's history. Every
// replay dispatches where the original run did, and a second engine can finish
// a run this one started without redirecting its tasks onto its own defaults.
//
// The orkflow travels as the plan resolved it: a clone of the stored document
// with every task's pool filled in (its own, its namespace's default, or the
// built-in "main"). The stored document is untouched.
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
	plan RunPlan,
) error {
	options := client.StartWorkflowOptions{
		ID:                                       runID,
		TaskQueue:                                s.queue,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
	options.Memo = RunStartMemo(wf, plan)
	if attrs := RunStartSearchAttributes(wf, plan, s.namespaceIndexed, s.originIndexed); len(attrs) > 0 {
		options.TypedSearchAttributes = temporal.NewSearchAttributes(attrs...)
	}
	_, err := s.client.ExecuteWorkflow(ctx, options,
		RunWorkflowType, runID, wf, input, plan)
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

// RunStartMemo is the memo every run is started with — top-level by StartRun,
// or as a child by ScheduleFireWorkflow — so a listing names a run the same
// way whatever started it. Pure: it reads only its arguments.
//
// The memo is how `run list` names an orkflow without reading a history per
// run: visibility returns it with the execution, on every replica. It is a
// copy of the definition's identity, never a second source of truth for it.
// The origin's kind and id ride it for the same reason: a listing row can say
// "trigger trg-…" without a history read per run.
func RunStartMemo(wf *orkstackv1.Orkflow, plan RunPlan) map[string]any {
	memo := map[string]any{
		MemoOrkflowName:    wf.GetName(),
		MemoOrkflowVersion: wf.GetVersion(),
		MemoNamespace:      wf.GetNamespace(),
	}
	if originSlug, ok := artifacts.OriginKindSlug(plan.Origin.Proto().GetKind()); ok {
		memo[MemoOriginKind] = originSlug
		memo[MemoOriginID] = plan.Origin.ID
	}
	return memo
}

// RunStartSearchAttributes is the search-attribute set for the same two
// callers, gated by the engine's boot answers exactly as StartRun gates them.
// Pure.
//
// Search attributes are collected and set ONCE: NewSearchAttributes takes
// every update in one call. The namespace is stamped a second time, as a
// search attribute, because a memo is returned with a record and cannot
// select one. A tenant listing its runs needs the cluster to do the selecting
// — see ListRuns — and this is the value it selects on. It is resolved, so
// the built-in default is spelled one way whether the document named it or
// left it empty. The origin goes in under its own boot-time answer, because a
// cluster may have accepted OrkflowNamespace last release and refuse the two
// new attributes now.
func RunStartSearchAttributes(
	wf *orkstackv1.Orkflow,
	plan RunPlan,
	namespaceIndexed, originIndexed bool,
) []temporal.SearchAttributeUpdate {
	var attrs []temporal.SearchAttributeUpdate
	if namespaceIndexed {
		attrs = append(attrs, task.SearchKeyOrkflowNamespace.ValueSet(
			artifacts.ResolveNamespace(wf.GetNamespace())))
	}
	if originSlug, ok := artifacts.OriginKindSlug(plan.Origin.Proto().GetKind()); ok && originIndexed {
		attrs = append(attrs,
			task.SearchKeyOrkflowOriginKind.ValueSet(originSlug),
			task.SearchKeyOrkflowOriginID.ValueSet(plan.Origin.ID))
	}
	return attrs
}

// IndexAnswers reports the boot answers this starter stamps with, for the
// activity that prepares a scheduled run to carry onto the fire.
func (s *TemporalStarter) IndexAnswers() (namespaceIndexed, originIndexed bool) {
	return s.namespaceIndexed, s.originIndexed
}

// CancelRun requests cancellation of a running workflow.
func (s *TemporalStarter) CancelRun(ctx context.Context, runID, _ string) error {
	if err := s.client.CancelWorkflow(ctx, runID, ""); err != nil {
		return fmt.Errorf("cancel run %s: %w", runID, err)
	}
	return nil
}

// ErrRunEnded reports that a run's execution has already closed, so nothing
// can be delivered to it.
var ErrRunEnded = errors.New("run has ended")

// SignalAwait delivers one resolution to a running run: exactly one
// client.SignalWorkflow call with ResolveAwaitSignal. It returns when the
// cluster has durably recorded it in the run's history, not when the run has
// applied it. A closed execution (serviceerror.NotFound) is ErrRunEnded,
// joined with the server's error; anything else is returned wrapped.
//
// It queues nothing and retries nothing: the caller is answered with the
// outcome of this one call (docs/DSL.md "Awaited tasks").
func (s *TemporalStarter) SignalAwait(ctx context.Context, runID string, signal *orkstackv1.AwaitSignal) error {
	if err := s.client.SignalWorkflow(ctx, runID, "", ResolveAwaitSignal, signal); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("signal run %s: %w", runID, errors.Join(ErrRunEnded, err))
		}
		return fmt.Errorf("signal run %s: %w", runID, err)
	}
	return nil
}

// DescribeOrker executes the Orker's DescribeOrker activity on this engine's
// default execution channel and returns the identity of whichever Orker
// answered.
//
// A successful answer is how a pool proves it is alive; which images exist is
// engine configuration (§4.2), so the probe returns identity only.
func (s *TemporalStarter) DescribeOrker(ctx context.Context) (*orkstackv1.DescribeOrkerResult, error) {
	return s.describeOrkerOn(ctx, s.orkerQueue, 10*time.Second)
}

// describeOrkerOn asks one execution channel's Orkers who they are. It is the
// shared half of DescribeOrker and the per-pool report (OrkerOnPool): the
// question is the same activity, the channel is what differs.
func (s *TemporalStarter) describeOrkerOn(
	ctx context.Context,
	queue string,
	within time.Duration,
) (*orkstackv1.DescribeOrkerResult, error) {
	result := &orkstackv1.DescribeOrkerResult{}
	handle, err := s.client.ExecuteActivity(ctx, client.StartActivityOptions{
		// The id is a collision-free token, not a clock reading: this call is
		// made concurrently by the admin inventory's pool fan-out
		// (`activityid.go`).
		ID:                     newActivityID("describe-orker"),
		TaskQueue:              queue,
		ScheduleToCloseTimeout: within,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}, task.ActivityDescribeOrker)
	if err != nil {
		return nil, fmt.Errorf("describe orker: %w", err)
	}
	if err := handle.Get(ctx, result); err != nil {
		return nil, fmt.Errorf("describe orker: %w", err)
	}
	return result, nil
}
