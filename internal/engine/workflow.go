package engine

import (
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

const (
	// RunWorkflowType is the registered Temporal workflow type for a run. It
	// names a Temporal concept, not an OrkStack one: the OrkStack entity a run
	// executes is an Orkflow, and this is the workflow TYPE that runs it.
	RunWorkflowType = "RunWorkflow"

	// EventQueue is the Temporal task queue for engine-internal projection
	// activities. It is deliberately separate from task.DefaultQueue so Orkers
	// polling for task execution never receive engine projection work, and
	// carries the ork- prefix every OrkStack Temporal name uses.
	EventQueue = "ork-engine-events"

	// ActivityPublishEvent is the registered name of the projection activity.
	ActivityPublishEvent = "PublishEvent"
)

// eventActivityTimeout bounds one projection activity attempt.
const eventActivityTimeout = 10 * time.Second

// RunWorkflow is the deterministic orchestration workflow. It interprets the
// compiled task DAG, schedules ready tasks as ExecuteTask activities on the
// queue their pool maps to, threads upstream outputs/artifacts into downstream
// tasks, and emits lifecycle events through the projection activity.
//
// Every side effect (event publication, read-model updates) happens in an
// activity; the workflow body uses only workflow.Now for time, ordered
// iteration, and activity futures.
//
// Arguments are passed separately rather than in one struct because the
// Temporal data converter encodes top-level protobuf messages with protojson;
// nesting them in a plain Go struct would fall back to encoding/json, which
// cannot round-trip google.protobuf.Struct.
//
// orkerQueue is where a task that names no pool runs. It is an argument, not a
// lookup, for two reasons that both matter: workflow code is deterministic and
// may not read process configuration, and the orkflow message must not carry
// it — a queue is Temporal's word, the definition is the user's document, and
// the two stopped touching at M10.1. Binding it into the input records it in
// history, so every replay dispatches where the original run did and a second
// engine finishes a run this one started without redirecting its tasks.
func RunWorkflow(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	orkerQueue string,
) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, temporal.NewNonRetryableApplicationError("run id is required", "INVALID_RUN", nil)
	}
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, temporal.NewNonRetryableApplicationError("orkflow has no tasks", "INVALID_ORKFLOW", nil)
	}

	state := newRunState(ctx, runID, wf, input, orkerQueue)
	// RUN_CREATED is emitted here, by the run itself, and not by whichever API
	// process accepted the submission. It is event 1 of every run that reaches
	// Temporal, so a replica rebuilding the stream from history starts at 1 with
	// no gap — which the API-published version could never give it, because that
	// copy only ever existed in one process's memory.
	if err := state.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, "",
		orkstackv1.Status_STATUS_PENDING,
		fmt.Sprintf("run created for orkflow %s@%s", wf.GetName(), wf.GetVersion())); err != nil {
		return nil, err
	}
	state.start(ctx)
	if err := state.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, "",
		orkstackv1.Status_STATUS_RUNNING, "run started"); err != nil {
		return nil, err
	}

	inFlight := map[string]workflow.Future{}
	for {
		// Schedule every task whose own dependencies are terminal. Re-scan after
		// each completion so a fast branch can advance without waiting for an
		// unrelated slow branch.
		for _, id := range state.readyTasks() {
			run, err := state.classify(ctx, id)
			if err != nil {
				return state.fail(ctx, id, err)
			}
			if !run {
				continue
			}
			future, err := state.schedule(ctx, id)
			if err != nil {
				return state.fail(ctx, id, err)
			}
			inFlight[id] = future
		}

		if len(inFlight) == 0 {
			if len(state.readyTasks()) == 0 {
				break
			}
			// The prior scan only propagated skips; scan again for their newly
			// ready dependants without waiting for an activity.
			continue
		}

		// Register futures in declaration order. Future readiness comes from
		// recorded Temporal history, while fixed registration order stabilizes
		// the case where several futures are ready in one workflow task.
		selector := workflow.NewSelector(ctx)
		selectedID := ""
		for _, id := range state.order {
			future, ok := inFlight[id]
			if !ok {
				continue
			}
			taskID := id
			selector.AddFuture(future, func(workflow.Future) {
				selectedID = taskID
			})
		}
		selector.Select(ctx)
		future := inFlight[selectedID]
		delete(inFlight, selectedID)
		if err := state.collect(ctx, selectedID, future); err != nil {
			return state.fail(ctx, selectedID, err)
		}
	}

	return state.succeed(ctx)
}

// runState holds the deterministic in-workflow state of one run.
type runState struct {
	runID string
	wf    *orkstackv1.Orkflow
	// orkerQueue is the queue a task that names no pool dispatches to. It
	// comes from the workflow input, never from the environment.
	orkerQueue string
	input      *orkstackv1.Payload
	inputs     map[string]any
	order      []string
	tasks      map[string]*orkstackv1.Task
	taskRuns   map[string]*orkstackv1.TaskRun
	results    map[string]task.UpstreamResult
	closure    map[string][]string
	leaves     map[string]struct{}

	createdAt *timestamppb.Timestamp
	startedAt *timestamppb.Timestamp
	status    orkstackv1.Status
	runError  *orkstackv1.Error
	finished  *timestamppb.Timestamp
	output    *orkstackv1.Payload
	eventSeq  int
}

func newRunState(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	orkerQueue string,
) *runState {
	mergedInput := ApplyInputDefaults(wf, input)
	state := &runState{
		runID:      runID,
		wf:         wf,
		orkerQueue: orkerQueue,
		input:      mergedInput,
		inputs:     map[string]any(payloadToPlugin(mergedInput)),
		tasks:      map[string]*orkstackv1.Task{},
		taskRuns:   map[string]*orkstackv1.TaskRun{},
		results:    map[string]task.UpstreamResult{},
		closure:    map[string][]string{},
		leaves:     map[string]struct{}{},
		// CreatedAt is the execution's own start time, taken from the
		// WorkflowExecutionStarted history event rather than from a clock, so
		// every replica that reads this run dates it identically and a replay
		// reproduces it exactly. StartedAt stays unset until the run actually
		// starts, one event later.
		createdAt: timestamppb.New(workflow.GetInfo(ctx).WorkflowStartTime),
		status:    orkstackv1.Status_STATUS_PENDING,
	}
	for _, t := range wf.GetTasks() {
		state.order = append(state.order, t.GetId())
		state.tasks[t.GetId()] = t
		state.taskRuns[t.GetId()] = &orkstackv1.TaskRun{
			TaskId: t.GetId(),
			Plugin: t.GetPlugin(),
			Status: orkstackv1.Status_STATUS_PENDING,
		}
		state.leaves[t.GetId()] = struct{}{}
	}
	for _, t := range wf.GetTasks() {
		for _, need := range t.GetNeeds() {
			delete(state.leaves, need)
		}
	}
	for _, id := range state.order {
		state.closure[id] = state.transitiveNeeds(id)
	}
	return state
}

// start moves the run from created to running. It is separate from
// newRunState so the RUN_CREATED event and the snapshot published with it
// describe a run that has not started yet.
func (s *runState) start(ctx workflow.Context) {
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.startedAt = timestamppb.New(workflow.Now(ctx))
}

// transitiveNeeds returns the sorted transitive `needs` closure of id. The DSL
// permits referencing any transitively upstream task, so the whole closure is
// threaded into the downstream activity input.
func (s *runState) transitiveNeeds(id string) []string {
	collected := map[string]struct{}{}
	var walk func(string)
	walk = func(current string) {
		for _, need := range s.tasks[current].GetNeeds() {
			if _, done := collected[need]; done {
				continue
			}
			collected[need] = struct{}{}
			if _, ok := s.tasks[need]; ok {
				walk(need)
			}
		}
	}
	walk(id)
	out := make([]string, 0, len(collected))
	for need := range collected {
		out = append(out, need)
	}
	sort.Strings(out)
	return out
}

// readyTasks returns pending tasks whose dependencies are all terminal, in
// declaration order.
func (s *runState) readyTasks() []string {
	var ready []string
	for _, id := range s.order {
		if s.taskRuns[id].GetStatus() != orkstackv1.Status_STATUS_PENDING {
			continue
		}
		blocked := false
		for _, need := range s.tasks[id].GetNeeds() {
			if !isTerminal(s.taskRuns[need].GetStatus()) {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, id)
		}
	}
	return ready
}

func isTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

// classify decides whether a ready task runs or is skipped.
func (s *runState) classify(ctx workflow.Context, id string) (bool, error) {
	for _, need := range s.tasks[id].GetNeeds() {
		if s.taskRuns[need].GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
			return false, s.markSkipped(ctx, id, fmt.Sprintf("upstream task %q was skipped", need))
		}
	}

	shouldRun, err := evaluateCondition(s.tasks[id].GetCondition(), s.inputs, s.upstreamFor(id))
	if err != nil {
		return false, err
	}
	if !shouldRun {
		return false, s.markSkipped(ctx, id,
			fmt.Sprintf("condition %q evaluated false", s.tasks[id].GetCondition()))
	}
	return true, nil
}

// markSkipped records a skip. The Event contract has no task-skipped type, so
// a skip is reported as a task log event carrying STATUS_SKIPPED; the
// authoritative record is TaskRun.Status.
func (s *runState) markSkipped(ctx workflow.Context, id, reason string) error {
	now := timestamppb.New(workflow.Now(ctx))
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_SKIPPED
	taskRun.StartedAt = now
	taskRun.FinishedAt = now
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id,
		orkstackv1.Status_STATUS_SKIPPED, "task skipped: "+reason)
}

// upstreamFor returns the results of every completed task in id's transitive
// needs closure.
func (s *runState) upstreamFor(id string) map[string]task.UpstreamResult {
	upstream := make(map[string]task.UpstreamResult, len(s.closure[id]))
	for _, need := range s.closure[id] {
		if result, ok := s.results[need]; ok {
			upstream[need] = result
		}
	}
	return upstream
}

// schedule emits the scheduled event and starts the ExecuteTask activity.
func (s *runState) schedule(ctx workflow.Context, id string) (workflow.Future, error) {
	t := s.tasks[id]
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_RUNNING
	taskRun.StartedAt = timestamppb.New(workflow.Now(ctx))

	// Two words for one decision: the queue is how the engine dispatches, the
	// pool is what the author wrote. Both come out of routeForTask, so
	// everything below this line that a client can read says pool, and that
	// pool names the Orkers the activity really goes to.
	queue, err := s.queueForTask(t)
	if err != nil {
		return nil, err
	}
	pool := s.poolForTask(t)
	taskRun.Pool = pool

	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, "task scheduled on pool "+pool); err != nil {
		return nil, err
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, "task dispatched to an orker"); err != nil {
		return nil, err
	}

	timeout := time.Duration(timeoutSecondsFor(t)) * time.Second
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: timeout,
		RetryPolicy:         retryPolicyFor(t),
	})

	input := task.ExecuteTaskInput{
		RunID:          s.runID,
		OrkflowName:    s.wf.GetName(),
		OrkflowVersion: s.wf.GetVersion(),
		Namespace:      s.wf.GetNamespace(),
		Pool:           pool,
		TaskID:         id,
		Plugin:         t.GetPlugin(),
		With:           t.GetWith().AsMap(),
		RunInput:       payloadToPlugin(s.input),
		Upstream:       s.upstreamFor(id),
		TimeoutSeconds: timeoutSecondsFor(t),
	}
	return workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input), nil
}

// collect awaits one task future and records its outcome.
func (s *runState) collect(ctx workflow.Context, id string, future workflow.Future) error {
	taskRun := s.taskRuns[id]

	var result task.ExecuteTaskResult
	if err := future.Get(ctx, &result); err != nil {
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
		taskRun.Error = &orkstackv1.Error{
			Code:    "TASK_FAILED",
			Message: err.Error(),
			Details: fmt.Sprintf("task %q (plugin %q)", id, s.tasks[id].GetPlugin()),
		}
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, err.Error()); emitErr != nil {
			return emitErr
		}
		return err
	}

	output, convErr := payloadFromPlugin(result.Output)
	if convErr != nil {
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
		taskRun.Error = &orkstackv1.Error{Code: "TASK_OUTPUT_INVALID", Message: convErr.Error()}
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, convErr.Error()); emitErr != nil {
			return emitErr
		}
		return convErr
	}

	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Output = output
	taskRun.Artifacts = artifactsFromPlugin(result.Artifacts)
	s.results[id] = task.UpstreamResult{Output: result.Output, Artifacts: result.Artifacts}

	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task succeeded")
}

// fail terminates the run after a task failure or cancellation.
func (s *runState) fail(ctx workflow.Context, taskID string, cause error) (*orkstackv1.Run, error) {
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError = &orkstackv1.Error{
		Code:    "RUN_FAILED",
		Message: cause.Error(),
		Details: fmt.Sprintf("task %q", taskID),
	}

	eventCtx := ctx
	eventType := orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	s.status = orkstackv1.Status_STATUS_FAILED
	if temporal.IsCanceledError(cause) || ctx.Err() != nil {
		// The workflow context is done; final projection must run on a
		// disconnected context or it would be canceled too.
		disconnected, cancelDisconnected := workflow.NewDisconnectedContext(ctx)
		defer cancelDisconnected()
		eventCtx = disconnected
		eventType = orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
		s.status = orkstackv1.Status_STATUS_CANCELED
		s.runError.Code = "RUN_CANCELED"
		if taskRun, ok := s.taskRuns[taskID]; ok {
			taskRun.Status = orkstackv1.Status_STATUS_CANCELED
		}
	}

	if err := s.emit(eventCtx, eventType, taskID, s.status, s.runError.GetMessage()); err != nil {
		return s.snapshot(), err
	}
	return s.snapshot(), cause
}

// succeed terminates the run successfully, aggregating leaf task outputs.
func (s *runState) succeed(ctx workflow.Context) (*orkstackv1.Run, error) {
	aggregated := map[string]any{}
	for _, id := range s.order {
		if _, leaf := s.leaves[id]; !leaf {
			continue
		}
		if result, ok := s.results[id]; ok {
			aggregated[id] = map[string]any(result.Output)
		}
	}
	output, err := payloadFromPlugin(aggregated)
	if err != nil {
		return nil, err
	}

	s.output = output
	s.status = orkstackv1.Status_STATUS_SUCCEEDED
	s.finished = timestamppb.New(workflow.Now(ctx))
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "",
		orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded"); err != nil {
		return nil, err
	}
	return s.snapshot(), nil
}

// snapshot materializes the current Run read model.
//
// It carries CreatedAt because this snapshot is the run as every replica sees
// it: Temporal records it alongside each event, and a replica that never saw
// the submission has no other source for the field.
func (s *runState) snapshot() *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:             s.runID,
		Namespace:      s.wf.GetNamespace(),
		OrkflowName:    s.wf.GetName(),
		OrkflowVersion: s.wf.GetVersion(),
		Status:         s.status,
		Input:          s.input,
		Output:         s.output,
		Error:          s.runError,
		CreatedAt:      s.createdAt,
		StartedAt:      s.startedAt,
		FinishedAt:     s.finished,
	}
	for _, id := range s.order {
		run.TaskRuns = append(run.TaskRuns, s.taskRuns[id])
	}
	return run
}

// emit publishes one lifecycle event through the projection activity.
//
// The workflow numbers the event itself, from a counter that is part of its
// deterministic state. That number is what Temporal records when the activity
// is *scheduled*, so it is on the copy every replica can read, and it is the
// same number the bus stores and the archive keeps. The dedupe key is derived
// from the same counter, so activity retries cannot duplicate an event.
func (s *runState) emit(
	ctx workflow.Context,
	eventType orkstackv1.EventType,
	taskID string,
	status orkstackv1.Status,
	message string,
) error {
	s.eventSeq++
	event := &orkstackv1.Event{
		RunId:     s.runID,
		Sequence:  int64(s.eventSeq),
		Type:      eventType,
		TaskId:    taskID,
		Status:    status,
		Message:   message,
		Timestamp: timestamppb.New(workflow.Now(ctx)),
	}
	dedupeKey := fmt.Sprintf("%s#%d", s.runID, s.eventSeq)

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: eventActivityTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	future := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, s.snapshot(), dedupeKey)
	if err := future.Get(ctx, nil); err != nil {
		// Projection is observability, not orchestration: a failed publish is
		// logged and the run continues.
		workflow.GetLogger(ctx).Error("publish event failed",
			"run_id", s.runID, "event", eventType.String(), "error", err)
	}
	return nil
}

// route is where one task runs: the pool its author asked for, and the
// Temporal task queue that pool's Orkers poll on this run. The two travel
// together because they are one decision. Computing them separately is how
// `pool: main` came to dispatch to task.DefaultQueue while a task that named
// no pool dispatched to the run's own orkerQueue — two disjoint sets of
// Orkers under one reported pool, and, under the ORK_ORKER_QUEUE isolation
// AGENTS.md §6 gives every parallel agent, an escape from that isolation onto
// the shared queue.
type route struct {
	pool  string
	queue string
}

// defaultRoute is where a task that names no pool runs, and the pool that
// route is called.
//
// The queue is the one TemporalStarter.StartRun bound into the workflow input,
// because workflow code is deterministic and may not read process
// configuration itself. The pool is that queue's own name in the user's
// vocabulary: task.PoolForQueue answers "main" for the default queue, names
// the pool when an operator pointed the default route at one, and declines for
// a raw isolation queue (ORK_ORKER_QUEUE=ork-tasks-m7-9), which is nobody's
// pool. Declining resolves to the built-in default, which is the pool a
// document that named none asked for.
func (s *runState) defaultRoute() route {
	queue := s.orkerQueue
	if queue == "" {
		queue = task.DefaultQueue
	}
	pool, ok := task.PoolForQueue(queue)
	if !ok {
		pool = task.DefaultPool
	}
	return route{pool: pool, queue: queue}
}

// routeForTask resolves one task to its route.
//
// A task that names the pool this run's default route already serves takes
// that route — naming a pool explicitly must reach the same Orkers as leaving
// it out, which is what docs/DSL.md promises ("a task with no `pool:` runs on
// the built-in `main` pool") and what TaskRun.pool means when a client
// resubmits against it. Any other pool maps through the one translation in
// internal/task.
//
// A pool name that cannot be mapped fails the run rather than falling back to
// a queue nobody asked for: silently running a gpu task on the shared pool is
// worse than not running it.
//
// Pure function of the workflow input, so it holds the determinism boundary
// and a replay dispatches and reports exactly what the original run did.
func (s *runState) routeForTask(t *orkstackv1.Task) (route, error) {
	def := s.defaultRoute()
	pool := t.GetPool()
	if pool == "" || pool == def.pool {
		return def, nil
	}
	queue, err := task.QueueForPool(pool)
	if err != nil {
		return route{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: %v", t.GetId(), err), "INVALID_POOL", err)
	}
	return route{pool: pool, queue: queue}, nil
}

// queueForTask is the queue a task's activity is scheduled on.
func (s *runState) queueForTask(t *orkstackv1.Task) (string, error) {
	r, err := s.routeForTask(t)
	return r.queue, err
}

// poolForTask is where a task ran, in the word its author typed. It is what
// every client-facing surface reports: the TASK_SCHEDULED message `orkctl
// watch` prints and the run page renders, TaskRun.pool on the projection, and
// the coordinate the Orker keys its output by. It is the same decision as
// queueForTask, so the two can never name different sets of Orkers.
//
// An unmappable pool reports the word the author typed; the run fails on it
// through queueForTask before any client reads this.
func (s *runState) poolForTask(t *orkstackv1.Task) string {
	r, err := s.routeForTask(t)
	if err != nil {
		return t.GetPool()
	}
	return r.pool
}

func timeoutSecondsFor(t *orkstackv1.Task) int {
	if t.GetTimeoutSeconds() > 0 {
		return int(t.GetTimeoutSeconds())
	}
	return 300
}

// retryPolicyFor converts the DSL retry policy to Temporal's. A missing policy
// means a single attempt: leaving it nil would inherit Temporal's default of
// unlimited retries, which contradicts the proto contract ("0 or 1 = no
// retries").
func retryPolicyFor(t *orkstackv1.Task) *temporal.RetryPolicy {
	retry := t.GetRetry()
	if retry == nil || retry.GetMaximumAttempts() <= 1 {
		return &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	policy := &temporal.RetryPolicy{
		MaximumAttempts:    retry.GetMaximumAttempts(),
		InitialInterval:    time.Duration(retry.GetInitialIntervalSeconds()) * time.Second,
		BackoffCoefficient: retry.GetBackoffCoefficient(),
		MaximumInterval:    time.Duration(retry.GetMaximumIntervalSeconds()) * time.Second,
	}
	if policy.InitialInterval <= 0 {
		policy.InitialInterval = time.Second
	}
	if policy.BackoffCoefficient <= 0 {
		policy.BackoffCoefficient = 2.0
	}
	if policy.MaximumInterval <= 0 {
		policy.MaximumInterval = 100 * time.Second
	}
	return policy
}
