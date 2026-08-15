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
	// WorkflowName is the registered Temporal workflow type for a run.
	WorkflowName = "RunWorkflow"

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
// Orker queue, threads upstream outputs/artifacts into downstream tasks, and
// emits lifecycle events through the projection activity.
//
// Every side effect (event publication, read-model updates) happens in an
// activity; the workflow body uses only workflow.Now for time, ordered
// iteration, and activity futures.
//
// Arguments are passed separately rather than in one struct because the
// Temporal data converter encodes top-level protobuf messages with protojson;
// nesting them in a plain Go struct would fall back to encoding/json, which
// cannot round-trip google.protobuf.Struct.
func RunWorkflow(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, temporal.NewNonRetryableApplicationError("run id is required", "INVALID_RUN", nil)
	}
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, temporal.NewNonRetryableApplicationError("workflow has no tasks", "INVALID_WORKFLOW", nil)
	}

	state := newRunState(ctx, runID, wf, input)
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
	runID    string
	wf       *orkstackv1.Orkflow
	input    *orkstackv1.Payload
	inputs   map[string]any
	order    []string
	tasks    map[string]*orkstackv1.Task
	taskRuns map[string]*orkstackv1.TaskRun
	results  map[string]task.UpstreamResult
	closure  map[string][]string
	leaves   map[string]struct{}

	startedAt *timestamppb.Timestamp
	status    orkstackv1.Status
	runError  *orkstackv1.Error
	finished  *timestamppb.Timestamp
	output    *orkstackv1.Payload
	eventSeq  int
}

func newRunState(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload) *runState {
	mergedInput := ApplyInputDefaults(wf, input)
	state := &runState{
		runID:     runID,
		wf:        wf,
		input:     mergedInput,
		inputs:    map[string]any(payloadToPlugin(mergedInput)),
		tasks:     map[string]*orkstackv1.Task{},
		taskRuns:  map[string]*orkstackv1.TaskRun{},
		results:   map[string]task.UpstreamResult{},
		closure:   map[string][]string{},
		leaves:    map[string]struct{}{},
		startedAt: timestamppb.New(workflow.Now(ctx)),
		status:    orkstackv1.Status_STATUS_RUNNING,
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

	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, "task scheduled on queue "+queueFor(t)); err != nil {
		return nil, err
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, "task dispatched to an orker"); err != nil {
		return nil, err
	}

	timeout := time.Duration(timeoutSecondsFor(t)) * time.Second
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queueFor(t),
		StartToCloseTimeout: timeout,
		RetryPolicy:         retryPolicyFor(t),
	})

	input := task.ExecuteTaskInput{
		RunID:           s.runID,
		WorkflowName:    s.wf.GetName(),
		WorkflowVersion: s.wf.GetVersion(),
		TaskID:          id,
		Plugin:          t.GetPlugin(),
		With:            t.GetWith().AsMap(),
		RunInput:        payloadToPlugin(s.input),
		Upstream:        s.upstreamFor(id),
		TimeoutSeconds:  timeoutSecondsFor(t),
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
func (s *runState) snapshot() *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:              s.runID,
		WorkflowName:    s.wf.GetName(),
		WorkflowVersion: s.wf.GetVersion(),
		Status:          s.status,
		Input:           s.input,
		Output:          s.output,
		Error:           s.runError,
		StartedAt:       s.startedAt,
		FinishedAt:      s.finished,
	}
	for _, id := range s.order {
		run.TaskRuns = append(run.TaskRuns, s.taskRuns[id])
	}
	return run
}

// emit publishes one lifecycle event through the projection activity. The
// dedupe key is derived from the workflow-local event counter, so activity
// retries cannot duplicate an event.
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

func queueFor(t *orkstackv1.Task) string {
	if t.GetQueue() != "" {
		return t.GetQueue()
	}
	return task.DefaultQueue
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
