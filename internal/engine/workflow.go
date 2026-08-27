package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// orkerHeartbeatTimeout is what the engine sets in ActivityOptions, and the
// number the Orker's heartbeat tick rate is derived from.
//
// The two halves must agree or cancellation silently stops working: Temporal
// delivers cancellation to an activity ONLY through a heartbeat response, so
// an activity that never calls RecordHeartbeat never observes ctx.Done().
// Before this, no HeartbeatTimeout was set anywhere in the tree and the Orker
// heartbeated only between phases, which meant cancellation could not reach a
// running container at all — the largest correctness gap in the system (§6.3).
const orkerHeartbeatTimeout = 30 * time.Second

const (
	// RunWorkflowType is the registered Temporal workflow type for a run. It
	// names a Temporal concept, not an OrkStack one: the OrkStack entity a run
	// executes is an Orkflow, and this is the workflow TYPE that runs it.
	RunWorkflowType = "RunWorkflow"

	// EventQueue is the Temporal task queue for engine-internal projection
	// activities. It is deliberately separate from task.DefaultQueue so Orkers
	// polling for task execution never receive engine projection work, and
	// carries the ork- prefix every OrkStack Temporal name uses. The literal
	// lives in internal/task because the two places that must refuse to run
	// tasks on it — task.QueueForPool and `ork` at boot — cannot import this
	// package.
	EventQueue = task.EngineEventQueue

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
// plan is this run's routing, resolved at submit by TemporalStarter.StartRun
// and bound into the input here. It is an argument, not a lookup, for two
// reasons that both matter: workflow code is deterministic and may not read
// process configuration or a namespace document, and the orkflow message must
// not carry a channel name — that is Temporal's word, the definition is the
// user's document, and the two stopped touching at M10.1. Binding the plan
// records the routing in history, so every replay dispatches where the
// original run did and a second engine finishes a run this one started without
// redirecting its tasks onto its own defaults.
func RunWorkflow(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan RunPlan,
) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, temporal.NewNonRetryableApplicationError("run id is required", "INVALID_RUN", nil)
	}
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, temporal.NewNonRetryableApplicationError("orkflow has no tasks", "INVALID_ORKFLOW", nil)
	}

	state := newRunState(ctx, runID, wf, input, plan)
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
	// plan is this run's routing, bound into the workflow input at submit. It
	// is the only source of a channel name here: never the environment, never
	// a namespace document, never a lookup.
	plan     RunPlan
	input    *orkstackv1.Payload
	inputs   map[string]any
	order    []string
	tasks    map[string]*orkstackv1.Task
	taskRuns map[string]*orkstackv1.TaskRun
	// results is each completed task's ExecuteTaskResult, which is what an
	// UpstreamRef is projected from. It holds REFERENCES, not bytes.
	results map[string]*orkstackv1.ExecuteTaskResult
	// outputs is each completed task's decoded values.json, for condition
	// evaluation and for the run's aggregated output. It exists separately
	// from results because a condition needs a map and an upstream ref needs
	// a reference, and conflating them is how bytes end up on the wire.
	outputs map[string]map[string]any
	closure map[string][]string
	leaves  map[string]struct{}

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
	plan RunPlan,
) *runState {
	mergedInput := ApplyInputDefaults(wf, input)
	state := &runState{
		runID:    runID,
		wf:       wf,
		plan:     plan,
		input:    mergedInput,
		inputs:   payloadMap(mergedInput),
		tasks:    map[string]*orkstackv1.Task{},
		taskRuns: map[string]*orkstackv1.TaskRun{},
		results:  map[string]*orkstackv1.ExecuteTaskResult{},
		outputs:  map[string]map[string]any{},
		closure:  map[string][]string{},
		leaves:   map[string]struct{}{},
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

	shouldRun, err := evaluateCondition(s.tasks[id].GetCondition(), s.inputs, s.upstreamOutputs(id))
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
// upstreamFor returns one UpstreamRef per completed task in id's transitive
// needs closure.
//
// It carries REFERENCES ONLY. Fan-in is O(ancestors) small records, never a
// transitive copy of every ancestor's payload: the Orker fetches each
// ancestor's values.json and artifacts from the store, which is where bytes
// live (OQ-2, AGENTS.md §2.2).
func (s *runState) upstreamFor(id string) []*orkstackv1.UpstreamRef {
	refs := make([]*orkstackv1.UpstreamRef, 0, len(s.closure[id]))
	for _, need := range s.closure[id] {
		result, ok := s.results[need]
		if !ok {
			continue
		}
		refs = append(refs, &orkstackv1.UpstreamRef{
			TaskId:    need,
			CommitUri: result.GetCommit().GetUri(),
			Values:    result.GetValuesRef(),
			Artifacts: result.GetArtifacts(),
		})
	}
	return refs
}

// upstreamOutputs is the condition-evaluation view of the same closure: each
// completed ancestor's decoded values.json.
func (s *runState) upstreamOutputs(id string) map[string]map[string]any {
	out := make(map[string]map[string]any, len(s.closure[id]))
	for _, need := range s.closure[id] {
		if values, ok := s.outputs[need]; ok {
			out[need] = values
		}
	}
	return out
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

	// The three timeouts must not be confused, and this is where the
	// confusion would happen (§6.3):
	//
	//   timeout_seconds       the TASK's wall clock. The ORKER enforces it,
	//                         kills the container, and returns
	//                         TimedOut/CONTAINER_TIMEOUT. It fires first.
	//   HeartbeatTimeout      Temporal's dead-worker detector. Without it the
	//                         SDK throttles heartbeats and a dead Orker is
	//                         only noticed at StartToClose.
	//   StartToCloseTimeout   the backstop for an Orker wedged OUTSIDE the
	//                         container run. It has to be generous, because
	//                         inside it sit input staging, the image pull,
	//                         hashing, and uploading up to
	//                         artifact_max_count x artifact_max_bytes.
	//
	// A fixed transfer budget was the earlier draft and is not survivable:
	// retryPolicyFor returns MaximumAttempts: 1 whenever `retry:` is absent,
	// so a legal large upload would be terminal on its first timeout with no
	// retry at all. The budget is therefore resolved at submit, where process
	// configuration is readable, and carried on the plan — the workflow only
	// adds it up, which keeps this a pure function of its arguments.
	taskTimeout := time.Duration(timeoutSecondsFor(t)) * time.Second
	grace := time.Duration(graceSecondsFor(t)) * time.Second
	startToClose := taskTimeout + grace + time.Duration(s.plan.TransferBudgetSeconds)*time.Second

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: startToClose,
		// Temporal delivers cancellation to an activity ONLY through a
		// heartbeat response. Without this timeout the SDK throttles
		// heartbeats to a 30 s default and cancellation cannot reach a
		// running container at all.
		HeartbeatTimeout: orkerHeartbeatTimeout,
		// Without this the workflow may mark the task terminal before the
		// Orker has finished killing the container and cleaning up.
		WaitForCancellation: true,
		RetryPolicy:         retryPolicyFor(t),
	})

	commitURI, err := s.commitURIFor(id)
	if err != nil {
		return nil, err
	}
	namespace := s.wf.GetNamespace()
	input := &orkstackv1.ExecuteTaskInput{
		RunId:          s.runID,
		OrkflowName:    s.wf.GetName(),
		OrkflowVersion: s.wf.GetVersion(),
		// READ from the version document, never computed here (§4.4). The
		// digest is stamped at APPLY, after image resolution, so it describes
		// the definition this run actually pinned. Computing it in workflow
		// code would be deterministic but would describe whatever the reader
		// reconstructed, and would record a digest for a definition that was
		// never resolved at all.
		OrkflowDefinitionDigest: s.wf.GetDefinitionDigest(),
		Namespace:               namespace,
		Pool:                    pool,
		TaskId:                  id,

		IdempotencyKey: task.IdempotencyKeyFor(namespace, s.runID, id),
		CommitUri:      commitURI,

		// The image is the APPLY-TIME resolution stamped into the immutable
		// version document. The engine never resolves a tag here: workflow
		// code may not reach a registry, and a run must execute what its
		// definition pinned even if the tag has since moved (§4.1).
		Image:   t.GetImage(),
		Command: t.GetCommand(),
		// RAW, un-rendered. Rendering stays in the Orker because
		// `artifact "<task>/<name>"` resolves to a container path only the
		// Orker knows after staging (OQ-1).
		With:    t.GetWith(),
		Env:     t.GetEnv(),
		Secrets: t.GetSecrets(),

		TimeoutSeconds:                 int32(timeoutSecondsFor(t)),
		CancellationGracePeriodSeconds: graceSecondsFor(t),
		Resources:                      t.GetResources(),
		Network:                        t.GetNetwork(),
		Limits:                         s.plan.Limits.Proto(),

		RunInput: s.input,
		Upstream: s.upstreamFor(id),

		RunAsOrkerUser: t.GetRunAsOrkerUser(),
	}
	// The wire bound is checked HERE, deterministically, before scheduling.
	// The deployment's limit is replay-pinned on the plan; the checker also
	// preserves Temporal's absolute 256 KiB ceiling.
	if err := task.CheckInputSize(input, s.plan.Limits.Proto().GetValuesMaxBytes()); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(), task.CategoryContractViolation, err)
	}
	return workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input), nil
}

// commitURIFor composes one activity's commit URI.
//
// It is plan.CommitRoot + "/" + artifacts.CommitKey(...), which is a pure
// function of workflow arguments and replays identically. artifacts.CommitKey
// is the single shared renderer — the Orker MUST use the value it was sent
// rather than re-deriving one, so there is exactly one convention with exactly
// one owner (§5.2).
//
// An empty CommitRoot yields an empty URI rather than an error: a plan built
// before the commit protocol existed, or by a test harness, still runs, and
// the Orker simply has no commit to check or write.
func (s *runState) commitURIFor(taskID string) (string, error) {
	if s.plan.CommitRoot == "" {
		return "", nil
	}
	key, err := artifacts.CommitKey(s.wf.GetNamespace(), s.runID, taskID)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: compose the commit uri: %v", taskID, err), "INVALID_COMMIT_KEY", err)
	}
	return strings.TrimSuffix(s.plan.CommitRoot, "/") + "/" + key, nil
}

// collect awaits one task future and records its outcome.
func (s *runState) collect(ctx workflow.Context, id string, future workflow.Future) error {
	taskRun := s.taskRuns[id]

	result := &orkstackv1.ExecuteTaskResult{}
	if err := future.Get(ctx, result); err != nil {
		s.recordFailure(ctx, id, err)
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, err.Error()); emitErr != nil {
			return emitErr
		}
		return err
	}

	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Artifacts = result.GetArtifacts()
	taskRun.SystemArtifacts = result.GetSystemArtifacts()
	taskRun.ValuesRef = result.GetValuesRef()
	taskRun.Commit = result.GetCommit()
	taskRun.ExitCode = result.GetExitCode()
	if result.GetAttempt() > 0 {
		taskRun.Attempt = result.GetAttempt()
	}

	// TaskRun.output is the inline copy, decoded FOR DISPLAY ONLY. values_ref
	// is the authoritative reference, and a downstream task reads the
	// committed object rather than this.
	values := decodeValues(result.GetValuesJson())
	output, convErr := payloadOf(values)
	if convErr != nil {
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
		taskRun.Error = &orkstackv1.Error{
			Code:     "TASK_OUTPUT_INVALID",
			Message:  convErr.Error(),
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		}
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, convErr.Error()); emitErr != nil {
			return emitErr
		}
		return convErr
	}
	taskRun.Output = output
	s.results[id] = result
	s.outputs[id] = values

	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task succeeded")
}

// recordFailure reads what the Orker actually said and records it as DATA.
//
// The engine stops flattening everything into TASK_FAILED with a stringified
// error. It reads the ApplicationError type — which IS the §8.1 category — and
// the ActivityFailureDetail, and populates Error{code, message, details,
// retryable, category}. The event type stays TASK_FAILED; the category is data
// on the error, so the UI can say "contract violation — this image did not
// write values.json" instead of rendering a Go error string (§8.3).
//
// The failure detail is what keeps a failed attempt as inspectable as a
// successful one: without it, an exit-1 task's uploaded test report and
// captured stderr would be unreachable.
func (s *runState) recordFailure(ctx workflow.Context, id string, cause error) {
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))

	failure := &orkstackv1.Error{
		Code:    "TASK_FAILED",
		Message: cause.Error(),
		Details: fmt.Sprintf("task %q", id),
	}

	var applicationErr *temporal.ApplicationError
	if errors.As(cause, &applicationErr) {
		category := applicationErr.Type()
		failure.Category = categoryEnumFor(category)
		failure.Retryable = !applicationErr.NonRetryable()

		// The target is a **ActivityFailureDetail: Details assigns the value
		// it holds, which is a *ActivityFailureDetail both in-process and
		// after the proto converter decodes it from the wire.
		var detail *orkstackv1.ActivityFailureDetail
		if err := applicationErr.Details(&detail); err == nil && detail.GetReasonCode() != "" {
			failure.Code = detail.GetReasonCode()
			if detail.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
				failure.Category = detail.GetCategory()
			}
			taskRun.Artifacts = detail.GetArtifacts()
			taskRun.SystemArtifacts = detail.GetSystemArtifacts()
			taskRun.ExitCode = detail.GetExitCode()
			if detail.GetAttempt() > 0 {
				taskRun.Attempt = detail.GetAttempt()
			}
			if notes := detail.GetScanNotes(); len(notes) > 0 {
				// Scan notes are notes ON the workload's error, never a
				// replacement for it (§7.1 step 14).
				failure.Details = failure.Details + "; " + strings.Join(notes, "; ")
			}
		}
	}

	// A Temporal-produced timeout arrives as *TimeoutError rather than an
	// ApplicationError, so the category is read from the timeout TYPE. A
	// HEARTBEAT timeout is the one timeout that stays RETRYABLE regardless:
	// it means the Orker died, which is an infrastructure fact about the
	// host and not a statement about the workload (§8.1).
	var timeoutErr *temporal.TimeoutError
	if errors.As(cause, &timeoutErr) {
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
		failure.Code = "ACTIVITY_TIMEOUT"
		if timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT {
			failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
			failure.Code = "ORKER_HEARTBEAT_TIMEOUT"
			failure.Retryable = true
		}
	}
	if temporal.IsCanceledError(cause) {
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
		failure.Code = "TASK_CANCELED"
		failure.Retryable = false
	}
	taskRun.Error = failure
}

// categoryEnumFor maps an ApplicationError type string to the proto enum.
// The category names ARE the type strings, which is what makes
// NonRetryableErrorTypes work at all (§8.1).
func categoryEnumFor(category string) orkstackv1.FailureCategory {
	switch category {
	case task.CategoryActivityFailed:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	case task.CategoryInfrastructureError:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	case task.CategoryContractViolation:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	case task.CategoryTimedOut:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	case task.CategoryCancelled:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
	default:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
	}
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
		if values, ok := s.outputs[id]; ok {
			aggregated[id] = values
		}
	}
	output, err := payloadOf(aggregated)
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
		// A run that started before the current pointer moved is still
		// explainable by the exact bytes it ran (§4.4). Read from the version
		// document for the same reason ExecuteTaskInput reads it.
		OrkflowDefinitionDigest: s.wf.GetDefinitionDigest(),
		Status:                  s.status,
		Input:                   s.input,
		Output:                  s.output,
		Error:                   s.runError,
		CreatedAt:               s.createdAt,
		StartedAt:               s.startedAt,
		FinishedAt:              s.finished,
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

// routeForTask resolves one task to its route: the queue its activity is
// scheduled on, and the pool a client is told it ran on.
//
// It is a table lookup and nothing else. The plan bound into this run's input
// already answered where every pool goes: the resolution — the task's own
// pool, then its namespace's default, then the built-in "main", then that
// pool's channel through task.RouteForPool — happened at submit, in
// PoolResolver.Plan, which is the last place allowed to read a store, the
// environment or the cluster. Here there is only the recorded answer, so a
// replay dispatches exactly where the original run did and a second engine
// finishing this run cannot redirect it onto its own defaults.
//
// Both halves come out of that ONE lookup, which is what stops them naming
// different sets of Orkers: the pool is the key and the queue is what the key
// maps to, so there is no second question to answer differently. The same
// property is held at submit by task.RouteForPool, which collapses a pool
// whose queue IS the deployment's bound channel onto that channel's own name —
// a client is never told `main` while the work runs on gpu's Orkers.
//
// A pool with no route fails the run rather than falling back to a channel
// nobody asked for: silently running a gpu task on the shared pool is worse
// than not running it. So does a plan that routes a pool onto one of the
// engine's own queues, which `ork` refuses at boot and the resolver refuses at
// submit, and which reaches here only from a harness that wired a plan by
// hand. Both are unreachable through SubmitRun, because the plan is built from
// this very definition; reached by hand, they say so.
//
// Pure function of the workflow input, so it holds the determinism boundary
// and a replay dispatches and reports exactly what the original run did.
func (s *runState) routeForTask(t *orkstackv1.Task) (task.Route, error) {
	pool := t.GetPool()
	if pool == "" {
		// A definition that reached this workflow without passing through the
		// resolver asks for the built-in default, which is the pool a document
		// that named none asked for. The plan either has a route for it or
		// refuses below; it is never invented here.
		pool = task.DefaultPool
	}
	queue, err := s.plan.Route(pool)
	if err != nil {
		reason := "INVALID_POOL"
		if errors.Is(err, task.ErrReservedQueue) {
			reason = "RESERVED_QUEUE"
		}
		return task.Route{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: %v", t.GetId(), err), reason, err)
	}
	return task.Route{Pool: pool, Queue: queue}, nil
}

// queueForTask is the queue a task's activity is scheduled on.
func (s *runState) queueForTask(t *orkstackv1.Task) (string, error) {
	r, err := s.routeForTask(t)
	return r.Queue, err
}

// poolForTask is where a task ran, in the word its author typed. It is what
// every client-facing surface reports: the TASK_SCHEDULED message `orkctl
// watch` prints and the run page renders, TaskRun.pool on the projection, and
// the coordinate the Orker keys its output by. It is the same lookup as
// queueForTask, so the two can never name different sets of Orkers.
//
// An unroutable pool reports the word the author typed; the run fails on it
// through queueForTask before any client reads this.
func (s *runState) poolForTask(t *orkstackv1.Task) string {
	r, err := s.routeForTask(t)
	if err != nil {
		return t.GetPool()
	}
	return r.Pool
}

// graceSecondsFor is the SIGTERM-to-SIGKILL window for one task. The default
// is AGENTS.md §3.3's 30 seconds; ordinary Unix signal handling is meant to be
// enough for cleanup inside it.
func graceSecondsFor(t *orkstackv1.Task) int32 {
	if seconds := t.GetCancellationGracePeriodSeconds(); seconds > 0 {
		return seconds
	}
	return 30
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
