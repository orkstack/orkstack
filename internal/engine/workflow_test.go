package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// harness wires a TestWorkflowEnvironment with a mocked ExecuteTask activity
// and a real (in-process) event projection activity so tests can assert on the
// resulting run read model and event stream.
type harness struct {
	env   *testsuite.TestWorkflowEnvironment
	bus   *engine.EventBus
	store *engine.RunStore

	mu    sync.Mutex
	calls []*orkstackv1.ExecuteTaskInput
	// queues records the task queue each task's activity actually arrived on,
	// read from Temporal's own activity info rather than from the engine. It
	// is what lets a test check the pool a client is TOLD against the Orkers
	// the task was dispatched to.
	queues map[string]string
	// activities is the type name of every activity the test environment
	// actually STARTED, in order. It is how a pin test observes Temporal's own
	// session activities — which the engine never names and cannot be mocked
	// away without changing what is under test — so "no session was ever
	// created" and "the pin closed before the run did" are both answerable.
	activities []string
	// released is every ReleasePin the engine scheduled.
	released []*orkstackv1.ReleasePinInput
	// values is the store CountEachItems reads a fan-out's task-sourced
	// array from: a test that wants one writes the bytes here and returns
	// their reference as the source task's values_ref.
	values *memValues
}

func newHarness(t *testing.T, suite *testsuite.WorkflowTestSuite, behavior func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *harness {
	t.Helper()
	h := &harness{
		env:    suite.NewTestWorkflowEnvironment(),
		bus:    engine.NewEventBus(),
		store:  engine.NewRunStore(),
		queues: map[string]string{},
		values: &memValues{objects: map[string][]byte{}},
	}
	h.env.SetTestTimeout(60 * time.Second)
	h.env.RegisterActivityWithOptions(engine.NewEachActivities(h.values).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	snapshots := newTestSnapshotStore()
	h.env.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	h.env.RegisterActivityWithOptions(
		engine.NewEventActivities(h.bus, h.store).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	h.env.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			h.mu.Lock()
			h.calls = append(h.calls, in)
			h.queues[in.TaskId] = activity.GetInfo(actCtx).TaskQueue
			h.mu.Unlock()
			return behavior(in)
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	t.Cleanup(func() { h.env.AssertExpectations(t) })
	return h
}

// sessionCreationActivityName and sessionCompletionActivityName are Temporal's
// own internal session activities, spelled here because a test observes them
// and the SDK does not export the names. They are the only place in the tree
// that names a session: the ENGINE never does, which is the §1 discipline
// (session is to pin what queue is to pool).
const (
	sessionCreationActivityName   = "internalSessionCreationActivity"
	sessionCompletionActivityName = "internalSessionCompletionActivity"
)

// sessionCreationResponse is the message a real pin host signals back to
// answer a creation request: the SDK's own struct is unexported, and the
// default data converter encodes both to the same JSON, so this stands in for
// it when a test plays the host itself.
type sessionCreationResponse struct {
	Taskqueue  string
	HostName   string
	ResourceID string
}

// newPinHarness is newHarness with Temporal's session worker enabled, the
// engine's ReleasePin activity registered, and every started activity
// recorded.
//
// EnableSessionWorker is what registers the SDK's internal creation and
// completion activities in the test environment, so a pinned run can be
// exercised end to end — session id and all — without a cluster.
func newPinHarness(
	t *testing.T,
	suite *testsuite.WorkflowTestSuite,
	behavior func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error),
	release func(*orkstackv1.ReleasePinInput) error,
) *harness {
	t.Helper()
	h := newHarness(t, suite, behavior)
	h.env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	h.env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ReleasePinInput) error {
			h.mu.Lock()
			h.released = append(h.released, in)
			h.mu.Unlock()
			if release == nil {
				return nil
			}
			return release(in)
		},
		activity.RegisterOptions{Name: task.ActivityReleasePin},
	)
	h.env.SetOnActivityStartedListener(
		func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
			h.mu.Lock()
			h.activities = append(h.activities, info.ActivityType.Name)
			h.mu.Unlock()
		})
	return h
}

// answerThenLosePinHost plays a pin host that answers the creation request —
// so the session really opens and the engine really schedules members onto
// it — and then dies.
//
// It is how a unit test reaches SessionStateFailed. Nothing in the SDK's
// public surface can fail a session from outside (its own tests reach the
// state through an unexported setter), but the session's health is decided by
// exactly one thing: whether its creation activity is still running. Signal
// the response, then end that activity with a non-retryable error, and the SDK
// takes the same path a dead Orker puts it on — marks the session FAILED and
// cancels every activity scheduled on it.
//
// gate, when non-nil, holds the death back until the test closes it.
func (h *harness) answerThenLosePinHost(gate <-chan struct{}, cause error) {
	h.env.OnActivity(sessionCreationActivityName, mock.Anything, mock.Anything).
		Return(func(_ context.Context, sessionID string) error {
			h.env.SignalWorkflow(sessionID, sessionCreationResponse{
				Taskqueue:  "test-session-queue",
				HostName:   "test-pin-host",
				ResourceID: "test-resource",
			})
			if gate != nil {
				<-gate
			}
			return cause
		})
}

// startedActivities is every activity type the environment started, in order.
func (h *harness) startedActivities() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.activities...)
}

func (h *harness) startedActivity(name string) bool {
	for _, started := range h.startedActivities() {
		if started == name {
			return true
		}
	}
	return false
}

// releases is every ReleasePin the engine asked for.
func (h *harness) releases() []*orkstackv1.ReleasePinInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*orkstackv1.ReleasePinInput(nil), h.released...)
}

func (h *harness) invocations() []*orkstackv1.ExecuteTaskInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*orkstackv1.ExecuteTaskInput(nil), h.calls...)
}

func (h *harness) taskOrder() []string {
	out := []string{}
	for _, call := range h.invocations() {
		out = append(out, call.TaskId)
	}
	return out
}

func (h *harness) inputFor(taskID string) (*orkstackv1.ExecuteTaskInput, bool) {
	for _, call := range h.invocations() {
		if call.TaskId == taskID {
			return call, true
		}
	}
	return (*orkstackv1.ExecuteTaskInput)(nil), false
}

// queueFor is the task queue taskID's activity arrived on.
func (h *harness) queueFor(taskID string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	queue, ok := h.queues[taskID]
	return queue, ok
}

func compile(t *testing.T, document string) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.Compile([]byte(document))
	if err != nil {
		t.Fatalf("compile test workflow: %v", err)
	}
	return wf
}

func payload(t *testing.T, values map[string]any) *orkstackv1.Payload {
	t.Helper()
	if len(values) == 0 {
		return &orkstackv1.Payload{}
	}
	data, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}
	return &orkstackv1.Payload{Data: data}
}

func statusOf(run *orkstackv1.Run, taskID string) orkstackv1.Status {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr.GetStatus()
		}
	}
	return orkstackv1.Status_STATUS_UNSPECIFIED
}

// taskRunOf is statusOf's sibling for the tests that read more of a TaskRun
// than its status — started_at above all, which is the only field that tells
// "this task ran" from "the run resolved it without ever dispatching it".
func taskRunOf(t *testing.T, run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	t.Helper()
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr
		}
	}
	t.Fatalf("run has no task run for %q", taskID)
	return nil
}

const linearDocument = `
name: linear
tasks:
  - id: alpha
    uses: shell
    with: {command: "echo alpha"}
  - id: beta
    uses: shell
    needs: [alpha]
    with: {command: "echo beta"}
  - id: gamma
    uses: shell
    needs: [beta]
    with: {command: "echo gamma"}
`

// The engine rejects an empty orkflow, and the run error names the entity the
// user authored — an orkflow, not a "workflow".
func TestRunWorkflow_RejectsAnOrkflowWithNoTasks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Fatalf("ExecuteTask called for %q, want no dispatch", in.TaskId)
		return &orkstackv1.ExecuteTaskResult{}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-1-1",
		&orkstackv1.Orkflow{Name: "empty", Version: "0.1.0"}, payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	err := h.env.GetWorkflowError()
	if err == nil {
		t.Fatal("error = nil, want the empty orkflow to be rejected")
	}
	if !strings.Contains(err.Error(), "orkflow has no tasks") {
		t.Fatalf("error = %q, want it to contain %q", err.Error(), "orkflow has no tasks")
	}
}

func TestRunWorkflow_ExecutesLinearNeedsInOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{
			TaskId:     in.TaskId,
			ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId}),
		}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-linear", compile(t, linearDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	got := h.taskOrder()
	want := []string{"alpha", "beta", "gamma"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("execution order = %v, want %v", got, want)
	}

	var run orkstackv1.Run
	if err := h.env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("run status = %v, want SUCCEEDED", run.GetStatus())
	}
	for _, id := range want {
		if statusOf(&run, id) != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Errorf("task %q status = %v, want SUCCEEDED", id, statusOf(&run, id))
		}
	}

	stored, ok := h.store.Get("run-linear")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("stored run status = %v, want SUCCEEDED", stored.GetStatus())
	}
	assertEventOrder(t, h.bus.Events("run-linear", 0))
}

func assertEventOrder(t *testing.T, events []*orkstackv1.Event) {
	t.Helper()
	if len(events) < 2 {
		t.Fatalf("published %d events, want at least RUN_CREATED then RUN_STARTED", len(events))
	}
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
		t.Errorf("first event = %v, want RUN_CREATED", events[0].GetType())
	}
	if events[1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		t.Errorf("second event = %v, want RUN_STARTED", events[1].GetType())
	}
	// 1..N with no gaps: the stream contract every replica has to be able to
	// reproduce from history.
	for i, event := range events {
		if got, want := event.GetSequence(), int64(i+1); got != want {
			t.Fatalf("event %d (%v) carries sequence %d, want %d",
				i, event.GetType(), got, want)
		}
	}
}

const fanOutDocument = `
name: fanout
tasks:
  - id: left
    uses: shell
    with: {command: "echo left"}
  - id: right
    uses: shell
    with: {command: "echo right"}
  - id: join
    uses: shell
    needs: [left, right]
    with: {command: "echo join"}
`

func TestRunWorkflow_FansOutIndependentTasks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite

	started := make(chan string, 2)
	// Each independent task blocks until the other has started. If the engine
	// serialized them, neither would ever be released and the barrier would
	// time out.
	barrier := func(in *orkstackv1.ExecuteTaskInput) error {
		if in.TaskId == "join" {
			return nil
		}
		started <- in.TaskId
		deadline := time.After(10 * time.Second)
		for {
			select {
			case <-deadline:
				return errors.New("independent tasks were not started concurrently")
			default:
			}
			if len(started) == 2 {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
	}

	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if err := barrier(in); err != nil {
			return &orkstackv1.ExecuteTaskResult{}, err
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-fanout", compile(t, fanOutDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	order := h.taskOrder()
	if len(order) != 3 {
		t.Fatalf("executed tasks = %v, want left, right and join", order)
	}
	if order[2] != "join" {
		t.Errorf("join ran at position %v, want last: %v", order[2], order)
	}
}

const upstreamDocument = `
name: upstream
tasks:
  - id: fetch
    uses: http
    with: {url: "https://example.test"}
  - id: transform
    uses: shell
    needs: [fetch]
    with:
      command: "cat {{ artifact \"fetch/body\" }}"
  - id: save
    uses: file
    needs: [transform]
    with:
      content: "{{ .tasks.fetch.output.status }}"
`

func TestRunWorkflow_PassesUpstreamOutputAndArtifactsDownstream(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "fetch":
			result := succeeded("fetch", map[string]any{"status": float64(200)})
			result.Artifacts = []*orkstackv1.ArtifactRef{{
				Name:        "body",
				Uri:         "s3://bucket/run/fetch/body",
				ContentType: "application/json",
				SizeBytes:   17,
				Sha256:      "abc123",
			}}
			return result, nil
		default:
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": "ok"})}, nil
		}
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-upstream", compile(t, upstreamDocument), payload(t, nil), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	transform, ok := h.inputFor("transform")
	if !ok {
		t.Fatal("transform task was never executed")
	}
	fetchRef := upstreamRef(transform, "fetch")
	if fetchRef == nil {
		t.Fatalf("transform upstream = %v, want a fetch entry", transform.GetUpstream())
	}
	// An UpstreamRef carries REFERENCES ONLY (OQ-2). The ancestor's
	// values.json is addressed, not copied: fan-in is O(ancestors) small
	// records rather than a transitive copy of every ancestor's payload, and
	// bytes never travel through Temporal (AGENTS.md §2.2).
	if fetchRef.GetValues().GetUri() == "" {
		t.Errorf("upstream fetch carries no values reference: %+v", fetchRef)
	}
	if len(fetchRef.GetArtifacts()) != 1 {
		t.Fatalf("upstream fetch artifacts = %v, want 1", fetchRef.GetArtifacts())
	}
	artifactRef := fetchRef.GetArtifacts()[0]
	if artifactRef.GetName() != "body" || artifactRef.GetUri() != "s3://bucket/run/fetch/body" ||
		artifactRef.GetContentType() != "application/json" || artifactRef.GetSizeBytes() != 17 ||
		artifactRef.GetSha256() != "abc123" {
		t.Errorf("upstream artifact = %+v, want the ref produced by fetch", artifactRef)
	}
	// The engine must NOT have rendered the template: `artifact "fetch/body"`
	// resolves to a container path only the Orker knows, after staging.
	if got := transform.GetWith().AsMap()["command"]; got != "cat {{ artifact \"fetch/body\" }}" {
		t.Errorf("with was rendered by the engine: %v (rendering belongs to the Orker)", got)
	}

	// save needs transform, which transitively needs fetch: both are visible.
	save, ok := h.inputFor("save")
	if !ok {
		t.Fatal("save task was never executed")
	}
	if upstreamRef(save, "fetch") == nil {
		t.Errorf("save upstream = %v, want the transitive fetch entry", save.GetUpstream())
	}
	if upstreamRef(save, "transform") == nil {
		t.Errorf("save upstream = %v, want the direct transform entry", save.GetUpstream())
	}

	var run orkstackv1.Run
	if err := h.env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() != "fetch" {
			continue
		}
		if len(tr.GetArtifacts()) != 1 || tr.GetArtifacts()[0].GetUri() != "s3://bucket/run/fetch/body" {
			t.Errorf("fetch task run artifacts = %v, want the produced ref", tr.GetArtifacts())
		}
	}
}

const conditionDocument = `
name: conditional
inputs:
  enabled:
    type: boolean
tasks:
  - id: always
    uses: shell
    with: {command: "echo always"}
  - id: maybe
    uses: shell
    needs: [always]
    condition: "{{ .inputs.enabled }}"
    with: {command: "echo maybe"}
  - id: after
    uses: shell
    needs: [maybe]
    with: {command: "echo after"}
`

func TestRunWorkflow_SkipsTaskWithUnmetCondition(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-condition",
		compile(t, conditionDocument), payload(t, map[string]any{"enabled": false}), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if order := h.taskOrder(); fmt.Sprint(order) != fmt.Sprint([]string{"always"}) {
		t.Fatalf("executed tasks = %v, want only [always]", order)
	}

	var run orkstackv1.Run
	if err := h.env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if got := statusOf(&run, "maybe"); got != orkstackv1.Status_STATUS_SKIPPED {
		t.Errorf("maybe status = %v, want SKIPPED", got)
	}
	if got := statusOf(&run, "after"); got != orkstackv1.Status_STATUS_SKIPPED {
		t.Errorf("after status = %v, want SKIPPED (skip propagates downstream)", got)
	}
	// A skipped task NEVER reached an Orker, so it has no start. Stamping one
	// made every reader — the run page, orkctl, the duration histogram —
	// believe a zero-length execution happened. finished_at stays: the run did
	// resolve these tasks, at that moment.
	for _, id := range []string{"maybe", "after"} {
		tr := taskRunOf(t, &run, id)
		if tr.GetScheduledAt() != nil {
			t.Errorf("%s scheduled_at = %v, want unset: a skipped task was never scheduled",
				id, tr.GetScheduledAt().AsTime())
		}
		if tr.GetStartedAt() != nil {
			t.Errorf("%s started_at = %v, want unset: a skipped task never reached an Orker",
				id, tr.GetStartedAt().AsTime())
		}
		if tr.GetFinishedAt() == nil {
			t.Errorf("%s finished_at is unset; the run did resolve this task", id)
		}
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("run status = %v, want SUCCEEDED", run.GetStatus())
	}

	var sawSkipEvent bool
	for _, event := range h.bus.Events("run-condition", 0) {
		if event.GetTaskId() == "maybe" && event.GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
			sawSkipEvent = true
		}
	}
	if !sawSkipEvent {
		t.Error("no event reported the skipped task")
	}
}

func TestRunWorkflow_RunsTaskWithMetCondition(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-condition-true",
		compile(t, conditionDocument), payload(t, map[string]any{"enabled": true}), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if order := h.taskOrder(); fmt.Sprint(order) != fmt.Sprint([]string{"always", "maybe", "after"}) {
		t.Fatalf("executed tasks = %v, want all three", order)
	}
}

const retryDocument = `
name: retrying
tasks:
  - id: flaky
    uses: shell
    timeout_seconds: 5
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 1.0
      maximum_interval_seconds: 1
    with: {command: "exit 1"}
  - id: downstream
    uses: shell
    needs: [flaky]
    with: {command: "echo never"}
`

func TestRunWorkflow_RetriesPerPolicyThenFailsRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var attempts int32
	var mu sync.Mutex

	h := newHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{}, nil
	})
	// Explicit mock: assert the activity is retried exactly three times.
	h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return &orkstackv1.ExecuteTaskResult{}, errors.New("workload exited 1")
		}).Times(3)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-retry", compile(t, retryDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	err := h.env.GetWorkflowError()
	if err == nil {
		t.Fatal("workflow error = nil, want the run to fail after retries are exhausted")
	}

	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 3 {
		t.Errorf("activity attempts = %d, want 3 (maximum_attempts from the DSL retry policy)", got)
	}

	stored, ok := h.store.Get("run-retry")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED", stored.GetStatus())
	}
	if statusOf(stored, "flaky") != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("flaky status = %v, want FAILED", statusOf(stored, "flaky"))
	}
	if statusOf(stored, "downstream") == orkstackv1.Status_STATUS_SUCCEEDED {
		t.Error("downstream task ran even though its dependency failed")
	}

	var sawRunFailed bool
	for _, event := range h.bus.Events("run-retry", 0) {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
			sawRunFailed = true
		}
	}
	if !sawRunFailed {
		t.Error("no RUN_FAILED event was published")
	}
}

// TestRunWorkflow_KeepsTheTaskCauseWhenSnapshotWritesFail pins the §15
// contract that a terminal failure keeps its machine-readable reason: when a
// task has already failed and object storage then dies, the run must fail
// with the task's own cause, not with the snapshot-write error that followed
// it. Before the fix collect returned the emit error, replacing what the
// Orker actually said.
func TestRunWorkflow_KeepsTheTaskCauseWhenSnapshotWritesFail(t *testing.T) {
	const document = `
name: cause-kept
tasks:
  - id: doomed
    uses: shell
    with: {command: "exit 3"}
`
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})

	// Storage dies at the exact moment the run starts recording the failure —
	// the moment a storage error could shadow the task's cause.
	healthy := newTestSnapshotStore()
	snapshots := engine.NewSnapshotActivities(healthy)
	env.RegisterActivityWithOptions(
		func(ctx context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			failing := run.GetStatus() == orkstackv1.Status_STATUS_FAILED
			for _, taskRun := range run.GetTaskRuns() {
				if taskRun.GetStatus() == orkstackv1.Status_STATUS_FAILED {
					failing = true
				}
			}
			if failing {
				return "", errors.New("object storage is down")
			}
			return snapshots.WriteSnapshot(ctx, run, sequence)
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(healthy).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return nil, errors.New("workload exited 3")
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	env.ExecuteWorkflow(engine.RunWorkflow, "run-cause-kept", compile(t, document), payload(t, nil), defaultPlan())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("workflow error = nil, want the task failure")
	}
	if !strings.Contains(err.Error(), "workload exited 3") {
		t.Fatalf("workflow error = %v, want the task's own cause", err)
	}
	if strings.Contains(err.Error(), "object storage is down") ||
		strings.Contains(err.Error(), "write snapshot") {
		t.Fatalf("workflow error = %v, the storage failure replaced the task's cause", err)
	}
}

func TestRunWorkflow_SingleAttemptWhenNoRetryPolicy(t *testing.T) {
	const document = `
name: no-retry
tasks:
  - id: only
    uses: shell
    timeout_seconds: 5
    with: {command: "exit 1"}
`
	var suite testsuite.WorkflowTestSuite
	var attempts int
	var mu sync.Mutex
	h := newHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return &orkstackv1.ExecuteTaskResult{}, errors.New("boom")
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-retry", compile(t, document), payload(t, nil), defaultPlan())

	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want failure")
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 when the DSL declares no retry policy", attempts)
	}
}

func TestRunWorkflow_PassesDeclaredInputDefaultsToExecuteTask(t *testing.T) {
	const document = `
name: default-input
inputs:
  url:
    type: string
    default: "https://default.test"
tasks:
  - id: fetch
    uses: http
    with:
      url: "{{ .inputs.url }}"
`
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-default-input", compile(t, document), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	input, ok := h.inputFor("fetch")
	if !ok {
		t.Fatal("fetch task was not executed")
	}
	if got := input.GetRunInput().GetData().AsMap()["url"]; got != "https://default.test" {
		t.Errorf("ExecuteTaskInput.RunInput.url = %v, want declared default", got)
	}
}

func TestRunWorkflow_SchedulesChildWhenItsOwnNeedCompletes(t *testing.T) {
	const document = `
name: readiness-driven
tasks:
  - id: slow
    uses: shell
    timeout_seconds: 5
  - id: fast
    uses: shell
  - id: child
    uses: shell
    needs: [fast]
`
	var suite testsuite.WorkflowTestSuite
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "slow":
			select {
			case <-releaseSlow:
				return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
			case <-time.After(2 * time.Second):
				return &orkstackv1.ExecuteTaskResult{}, errors.New("child was not scheduled before unrelated slow task")
			}
		case "child":
			releaseOnce.Do(func() { close(releaseSlow) })
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-readiness", compile(t, document), payload(t, nil), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if _, ok := h.inputFor("child"); !ok {
		t.Fatal("child was not scheduled after fast completed")
	}
	if got := statusOf(mustWorkflowResult(t, h.env), "slow"); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("slow status = %v, want SUCCEEDED", got)
	}
}

func mustWorkflowResult(t *testing.T, env *testsuite.TestWorkflowEnvironment) *orkstackv1.Run {
	t.Helper()
	var run orkstackv1.Run
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	return &run
}

// TestRunWorkflow_CarriesThePlanTelemetryOntoTheInputAndTheSnapshot is the
// workflow half of the telemetry wire: a plan decided at submit
// (pools.go telemetryFor) must reach both places an Orker or a client can see
// it — the ExecuteTaskInput each task is dispatched with, and the Run
// snapshot returned as the workflow's result — unchanged, since workflow code
// only carries what the plan already decided (§2.1).
func TestRunWorkflow_CarriesThePlanTelemetryOntoTheInputAndTheSnapshot(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})

	plan := defaultPlan()
	plan.Telemetry = &engine.TelemetrySelection{
		Sink:       "otel-prod",
		TaskLogs:   "TASK_LOG_EXPORT_ENABLED",
		Attributes: map[string]string{"ork.ns.team": "payments"},
	}

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-telemetry-set", compile(t, dispatchDocument), payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	in, ok := h.inputFor("build")
	if !ok {
		t.Fatal("build was never dispatched")
	}
	if got := in.GetTelemetry(); got.GetSink() != "otel-prod" ||
		got.GetTaskLogs() != orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED ||
		got.GetAttributes()["ork.ns.team"] != "payments" {
		t.Errorf("ExecuteTaskInput.telemetry = %v, want the plan's selection", got)
	}

	run := mustWorkflowResult(t, h.env)
	if got := run.GetTelemetry(); got.GetSink() != "otel-prod" ||
		got.GetTaskLogs() != orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED ||
		got.GetAttributes()["ork.ns.team"] != "payments" {
		t.Errorf("Run.telemetry = %v, want the plan's selection", got)
	}
}

// TestRunWorkflow_LeavesTelemetryUnsetWithNoPlanSelection is the golden-history
// invariant restated at the workflow boundary: a plan with no telemetry
// selection — every plan before this feature, and every plan for a namespace
// with no policy and an orkflow with no labels today — produces NO telemetry
// field on either the input or the snapshot, not a zero-value message.
func TestRunWorkflow_LeavesTelemetryUnsetWithNoPlanSelection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})

	plan := defaultPlan()
	plan.Telemetry = nil

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-telemetry-unset", compile(t, dispatchDocument), payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	in, ok := h.inputFor("build")
	if !ok {
		t.Fatal("build was never dispatched")
	}
	if in.GetTelemetry() != nil {
		t.Errorf("ExecuteTaskInput.telemetry = %v, want nil", in.GetTelemetry())
	}

	run := mustWorkflowResult(t, h.env)
	if run.GetTelemetry() != nil {
		t.Errorf("Run.telemetry = %v, want nil", run.GetTelemetry())
	}
}

const cancelSiblingsDocument = `
name: cancel-siblings
tasks:
  - id: doomed
    uses: shell
    with: {command: "exit 1"}
  - id: sibling
    uses: shell
    with: {command: "sleep 999"}
  - id: downstream
    uses: shell
    needs: [doomed]
    with: {command: "echo never"}
`

// TestRunWorkflow_CancelsSiblingsAndSkipsUnstartedTasksWhenATaskFails pins the
// fail-fast contract's other half: a run that ends because one task failed must
// also END its other tasks. Before this, the workflow returned the moment any
// future errored, which left every sibling activity genuinely running — Temporal
// does not cancel activities a workflow abandons by returning, only an explicit
// cancellation on the context they were scheduled with does — and left every
// task the loop never reached sitting at PENDING forever, which reads as "still
// queued" for a run that is already over.
func TestRunWorkflow_CancelsSiblingsAndSkipsUnstartedTasksWhenATaskFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	siblingCanceled := make(chan struct{})

	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	snapshots := newTestSnapshotStore()
	env.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	bus := engine.NewEventBus()
	store := engine.NewRunStore()
	env.RegisterActivityWithOptions(engine.NewEventActivities(bus, store).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.RegisterActivityWithOptions(cancelSiblingsTask(t, siblingCanceled),
		activity.RegisterOptions{Name: task.ActivityExecuteTask})

	started := time.Now()
	env.ExecuteWorkflow(engine.RunWorkflow, "run-cancel-siblings", compile(t, cancelSiblingsDocument), payload(t, nil), defaultPlan())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow error = nil, want doomed's failure to fail the run")
	}
	// A deadline, not a non-blocking read: cancellation reaches an activity
	// asynchronously through its heartbeat, and the test environment resolves
	// the workflow-side future the moment it issues the cancel — it does not
	// honour WaitForCancellation for activities the way a real server does. So
	// the workflow can complete a few milliseconds before the activity
	// goroutine observes actCtx.Done(). Waiting is the only correct read; the
	// deadline is what makes "never canceled" a failure rather than a hang.
	select {
	case <-siblingCanceled:
		t.Logf("sibling observed cancellation %s after the run was submitted", time.Since(started))
	case <-time.After(30 * time.Second):
		t.Fatal("sibling never observed cancellation: it was left running after the run failed")
	}

	stored, ok := store.Get("run-cancel-siblings")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED", stored.GetStatus())
	}
	if got := statusOf(stored, "sibling"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("sibling status = %v, want CANCELED, not left RUNNING", got)
	}
	if got := statusOf(stored, "downstream"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("downstream status = %v, want CANCELED (never started), not left PENDING", got)
	}

	// BOTH tasks read CANCELED, and that is correct — it is the lifecycle
	// truth for each. scheduled_at is the ONLY thing on the snapshot that
	// tells them apart, so it has to be honest: `sibling` was dispatched and
	// killed, `downstream` never reached an Orker at all. Stamping both made a
	// task that never ran render with the same CANCELED badge as one that
	// did, on the run page and everywhere else that reads the snapshot.
	if tr := taskRunOf(t, stored, "downstream"); tr.GetScheduledAt() != nil || tr.GetStartedAt() != nil {
		t.Errorf("downstream scheduled_at = %v, started_at = %v, want both unset: the run ended "+
			"before it was dispatched", tr.GetScheduledAt(), tr.GetStartedAt())
	} else if tr.GetFinishedAt() == nil {
		t.Error("downstream finished_at is unset; the run did resolve this task")
	}
	if tr := taskRunOf(t, stored, "sibling"); tr.GetScheduledAt() == nil {
		t.Error("sibling scheduled_at is unset; it really was dispatched before it was canceled, and a " +
			"client cannot tell it from downstream without that stamp")
	} else if tr.GetStartedAt() != nil {
		// Its cancellation reached the Orker as a plain context cancellation:
		// no Orker reported a start, and none is invented.
		t.Errorf("sibling started_at = %v, want unset: no Orker reported a start for it",
			tr.GetStartedAt().AsTime())
	}
}

// cancelSiblingsTask is cancelSiblingsDocument's task activity: doomed fails,
// sibling runs until it is canceled (closing siblingCanceled), and any other
// task is an error because the run must end before it is dispatched.
//
// doomed fails only once sibling's function is really running. Without that
// ordering the test raced the SDK's test environment: when the workflow's
// cancel lands before the environment's goroutine has invoked sibling, the
// environment answers Canceled WITHOUT calling the function at all
// (activityExecutorWrapper.Execute: "If activity handle cannot be found, we
// assume it was cancelled"), so siblingCanceled never closed and the test
// failed after its 30s deadline — about one run in twenty on an idle host,
// more under load, on this branch's base as well as on it. Waiting here makes
// sibling genuinely running when the run fails, which is the case the test
// exists to pin.
func cancelSiblingsTask(
	t *testing.T,
	siblingCanceled chan struct{},
) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	siblingRunning := make(chan struct{})
	var markRunning sync.Once
	return func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch in.TaskId {
		case "doomed":
			select {
			case <-siblingRunning:
			case <-actCtx.Done():
				return &orkstackv1.ExecuteTaskResult{}, actCtx.Err()
			case <-time.After(30 * time.Second):
				t.Error("sibling was never started while doomed ran; the fixture cannot pin cancellation")
			}
			return &orkstackv1.ExecuteTaskResult{}, errors.New("workload exited 1")
		case "sibling":
			markRunning.Do(func() { close(siblingRunning) })
			// Temporal delivers cancellation to an activity ONLY through a
			// heartbeat response (see workflow.go's orkerHeartbeatTimeout
			// comment) — this must heartbeat in a loop, exactly like the
			// real Orker does, or it will never observe actCtx.Done().
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-actCtx.Done():
					close(siblingCanceled)
					return &orkstackv1.ExecuteTaskResult{}, actCtx.Err()
				case <-ticker.C:
					activity.RecordHeartbeat(actCtx, "waiting")
				}
			}
		default:
			t.Errorf("task %q ran, want it never scheduled", in.TaskId)
			return &orkstackv1.ExecuteTaskResult{}, nil
		}
	}
}

// TestRunWorkflow_NeverAnnouncesACanceledSiblingAsATaskOutcome drives the real
// interpreter through the case the webhook contract excludes by status: a task
// that was RUNNING when its run ended comes back through collect as a
// TASK_FAILED event carrying STATUS_CANCELED. The bus must still record it —
// that is the fixture the guard exists for, and asserting it turns an
// interpreter change that stops emitting it into a visible failure — while the
// notifier must never be told that task failed.
func TestRunWorkflow_NeverAnnouncesACanceledSiblingAsATaskOutcome(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	siblingCanceled := make(chan struct{})

	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	snapshots := newTestSnapshotStore()
	env.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	bus := engine.NewEventBus()
	store := engine.NewRunStore()
	notifier := &recordingNotifier{}
	env.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, store).WithSnapshotStore(snapshots).WithRunNotifier(notifier).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.RegisterActivityWithOptions(cancelSiblingsTask(t, siblingCanceled),
		activity.RegisterOptions{Name: task.ActivityExecuteTask})

	const runID = "run-cancel-siblings-announce"
	env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, cancelSiblingsDocument), payload(t, nil), defaultPlan())
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow error = nil, want doomed's failure to fail the run")
	}
	select {
	case <-siblingCanceled:
	case <-time.After(30 * time.Second):
		t.Fatal("sibling never observed cancellation")
	}

	// (1) The interpreter emitted TASK_FAILED carrying CANCELED for sibling.
	emitted := false
	for _, event := range bus.Events(runID, 0) {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && event.GetTaskId() == "sibling" &&
			event.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
			emitted = true
		}
	}
	if !emitted {
		t.Fatal("the bus holds no TASK_FAILED/CANCELED event for sibling; the guard this test pins has nothing to guard")
	}

	notifier.mu.Lock()
	events := append([]*orkstackv1.Event{}, notifier.events...)
	runs := append([]*orkstackv1.Run{}, notifier.runs...)
	notifier.mu.Unlock()

	// (2) Exactly one TASK_FAILED is announced: doomed's, with FAILED.
	// (3) None names sibling.
	var failures []*orkstackv1.Event
	for _, event := range events {
		if event.GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
			continue
		}
		failures = append(failures, event)
		if event.GetTaskId() == "sibling" {
			t.Errorf("announced TASK_FAILED for sibling with status %v; a canceled task is not a failure", event.GetStatus())
		}
	}
	if len(failures) != 1 || failures[0].GetTaskId() != "doomed" || failures[0].GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("announced TASK_FAILED events %v, want exactly doomed's with STATUS_FAILED", failures)
	}

	// (4) The announcements end with RUN_FAILED, whose run reads sibling as
	// CANCELED with a scheduledAt (it was dispatched) and downstream as
	// CANCELED with neither scheduledAt nor startedAt (it never was).
	if len(events) == 0 {
		t.Fatal("nothing was announced")
	}
	last := events[len(events)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("last announcement = %v, want RUN_FAILED", last.GetType())
	}
	final := runs[len(runs)-1]
	if tr := taskRunOf(t, final, "sibling"); tr.GetStatus() != orkstackv1.Status_STATUS_CANCELED || tr.GetScheduledAt() == nil {
		t.Errorf("RUN_FAILED run reads sibling as %v (scheduledAt %v), want CANCELED with a scheduledAt", tr.GetStatus(), tr.GetScheduledAt())
	}
	if tr := taskRunOf(t, final, "downstream"); tr.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		tr.GetScheduledAt() != nil || tr.GetStartedAt() != nil {
		t.Errorf("RUN_FAILED run reads downstream as %v (scheduledAt %v, startedAt %v), want CANCELED with neither",
			tr.GetStatus(), tr.GetScheduledAt(), tr.GetStartedAt())
	}
}

// ---------------------------------------------------------------------------
// Task pins (docs/design/task-pinning-worker-session.md §6)
// ---------------------------------------------------------------------------

// pinnedDocument is the §4.1 shape: two members of one pin, plus an unpinned
// downstream task that sees them only through what they committed.
const pinnedDocument = `
name: pinned
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: publish
    uses: shell
    needs: [compile]
    with: {command: "echo publish"}
`

// TestRunWorkflow_SchedulesEveryPinnedTaskOnOnePinSessionAndReleasesIt is the
// whole happy path of a pin, in the terms a client reads it in.
//
// Every member carries the SAME PinContext.instance_id, which is the engine's
// promise that they ran on one Orker process against one /orkstack/shared; the
// unpinned task carries NO pin field at all, which is what keeps every
// pre-pin payload byte-identical (§10.2); and the pin is released and closed
// rather than left holding the host's slot.
func TestRunWorkflow_SchedulesEveryPinnedTaskOnOnePinSessionAndReleasesIt(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := succeeded(in.TaskId, map[string]any{"ok": true})
		result.OrkerId = "orker-pin-host"
		return result, nil
	}, nil)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pinned",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	prep, ok := h.inputFor("prep")
	if !ok {
		t.Fatal("prep was never dispatched")
	}
	compileIn, ok := h.inputFor("compile")
	if !ok {
		t.Fatal("compile was never dispatched")
	}
	if prep.GetPin().GetName() != "build" || compileIn.GetPin().GetName() != "build" {
		t.Fatalf("pin names = %q/%q, want both to be build",
			prep.GetPin().GetName(), compileIn.GetPin().GetName())
	}
	if prep.GetPin().GetInstanceId() == "" {
		t.Fatal("prep carries no pin instance id: the Orker would have nothing to key the shared directory by")
	}
	if prep.GetPin().GetInstanceId() != compileIn.GetPin().GetInstanceId() {
		t.Errorf("pin instance ids = %q and %q, want ONE opening of the pin for both members",
			prep.GetPin().GetInstanceId(), compileIn.GetPin().GetInstanceId())
	}
	// The derived lifetime (§6.3): both members' own budgets, their grace
	// periods and this plan's transfer budget, plus the release budget.
	if got := prep.GetPin().GetTimeoutSeconds(); got != 2*(300+30)+60 {
		t.Errorf("pin timeout_seconds = %d, want the derived sequential worst case", got)
	}

	// The unpinned task's payload is UNCHANGED by this feature. protojson omits
	// an unset message field, which is why the golden history fixture keeps
	// matching without being re-recorded.
	publish, ok := h.inputFor("publish")
	if !ok {
		t.Fatal("publish was never dispatched")
	}
	if publish.GetPin() != nil {
		t.Errorf("unpinned task carries pin = %v, want nil", publish.GetPin())
	}

	run := mustWorkflowResult(t, h.env)
	for _, id := range []string{"prep", "compile"} {
		if got := taskRunOf(t, run, id).GetPin(); got != "build" {
			t.Errorf("%s TaskRun.pin = %q, want build", id, got)
		}
	}
	if got := taskRunOf(t, run, "publish").GetPin(); got != "" {
		t.Errorf("publish TaskRun.pin = %q, want empty", got)
	}
	// orker_id for EVERY task, pinned or not (§9). For a pin it is the
	// client-visible proof the pin held: same pin, same Orker.
	for _, id := range []string{"prep", "compile", "publish"} {
		if got := taskRunOf(t, run, id).GetOrkerId(); got != "orker-pin-host" {
			t.Errorf("%s TaskRun.orker_id = %q, want the Orker that produced it", id, got)
		}
	}

	// TASK_SCHEDULED says pool AND pin for a member, and pool alone otherwise.
	messages := map[string]string{}
	for _, event := range h.bus.Events("run-pinned", 0) {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			messages[event.GetTaskId()] = event.GetMessage()
		}
	}
	if got := messages["prep"]; got != "task scheduled on pool main, pin build" {
		t.Errorf("prep TASK_SCHEDULED = %q, want it to name the pin", got)
	}
	if got := messages["publish"]; got != "task scheduled on pool main" {
		t.Errorf("publish TASK_SCHEDULED = %q, want the message unchanged for an unpinned task", got)
	}

	// Released on the pin host, then the session completed: the host's slot
	// and its shared directory are both given back (§6.4, §8.5).
	releases := h.releases()
	if len(releases) != 1 {
		t.Fatalf("ReleasePin calls = %d, want exactly one", len(releases))
	}
	if got := releases[0].GetPin().GetInstanceId(); got != prep.GetPin().GetInstanceId() {
		t.Errorf("ReleasePin instance id = %q, want the opening the members ran against", got)
	}
	if releases[0].GetRunId() != "run-pinned" {
		t.Errorf("ReleasePin run id = %q, want run-pinned", releases[0].GetRunId())
	}
	if !h.startedActivity(sessionCompletionActivityName) {
		t.Errorf("the session was never completed, so the host would hold its slot: %v",
			h.startedActivities())
	}
}

// TestRunWorkflow_NeverOpensAPinWhoseTasksAreAllSkipped is the laziness
// contract (§6.2): a pin is opened by the first member the run actually
// decides to RUN, so a pin whose every member is skipped by condition costs an
// Orker nothing — no session, no slot, no shared directory, no lifetime clock.
func TestRunWorkflow_NeverOpensAPinWhoseTasksAreAllSkipped(t *testing.T) {
	const document = `
name: pinned-skipped
inputs:
  enabled:
    type: boolean
pins:
  build: {}
tasks:
  - id: gate
    uses: shell
    with: {command: "echo gate"}
  - id: prep
    uses: shell
    pin: build
    needs: [gate]
    condition: "{{ .inputs.enabled }}"
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep]
    run_as_orker_user: true
    with: {command: "echo compile"}
`
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, func(in *orkstackv1.ReleasePinInput) error {
		t.Errorf("ReleasePin was called for a pin that never opened: %v", in)
		return nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pinned-skipped",
		compile(t, document), payload(t, map[string]any{"enabled": false}), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if h.startedActivity(sessionCreationActivityName) {
		t.Errorf("a session was created for a pin whose every task was skipped: %v",
			h.startedActivities())
	}
	run := mustWorkflowResult(t, h.env)
	for _, id := range []string{"prep", "compile"} {
		if got := statusOf(run, id); got != orkstackv1.Status_STATUS_SKIPPED {
			t.Errorf("%s status = %v, want SKIPPED", id, got)
		}
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("run status = %v, want SUCCEEDED", run.GetStatus())
	}
}

// TestRunWorkflow_HoldsAPinnedTaskUntilItsSessionIsReadyWithoutHoldingSiblings
// is the property the pin's coroutine exists for (§6.2).
//
// A member waits for its session — it cannot be dispatched before one exists —
// but an UNRELATED unpinned task must advance in the meantime. A blocking
// CreateSession in the main loop would have stalled every branch of the run
// behind a saturated pool, which is the one thing the re-scanning loop
// promises never to do.
func TestRunWorkflow_HoldsAPinnedTaskUntilItsSessionIsReadyWithoutHoldingSiblings(t *testing.T) {
	const document = `
name: pinned-held
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: elsewhere
    uses: shell
    with: {command: "echo elsewhere"}
`
	var suite testsuite.WorkflowTestSuite
	openSession := make(chan struct{})
	siblingRan := make(chan struct{})

	var suiteOnce sync.Once
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "elsewhere" {
			// The unpinned sibling releases the session only after it has run
			// itself: if the engine had waited for the session before
			// dispatching it, neither would ever move and the deadline below
			// would be the failure.
			suiteOnce.Do(func() { close(siblingRan) })
			close(openSession)
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		}
		select {
		case <-siblingRan:
		default:
			return nil, errors.New("the pinned task was dispatched before its unpinned sibling ran")
		}
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, nil)
	// The pin host answers only once the unpinned sibling has been through.
	h.env.OnActivity(sessionCreationActivityName, mock.Anything, mock.Anything).
		Return(func(actCtx context.Context, sessionID string) error {
			select {
			case <-openSession:
			case <-time.After(20 * time.Second):
				return errors.New("the unpinned sibling never ran while the pin was opening")
			}
			h.env.SignalWorkflow(sessionID, sessionCreationResponse{
				Taskqueue:  "test-session-queue",
				HostName:   "test-pin-host",
				ResourceID: "test-resource",
			})
			// Stay alive: a creation activity that returns is a session that
			// failed, and this test is about a session that opens.
			<-actCtx.Done()
			return actCtx.Err()
		})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pinned-held",
		compile(t, document), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got := h.taskOrder(); len(got) != 2 || got[0] != "elsewhere" {
		t.Fatalf("dispatch order = %v, want the unpinned sibling first, while the pin opened", got)
	}
	run := mustWorkflowResult(t, h.env)
	if got := statusOf(run, "prep"); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("prep status = %v, want SUCCEEDED once its session opened", got)
	}
}

// TestRunWorkflow_RunsConcurrentPinMembersOnOneSession is §4.6 restated at the
// engine: a pin imposes NO ordering, so two members with no `needs:` edge
// between them run at the same time — and, being one pin, on one session.
func TestRunWorkflow_RunsConcurrentPinMembersOnOneSession(t *testing.T) {
	const document = `
name: pinned-concurrent
pins:
  build: {}
tasks:
  - id: left
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo left"}
  - id: right
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo right"}
`
	var suite testsuite.WorkflowTestSuite
	started := make(chan string, 2)
	// Each member blocks until the other has started. Serialized members would
	// never release each other and the deadline would be the failure.
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		started <- in.TaskId
		deadline := time.After(20 * time.Second)
		for len(started) < 2 {
			select {
			case <-deadline:
				return nil, errors.New("two pin members with no edge between them did not run concurrently")
			default:
				time.Sleep(time.Millisecond)
			}
		}
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, nil)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pinned-concurrent",
		compile(t, document), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	left, ok := h.inputFor("left")
	if !ok {
		t.Fatal("left was never dispatched")
	}
	right, ok := h.inputFor("right")
	if !ok {
		t.Fatal("right was never dispatched")
	}
	if left.GetPin().GetInstanceId() != right.GetPin().GetInstanceId() {
		t.Errorf("concurrent members got instance ids %q and %q, want one session for the pin",
			left.GetPin().GetInstanceId(), right.GetPin().GetInstanceId())
	}
	// One creation, not one per member.
	creations := 0
	for _, name := range h.startedActivities() {
		if name == sessionCreationActivityName {
			creations++
		}
	}
	if creations != 1 {
		t.Errorf("session creations = %d, want 1 for one pin", creations)
	}
}

// TestRunWorkflow_ClassifiesALostPinHostAsPinHostLost is the sharpest rule of
// the feature (§6.5): A PIN NEVER MOVES.
//
// When the host dies the SDK marks the session failed and cancels every
// activity on it, so the task's own future returns a CANCELLATION. Recording
// that as CANCELED would say the operator ended the run, and would invite a
// reader to retry the task somewhere else — where /orkstack/shared would be
// empty and the result silently wrong. So the member is FAILED with
// PIN_HOST_LOST, the run FAILS rather than reporting itself cancelled, and the
// error says the task cannot be retried elsewhere.
func TestRunWorkflow_ClassifiesALostPinHostAsPinHostLost(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, nil)
	h.answerThenLosePinHost(nil, temporal.NewNonRetryableApplicationError(
		"pin host stopped heartbeating", task.CategoryInfrastructureError, nil))

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-lost",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the lost pin to fail the run")
	}
	stored, ok := h.store.Get("run-pin-lost")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED — nobody cancelled this run, the platform lost the host",
			stored.GetStatus())
	}
	prep := taskRunOf(t, stored, "prep")
	if prep.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("prep status = %v, want FAILED and not CANCELED", prep.GetStatus())
	}
	if got := prep.GetError().GetCode(); got != "PIN_HOST_LOST" {
		t.Errorf("prep error code = %q, want PIN_HOST_LOST", got)
	}
	if got := prep.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("prep error category = %v, want INFRASTRUCTURE_ERROR", got)
	}
	if prep.GetError().GetRetryable() {
		t.Error("prep error is retryable; a pin never moves, so retrying it is exactly what must not happen")
	}
	// The run's own reason is the pin's, not the bare word "canceled".
	if got := stored.GetError().GetMessage(); !strings.Contains(got, "PIN") &&
		!strings.Contains(got, "lost the Orker") {
		t.Errorf("run error message = %q, want it to explain the lost pin", got)
	}
	// A member the run never reached is CANCELED, with started_at unset: it
	// never got to an Orker at all (§6.5's table).
	if got := statusOf(stored, "compile"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("compile status = %v, want CANCELED", got)
	}
	if tr := taskRunOf(t, stored, "compile"); tr.GetScheduledAt() != nil || tr.GetStartedAt() != nil {
		t.Errorf("compile scheduled_at = %v, started_at = %v, want both unset",
			tr.GetScheduledAt(), tr.GetStartedAt())
	}
	// E4's pin-loss half: prep WAS dispatched, but its host died without
	// reporting, so it has a scheduled_at and no started_at.
	if prep.GetScheduledAt() == nil {
		t.Error("prep scheduled_at is unset; it was dispatched before the host was lost")
	}
	if prep.GetStartedAt() != nil {
		t.Errorf("prep started_at = %v, want unset: the lost host never reported a start",
			prep.GetStartedAt().AsTime())
	}

	// ── the no-sibling half of "which Orker died?" ────────────────────────
	//
	// prep is the pin's FIRST dispatched member, so when its host dies nothing
	// has reported an orker_id and nothing ever will. The engine does not
	// invent one: orker_id stays empty, which is exactly what that field has
	// always meant. What it does leave is a labelled lead — the Temporal
	// session's own host name, which is the SDK's os.Hostname() of the worker
	// that answered the creation request and NOT an ORK_ORKER_ID, hence the
	// label saying so.
	if got := prep.GetOrkerId(); got != "" {
		t.Errorf("prep orker_id = %q, want empty: no member of this pin ever reported one, "+
			"and a raw session host name must never be passed off as an orker_id", got)
	}
	details := prep.GetError().GetDetails()
	if !strings.Contains(details, "test-pin-host") {
		t.Errorf("prep error details = %q, want the lost session's host name as a lead", details)
	}
	if !strings.Contains(details, "may differ from orker_id") {
		t.Errorf("prep error details = %q, want the lead labelled as NOT an orker_id", details)
	}
	// The task that never reached an Orker gets neither: it was not on the
	// dead host, so saying it was would be the lie an empty orker_id avoids.
	compile := taskRunOf(t, stored, "compile")
	if got := compile.GetOrkerId(); got != "" {
		t.Errorf("compile orker_id = %q, want empty: it never reached an Orker", got)
	}
	if got := compile.GetError().GetDetails(); strings.Contains(got, "session host") {
		t.Errorf("compile error details = %q, want no host lead on a task that never ran", got)
	}
}

// TestRunWorkflow_BackfillsALostPinMembersOrkerIDFromItsSibling is the
// operability half of §6.5: "which Orker died?" is the first question a lost
// pin raises, and the run record has to answer it.
//
// A member lost WITH its pin reports nothing — its host died, so there is no
// ExecuteTaskResult and no ActivityFailureDetail — and TaskRun.orker_id came
// back blank on precisely the task an operator is looking at. The fix needs no
// new field on the wire, because A PIN NEVER MOVES: every member executes on
// the same Orker process for the pin's whole life, so the orker_id a SIBLING
// recorded is this task's by construction and not by inference.
//
// prep succeeds on a named Orker; compile is still running on that same host
// when it dies. compile therefore learns who killed it from prep.
func TestRunWorkflow_BackfillsALostPinMembersOrkerIDFromItsSibling(t *testing.T) {
	const pinHost = "orker-pin-host-7"
	var suite testsuite.WorkflowTestSuite
	compiling := make(chan struct{})
	var once sync.Once
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId != "compile" {
			result := succeeded(in.TaskId, map[string]any{"ok": true})
			// The one honest source of an orker_id: a healthy Orker naming
			// itself on the result it returns.
			result.OrkerId = pinHost
			return result, nil
		}
		// The pin's host dies the moment compile is executing on it, which is
		// the state the backfill exists for.
		once.Do(func() { close(compiling) })
		// Cancelled by the session failure, never by this deadline — it is a
		// hang guard so a regression fails the test instead of the suite.
		<-time.After(30 * time.Second)
		return nil, errors.New("compile was never cancelled by its lost pin")
	}, nil)
	h.answerThenLosePinHost(compiling, temporal.NewNonRetryableApplicationError(
		"pin host stopped heartbeating", task.CategoryInfrastructureError, nil))

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-lost-backfill",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the lost pin to fail the run")
	}
	stored, ok := h.store.Get("run-pin-lost-backfill")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	prep := taskRunOf(t, stored, "prep")
	if prep.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("prep status = %v, want SUCCEEDED — it is the sibling the backfill reads",
			prep.GetStatus())
	}
	if got := prep.GetOrkerId(); got != pinHost {
		t.Fatalf("prep orker_id = %q, want %q off its own result", got, pinHost)
	}

	held := taskRunOf(t, stored, "compile")
	if held.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("compile status = %v, want FAILED with its pin", held.GetStatus())
	}
	if got := held.GetError().GetCode(); got != "PIN_HOST_LOST" {
		t.Fatalf("compile error code = %q, want PIN_HOST_LOST", got)
	}
	// THE assertion. Before this backfill existed the field was empty here,
	// because a dead host reports nothing — and the run record could not say
	// which Orker an operator had just lost.
	if got := held.GetOrkerId(); got != pinHost {
		t.Errorf("compile orker_id = %q, want %q backfilled from its pin sibling: a pin never "+
			"moves, so prep's Orker IS compile's, and a blank here leaves the operator with "+
			"no answer to \"which host died?\"", got, pinHost)
	}
	// With a verified identity recorded there is nothing to guess at, so the
	// best-effort session-host lead must NOT also be present: two answers to
	// one question invite a reader to compare them.
	if got := held.GetError().GetDetails(); strings.Contains(got, "may differ from orker_id") {
		t.Errorf("compile error details = %q, want no best-effort host lead beside a "+
			"backfilled orker_id", got)
	}
}

// TestRunWorkflow_ClassifiesAPinThatOutlivedItsTimeoutAsPinTimeout is
// PIN_HOST_LOST's sibling, and the only thing separating them is the clock
// (§6.5): both arrive as the same session failure, and a pin that has been
// open for at least its own ExecutionTimeout hit its lifetime bound rather
// than losing its host.
//
// The pin's declared lifetime is what the SDK sets as the creation activity's
// StartToCloseTimeout, so a host that outlives it is failed by Temporal
// itself — which is exactly what this arranges.
//
// The member declares a retry, and that is not incidental: the test
// environment's clock only advances through timers, so the retry's own backoff
// is what moves workflow.Now past the pin's one-second lifetime. Without it the
// session still fails, but at workflow time zero — and the engine would then
// correctly call it PIN_HOST_LOST, because no time had passed.
func TestRunWorkflow_ClassifiesAPinThatOutlivedItsTimeoutAsPinTimeout(t *testing.T) {
	const document = `
name: pinned-brief
pins:
  brief:
    timeout_seconds: 1
tasks:
  - id: only
    uses: shell
    pin: brief
    run_as_orker_user: true
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 1.0
      maximum_interval_seconds: 1
    with: {command: "echo only"}
`
	var suite testsuite.WorkflowTestSuite
	dispatched := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	attempts := 0
	h := newPinHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			// Fails fast, so Temporal schedules the backoff timer that carries
			// workflow time over the pin's lifetime.
			return nil, errors.New("first attempt failed")
		}
		once.Do(func() { close(dispatched) })
		// Still running when the pin's lifetime runs out, which is the case
		// the classification is for.
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.After(30 * time.Second)
		for {
			select {
			case <-deadline:
				return nil, errors.New("the pin outlasted its own declared lifetime")
			case <-ticker.C:
			}
		}
	}, nil)
	// A host that answers and then simply keeps holding the pin: Temporal ends
	// the creation activity at the session's ExecutionTimeout — the pin's
	// declared 1s — and the session is failed by that, not by a dead host.
	h.env.OnActivity(sessionCreationActivityName, mock.Anything, mock.Anything).
		Return(func(actCtx context.Context, sessionID string) error {
			h.env.SignalWorkflow(sessionID, sessionCreationResponse{
				Taskqueue:  "test-session-queue",
				HostName:   "test-pin-host",
				ResourceID: "test-resource",
			})
			<-dispatched
			<-actCtx.Done()
			return actCtx.Err()
		})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-expired",
		compile(t, document), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the expired pin to fail the run")
	}
	stored, ok := h.store.Get("run-pin-expired")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	only := taskRunOf(t, stored, "only")
	if only.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("only status = %v, want FAILED", only.GetStatus())
	}
	if got := only.GetError().GetCode(); got != "PIN_TIMEOUT" {
		t.Errorf("only error code = %q, want PIN_TIMEOUT — the pin reached its own lifetime bound", got)
	}
	if got := only.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT {
		t.Errorf("only error category = %v, want TIMED_OUT", got)
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED", stored.GetStatus())
	}
}

// TestRunWorkflow_FailsTheTriggeringTaskWhenNoPinHostHasAFreeSlot is the
// PIN_UNAVAILABLE row of §6.5.
//
// Submit already refused a pool whose Orkers host no pins at all
// (PoolNoPinHostError), so reaching here means the pool's hosts are
// SATURATED: every one is at its ORK_ORKER_MAX_PINS for the whole of
// pinCreationTimeout. The engine never re-creates a session for a pin, so this
// is terminal — reported on the member whose readiness asked for the pin,
// because it is the only one the run had decided to run.
func TestRunWorkflow_FailsTheTriggeringTaskWhenNoPinHostHasAFreeSlot(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Errorf("task %q was dispatched even though its pin never opened", in.TaskId)
		return succeeded(in.TaskId, nil), nil
	}, func(in *orkstackv1.ReleasePinInput) error {
		t.Errorf("ReleasePin was called for a pin that never opened: %v", in)
		return nil
	})
	// Never signals: the creation request is refused outright, which is how a
	// pool with no free slot looks once pinCreationTimeout has run out.
	h.env.OnActivity(sessionCreationActivityName, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError(
			"too many outstanding sessions", task.CategoryInfrastructureError, nil))

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-unavailable",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the unopenable pin to fail the run")
	}
	stored, ok := h.store.Get("run-pin-unavailable")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	prep := taskRunOf(t, stored, "prep")
	if prep.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("prep status = %v, want FAILED", prep.GetStatus())
	}
	if got := prep.GetError().GetCode(); got != "PIN_UNAVAILABLE" {
		t.Errorf("prep error code = %q, want PIN_UNAVAILABLE", got)
	}
	if got := prep.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("prep error category = %v, want INFRASTRUCTURE_ERROR", got)
	}
	// It never reached an Orker, and scheduled_at is the only thing that says
	// so.
	if prep.GetScheduledAt() != nil || prep.GetStartedAt() != nil {
		t.Errorf("prep scheduled_at = %v, started_at = %v, want both unset: the pin never opened, "+
			"so it was never dispatched", prep.GetScheduledAt(), prep.GetStartedAt())
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED", stored.GetStatus())
	}
	for _, id := range []string{"compile", "publish"} {
		if got := statusOf(stored, id); got != orkstackv1.Status_STATUS_CANCELED {
			t.Errorf("%s status = %v, want CANCELED", id, got)
		}
	}
}

// TestRunWorkflow_ClosesAPinAsSoonAsItsOwnMembersAreDone is §6.4's promptness
// claim: a pin closes when ITS work is done, not when the run is.
//
// A run with a fast pin and slow unpinned work after it must give the host's
// session slot and the shared directory back immediately, rather than holding
// a whole Orker hostage to a downstream task that has nothing to do with it.
func TestRunWorkflow_ClosesAPinAsSoonAsItsOwnMembersAreDone(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	var releasedBeforePublish bool
	var released bool

	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "publish" {
			// The last unpinned task looks back: was the pin already given up
			// by the time the run's remaining work started?
			deadline := time.After(20 * time.Second)
			for {
				mu.Lock()
				done := released
				mu.Unlock()
				if done {
					mu.Lock()
					releasedBeforePublish = true
					mu.Unlock()
					break
				}
				select {
				case <-deadline:
					return nil, errors.New("the pin was still open when the run's unpinned work began")
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, func(*orkstackv1.ReleasePinInput) error {
		mu.Lock()
		released = true
		mu.Unlock()
		return nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-closes-early",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !releasedBeforePublish {
		t.Error("the pin was not released before the run's unpinned downstream task ran")
	}
}

// TestRunWorkflow_DoesNotReturnBeforeItsPinHasFinishedClosing is the other
// half of §6.4, and the reason succeed()/fail() wait at all.
//
// A workflow that returned first would ABANDON the pin's coroutine: the host
// would give its session slot back on its next heartbeat, but the release
// would never run and /orkstack/shared would sit on the host's disk until its
// lease expired. So the run waits — for a bounded time — and the proof is that
// a deliberately slow release has FINISHED by the moment the workflow returns.
func TestRunWorkflow_DoesNotReturnBeforeItsPinHasFinishedClosing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var mu sync.Mutex
	var releaseFinished time.Time

	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, func(*orkstackv1.ReleasePinInput) error {
		time.Sleep(250 * time.Millisecond)
		mu.Lock()
		releaseFinished = time.Now()
		mu.Unlock()
		return nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-close-awaited",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())
	returned := time.Now()

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	finished := releaseFinished
	mu.Unlock()
	if finished.IsZero() {
		t.Fatal("the release never finished: the workflow returned and abandoned the pin's coroutine")
	}
	if !finished.Before(returned) {
		t.Errorf("release finished at %v but the workflow returned at %v; the run did not wait for its pin",
			finished, returned)
	}
	if !h.startedActivity(sessionCompletionActivityName) {
		t.Error("the session was never completed before the run returned")
	}
}

// TestRunWorkflow_LeavesAPinnedTaskThatFailedOnItsOwnToTheOrdinaryPath is the
// last row of §6.5, and the boundary the rest of the pin machinery must not
// cross.
//
// A member whose container exits non-zero has failed on its OWN. Temporal
// leaves the session open for it ("Session will not be marked as failed if an
// activity within it returns an error"), so nothing about the pin is
// implicated: the failure is recorded exactly as an unpinned task's would be,
// with the Orker's own reason code and orker_id — and the pin still closes.
func TestRunWorkflow_LeavesAPinnedTaskThatFailedOnItsOwnToTheOrdinaryPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId != "prep" {
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		}
		return nil, temporal.NewNonRetryableApplicationError(
			"workload exited 3", task.CategoryActivityFailed, nil,
			&orkstackv1.ActivityFailureDetail{
				ReasonCode: "CONTAINER_NONZERO_EXIT",
				Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
				ExitCode:   3,
				OrkerId:    "orker-pin-host",
			})
	}, nil)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-own-failure",
		compile(t, pinnedDocument), payload(t, nil), defaultPlan())

	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the member's own failure to fail the run")
	}
	stored, ok := h.store.Get("run-pin-own-failure")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	prep := taskRunOf(t, stored, "prep")
	if got := prep.GetError().GetCode(); got != "CONTAINER_NONZERO_EXIT" {
		t.Errorf("prep error code = %q, want the Orker's own reason and not a pin verdict", got)
	}
	if got := prep.GetExitCode(); got != 3 {
		t.Errorf("prep exit code = %d, want 3", got)
	}
	// orker_id off the failure detail, for a failed attempt as for a
	// successful one (§9): a failure is exactly when an operator wants to know
	// which host it happened on.
	if got := prep.GetOrkerId(); got != "orker-pin-host" {
		t.Errorf("prep orker_id = %q, want it read off the failure detail", got)
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED", stored.GetStatus())
	}
	// The pin is still closed on the way out: a failed member is a terminal
	// member (§4.7).
	if len(h.releases()) != 1 {
		t.Errorf("ReleasePin calls = %d, want the pin released even though a member failed",
			len(h.releases()))
	}
}

// TestRunWorkflow_RecordsTheOrkerOfAnUnpinnedTaskThatFailed is §9's
// unconditional half, on the path that has nothing to do with pins.
//
// orker_id is populated for EVERY task, pinned or not: the field already
// travelled on ExecuteTaskResult and ActivityFailureDetail, and the snapshot
// simply never copied it. Stated separately from the pin tests because it
// changes what every existing unpinned run reports, and a reader should not
// have to infer that from a test about pins.
func TestRunWorkflow_RecordsTheOrkerOfAnUnpinnedTaskThatFailed(t *testing.T) {
	const document = `
name: unpinned-orker-id
tasks:
  - id: doomed
    uses: shell
    with: {command: "exit 4"}
`
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, temporal.NewNonRetryableApplicationError(
			"workload exited 4", task.CategoryActivityFailed, nil,
			&orkstackv1.ActivityFailureDetail{
				ReasonCode: "CONTAINER_NONZERO_EXIT",
				ExitCode:   4,
				OrkerId:    "orker-7",
			})
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-unpinned-orker-id",
		compile(t, document), payload(t, nil), defaultPlan())

	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want the task failure")
	}
	stored, ok := h.store.Get("run-unpinned-orker-id")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	doomed := taskRunOf(t, stored, "doomed")
	if got := doomed.GetOrkerId(); got != "orker-7" {
		t.Errorf("TaskRun.orker_id = %q, want it read off the failure detail for an unpinned task too",
			got)
	}
	if got := doomed.GetPin(); got != "" {
		t.Errorf("TaskRun.pin = %q, want empty for an unpinned task", got)
	}
}

// upstreamRef finds one ancestor's contribution in a task's upstream list.
//
// The list is a slice rather than a map because it is proto, and a repeated
// field is what keeps the ordering deterministic for replay.
func upstreamRef(in *orkstackv1.ExecuteTaskInput, taskID string) *orkstackv1.UpstreamRef {
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == taskID {
			return ref
		}
	}
	return nil
}
