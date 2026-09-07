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
	"go.temporal.io/sdk/testsuite"
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
}

func newHarness(t *testing.T, suite *testsuite.WorkflowTestSuite, behavior func(*orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *harness {
	t.Helper()
	h := &harness{
		env:    suite.NewTestWorkflowEnvironment(),
		bus:    engine.NewEventBus(),
		store:  engine.NewRunStore(),
		queues: map[string]string{},
	}
	h.env.SetTestTimeout(60 * time.Second)
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
