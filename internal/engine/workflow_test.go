package engine_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/orkstack/orkstack/pkg/plugin"
)

// harness wires a TestWorkflowEnvironment with a mocked ExecuteTask activity
// and a real (in-process) event projection activity so tests can assert on the
// resulting run read model and event stream.
type harness struct {
	env   *testsuite.TestWorkflowEnvironment
	bus   *engine.EventBus
	store *engine.RunStore

	mu    sync.Mutex
	calls []task.ExecuteTaskInput
}

func newHarness(t *testing.T, suite *testsuite.WorkflowTestSuite, behavior func(task.ExecuteTaskInput) (task.ExecuteTaskResult, error)) *harness {
	t.Helper()
	h := &harness{
		env:   suite.NewTestWorkflowEnvironment(),
		bus:   engine.NewEventBus(),
		store: engine.NewRunStore(),
	}
	h.env.SetTestTimeout(60 * time.Second)
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.WorkflowName})
	h.env.RegisterActivityWithOptions(
		engine.NewEventActivities(h.bus, h.store).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	h.env.RegisterActivityWithOptions(
		func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			h.mu.Lock()
			h.calls = append(h.calls, in)
			h.mu.Unlock()
			return behavior(in)
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	t.Cleanup(func() { h.env.AssertExpectations(t) })
	return h
}

func (h *harness) invocations() []task.ExecuteTaskInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]task.ExecuteTaskInput(nil), h.calls...)
}

func (h *harness) taskOrder() []string {
	out := []string{}
	for _, call := range h.invocations() {
		out = append(out, call.TaskID)
	}
	return out
}

func (h *harness) inputFor(taskID string) (task.ExecuteTaskInput, bool) {
	for _, call := range h.invocations() {
		if call.TaskID == taskID {
			return call, true
		}
	}
	return task.ExecuteTaskInput{}, false
}

func compile(t *testing.T, document string) *orkstackv1.Workflow {
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
    plugin: shell
    with: {command: "echo alpha"}
  - id: beta
    plugin: shell
    needs: [alpha]
    with: {command: "echo beta"}
  - id: gamma
    plugin: shell
    needs: [beta]
    with: {command: "echo gamma"}
`

func TestRunWorkflow_ExecutesLinearNeedsInOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{
			TaskID: in.TaskID,
			Output: plugin.Payload{"stdout": in.TaskID},
		}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-linear", compile(t, linearDocument), payload(t, nil))

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
	if len(events) == 0 {
		t.Fatal("no events published")
	}
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		t.Errorf("first event = %v, want RUN_STARTED", events[0].GetType())
	}
	var previous int64
	for _, event := range events {
		if event.GetSequence() <= previous {
			t.Fatalf("event sequences not strictly increasing: %d after %d", event.GetSequence(), previous)
		}
		previous = event.GetSequence()
	}
}

const fanOutDocument = `
name: fanout
tasks:
  - id: left
    plugin: shell
    with: {command: "echo left"}
  - id: right
    plugin: shell
    with: {command: "echo right"}
  - id: join
    plugin: shell
    needs: [left, right]
    with: {command: "echo join"}
`

func TestRunWorkflow_FansOutIndependentTasks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite

	started := make(chan string, 2)
	// Each independent task blocks until the other has started. If the engine
	// serialized them, neither would ever be released and the barrier would
	// time out.
	barrier := func(in task.ExecuteTaskInput) error {
		if in.TaskID == "join" {
			return nil
		}
		started <- in.TaskID
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

	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		if err := barrier(in); err != nil {
			return task.ExecuteTaskResult{}, err
		}
		return task.ExecuteTaskResult{TaskID: in.TaskID, Output: plugin.Payload{"stdout": in.TaskID}}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-fanout", compile(t, fanOutDocument), payload(t, nil))

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
    plugin: http
    with: {url: "https://example.test"}
  - id: transform
    plugin: shell
    needs: [fetch]
    with:
      command: "cat {{ artifact \"fetch/body\" }}"
  - id: save
    plugin: file
    needs: [transform]
    with:
      content: "{{ .tasks.fetch.output.status }}"
`

func TestRunWorkflow_PassesUpstreamOutputAndArtifactsDownstream(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		switch in.TaskID {
		case "fetch":
			return task.ExecuteTaskResult{
				TaskID: "fetch",
				Output: plugin.Payload{"status": float64(200)},
				Artifacts: []plugin.ArtifactRef{{
					Name:        "body",
					URI:         "s3://bucket/run/fetch/body",
					ContentType: "application/json",
					SizeBytes:   17,
					SHA256:      "abc123",
				}},
			}, nil
		default:
			return task.ExecuteTaskResult{TaskID: in.TaskID, Output: plugin.Payload{"stdout": "ok"}}, nil
		}
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-upstream", compile(t, upstreamDocument), payload(t, nil))

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	transform, ok := h.inputFor("transform")
	if !ok {
		t.Fatal("transform task was never executed")
	}
	fetchResult, ok := transform.Upstream["fetch"]
	if !ok {
		t.Fatalf("transform upstream = %v, want a fetch entry", transform.Upstream)
	}
	if got := fetchResult.Output["status"]; got != float64(200) {
		t.Errorf("upstream fetch output status = %v, want 200", got)
	}
	if len(fetchResult.Artifacts) != 1 {
		t.Fatalf("upstream fetch artifacts = %v, want 1", fetchResult.Artifacts)
	}
	artifactRef := fetchResult.Artifacts[0]
	if artifactRef.Name != "body" || artifactRef.URI != "s3://bucket/run/fetch/body" ||
		artifactRef.ContentType != "application/json" || artifactRef.SizeBytes != 17 || artifactRef.SHA256 != "abc123" {
		t.Errorf("upstream artifact = %+v, want the ref produced by fetch", artifactRef)
	}
	if transform.With["command"] != "cat {{ artifact \"fetch/body\" }}" {
		t.Errorf("with was rendered by the engine: %v (rendering belongs to the Orker)", transform.With)
	}

	// save needs transform, which transitively needs fetch: both are visible.
	save, ok := h.inputFor("save")
	if !ok {
		t.Fatal("save task was never executed")
	}
	if _, ok := save.Upstream["fetch"]; !ok {
		t.Errorf("save upstream = %v, want the transitive fetch entry", save.Upstream)
	}
	if _, ok := save.Upstream["transform"]; !ok {
		t.Errorf("save upstream = %v, want the direct transform entry", save.Upstream)
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
    plugin: shell
    with: {command: "echo always"}
  - id: maybe
    plugin: shell
    needs: [always]
    condition: "{{ .inputs.enabled }}"
    with: {command: "echo maybe"}
  - id: after
    plugin: shell
    needs: [maybe]
    with: {command: "echo after"}
`

func TestRunWorkflow_SkipsTaskWithUnmetCondition(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID, Output: plugin.Payload{"stdout": in.TaskID}}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-condition",
		compile(t, conditionDocument), payload(t, map[string]any{"enabled": false}))

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
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID, Output: plugin.Payload{"stdout": in.TaskID}}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-condition-true",
		compile(t, conditionDocument), payload(t, map[string]any{"enabled": true}))

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
    plugin: shell
    timeout_seconds: 5
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 1.0
      maximum_interval_seconds: 1
    with: {command: "exit 1"}
  - id: downstream
    plugin: shell
    needs: [flaky]
    with: {command: "echo never"}
`

func TestRunWorkflow_RetriesPerPolicyThenFailsRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var attempts int32
	var mu sync.Mutex

	h := newHarness(t, &suite, func(task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{}, nil
	})
	// Explicit mock: assert the activity is retried exactly three times.
	h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return task.ExecuteTaskResult{}, errors.New("plugin exited 1")
		}).Times(3)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-retry", compile(t, retryDocument), payload(t, nil))

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

func TestRunWorkflow_SingleAttemptWhenNoRetryPolicy(t *testing.T) {
	const document = `
name: no-retry
tasks:
  - id: only
    plugin: shell
    timeout_seconds: 5
    with: {command: "exit 1"}
`
	var suite testsuite.WorkflowTestSuite
	var attempts int
	var mu sync.Mutex
	h := newHarness(t, &suite, func(task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return task.ExecuteTaskResult{}, errors.New("boom")
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-retry", compile(t, document), payload(t, nil))

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
    plugin: http
    with:
      url: "{{ .inputs.url }}"
`
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-default-input", compile(t, document), payload(t, nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	input, ok := h.inputFor("fetch")
	if !ok {
		t.Fatal("fetch task was not executed")
	}
	if input.RunInput["url"] != "https://default.test" {
		t.Errorf("ExecuteTaskInput.RunInput.url = %v, want declared default", input.RunInput["url"])
	}
}

func TestRunWorkflow_SchedulesChildWhenItsOwnNeedCompletes(t *testing.T) {
	const document = `
name: readiness-driven
tasks:
  - id: slow
    plugin: shell
    timeout_seconds: 5
  - id: fast
    plugin: shell
  - id: child
    plugin: shell
    needs: [fast]
`
	var suite testsuite.WorkflowTestSuite
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		switch in.TaskID {
		case "slow":
			select {
			case <-releaseSlow:
				return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
			case <-time.After(2 * time.Second):
				return task.ExecuteTaskResult{}, errors.New("child was not scheduled before unrelated slow task")
			}
		case "child":
			releaseOnce.Do(func() { close(releaseSlow) })
		}
		return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-readiness", compile(t, document), payload(t, nil))

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
