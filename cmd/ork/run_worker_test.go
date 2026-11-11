package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// pinCreationPriorities records the priority of every pin creation request a
// workflow schedules. Listed before a worker's own interceptors it sees the
// request after they have acted (an outbound call runs the list in reverse),
// which is the priority the cluster is given.
type pinCreationPriorities struct {
	interceptor.WorkerInterceptorBase
	mu  sync.Mutex
	got []temporal.Priority
}

// sessionCreationActivity is the SDK's own name for a pin's creation request
// (internal/engine/fairness.go names it the same way).
const sessionCreationActivity = "internalSessionCreationActivity"

func (c *pinCreationPriorities) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &creationInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: c}
}

type creationInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *pinCreationPriorities
}

func (i *creationInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&creationOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type creationOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *pinCreationPriorities
}

func (o *creationOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if activityType == sessionCreationActivity {
		o.root.mu.Lock()
		o.root.got = append(o.root.got, workflow.GetActivityOptions(ctx).Priority)
		o.root.mu.Unlock()
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (c *pinCreationPriorities) priorities() []temporal.Priority {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]temporal.Priority(nil), c.got...)
}

// pinnedOrkflow has two tasks sharing one pin, so a run opens a pin.
const pinnedOrkflow = `
apiVersion: orkstack/v1
kind: Orkflow
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
`

// TestRunWorkerOptions_CarryAPlansFairnessToItsPinsCreationRequest: a worker
// built from the options this binary builds its run worker with opens a
// pin with the run's fairness key and weight on the creation request. The SDK
// builds that request's options itself, with no priority and no way to pass
// one, so the key reaches the cluster only through the worker's interceptors
// (engine.RunWorkerInterceptors); a worker built without them opens every pin
// with no fairness key. The run is driven through the workflow test
// environment with the activities mocked.
func TestRunWorkerOptions_CarryAPlansFairnessToItsPinsCreationRequest(t *testing.T) {
	options := runWorkerOptions(config{})
	options.EnableSessionWorker = true
	options.DeadlockDetectionTimeout = time.Minute
	capture := &pinCreationPriorities{}
	options.Interceptors = append([]interceptor.WorkerInterceptor{capture}, options.Interceptors...)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(options)
	env.SetTestTimeout(time.Minute)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.Run, int64) (string, error) { return "mem://snapshot", nil },
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId: in.TaskId, ValuesJson: []byte(`{"ok":true}`), ValuesInlined: true, Attempt: 1,
				ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://values/" + in.TaskId,
					Sha256: "0000000000000000000000000000000000000000000000000000000000000000", SizeBytes: 11,
					ContentType: "application/json"},
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})

	route, err := task.RouteForPool(task.DefaultPool, task.DefaultQueue)
	must.OK(t, err)
	plan := engine.RunPlan{
		Namespace: "default",
		Routes:    map[string]string{route.Pool: route.Queue},
		Fairness:  &engine.FairnessSelection{Key: "payments", Weight: 3, PinRequests: true},
	}
	definition, err := dsl.Compile([]byte(pinnedOrkflow))
	must.OKf(t, err, "compile")
	env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-fairness", definition, &orkstackv1.Payload{}, plan,
		(*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("the run did not complete")
	}
	must.OKf(t, env.GetWorkflowError(), "workflow error")

	want := temporal.Priority{FairnessKey: "payments", FairnessWeight: 3}
	got := capture.priorities()
	if len(got) == 0 {
		t.Fatal("the run opened no pin: no creation request was scheduled")
	}
	for i, priority := range got {
		if priority != want {
			t.Errorf("pin creation request %d was scheduled with priority %+v, want %+v", i, priority, want)
		}
	}
}
