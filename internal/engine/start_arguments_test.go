package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// RunWorkflow takes a fifth argument, the carry of a run that continued as
// new, and every START keeps passing four (spec 1.3). That is what keeps a
// start's payloads byte-identical to what every earlier release recorded: a
// fifth argument — even a nil one — is a fifth payload, which changes the
// start event, and with a nil carry it would also be the first thing a
// carried path could mistake for one. The committed fixtures prove the
// recorded starts (TestCommittedFixtures_StartWithFourArgumentsAndNoCarry);
// these prove the three live start sites.

// childStartArgs records how many arguments each child start of RunWorkflow
// passed, as the SDK received them — before conversion, one per payload.
type childStartArgs struct {
	mu     sync.Mutex
	counts []int
}

func (c *childStartArgs) seen() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.counts...)
}

type childStartArgsInterceptor struct {
	interceptor.WorkerInterceptorBase
	capture *childStartArgs
}

func (w *childStartArgsInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &childStartArgsInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, capture: w.capture}
}

type childStartArgsInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	capture *childStartArgs
}

func (i *childStartArgsInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&childStartArgsOutbound{
		WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound},
		capture:                         i.capture,
	})
}

type childStartArgsOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	capture *childStartArgs
}

func (o *childStartArgsOutbound) ExecuteChildWorkflow(ctx workflow.Context, childType string, args ...any) workflow.ChildWorkflowFuture {
	if childType == engine.RunWorkflowType {
		o.capture.mu.Lock()
		o.capture.counts = append(o.capture.counts, len(args))
		o.capture.mu.Unlock()
	}
	return o.Next.ExecuteChildWorkflow(ctx, childType, args...)
}

func TestStartRun_PassesExactlyFourArguments(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, task.DefaultQueue, task.DefaultQueue)
	if err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil, defaultPlan()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if len(c.args) != 4 {
		t.Fatalf("StartRun passed %d arguments to RunWorkflow, want exactly 4 (run id, orkflow, input, plan): %v",
			len(c.args), c.args)
	}
}

func TestStartChild_PassesExactlyFourArguments(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	capture := &childStartArgs{}
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&childStartArgsInterceptor{capture: capture}}}))
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got := capture.seen(); len(got) != 1 || got[0] != 4 {
		t.Fatalf("child starts passed %v arguments, want one start of exactly 4", got)
	}
}

func TestScheduleFire_PassesExactlyFourArguments(t *testing.T) {
	h := newFireHarness(t)
	capture := &childStartArgs{}
	// The fire harness already intercepts the fire's own child start; this
	// one counts its arguments alongside.
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{
		&fireWorkerInterceptor{capture: h.capture}, &childStartArgsInterceptor{capture: capture},
	}}))
	h.scheduled(t, nightly)
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return firePreparation(t, "ork-20260924-1"), nil
	})
	h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
		return nil
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("fire failed: %v", err)
	}
	if got := capture.seen(); len(got) != 1 || got[0] != 4 {
		t.Fatalf("the fire's run start passed %v arguments, want one start of exactly 4", got)
	}
}
