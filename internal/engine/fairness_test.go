package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

func TestFairnessKeyFor_FitsTemporalsBoundAndKeepsLongNamesApart(t *testing.T) {
	if got := engine.FairnessKeyFor("payments"); got != "payments" {
		t.Fatalf("FairnessKeyFor(payments) = %q, want the name", got)
	}
	long := strings.Repeat("a", 190)
	one, two := engine.FairnessKeyFor(long+"-one"), engine.FairnessKeyFor(long+"-two")
	for _, key := range []string{one, two} {
		if len(key) != 64 || !strings.HasPrefix(key, "aaaa") {
			t.Fatalf("FairnessKeyFor(long) = %q (%d bytes), want 64 bytes with a readable prefix", key, len(key))
		}
	}
	if one == two || engine.FairnessKeyFor(long+"-one") != one {
		t.Fatalf("two long names share a key, or one name has two: %q %q", one, two)
	}
}

func TestPoolResolver_PinsTheNamespacesFairnessInThePlan(t *testing.T) {
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}}
	_, plan, err := resolver.Plan(context.Background(),
		&orkstackv1.Namespace{Name: "payments", FairShareWeight: 3}, compile(t, gpuOnlyDocument), false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Fairness == nil || plan.Fairness.Key != "payments" || plan.Fairness.Weight != 3 {
		t.Fatalf("plan.Fairness = %+v, want payments at weight 3", plan.Fairness)
	}
}

// fairnessCapture records the priority every ExecuteTask is scheduled with,
// and the versions the fairness-keys gate is answered.
type fairnessCapture struct {
	interceptor.WorkerInterceptorBase
	mu         sync.Mutex
	priorities []temporal.Priority
	asked      []workflow.Version
}

func (c *fairnessCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &fairnessInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: c}
}

type fairnessInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *fairnessCapture
}

func (i *fairnessInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&fairnessOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type fairnessOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *fairnessCapture
}

func (o *fairnessOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if activityType == task.ActivityExecuteTask {
		o.root.mu.Lock()
		o.root.priorities = append(o.root.priorities, workflow.GetActivityOptions(ctx).Priority)
		o.root.mu.Unlock()
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *fairnessOutbound) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	v := o.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
	if changeID == engine.FairnessKeysChange {
		o.root.mu.Lock()
		o.root.asked = append(o.root.asked, v)
		o.root.mu.Unlock()
	}
	return v
}

func runCapturingFairness(t *testing.T, plan engine.RunPlan) *fairnessCapture {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	capture := &fairnessCapture{}
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-fairness",
		compile(t, linearDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	return capture
}

func TestRunWorkflow_SchedulesEveryTaskWithThePlansFairness(t *testing.T) {
	plan := defaultPlan()
	plan.Fairness = &engine.FairnessSelection{Key: "payments", Weight: 3}
	capture := runCapturingFairness(t, plan)
	if len(capture.priorities) < 2 {
		t.Fatalf("captured %d task activities, want every task of the run", len(capture.priorities))
	}
	for _, p := range capture.priorities {
		if p.FairnessKey != "payments" || p.FairnessWeight != 3 {
			t.Fatalf("a task was scheduled with %+v, want payments at weight 3", p)
		}
	}
	if len(capture.asked) != 1 || capture.asked[0] != 1 {
		t.Fatalf("%s was answered %v, want one question answered 1", engine.FairnessKeysChange, capture.asked)
	}
}

func TestRunWorkflow_APlanWithoutFairnessAsksNothingAndSetsNoPriority(t *testing.T) {
	capture := runCapturingFairness(t, defaultPlan())
	for _, p := range capture.priorities {
		if p != (temporal.Priority{}) {
			t.Fatalf("a run whose plan carries no fairness scheduled %+v", p)
		}
	}
	if len(capture.asked) != 0 {
		t.Fatalf("%s was asked %v by a plan without fairness", engine.FairnessKeysChange, capture.asked)
	}
}

func TestRunWorkflow_AGateOffRunSetsNoPriority(t *testing.T) {
	defer engine.DisableFairnessKeysGateForTest()()
	plan := defaultPlan()
	plan.Fairness = &engine.FairnessSelection{Key: "payments"}
	capture := runCapturingFairness(t, plan)
	for _, p := range capture.priorities {
		if p != (temporal.Priority{}) {
			t.Fatalf("a gate-off run scheduled %+v", p)
		}
	}
}
