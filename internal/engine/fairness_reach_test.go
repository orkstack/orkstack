package engine_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The reach of a run's fairness (docs/DEPLOY.md "Fair dispatch across
// namespaces"): every activity a run dispatches onto an Orker's channel
// carries the plan's key and weight — a task, every item of a fan-out, every
// member of a pin and each pin's creation request — in every generation of a
// run that carries on.

// dispatchCapture records the options of every activity a workflow schedules,
// by activity type, and every version the code asks with its answer. It sees
// a context after every interceptor listed after it has acted, as an
// outbound call runs the interceptors in reverse order of the list.
type dispatchCapture struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	options map[string][]workflow.ActivityOptions
	asked   map[string][]workflow.Version
}

func (c *dispatchCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &dispatchInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: c}
}

type dispatchInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *dispatchCapture
}

func (i *dispatchInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&dispatchOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type dispatchOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *dispatchCapture
}

func (o *dispatchOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.root.mu.Lock()
	if o.root.options == nil {
		o.root.options = map[string][]workflow.ActivityOptions{}
	}
	o.root.options[activityType] = append(o.root.options[activityType], workflow.GetActivityOptions(ctx))
	o.root.mu.Unlock()
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *dispatchOutbound) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	v := o.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
	o.root.mu.Lock()
	if o.root.asked == nil {
		o.root.asked = map[string][]workflow.Version{}
	}
	o.root.asked[changeID] = append(o.root.asked[changeID], v)
	o.root.mu.Unlock()
	return v
}

func (c *dispatchCapture) of(activityType string) []workflow.ActivityOptions {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]workflow.ActivityOptions(nil), c.options[activityType]...)
}

func (c *dispatchCapture) versionsAsked(change string) []workflow.Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]workflow.Version(nil), c.asked[change]...)
}

// requireKeyed fails unless there are at least minimum option sets and every
// one carries want.
func requireKeyed(t *testing.T, what string, got []workflow.ActivityOptions, minimum int, want temporal.Priority) {
	t.Helper()
	if len(got) < minimum {
		t.Fatalf("captured %d %s, want at least %d", len(got), what, minimum)
	}
	for i, options := range got {
		if options.Priority != want {
			t.Fatalf("%s %d was scheduled with priority %+v, want %+v", what, i, options.Priority, want)
		}
	}
}

func fairPlan() engine.RunPlan {
	plan := defaultPlan()
	plan.Fairness = &engine.FairnessSelection{Key: "payments", Weight: 3, PinRequests: true}
	return plan
}

var paymentsPriority = temporal.Priority{FairnessKey: "payments", FairnessWeight: 3}

// TestRunWorkflow_SchedulesEveryItemWithThePlansFairness: a fan-out's items
// are dispatched through the task's own activity options, so each carries
// the run's key and weight, and the gate is asked once for the run.
func TestRunWorkflow_SchedulesEveryItemWithThePlansFairness(t *testing.T) {
	capture := &dispatchCapture{}
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, itemBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-fair-items",
		compile(t, spreadDocument), spreadInput(t, 6), fairPlan(), (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if items := len(itemsDispatched(h)); items != 6 {
		t.Fatalf("%d items ran, want 6", items)
	}
	requireKeyed(t, "ExecuteTask (task or item)", capture.of(task.ActivityExecuteTask), 7, paymentsPriority)
	if asked := capture.versionsAsked(engine.FairnessKeysChange); len(asked) != 1 || asked[0] != 1 {
		t.Fatalf("%s was answered %v, want one question answered 1", engine.FairnessKeysChange, asked)
	}
}

// TestRunWorkflow_SchedulesPinnedMembersWithThePlansFairness: a pinned
// member's options keep the plan's priority when the SDK moves the member
// onto its pin's own channel, which it does by changing the task queue alone.
func TestRunWorkflow_SchedulesPinnedMembersWithThePlansFairness(t *testing.T) {
	capture := runPinned(t, pinnedDocument, fairPlan())
	requireKeyed(t, "ExecuteTask", capture.of(task.ActivityExecuteTask), 3, paymentsPriority)
}

// TestRunWorkflow_APinsCreationRequestCarriesThePlansFairness: the request
// that opens a pin waits for its pool's pin hosts beside every other
// namespace's, so it carries the run's key and weight too. The SDK builds that
// request's options itself, so the run worker's interceptor sets the priority
// the workflow decided (engine.RunWorkerInterceptors). The gate is asked once,
// where the first pin opens.
func TestRunWorkflow_APinsCreationRequestCarriesThePlansFairness(t *testing.T) {
	capture := runPinned(t, twoPinsDocument, fairPlan())
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 2, paymentsPriority)
	if asked := capture.versionsAsked(engine.FairnessPinCreationChange); len(asked) != 1 || asked[0] != 1 {
		t.Fatalf("%s was answered %v, want one question answered 1", engine.FairnessPinCreationChange, asked)
	}
}

// TestRunWorkflow_AGateOffPinCreationCarriesNoFairness is the DefaultVersion
// branch, the command sequence of every build before the gate: the creation
// request carries no priority, whatever the plan says, and no version is
// asked for it. The members keep theirs, which fairness-keys decides.
func TestRunWorkflow_AGateOffPinCreationCarriesNoFairness(t *testing.T) {
	defer engine.DisableFairnessPinCreationGateForTest()()
	capture := runPinned(t, twoPinsDocument, fairPlan())
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 2, temporal.Priority{})
	requireKeyed(t, "ExecuteTask", capture.of(task.ActivityExecuteTask), 3, paymentsPriority)
	if asked := capture.versionsAsked(engine.FairnessPinCreationChange); len(asked) != 0 {
		t.Fatalf("a gate-off run asked %s %v", engine.FairnessPinCreationChange, asked)
	}
}

// TestRunWorkflow_APlanWithoutFairnessKeysNoPinCreation: a plan built before
// RunPlan.Fairness carries none, so its pins open with no priority and the
// run asks neither gate.
func TestRunWorkflow_APlanWithoutFairnessKeysNoPinCreation(t *testing.T) {
	capture := runPinned(t, twoPinsDocument, defaultPlan())
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 2, temporal.Priority{})
	for _, change := range []string{engine.FairnessKeysChange, engine.FairnessPinCreationChange} {
		if asked := capture.versionsAsked(change); len(asked) != 0 {
			t.Fatalf("a plan without fairness asked %s %v", change, asked)
		}
	}
}

// TestRunWorkflow_ASelectionFromBeforePinRequestsKeysNoPinCreation: a plan a
// build before FairnessSelection.PinRequests submitted carries a selection
// without it, so the run's tasks keep their key while its pins open unkeyed
// and it never asks the gate — a run in flight when the gate shipped never
// changes how it opens pins, whatever build carries it on.
func TestRunWorkflow_ASelectionFromBeforePinRequestsKeysNoPinCreation(t *testing.T) {
	plan := fairPlan()
	plan.Fairness.PinRequests = false
	capture := runPinned(t, twoPinsDocument, plan)
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 2, temporal.Priority{})
	requireKeyed(t, "ExecuteTask", capture.of(task.ActivityExecuteTask), 3, paymentsPriority)
	if asked := capture.versionsAsked(engine.FairnessPinCreationChange); len(asked) != 0 {
		t.Fatalf("a selection without pin_requests asked %s %v", engine.FairnessPinCreationChange, asked)
	}
}

func runPinned(t *testing.T, document string, plan engine.RunPlan) *dispatchCapture {
	t.Helper()
	capture := &dispatchCapture{}
	h := newPinHarness(t, &testsuite.WorkflowTestSuite{}, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}, nil)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{
		EnableSessionWorker: true,
		Interceptors:        []interceptor.WorkerInterceptor{capture},
	}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-fair-pinned",
		compile(t, document), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	return capture
}

// carriedFairDocument has a pin, a fan-out and a second pin in a chain, so a
// run that continues at every idle point dispatches a pin's creation, its
// member, items and plain tasks in more than one generation.
const carriedFairDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: carried-fair
inputs:
  parts: {type: array}
pins:
  first: {}
  second: {}
tasks:
  - id: a
    uses: shell
    pin: first
    run_as_orker_user: true
    with: {command: "echo a"}
  - id: gap
    uses: shell
    needs: [a]
    with: {command: "echo gap"}
  - id: spread
    uses: shell
    needs: [gap]
    each: {over: "{{ .inputs.parts }}", max_items: 10, max_parallel: 2}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: b
    uses: shell
    pin: second
    needs: [spread]
    run_as_orker_user: true
    with: {command: "echo b"}
`

// TestRunWorkflow_ACarriedGenerationKeepsThePlansFairness: a run that carries
// on keeps its plan, and its version decisions cross in the carry, so every
// generation dispatches its tasks, items, members and pin creation requests
// with the same key and weight, and asks neither gate again.
func TestRunWorkflow_ACarriedGenerationKeepsThePlansFairness(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	capture := &dispatchCapture{}
	plan := fairPlan()
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
		MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget

	var perGeneration []*dispatchCapture
	g := runGenerations(t, "run-carried-fair", compile(t, carriedFairDocument), spreadInput(t, 4), plan, itemBehavior,
		func(h *harness, _ int) {
			own := &dispatchCapture{}
			perGeneration = append(perGeneration, own)
			enablePins(h, worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture, own}})
		})
	if g.err != nil {
		t.Fatalf("the run ended with %v", g.err)
	}
	if len(g.carries) == 0 {
		t.Fatal("the run never carried on, so nothing crossed a generation")
	}
	// The second pin opens in a carried generation: what that generation
	// dispatched is keyed from its carried plan and decisions alone.
	carried := perGeneration[len(perGeneration)-1]
	requireKeyed(t, "carried generation's pin creation request", carried.of(sessionCreationActivityName), 1, paymentsPriority)
	requireKeyed(t, "carried generation's ExecuteTask", carried.of(task.ActivityExecuteTask), 1, paymentsPriority)
	last := g.carries[len(g.carries)-1].GetVersionDecisions()
	for _, change := range []string{engine.FairnessKeysChange, engine.FairnessPinCreationChange} {
		if got := last[change]; got != 1 {
			t.Errorf("the last carry holds %s = %d, want the decision 1 (carry %v)", change, got, last)
		}
		if asked := capture.versionsAsked(change); len(asked) != 1 {
			t.Errorf("%s was asked %v across the generations, want exactly one question", change, asked)
		}
	}
	requireKeyed(t, "ExecuteTask (task, item or member)", capture.of(task.ActivityExecuteTask), 7, paymentsPriority)
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 2, paymentsPriority)
	for i, carried := range g.plans {
		if carried.Fairness == nil || *carried.Fairness != *plan.Fairness {
			t.Fatalf("generation %d started with fairness %+v, want the run's %+v", i+1, carried.Fairness, plan.Fairness)
		}
	}
}
