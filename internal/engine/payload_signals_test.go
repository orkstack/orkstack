package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// unreadableEvents is the run's RESOLUTION_UNREADABLE events.
func unreadableEvents(h *harness, runID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range h.bus.Events(runID, 0) {
		if e.GetMessage() == engine.ResolutionUnreadableMessage {
			out = append(out, e)
		}
	}
	return out
}

// A resolution that cannot be read is an event of the run, with no task
// id, and its await stays open: a readable one sent after it resolves it.
func TestRunWorkflow_AnUnreadableResolutionIsAnEvent(t *testing.T) {
	s := newSealedRun(t, "ork-await")
	cases := []struct {
		name       string
		sealed     bool
		unreadable *payloadcrypt.Scope // what the first resolution is sealed under
		readable   *payloadcrypt.Scope // and the second
	}{
		{"marked sealed in a run that is not", false, s.scope, payloadcrypt.Unsealed()},
		{"sealed for a sibling in a sealed run", true, s.scope.For(payloadcrypt.RunContext("ork-sibling")), s.scope},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, okTask)
			memoForReader := map[string]any{}
			if tc.sealed {
				memoForReader[payloadcrypt.MemoField] = s.memo
			}
			dc := payloadcrypt.NewConverter(engine.DataConverter(), s.keys).WithReaderKeys(treeReader(s.keys, memoForReader))
			h.env.SetDataConverter(dc)
			h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{payloadcrypt.WorkflowInterceptor(s.keys)}}))
			h.env.SetStartWorkflowOptions(clientOptions("ork-await"))
			wf := compile(t, awaitedDocument)
			if tc.sealed {
				memo := engine.RunStartMemo(wf, defaultPlan())
				memo[payloadcrypt.MemoField] = s.memo
				must.OK(t, h.env.SetMemoOnStart(memo))
				sealedOrker(h, s.scope)
			}
			body := `{"model_uri": "s3://models/1"}`
			h.env.RegisterDelayedCallback(func() {
				h.env.SignalWorkflow(engine.ResolveAwaitSignal, payloadcrypt.Seal(tc.unreadable, completion("b", body, "mallory")))
			}, time.Minute)
			h.env.RegisterDelayedCallback(func() {
				h.env.SignalWorkflow(engine.ResolveAwaitSignal, payloadcrypt.Seal(tc.readable, completion("b", body, "alice")))
			}, 2*time.Minute)
			args := []any{"ork-await", wf, payload(t, nil), defaultPlan()}
			for i := range args {
				args[i] = payloadcrypt.Seal(map[bool]*payloadcrypt.Scope{true: s.scope, false: payloadcrypt.Unsealed()}[tc.sealed], args[i])
			}
			h.env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, rawOptions())
			h.env.ExecuteWorkflow(rawRunWorkflow, args...)
			must.OKf(t, h.env.GetWorkflowError(), "workflow error")
			got := unreadableEvents(h, "ork-await")
			if len(got) != 1 || got[0].GetTaskId() != "" || got[0].GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_LOG {
				t.Fatalf("RESOLUTION_UNREADABLE events = %v, want one TASK_LOG with no task id", got)
			}
			resolved := eventsFor(h, "ork-await", "b")
			if types := strings.Join(eventTypes(resolved), ","); types != "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED" {
				t.Fatalf("b's events = %s: the unreadable resolution changed the await", types)
			}
			if !strings.Contains(resolved[1].GetMessage(), "alice") {
				t.Fatalf("b was resolved by %q, want the readable resolution", resolved[1].GetMessage())
			}
			var result converter.RawValue
			must.OK(t, h.env.GetWorkflowResult(&result))
		})
	}
}

// A child of a sealed tree is started with the tree's key field and its
// start sealed for its own id; its result, sealed for its own id, is opened
// by the parent with the tree's key.
func TestRunWorkflow_ASealedTreesChild(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	s := newSealedRun(t, "ork-parent")
	dc := payloadcrypt.NewConverter(engine.DataConverter(), s.keys).
		WithReaderKeys(treeReader(s.keys, map[string]any{payloadcrypt.MemoField: s.memo}))
	h.env.SetDataConverter(dc)
	childMemo := &childMemoRecorder{}
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{childMemo, payloadcrypt.WorkflowInterceptor(s.keys)}}))
	memo := engine.RunStartMemo(parent, plan)
	memo[payloadcrypt.MemoField] = s.memo
	must.OK(t, h.env.SetMemoOnStart(memo))
	sealedComposeOrker(h, s.scope)
	cp := plan.Children["release"]
	childScope := s.scope.For(payloadcrypt.RunContext(cp.RunID))
	var sawStart bool
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, runIDArg, wfArg, inputArg, planArg converter.RawValue) (any, error) {
		var runID string
		present, err := payloadcrypt.OpenRaw(childScope, engine.DataConverter(), runIDArg, &runID)
		sawStart = err == nil && present && runID == cp.RunID && payloadcrypt.IsSealed(wfArg.Payload()) &&
			payloadcrypt.IsSealed(inputArg.Payload()) && payloadcrypt.IsSealed(planArg.Payload())
		return payloadcrypt.Seal(childScope, childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"})), nil
	}, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, workflow.RegisterOptions{Name: "SealedParent"})
	h.env.SetStartWorkflowOptions(clientOptions("ork-parent"))
	h.env.ExecuteWorkflow("SealedParent", payloadcrypt.Seal(s.scope, "ork-parent"), payloadcrypt.Seal(s.scope, parent),
		payloadcrypt.Seal(s.scope, payload(t, nil)), payloadcrypt.Seal(s.scope, plan))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if !childMemo.sawKey || !sawStart {
		t.Fatalf("the child was started with its key field %v and a start sealed for its own id %v", childMemo.sawKey, sawStart)
	}
	run, _ := h.store.Get("ork-parent")
	if row := taskRunOfRun(run, "release"); row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
		row.GetOutput().GetData().AsMap()["digest"] != "sha256:abc" {
		t.Fatalf("release row = %v: the parent did not open its child's result", row)
	}
}

// childMemoRecorder records whether a child was started with the tree's key
// field in its memo (the test environment hands a child no memo of its own).
type childMemoRecorder struct {
	interceptor.WorkerInterceptorBase
	sawKey bool
}

func (r *childMemoRecorder) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &childMemoInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, r: r}
}

type childMemoInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	r *childMemoRecorder
}

func (i *childMemoInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&childMemoOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, r: i.r})
}

type childMemoOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	r *childMemoRecorder
}

func (o *childMemoOutbound) ExecuteChildWorkflow(ctx workflow.Context, childWorkflowType string, args ...any) workflow.ChildWorkflowFuture {
	if _, ok := workflow.GetChildWorkflowOptions(ctx).Memo[payloadcrypt.MemoField]; ok {
		o.r.sawKey = true
	}
	return o.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

// A sealed tree's child that fails is read with the child's own key and id:
// the parent records the child's real reason, not PAYLOAD_UNREADABLE.
func TestRunWorkflow_ASealedTreesChildFailure(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	s := newSealedRun(t, "ork-parent")
	dc := payloadcrypt.NewConverter(engine.DataConverter(), s.keys).
		WithReaderKeys(treeReader(s.keys, map[string]any{payloadcrypt.MemoField: s.memo}))
	h.env.SetDataConverter(dc)
	h.env.SetFailureConverter(payloadcrypt.NewFailureConverter(dc, false))
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{payloadcrypt.WorkflowInterceptor(s.keys)}}))
	memo := engine.RunStartMemo(parent, plan)
	memo[payloadcrypt.MemoField] = s.memo
	must.OK(t, h.env.SetMemoOnStart(memo))
	sealedComposeOrker(h, s.scope)
	cp := plan.Children["release"]
	childScope := s.scope.For(payloadcrypt.RunContext(cp.RunID))
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _, _, _, _ converter.RawValue) (any, error) {
		return nil, payloadcrypt.WithKey(temporal.NewNonRetryableApplicationError("child canary", engine.RunTimedOutCode, nil),
			childScope, engine.RunTimedOutCode)
	}, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, workflow.RegisterOptions{Name: "SealedParent"})
	h.env.SetStartWorkflowOptions(clientOptions("ork-parent"))
	h.env.ExecuteWorkflow("SealedParent", payloadcrypt.Seal(s.scope, "ork-parent"), payloadcrypt.Seal(s.scope, parent),
		payloadcrypt.Seal(s.scope, payload(t, nil)), payloadcrypt.Seal(s.scope, plan))
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT {
		t.Fatalf("release row error = %v, want the child's own timeout", row.GetError())
	}
}

// sealedComposeOrker answers the parent's tasks as an Orker with the run
// tree's key does, with the harness's default behaviour.
func sealedComposeOrker(h *composeHarness, scope *payloadcrypt.Scope) {
	h.env.RegisterActivityWithOptions(func(ctx context.Context, raw converter.RawValue) (any, error) {
		var in *orkstackv1.ExecuteTaskInput
		if _, err := payloadcrypt.OpenRaw(scope, engine.DataConverter(), raw, &in); err != nil {
			return nil, err
		}
		h.mu.Lock()
		h.calls = append(h.calls, in)
		h.mu.Unlock()
		res, err := defaultBehavior(h, in)
		if res == nil {
			return nil, err
		}
		return payloadcrypt.Seal(scope, res), err
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask, DisableAlreadyRegisteredCheck: true})
}
