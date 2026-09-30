package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// fireCapture records what ScheduleFireWorkflow asked of the SDK: the memo it
// upserted and the options it started its child with.
type fireCapture struct {
	mu        sync.Mutex
	memo      map[string]any
	childOpts []workflow.ChildWorkflowOptions
	childType []string
}

func (c *fireCapture) memoString(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, _ := c.memo[key].(string)
	return v
}

type fireWorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
	capture *fireCapture
}

func (w *fireWorkerInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &fireInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, capture: w.capture}
}

type fireInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	capture *fireCapture
}

func (i *fireInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&fireOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, capture: i.capture})
}

type fireOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	capture *fireCapture
}

func (o *fireOutbound) UpsertMemo(ctx workflow.Context, memo map[string]any) error {
	if workflow.GetInfo(ctx).WorkflowType.Name == engine.ScheduleFireWorkflowType {
		o.capture.mu.Lock()
		for k, v := range memo {
			o.capture.memo[k] = v
		}
		o.capture.mu.Unlock()
	}
	return o.Next.UpsertMemo(ctx, memo)
}

func (o *fireOutbound) ExecuteChildWorkflow(ctx workflow.Context, childType string, args ...any) workflow.ChildWorkflowFuture {
	if workflow.GetInfo(ctx).WorkflowType.Name == engine.ScheduleFireWorkflowType {
		o.capture.mu.Lock()
		o.capture.childOpts = append(o.capture.childOpts, workflow.GetChildWorkflowOptions(ctx))
		o.capture.childType = append(o.capture.childType, childType)
		o.capture.mu.Unlock()
	}
	return o.Next.ExecuteChildWorkflow(ctx, childType, args...)
}

var nightly = time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)

func fireRecord() *orkstackv1.ScheduleRecord {
	return &orkstackv1.ScheduleRecord{
		ScheduleId: "sch-x", Name: "nightly", Namespace: "payments", OrkflowName: "reconcile",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
	}
}

func firePlan() engine.RunPlan {
	return engine.RunPlan{
		Routes: map[string]string{"main": "ork-tasks"},
		Origin: &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: "sch-x",
			Reference: "2026-09-24T01:00:00Z", Description: "nightly"},
	}
}

func firePreparation(t testing.TB, runID string) *orkstackv1.ScheduledRunPreparation {
	plan, err := json.Marshal(firePlan())
	if err != nil {
		t.Fatal(err)
	}
	input, _ := structpb.NewStruct(map[string]any{"mode": "full"})
	return &orkstackv1.ScheduledRunPreparation{
		Run: &orkstackv1.Run{Id: runID, Namespace: "payments", OrkflowName: "reconcile", OrkflowVersion: "3"},
		Orkflow: &orkstackv1.Orkflow{Name: "reconcile", Version: "3", Namespace: "payments",
			Tasks: []*orkstackv1.Task{{Id: "sync", Uses: "shell", Pool: "main"}}},
		Input: &orkstackv1.Payload{Data: input}, Plan: plan, NamespaceIndexed: true, OriginIndexed: true,
	}
}

type fireHarness struct {
	env     *testsuite.TestWorkflowEnvironment
	capture *fireCapture
}

func newFireHarness(t *testing.T) *fireHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	capture := &fireCapture{memo: map[string]any{}}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&fireWorkerInterceptor{capture: capture}}})
	env.RegisterWorkflowWithOptions(engine.ScheduleFireWorkflow, workflow.RegisterOptions{Name: engine.ScheduleFireWorkflowType})
	// The schedule's action starts the fire on the engine's run channel; the
	// child run is started on the fire's own channel.
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: task.DefaultQueue})
	return &fireHarness{env: env, capture: capture}
}

func (h *fireHarness) scheduled(t *testing.T, at time.Time) {
	t.Helper()
	if err := h.env.SetTypedSearchAttributesOnStart(temporal.NewSearchAttributes(
		task.SearchKeyTemporalScheduledStartTime.ValueSet(at))); err != nil {
		t.Fatal(err)
	}
}

func (h *fireHarness) prepare(fn any) {
	h.env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
}

func (h *fireHarness) child(fn any) {
	h.env.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: engine.RunWorkflowType})
}

func assertRefused(t *testing.T, err error, wantMsg string) {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("fire error = %v, want an ApplicationError", err)
	}
	if app.Type() != engine.ScheduleFireRefusedType || !app.NonRetryable() {
		t.Fatalf("fire error type %q non-retryable %v; want %s, true", app.Type(), app.NonRetryable(), engine.ScheduleFireRefusedType)
	}
	if !strings.Contains(app.Message(), wantMsg) {
		t.Fatalf("fire error message %q does not contain %q", app.Message(), wantMsg)
	}
}

func TestScheduleFireWorkflow_StartsChildRunWithScheduleOrigin(t *testing.T) {
	h := newFireHarness(t)
	h.scheduled(t, nightly)
	var gotRec *orkstackv1.ScheduleRecord
	var gotAt time.Time
	h.prepare(func(_ context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		gotRec, gotAt = rec, at
		return firePreparation(t, "ork-20260924-1"), nil
	})
	var childID string
	var childWf *orkstackv1.Orkflow
	var childPlan engine.RunPlan
	h.child(func(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, _ *orkstackv1.Payload, plan engine.RunPlan) error {
		childID, childWf, childPlan = runID, wf, plan
		if workflow.GetInfo(ctx).WorkflowExecution.ID != runID {
			return fmt.Errorf("child workflow id %q, run id %q", workflow.GetInfo(ctx).WorkflowExecution.ID, runID)
		}
		return nil
	})

	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("fire failed: %v", err)
	}
	if !proto.Equal(gotRec, fireRecord()) || !gotAt.Equal(nightly) {
		t.Fatalf("prepare got %v at %v; want the record at %v", gotRec, gotAt, nightly)
	}
	if len(h.capture.childOpts) != 1 || h.capture.childType[0] != engine.RunWorkflowType {
		t.Fatalf("children started: %v", h.capture.childType)
	}
	opts := h.capture.childOpts[0]
	if opts.WorkflowID != "ork-20260924-1" || opts.TaskQueue != task.DefaultQueue ||
		opts.ParentClosePolicy != enumspb.PARENT_CLOSE_POLICY_ABANDON || !opts.WaitForCancellation ||
		opts.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Fatalf("child options = %+v", opts)
	}
	prep := firePreparation(t, "ork-20260924-1")
	if !reflect.DeepEqual(opts.Memo, engine.RunStartMemo(prep.GetOrkflow(), firePlan())) {
		t.Fatalf("child memo = %v, want RunStartMemo's %v", opts.Memo, engine.RunStartMemo(prep.GetOrkflow(), firePlan()))
	}
	if opts.Memo[engine.MemoOriginKind] != "schedule" || opts.Memo[engine.MemoOriginID] != "sch-x" {
		t.Fatalf("child memo carries origin %v/%v, want schedule/sch-x", opts.Memo[engine.MemoOriginKind], opts.Memo[engine.MemoOriginID])
	}
	if opts.TypedSearchAttributes.Size() != 3 {
		t.Fatalf("child search attributes = %d, want namespace and origin kind/id", opts.TypedSearchAttributes.Size())
	}
	if childID != "ork-20260924-1" || !proto.Equal(childWf, prep.GetOrkflow()) {
		t.Fatalf("child args id %q wf %v", childID, childWf)
	}
	if !reflect.DeepEqual(childPlan, firePlan()) {
		t.Fatalf("child plan = %+v, want %+v", childPlan, firePlan())
	}
	if got := h.capture.memoString(engine.MemoScheduledRunID); got != "ork-20260924-1" {
		t.Fatalf("fire memo run id = %q", got)
	}
	if got := h.capture.memoString(engine.MemoScheduleFireRefusal); got != "" {
		t.Fatalf("fire memo refusal = %q on success", got)
	}
}

func TestScheduleFireWorkflow_MissingScheduledStartTimeRefuses(t *testing.T) {
	h := newFireHarness(t)
	calls := 0
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		calls++
		return firePreparation(t, "ork-1"), nil
	})
	h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
		return nil
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	assertRefused(t, h.env.GetWorkflowError(), "not started by a schedule")
	if calls != 0 || len(h.capture.childOpts) != 0 {
		t.Fatalf("prepare called %d times, %d children started", calls, len(h.capture.childOpts))
	}
	if got := h.capture.memoString(engine.MemoScheduleFireRefusal); got != "not started by a schedule" {
		t.Fatalf("memo refusal = %q", got)
	}
}

func TestScheduleFireWorkflow_RefusalSetsMemoAndFailsNonRetryably(t *testing.T) {
	h := newFireHarness(t)
	h.scheduled(t, nightly)
	const refusal = `orkflow "reconcile" version "3" in namespace "payments" not found`
	calls := 0
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		calls++
		return nil, temporal.NewNonRetryableApplicationError(refusal, engine.ScheduleFireRefusedType, nil)
	})
	h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
		return nil
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	assertRefused(t, h.env.GetWorkflowError(), refusal)
	if calls != 1 {
		t.Fatalf("a refusal was attempted %d times, want 1", calls)
	}
	if got := h.capture.memoString(engine.MemoScheduleFireRefusal); got != refusal {
		t.Fatalf("memo refusal = %q, want %q", got, refusal)
	}
	if len(h.capture.childOpts) != 0 {
		t.Fatalf("a refused fire started a child")
	}
}

func TestScheduleFireWorkflow_InfrastructureErrorRetriesThenFails(t *testing.T) {
	t.Run("recovers", func(t *testing.T) {
		h := newFireHarness(t)
		h.scheduled(t, nightly)
		calls := 0
		h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
			calls++
			if calls <= 3 {
				return nil, errors.New("object store unavailable")
			}
			return firePreparation(t, "ork-2"), nil
		})
		h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
			return nil
		})
		h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("fire failed after a transient outage: %v", err)
		}
		if calls != 4 || len(h.capture.childOpts) != 1 {
			t.Fatalf("calls %d, children %d; want 4, 1", calls, len(h.capture.childOpts))
		}
	})
	t.Run("gives up", func(t *testing.T) {
		h := newFireHarness(t)
		h.scheduled(t, nightly)
		calls := 0
		h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
			calls++
			return nil, fmt.Errorf("object store unavailable (attempt %d)", calls)
		})
		h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
			return nil
		})
		h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
		assertRefused(t, h.env.GetWorkflowError(), "object store unavailable")
		if calls < 2 {
			t.Fatalf("an outage was attempted %d times; the policy retries it", calls)
		}
		if got := h.capture.memoString(engine.MemoScheduleFireRefusal); !strings.Contains(got, "object store unavailable") {
			t.Fatalf("memo refusal = %q", got)
		}
		if len(h.capture.childOpts) != 0 {
			t.Fatalf("a fire that never prepared started a child")
		}
	})
}

func TestScheduleFireWorkflow_CancelPropagatesToChildAndWaits(t *testing.T) {
	h := newFireHarness(t)
	h.env.SetDetachedChildWait(false)
	h.scheduled(t, nightly)
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return firePreparation(t, "ork-3"), nil
	})
	var sawCancel, cleanedUp bool
	h.child(func(ctx workflow.Context, _ string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload, _ engine.RunPlan) error {
		err := workflow.Sleep(ctx, time.Hour)
		if !temporal.IsCanceledError(err) {
			return err
		}
		sawCancel = true
		// The grace period: cleanup on a disconnected context, as a run's
		// SIGTERM handling takes time after cancellation.
		cleanup, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.Sleep(cleanup, 30*time.Second)
		cleanedUp = true
		return err
	})
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	err := h.env.GetWorkflowError()
	if !temporal.IsCanceledError(err) {
		t.Fatalf("fire error = %v, want the child's CanceledError", err)
	}
	if !sawCancel {
		t.Fatalf("cancelling the fire did not cancel its run")
	}
	if !cleanedUp {
		t.Fatalf("the fire completed before its run had finished cancelling")
	}
}

func TestScheduleFireWorkflow_ChildOutcomeIsTheFireOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		childFn func() error
		wantErr string
	}{
		{"success", func() error { return nil }, ""},
		{"failure", func() error { return temporal.NewApplicationError("task sync failed", "ActivityFailed") }, "task sync failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFireHarness(t)
			h.scheduled(t, nightly)
			h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
				return firePreparation(t, "ork-4"), nil
			})
			h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
				return tc.childFn()
			})
			h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
			err := h.env.GetWorkflowError()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("fire failed although its run succeeded: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("fire error = %v, want the run's %q", err, tc.wantErr)
			}
			var app *temporal.ApplicationError
			if errors.As(err, &app) && app.Type() == engine.ScheduleFireRefusedType {
				t.Fatalf("a failed run was reported as a refused fire")
			}
		})
	}
}

// fireBesideRun is a harness workflow: it holds the run id "ork-dup" open
// with a child of its own, then runs a fire whose preparation names the same
// id, so the fire's child start collides.
func fireBesideRun(ctx workflow.Context) error {
	holder := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: "ork-dup", ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}), engine.RunWorkflowType, "ork-dup", (*orkstackv1.Orkflow)(nil), (*orkstackv1.Payload)(nil), engine.RunPlan{})
	if err := holder.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return err
	}
	fire := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: "fire-1",
		TypedSearchAttributes: temporal.NewSearchAttributes(
			task.SearchKeyTemporalScheduledStartTime.ValueSet(nightly)),
	}), engine.ScheduleFireWorkflowType, fireRecord())
	return fire.Get(ctx, nil)
}

func TestScheduleFireWorkflow_ChildStartFailureIsARefusal(t *testing.T) {
	h := newFireHarness(t)
	h.env.SetDetachedChildWait(false)
	h.env.RegisterWorkflow(fireBesideRun)
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return firePreparation(t, "ork-dup"), nil
	})
	h.child(func(ctx workflow.Context, _ string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload, _ engine.RunPlan) error {
		return workflow.Sleep(ctx, time.Hour)
	})
	h.env.ExecuteWorkflow(fireBesideRun)
	assertRefused(t, h.env.GetWorkflowError(), "start run ork-dup")
	if got := h.capture.memoString(engine.MemoScheduleFireRefusal); !strings.Contains(got, "start run ork-dup") {
		t.Fatalf("memo refusal = %q", got)
	}
}
