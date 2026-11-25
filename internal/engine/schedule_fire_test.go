package engine_test

import (
	"context"
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
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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
	plan, err := stablejson.Marshal(firePlan())
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
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	capture := &fireCapture{memo: map[string]any{}}
	env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&fireWorkerInterceptor{capture: capture}}}))
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
	must.OKf(t, h.env.GetWorkflowError(), "fire failed")
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
	stamp, ok := opts.Memo[engine.MemoStartStamp].(string)
	if !ok {
		t.Fatalf("the fire's run carries no start stamp: %v", opts.Memo)
	}
	if want := engine.RunStartMemoAt(prep.GetOrkflow(), firePlan(), mustTime(t, stamp)); !reflect.DeepEqual(opts.Memo, want) {
		t.Fatalf("child memo = %v, want RunStartMemoAt's %v", opts.Memo, want)
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
		must.OKf(t, h.env.GetWorkflowError(), "fire failed after a transient outage")
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
				must.OKf(t, err, "fire failed although its run succeeded")
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

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// firePreparationWith is firePreparation whose plan holds a run-level
// concurrency slot of strategy on key db:prod.
func firePreparationWith(t testing.TB, runID, strategy string) *orkstackv1.ScheduledRunPreparation {
	prep := firePreparation(t, runID)
	plan := firePlan()
	run := 0
	plan.Concurrency = &engine.ConcurrencyPlan{Run: &run, Slots: []engine.ConcurrencySlotPlan{{
		GateID: "g", Key: "db:prod", Label: "db:{{ .inputs.env }}", Limit: 1, Strategy: strategy, QueueTimeoutSeconds: 60}}}
	raw, err := stablejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	prep.Plan = raw
	return prep
}

// TestScheduleFireWorkflow_AConcurrencyRefusalCompletesTheFire: a preparation
// that started no run because of a concurrency limit (a skip limit's full
// key, a full line) completes the fire with the record's code and text, so
// the schedule does not pause; it asks once and starts no child.
func TestScheduleFireWorkflow_AConcurrencyRefusalCompletesTheFire(t *testing.T) {
	for _, tc := range []engine.FireNotStarted{
		{Code: engine.FireSkippedConcurrencyCode, Message: "key db:prod was full; this fire started no run"},
		{Code: engine.FireNotStartedConcurrencyCode, Message: "this fire started no run: CONCURRENCY_QUEUE_FULL (cause=max_queued): the line is full"},
	} {
		t.Run(tc.Code, func(t *testing.T) {
			h := newFireHarness(t)
			h.scheduled(t, nightly)
			calls := 0
			h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
				calls++
				return nil, engine.NewFireNotStarted(tc)
			})
			h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
				return nil
			})
			h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
			must.OKf(t, h.env.GetWorkflowError(), "a fire a limit kept from starting failed")
			if calls != 1 || len(h.capture.childOpts) != 0 {
				t.Fatalf("calls %d, children %d; want 1, 0", calls, len(h.capture.childOpts))
			}
			if got := h.capture.memoString(engine.MemoScheduleFireSkipCode); got != tc.Code {
				t.Fatalf("memo skip code = %q, want %q", got, tc.Code)
			}
			if got := h.capture.memoString(engine.MemoScheduleFireSkipMessage); got != tc.Message {
				t.Fatalf("memo skip message = %q, want %q", got, tc.Message)
			}
			if got := h.capture.memoString(engine.MemoScheduleFireRefusal); got != "" {
				t.Fatalf("memo refusal = %q, want none", got)
			}
		})
	}
}

// TestScheduleFireWorkflow_AnUnavailableCheckAsksAgainThenCompletes: a
// concurrency check that cannot be made is asked again for the window its
// preparation names; a check that answers in it starts the run, and one
// that never does completes the fire FIRE_CONCURRENCY_UNAVAILABLE.
func TestScheduleFireWorkflow_AnUnavailableCheckAsksAgainThenCompletes(t *testing.T) {
	unavailable := engine.FireNotStarted{Code: engine.FireConcurrencyUnavailableCode,
		Message: "CONCURRENCY_CHECK_UNAVAILABLE (scope=key): the line for key db:prod could not be read", RetryFor: 5 * time.Minute}
	t.Run("answers in the window", func(t *testing.T) {
		h := newFireHarness(t)
		h.scheduled(t, nightly)
		calls := 0
		h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
			calls++
			if calls < 3 {
				return nil, engine.NewFireNotStarted(unavailable)
			}
			return firePreparation(t, "ork-5"), nil
		})
		h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
			return nil
		})
		h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
		must.OKf(t, h.env.GetWorkflowError(), "fire")
		if calls != 3 || len(h.capture.childOpts) != 1 {
			t.Fatalf("calls %d, children %d; want 3, 1", calls, len(h.capture.childOpts))
		}
		if got := h.capture.memoString(engine.MemoScheduleFireSkipCode); got != "" {
			t.Fatalf("memo skip code = %q on a fire that started its run", got)
		}
	})
	t.Run("never answers", func(t *testing.T) {
		h := newFireHarness(t)
		h.scheduled(t, nightly)
		calls := 0
		var first, last time.Time
		h.prepare(func(ctx context.Context, _ *orkstackv1.ScheduleRecord, _ time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
			calls++
			now := activity.GetInfo(ctx).StartedTime
			if first.IsZero() {
				first = now
			}
			last = now
			return nil, engine.NewFireNotStarted(unavailable)
		})
		h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
			return nil
		})
		h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
		must.OKf(t, h.env.GetWorkflowError(), "a fire whose limit never answered failed")
		if calls < 3 || len(h.capture.childOpts) != 0 {
			t.Fatalf("calls %d, children %d; want several, 0", calls, len(h.capture.childOpts))
		}
		if span := last.Sub(first); span > unavailable.RetryFor {
			t.Fatalf("asked for %s, beyond the %s window", span, unavailable.RetryFor)
		}
		if got := h.capture.memoString(engine.MemoScheduleFireSkipCode); got != engine.FireConcurrencyUnavailableCode {
			t.Fatalf("memo skip code = %q, want %s", got, engine.FireConcurrencyUnavailableCode)
		}
		msg := h.capture.memoString(engine.MemoScheduleFireSkipMessage)
		for _, want := range []string{"concurrency could not be checked for 5m0s", "could not be read", "this fire started no run"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("memo skip message = %q, want it to name %q", msg, want)
			}
		}
	})
}

// TestScheduleFireWorkflow_ConcurrencyEndingsOfTheRun: a run a limit ended
// before it started maps to the fire's outcome: a skip limit's full key is
// a skipped fire, a refuse limit's a refused one, and a queued run that
// never started (its queue timed out, its line full, its limit not
// answering) completes the fire not started. Any other failure stays the
// run's own.
func TestScheduleFireWorkflow_ConcurrencyEndingsOfTheRun(t *testing.T) {
	for _, tc := range []struct {
		name, strategy, code, msg string
		wantSkip, wantRefused     string
	}{
		{name: "skip, key full", strategy: engine.ConcurrencyStrategySkip, code: engine.RunConcurrencyLimitCode,
			msg: "key db:prod already has its limit of 1 runs", wantSkip: engine.FireSkippedConcurrencyCode},
		{name: "refuse, key full", strategy: engine.ConcurrencyStrategyRefuse, code: engine.RunConcurrencyLimitCode,
			msg: "key db:prod already has its limit of 1 runs", wantRefused: "key db:prod already has its limit"},
		{name: "queue timed out", strategy: engine.ConcurrencyStrategyQueue, code: engine.RunQueueTimeoutCode,
			msg: "waited 1m0s for key db:prod's slot without starting", wantSkip: engine.FireNotStartedConcurrencyCode},
		{name: "line full", strategy: engine.ConcurrencyStrategyQueue, code: engine.RunConcurrencyFullCode,
			msg: "the line for key db:prod is full", wantSkip: engine.FireNotStartedConcurrencyCode},
		{name: "limit not answering", strategy: engine.ConcurrencyStrategyQueue, code: engine.RunConcurrencyUnavailableCode,
			msg: "the limit on key db:prod did not answer", wantSkip: engine.FireNotStartedConcurrencyCode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFireHarness(t)
			h.scheduled(t, nightly)
			h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
				return firePreparationWith(t, "ork-6", tc.strategy), nil
			})
			h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
				return temporal.NewNonRetryableApplicationError(tc.msg, tc.code, nil, engine.ConcurrencyRunNotStarted{NotStarted: true})
			})
			h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
			err := h.env.GetWorkflowError()
			if tc.wantRefused != "" {
				assertRefused(t, err, tc.wantRefused)
				return
			}
			must.OKf(t, err, "a fire whose run a limit kept from starting failed")
			if got := h.capture.memoString(engine.MemoScheduleFireSkipCode); got != tc.wantSkip {
				t.Fatalf("memo skip code = %q, want %q", got, tc.wantSkip)
			}
			if got := h.capture.memoString(engine.MemoScheduleFireSkipMessage); !strings.Contains(got, tc.msg) {
				t.Fatalf("memo skip message = %q, want it to carry %q", got, tc.msg)
			}
			if got := h.capture.memoString(engine.MemoScheduledRunID); got != "ork-6" {
				t.Fatalf("memo run id = %q, want ork-6", got)
			}
		})
	}
	// A run that ran and then lost its slot (after a pause) fails its fire
	// with its own reason, though the code is a limit's.
	h := newFireHarness(t)
	h.scheduled(t, nightly)
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return firePreparationWith(t, "ork-9", engine.ConcurrencyStrategyQueue), nil
	})
	h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
		return temporal.NewNonRetryableApplicationError("the limit on key db:prod did not answer", engine.RunConcurrencyUnavailableCode, nil)
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	if err := h.env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "did not answer") {
		t.Fatalf("fire error = %v, want the run's own failure", err)
	}
	if got := h.capture.memoString(engine.MemoScheduleFireSkipCode); got != "" {
		t.Fatalf("memo skip code = %q for a run that ran", got)
	}

	h = newFireHarness(t)
	h.scheduled(t, nightly)
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return firePreparationWith(t, "ork-7", engine.ConcurrencyStrategySkip), nil
	})
	h.child(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) error {
		return temporal.NewApplicationError("task sync failed", "ActivityFailed")
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	if err := h.env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "task sync failed") {
		t.Fatalf("fire error = %v, want the run's own failure", err)
	}
}

// TestScheduleFireWorkflow_StartsARunWithAConcurrencyLimit: a fire decodes a
// prepared plan whose concurrency limit carries durations (written by
// stablejson as nanoseconds) and starts the run with the plan intact.
func TestScheduleFireWorkflow_StartsARunWithAConcurrencyLimit(t *testing.T) {
	h := newFireHarness(t)
	h.scheduled(t, nightly)
	prep := firePreparation(t, "ork-8")
	plan := firePlan()
	run := 0
	plan.Concurrency = &engine.ConcurrencyPlan{Run: &run, AcquireTimeout: 2 * time.Minute, RequeueInterval: time.Minute,
		Slots: []engine.ConcurrencySlotPlan{{GateID: "g", Key: "db:prod", Label: "db:prod", Limit: 1,
			Strategy: engine.ConcurrencyStrategyQueue, QueueTimeoutSeconds: 60}}}
	raw, err := stablejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	prep.Plan = raw
	h.prepare(func(context.Context, *orkstackv1.ScheduleRecord, time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		return prep, nil
	})
	var got engine.RunPlan
	h.child(func(_ workflow.Context, _ string, _ *orkstackv1.Orkflow, _ *orkstackv1.Payload, p engine.RunPlan) error {
		got = p
		return nil
	})
	h.env.ExecuteWorkflow(engine.ScheduleFireWorkflowType, fireRecord())
	must.OKf(t, h.env.GetWorkflowError(), "a fire of a limited Orkflow")
	if got.Concurrency == nil || got.Concurrency.AcquireTimeout != 2*time.Minute || got.Concurrency.RequeueInterval != time.Minute {
		t.Fatalf("the run's plan concurrency = %+v; want the prepared durations", got.Concurrency)
	}
}
