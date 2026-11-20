package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// A run whose plan names a run-context object sends a run context with every
// task and every fan-out item, each with its own scheduled time, and the
// run's snapshot says so.
func TestRunWorkflow_EveryTaskAndItemCarriesTheRunContext(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, partsJSON(3))(ctx, in)
	})
	sha := strings.Repeat("e", 64)
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	plan.RunContextSHA256 = sha
	plan.RunContextMaxBytes = engine.DefaultRunContextMaxBytes
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-rc", compile(t, fmt.Sprintf(eachTaskDocument, "")), payload(t, nil), plan,
		(*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get("run-rc")
	if !ok || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v", run)
	}
	if run.GetRunContext().GetStatus() != orkstackv1.RunContextStatus_RUN_CONTEXT_STATUS_AVAILABLE {
		t.Fatalf("run context state = %v", run.GetRunContext())
	}
	h.mu.Lock()
	calls := append([]*orkstackv1.ExecuteTaskInput(nil), h.calls...)
	h.mu.Unlock()
	if n := len(h.itemCalls("convert")); n != 3 || len(calls) <= n {
		t.Fatalf("%d activities, %d of them items: want three items and the other tasks", len(calls), n)
	}
	for _, in := range calls {
		rc := in.GetRunContext()
		if rc.GetObjectSha256() != sha || rc.GetCreatedAt() == nil || rc.GetStartedAt() == nil ||
			rc.GetTaskScheduledAt() == nil || rc.GetDeclaredMaxAttempts() != 1 {
			t.Fatalf("task %s item %d: run context = %v", in.GetTaskId(), in.GetEach().GetIndex(), rc)
		}
	}
	node := taskRunOf(t, run, "convert")
	for _, in := range h.itemCalls("convert") {
		item := node.GetItems()[in.GetEach().GetIndex()]
		if !in.GetRunContext().GetTaskScheduledAt().AsTime().Equal(item.GetScheduledAt().AsTime()) {
			t.Fatalf("item %d: scheduled %v, row says %v", in.GetEach().GetIndex(),
				in.GetRunContext().GetTaskScheduledAt().AsTime(), item.GetScheduledAt().AsTime())
		}
	}
}
