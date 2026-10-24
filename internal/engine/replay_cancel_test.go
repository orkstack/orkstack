package engine_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// cancelBeforeDispatchFixture is the cancel-before-dispatch fixture: a run
// whose operator cancel landed in the same workflow task as the completion
// that made its next task ready (TestIntegration_RecordCancelBeforeDispatchHistory,
// testdata/history/README.md).
const cancelBeforeDispatchFixture = "testdata/history/run-cancel-before-dispatch-v1.json"

func requireCancelBeforeDispatchFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(cancelBeforeDispatchFixture); err != nil {
		const hint = "record it with ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ " +
			"-run TestIntegration_RecordCancelBeforeDispatchHistory"
		if os.Getenv("ORK_REQUIRE_CANCEL_FIXTURE") != "" {
			t.Fatalf("the cancel-before-dispatch fixture %s is missing (%v); %s", cancelBeforeDispatchFixture, err, hint)
		}
		t.Skipf("%s is not recorded (%v): %s", cancelBeforeDispatchFixture, err, hint)
	}
}

// TestRunWorkflow_ReplaysCommittedCancelBeforeDispatchHistory is the command
// half: today's body reproduces the recorded run's switch refused by the
// server, its lazy cancel-before-dispatch marker at the next dispatch, and
// the canceled ending with nothing more dispatched.
func TestRunWorkflow_ReplaysCommittedCancelBeforeDispatchHistory(t *testing.T) {
	requireCancelBeforeDispatchFixture(t)
	recorded := loadHistoryFile(t, cancelBeforeDispatchFixture)
	runID, _, _, _, _ := startArgs(t, recorded)
	if err := replayAs(t, recorded, runID); err != nil {
		t.Fatalf("replaying %s against current code failed: %v", cancelBeforeDispatchFixture, err)
	}
}

// TestCancelBeforeDispatchHistory_CatchesTheCheckRemoved is the negative
// test: replayed with the check skipped — the builds before it, which went
// on to dispatch — the fixture must fail, so it is known to see the path it
// guards.
func TestCancelBeforeDispatchHistory_CatchesTheCheckRemoved(t *testing.T) {
	requireCancelBeforeDispatchFixture(t)
	recorded := loadHistoryFile(t, cancelBeforeDispatchFixture)
	runID, _, _, _, _ := startArgs(t, recorded)
	restore := engine.DisableCancelBeforeDispatchForTest()
	defer restore()
	if err := replayAs(t, recorded, runID); err == nil {
		t.Fatal("the fixture replayed with the cancel-before-dispatch check removed: it does not pin the path")
	}
}

// TestCancelBeforeDispatchHistory_PayloadsMatchWhatCurrentCodeSends is the
// payload half. The run is re-executed from the fixture's own arguments with
// its recorded results; its history is raised past its budget once the last
// task the recorded run completed has succeeded, so it switches where the
// recorded run did, and the operator's cancel lands during that switch's
// checkpoint write, as it did on the cluster. Every ExecuteTask input and
// every published event — the canceled tail and the run's own RUN_CANCELED
// included — equals the recorded one.
func TestCancelBeforeDispatchHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireCancelBeforeDispatchFixture(t)
	recorded := loadHistoryFile(t, cancelBeforeDispatchFixture)
	runID, wf, input, plan, _ := startArgs(t, recorded)
	inputs, results := recordedItemPayloads(t, recorded)
	last := ""
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		var in *orkstackv1.ExecuteTaskInput
		if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
			t.Fatal(err)
		}
		last = scheduleKey(in)
	}
	if last == "" || plan.Continuation == nil || plan.History == nil {
		t.Fatalf("the fixture dispatched nothing or carries no budget (last %q)", last)
	}

	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if result, ok := results[scheduleKey(in)]; ok {
			return result, nil
		}
		return nil, errors.New("current code dispatched " + scheduleKey(in) + ", which the recorded run never did")
	})
	h.env.SetStartTime(startedAt(recorded))
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
	// A test environment's history length stands still and the commands it
	// counts pile up, so the recorded budget would cross early here: the
	// re-execution decides with a budget only the raised length crosses —
	// numbers no payload carries.
	budget := *plan.Continuation
	budget.Events, budget.RetryEvents = 1<<20, 1<<18
	budget.Bytes, budget.RetryBytes = 1<<40, 1<<38
	limit := *plan.History
	limit.MaxEvents, limit.MaxBytes = 1<<30, 1<<50
	plan.Continuation, plan.History = &budget, &limit
	raiseHistoryWhen(h, 1<<21, taskStatusIs(last, orkstackv1.Status_STATUS_SUCCEEDED))
	var once sync.Once
	h.onContinuation = func(kind string, _ int64) error {
		if kind == "checkpoint" {
			once.Do(h.env.CancelWorkflow)
		}
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflowType, runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); !temporal.IsCanceledError(errors.Unwrap(err)) && !temporal.IsCanceledError(err) {
		t.Fatalf("the re-execution ended %v, want canceled", err)
	}
	for _, call := range h.calls {
		key := scheduleKey(call)
		if diff := payloadDiff(inputs[key], canonicalPayload(t, call)); diff != "" {
			t.Errorf("the ExecuteTask input for %q differs:\n%s", key, diff)
		}
	}
	if len(h.calls) != len(inputs) {
		t.Errorf("current code dispatched %d tasks, the recorded run %d", len(h.calls), len(inputs))
	}
	want := publishedEvents(t, recorded, 0)
	got := h.bus.Events(runID, 0)
	if len(got) != len(want) {
		t.Fatalf("current code published %d events, the recorded run %d", len(got), len(want))
	}
	for _, event := range got {
		if diff := payloadDiff(want[event.GetSequence()], canonicalPayload(t, event)); diff != "" {
			t.Errorf("event %d differs:\n%s", event.GetSequence(), diff)
		}
	}
}
