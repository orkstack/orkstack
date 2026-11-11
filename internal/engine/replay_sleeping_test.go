package engine_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sleepingHistory is the sleeping-run fixture: a real history recorded by a
// versioned worker against a real cluster by
// TestIntegration_RecordSleepingRunHistory, never hand-edited
// (testdata/history/README.md). It holds the sleep path — a Timer with no
// Signal, whose firing is the task's success — still.
const sleepingHistory = "testdata/history/run-sleeping-v1.json"

// requireSleepingFixture skips while the fixture has not been recorded yet,
// and fails instead under ORK_REQUIRE_SLEEPING_FIXTURES.
func requireSleepingFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(sleepingHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_SLEEPING_FIXTURES") != "" {
			t.Fatalf("the sleeping replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordSleepingRunHistory", sleepingHistory, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordSleepingRunHistory", sleepingHistory, err)
	}
}

func TestRunWorkflow_ReplaysCommittedSleepingHistory(t *testing.T) {
	requireSleepingFixture(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name: engine.RunWorkflowType,
	})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, sleepingHistory); err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A run that was sleeping when this build was deployed would fail its next "+
			"workflow task. Restore the sleep command stream, or re-record and say why.", sleepingHistory, err)
	}
}

// withoutSleepClocks is withoutAwaitClocks plus the sleep's own clock, the
// TaskSleep.wake_at field: a re-execution can never reproduce the recorded
// wall clock it was computed from.
func withoutSleepClocks(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			if key == "wakeAt" {
				continue
			}
			out[key] = withoutSleepClocks(inner)
		}
		return withoutAwaitClocks(out)
	case []any:
		out := make([]any, 0, len(typed))
		for _, inner := range typed {
			out = append(out, withoutSleepClocks(inner))
		}
		return out
	default:
		return withoutAwaitClocks(value)
	}
}

// TestSleepingHistory_ActivityPayloadsMatchWhatCurrentCodeSends is the payload
// half for the sleep path: re-execute the fixture from its own input, feed
// back its recorded activity results, and compare every payload current code
// sends — task inputs, events, the finished run — with what the recorded
// build sent. The test environment fires the timer at once; the sleep's
// clocks are the only fields set aside.
func TestSleepingHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireSleepingFixture(t)
	recorded := loadHistoryFile(t, sleepingHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	recordedTaskInputs, recordedEvents, recordedResults, recordedRun := goldenActivityPayloads(t, recorded)

	var (
		mu        sync.Mutex
		sentTasks []*orkstackv1.ExecuteTaskInput
		sentEvent []*orkstackv1.Event
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sentTasks = append(sentTasks, in)
			result, ok := recordedResults[in.TaskId]
			if !ok {
				return nil, fmt.Errorf("current code ran task %q, which the recorded run never executed", in.TaskId)
			}
			return result, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, event *orkstackv1.Event, _ string, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			sentEvent = append(sentEvent, event)
			return nil
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput, plan, (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("re-executing the sleeping fixture did not complete the workflow")
	}
	must.OKf(t, env.GetWorkflowError(), "re-executing the sleeping fixture failed")

	sentTaskInputs := map[string]map[string]any{}
	for _, in := range sentTasks {
		sentTaskInputs[in.TaskId] = withoutSleepClocks(canonicalPayload(t, in)).(map[string]any)
	}
	normalizedRecordedInputs := map[string]map[string]any{}
	for id, in := range recordedTaskInputs {
		normalizedRecordedInputs[id] = withoutSleepClocks(in).(map[string]any)
	}
	compareTaskInputs(t, normalizedRecordedInputs, sentTaskInputs)

	normalize := func(in []map[string]any) []map[string]any {
		out := make([]map[string]any, 0, len(in))
		for _, m := range in {
			out = append(out, withoutSleepClocks(m).(map[string]any))
		}
		return out
	}
	sent := make([]map[string]any, 0, len(sentEvent))
	for _, event := range sentEvent {
		sent = append(sent, canonicalPayload(t, event))
	}
	compareEventPayloads(t, normalize(recordedEvents), normalize(sent))

	var finished *orkstackv1.Run
	must.OKf(t, env.GetWorkflowResult(&finished), "decode the re-executed run")
	want := withoutSleepClocks(compactRecordedRun(t, recordedRun)).(map[string]any)
	got := withoutSleepClocks(canonicalPayload(t, finished)).(map[string]any)
	if diff := payloadDiff(want, got); diff != "" {
		t.Errorf("the Run this code returns for the sleeping fixture differs from the recorded one:\n%s", diff)
	}
}
