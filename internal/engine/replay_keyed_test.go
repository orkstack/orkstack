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

// keyedHistory is the keyed-run fixture: a real history, recorded by
// TestIntegration_RecordKeyedRunHistory on a versioned worker, of a run
// started under an idempotency key. It pins the plan shape such a run
// starts with — IdempotencyFingerprint included — for every later replay:
// the field is one workflow code never reads, and this is what holds it to
// that.
const keyedHistory = "testdata/history/run-keyed-v1.json"

// keyedFixtureFingerprint is the fingerprint the recorder put on the plan.
const keyedFixtureFingerprint = "6b65796564207265706c617920666978747572652066696e6765727072696e74"

func requireKeyedFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(keyedHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_KEYED_FIXTURES") != "" {
			t.Fatalf("the keyed replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordKeyedRunHistory", keyedHistory, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordKeyedRunHistory", keyedHistory, err)
	}
}

// The command half: a keyed run's history replays, so a run started under a
// key before a deploy finishes after it.
func TestRunWorkflow_ReplaysCommittedKeyedHistory(t *testing.T) {
	requireKeyedFixture(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, keyedHistory); err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A run started under an idempotency key before this build was deployed would fail "+
			"its next workflow task. Workflow code must not read the plan's fingerprint.", keyedHistory, err)
	}
}

// The fixture is the shape it was recorded for: its start carries the
// fingerprint in the plan, where a retry under the key reads it.
func TestKeyedHistory_StartCarriesTheFingerprint(t *testing.T) {
	requireKeyedFixture(t)
	_, _, _, plan := goldenWorkflowArgs(t, loadHistoryFile(t, keyedHistory))
	if plan.IdempotencyFingerprint != keyedFixtureFingerprint {
		t.Fatalf("the fixture's plan carries fingerprint %q, want %q",
			plan.IdempotencyFingerprint, keyedFixtureFingerprint)
	}
}

// The payload half: re-execute the fixture from its own input and compare
// every payload current code sends with what the recorded build sent.
func TestKeyedHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireKeyedFixture(t)
	recorded := loadHistoryFile(t, keyedHistory)
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
		t.Fatal("re-executing the keyed fixture did not complete the workflow")
	}
	must.OKf(t, env.GetWorkflowError(), "re-executing the keyed fixture failed")

	sentTaskInputs := map[string]map[string]any{}
	for _, in := range sentTasks {
		sentTaskInputs[in.TaskId] = withoutAwaitClocks(canonicalPayload(t, in)).(map[string]any)
	}
	normalizedRecordedInputs := map[string]map[string]any{}
	for id, in := range recordedTaskInputs {
		normalizedRecordedInputs[id] = withoutAwaitClocks(in).(map[string]any)
	}
	compareTaskInputs(t, normalizedRecordedInputs, sentTaskInputs)

	normalize := func(in []map[string]any) []map[string]any {
		out := make([]map[string]any, 0, len(in))
		for _, m := range in {
			out = append(out, withoutAwaitClocks(m).(map[string]any))
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
	want := withoutAwaitClocks(compactRecordedRun(t, recordedRun)).(map[string]any)
	got := withoutAwaitClocks(canonicalPayload(t, finished)).(map[string]any)
	if diff := payloadDiff(want, got); diff != "" {
		t.Errorf("the Run this code returns for the keyed fixture differs from the recorded one:\n%s", diff)
	}
}
