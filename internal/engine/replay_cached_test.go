package engine_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// cachedHistory is the caching fixture: the SECOND run of
// examples/cached-pipeline.ork.yaml — featurize a hit, stamp uncached, train a
// miss — recorded by a versioned worker against a real cluster by
// TestIntegration_RecordCachedRunHistory, never hand-edited
// (testdata/history/README.md). It holds still the first CacheContext an
// ExecuteTask input ever carried, the first ExecuteTaskResult.cache, and the
// first "(cached from run …)" event.
const cachedHistory = "testdata/history/run-cached-v1.json"

// requireCachedFixture skips while the fixture has not been recorded yet. It
// can only be produced against a real cluster; once committed this
// never skips again. ORK_REQUIRE_CACHED_FIXTURE makes its absence a failure.
func requireCachedFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(cachedHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_CACHED_FIXTURE") != "" {
			t.Fatalf("the cached replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordCachedRunHistory", cachedHistory, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordCachedRunHistory", cachedHistory, err)
	}
}

func TestRunWorkflow_ReplaysCommittedCachedHistory(t *testing.T) {
	requireCachedFixture(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, cachedHistory); err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A run with a cached task that was in flight when this build was deployed would fail its "+
			"next workflow task. Restore the command stream, or re-record and say why.", cachedHistory, err)
	}
}

// reexecuteCached re-executes RunWorkflow from the fixture's own input,
// feeding back its recorded results, and returns what this code sent each
// task, every event it published, and the run it finished with.
func reexecuteCached(t *testing.T, recorded *historypb.History) (map[string]map[string]any, []map[string]any, map[string]any) {
	t.Helper()
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	_, _, recordedResults, _ := goldenActivityPayloads(t, recorded)
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
		t.Fatal("re-executing the fixture's own input did not complete the workflow")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("re-executing the fixture's own input failed: %v", err)
	}
	inputs := map[string]map[string]any{}
	for _, in := range sentTasks {
		inputs[in.TaskId] = canonicalPayload(t, in)
	}
	events := make([]map[string]any, 0, len(sentEvent))
	for _, event := range sentEvent {
		events = append(events, canonicalPayload(t, event))
	}
	var finished *orkstackv1.Run
	if err := env.GetWorkflowResult(&finished); err != nil {
		t.Fatalf("decode the re-executed run: %v", err)
	}
	return inputs, events, canonicalPayload(t, finished)
}

// cachedInputDiffs is compareTaskInputs as data: one line per task whose
// input differs from the recorded one.
func cachedInputDiffs(recorded, sent map[string]map[string]any) []string {
	var out []string
	for _, id := range sortedPayloadKeys(recorded) {
		got, ok := sent[id]
		if !ok {
			out = append(out, fmt.Sprintf("task %q: not executed", id))
			continue
		}
		if diff := payloadDiff(recorded[id], got); diff != "" {
			out = append(out, fmt.Sprintf("task %q:\n%s", id, diff))
		}
	}
	return out
}

func TestCachedHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireCachedFixture(t)
	recorded := loadHistoryFile(t, cachedHistory)
	recordedInputs, recordedEvents, _, recordedRun := goldenActivityPayloads(t, recorded)
	// The fixture is what it claims to be: a cached task's input carries a
	// CacheContext and an uncached one does not.
	for id, want := range map[string]bool{"featurize": true, "stamp": false, "train": true} {
		if _, has := recordedInputs[id]["cache"]; has != want {
			t.Fatalf("the fixture's %s input has cache=%v, want %v", id, has, want)
		}
	}
	sent, events, run := reexecuteCached(t, recorded)
	for _, diff := range cachedInputDiffs(recordedInputs, sent) {
		t.Errorf("the ExecuteTask input is not the one the recorded build sent: %s", diff)
	}
	compareEventPayloads(t, recordedEvents, events)
	if diff := payloadDiff(compactRecordedRun(t, recordedRun), run); diff != "" {
		t.Errorf("the Run this code returns no longer matches the recorded one:\n%s", diff)
	}
}

// TestCachedHistory_PayloadCheckCatchesADroppedCacheContext is what makes the
// cached fixture worth having the day it is recorded: the same comparison,
// against a body whose cacheContextFor returns nil, MUST find a difference.
func TestCachedHistory_PayloadCheckCatchesADroppedCacheContext(t *testing.T) {
	requireCachedFixture(t)
	restore := engine.DropCacheContextForTest()
	defer restore()
	recorded := loadHistoryFile(t, cachedHistory)
	recordedInputs, _, _, _ := goldenActivityPayloads(t, recorded)
	sent, _, _ := reexecuteCached(t, recorded)
	diffs := cachedInputDiffs(recordedInputs, sent)
	if len(diffs) == 0 {
		t.Fatal("a body that drops the CacheContext sent the recorded inputs: the payload check cannot see it")
	}
	joined := strings.Join(diffs, "\n")
	if !strings.Contains(joined, "featurize") || !strings.Contains(joined, "cache") {
		t.Fatalf("the check saw a difference, but not the dropped cache: %s", joined)
	}
}
