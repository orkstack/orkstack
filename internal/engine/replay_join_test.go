package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// joinHistory is the join fixture: examples/branch-and-recover.ork.yaml with
// route=full, fail_deploy=true and smoke_seconds=120, recorded by a versioned
// worker against a real cluster by TestIntegration_RecordJoinRunHistory
// and never hand-edited (testdata/history/README.md). It holds still the
// first none_failed decision, the first handler dispatched after fail-fast
// with a JoinContext and a failed-need artifact ref, the first failing note
// and the first RUN_FAILED after handlers.
const joinHistory = "testdata/history/run-join-v1.json"

// requireJoinFixture skips while the fixture has not been recorded yet. It can
// only be produced against a real cluster; once committed this never
// skips again. ORK_REQUIRE_JOIN_FIXTURE makes its absence a failure.
func requireJoinFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(joinHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_JOIN_FIXTURE") != "" {
			t.Fatalf("the join replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordJoinRunHistory", joinHistory, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordJoinRunHistory", joinHistory, err)
	}
}

// replayJoin replays the fixture and returns the replay's error and the
// ExecuteTask schedule it produced (the each fixture's interceptor).
func replayJoin(t *testing.T) ([]string, error) {
	t.Helper()
	recorder := &scheduleRecorder{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewWorkflowReplayerWithOptions: %v", err)
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	err = replayer.ReplayWorkflowHistoryFromJSONFile(nil, joinHistory)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.scheduled...), err
}

func TestRunWorkflow_ReplaysCommittedJoinHistory(t *testing.T) {
	requireJoinFixture(t)
	scheduled, err := replayJoin(t)
	if err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A run whose failure handlers were running when this build was deployed would fail its "+
			"next workflow task. Restore the failing path's command stream, or re-record and say why.",
			joinHistory, err)
	}
	// The replayer schedules each activity once per command, and a retried
	// attempt is not a new command, so the recorded schedule is per command.
	recorded := recordedSchedule(t, loadHistoryFile(t, joinHistory))
	if fmt.Sprint(scheduled) != fmt.Sprint(recorded) {
		t.Fatalf("the replay dispatched %v; the recorded build dispatched %v", scheduled, recorded)
	}
}

func TestJoinHistory_CatchesAHandlerOnTheRunContext(t *testing.T) {
	requireJoinFixture(t)
	restore := engine.JoinHandlersOnRunContextForTest()
	defer restore()
	_, err := replayJoin(t)
	if err == nil || !strings.Contains(err.Error(), "TMPRL1100") {
		t.Fatalf("replaying with failure handlers on the run context = %v, want a TMPRL1100 "+
			"nondeterminism error: the fixture cannot see a handler fail-fast would cancel", err)
	}
}

// activityErrorPrefix is the SDK's decoration of an activity failure's text:
// its scheduled and started event ids and the worker identity are facts of
// the recording cluster, never of the code, so a re-execution cannot
// reproduce them.
var activityErrorPrefix = regexp.MustCompile(`activity error \(type: [^,]*, scheduledEventID: \d+, startedEventID: \d+, identity: [^)]*\): `)

// normalized is value as JSON with the activity error decoration removed.
func normalized(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	stripped := activityErrorPrefix.ReplaceAll(encoded, nil)
	var out map[string]any
	if err := json.Unmarshal(stripped, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// joinRecorded reads, from the join fixture: the ExecuteTask input the
// recorded build sent per task (its first attempt's), every Event it
// published, how each ExecuteTask command ended — its result, its failure, or
// its cancellation — and the run's terminal failure message.
func joinRecorded(t *testing.T, recorded *historypb.History) (
	inputs map[string]map[string]any,
	events []map[string]any,
	results map[string]*orkstackv1.ExecuteTaskResult,
	failures map[string]error,
	canceled map[string]bool,
	runFailure string,
) {
	t.Helper()
	inputs = map[string]map[string]any{}
	results = map[string]*orkstackv1.ExecuteTaskResult{}
	failures = map[string]error{}
	canceled = map[string]bool{}
	typeOf := map[int64]string{}
	taskOf := map[int64]string{}
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			name := attrs.GetActivityType().GetName()
			typeOf[event.GetEventId()] = name
			switch name {
			case task.ActivityExecuteTask:
				decoded := recordedPayload(t, attrs.GetInput(), 0)
				taskID, _ := decoded["taskId"].(string)
				taskOf[event.GetEventId()] = taskID
				inputs[taskID] = decoded
			case engine.ActivityPublishEvent:
				events = append(events, recordedPayload(t, attrs.GetInput(), 0))
			}
			continue
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil &&
			typeOf[attrs.GetScheduledEventId()] == task.ActivityExecuteTask {
			var result *orkstackv1.ExecuteTaskResult
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &result); err != nil {
				t.Fatalf("decode a recorded ExecuteTask result: %v", err)
			}
			results[taskOf[attrs.GetScheduledEventId()]] = result
			continue
		}
		if attrs := event.GetActivityTaskFailedEventAttributes(); attrs != nil &&
			typeOf[attrs.GetScheduledEventId()] == task.ActivityExecuteTask {
			failures[taskOf[attrs.GetScheduledEventId()]] =
				temporalFailureToError(attrs.GetFailure())
			continue
		}
		if attrs := event.GetActivityTaskCanceledEventAttributes(); attrs != nil &&
			typeOf[attrs.GetScheduledEventId()] == task.ActivityExecuteTask {
			canceled[taskOf[attrs.GetScheduledEventId()]] = true
			continue
		}
		if attrs := event.GetWorkflowExecutionFailedEventAttributes(); attrs != nil {
			// The whole chain, as the error the recorded workflow returned: the
			// top-level Failure's message is only "activity error" — what the
			// run failed WITH (deploy's CONTAINER_EXIT_NONZERO) is its cause.
			runFailure = temporalFailureToError(attrs.GetFailure()).Error()
		}
	}
	if len(inputs) == 0 || len(events) == 0 || runFailure == "" {
		t.Fatalf("the join fixture carries %d task inputs, %d events and run failure %q; it holds nothing to a contract",
			len(inputs), len(events), runFailure)
	}
	return inputs, events, results, failures, canceled, runFailure
}

// TestJoinHistory_ActivityPayloadsMatchWhatCurrentCodeSends re-executes the
// fixture from its own input, feeding back every recorded outcome — results,
// failures (deploy's, after its retries) and cancellations (smoke's) — and
// compares every ExecuteTask input current code sends (the handler's join and
// failed-need ref included), every event message and the run's failure with
// what the recorded build did.
func TestJoinHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireJoinFixture(t)
	recorded := loadHistoryFile(t, joinHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	recordedInputs, recordedEvents, results, failures, canceled, runFailure := joinRecorded(t, recorded)

	var (
		mu        sync.Mutex
		sentTasks = map[string]*orkstackv1.ExecuteTaskInput{}
		sentEvent []*orkstackv1.Event
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(120 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			mu.Lock()
			if _, seen := sentTasks[in.TaskId]; !seen {
				sentTasks[in.TaskId] = in
			}
			mu.Unlock()
			switch {
			case canceled[in.TaskId]:
				return runUntilCanceled(actCtx, nil)
			case failures[in.TaskId] != nil:
				return nil, failures[in.TaskId]
			}
			result, ok := results[in.TaskId]
			if !ok {
				return nil, fmt.Errorf("current code ran task %q, which the recorded run never completed", in.TaskId)
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
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("re-executing the join fixture succeeded; the recorded run failed")
	}

	mu.Lock()
	defer mu.Unlock()
	sent := map[string]map[string]any{}
	for id, in := range sentTasks {
		sent[id] = normalized(t, canonicalPayload(t, in))
	}
	normalizedRecorded := map[string]map[string]any{}
	for id, in := range recordedInputs {
		normalizedRecorded[id] = normalized(t, in)
	}
	compareTaskInputs(t, normalizedRecorded, sent)

	var events []map[string]any
	for _, event := range sentEvent {
		events = append(events, normalized(t, canonicalPayload(t, event)))
	}
	var recordedNormalized []map[string]any
	for _, event := range recordedEvents {
		recordedNormalized = append(recordedNormalized, normalized(t, event))
	}
	compareEventPayloads(t, recordedNormalized, events)

	if !strings.Contains(activityErrorPrefix.ReplaceAllString(err.Error(), ""), activityErrorPrefix.ReplaceAllString(runFailure, "")) {
		t.Fatalf("the re-executed run failed with %q; the recorded run with %q", err, runFailure)
	}
}

// temporalFailureToError is a recorded activity failure as the error the
// recorded Orker returned, through the SDK's own failure converter.
func temporalFailureToError(failure *failurepb.Failure) error {
	return temporal.GetDefaultFailureConverter().FailureToError(failure)
}
