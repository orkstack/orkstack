package engine_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// eachHistory is the fan-out fixture: a real history recorded by a versioned
// worker against a real cluster by TestIntegration_RecordEachRunHistory (I8),
// never hand-edited (testdata/history/README.md). A 6-item fan-out over an
// upstream output with max_parallel: 2, one item retried, and a direct
// dependent: the first local-activity count marker, the first item
// activities, the first yield-free stride snapshots and the first EachUpstream
// RunWorkflow ever issued, held still.
const eachHistory = "testdata/history/run-each-v1.json"

// requireEachFixture skips while the fixture has not been recorded yet. It
// can only be produced against a real cluster (I8); once committed this never
// skips again, and the replay below is a hard gate. ORK_REQUIRE_EACH_FIXTURE
// makes its absence a failure.
func requireEachFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(eachHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_EACH_FIXTURE") != "" {
			t.Fatalf("the fan-out replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordEachRunHistory", eachHistory, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordEachRunHistory", eachHistory, err)
	}
}

// scheduleRecorder captures, in workflow order, every ExecuteTask the
// replaying workflow schedules, as "task" or "task#item". The replayer
// compares commands by activity type and id only, never by input, so this is
// what lets a replay see WHICH item a command dispatched.
type scheduleRecorder struct {
	interceptor.WorkerInterceptorBase
	mu        sync.Mutex
	scheduled []string
}

func (r *scheduleRecorder) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &scheduleInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: r}
}

type scheduleInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *scheduleRecorder
}

func (i *scheduleInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&scheduleOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type scheduleOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *scheduleRecorder
}

func (o *scheduleOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...interface{}) workflow.Future {
	if activityType == task.ActivityExecuteTask && len(args) > 0 {
		if in, ok := args[0].(*orkstackv1.ExecuteTaskInput); ok {
			o.root.mu.Lock()
			o.root.scheduled = append(o.root.scheduled, scheduleKey(in))
			o.root.mu.Unlock()
		}
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func scheduleKey(in *orkstackv1.ExecuteTaskInput) string {
	if each := in.GetEach(); each != nil {
		return fmt.Sprintf("%s#%d", in.GetTaskId(), each.GetIndex())
	}
	return in.GetTaskId()
}

// recordedSchedule is the fixture's own ExecuteTask scheduling order, keyed
// as scheduleKey keys a replay's.
func recordedSchedule(t *testing.T, recorded *historypb.History) []string {
	t.Helper()
	var out []string
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		var in *orkstackv1.ExecuteTaskInput
		if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
			t.Fatalf("decode a recorded ExecuteTask input: %v", err)
		}
		out = append(out, scheduleKey(in))
	}
	return out
}

// replayEach replays the fixture against RunWorkflow (or body) and returns
// the replay's error and the ExecuteTask schedule it produced.
func replayEach(t *testing.T, body any) ([]string, error) {
	t.Helper()
	recorder := &scheduleRecorder{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewWorkflowReplayerWithOptions: %v", err)
	}
	replayer.RegisterWorkflowWithOptions(body, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	err = replayer.ReplayWorkflowHistoryFromJSONFile(nil, eachHistory)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.scheduled...), err
}

func TestRunWorkflow_ReplaysCommittedEachHistory(t *testing.T) {
	requireEachFixture(t)
	scheduled, err := replayEach(t, engine.RunWorkflow)
	if err != nil {
		t.Fatalf("replaying %s against current code failed: %v\n\n"+
			"A run that was fanning out when this build was deployed would fail its next workflow "+
			"task. Restore the fan-out command stream, or re-record and say why.", eachHistory, err)
	}
	recorded := recordedSchedule(t, loadHistoryFile(t, eachHistory))
	if fmt.Sprint(scheduled) != fmt.Sprint(recorded) {
		t.Fatalf("the replay dispatched %v; the recorded build dispatched %v", scheduled, recorded)
	}
}

// TestEachHistory_ReplayNeverRecountsTheArray is the research's "an expansion
// whose upstream array changed between replays". A replay never calls
// CountEachItems — the replayer executes no activity, local or otherwise, and
// reads the count from its marker — so whatever the store would now answer,
// the replay passes and the replayed run has the recorded items.
func TestEachHistory_ReplayNeverRecountsTheArray(t *testing.T) {
	requireEachFixture(t)
	recorder := &scheduleRecorder{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{recorder},
	})
	if err != nil {
		t.Fatal(err)
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, eachHistory); err != nil {
		t.Fatalf("replay: %v", err)
	}
	type resulting interface {
		GetWorkflowResult(workflowID string, valuePtr interface{}) error
	}
	var run *orkstackv1.Run
	if r, ok := replayer.(resulting); ok {
		if err := r.GetWorkflowResult("", &run); err != nil {
			t.Fatalf("the replayed result: %v", err)
		}
		if got := taskRunOf(t, run, "convert").GetEach(); !got.GetExpanded() || got.GetCount() != 6 {
			t.Fatalf("the replayed fan-out = %v, want the recorded 6 items", got)
		}
	}
	items := 0
	for _, key := range recorder.scheduled {
		if strings.HasPrefix(key, "convert#") {
			items++
		}
	}
	if items != 6 {
		t.Fatalf("the replay dispatched %d items, want the recorded 6", items)
	}
}

// TestEachHistory_ActivityPayloadsMatchWhatCurrentCodeSends is the payload
// half for the fan-out path: re-execute the fixture from its own input,
// feeding back each recorded ExecuteTask result (by task and item) and the
// recorded count, and compare every input current code sends with what the
// recorded build sent.
func TestEachHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireEachFixture(t)
	recorded := loadHistoryFile(t, eachHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	inputs, results := recordedItemPayloads(t, recorded)

	var count int32
	for _, in := range inputs {
		if each, ok := in["each"].(map[string]any); ok {
			if n, ok := each["count"].(float64); ok {
				count = int32(n)
			}
		}
	}
	var (
		mu   sync.Mutex
		sent = map[string]map[string]any{}
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		defer mu.Unlock()
		key := scheduleKey(in)
		sent[key] = canonicalPayload(t, in)
		result, ok := results[key]
		if !ok {
			return nil, fmt.Errorf("current code ran %q, which the recorded run never executed", key)
		}
		return result, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ArtifactRef, []string, string, int64) (int32, error) {
		return count, nil
	}, activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
		return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput, plan)
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("re-executing the fixture: completed=%v err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		got, ok := sent[key]
		if !ok {
			t.Errorf("the recorded run executed %q and this code does not", key)
			continue
		}
		if diff := payloadDiff(inputs[key], got); diff != "" {
			t.Errorf("the ExecuteTask input for %q is not the one the recorded build sent:\n%s", key, diff)
		}
	}
	for key := range sent {
		if _, ok := inputs[key]; !ok {
			t.Errorf("this code executes %q, which the recorded run never scheduled", key)
		}
	}
}

// recordedItemPayloads reads the fixture's ExecuteTask inputs and results,
// keyed by scheduleKey: an item's retries are server-side, so each key has
// exactly one scheduling and one completion.
func recordedItemPayloads(t *testing.T, recorded *historypb.History) (map[string]map[string]any, map[string]*orkstackv1.ExecuteTaskResult) {
	t.Helper()
	inputs := map[string]map[string]any{}
	results := map[string]*orkstackv1.ExecuteTaskResult{}
	keyByScheduledID := map[int64]string{}
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			var in *orkstackv1.ExecuteTaskInput
			if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
				t.Fatalf("decode a recorded input: %v", err)
			}
			key := scheduleKey(in)
			keyByScheduledID[event.GetEventId()] = key
			inputs[key] = recordedPayload(t, attrs.GetInput(), 0)
			continue
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			key, ok := keyByScheduledID[attrs.GetScheduledEventId()]
			if !ok {
				continue
			}
			var result *orkstackv1.ExecuteTaskResult
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &result); err != nil {
				t.Fatalf("decode a recorded result: %v", err)
			}
			results[key] = result
		}
	}
	return inputs, results
}
