package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The composed fixtures: real histories of the §2.6 examples — a parent
// (composed-release) whose composed task's child (release-block) declares
// outputs and whose own outputs project them — recorded once by a versioned
// worker against a real cluster by TestIntegration_RecordComposedRunHistory
// (I10), never hand-edited (testdata/history/README.md). They hold the first
// ExecuteChildWorkflow, the first RenderComposedValues and
// StoreComposedValues markers and the first declared-outputs render
// RunWorkflow ever issued.
const (
	composedParentHistory = "testdata/history/run-composed-parent-v1.json"
	composedChildHistory  = "testdata/history/run-composed-child-v1.json"
)

// requireComposedFixtures skips while the fixtures have not been recorded.
// They can only be produced against a real cluster (I10); once committed this
// never skips again. ORK_REQUIRE_COMPOSED_FIXTURES makes their absence a
// failure.
func requireComposedFixtures(t *testing.T) {
	t.Helper()
	for _, path := range []string{composedParentHistory, composedChildHistory} {
		if _, err := os.Stat(path); err != nil {
			if os.Getenv("ORK_REQUIRE_COMPOSED_FIXTURES") != "" {
				t.Fatalf("the composed replay fixture %s is missing (%v); record it with "+
					"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
					"-run TestIntegration_RecordComposedRunHistory", path, err)
			}
			t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
				"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordComposedRunHistory", path, err)
		}
	}
}

func TestRunWorkflow_ReplaysCommittedComposedHistories(t *testing.T) {
	requireComposedFixtures(t)
	for _, path := range []string{composedParentHistory, composedChildHistory} {
		t.Run(path, func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
				t.Fatalf("replaying %s against current code failed: %v\n\n"+
					"A run that was composing when this build was deployed would fail its next workflow task. "+
					"Restore the composed command stream, or re-record and say why.", path, err)
			}
		})
	}
}

// recordedMarkers reads every local-activity marker of a history, in order,
// as (activity type, result payloads).
func recordedMarkers(t *testing.T, recorded *historypb.History) map[string][]*commonpb.Payloads {
	t.Helper()
	out := map[string][]*commonpb.Payloads{}
	for _, event := range recorded.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" {
			continue
		}
		var data struct{ ActivityType string }
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data); err != nil {
			t.Fatalf("decode a local activity marker: %v", err)
		}
		out[data.ActivityType] = append(out[data.ActivityType], attrs.GetDetails()["result"])
	}
	return out
}

// childStartCapture records the options and arguments of every child start.
type childStartCapture struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	options []workflow.ChildWorkflowOptions
	args    [][]any
}

func (c *childStartCapture) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &childStartInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: c}
}

type childStartInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *childStartCapture
}

func (i *childStartInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&childStartOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type childStartOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *childStartCapture
}

func (o *childStartOutbound) ExecuteChildWorkflow(ctx workflow.Context, childType string, args ...any) workflow.ChildWorkflowFuture {
	if childType == engine.RunWorkflowType {
		o.root.mu.Lock()
		o.root.options = append(o.root.options, workflow.GetChildWorkflowOptions(ctx))
		o.root.args = append(o.root.args, args)
		o.root.mu.Unlock()
	}
	return o.Next.ExecuteChildWorkflow(ctx, childType, args...)
}

// TestComposedHistory_PayloadsMatchWhatCurrentCodeSends is the payload half
// for the composed path: re-execute the parent from its own input, feeding
// back its recorded task results, its recorded render and store markers and
// its child's recorded result, and compare every ExecuteTask input AND the
// child start — id, task queue, parent-close policy, memo, search attributes
// and the four input payloads — against what the recorded build sent.
func TestComposedHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireComposedFixtures(t)
	recorded := loadHistoryFile(t, composedParentHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	inputs, results := recordedItemPayloads(t, recorded)
	markers := recordedMarkers(t, recorded)

	var started *historypb.StartChildWorkflowExecutionInitiatedEventAttributes
	var childResult *commonpb.Payloads
	var parentQueue string
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			parentQueue = attrs.GetTaskQueue().GetName()
		}
		if attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); attrs != nil {
			started = attrs
		}
		if attrs := event.GetChildWorkflowExecutionCompletedEventAttributes(); attrs != nil {
			childResult = attrs.GetResult()
		}
	}
	if started == nil || childResult == nil {
		t.Fatal("the parent fixture records no child start or no child completion")
	}

	var (
		mu      sync.Mutex
		sent    = map[string]map[string]any{}
		renders = markers[engine.ActivityRenderComposedValues]
		stores  = markers[engine.ActivityStoreComposedValues]
	)
	capture := &childStartCapture{}
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID, TaskQueue: parentQueue})
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: "ParentRunWorkflow"})
	env.RegisterWorkflowWithOptions(func(workflow.Context, string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan) (*orkstackv1.Run, error) {
		var run *orkstackv1.Run
		if err := converter.GetDefaultDataConverter().FromPayloads(childResult, &run); err != nil {
			return nil, err
		}
		return run, nil
	}, workflow.RegisterOptions{Name: engine.RunWorkflowType})
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
	env.RegisterActivityWithOptions(func(context.Context, engine.ComposeRenderRequest) (json.RawMessage, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(renders) == 0 {
			return nil, fmt.Errorf("current code rendered more often than the recorded run")
		}
		var out json.RawMessage
		err := converter.GetDefaultDataConverter().FromPayloads(renders[0], &out)
		renders = renders[1:]
		return out, err
	}, activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	env.RegisterActivityWithOptions(func(context.Context, engine.ComposeStoreRequest) (*orkstackv1.ArtifactRef, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(stores) == 0 {
			return nil, fmt.Errorf("current code stored more often than the recorded run")
		}
		var out *orkstackv1.ArtifactRef
		err := converter.GetDefaultDataConverter().FromPayloads(stores[0], &out)
		stores = stores[1:]
		return out, err
	}, activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
		return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.ExecuteWorkflow("ParentRunWorkflow", runID, wf, runInput, plan)
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("re-executing the parent fixture: completed=%v err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
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

	if len(capture.options) != 1 {
		t.Fatalf("current code started %d children; the recorded run started one", len(capture.options))
	}
	opts := capture.options[0]
	if opts.WorkflowID != started.GetWorkflowId() || opts.TaskQueue != started.GetTaskQueue().GetName() {
		t.Errorf("child id/queue = %q/%q, recorded %q/%q", opts.WorkflowID, opts.TaskQueue,
			started.GetWorkflowId(), started.GetTaskQueue().GetName())
	}
	if opts.ParentClosePolicy != started.GetParentClosePolicy() {
		t.Errorf("parent-close policy = %v, recorded %v", opts.ParentClosePolicy, started.GetParentClosePolicy())
	}
	if !opts.WaitForCancellation {
		t.Error("the child is started without WaitForCancellation")
	}
	for key, payload := range started.GetMemo().GetFields() {
		var want any
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &want); err != nil {
			t.Fatalf("decode recorded memo %s: %v", key, err)
		}
		if fmt.Sprint(opts.Memo[key]) != fmt.Sprint(want) {
			t.Errorf("memo %s = %v, recorded %v", key, opts.Memo[key], want)
		}
	}
	if len(opts.Memo) != len(started.GetMemo().GetFields()) {
		t.Errorf("memo has %d keys, recorded %d", len(opts.Memo), len(started.GetMemo().GetFields()))
	}
	if got, want := opts.TypedSearchAttributes.Size(), len(started.GetSearchAttributes().GetIndexedFields()); got != want {
		t.Errorf("search attributes = %d, recorded %d", got, want)
	}
	args := capture.args[0]
	recordedArgs := started.GetInput()
	if len(args) != 4 || len(recordedArgs.GetPayloads()) != 4 {
		t.Fatalf("child start carries %d arguments, recorded %d", len(args), len(recordedArgs.GetPayloads()))
	}
	for i, arg := range args {
		var want any
		if err := json.Unmarshal(recordedArgs.GetPayloads()[i].GetData(), &want); err != nil {
			t.Fatalf("decode recorded child argument %d: %v", i, err)
		}
		payload, err := converter.GetDefaultDataConverter().ToPayload(arg)
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if err := json.Unmarshal(payload.GetData(), &got); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(withoutTimestamps(got)) != fmt.Sprint(withoutTimestamps(want)) {
			t.Errorf("child start argument %d differs from the recorded one:\n got %s\nwant %s", i, payloadJSON(got), payloadJSON(want))
		}
	}
}
