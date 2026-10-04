package engine

import (
	"context"
	"os"
	"strconv"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestRunObjectIDs_IncludesItemReferences(t *testing.T) {
	ref := func(name, digit string) *orkstackv1.ArtifactRef {
		return &orkstackv1.ArtifactRef{Name: name, Uri: "s3://bucket/orkflows/fan/ork-1/convert/" + name + "/" + strings.Repeat(digit, 64)}
	}
	run := &orkstackv1.Run{Id: "ork-1", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "convert",
		Each:   &orkstackv1.TaskEach{Expanded: true, Count: 2},
		Items: []*orkstackv1.TaskRun{
			{TaskId: "convert", Item: proto.Int32(0), Artifacts: []*orkstackv1.ArtifactRef{ref("part.txt", "a")}, ValuesRef: ref("values.json", "b")},
			{TaskId: "convert", Item: proto.Int32(1), SystemArtifacts: []*orkstackv1.ArtifactRef{ref("stdout.log", "c")}},
		},
	}}}
	ids := runObjectIDs(run)
	if len(ids) != 3 {
		t.Fatalf("runObjectIDs = %v, want the three item references", ids)
	}
	for i, digit := range []string{"a", "b", "c"} {
		if !strings.HasSuffix(ids[i], strings.Repeat(digit, 64)) {
			t.Fatalf("ids[%d] = %q", i, ids[i])
		}
	}
}

// An item event between stride snapshots carries an EMPTY snapshot URI. The
// reader keeps the previous snapshot across it — GetRun answers from the last
// event that had one — and still delivers the item event itself, with its
// item, in the ordered stream.
func TestTemporalRunReader_KeepsTheLastSnapshotAcrossEventsWithoutOne(t *testing.T) {
	const runID = "ork-each-reader"
	wf := &orkstackv1.Orkflow{Name: "each-ork", Version: "1.0.0"}
	fresh := &orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_RUNNING,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "convert", Status: orkstackv1.Status_STATUS_RUNNING,
			Each: &orkstackv1.TaskEach{Expanded: true, Count: 2}}}}
	freshBytes, _ := proto.Marshal(fresh)
	store := &countingSnapshotStore{objects: map[string][]byte{"memory://fresh": freshBytes}}
	history := []*historypb.HistoryEvent{
		startedEvent(t, runID, wf),
		publishEventWithURI(t, 2, &orkstackv1.Event{RunId: runID, Sequence: 1,
			Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, TaskId: "convert",
			Status: orkstackv1.Status_STATUS_RUNNING}, "memory://fresh"),
		publishEventWithURI(t, 3, &orkstackv1.Event{RunId: runID, Sequence: 2,
			Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG, TaskId: "convert", Item: proto.Int32(0),
			Status: orkstackv1.Status_STATUS_SUCCEEDED, Message: "item 0 succeeded"}, ""),
	}
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got := run.GetTaskRuns(); len(got) != 1 || got[0].GetEach().GetCount() != 2 {
		t.Fatalf("GetRun = %v, want the last snapshot that had one", run)
	}
	events, err := reader.Events(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	var item *orkstackv1.Event
	for _, e := range events {
		if e.GetSequence() == 2 {
			item = e
		}
	}
	if item == nil || item.Item == nil || item.GetItem() != 0 {
		t.Fatalf("the item event was not delivered with its item: %v", events)
	}
}

// TestEachHistory_CatchesAReorderedDispatch is what makes the fan-out fixture
// worth having on the day it is recorded: it replays it against the real
// RunWorkflow with ONE thing changed — items dispatched in DESCENDING index
// order — and that replay MUST be caught. The replayer matches an activity
// command by type and id, never by input, so a reordered dispatch can pass
// its command check; what catches it is the schedule the replay produces,
// read through an interceptor, against the fixture's own.
func TestEachHistory_CatchesAReorderedDispatch(t *testing.T) {
	const path = "testdata/history/run-each-v1.json"
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_EACH_FIXTURE") != "" {
			t.Fatalf("the fan-out replay fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	eachDispatchDescending = true
	defer func() { eachDispatchDescending = false }()

	recorder := &itemScheduleRecorder{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{recorder},
	})
	if err != nil {
		t.Fatal(err)
	}
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	replayErr := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
	if replayErr != nil {
		message := replayErr.Error()
		if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
			t.Fatalf("replay failed, but not with a determinism violation: %v", replayErr)
		}
		t.Logf("reordered dispatch rejected by the replayer: %v", replayErr)
		return
	}
	first := ""
	for _, index := range recorder.items {
		first = index
		break
	}
	if first == "" || first == "0" {
		t.Fatalf("replaying the fan-out fixture against a body that dispatches descending produced the "+
			"recorded schedule %v: the fixture cannot see a reordered dispatch", recorder.items)
	}
	t.Logf("reordered dispatch seen: the replay dispatched item %s first, the recording item 0", first)
}

type itemScheduleRecorder struct {
	interceptor.WorkerInterceptorBase
	items []string
}

func (r *itemScheduleRecorder) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &itemScheduleInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: r}
}

type itemScheduleInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *itemScheduleRecorder
}

func (i *itemScheduleInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&itemScheduleOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type itemScheduleOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *itemScheduleRecorder
}

func (o *itemScheduleOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...interface{}) workflow.Future {
	if len(args) > 0 {
		if in, ok := args[0].(*orkstackv1.ExecuteTaskInput); ok && in.GetEach() != nil {
			o.root.items = append(o.root.items, strconv.Itoa(int(in.GetEach().GetIndex())))
		}
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}
