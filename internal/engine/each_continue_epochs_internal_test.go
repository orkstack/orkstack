package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"google.golang.org/protobuf/proto"
)

// TestUpstreamFor_AContinueFanOutsHandlerGetsItsItemsEpochs: a failure handler
// stages the items of a failed continue fan-out that succeeded, so it is
// handed their epochs as an ordinary dependent is: without them an item a
// resume ran again would be looked for under the commit key of its first run.
func TestUpstreamFor_AContinueFanOutsHandlerGetsItsItemsEpochs(t *testing.T) {
	fan := &orkstackv1.Task{Id: "convert", Each: &orkstackv1.EachSpec{
		OnItemFailure: orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE}}
	handler := &orkstackv1.Task{Id: "report", Needs: []string{"convert"}, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED}
	item := func(i int32, status orkstackv1.Status, epoch int64) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: "convert", Item: proto.Int32(i), Status: status, ResultEpoch: epoch}
	}
	s := &runState{
		tasks:   map[string]*orkstackv1.Task{"convert": fan, "report": handler},
		results: map[string]*orkstackv1.ExecuteTaskResult{},
		eaches:  map[string]*eachState{"convert": {spec: fan.Each, expanded: true, count: 3}},
		taskRuns: map[string]*orkstackv1.TaskRun{"convert": {TaskId: "convert", Status: orkstackv1.Status_STATUS_FAILED,
			Items: []*orkstackv1.TaskRun{
				item(0, orkstackv1.Status_STATUS_SUCCEEDED, 0),
				item(1, orkstackv1.Status_STATUS_SUCCEEDED, 7),
				item(2, orkstackv1.Status_STATUS_FAILED, 7),
			}}},
	}
	refs := s.upstreamFor("report")
	if len(refs) != 1 {
		t.Fatalf("upstream = %v; want the fan-out's one reference", refs)
	}
	each := refs[0].GetEach()
	if got := each.GetFailed(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("failed = %v; want [2]", got)
	}
	// The ranges cover every item, a failed one included: failed leaves it out.
	if got := each.GetEpochs(); len(got) != 1 || got[0].GetFirst() != 1 || got[0].GetCount() != 2 || got[0].GetEpoch() != 7 {
		t.Fatalf("epochs = %v; want items 1 and 2 at scope 7", got)
	}
}

// TestEachLines_RunContextIsOutsideTheItemBound: an item whose input is
// exactly at its bound is sent with the largest run context its run pins.
func TestEachLines_RunContextIsOutsideTheItemBound(t *testing.T) {
	s := &runState{plan: RunPlan{RunContextMaxBytes: DefaultRunContextMaxBytes}}
	in := &orkstackv1.ExecuteTaskInput{TaskId: "convert", Each: &orkstackv1.EachContext{Count: 1,
		Lines: &orkstackv1.EachLines{Source: &orkstackv1.ArtifactRef{Name: "a", Uri: "s3://b/a"}}}}
	bound, err := task.PayloadSize(in)
	if err != nil {
		t.Fatal(err)
	}
	in.RunContext = &orkstackv1.RunContext{RootRunId: "ork-root", ParentRunId: "ork-parent", TreeIndex: 3}
	whole, err := task.PayloadSize(in)
	if err != nil {
		t.Fatal(err)
	}
	if whole <= bound {
		t.Fatalf("the run context added nothing: %d and %d bytes", bound, whole)
	}
	got, err := s.userInputSize(in, whole, bound)
	if err != nil || got != bound {
		t.Fatalf("userInputSize = %d, %v; want the %d bytes without the run context", got, err, bound)
	}
}
