package engine

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
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
