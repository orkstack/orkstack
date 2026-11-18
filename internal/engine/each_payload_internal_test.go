package engine

import (
	"testing"

	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A fan-out item's input is encoded once by the engine's converter and
// handed to the SDK as a raw value. Those must be the very bytes the SDK
// would have written from the message, metadata included, or the commands
// a run issues, and every recorded history's replay, would change.
func TestItemInputPayload_IsWhatTheSDKWouldWrite(t *testing.T) {
	with, err := structpb.NewStruct(map[string]any{"part": "é<&>", "n": 3.5, "list": []any{"a", true, nil}})
	if err != nil {
		t.Fatal(err)
	}
	in := &orkstackv1.ExecuteTaskInput{TaskId: "convert", RunId: "ork-abc", IdempotencyKey: "default/ork-abc/convert/3",
		With: with, Each: &orkstackv1.EachContext{Index: 3, Count: 400, As: "item"}}
	ours, err := DataConverter().ToPayload(in)
	if err != nil {
		t.Fatal(err)
	}
	sdk, err := converter.GetDefaultDataConverter().ToPayload(in)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(ours, sdk) {
		t.Fatalf("the engine's payload differs from the SDK default's:\n%v\n%v", ours, sdk)
	}
	raw, err := DataConverter().ToPayloads(converter.NewRawValue(ours))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.GetPayloads()) != 1 || !proto.Equal(raw.GetPayloads()[0], sdk) {
		t.Fatalf("a raw value is not passed through as it is: %v", raw)
	}
}

// A plain task's input is encoded once too (taskDispatch), and those bytes,
// handed to the SDK as a raw value, are the very ones it would have written
// from the message; its size is the measure CheckInputSize bounds.
func TestTaskInputPayload_IsWhatTheSDKWouldWrite(t *testing.T) {
	with, err := structpb.NewStruct(map[string]any{"cmd": "echo é<&>", "n": 2.0})
	if err != nil {
		t.Fatal(err)
	}
	plain := &orkstackv1.Task{Id: "plain", With: with}
	s := &runState{runID: "ork-p", wf: &orkstackv1.Orkflow{Name: "p", Tasks: []*orkstackv1.Task{plain}}}
	got, err := s.taskDispatch(plain, "plain", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := s.taskDispatchInput(plain, "plain", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	sdk, err := converter.GetDefaultDataConverter().ToPayload(msg)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.payload, sdk) {
		t.Fatalf("the engine's payload differs from the SDK default's:\n%v\n%v", got.payload, sdk)
	}
	if want, err := task.PayloadSize(msg); err != nil || got.size != want {
		t.Fatalf("size %d, PayloadSize %d (%v)", got.size, want, err)
	}
}
