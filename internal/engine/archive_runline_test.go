package engine_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run line is all `run get` and a list page need, and it is the first
// line: UnmarshalRunLine decodes it and never decodes an event line, so an
// event line that would not decode does not cost a run read anything.
func TestUnmarshalRunLine_DecodesTheRunLineAndNeverAnEventLine(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "reconcile", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC))}
	body, err := engine.MarshalRunArchive(run, []*orkstackv1.Event{{RunId: "ork-1-1", Sequence: 1}})
	if err != nil {
		t.Fatalf("MarshalRunArchive: %v", err)
	}
	body = append(body, []byte(`{"kind":"event","event":{"sequence":"not a number"}}`+"\n")...)

	got, err := engine.UnmarshalRunLine(body)
	if err != nil {
		t.Fatalf("UnmarshalRunLine: %v", err)
	}
	if !proto.Equal(got, run) {
		t.Fatalf("UnmarshalRunLine = %v, want %v", got, run)
	}
	if _, _, err := engine.UnmarshalRunArchive(body); err == nil {
		t.Fatal("the whole-body decode accepted the broken event line; the test proves nothing")
	}
	if _, err := engine.UnmarshalRunLine([]byte(`{"kind":"event","event":{}}` + "\n")); err == nil {
		t.Fatal("a body with no run line decoded")
	}

	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.PutRun(context.Background(), artifacts.RunRecord{
		RunID: "ork-1-1", Namespace: "default", Orkflow: "reconcile", Status: "succeeded",
		Time: run.GetCreatedAt().AsTime(), JSONL: body,
	}); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	archiver := engine.NewRunArchiver(store)
	loaded, err := archiver.LoadRun(context.Background(), "ork-1-1")
	if err != nil || !proto.Equal(loaded, run) {
		t.Fatalf("LoadRun = %v, %v; want the run line alone", loaded, err)
	}
	listed, err := archiver.ListRuns(context.Background(), engine.ArchiveQuery{Namespace: "default", Limit: 10})
	if err != nil || len(listed) != 1 || !proto.Equal(listed[0], run) {
		t.Fatalf("ListRuns = %v, %v; want the run line alone", listed, err)
	}
}
