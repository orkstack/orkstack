package engine_test

import (
	"context"
	"fmt"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The producer-built line index (fanout-artifacts design §3, b′): the
// producer's Orker is asked to index its artifact as it commits it, and the
// fan-out expands from that index without indexing again; any doubt about it
// falls back to the fan-out's own indexer.

// producerIndexes answers list with its artifacts and the line indexes
// built, and every item with success.
func producerIndexes(built ...*orkstackv1.LineIndexBuilt) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		r, err := listWrites(linesSource)(ctx, in)
		if in.GetTaskId() == "list" && r != nil {
			r.LineIndexes = built
		}
		return r, err
	}
}

func builtIndex(count int32, total int64) *orkstackv1.LineIndexBuilt {
	return &orkstackv1.LineIndexBuilt{TaskId: "convert", Name: "files.jsonl", Index: linesIndex, Count: count, TotalLines: total}
}

// TestEachLines_ProducerIndexIsUsed: the producer is asked to index the
// lines its fan-out reads, with the fan-out's own limits; the fan-out expands
// from the index it built, with no index activity of its own.
func TestEachLines_ProducerIndexIsUsed(t *testing.T) {
	h := newLinesHarness(t, indexes(3, 3), producerIndexes(builtIndex(3, 3)))
	run := h.runLines(t, "run-lines-producer", fmt.Sprintf(linesDocument, "", ""))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.indexCalls() != 0 {
		t.Fatalf("indexed %d times; the producer's index makes the fan-out's own unnecessary", h.indexCalls())
	}
	list := h.itemCalls("list")
	if len(list) != 1 || len(list[0].GetIndexLines()) != 1 {
		t.Fatalf("list's input asks for %v; want one line index", list[0].GetIndexLines())
	}
	req := list[0].GetIndexLines()[0]
	if req.GetTaskId() != "convert" || req.GetName() != "files.jsonl" || req.GetFormat() != orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL ||
		req.GetBatch() != 1 || req.GetMaxItems() != 50 || req.GetReadCap() <= 0 || req.GetSliceMaxBytes() <= 0 || req.GetValuesMaxBytes() <= 0 {
		t.Fatalf("index request = %v", req)
	}
	calls := h.itemCalls("convert")
	if len(calls) != 3 || calls[0].GetEach().GetLines().GetIndex().GetUri() != linesIndex.GetUri() {
		t.Fatalf("%d items, lines %v", len(calls), calls[0].GetEach().GetLines())
	}
	if node := taskRunOf(t, run, "convert"); node.GetEach().GetLines().GetIndex().GetUri() != linesIndex.GetUri() || node.GetScheduledAt() == nil {
		t.Fatalf("node = %v", node)
	}
	if got := len(h.itemCalls("merge")); got != 1 || len(h.itemCalls("merge")[0].GetIndexLines()) != 0 {
		t.Fatal("a task no fan-out reads the lines of was asked for an index")
	}
}

// TestEachLines_ProducerIndexInDoubtFallsBack: an index the producer did not
// build, or built for another fan-out, artifact or count, is not used: the
// fan-out indexes the lines itself, once.
func TestEachLines_ProducerIndexInDoubtFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name  string
		built *orkstackv1.LineIndexBuilt
	}{
		{"none built", nil},
		{"another fan-out's", &orkstackv1.LineIndexBuilt{TaskId: "other", Name: "files.jsonl", Index: linesIndex, Count: 3, TotalLines: 3}},
		{"another artifact's", &orkstackv1.LineIndexBuilt{TaskId: "convert", Name: "notes.txt", Index: linesIndex, Count: 3, TotalLines: 3}},
		{"a count its lines do not make", builtIndex(5, 3)},
		{"more items than max_items", builtIndex(51, 51)},
		{"no index", &orkstackv1.LineIndexBuilt{TaskId: "convert", Name: "files.jsonl", Count: 3, TotalLines: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var built []*orkstackv1.LineIndexBuilt
			if tc.built != nil {
				built = append(built, tc.built)
			}
			h := newLinesHarness(t, indexes(3, 3), producerIndexes(built...))
			run := h.runLines(t, "run-lines-doubt", fmt.Sprintf(linesDocument, "", ""))
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || h.indexCalls() != 1 || len(h.itemCalls("convert")) != 3 {
				t.Fatalf("run %v %v after %d indexes, %d items; want the fan-out's own index", run.GetStatus(), run.GetError(),
					h.indexCalls(), len(h.itemCalls("convert")))
			}
		})
	}
}
