package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"google.golang.org/protobuf/proto"
)

func TestItemEpochRuns_RangesTheItemsAResumeRanAgain(t *testing.T) {
	at := func(epochs ...int64) []*orkstackv1.TaskRun {
		out := make([]*orkstackv1.TaskRun, len(epochs))
		for i, e := range epochs {
			out[i] = &orkstackv1.TaskRun{ResultEpoch: e}
		}
		return out
	}
	run := func(first, count int32, epoch uint64) *orkstackv1.ItemEpochRun {
		return &orkstackv1.ItemEpochRun{First: first, Count: count, Epoch: epoch}
	}
	for _, tc := range []struct {
		name  string
		items []*orkstackv1.TaskRun
		want  []*orkstackv1.ItemEpochRun
	}{
		{"never resumed", at(0, 0, 0), nil},
		{"no items", nil, nil},
		{"one reran item", at(0, 7, 0), []*orkstackv1.ItemEpochRun{run(1, 1, 7)}},
		{"a run of reran items", at(7, 7, 7, 0), []*orkstackv1.ItemEpochRun{run(0, 3, 7)}},
		{"two resumes", at(7, 7, 9, 0, 9), []*orkstackv1.ItemEpochRun{run(0, 2, 7), run(2, 1, 9), run(4, 1, 9)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := itemEpochRuns(tc.items)
			if len(got) != len(tc.want) {
				t.Fatalf("runs = %v, want %v", got, tc.want)
			}
			for i := range got {
				if !proto.Equal(got[i], tc.want[i]) {
					t.Fatalf("runs = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// At their bound, a fan-out's ranges take a small share of a dependent's
// payload bound.
func TestItemEpochRuns_AtTheirBoundFitTheInput(t *testing.T) {
	items := make([]*orkstackv1.TaskRun, 2*MaxItemEpochRuns)
	for i := range items {
		items[i] = &orkstackv1.TaskRun{}
		if i%2 == 0 {
			items[i].ResultEpoch = maxResumeScope
		}
	}
	runs := itemEpochRuns(items)
	if len(runs) != MaxItemEpochRuns {
		t.Fatalf("%d ranges, want %d", len(runs), MaxItemEpochRuns)
	}
	size, err := task.PayloadSize(&orkstackv1.EachUpstream{Count: int32(len(items)), CommitRoot: "s3://bucket", Epochs: runs})
	if err != nil {
		t.Fatal(err)
	}
	if size > task.MaxActivityInputBytes/4 {
		t.Fatalf("%d ranges encode to %d bytes, over a quarter of the %d-byte payload bound", len(runs), size, task.MaxActivityInputBytes)
	}
}
