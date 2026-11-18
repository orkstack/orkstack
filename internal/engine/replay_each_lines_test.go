package engine_test

import (
	"path/filepath"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The lines fan-out command paths (E89 M2), each replayed from its committed
// fixture (recorded by TestIntegration_RecordEachLinesHistories): one item
// per line, batches of three lines, a lines and an array fan-out side by
// side, and a lines fan-out whose item waits across a continuation. The path
// is entered only by a definition declaring over_artifact, which no older
// history holds, so it has no version gate.

const (
	replayEachLines      = "testdata/history/run-each-lines-v1.json"
	replayEachLinesBatch = "testdata/history/run-each-lines-batch-v1.json"
	replayEachLinesG0    = "testdata/history/run-each-lines-carry-g0-v1.json"
	replayEachLinesG1    = "testdata/history/run-each-lines-carry-g1-v1.json"
	replayEachMixed      = "testdata/history/run-each-mixed-v1.json"
	eachLinesRecorder    = "TestIntegration_RecordEachLinesHistories"
)

// eachLinesFixtureNames are every lines fixture's file name, all recorded on
// the paced stack, whose plan carries the publish window and the
// interruption budgets.
var eachLinesFixtureNames = []string{
	filepath.Base(replayEachLines), filepath.Base(replayEachLinesBatch), filepath.Base(replayEachMixed),
	filepath.Base(replayEachLinesG0), filepath.Base(replayEachLinesG1),
}

// continuedEachLinesFixtureNames are the lines fixtures whose plan carries a
// continuation budget, recorded after the item input floor existed.
var continuedEachLinesFixtureNames = []string{filepath.Base(replayEachLinesG0), filepath.Base(replayEachLinesG1)}

// linesItems decodes every ExecuteTask input of id's items a history
// scheduled.
func linesItems(t *testing.T, h *historypb.History, id string) []*orkstackv1.ExecuteTaskInput {
	t.Helper()
	var out []*orkstackv1.ExecuteTaskInput
	for _, attrs := range recordedScheduled(h, task.ActivityExecuteTask) {
		in := &orkstackv1.ExecuteTaskInput{}
		must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &in), "decode an ExecuteTask input")
		if in.GetTaskId() == id && in.GetEach() != nil {
			out = append(out, in)
		}
	}
	return out
}

// checkLinesItems: every item of convert says where its lines come from (the
// source list wrote, its index, the format and batch), and none stages the
// source as an upstream artifact.
func checkLinesItems(t *testing.T, items []*orkstackv1.ExecuteTaskInput, batch int32) {
	t.Helper()
	if len(items) == 0 {
		t.Fatal("no lines item was dispatched")
	}
	for _, in := range items {
		lines := in.GetEach().GetLines()
		if lines.GetSource().GetName() != "rows.jsonl" || lines.GetSource().GetSha256() == "" || lines.GetIndex().GetUri() == "" ||
			lines.GetFormat() != orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL || lines.GetBatch() != batch {
			t.Fatalf("item %d carries lines %v; want rows.jsonl with its digest, an index, jsonl, batch %d", in.GetEach().GetIndex(), lines, batch)
		}
		for _, up := range in.GetUpstream() {
			for _, ref := range up.GetArtifacts() {
				if ref.GetName() == "rows.jsonl" {
					t.Fatalf("item %d stages the source as an upstream artifact", in.GetEach().GetIndex())
				}
			}
		}
	}
}

// TestRunWorkflow_ReplaysCommittedEachLinesHistories: every fixture replays
// against current code and records the commands its path issues: one index,
// then one item per line (or per batch), each carrying its lines.
func TestRunWorkflow_ReplaysCommittedEachLinesHistories(t *testing.T) {
	for _, tc := range []struct {
		path         string
		indexes      int
		items, batch int32
	}{
		{replayEachLines, 1, 3, 0},
		{replayEachLinesBatch, 1, 3, 3},
		{replayEachMixed, 1, 2, 0},
		{replayEachLinesG0, 1, -1, 0},
	} {
		t.Run(filepath.Base(tc.path), func(t *testing.T) {
			requireE1Fixture(t, tc.path, eachLinesRecorder)
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if plan.EachLines == nil {
				t.Fatalf("%s was started with no line policy: it does not record what it says", tc.path)
			}
			if got := len(recordedScheduled(recorded, task.ActivityIndexEachLines)); got != tc.indexes {
				t.Fatalf("%s indexed %d times, want %d", tc.path, got, tc.indexes)
			}
			items := linesItems(t, recorded, "convert")
			checkLinesItems(t, items, tc.batch)
			if tc.items >= 0 && int32(len(items)) != tc.items {
				t.Fatalf("%s dispatched %d convert items, want %d", tc.path, len(items), tc.items)
			}
			must.OKf(t, replayAs(t, recorded, runID), "replaying %s against current code failed", tc.path)
		})
	}
	t.Run(filepath.Base(replayEachLinesG1), func(t *testing.T) {
		requireE1Fixture(t, replayEachLinesG0, eachLinesRecorder)
		requireE1Fixture(t, replayEachLinesG1, eachLinesRecorder)
		runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, replayEachLinesG0))
		recorded := loadHistoryFile(t, replayEachLinesG1)
		if got := len(recordedScheduled(recorded, task.ActivityIndexEachLines)); got != 0 {
			t.Fatalf("generation 1 indexed %d times: the carried lines were not restored", got)
		}
		checkLinesItems(t, linesItems(t, recorded, "convert"), 0)
		must.OKf(t, replayAs(t, recorded, runID), "replaying %s against current code failed", replayEachLinesG1)
	})
}

// TestEachLinesHistories_CatchAChangedBatch is the negative test: with the
// recorded definition's batch changed, the fan-out expands to another number
// of items, and current code cannot replay the history.
func TestEachLinesHistories_CatchAChangedBatch(t *testing.T) {
	for path, batch := range map[string]int32{replayEachLines: 2, replayEachLinesBatch: 0} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			requireE1Fixture(t, path, eachLinesRecorder)
			recorded := loadHistoryFile(t, path)
			attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			id, wf, input, plan, carry := startArgs(t, recorded)
			unchanged, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = unchanged
			must.OKf(t, replayAs(t, recorded, id), "%s re-encoded unchanged does not replay", path)
			changed := false
			for _, tk := range wf.GetTasks() {
				if tk.GetEach().GetOverArtifact() != "" {
					tk.Each.Batch, changed = batch, true
				}
			}
			if !changed {
				t.Fatalf("%s declares no lines fan-out", path)
			}
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			if err := replayAs(t, recorded, id); err == nil {
				t.Fatalf("%s replayed with its batch changed to %d: it does not pin the expansion's commands", path, batch)
			}
		})
	}
}
