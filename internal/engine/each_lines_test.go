package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// linesHarness is eachHarness with IndexEachLines registered: index answers
// every index, and every input it was given is recorded.
type linesHarness struct {
	*eachHarness
	mu      sync.Mutex
	indexed []*orkstackv1.IndexEachLinesInput
}

func newLinesHarness(t *testing.T, index func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error),
	behavior func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *linesHarness {
	t.Helper()
	h := &linesHarness{}
	h.eachHarness = newEachHarness(t, behavior)
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		h.mu.Lock()
		h.indexed = append(h.indexed, in)
		h.mu.Unlock()
		return index(ctx, in)
	}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	return h
}

func (h *linesHarness) indexCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.indexed)
}

func (h *linesHarness) runLines(t *testing.T, runID, document string) *orkstackv1.Run {
	t.Helper()
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	plan.EachLines = engine.DefaultEachLinesPolicy()
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, document), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run
}

const linesDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: lines
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/files.jsonl, format: jsonl, max_items: 50%s}
    with: {op: exec%s}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`

var (
	linesSource = &orkstackv1.ArtifactRef{Name: "files.jsonl", Uri: "mem://artifacts/files.jsonl",
		Sha256: strings.Repeat("ab", 32), SizeBytes: 1234}
	linesOther = &orkstackv1.ArtifactRef{Name: "notes.txt", Uri: "mem://artifacts/notes.txt",
		Sha256: strings.Repeat("cd", 32), SizeBytes: 10}
	linesIndex = &orkstackv1.ArtifactRef{Name: "jsonl-ab.idx", Uri: "mem://commits/each-lines/jsonl-ab.idx",
		Sha256: strings.Repeat("ef", 32), SizeBytes: 64 + 56*3}
)

// listWrites answers `list` with artifacts, and every item with success.
func listWrites(artifacts ...*orkstackv1.ArtifactRef) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "list" {
			return &orkstackv1.ExecuteTaskResult{TaskId: "list", ValuesJson: []byte(`{}`), ValuesInlined: true,
				ValuesRef: &orkstackv1.ArtifactRef{Uri: "mem://values/list"}, Artifacts: artifacts}, nil
		}
		return itemResult(in), nil
	}
}

func indexes(count int32, total int64) func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
	return func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		return &orkstackv1.IndexEachLinesResult{Count: count, TotalLines: total, Index: linesIndex}, nil
	}
}

// refusal is what an Orker's indexer returns for a reason the source decides.
func refusal(reason string) error {
	return temporal.NewNonRetryableApplicationError("refused: "+reason, reason, nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: reason})
}

func TestEachLines_ExpandsFromIndex(t *testing.T) {
	h := newLinesHarness(t, indexes(3, 3), listWrites(linesSource, linesOther))
	run := h.runLines(t, "run-lines", fmt.Sprintf(linesDocument, "", ""))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.indexCalls() != 1 || h.counts != 0 {
		t.Fatalf("indexed %d times, counted %d; want one index and no count", h.indexCalls(), h.counts)
	}
	in := h.indexed[0]
	if in.GetTaskId() != "convert" || in.GetSource().GetSha256() != linesSource.GetSha256() || in.GetProducer() != "list" ||
		in.GetName() != "files.jsonl" || in.GetFormat() != orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL ||
		in.GetBatch() != 1 || in.GetMaxItems() != 50 || in.GetReadCap() <= 0 || in.GetSliceMaxBytes() <= 0 {
		t.Fatalf("index input = %v", in)
	}
	calls := h.itemCalls("convert")
	if len(calls) != 3 {
		t.Fatalf("%d convert activities, want 3", len(calls))
	}
	for _, c := range calls {
		lines := c.GetEach().GetLines()
		if lines.GetIndex().GetSha256() != linesIndex.GetSha256() || lines.GetSource().GetSha256() != linesSource.GetSha256() || c.GetEach().GetCount() != 3 {
			t.Fatalf("item each = %v", c.GetEach())
		}
		// DropsSourceFromItemUpstream: the source is never staged; the
		// producer's other artifacts are.
		up := upstreamRefOf(c, "list")
		for _, a := range up.GetArtifacts() {
			if a.GetName() == "files.jsonl" {
				t.Fatalf("item %d's upstream carries the source", c.GetEach().GetIndex())
			}
		}
		if len(up.GetArtifacts()) != 1 || up.GetArtifacts()[0].GetName() != "notes.txt" {
			t.Fatalf("item upstream artifacts = %v; want notes.txt alone", up.GetArtifacts())
		}
	}
	events := h.eventsFor("run-lines", "convert")
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED || !strings.Contains(events[0].GetMessage(), "indexing the lines of list/files.jsonl") {
		t.Fatalf("first convert event = %v %q", events[0].GetType(), events[0].GetMessage())
	}
	if events[1].GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_STARTED || events[1].GetMessage() != "fan-out of 3 items: dispatching to orkers" {
		t.Fatalf("second convert event = %v %q", events[1].GetType(), events[1].GetMessage())
	}
	node := taskRunOf(t, run, "convert")
	if !node.GetEach().GetExpanded() || node.GetEach().GetCount() != 3 || node.GetEach().GetLines().GetIndex().GetUri() != linesIndex.GetUri() {
		t.Fatalf("node each = %v", node.GetEach())
	}
	// The dependent still sees one fan-out upstream entry of 3 items.
	if merge := h.itemCalls("merge"); len(merge) != 1 || upstreamRefOf(merge[0], "convert").GetEach().GetCount() != 3 {
		t.Fatalf("merge calls = %v", merge)
	}
}

func TestEachLines_BatchCountsItemsByCeil(t *testing.T) {
	h := newLinesHarness(t, indexes(3, 7), listWrites(linesSource))
	run := h.runLines(t, "run-lines-batch", fmt.Sprintf(linesDocument, ", batch: 3", ""))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.indexed[0].GetBatch() != 3 {
		t.Fatalf("index batch = %d; want 3", h.indexed[0].GetBatch())
	}
	calls := h.itemCalls("convert")
	if len(calls) != 3 || calls[0].GetEach().GetLines().GetBatch() != 3 {
		t.Fatalf("%d items, lines %v", len(calls), calls[0].GetEach().GetLines())
	}
	if got := h.eventsFor("run-lines-batch", "convert")[1].GetMessage(); got != "fan-out of 3 items of 3 lines: dispatching to orkers" {
		t.Fatalf("started = %q", got)
	}
}

func TestEachLines_ZeroLinesSucceeds(t *testing.T) {
	h := newLinesHarness(t, indexes(0, 0), listWrites(linesSource))
	run := h.runLines(t, "run-lines-zero", fmt.Sprintf(linesDocument, "", ""))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(h.itemCalls("convert")) != 0 {
		t.Fatalf("run = %v, %d items", run.GetStatus(), len(h.itemCalls("convert")))
	}
	if node := taskRunOf(t, run, "convert"); node.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !node.GetEach().GetExpanded() {
		t.Fatalf("node = %v %v", node.GetStatus(), node.GetEach())
	}
}

func TestEachLines_MissingArtifactIsEachOverMissing(t *testing.T) {
	h := newLinesHarness(t, indexes(3, 3), listWrites(linesOther))
	run := h.runLines(t, "run-lines-missing", fmt.Sprintf(linesDocument, "", ""))
	node := taskRunOf(t, run, "convert")
	if node.GetError().GetCode() != engine.EachOverMissingCode || !strings.Contains(node.GetError().GetMessage(), `"notes.txt"`) {
		t.Fatalf("node error = %v", node.GetError())
	}
	if h.indexCalls() != 0 || len(h.itemCalls("convert")) != 0 {
		t.Fatal("a missing source was indexed or dispatched")
	}
}

func TestEachLines_SizePrecheckSchedulesNothing(t *testing.T) {
	huge := &orkstackv1.ArtifactRef{Name: "files.jsonl", Uri: linesSource.GetUri(), Sha256: linesSource.GetSha256(), SizeBytes: 1 << 40}
	h := newLinesHarness(t, indexes(3, 3), listWrites(huge))
	run := h.runLines(t, "run-lines-huge", fmt.Sprintf(linesDocument, "", ""))
	if code := taskRunOf(t, run, "convert").GetError().GetCode(); code != eachlines.ReasonSourceTooLarge {
		t.Fatalf("node error = %v", taskRunOf(t, run, "convert").GetError())
	}
	if h.indexCalls() != 0 {
		t.Fatal("a source over the read cap was indexed")
	}
}

func TestEachLines_RefusalFailsBeforeAnyItem(t *testing.T) {
	for _, reason := range []string{eachlines.ReasonLineInvalid, eachlines.ReasonTooManyItems,
		eachlines.ReasonSourceTooLarge, eachlines.ReasonBatchTooLarge, eachlines.ReasonExpansionFailed} {
		t.Run(reason, func(t *testing.T) {
			var calls atomic.Int32
			h := newLinesHarness(t, func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
				calls.Add(1)
				return nil, refusal(reason)
			}, listWrites(linesSource))
			run := h.runLines(t, "run-lines-refused", fmt.Sprintf(linesDocument, "", ""))
			node := taskRunOf(t, run, "convert")
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != reason || len(node.GetItems()) != 0 {
				t.Fatalf("run %v node %v %v, %d items", run.GetStatus(), node.GetStatus(), node.GetError(), len(node.GetItems()))
			}
			if calls.Load() != 1 || len(h.itemCalls("convert")) != 0 {
				t.Fatalf("indexed %d times, %d items; want one index and no item", calls.Load(), len(h.itemCalls("convert")))
			}
		})
	}
}

func TestEachLines_ExhaustedAttemptsAreExpansionFailed(t *testing.T) {
	var calls atomic.Int32
	h := newLinesHarness(t, func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		calls.Add(1)
		return nil, errors.New("store down")
	}, listWrites(linesSource))
	run := h.runLines(t, "run-lines-outage", fmt.Sprintf(linesDocument, "", ""))
	if code := taskRunOf(t, run, "convert").GetError().GetCode(); code != engine.EachExpansionFailedCode {
		t.Fatalf("node error = %v", taskRunOf(t, run, "convert").GetError())
	}
	if calls.Load() != engine.DefaultEachIndexMaxAttempts {
		t.Fatalf("indexed %d times; want the policy's %d attempts", calls.Load(), engine.DefaultEachIndexMaxAttempts)
	}
}

func TestEachLines_ContradictoryResultIsExpansionFailed(t *testing.T) {
	h := newLinesHarness(t, indexes(5, 3), listWrites(linesSource))
	run := h.runLines(t, "run-lines-contradiction", fmt.Sprintf(linesDocument, "", ""))
	if code := taskRunOf(t, run, "convert").GetError().GetCode(); code != engine.EachExpansionFailedCode || len(h.itemCalls("convert")) != 0 {
		t.Fatalf("node error = %v", taskRunOf(t, run, "convert").GetError())
	}
}

func TestEachLines_ProducerSkippedUnderNoneFailed(t *testing.T) {
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: lines-skip
tasks:
  - id: list
    uses: shell
    condition: "{{ eq 1 2 }}"
    with: {op: exec}
  - id: other
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list, other]
    join: none_failed
    each: {over_artifact: list/files.jsonl, format: jsonl, max_items: 5}
    with: {op: exec}
`
	h := newLinesHarness(t, indexes(3, 3), listWrites(linesSource))
	run := h.runLines(t, "run-lines-skip", doc)
	if code := taskRunOf(t, run, "convert").GetError().GetCode(); code != engine.EachOverMissingCode || h.indexCalls() != 0 {
		t.Fatalf("node %v error = %v after %d indexes; list %v", taskRunOf(t, run, "convert").GetStatus(), taskRunOf(t, run, "convert").GetError(), h.indexCalls(), taskRunOf(t, run, "list").GetStatus())
	}
}

func TestEachLines_OperatorCancelCancelsIndex(t *testing.T) {
	var h *linesHarness
	var canceled atomic.Bool
	h = newLinesHarness(t, func(ctx context.Context, _ *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		h.env.CancelWorkflow()
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
			return nil, errors.New("never canceled")
		}
	}, listWrites(linesSource))
	run := h.runLines(t, "run-lines-cancel", fmt.Sprintf(linesDocument, "", ""))
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	node := taskRunOf(t, run, "convert")
	// Whether the activity itself saw the cancel is the cluster's to deliver:
	// TestIntegration_EachLinesCancelDuringIndex proves it on a real one.
	if node.GetStatus() != orkstackv1.Status_STATUS_CANCELED || len(node.GetItems()) != 0 {
		t.Fatalf("node = %v, %d items, index canceled %v, indexed %d", node.GetStatus(), len(node.GetItems()), canceled.Load(), h.indexCalls())
	}
	if len(h.itemCalls("convert")) != 0 {
		t.Fatal("an item started after the cancel")
	}
}

func TestEachLines_PlanWithoutPolicyIsInvalidEach(t *testing.T) {
	h := newLinesHarness(t, indexes(3, 3), listWrites(linesSource))
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-lines-noplan", compile(t, fmt.Sprintf(linesDocument, "", "")), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	run, _ := h.store.Get("run-lines-noplan")
	if code := taskRunOf(t, run, "convert").GetError().GetCode(); code != engine.InvalidEachCode || h.indexCalls() != 0 {
		t.Fatalf("node error = %v", taskRunOf(t, run, "convert").GetError())
	}
}

func TestEachLines_PlanPinsIndexPolicy(t *testing.T) {
	if !engine.DeclaresEachLines(compile(t, fmt.Sprintf(linesDocument, "", ""))) {
		t.Fatal("a lines definition is not seen to declare lines")
	}
	if engine.DeclaresEachLines(compile(t, fmt.Sprintf(eachTaskDocument, ""))) {
		t.Fatal("an array fan-out is seen to declare lines")
	}
	p := engine.DefaultEachLinesPolicy()
	if p.IndexMaxAttempts != 5 || p.IndexHeartbeatSeconds != 30 || p.SliceMaxBytes != 64<<20 || p.IndexMinThroughputBytesPerSecond != 24_000_000 {
		t.Fatalf("policy = %+v", p)
	}
}

// TestSimulate_LinesCountUnknown: a plan cannot count an artifact's lines,
// stubbed values or not; the fan-out runs, its count unset.
func TestSimulate_LinesCountUnknown(t *testing.T) {
	wf := compile(t, fmt.Sprintf(linesDocument, "", ""))
	plan := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"list": {"parts": []any{1.0}}}))
	got := planTaskOf(t, plan, "convert")
	if got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || got.EachCount != nil || got.GetEach().GetOverArtifact() != "list/files.jsonl" {
		t.Fatalf("convert = %v", got)
	}
}
