package engine_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestEachBounds_ReplayNeutralForRecordedSizes: every committed fixture's
// definition — each recorded with at most 1,000 declared items — gives the
// same per-item bounds with the 16 KiB floor as without one, and a plan
// built before the floor existed decodes with none. So the floor changes no
// command any recorded history issued.
func TestEachBounds_ReplayNeutralForRecordedSizes(t *testing.T) {
	fanOuts := 0
	for _, path := range committedFixtures(t) {
		recorded := loadHistoryFile(t, path)
		_, wf, _, plan := goldenWorkflowArgs(t, recorded)
		if plan.Continuation != nil && plan.Continuation.ItemInputFloorBytes != 0 {
			t.Errorf("%s: a recorded plan carries an item input floor of %d", filepath.Base(path),
				plan.Continuation.ItemInputFloorBytes)
		}
		if dsl.EachItemsDeclared(wf) == 0 {
			continue
		}
		fanOuts++
		without, artifactsWithout := engine.EachItemBounds(wf, plan.Limits, 0)
		with, artifactsWith := engine.EachItemBounds(wf, plan.Limits, engine.DefaultItemInputFloorBytes)
		if without != with || artifactsWithout != artifactsWith {
			t.Errorf("%s (%d items): bounds %d/%d without the floor, %d/%d with it", filepath.Base(path),
				dsl.EachItemsDeclared(wf), without, artifactsWithout, with, artifactsWith)
		}
	}
	if fanOuts == 0 {
		t.Fatal("no committed fixture declares a fan-out; nothing was compared")
	}
}

// flooredFanOutDocument declares 10,000 items over the run input's two parts;
// each item's own `with:` carries pad bytes.
func flooredFanOutDocument(pad int) string {
	return `
name: wide-fan-out
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 10000}
    with: {op: exec, part: "{{ .each.item }}", pad: "` + strings.Repeat("p", pad) + `"}
`
}

// TestLocalItemInput_AcceptsWhatTheEngineAccepts: at S = 10,000 an item
// input of about 12 KiB is composed by LocalTaskInput (what `orkctl task
// run` calls) exactly as scheduleItem composes it under a plan carrying the
// floor, and one of about 17 KiB is refused INPUT_TOO_LARGE by both — the
// engine's limit and the local one are the same 16 KiB.
func TestLocalItemInput_AcceptsWhatTheEngineAccepts(t *testing.T) {
	const runID = "ork-wide-items"
	input := payload(t, map[string]any{"parts": []any{"a", "b"}})
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
		engine.DefaultContinuationRestoreTimeout)
	for _, tc := range []struct {
		name    string
		pad     int
		refused bool
	}{{"12KiB", 12 << 10, false}, {"17KiB", 17 << 10, true}} {
		t.Run(tc.name, func(t *testing.T) {
			wf := compile(t, flooredFanOutDocument(tc.pad))
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return itemResult(in), nil
			})
			h.env.SetTestTimeout(time.Minute)
			plan := defaultPlan()
			plan.CommitRoot = "mem://commits"
			plan.Continuation = &budget
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
			if !h.env.IsWorkflowCompleted() {
				t.Fatal("the run did not complete")
			}
			run, ok := h.store.Get(runID)
			if !ok {
				t.Fatal("no run read model")
			}
			index, count := int32(1), int32(2)
			local, err := engine.LocalTaskInput(engine.LocalTask{
				Orkflow: wf, TaskID: "spread", RunID: runID, Input: input, Item: &index, ItemCount: &count,
			})
			if tc.refused {
				node := taskRunOf(t, run, "spread")
				if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != task.ReasonInputTooLarge {
					t.Fatalf("engine: run %v, node %v; want INPUT_TOO_LARGE", run.GetStatus(), node.GetError())
				}
				if !strings.Contains(node.GetError().GetMessage(), "16384-byte per-item limit") {
					t.Fatalf("engine refusal %q does not name the 16 KiB floor", node.GetError().GetMessage())
				}
				var refusal *engine.LocalTaskRefusal
				if !errors.As(err, &refusal) || refusal.Code != task.ReasonInputTooLarge {
					t.Fatalf("local: %v, want an INPUT_TOO_LARGE refusal", err)
				}
				return
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("engine: run %v %v", run.GetStatus(), run.GetError())
			}
			if err != nil {
				t.Fatalf("local: %v", err)
			}
			var scheduled *orkstackv1.ExecuteTaskInput
			for _, in := range h.invocations() {
				if in.GetEach().GetIndex() == index {
					scheduled = in
				}
			}
			if scheduled == nil {
				t.Fatal("the engine never scheduled item 1")
			}
			size, _ := task.InputSize(scheduled)
			if size < 12<<10 || size > 16<<10 {
				t.Fatalf("item 1's input is %d bytes, want between 12 and 16 KiB", size)
			}
			// The deliberate differences LocalTaskInput names.
			cleared := proto.Clone(scheduled).(*orkstackv1.ExecuteTaskInput)
			cleared.CommitUri, cleared.Pin, cleared.Telemetry, cleared.ImagePolicy = "", nil, nil, nil
			if !proto.Equal(local, cleared) {
				t.Fatalf("LocalTaskInput differs from scheduleItem's input\n got: %v\nwant: %v", local, cleared)
			}
		})
	}
}
