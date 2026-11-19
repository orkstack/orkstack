//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The lines fan-out fixtures (replay_each_lines_test.go), each from a REAL
// run on a real cluster by a versioned, paced worker: one completion per
// workflow task, so every recording of a shape is the same sequence. The
// line index is the real Orker's (orker.Activities.IndexEachLines over the
// real object store), paced like every other activity. ExecuteTask is a
// scripted stand-in for the Orker's container work: list uploads its source
// to the real store as a container's output would be, and each item
// succeeds, except where the carry shape hands one back as a store fault, a
// fault a real Orker produces only when its store fails at an exact point.

// The committed fixtures.
const (
	eachLinesFixture      = "testdata/history/run-each-lines-v1.json"
	eachLinesBatchFixture = "testdata/history/run-each-lines-batch-v1.json"
	eachLinesCarryG0      = "testdata/history/run-each-lines-carry-g0-v1.json"
	eachLinesCarryG1      = "testdata/history/run-each-lines-carry-g1-v1.json"
	eachMixedFixture      = "testdata/history/run-each-mixed-v1.json"
)

// eachLinesFixtureDocument fans out over list's rows.jsonl, %s being the
// batch, and merges the items.
const eachLinesFixtureDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: each-lines-fixture
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/rows.jsonl, format: jsonl, max_items: 20%s}
    with: {op: exec, index: "{{ .each.index }}"}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`

// eachLinesCarryDocument is the fan-out alone. With an 80-event budget the
// run is under it when list ends (about 60 events) and past it at its item's
// wait (the second store fault's back-off), so it carries on there, once.
const eachLinesCarryDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: each-lines-carry
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/rows.jsonl, format: jsonl, max_items: 20}
    with: {op: exec, index: "{{ .each.index }}"}
`

// eachMixedFixtureDocument runs an array fan-out and a lines fan-out of the
// same producer side by side.
const eachMixedFixtureDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: each-mixed-fixture
inputs:
  parts: {type: array}
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: spread
    uses: shell
    needs: [list]
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/rows.jsonl, format: jsonl, max_items: 20}
    with: {op: exec, index: "{{ .each.index }}"}
`

// linesFixtureOrker is the scripted ExecuteTask: list uploads rows to the
// real store and returns its reference; every other unit follows steps (the
// scriptedOrker of the interruption fixtures), then succeeds.
type linesFixtureOrker struct {
	store  artifacts.Store
	rows   []byte
	script *scriptedOrker
}

func (o *linesFixtureOrker) behavior(ctx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() != "list" {
		return o.script.behavior(ctx, s, in)
	}
	sum := sha256.Sum256(o.rows)
	key := fmt.Sprintf("fixtures/%s/list/rows.jsonl", in.GetRunId())
	ref, err := o.store.Put(ctx, key, bytes.NewReader(o.rows), artifacts.PutOptions{
		ContentType: "application/jsonl", ExpectedSHA256: hex.EncodeToString(sum[:]), ExpectedSize: int64(len(o.rows)),
	})
	if err != nil {
		return nil, fmt.Errorf("upload list's rows: %w", err)
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: "list", Attempt: 1, OrkerId: "orker-fixture", ValuesJson: []byte(`{}`),
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "rows.jsonl", Uri: ref.URI, ContentType: "application/jsonl",
			SizeBytes: ref.SizeBytes, Sha256: ref.SHA256}}}, nil
}

// startPacedLinesStack is startPacedCANStack whose Orker worker also hosts
// the real line indexer, paced like every other activity.
func startPacedLinesStack(t *testing.T, ctx context.Context, label string, o *linesFixtureOrker) *canStack {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	store, err := artifacts.New(cfg)
	must.OKf(t, err, "open artifact store")
	indexes, err := artifacts.NewEachIndexStore(ctx, cfg)
	must.OKf(t, err, "open line-index store")
	o.store = store
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Pool: task.DefaultPool, OrkerID: "orker-fixture", WorkRoot: t.TempDir(), EachIndexes: indexes,
	})
	p := &pacer{registry: parkedActivities, keep: map[string]bool{}}
	p.register = func(s *canStack, w worker.Worker) {
		w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			release, err := p.turn(actx, s, false)
			if err != nil {
				return nil, err
			}
			defer release()
			return activities.IndexEachLines(actx, in)
		}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	}
	return startCANStackWith(t, ctx, label, true, o.behavior, nil, p)
}

// linesFixturePlan is s.plan with the line policy pinned, as submit pins it
// for a definition declaring lines.
func linesFixturePlan(s *canStack, budget *engine.ContinuationBudget, mutate func(*engine.InterruptionBudgets)) engine.RunPlan {
	plan := interruptionFixturePlan(s, budget, mutate)
	plan.EachLines = engine.DefaultEachLinesPolicy()
	return plan
}

// historyShape is a history's sequence of event types, each with the
// activity it scheduled or the marker it recorded: what must be identical
// across repeats of one paced shape.
func historyShape(h *historypb.History) []string {
	out := make([]string, 0, len(h.GetEvents()))
	for _, e := range h.GetEvents() {
		item := e.GetEventType().String()
		if a := e.GetActivityTaskScheduledEventAttributes(); a != nil {
			item += ":" + a.GetActivityType().GetName()
		}
		if m := e.GetMarkerRecordedEventAttributes(); m != nil {
			item += ":" + m.GetMarkerName()
		}
		out = append(out, item)
	}
	return out
}

func jsonlRows(n int) []byte {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "{\"n\":%d}\n", i)
	}
	return []byte(b.String())
}

// TestIntegration_RecordEachLinesHistories records every lines fixture,
// each shape ORK_RECORD_REPEATS times (default 3), and fails unless every
// repeat of a shape has the same shape, ends as it should and replays
// against current code. It never skips: by default the recordings land under
// ORK_PROOF_DIR; only ORK_RECORD_GOLDEN_HISTORY=1 also writes the fixtures.
func TestIntegration_RecordEachLinesHistories(t *testing.T) {
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	repeats := 3
	if v := os.Getenv("ORK_RECORD_REPEATS"); v != "" {
		_, err := fmt.Sscanf(v, "%d", &repeats)
		must.OKf(t, err, "ORK_RECORD_REPEATS %q", v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	var mu sync.Mutex
	files := map[string]*historypb.History{}
	t.Cleanup(func() {
		for path, h := range files {
			data := encodeHistory(t, h)
			proof.Write(t, "each-lines", filepath.Base(path), data)
			if write {
				must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
			}
			t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
		}
	})
	// record runs one shape repeats times; each run returns its histories,
	// one per generation, and the first repeat's are kept.
	record := func(t *testing.T, paths []string, run func(t *testing.T, n int) []*historypb.History) {
		var first [][]string
		for n := range repeats {
			histories := run(t, n)
			if len(histories) != len(paths) {
				t.Fatalf("repeat %d recorded %d histories, want %d", n, len(histories), len(paths))
			}
			for i, h := range histories {
				shape := historyShape(h)
				if n == 0 {
					first = append(first, shape)
					continue
				}
				if !slices.Equal(first[i], shape) {
					t.Fatalf("repeat %d of %s differs from repeat 0:\n got %v\nwant %v", n, filepath.Base(paths[i]), shape, first[i])
				}
			}
			if n == 0 {
				mu.Lock()
				for i, h := range histories {
					files[paths[i]] = h
				}
				mu.Unlock()
			}
		}
		t.Logf("%d repeats identical", repeats)
	}
	// single runs document once on its own paced stack and returns its one
	// history.
	single := func(t *testing.T, label, document string, input *orkstackv1.Payload, rows int, steps map[string][]orkerStep, items int) *historypb.History {
		o := &linesFixtureOrker{rows: jsonlRows(rows), script: &scriptedOrker{steps: steps, calls: map[string]int{}}}
		s := startPacedLinesStack(t, ctx, label, o)
		runID := fmt.Sprintf("ork-%s-%d", label, time.Now().UnixNano())
		must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, document), input, linesFixturePlan(s, nil, nil)), "StartRun")
		must.OKf(t, s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil), "%s did not succeed", label)
		if got := countItems(o.script, "convert"); got != items {
			t.Fatalf("%s ran %d convert items, want %d", label, got, items)
		}
		recorded := s.history(runID, "")
		must.OKf(t, replayRecorded(recorded, runID), "%s just recorded does not replay", label)
		return recorded
	}

	t.Run("lines", func(t *testing.T) {
		record(t, []string{eachLinesFixture}, func(t *testing.T, n int) []*historypb.History {
			return []*historypb.History{single(t, fmt.Sprintf("el-one-%d", n), fmt.Sprintf(eachLinesFixtureDocument, ""), nil, 3, nil, 3)}
		})
	})
	t.Run("batch", func(t *testing.T) {
		record(t, []string{eachLinesBatchFixture}, func(t *testing.T, n int) []*historypb.History {
			return []*historypb.History{single(t, fmt.Sprintf("el-batch-%d", n), fmt.Sprintf(eachLinesFixtureDocument, ", batch: 3"), nil, 7, nil, 3)}
		})
	})
	t.Run("mixed", func(t *testing.T) {
		record(t, []string{eachMixedFixture}, func(t *testing.T, n int) []*historypb.History {
			return []*historypb.History{single(t, fmt.Sprintf("el-mixed-%d", n), eachMixedFixtureDocument,
				payload(t, map[string]any{"parts": []any{"x", "y"}}), 2, nil, 2)}
		})
	})
	t.Run("carry", func(t *testing.T) {
		record(t, []string{eachLinesCarryG0, eachLinesCarryG1}, func(t *testing.T, n int) []*historypb.History {
			fault := orkerStep{err: storeFaultError()}
			o := &linesFixtureOrker{rows: jsonlRows(2),
				script: &scriptedOrker{steps: map[string][]orkerStep{"convert#0": {fault, fault}}, calls: map[string]int{}}}
			s := startPacedLinesStack(t, ctx, fmt.Sprintf("el-carry-%d", n), o)
			runID := fmt.Sprintf("ork-el-carry-%d-%d", n, time.Now().UnixNano())
			budget := engine.DefaultContinuationBudget(80, 1<<20, 3*time.Minute)
			plan := linesFixturePlan(s, &budget, func(b *engine.InterruptionBudgets) { b.NeverStartedFaultFloorMillis = 20000 })
			must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, eachLinesCarryDocument), nil, plan), "StartRun")
			must.OKf(t, s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil), "the carry run failed")
			described, err := describeExecution(s.client, s.ctx, runID, "")
			must.OK(t, err)
			chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
			if len(chain) != 2 {
				t.Fatalf("the carry run had %d generations, want 2", len(chain))
			}
			if got := o.script.count("convert#0"); got != 3 {
				t.Fatalf("item 0 ran %d times, want 3", got)
			}
			g0, g1 := s.history(runID, chain[0]), s.history(runID, chain[1])
			if !continuedAsNew(g0) {
				t.Fatal("generation 0 did not carry on")
			}
			for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
				must.OKf(t, replayRecorded(h, runID), "carry %s just recorded does not replay", name)
			}
			return []*historypb.History{g0, g1}
		})
	})
}

// countItems is how many distinct items of id the script was asked to run.
func countItems(o *scriptedOrker, id string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for key := range o.calls {
		if strings.HasPrefix(key, id+"#") {
			n++
		}
	}
	return n
}
