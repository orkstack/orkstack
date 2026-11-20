//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The lines fan-out suite (docs/DSL.md, "Fan-out over an artifact's lines"):
// real Temporal, the real object store and a real Orker running real
// containers. list writes an artifact of JSON lines, convert fans out over
// its lines, merge counts the staged items.

// linesTap is how a test steps in front of the Orker's line indexer: before,
// when set, runs first and may hold the index until its context ends.
type linesTap struct {
	mu      sync.Mutex
	before  func(ctx context.Context) error
	indexed atomic.Int32
	// beforeIndex, when set, runs before the index with its input.
	beforeIndex func(ctx context.Context, in *orkstackv1.IndexEachLinesInput) error
	// store, when set, wraps the Orker's artifact store: what its indexer and
	// its items read the source through.
	store func(artifacts.Store) artifacts.Store
	// execute, when set, sits in front of the Orker's ExecuteTask.
	execute func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, next func() (*orkstackv1.ExecuteTaskResult, error)) (*orkstackv1.ExecuteTaskResult, error)
	// memos, when set, is the Orker's memo store.
	memos artifacts.MemoStore
	// producerIndexes lets the producer's Orker build the line index as it
	// commits (b'). Off, the Orker is not asked to, so the fan-out's own
	// indexer runs — the path a test of the indexer means to exercise.
	producerIndexes bool
}

// startLinesOrker is startOrker with a line-index store, the cordon
// interceptor and IndexEachLines registered, as cmd/orker registers them.
func (e *eachE2E) startLinesOrker(id string, tap *linesTap) (worker.Worker, *orker.Activities) {
	e.t.Helper()
	refs, _ := e.store.(artifacts.RefPredictor)
	indexes, err := artifacts.NewEachIndexStore(e.ctx, e.cfg)
	must.OKf(e.t, err, "open line-index store")
	store := e.store
	var memos artifacts.MemoStore
	if tap != nil && tap.store != nil {
		store = tap.store(store)
	}
	if tap != nil {
		memos = tap.memos
	}
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs, Memos: memos,
		Pool: task.DefaultPool, OrkerID: id, WorkRoot: e.workRoot, EachIndexes: indexes,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(e.t, "orker boot checks failed: %v", err)
	}
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
		Interceptors: []interceptor.WorkerInterceptor{activities.WorkerInterceptor()},
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		e.mu.Lock()
		e.scheduled[scheduleKey(in)]++
		e.mu.Unlock()
		if tap == nil || !tap.producerIndexes {
			in.IndexLines = nil
		}
		next := func() (*orkstackv1.ExecuteTaskResult, error) { return activities.ExecuteTask(actx, in) }
		if tap != nil && tap.execute != nil {
			return tap.execute(actx, in, next)
		}
		return next()
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		if tap != nil {
			tap.indexed.Add(1)
			tap.mu.Lock()
			before := tap.before
			tap.mu.Unlock()
			if before != nil {
				if err := before(actx); err != nil {
					return nil, err
				}
			}
			if tap.beforeIndex != nil {
				if err := tap.beforeIndex(actx, in); err != nil {
					return nil, err
				}
			}
		}
		return activities.IndexEachLines(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	must.OKf(e.t, w.Start(), "start orker %s", id)
	e.mu.Lock()
	e.orkers = append(e.orkers, w)
	e.mu.Unlock()
	e.t.Cleanup(w.Stop)
	return w, activities
}

// linesDefinition: list writes lines (one per element) to rows.jsonl;
// convert fans out over its lines, batch k, running convert; merge counts the
// staged item directories.
func (e *eachE2E) linesDefinition(name string, lines []string, k int32, convert string) *orkstackv1.Orkflow {
	list := fmt.Sprintf(`printf '%%s\n' %s > /orkstack/output/artifacts/rows.jsonl && printf '{}' > /orkstack/output/values.json`,
		shellWords(lines))
	merge := `n=$(ls /orkstack/input/upstream/convert | wc -l) && printf '{"items":%d}' "$n" > /orkstack/output/values.json`
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	withMap := map[string]any{"index": "{{ .each.index }}"}
	if k < 2 {
		withMap["n"] = "{{ .each.item.n }}"
	}
	with, _ := structpbStruct(withMap)
	batch := k
	if batch < 2 {
		batch = 0
	}
	return &orkstackv1.Orkflow{
		Name:    name,
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "list", Uses: e.alpine, Image: image, Command: []string{"/bin/sh", "-c", list},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "convert", Uses: e.alpine, Image: image, Needs: []string{"list"},
				Command: []string{"/bin/sh", "-c", convert}, With: with,
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
				Each: &orkstackv1.EachSpec{OverArtifact: "list/rows.jsonl", Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
					Batch: batch, As: "item", MaxItems: 100, MaxParallel: 8,
					Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "list", Name: "rows.jsonl"}}}},
			{Id: "merge", Uses: e.alpine, Image: image, Needs: []string{"convert"},
				Command:        []string{"/bin/sh", "-c", merge},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
}

// shellWords quotes each word for sh.
func shellWords(words []string) string {
	quoted := make([]string, len(words))
	for i, w := range words {
		quoted[i] = "'" + strings.ReplaceAll(w, "'", `'\''`) + "'"
	}
	return strings.Join(quoted, " ")
}

func rowsOf(n int) []string {
	rows := make([]string, n)
	for i := range rows {
		rows[i] = fmt.Sprintf(`{"n":%d}`, i)
	}
	return rows
}

func (e *eachE2E) startLines(runID string, wf *orkstackv1.Orkflow) {
	e.t.Helper()
	plan := e.plan()
	plan.EachLines = engine.DefaultEachLinesPolicy()
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	must.OKf(e.t, starter.StartRun(e.ctx, runID, wf, nil, plan), "StartRun")
}

// itemsScheduled counts the ExecuteTask invocations of id's items.
func (e *eachE2E) itemsScheduled(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for key, count := range e.scheduled {
		if strings.HasPrefix(key, id+"#") {
			n += count
		}
	}
	return n
}

func TestIntegration_EachLinesFanOut(t *testing.T) {
	e := startEachE2E(t, true)
	tap := &linesTap{}
	e.startLinesOrker("lines-i1", tap)
	runID := fmt.Sprintf("ork-lines-i1-%d", time.Now().UnixNano())
	// Each item writes back the n its .each.item.n rendered into config.json.
	convert := `n=$(sed -n 's/.*"n": *"\([0-9]*\)".*/\1/p' /orkstack/input/config.json) && ` +
		`printf '{"n":%s}' "$n" > /orkstack/output/values.json`
	e.startLines(runID, e.linesDefinition("lines-i1", rowsOf(5), 1, convert))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	node := taskRunNamed(run, "convert")
	if node.GetEach().GetCount() != 5 || node.GetEach().GetLines().GetIndex().GetUri() == "" || tap.indexed.Load() != 1 {
		t.Fatalf("convert each = %v after %d indexes", node.GetEach(), tap.indexed.Load())
	}
	for i, item := range node.GetItems() {
		if got := readValues(t, e, item.GetValuesRef()); got != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("item %d read %s", i, got)
		}
	}
	if got := readValues(t, e, taskRunNamed(run, "merge").GetValuesRef()); !strings.Contains(got, `"items":5`) {
		t.Fatalf("merge read %s, want 5 items", got)
	}
}

func TestIntegration_EachLinesBatchFanOut(t *testing.T) {
	e := startEachE2E(t, true)
	e.startLinesOrker("lines-b1", nil)
	runID := fmt.Sprintf("ork-lines-b1-%d", time.Now().UnixNano())
	convert := `printf '{"lines":%d}' "$(wc -l < /orkstack/input/each/lines.jsonl)" > /orkstack/output/values.json`
	e.startLines(runID, e.linesDefinition("lines-b1", rowsOf(2500), 100, convert))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	node := taskRunNamed(run, "convert")
	if node.GetEach().GetCount() != 25 || len(node.GetItems()) != 25 {
		t.Fatalf("convert each = %v, %d items; want 25", node.GetEach(), len(node.GetItems()))
	}
	for i, item := range node.GetItems() {
		if got := readValues(t, e, item.GetValuesRef()); got != `{"lines":100}` {
			t.Fatalf("batch %d read %s; want 100 lines", i, got)
		}
	}
	if got := readValues(t, e, taskRunNamed(run, "merge").GetValuesRef()); !strings.Contains(got, `"items":25`) {
		t.Fatalf("merge read %s, want 25 items", got)
	}
}

// TestIntegration_EachLinesInvalidLineFailsBeforeAnyItem: one invalid line
// among 50 fails the fan-out EACH_LINE_INVALID before any item is scheduled.
func TestIntegration_EachLinesInvalidLineFailsBeforeAnyItem(t *testing.T) {
	linesInvalidLine(t, "lines-bad-u", 1, rowsOf(50), 3)
}

// TestIntegration_EachLinesBatchInvalidLineFailsBeforeAnyItem: one invalid
// line in batch 20 of 25 fails the fan-out before batch 0 is scheduled.
func TestIntegration_EachLinesBatchInvalidLineFailsBeforeAnyItem(t *testing.T) {
	linesInvalidLine(t, "lines-bad-b", 100, rowsOf(2500), 20*100+7)
}

// linesInvalidLine runs a fan-out of batch k over rows with row bad made
// invalid, on an Orker of identity id: Boot refuses one a live Orker holds.
func linesInvalidLine(t *testing.T, id string, k int32, rows []string, bad int) {
	t.Helper()
	e := startEachE2E(t, true)
	e.startLinesOrker(id, nil)
	rows[bad] = `{"n": not json}`
	runID := fmt.Sprintf("ork-%s-%d", id, time.Now().UnixNano())
	e.startLines(runID, e.linesDefinition(id, rows, k, `printf '{}' > /orkstack/output/values.json`))
	run := e.wait(runID)
	node := taskRunNamed(run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != eachlines.ReasonLineInvalid {
		t.Fatalf("run %v, convert %v", run.GetStatus(), node.GetError())
	}
	if n := e.itemsScheduled("convert"); n != 0 || len(node.GetItems()) != 0 {
		t.Fatalf("%d items scheduled, %d rows; want none before an invalid line", n, len(node.GetItems()))
	}
}

func TestIntegration_EachLinesCancelDuringIndex(t *testing.T) {
	e := startEachE2E(t, true)
	started := make(chan struct{})
	var sawCancel atomic.Bool
	tap := &linesTap{before: func(ctx context.Context) error {
		close(started)
		// A cancel reaches an activity only through its heartbeat, as the
		// real index heartbeats while it reads.
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				sawCancel.Store(true)
				return ctx.Err()
			case <-tick.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}}
	e.startLinesOrker("lines-c1", tap)
	runID := fmt.Sprintf("ork-lines-c1-%d", time.Now().UnixNano())
	e.startLines(runID, e.linesDefinition("lines-c1", rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`))
	select {
	case <-started:
	case <-time.After(2 * time.Minute):
		t.Fatal("the index never started")
	}
	must.OKf(t, e.temporal.CancelWorkflow(e.ctx, runID, ""), "cancel")
	run := e.wait(runID)
	node := taskRunNamed(run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || node.GetStatus() != orkstackv1.Status_STATUS_CANCELED || len(node.GetItems()) != 0 {
		t.Fatalf("run %v, convert %v with %d items", run.GetStatus(), node.GetStatus(), len(node.GetItems()))
	}
	if !sawCancel.Load() {
		t.Fatal("the index activity never heard the cancel")
	}
}

// TestIntegration_EachLinesOrkerKilledDuringIndex: an Orker lost while it
// indexes is noticed at the index's heartbeat timeout, and another Orker of
// the pool indexes again; the run succeeds once.
func TestIntegration_EachLinesOrkerKilledDuringIndex(t *testing.T) {
	e := startEachE2E(t, true)
	held := make(chan struct{})
	first := &linesTap{before: func(ctx context.Context) error {
		close(held)
		<-ctx.Done() // never answers: its worker is stopped under it
		return ctx.Err()
	}}
	w, _ := e.startLinesOrker("lines-k1", first)
	runID := fmt.Sprintf("ork-lines-k1-%d", time.Now().UnixNano())
	e.startLines(runID, e.linesDefinition("lines-k1", rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`))
	select {
	case <-held:
	case <-time.After(2 * time.Minute):
		t.Fatal("the index never started")
	}
	w.Stop()
	second := &linesTap{}
	e.startLinesOrker("lines-k2", second)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || taskRunNamed(run, "convert").GetEach().GetCount() != 5 {
		t.Fatalf("run %v, convert %v", run.GetStatus(), taskRunNamed(run, "convert").GetEach())
	}
	if second.indexed.Load() < 1 {
		t.Fatal("the surviving Orker never indexed")
	}
}
