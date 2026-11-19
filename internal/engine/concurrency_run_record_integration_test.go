//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// holdGate is the test Orker's hold: a task named "hold" waits until its
// run is let go (or its attempt is cancelled), every other task succeeds at
// once. It is how a recorder keeps a run holding its slot for exactly as
// long as a scenario needs.
type holdGate struct {
	mu   sync.Mutex
	held map[string]chan struct{}
	seen map[string]bool
}

func newHoldGate() *holdGate {
	return &holdGate{held: map[string]chan struct{}{}, seen: map[string]bool{}}
}

func (g *holdGate) ch(run string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.held[run]
	if !ok {
		c = make(chan struct{})
		g.held[run] = c
	}
	return c
}

// let lets run's hold task finish.
func (g *holdGate) let(run string) { close(g.ch(run)) }

// holding reports that run's hold task has started.
func (g *holdGate) holding(run string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seen[run]
}

func (g *holdGate) behavior(ctx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() == "hold" {
		c := g.ch(in.GetRunId())
		g.mu.Lock()
		g.seen[in.GetRunId()] = true
		g.mu.Unlock()
		select {
		case <-c:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
		ValuesJson: valuesJSON(map[string]any{"task": in.GetTaskId()})}, nil
}

// concurrencyDocument is a run of the named tasks, each after the one
// before it, under a run-level limit of one.
func concurrencyDocument(name string, tasks ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\nconcurrency:\n  limit: 1\ntasks:\n", name)
	for i, id := range tasks {
		fmt.Fprintf(&b, "  - id: %s\n    uses: shell\n    with: {op: exec}\n", id)
		if i > 0 {
			fmt.Fprintf(&b, "    needs: [%s]\n", tasks[i-1])
		}
	}
	return b.String()
}

// concurrencyRunPlan is the stack's plan with a run-level limit of one on
// key, as submit pins it, its gate on the stack's engine channel.
func concurrencyRunPlan(s *canStack, key string, queueTimeout int64, requeue time.Duration, budget *engine.ContinuationBudget,
	tune func(*engine.GateTunables)) engine.RunPlan {
	plan := s.plan(engine.DefaultHistoryMaxEvents, budget)
	gate := gateTunablesForTest()
	if tune != nil {
		tune(&gate)
	}
	run := 0
	plan.Concurrency = &engine.ConcurrencyPlan{
		Slots: []engine.ConcurrencySlotPlan{{GateID: engine.GateID("default", "", key), Key: key, Label: key, Limit: 1,
			Strategy: engine.ConcurrencyStrategyQueue, QueueTimeoutSeconds: queueTimeout}},
		Run: &run, SeqBase: 1, GateQueue: s.runQueue, Namespace: "default",
		AcquireTimeout: 20 * time.Second, AcquireBackoffMin: 200 * time.Millisecond, AcquireBackoffMax: 2 * time.Second,
		RequeueInterval: requeue, RequeueBackoffFactor: 2, RequeueIntervalMax: 4 * requeue,
		PerSlotBytes: 2048, EventHoldersShown: 5, Gate: gate,
	}
	return plan
}

// concurrencyRunFixture is one recorded run of E88 P3's run side.
func concurrencyRunFixture(name string) string {
	return filepath.Join("testdata", "history", "concurrency-run-"+name+"-v1.json")
}

// concurrencyRunFixtureGen is one generation's fixture of a run that
// continued as new.
func concurrencyRunFixtureGen(name string, gen int) string {
	return filepath.Join("testdata", "history", fmt.Sprintf("concurrency-run-%s-g%d-v1.json", name, gen))
}

// TestIntegration_RecordConcurrencyRunHistories records E88 P3's run-side
// fixtures (replay_concurrency_run_test.go's concurrencyRunFixtures) from
// real runs and real gates on a real cluster, by a versioned worker. It
// never skips: by default the recordings land under ORK_PROOF_DIR and the
// committed fixtures are left alone; only ORK_RECORD_GOLDEN_HISTORY=1 also
// writes them. It FAILS unless every run ends as its path promises and its
// recording replays.
func TestIntegration_RecordConcurrencyRunHistories(t *testing.T) {
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	// The gate's local activities read the cluster through a client of
	// their own on the stack's namespace, as cmd/ork's do.
	gateClient := dialVersioningCluster(t, ctx, versioningNamespace())
	holds := newHoldGate()
	// An emit fails only when its snapshot cannot be written (a publish that
	// fails is logged): the store refuses this run's second snapshot, its
	// RUN_STARTED's, after the run has acquired its slot.
	var failStarted sync.Map // run id → its RUN_STARTED snapshot write fails
	canStackSnapshotWrap = func(inner engine.SnapshotStore) engine.SnapshotStore {
		return &refusingStore{SnapshotStore: inner, refuse: func(runID string, sequence int64) bool {
			_, fail := failStarted.Load(runID)
			return fail && sequence == 2
		}}
	}
	s := startCANStackWith(t, ctx, "conc-run", true, holds.behavior, nil, nil, func(w worker.Worker) {
		engine.RegisterGateActivities(w, engine.NewGateActivities(gateClient))
	})
	canStackSnapshotWrap = nil
	// A gate is rebuilt from the limit's attributes and the namespace's, as
	// cmd/ork registers both.
	nsIndexed, err := task.EnsureRunSearchAttributes(ctx, s.client, s.namespace)
	must.OKf(t, err, "register the namespace attribute in %s", s.namespace)
	if !nsIndexed {
		t.Fatalf("the namespace attribute is not usable in %s", s.namespace)
	}
	usable, err := task.EnsureConcurrencySearchAttributes(ctx, s.client, s.namespace)
	must.OKf(t, err, "register the concurrency attributes in %s", s.namespace)
	if !usable {
		t.Fatalf("the concurrency attributes are not usable in %s", s.namespace)
	}

	type recorded struct {
		h  *historypb.History
		id string // the run id it replays as: a later generation's start names none
	}
	files := map[string]recorded{}
	// A gate carries run ids of at most 48 bytes, as real ones (30) are: a
	// short label and the stack's time in base 36 keep these within it.
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
	runID := func(label string) string {
		id := "ork-" + label + "-" + stamp
		if len(id) > 48 {
			t.Fatalf("test run id %s is over the 48 bytes a gate carries", id)
		}
		return id
	}
	start := func(t *testing.T, id, document string, plan engine.RunPlan) {
		t.Helper()
		must.OKf(t, s.starter.StartRun(ctx, id, compile(t, document), nil, plan), "StartRun %s", id)
	}
	end := func(id string) error { return s.client.GetWorkflow(ctx, id, "").Get(ctx, nil) }
	wantCode := func(t *testing.T, id, code string, err error) {
		t.Helper()
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || app.Type() != code {
			t.Fatalf("run %s ended %v, want %s", id, err, code)
		}
	}
	waitFor := func(t *testing.T, what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			select {
			case <-ctx.Done():
				t.Fatalf("waiting for %s: %v", what, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	// queued reports that the run published RUN_QUEUED: it waited in a
	// line at least once.
	queued := func(id string) bool {
		for _, e := range s.history(id, "").GetEvents() {
			if a := e.GetActivityTaskScheduledEventAttributes(); a != nil && a.GetActivityType().GetName() == engine.ActivityPublishEvent &&
				strings.Contains(a.GetInput().String(), "RUN_QUEUED") {
				return true
			}
		}
		return false
	}
	// acquires counts the run's acquire local activities.
	acquires := func(id string) int {
		n := 0
		for _, e := range s.history(id, "").GetEvents() {
			if a := e.GetMarkerRecordedEventAttributes(); a != nil && strings.Contains(a.String(), engine.ActivityAcquireConcurrency) {
				n++
			}
		}
		return n
	}
	chain := func(t *testing.T, id string) []string {
		t.Helper()
		described, err := describeExecution(s.client, ctx, id, "")
		must.OK(t, err)
		return s.chainOf(id, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	}

	// granted: a free key grants at once; the run runs and releases.
	t.Run("granted", func(t *testing.T) {
		id := runID("g")
		start(t, id, concurrencyDocument("conc-granted", "a"), concurrencyRunPlan(s, "granted", 60, time.Minute, nil, nil))
		must.OKf(t, end(id), "run %s", id)
		files[concurrencyRunFixture("granted")] = recorded{s.history(id, ""), id}
	})

	// queued, its deadline timer cancelled at the grant: a run waits behind
	// a holder and starts on its grant signal.
	t.Run("queued", func(t *testing.T) {
		h, q := runID("qh"), runID("q")
		start(t, h, concurrencyDocument("conc-queued", "hold"), concurrencyRunPlan(s, "queued", 60, time.Minute, nil, nil))
		waitFor(t, h+" holding", func() bool { return holds.holding(h) })
		start(t, q, concurrencyDocument("conc-queued", "a"), concurrencyRunPlan(s, "queued", 60, time.Minute, nil, nil))
		waitFor(t, q+" queued", func() bool { return queued(q) })
		holds.let(h)
		must.OKf(t, end(h), "run %s", h)
		must.OKf(t, end(q), "run %s", q)
		files[concurrencyRunFixture("queued")] = recorded{s.history(q, ""), q}
	})

	// requeued: a waiting run re-asserts its place on its timer before its
	// grant.
	t.Run("requeued", func(t *testing.T) {
		h, q := runID("rh"), runID("r")
		start(t, h, concurrencyDocument("conc-requeued", "hold"), concurrencyRunPlan(s, "requeued", 60, time.Minute, nil, nil))
		waitFor(t, h+" holding", func() bool { return holds.holding(h) })
		start(t, q, concurrencyDocument("conc-requeued", "a"), concurrencyRunPlan(s, "requeued", 60, 2*time.Second, nil, nil))
		waitFor(t, q+"'s re-assert", func() bool { return acquires(q) >= 2 })
		holds.let(h)
		must.OKf(t, end(h), "run %s", h)
		must.OKf(t, end(q), "run %s", q)
		files[concurrencyRunFixture("requeued")] = recorded{s.history(q, ""), q}
	})

	// queue-canceled: an operator's cancel while the run waits ends it
	// canceled, and it releases.
	t.Run("queue-canceled", func(t *testing.T) {
		h, q := runID("ch"), runID("c")
		start(t, h, concurrencyDocument("conc-canceled", "hold"), concurrencyRunPlan(s, "canceled", 60, time.Minute, nil, nil))
		waitFor(t, h+" holding", func() bool { return holds.holding(h) })
		start(t, q, concurrencyDocument("conc-canceled", "a"), concurrencyRunPlan(s, "canceled", 60, time.Minute, nil, nil))
		waitFor(t, q+" queued", func() bool { return queued(q) })
		must.OKf(t, s.starter.CancelRun(ctx, q, engine.CancelRequest{}), "CancelRun %s", q)
		var canceled *temporal.CanceledError
		if err := end(q); !errors.As(err, &canceled) {
			t.Fatalf("run %s ended %v, want canceled", q, err)
		}
		holds.let(h)
		must.OKf(t, end(h), "run %s", h)
		files[concurrencyRunFixture("queue-canceled")] = recorded{s.history(q, ""), q}
	})

	// queue-timeout: a run waits out its queue_timeout and ends
	// RUN_QUEUE_TIMEOUT.
	t.Run("queue-timeout", func(t *testing.T) {
		h, q := runID("th"), runID("t")
		start(t, h, concurrencyDocument("conc-timeout", "hold"), concurrencyRunPlan(s, "timeout", 60, time.Minute, nil, nil))
		waitFor(t, h+" holding", func() bool { return holds.holding(h) })
		start(t, q, concurrencyDocument("conc-timeout", "a"), concurrencyRunPlan(s, "timeout", 3, time.Minute, nil, nil))
		wantCode(t, q, engine.RunQueueTimeoutCode, end(q))
		holds.let(h)
		must.OKf(t, end(h), "run %s", h)
		files[concurrencyRunFixture("queue-timeout")] = recorded{s.history(q, ""), q}
	})

	// queue-timeout-rebuilding: a run whose gate is still rebuilding its
	// record when its wait ends ends RUN_CONCURRENCY_UNAVAILABLE, never a
	// queue timeout.
	t.Run("queue-timeout-rebuilding", func(t *testing.T) {
		// The run's key has no gate, so its acquire starts one, which
		// rebuilds its record first. Every page of that listing times out,
		// so the gate stays rebuilding for longer than the run waits.
		q := runID("tr")
		failing := func(g *engine.GateTunables) {
			g.RebuildPageTimeout, g.RebuildTimeout = time.Nanosecond, time.Millisecond
			g.RebuildBackoffMin, g.RebuildBackoffMax = 30*time.Second, 30*time.Second
		}
		start(t, q, concurrencyDocument("conc-rebuilding", "a"), concurrencyRunPlan(s, "rebuilding", 3, time.Minute, nil, failing))
		wantCode(t, q, engine.RunConcurrencyUnavailableCode, end(q))
		files[concurrencyRunFixture("queue-timeout-rebuilding")] = recorded{s.history(q, ""), q}
	})

	// holds-across-continue: a run holding its slot continues as new; its
	// held slot crosses in the carry and is recorded again, and it releases
	// once, at its end.
	t.Run("holds-across-continue", func(t *testing.T) {
		id := runID("hc")
		budget := engine.DefaultContinuationBudget(60, 1<<20, 3*time.Minute)
		start(t, id, concurrencyDocument("conc-continue", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"),
			concurrencyRunPlan(s, "continue", 60, time.Minute, &budget, nil))
		must.OKf(t, end(id), "run %s", id)
		// The first generation, which switches holding the slot, and the
		// last, which adopted it from a carry and releases it.
		gens := chain(t, id)
		if len(gens) < 2 {
			t.Fatalf("%d generations, want the run to carry on", len(gens))
		}
		files[concurrencyRunFixtureGen("holds-across-continue", 0)] = recorded{s.history(id, gens[0]), id}
		files[concurrencyRunFixtureGen("holds-across-continue", 1)] = recorded{s.history(id, gens[len(gens)-1]), id}
	})

	// continue-gate-rebuild: the gate is lost while a run that continued
	// holds its slot; the gate rebuilt from the cluster's index finds the
	// run by the records its new generation issued again, and a second run
	// waits behind it.
	t.Run("continue-gate-rebuild", func(t *testing.T) {
		id, q := runID("cr"), runID("crq")
		budget := engine.DefaultContinuationBudget(60, 1<<20, 3*time.Minute)
		start(t, id, concurrencyDocument("conc-rebuild", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "hold"),
			concurrencyRunPlan(s, "rebuild", 60, time.Minute, &budget, nil))
		waitFor(t, id+" holding after it carried on", func() bool { return holds.holding(id) && len(chain(t, id)) >= 2 })
		gate := engine.GateID("default", "", "rebuild")
		// The second generation's records are in the cluster's index before
		// the gate is lost, so its rebuild finds them.
		waitFor(t, id+" indexed under the gate", func() bool { return indexedUnder(t, s, gate) >= 1 })
		must.OKf(t, s.client.TerminateWorkflow(ctx, gate, "", "the gate is lost"), "terminate gate %s", gate)
		start(t, q, concurrencyDocument("conc-rebuild", "a"), concurrencyRunPlan(s, "rebuild", 60, time.Minute, nil, nil))
		waitFor(t, q+" queued behind the restored holder", func() bool { return queued(q) })
		holds.let(id)
		must.OKf(t, end(id), "run %s", id)
		must.OKf(t, end(q), "run %s", q)
		gens := chain(t, id)
		files[concurrencyRunFixture("continue-gate-rebuild")] = recorded{s.history(id, gens[len(gens)-1]), id}
	})

	// emit-failure-releases: a run whose RUN_STARTED cannot be written ends
	// on that error and still releases its slot, so the next run on its key
	// is granted at once.
	t.Run("emit-failure-releases", func(t *testing.T) {
		id, next := runID("e"), runID("en")
		failStarted.Store(id, true)
		start(t, id, concurrencyDocument("conc-emit", "a"), concurrencyRunPlan(s, "emit", 60, time.Minute, nil, nil))
		if err := end(id); err == nil {
			t.Fatalf("run %s succeeded; its RUN_STARTED snapshot write fails", id)
		}
		start(t, next, concurrencyDocument("conc-emit", "a"), concurrencyRunPlan(s, "emit", 60, time.Minute, nil, nil))
		must.OKf(t, end(next), "run %s after the failed one released", next)
		if queued(next) {
			t.Fatalf("run %s waited; the failed run did not release its slot", next)
		}
		files[concurrencyRunFixture("emit-failure-releases")] = recorded{s.history(id, ""), id}
	})

	if len(files) != len(concurrencyRunFixtures) {
		t.Fatalf("recorded %d fixtures, the replay test reads %d", len(files), len(concurrencyRunFixtures))
	}
	for path, r := range files {
		must.OKf(t, replayRecorded(r.h, r.id), "%s just recorded does not replay", path)
		data := encodeHistory(t, r.h)
		proof.Write(t, "concurrency-run", filepath.Base(path), data)
		if write {
			must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
		}
	}
}

// refusingStore refuses the snapshot writes refuse names, as an object
// store that is down for one run would, and passes every other through.
type refusingStore struct {
	engine.SnapshotStore
	refuse func(runID string, sequence int64) bool
}

func (r *refusingStore) WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	if r.refuse(runID, sequence) {
		return "", errors.New("the object store refused the snapshot")
	}
	return r.SnapshotStore.WriteSnapshot(ctx, namespace, runID, sequence, data)
}

func (r *refusingStore) WriteSnapshotOverlay(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	if r.refuse(runID, sequence) {
		return "", errors.New("the object store refused the snapshot")
	}
	return r.SnapshotStore.WriteSnapshotOverlay(ctx, namespace, runID, sequence, data)
}

// indexedUnder counts the running runs the cluster's index lists as holding
// or waiting at gate.
func indexedUnder(t *testing.T, s *canStack, gate string) int64 {
	t.Helper()
	query := fmt.Sprintf("WorkflowType = %q AND (%s = %q OR %s = %q) AND ExecutionStatus = %q",
		engine.RunWorkflowType, task.SearchAttrOrkflowConcurrencyHeld, gate, task.SearchAttrOrkflowConcurrencyQueued, gate, "Running")
	resp, err := s.client.CountWorkflow(s.ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	if err != nil {
		return 0
	}
	return resp.GetCount()
}
