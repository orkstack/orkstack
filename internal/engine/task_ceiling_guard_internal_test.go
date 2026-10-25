package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The landing arrangements of a flat stage (spec 7.4, T7).
const (
	// landLate: no completion lands until the hold starts; each then lands
	// and is collected together (c events).
	landLate = "late"
	// landInPass: no completion lands until the dispatch that makes the
	// budget due; during that dispatch's publishes every running task's
	// completion lands at once (u each), so the loop meets them uncollected
	// at its next guard check — the closed form's worst case.
	landInPass = "pass"
	// landRandom: completions land at random points of the dispatch pass and
	// of the hold, from a seed.
	landRandom = "random"
)

// flatStageRun drives a flat stage of plain tasks through RunWorkflow in a
// test environment whose raw history length moves exactly as 7.4's measured
// costs say: d per dispatch (TASK_SCHEDULED 7, TASK_STARTED with the
// ExecuteTask scheduling event 7), u when a completion lands, and c − u when
// the loop collects a landed one (c for one that had not landed). Every
// generation starts from a base of B = RestoreHistoryBound + generation0Start.
// No ExecuteTask completes until the generation's dispatch pass is over and
// the loop reaches its Selector, which is where a hold that started during
// the pass first waits; landings are moves of the raw length, at the
// publishes the arrangement picks.
type flatStageRun struct {
	t     *testing.T
	runID string
	wf    *orkstackv1.Orkflow
	plan  RunPlan
	mode  string
	rng   *rand.Rand
	store *memSnapshotStore
	base  int

	mu sync.Mutex
	// This generation's environment and raw length.
	env    *testsuite.TestWorkflowEnvironment
	length int
	// dispatched counts this generation's dispatches and arrived the
	// ExecuteTask activities that started; release closes when the loop
	// first reaches its Selector. landAt is the dispatch whose publishes
	// landPass lands every running completion at: the closed form's ⌈G/d⌉,
	// or the last task left.
	dispatched, arrived, landAt int
	release                     chan struct{}
	released                    bool
	running                     map[string]bool // started, not collected
	landed                      map[string]bool
	// landRandom: the chance a publish lands some completions, and the
	// dispatch of the pass from which they may (any point of the hold).
	landing  float64
	landFrom int
	// Across the run.
	generation  int
	total       int   // dispatches over every generation
	genDispatch []int // dispatches per generation
	peaks       []int64
	maxLanded   int
}

// flatStageRuns lets the guard probe find a run by its id: seeds run in
// parallel, each under its own run id.
var flatStageRuns sync.Map

func newFlatStageRun(t *testing.T, tasks int, mode string, seed uint64) *flatStageRun {
	wf := &orkstackv1.Orkflow{Name: "flat", Version: "v1", Namespace: "team-a"}
	for i := range tasks {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"}})
	}
	plan := refusalRoutedPlan(t)
	inFlight := DefaultInFlightLimit(DefaultRunMaxInFlight)
	plan.InFlight = &inFlight
	plan.Limits = WorkspaceLimits{ValuesMaxBytes: 4096}
	r := &flatStageRun{
		t: t, runID: fmt.Sprintf("ork-flat-%s-%d-%d", mode, tasks, seed), wf: wf, plan: plan, mode: mode,
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		store: newMemSnapshotStore(),
		base:  int(RestoreHistoryBound(*plan.Continuation) + generation0Start),
	}
	r.landing = r.rng.Float64() * 0.2
	r.landFrom = r.rng.IntN(r.stageDueAt() + 1)
	return r
}

// stageDueAt is the closed form's ⌈G/d⌉: the dispatches after which a
// generation's budget is due.
func (r *flatStageRun) stageDueAt() int {
	return int(ceilDiv(r.plan.Continuation.Events, PlainDispatchEvents))
}

func (r *flatStageRun) stageSetLength(length int) {
	r.length = length
	r.env.SetCurrentHistoryLength(length)
}

// stageLand lands the completion of every running task in ids not landed yet.
func (r *flatStageRun) stageLand(ids []string) {
	for _, id := range ids {
		if r.running[id] && !r.landed[id] {
			r.landed[id] = true
			r.length += int(LandedCompletionEvents)
		}
	}
	waiting := 0
	for id := range r.running {
		if r.landed[id] {
			waiting++
		}
	}
	r.maxLanded = max(r.maxLanded, waiting)
	r.env.SetCurrentHistoryLength(r.length)
}

func (r *flatStageRun) stageRunningIDs() []string {
	ids := make([]string, 0, len(r.running))
	for _, task := range r.wf.GetTasks() { // declaration order: the seed decides, not map order
		if r.running[task.GetId()] {
			ids = append(ids, task.GetId())
		}
	}
	return ids
}

// maybeLandRandom, for landRandom, lands a random subset of the running
// completions at this publish, with the seed's chance.
func (r *flatStageRun) stageMaybeLand() {
	if r.mode != landRandom || (!r.released && r.dispatched < r.landFrom) || r.rng.Float64() >= r.landing {
		return
	}
	var pick []string
	for _, id := range r.stageRunningIDs() {
		if r.rng.IntN(2) == 0 {
			pick = append(pick, id)
		}
	}
	r.stageLand(pick)
}

func (r *flatStageRun) stagePublish(_ context.Context, event *orkstackv1.Event, _, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_STARTED:
		r.stageSetLength(r.base)
	case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED:
		r.dispatched++
		r.total++
		r.stageSetLength(r.length + 7)
		r.stageMaybeLand()
	case orkstackv1.EventType_EVENT_TYPE_TASK_STARTED:
		r.stageSetLength(r.length + int(PlainDispatchEvents) - 7)
		if r.mode == landInPass && r.dispatched == r.landAt {
			r.stageLand(r.stageRunningIDs())
		}
		// A TASK_STARTED event means the task started; the activity's own
		// goroutine may not have run yet under load. Counting it after the
		// landing dispatch's own land makes the arrangement a function of the
		// event order alone: the task starting at that dispatch is the one
		// the closed form does not yet count as landed.
		if id := event.GetTaskId(); id != "" {
			r.running[id] = true
		}
		r.stageMaybeLand()
	case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
		id := event.GetTaskId()
		collect := PlainCompletionEvents
		if r.landed[id] {
			collect -= LandedCompletionEvents
		}
		delete(r.running, id)
		delete(r.landed, id)
		r.stageSetLength(r.length + int(collect))
		r.stageMaybeLand()
	}
	return nil
}

func (r *flatStageRun) stageExecute(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	r.mu.Lock()
	r.arrived++
	r.running[in.GetTaskId()] = true
	release := r.release
	r.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
}

// stageGeneration builds generation g's environment.
func (r *flatStageRun) stageGeneration(remaining int) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(5 * time.Minute)
	env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	activities := NewSnapshotActivities(r.store)
	// An event's own snapshot is never read here: a name is enough, and a
	// flat stage of thousands of rows would otherwise be kept thousands of
	// times over.
	env.RegisterActivityWithOptions(func(_ context.Context, _ *orkstackv1.Run, sequence int64) (string, error) {
		return fmt.Sprintf("mem://snapshot/%d", sequence), nil
	}, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(activities.WriteFailedRunSnapshot, activity.RegisterOptions{Name: ActivityWriteFailedRunSnapshot})
	env.RegisterActivityWithOptions(activities.WriteEventSegment, activity.RegisterOptions{Name: ActivityWriteEventSegment})
	env.RegisterActivityWithOptions(activities.WriteSwitchSnapshot, activity.RegisterOptions{Name: ActivityWriteSwitchSnapshot})
	env.RegisterActivityWithOptions(activities.WriteCheckpoint, activity.RegisterOptions{Name: ActivityWriteCheckpoint})
	env.RegisterActivityWithOptions(activities.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	env.RegisterActivityWithOptions(r.stagePublish, activity.RegisterOptions{Name: ActivityPublishEvent})
	env.RegisterActivityWithOptions(r.stageExecute, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: r.runID})
	r.mu.Lock()
	r.env = env
	r.dispatched, r.arrived = 0, 0
	r.landAt = min(r.stageDueAt(), remaining)
	r.release, r.released = make(chan struct{}), false
	r.running, r.landed = map[string]bool{}, map[string]bool{}
	r.peaks = append(r.peaks, 0)
	if r.generation > 0 {
		r.stageSetLength(r.base) // a carried generation's base, as a slow restore leaves it
	}
	r.mu.Unlock()
	return env
}

// stageAtSelector releases every ExecuteTask once the generation's pass is over.
func (r *flatStageRun) stageAtSelector() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.released {
		r.released = true
		close(r.release)
	}
}

// stageObserve is the guard probe's view of this run: the largest history plus
// reserve of the current generation.
func (r *flatStageRun) stageObserve(events, reserve int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peaks[len(r.peaks)-1] = max(r.peaks[len(r.peaks)-1], events+reserve)
}

// drive takes the run through its generations, or only its first when
// firstOnly; it returns the last generation's workflow error.
func (r *flatStageRun) drive(firstOnly bool) error {
	r.t.Helper()
	flatStageRuns.Store(r.runID, r)
	defer flatStageRuns.Delete(r.runID)
	args := []any{r.runID, r.wf, (*orkstackv1.Payload)(nil), r.plan, (*orkstackv1.RunContinuation)(nil)}
	tasks := len(r.wf.GetTasks())
	for r.generation = 0; r.generation < 64; r.generation++ {
		env := r.stageGeneration(tasks - r.total)
		env.ExecuteWorkflow(RunWorkflowType, args...)
		if !env.IsWorkflowCompleted() {
			r.t.Fatalf("generation %d did not end", r.generation)
		}
		r.mu.Lock()
		r.genDispatch = append(r.genDispatch, r.dispatched)
		r.mu.Unlock()
		err := env.GetWorkflowError()
		var can *workflow.ContinueAsNewError
		if !errors.As(err, &can) || firstOnly {
			return err
		}
		var (
			nextID    string
			nextWf    *orkstackv1.Orkflow
			nextInput *orkstackv1.Payload
			nextPlan  RunPlan
			carry     *orkstackv1.RunContinuation
		)
		if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &nextID, &nextWf, &nextInput, &nextPlan, &carry); err != nil {
			r.t.Fatalf("decode generation %d's continue-as-new arguments: %v", r.generation, err)
		}
		args = []any{nextID, nextWf, nextInput, nextPlan, carry}
	}
	r.t.Fatal("the run did not end within 64 generations")
	return nil
}

// installGuardProbe routes every guard check, and every arrival at the
// Selector, to its run.
func installGuardProbe(t *testing.T) {
	historyGuardProbeForTest = func(runID string, events, reserve int64) {
		if r, ok := flatStageRuns.Load(runID); ok {
			r.(*flatStageRun).stageObserve(events, reserve)
		}
	}
	readyLoopProbeForTest = &readyLoopProbe{selectorRun: func(runID string) {
		if r, ok := flatStageRuns.Load(runID); ok {
			r.(*flatStageRun).stageAtSelector()
		}
	}}
	t.Cleanup(func() { historyGuardProbeForTest, readyLoopProbeForTest = nil, nil })
}

// isHistoryLimit reports whether err is the guard's RUN_HISTORY_LIMIT.
func isRunHistoryLimit(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == RunHistoryLimitCode
}

// predictedPeak is 7.4's B + F + p·T + Δ(W) for the first generation of a
// flat stage of tasks at the defaults.
func predictedPeak(r *flatStageRun, tasks int) int64 {
	limit, budget := *r.plan.History, *r.plan.Continuation
	return ShapePeak(limit, budget, int64(r.plan.InFlight.Max), int64(r.base), int64(tasks))
}

// TestHistoryGuard_FlatStageAtTheClosedFormMaximum: a flat stage of
// ShapeMaxTasks(defaults) plain tasks, from a base of B, completes across
// generations without RUN_HISTORY_LIMIT whether its completions land late
// (a) or all at once during the dispatch that makes the budget due (b, the
// worst case). With one task more, (b) fails RUN_HISTORY_LIMIT in generation
// 0 at the guard check right after that dispatch, before any hold.
func TestHistoryGuard_FlatStageAtTheClosedFormMaximum(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test: every proof's unit gate runs it; make check skips it")
	}
	installGuardProbe(t)
	limit, budget := ceilingDefaults()
	tasks := int(ShapeMaxTasks(limit, budget, int64(DefaultRunMaxInFlight)))
	if tasks != 4222 {
		t.Fatalf("ShapeMaxTasks at the defaults = %d, want 4,222", tasks)
	}
	for _, mode := range []string{landLate, landInPass} {
		t.Run(mode, func(t *testing.T) {
			r := newFlatStageRun(t, tasks, mode, 1)
			if err := r.drive(false); err != nil {
				t.Fatalf("a flat stage of %d tasks, landings %s: %v", tasks, mode, err)
			}
			if r.total != tasks {
				t.Fatalf("%d dispatches over the run, want each of %d tasks once", r.total, tasks)
			}
			if got := r.genDispatch[0]; got != r.stageDueAt() {
				t.Fatalf("generation 0 dispatched %d tasks before its hold, want ⌈G/d⌉ = %d", got, r.stageDueAt())
			}
			predicted := predictedPeak(r, tasks)
			t.Logf("E7 flat stage of %d tasks, landings %s: %d generations %v; generation 0's peak of history "+
				"plus reserve %d (predicted %d, limit %d); most landed and uncollected %d",
				tasks, mode, len(r.genDispatch), r.genDispatch, r.peaks[0], predicted, limit.MaxEvents, r.maxLanded)
			for g, peak := range r.peaks {
				if peak > predicted || peak >= limit.MaxEvents {
					t.Fatalf("generation %d's peak %d is over the predicted %d (limit %d)", g, peak, predicted, limit.MaxEvents)
				}
			}
		})
	}
	t.Run("one task more, landing in the pass", func(t *testing.T) {
		r := newFlatStageRun(t, tasks+1, landInPass, 1)
		err := r.drive(true)
		if !isRunHistoryLimit(err) {
			t.Fatalf("a flat stage of %d tasks = %v, want RUN_HISTORY_LIMIT in generation 0", tasks+1, err)
		}
		if got := r.genDispatch[0]; got != r.stageDueAt() {
			t.Fatalf("it failed after %d dispatches, want right after the %dth, before any hold", got, r.stageDueAt())
		}
		t.Logf("E7 one task over (%d): RUN_HISTORY_LIMIT in generation 0 after %d dispatches, peak %d", tasks+1, r.genDispatch[0], r.peaks[0])
	})
}

// TestHistoryGuard_CompletionsLandingDuringThePassNeverTripTheGuard: a flat
// stage of ShapeMaxTasks(defaults) tasks whose completions land at random
// points of the dispatch pass and of the hold, over 200 seeds: no seed ends
// RUN_HISTORY_LIMIT, and the largest history plus reserve seen at any guard
// check is at most B + F + p·T + Δ(W). Each seed runs the first generation,
// the worst one (7.4): every later one starts from the same base with fewer
// tasks PENDING.
func TestHistoryGuard_CompletionsLandingDuringThePassNeverTripTheGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test: every proof's unit gate runs it; make check skips it")
	}
	installGuardProbe(t)
	limit, budget := ceilingDefaults()
	tasks := int(ShapeMaxTasks(limit, budget, int64(DefaultRunMaxInFlight)))
	const seeds = 200
	var mu sync.Mutex
	var worst int64
	var worstSeed uint64
	var predicted int64
	t.Run("seeds", func(t *testing.T) {
		for seed := uint64(1); seed <= seeds; seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				r := newFlatStageRun(t, tasks, landRandom, seed)
				err := r.drive(true)
				var can *workflow.ContinueAsNewError
				if !errors.As(err, &can) {
					t.Fatalf("seed %d: generation 0 ended %v, want it to carry on", seed, err)
				}
				want := predictedPeak(r, tasks)
				mu.Lock()
				predicted = want
				if r.peaks[0] > worst {
					worst, worstSeed = r.peaks[0], seed
				}
				mu.Unlock()
				if r.peaks[0] > want {
					t.Fatalf("seed %d: peak %d over the predicted %d", seed, r.peaks[0], want)
				}
			})
		}
	})
	t.Logf("E7 %d seeds of a %d-task flat stage: largest history plus reserve %d (seed %d), predicted %d, limit %d",
		seeds, tasks, worst, worstSeed, predicted, limit.MaxEvents)
}
