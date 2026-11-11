package engine_test

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// rampPublishLatency is what one PublishEvent takes on the workflow's clock
// in the ramp tests, the unit their bounds are stated in: a round trip (a
// real cluster's take 12–49 ms, and each awaited one costs a workflow task of
// 14–50 ms besides). It is long, and under eventActivityTimeout, because the
// test environment moves its clock by the wall clock while a mocked activity
// has started and not yet blocked on its delay: with a short latency, a slow
// host (the race detector, 500 activities starting at once) let the clock run
// ahead of the round trips (5.0 s of a 3 s bound at 100 ms). At 9 s no
// activity takes that long to block, so the clock moves only when the
// workflow waits, and the measure counts round trips on any host.
const rampPublishLatency = 9 * time.Second

// rampDocument is C25's shape at its size: n plain tasks that all start
// together and a fan-out over the input's parts, declared last, so one pass
// dispatches every task and then the fan-out's items up to the cap.
func rampDocument(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: ramp\ninputs:\n  parts: {type: array}\ntasks:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: f%03d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	b.WriteString("  - id: spread\n    uses: shell\n    needs: []\n" +
		"    each: {over: \"{{ .inputs.parts }}\", max_items: 1000}\n" +
		"    with: {op: exec, part: \"{{ .each.item }}\"}\n")
	return b.String()
}

// rampClock records, from the snapshots a run writes, when the run started
// and when each item of spread was scheduled, on the workflow's clock.
type rampClock struct {
	mu        sync.Mutex
	started   time.Time
	scheduled map[int32]time.Time
}

func (c *rampClock) observe(run *orkstackv1.Run, _ int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if run.GetStartedAt() != nil {
		c.started = run.GetStartedAt().AsTime()
	}
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() != "spread" {
			continue
		}
		for _, item := range row.GetItems() {
			if item.GetScheduledAt() != nil {
				c.scheduled[item.GetItem()] = item.GetScheduledAt().AsTime()
			}
		}
	}
	return nil
}

// reachedCap is how long after the run started its in-flight work first
// reached limit: when the item that took it there was scheduled.
func (c *rampClock) reachedCap(t *testing.T, tasks int, limit int32) time.Duration {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	index := limit - int32(tasks) - 1
	at, ok := c.scheduled[index]
	if !ok || c.started.IsZero() {
		t.Fatalf("item %d (the one that reaches the %d cap) was never seen scheduled", index, limit)
	}
	return at.Sub(c.started)
}

// runRamp runs rampDocument(tasks) over items parts under plan, every
// PublishEvent taking rampPublishLatency, and returns how long the run took
// to first reach its cap and the most work it kept pending.
func runRamp(t *testing.T, runID string, tasks, items int, plan engine.RunPlan) (time.Duration, int) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, itemBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{DeadlockDetectionTimeout: productionDeadlock()}))
	h.env.SetTestTimeout(5 * time.Minute)
	h.env.OnActivity(engine.ActivityPublishEvent, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		After(rampPublishLatency).Return(nil)
	clock := &rampClock{scheduled: map[int32]time.Time{}}
	peak := &runningPeak{}
	h.onSnapshot = func(run *orkstackv1.Run, seq int64) error {
		_ = peak.observe(run, seq)
		return clock.observe(run, seq)
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, rampDocument(tasks)), spreadInput(t, items), plan,
		(*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	if got := len(h.invocations()); got != tasks+items {
		t.Fatalf("%d activities ran, want %d tasks and %d items", got, tasks, items)
	}
	return clock.reachedCap(t, tasks, plan.InFlight.Max), peak.value()
}

// rampPlan is a plan with C25's 500 in-flight cap and no history limit:
// TestWorkflowEnvironment never moves the history counters, so a guarded
// plan would over-count a long pass. With window set it pins the detached
// publish path's numbers, as every plan built at submit now does.
func rampPlan(window bool) engine.RunPlan {
	plan := defaultPlan()
	limit := engine.DefaultInFlightLimit(500)
	if !window {
		limit = engine.InFlightLimit{Max: 500, HardStop: engine.DefaultPendingActivityHardStop}
	}
	plan.InFlight = &limit
	return plan
}

// TestDispatchRamp_AWideStageReachesItsCapInAFewRoundTrips is E77: 300 plain
// tasks that start together and a fan-out, under a 500 cap, with every
// PublishEvent taking rampPublishLatency on the workflow's clock. On the
// detached publish path the run reaches its cap within 30 publish latencies
// of its start (22) — a window of 32 publishes is one round trip — where
// awaiting each task's two publishes took 603 (60.9 s on a real cluster). It
// never keeps more than its cap pending, and every task and item runs.
func TestDispatchRamp_AWideStageReachesItsCapInAFewRoundTrips(t *testing.T) {
	ramp, peak := runRamp(t, "ork-ramp-detached", 300, 400, rampPlan(true))
	t.Logf("cap reached %v after the run started, at %v per publish; peak pending %d", ramp, rampPublishLatency, peak)
	if bound := 30 * rampPublishLatency; ramp > bound {
		t.Fatalf("the cap was reached %v after the run started, over %v (30 publish round trips)", ramp, bound)
	}
	if peak > 500 {
		t.Fatalf("%d pending at once, over the cap of 500", peak)
	}
}

// TestDispatchRamp_APlanWithoutTheWindowAwaitsEveryPublish: a plan that pins
// no publish window — every plan built before the detached path — takes the
// awaited path, one publish round trip after another: 603 of them before the
// cap. It is the cause E77 measured, and what a run submitted before the
// change still does when it replays.
func TestDispatchRamp_APlanWithoutTheWindowAwaitsEveryPublish(t *testing.T) {
	ramp, _ := runRamp(t, "ork-ramp-awaited", 300, 400, rampPlan(false))
	if want := 603 * rampPublishLatency; ramp < want {
		t.Fatalf("the cap was reached %v after the run started, before %v: the awaited path did not await", ramp, want)
	}
}

// TestDispatchRamp_ABudgetBelowOneTaskEndsAWorkflowTaskPerTask: the byte
// budget decides how many tasks one workflow task schedules. Below one
// task's input, each dispatch after the first waits for the newest publish —
// one round trip per task, like the awaited path's two — and the run still
// reaches its cap and completes.
func TestDispatchRamp_ABudgetBelowOneTaskEndsAWorkflowTaskPerTask(t *testing.T) {
	plan := rampPlan(true)
	plan.InFlight.DispatchBytes = 1
	ramp, _ := runRamp(t, "ork-ramp-one-byte", 300, 400, plan)
	if want := 299 * rampPublishLatency; ramp < want {
		t.Fatalf("the cap was reached %v after the run started, before %v: the budget did not end a workflow task per task", ramp, want)
	}
	if over := 450 * rampPublishLatency; ramp > over {
		t.Fatalf("the cap was reached %v after the run started, after %v: more than one wait per task", ramp, over)
	}
}

// publishGatePlan is a plan with an in-flight cap of 16 and, when window is
// set, the detached publish path's numbers.
func publishGatePlan(window bool) engine.RunPlan {
	plan := defaultPlan()
	limit := engine.DefaultInFlightLimit(16)
	if !window {
		limit.PublishWindow, limit.DispatchBytes = 0, 0
	}
	plan.InFlight = &limit
	return plan
}

// runRecordingCommands runs doc under plan and returns every command it
// issued and every version it asked.
func runRecordingCommands(t *testing.T, runID, doc string, input *orkstackv1.Payload, plan engine.RunPlan) ([]string, *dispatchCapture) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, itemBehavior)
	recorder, capture := &commandRecorder{}, &dispatchCapture{}
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{recorder, capture}}))
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, doc), input, plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	return recorder.list(), capture
}

// TestTaskPublish_TheGateIsAskedOnceBeforeTheFirstTaskEvent: a plan that
// pins the publish window asks TaskPublishChange once, at the first plain
// task, right after the lean event path's own marker and before that task's
// first snapshot; a plan without it, and a run with no plain task, never
// ask.
func TestTaskPublish_TheGateIsAskedOnceBeforeTheFirstTaskEvent(t *testing.T) {
	commands, capture := runRecordingCommands(t, "ork-publish-gate", flatDocument(3), payload(t, nil), publishGatePlan(true))
	if asked := capture.versionsAsked(engine.TaskPublishChange); len(asked) != 1 || asked[0] != 1 {
		t.Fatalf("%s was answered %v, want exactly one question answered 1", engine.TaskPublishChange, asked)
	}
	at := slices.Index(commands, "version:"+engine.TaskPublishChange)
	if at < 1 || commands[at-1] != "version:lean-event-path" || at+1 >= len(commands) ||
		commands[at+1] != "local:"+engine.ActivityWriteSnapshot {
		t.Fatalf("the marker is not between the lean path's marker and the first task's snapshot: %v", commands)
	}
	if _, capture := runRecordingCommands(t, "ork-publish-no-window", flatDocument(3), payload(t, nil), publishGatePlan(false)); len(capture.versionsAsked(engine.TaskPublishChange)) != 0 {
		t.Fatalf("a plan without the window asked %s", engine.TaskPublishChange)
	}
	if _, capture := runRecordingCommands(t, "ork-publish-fan-out", wideFanOutDocument, spreadInput(t, 4), publishGatePlan(true)); len(capture.versionsAsked(engine.TaskPublishChange)) != 0 {
		t.Fatalf("a run with no plain task asked %s", engine.TaskPublishChange)
	}
}

// TestTaskPublish_ACarriedGenerationKeepsTheDecision: a run that carries on
// at every idle point dispatches each task of a chain in a generation of its
// own; the decision crosses in the carry and is asked once over the run.
func TestTaskPublish_ACarriedGenerationKeepsTheDecision(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	capture := &dispatchCapture{}
	plan := publishGatePlan(true)
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
		MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget
	g := runGenerations(t, "run-carried-publish", compile(t, retryChainDocument), payload(t, nil), plan,
		itemBehavior, func(h *harness, _ int) {
			h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
		})
	if g.err != nil {
		t.Fatalf("the run ended with %v", g.err)
	}
	if len(g.carries) < 2 {
		t.Fatalf("the run carried on %d times, want at least 2", len(g.carries))
	}
	if got := g.carries[len(g.carries)-1].GetVersionDecisions()[engine.TaskPublishChange]; got != 1 {
		t.Errorf("the last carry holds %s = %d, want the decision 1", engine.TaskPublishChange, got)
	}
	if asked := capture.versionsAsked(engine.TaskPublishChange); len(asked) != 1 {
		t.Errorf("%s was asked %v across the generations, want exactly one question", engine.TaskPublishChange, asked)
	}
}
