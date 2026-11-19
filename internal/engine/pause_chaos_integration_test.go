//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The pause under chaos (E103's integration tests 2, 6, 15, 16 and 25): an
// engine replica killed while a run is paused and while it applies a pause,
// an unpause racing a task's failure and the pause's own expiry, and a
// replica killed while a pause cascades to child runs. Every window in which
// nothing may start also observes the run making progress in it (AGENTS.md
// §11.1), so a slow host cannot pass one.

// heldSnapshots is a replica's snapshot store whose event snapshot writes go
// through hook first once hold says so for their run. The write is a local
// activity of the workflow task that emits the event, so a write held there
// holds that task open: stopping the replica then leaves the cluster with no
// decision from it. It is a hand-written fake for one fault the real object
// store cannot produce on demand: a write that stays in flight, at a chosen
// point of the workflow task, until the test releases it. Every other call
// goes to the real store (AGENTS.md §11.1).
type heldSnapshots struct {
	engine.SnapshotStore
	hook *blockingHook
	hold func(runID string) bool
}

func (h *heldSnapshots) WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	if h.hold(runID) {
		if err := h.hook.call("event-snapshot", sequence); err != nil {
			return "", err
		}
	}
	return h.SnapshotStore.WriteSnapshot(ctx, namespace, runID, sequence, data)
}

func (h *heldSnapshots) WriteSnapshotOverlay(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	if h.hold(runID) {
		if err := h.hook.call("event-overlay", sequence); err != nil {
			return "", err
		}
	}
	return h.SnapshotStore.WriteSnapshotOverlay(ctx, namespace, runID, sequence, data)
}

// pauseChaosDoc: a runs (its Orker holds it until the test releases it) and
// b needs a.
const pauseChaosDoc = `apiVersion: orkstack/v1
kind: Orkflow
name: pause-chaos
version: "1.0.0"
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
`

// pauseChaosHandlerDoc is pauseChaosDoc with h, a failure handler of a.
const pauseChaosHandlerDoc = pauseChaosDoc + `  - id: h
    uses: shell
    needs: [a]
    join: any_failed
    with: {op: exec}
`

// pausePlan is the stack's plan with pausing pinned: no continuation, the
// default bounds, and no paused-run index (the scenario's namespace
// registers none).
func (s *chaosStack) pausePlan() engine.RunPlan {
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil)
	p := engine.DefaultPausePlan(7*24*3600, 31*24*3600)
	plan.Pause = &p
	return plan
}

// sendPause sends the API's pause (or unpause) request to runID, as
// engine.TemporalStarter.SignalPause does, and returns once the cluster
// recorded it.
func (s *chaosStack) sendPause(runID string, pause bool, requestID string, timeoutSeconds int64) {
	s.t.Helper()
	request := map[string]any{"format": 1, "pause": pause, "principal": "chaos", "reason": "chaos",
		"requestId": requestID, "timeoutSeconds": fmt.Sprint(timeoutSeconds)}
	must.OKf(s.t, s.control.SignalWorkflow(s.ctx, runID, "", engine.PauseSignal, request), "signal %s", requestID)
	s.transcript.step("%s sent (pause %t)", requestID, pause)
}

// awaitPublished polls runID's current history until one of its published
// events matches want, and returns it.
func (s *chaosStack) awaitPublished(runID, what string, within time.Duration, want func(*orkstackv1.Event) bool) *orkstackv1.Event {
	s.t.Helper()
	deadline := time.Now().Add(within)
	for {
		for _, e := range publishedOf(s.t, s.history(runID, "")) {
			if want(e) {
				return e
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s published no %s within %s", runID, what, within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func ofType(typ orkstackv1.EventType) func(*orkstackv1.Event) bool {
	return func(e *orkstackv1.Event) bool { return e.GetType() == typ }
}

// countPublished is how many of h's published events are of type typ.
func countPublished(t *testing.T, h *historypb.History, typ orkstackv1.EventType) int {
	t.Helper()
	n := 0
	for _, e := range publishedOf(t, h) {
		if e.GetType() == typ {
			n++
		}
	}
	return n
}

// taskEnded reports whether h published task's end (TASK_SUCCEEDED or
// TASK_FAILED): the engine decided on its attempt's result.
func taskEnded(t *testing.T, h *historypb.History, task string) bool {
	t.Helper()
	for _, e := range publishedOf(t, h) {
		if e.GetTaskId() == task && (e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED ||
			e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED) {
			return true
		}
	}
	return false
}

// activityClosed reports whether h records any ExecuteTask activity that
// completed or failed: an attempt's result recorded by the cluster, whether
// or not a replica has decided on it yet.
func activityClosed(h *historypb.History) bool {
	for _, e := range h.GetEvents() {
		if e.GetActivityTaskCompletedEventAttributes() != nil || e.GetActivityTaskFailedEventAttributes() != nil {
			return true
		}
	}
	return false
}

// requireHeldWithProgress watches runID for window: held tasks may not be
// dispatched in it, and progress must report true before it ends, so the
// window cannot pass on a run that simply stalled.
func (s *chaosStack) requireHeldWithProgress(runID string, window time.Duration, progress func(h *historypb.History) bool, held ...string) {
	s.t.Helper()
	end := time.Now().Add(window)
	progressed := false
	for time.Now().Before(end) {
		h := s.history(runID, "")
		dispatched := dispatchedTasks(s.t, h)
		for _, task := range held {
			if attempts, _ := s.attemptsOf(task); attempts > 0 || dispatched[task] {
				s.t.Fatalf("%s was dispatched while the run was paused", task)
			}
		}
		progressed = progressed || progress(h)
		time.Sleep(200 * time.Millisecond)
	}
	if !progressed {
		s.t.Fatalf("the run made no progress in the %s window, so its nothing-started proves nothing", window)
	}
	s.transcript.step("held %v for %s while the run made progress", held, window)
}

// TestIntegration_PauseEngineKilledWhilePaused (2): the replica running a
// paused run is stopped; the task that was running completes with no replica
// up; a second replica takes the run over, decides on that completion and
// still starts nothing, until the unpause lets b run and the run succeed.
// The pause and the unpause are each published once.
func TestIntegration_PauseEngineKilledWhilePaused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "pause-killed")
	g := newGates()
	a := s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "a"))
	runID := s.recordedRunID()
	s.start(runID, compile(t, pauseChaosDoc), nil, s.pausePlan())
	g.waitEntered(t, "a", time.Minute)
	s.sendPause(runID, true, "pause-1", 3600)
	s.awaitPublished(runID, "RUN_PAUSED", time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))

	a.stop()
	s.transcript.step("replica-a stopped with the run paused and a running")
	g.open("a")
	must.OKf(t, waitFor(ctx, "a's result recorded with no replica up", func() (bool, error) {
		return activityClosed(s.history(runID, "")), nil
	}), "wait")
	s.startReplica("replica-b", replicaOptions{})
	// The positive control: replica-b decides on a's result inside the
	// window (a's end is published), and b still does not start.
	s.requireHeldWithProgress(runID, 20*time.Second, func(h *historypb.History) bool {
		return taskEnded(t, h, "a")
	}, "b")

	s.sendPause(runID, false, "unpause-1", 0)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	h := s.history(runID, "")
	requirePauseOnce(t, h)
	if attempts, _ := s.attemptsOf("b"); attempts != 1 {
		t.Fatalf("b ran %d times after the unpause, want once", attempts)
	}
	must.OKf(t, replayRecorded(h, runID), "the history does not replay")
}

// TestIntegration_PauseEngineKilledMidApply (16): the replica applying a
// pause is stopped inside that workflow task (its RUN_PAUSED snapshot write
// held), so the cluster records no decision; a second replica takes the task
// over and applies the pause exactly once. Only then is the first replica's
// late write released. Nothing new starts until the unpause.
func TestIntegration_PauseEngineKilledMidApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "pause-mid-apply")
	var armed atomic.Bool
	hook := newBlockingHook(func(string, int64) bool { return true })
	t.Cleanup(func() { hook.open(nil) })
	g := newGates()
	a := s.startReplica("replica-a", replicaOptions{store: &heldSnapshots{SnapshotStore: s.store, hook: hook,
		hold: func(string) bool { return armed.Load() }}})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "a"))
	runID := s.recordedRunID()
	s.start(runID, compile(t, pauseChaosDoc), nil, s.pausePlan())
	g.waitEntered(t, "a", time.Minute)
	// a is held, so the run writes nothing until the pause: the next
	// snapshot write is the pause's own.
	armed.Store(true)
	s.sendPause(runID, true, "pause-1", 3600)
	entered := hook.waitEntered(t, time.Minute)
	before := int64(len(s.history(runID, "").GetEvents()))
	s.startReplica("replica-b", replicaOptions{})
	a.stop()
	if since := time.Since(entered); since > 5*time.Second {
		t.Fatalf("replica-a was stopped %s after its apply's write entered the hook, outside the 5 s window", since)
	}
	s.transcript.step("replica-a stopped inside the pause's workflow task")
	s.awaitPublished(runID, "RUN_PAUSED", 2*time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))
	if identitiesAfter(s.history(runID, ""), before)["replica-b"] == 0 {
		t.Fatal("RUN_PAUSED was published, but replica-b took no workflow task of the run")
	}
	hook.open(nil)
	s.transcript.step("replica-a's late write released")

	// a's end, published while paused, is the progress the window observes.
	g.open("a")
	s.requireHeldWithProgress(runID, 15*time.Second, func(h *historypb.History) bool {
		return taskEnded(t, h, "a")
	}, "b")
	s.sendPause(runID, false, "unpause-1", 0)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	h := s.history(runID, "")
	requirePauseOnce(t, h)
	must.OKf(t, replayRecorded(h, runID), "the history does not replay")
}

// failingGate holds "a" until the test releases it and then fails it; every
// other task is answered at once.
func failingGate(g *gates) chaosBehavior {
	return func(ctx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "a" {
			if err := g.hold(ctx, "a"); err != nil {
				return nil, err
			}
			return nil, errors.New("a failed under chaos")
		}
		return instant(ctx, s, orker, in)
	}
}

// TestIntegration_PauseUnpauseRacesFailure (6): a paused run's running task
// fails as its unpause arrives, in both orders and at once. Every way the
// two land, the run ends FAILED with a's own cause, its failure handler
// runs exactly once, b never runs, and the pause and the unpause are each
// published once.
func TestIntegration_PauseUnpauseRacesFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	for _, gap := range []time.Duration{-300 * time.Millisecond, -50 * time.Millisecond, 0, 50 * time.Millisecond, 300 * time.Millisecond} {
		t.Run(fmt.Sprintf("unpause %+dms after the failure", gap.Milliseconds()), func(t *testing.T) {
			s := newChaosStack(t, ctx, "pause-race-failure")
			g := newGates()
			s.startReplica("replica-a", replicaOptions{})
			s.startProjection(nil)
			s.startOrker("orker-1", failingGate(g))
			runID := s.recordedRunID()
			s.start(runID, compile(t, pauseChaosHandlerDoc), nil, s.pausePlan())
			g.waitEntered(t, "a", time.Minute)
			s.sendPause(runID, true, "pause-1", 3600)
			s.awaitPublished(runID, "RUN_PAUSED", time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))

			fail := func() { g.open("a") }
			unpause := func() { s.sendPause(runID, false, "unpause-1", 0) }
			first, second, wait := fail, unpause, gap
			if gap < 0 {
				first, second, wait = unpause, fail, -gap
			}
			first()
			time.Sleep(wait)
			second()

			if status, _ := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
				t.Fatalf("the run closed %v, want failed", status)
			}
			run, err := s.reader.GetRun(ctx, runID)
			must.OKf(t, err, "GetRun")
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() == engine.RunPauseTimeoutCode {
				t.Fatalf("the run reads %v with %q, want FAILED with a's own cause", run.GetStatus(), run.GetError().GetCode())
			}
			if attempts, _ := s.attemptsOf("h"); attempts != 1 {
				t.Fatalf("the failure handler ran %d times, want once", attempts)
			}
			if attempts, _ := s.attemptsOf("b"); attempts != 0 {
				t.Fatalf("b ran %d times, want never", attempts)
			}
			h := s.history(runID, "")
			requirePauseOnce(t, h)
			must.OKf(t, replayRecorded(h, runID), "the history does not replay")
		})
	}
}

// TestIntegration_PauseUnpauseRacesExpiry (15): an unpause sent around the
// instant a pause expires. Either it lands first, and b runs and the run
// succeeds, or the expiry does, and the run fails RUN_PAUSE_TIMEOUT with b
// never started and the unpause changing nothing: never both, never
// neither, and what the run reads agrees with what it published.
func TestIntegration_PauseUnpauseRacesExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	outcomes := map[string]int{}
	for _, offset := range []time.Duration{-400 * time.Millisecond, -100 * time.Millisecond, 0, 100 * time.Millisecond, 400 * time.Millisecond} {
		t.Run(fmt.Sprintf("unpause at expiry %+dms", offset.Milliseconds()), func(t *testing.T) {
			s := newChaosStack(t, ctx, "pause-race-expiry")
			g := newGates()
			s.startReplica("replica-a", replicaOptions{})
			s.startProjection(nil)
			s.startOrker("orker-1", gated(g, "a"))
			runID := s.recordedRunID()
			s.start(runID, compile(t, pauseChaosDoc), nil, s.pausePlan())
			g.waitEntered(t, "a", time.Minute)
			const timeout = 3
			s.sendPause(runID, true, "pause-1", timeout)
			paused := s.awaitPublished(runID, "RUN_PAUSED", time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))
			// a ends inside the pause, so the expiry finds the run quiet.
			g.open("a")
			expiry := paused.GetTimestamp().AsTime().Add(timeout * time.Second)
			time.Sleep(time.Until(expiry.Add(offset)))
			// After the expiry the run may already have ended: the cluster
			// then refuses the signal, which is the timed-out outcome.
			request := map[string]any{"format": 1, "pause": false, "principal": "chaos", "requestId": "unpause-1"}
			signalErr := s.control.SignalWorkflow(ctx, runID, "", engine.PauseSignal, request)
			var ended *serviceerror.NotFound
			if signalErr != nil && !errors.As(signalErr, &ended) {
				t.Fatalf("unpause: %v", signalErr)
			}

			status, _ := s.wait(runID, 2*time.Minute)
			run, err := s.reader.GetRun(ctx, runID)
			must.OKf(t, err, "GetRun")
			h := s.history(runID, "")
			unpaused := false
			for _, e := range publishedOf(t, h) {
				if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED &&
					e.GetPauseTransition() == orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED {
					unpaused = true
				}
			}
			bRan, _ := s.attemptsOf("b")
			if unpaused && signalErr != nil {
				t.Fatalf("the run published its unpause, yet the cluster refused the unpause signal: %v", signalErr)
			}
			if unpaused {
				if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || bRan != 1 ||
					run.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_UNPAUSED {
					t.Fatalf("unpaused, yet the run closed %v, b ran %d times, its last pause ended %v",
						status, bRan, run.GetLastPause().GetEnd())
				}
				outcomes["unpaused"]++
			} else {
				if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED || run.GetError().GetCode() != engine.RunPauseTimeoutCode || bRan != 0 ||
					run.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT {
					t.Fatalf("not unpaused, yet the run closed %v with %q, b ran %d times, its last pause ended %v",
						status, run.GetError().GetCode(), bRan, run.GetLastPause().GetEnd())
				}
				outcomes["timed out"]++
			}
			if n := countPublished(t, h, orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED); n != 1 {
				t.Fatalf("RUN_PAUSED published %d times, want once", n)
			}
			must.OKf(t, replayRecorded(h, runID), "the history does not replay")
		})
	}
	t.Logf("outcomes: %v", outcomes)
}

// pauseChaosChildDoc is the composed child: k (held by its Orker), then
// after.
const pauseChaosChildDoc = `apiVersion: orkstack/v1
kind: Orkflow
name: pause-child
version: "1.0.0"
tasks:
  - id: k
    uses: shell
    with: {op: exec}
  - id: after
    uses: shell
    needs: [k]
    with: {op: exec}
`

// TestIntegration_PauseEngineKilledMidCascade (25): a parent with two
// composed children is paused; the replica is stopped while a child is
// inside applying the pause its parent cascaded to it (that child's
// RUN_PAUSED write held), and a second replica completes the round: each
// child is paused once, nothing new starts in any of the three runs, and the
// unpause lets all of them succeed.
func TestIntegration_PauseEngineKilledMidCascade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "pause-mid-cascade")
	// Once armed, the first event snapshot a child writes (applying the
	// pause its parent cascaded) is held; the parent's own writes pass.
	var armed atomic.Bool
	runID := s.recordedRunID()
	hook := newBlockingHook(func(string, int64) bool { return true })
	t.Cleanup(func() { hook.open(nil) })
	g := newGates()
	a := s.startReplica("replica-a", replicaOptions{store: &heldSnapshots{SnapshotStore: s.store, hook: hook,
		hold: func(id string) bool { return armed.Load() && id != runID }}})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "k"))

	child, err := dsl.Compile([]byte(pauseChaosChildDoc))
	must.OK(t, err)
	child.Namespace, child.DefinitionDigest = "default", childDigest
	parent := compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: pause-parent\nversion: \"1.0.0\"\ntasks:\n"+
		"  - id: one\n    uses: orkflow:pause-child@1.0.0\n"+
		"  - id: two\n    uses: orkflow:pause-child@1.0.0\n"+
		"  - id: last\n    uses: shell\n    needs: [one, two]\n    with: {op: exec}\n")
	for _, tk := range parent.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = childDigest
		}
	}
	plan := s.pausePlan()
	plan.Children = map[string]*engine.ChildRunPlan{}
	for _, task := range []string{"one", "two"} {
		cp, err := engine.NewChildRunPlan(child, s.pausePlan(), false, false)
		must.OK(t, err)
		cp.RunID = runID + "-" + task
		plan.Children[task] = cp
	}
	s.start(runID, parent, nil, plan)
	must.OKf(t, waitFor(ctx, "both children holding k", func() (bool, error) {
		attempts, _ := s.attemptsOf("k")
		return attempts >= 2, nil
	}), "wait")

	armed.Store(true)
	s.sendPause(runID, true, "pause-1", 3600)
	entered := hook.waitEntered(t, time.Minute)
	s.startReplica("replica-b", replicaOptions{})
	a.stop()
	if since := time.Since(entered); since > 5*time.Second {
		t.Fatalf("replica-a was stopped %s after the cascade's write entered the hook, outside the 5 s window", since)
	}
	s.transcript.step("replica-a stopped inside a child's apply of the cascaded pause")
	for _, task := range []string{"one", "two"} {
		s.awaitPublished(runID+"-"+task, "the child's RUN_PAUSED", 2*time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))
	}
	hook.open(nil)

	// k's end, published in both children while paused, is the progress
	// the window observes.
	g.open("k")
	end := time.Now().Add(15 * time.Second)
	for time.Now().Before(end) {
		if n, _ := s.attemptsOf("after"); n > 0 {
			t.Fatal("a paused child started after")
		}
		if n, _ := s.attemptsOf("last"); n > 0 {
			t.Fatal("the paused parent started last")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, task := range []string{"one", "two"} {
		if !taskEnded(t, s.history(runID+"-"+task, ""), "k") {
			t.Fatalf("child %s published no end of k in the window, so it proves nothing", task)
		}
	}

	s.sendPause(runID, false, "unpause-1", 0)
	if status, err := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the parent closed %v: %v", status, err)
	}
	parentHistory := s.history(runID, "")
	requirePauseOnce(t, parentHistory)
	if n := countHistoryEvents(parentHistory, func(e *historypb.HistoryEvent) bool {
		return e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes() != nil
	}); n != 4 {
		t.Fatalf("the parent sent %d signals to its children, want a pause and an unpause to each", n)
	}
	must.OKf(t, replayRecorded(parentHistory, runID), "the parent's history does not replay")
	for _, task := range []string{"one", "two"} {
		id := runID + "-" + task
		h := s.history(id, "")
		requirePauseOnce(t, h)
		must.OKf(t, replayRecorded(h, id), "child %s's history does not replay", task)
	}
	if n, _ := s.attemptsOf("after"); n != 2 {
		t.Fatalf("after ran %d times across the two children, want once in each", n)
	}
}

// requirePauseOnce fails unless h published exactly one RUN_PAUSED and one
// RUN_UNPAUSED, in that order, and its published events are contiguous.
func requirePauseOnce(t *testing.T, h *historypb.History) {
	t.Helper()
	published := publishedOf(t, h)
	requireContiguous(t, published)
	paused, unpaused := -1, -1
	for i, e := range published {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED:
			if paused >= 0 {
				t.Fatalf("RUN_PAUSED published twice (sequences %d and %d)", published[paused].GetSequence(), e.GetSequence())
			}
			paused = i
		case orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED:
			if unpaused >= 0 {
				t.Fatalf("RUN_UNPAUSED published twice (sequences %d and %d)", published[unpaused].GetSequence(), e.GetSequence())
			}
			unpaused = i
		}
	}
	if paused < 0 || unpaused < paused {
		t.Fatalf("RUN_PAUSED at %d and RUN_UNPAUSED at %d: want one of each, the pause first", paused, unpaused)
	}
}
