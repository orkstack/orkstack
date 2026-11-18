//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// writeRecording lands h under ORK_PROOF_DIR, and at path only with
// ORK_RECORD_GOLDEN_HISTORY=1.
func writeRecording(t *testing.T, dir, path string, h *historypb.History) {
	t.Helper()
	data := encodeHistory(t, h)
	proof.Write(t, dir, filepath.Base(path), data)
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "1"
	if write {
		must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
	}
	t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
}

// readHistory is one workflow's whole current history.
func readHistory(t *testing.T, ctx context.Context, c client.Client, id string) *historypb.History {
	t.Helper()
	recorded := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		must.OKf(t, err, "read history of %s", id)
		recorded.Events = append(recorded.Events, event)
	}
	return recorded
}

// countHistoryEvents counts h's events match accepts.
func countHistoryEvents(h *historypb.History, match func(*historypb.HistoryEvent) bool) int {
	n := 0
	for _, e := range h.GetEvents() {
		if match(e) {
			n++
		}
	}
	return n
}

// signaled reports that workflow id has received a PauseSignal, and quiet
// that it is running with nothing pending: the probes the recorders pace
// themselves by. A workflow not started yet has received nothing; any other
// error is the probe's own and ends the wait.
func signaled(ctx context.Context, c client.Client, id string) (bool, error) {
	iter := c.GetWorkflowHistory(ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("read the history of %s: %w", id, err)
		}
		if event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName() == engine.PauseSignal {
			return true, nil
		}
	}
	return false, nil
}

// A pin's session creation activity stays pending while the pin is open, so
// it alone is ignored; every other activity counts, the pause's own event
// publish included, so an unpause sent once the run is quiet always lands
// after it. A closed workflow is never quiet.
func quiet(ctx context.Context, c client.Client, id string) (bool, error) {
	described, err := describeExecution(c, ctx, id, "")
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("describe %s: %w", id, err)
	}
	if described.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return false, fmt.Errorf("%s closed %v while it should be paused", id, described.GetWorkflowExecutionInfo().GetStatus())
	}
	if described.GetPendingWorkflowTask() != nil {
		return false, nil
	}
	for _, a := range described.GetPendingActivities() {
		if a.GetActivityType().GetName() != sessionCreationActivity {
			return false, nil
		}
	}
	return true, nil
}

// waitFor polls probe until it holds, its error, or ctx's end.
func waitFor(ctx context.Context, what string, probe func() (bool, error)) error {
	for {
		ok, err := probe()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// pauseOnce signals the pause the first time it succeeds, so an attempt
// retried after a failed send sends it again.
type pauseOnce struct{ sent atomic.Bool }

func (p *pauseOnce) send(ctx context.Context, c client.Client, runID string) error {
	if p.sent.Load() {
		return nil
	}
	if err := c.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(true)); err != nil {
		return err
	}
	p.sent.Store(true)
	return nil
}

// deferredPausePlan is the pause-carry stack's plan with a history limit of
// 2,000 events and an apply reserve of 1,700: generation 0, hundreds of
// events in when q30 asks for the pause, cannot record it, and generation 1,
// a few dozen in, can.
func deferredPausePlan(s *canStack) engine.RunPlan {
	budget := engine.DefaultContinuationBudget(60, 1<<20, 3*time.Minute)
	plan := s.plan(2000, &budget)
	pause := engine.DefaultPausePlan(7*24*3600, 31*24*3600)
	pause.ApplyReserveEvents = 1700
	pause.Cascades, pause.PinTimeoutPausable = false, false
	plan.Pause = &pause
	return plan
}

// TestIntegration_RecordPauseDeferredHistories records the carried pause
// request's two generations (pauseDeferredFixtures, replay_pause_test.go):
// the pause from q30 does not fit generation 0's history, so the run starts
// nothing more, switches once slow ends and carries the request; generation
// 1 applies it first, starts nothing until the unpause, then runs final.
func TestIntegration_RecordPauseDeferredHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startPacedCANStack(t, ctx, "pause-deferred", pausedByLastQuickTask(), nil, "slow")
	runID := fmt.Sprintf("ork-pause-deferred-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, pauseCarryDocument(30)), nil, deferredPausePlan(s)), "StartRun")
	first, err := describeExecution(s.client, ctx, runID, "")
	must.OK(t, err)
	current := waitForNextExecution(t, s, ctx, runID, first.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	must.OKf(t, s.waitIdle(runID, 90*time.Second), "the next generation never went idle")
	must.OKf(t, s.client.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(false)), "unpause")
	must.OKf(t, s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil), "the run did not succeed")

	chain := s.chainOf(runID, current)
	if len(chain) != 2 {
		t.Fatalf("%d generations, want 2", len(chain))
	}
	g0, g1 := s.history(runID, chain[0]), s.history(runID, chain[1])
	carried := carryOf(t, g1).GetDeferredPauseRequests()
	if !continuedAsNew(g0) || len(carried) != 1 || !carried[0].GetPause() || carryOf(t, g1).GetPause() != nil {
		t.Fatalf("g0 switched %v carrying %v; want it to carry the pause request, unapplied", continuedAsNew(g0), carried)
	}
	if versionMarkersOf(t, g0)["pause-deferred"] == 0 {
		t.Fatal("g0 recorded no pause-deferred marker")
	}
	if !dispatchedTasks(t, g1)["final"] || len(dispatchedTasks(t, g1)) != 1 {
		t.Fatalf("g1 dispatched %v, want final alone", dispatchedTasks(t, g1))
	}
	must.OKf(t, checkDeferredG1Shape(g1), "g1 did not hold the carried pause until the unpause")
	for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
		must.OKf(t, replayRecorded(h, runID), "%s just recorded does not replay", name)
	}
	writeRecording(t, "pause", pauseDeferredG0, g0)
	writeRecording(t, "pause", pauseDeferredG1, g1)
}

// TestIntegration_RecordPauseCascadeHistories records the cascade's parent
// and child (pauseCascadeFixtures): the child's sign task asks for the
// parent's pause and ends once the child has received it; the child, paused,
// starts nothing; the parent's unpause, sent once the child is quiet, lifts
// it, and both succeed.
func TestIntegration_RecordPauseCascadeHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue, orkerQueue := "ork-cascade-rec-run-"+suffix, "ork-cascade-rec-tasks-"+suffix
	deployment := task.Deployment{Name: "ork-cascade-rec-" + suffix, BuildID: "v1"}
	runID := fmt.Sprintf("ork-pause-cascade-%d", time.Now().UnixNano())
	childID := runID + "-child"

	values := &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}}
	runWorker := worker.New(c, runQueue, deployment.Apply(worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	compose := engine.NewComposeActivities(values)
	runWorker.RegisterActivityWithOptions(compose.RenderComposedValues, activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	runWorker.RegisterActivityWithOptions(compose.StoreComposedValues, activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	var pause pauseOnce
	orkerWorker := worker.New(c, orkerQueue, deployment.Apply(worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true}))
	orkerWorker.RegisterActivityWithOptions(func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		value := map[string]any{"value": "fixture-" + in.GetTaskId()}
		if in.GetTaskId() == "sign" {
			value["value"] = "sha256:" + strings.Repeat("ab", 32)
			if err := pause.send(ctx, c, runID); err != nil {
				return nil, fmt.Errorf("pause the parent from sign: %w", err)
			}
			if err := waitFor(ctx, "the child's pause", func() (bool, error) {
				activity.RecordHeartbeat(ctx)
				return signaled(ctx, c, childID)
			}); err != nil {
				return nil, err
			}
		}
		raw := valuesJSON(value)
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture", ValuesJson: raw, ValuesInlined: true,
			ValuesRef: values.put("mem://values/"+in.GetRunId()+"/"+in.GetTaskId(), raw)}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	eventWorker.RegisterActivityWithOptions(engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		must.OKf(t, w.Start(), "start %s worker", name)
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, c, deployment, runQueue, orkerQueue)

	pausePlan := engine.DefaultPausePlan(7*24*3600, 31*24*3600)
	pausePlan.PinTimeoutPausable, pausePlan.MaxDeferredRequests = false, 0
	child := exampleOrkflow(t, "release-block", "0.1.0")
	parent := exampleOrkflow(t, "composed-release", "0.1.0")
	childPlan := rawPlan(orkerQueue)
	childPlan.CommitRoot = "mem://commits"
	childPlan.Pause = &pausePlan
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	must.OK(t, err)
	cp.RunID = childID
	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.Pause = &pausePlan
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	must.OKf(t, starter.StartRun(ctx, runID, parent, nil, plan), "StartRun")

	// The unpause once the child, paused, is quiet: sign has returned.
	must.OKf(t, waitFor(ctx, "the child holding its parent's pause", func() (bool, error) {
		if ok, err := signaled(ctx, c, childID); !ok || err != nil {
			return false, err
		}
		return quiet(ctx, c, childID)
	}), "wait")
	must.OKf(t, c.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(false)), "unpause")
	must.OKf(t, c.GetWorkflow(ctx, runID, "").Get(ctx, nil), "the run did not succeed")

	parentHistory, childHistory := readHistory(t, ctx, c, runID), readHistory(t, ctx, c, childID)
	if versionMarkersOf(t, parentHistory)["pause-cascade"] == 0 {
		t.Fatal("the parent recorded no pause-cascade marker")
	}
	if n := countHistoryEvents(parentHistory, func(e *historypb.HistoryEvent) bool {
		return e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes() != nil
	}); n != 2 {
		t.Fatalf("the parent sent %d signals to its child, want the pause and the unpause", n)
	}
	must.OKf(t, checkCascadeChildShape(childHistory), "the child did not hold its parent's pause")
	for id, h := range map[string]*historypb.History{runID: parentHistory, childID: childHistory} {
		must.OKf(t, replayRecorded(h, id), "%s just recorded does not replay", id)
	}
	writeRecording(t, "pause", pauseCascadeParent, parentHistory)
	writeRecording(t, "pause", pauseCascadeChild, childHistory)
}

// pausedPinDocument is one pin of two members with a 30-second timeout,
// paused by its first member and unpaused once the run is quiet.
const pausedPinDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: paused-pin
pins:
  p:
    timeout_seconds: 30
tasks:
  - id: a
    uses: shell
    pin: p
    run_as_orker_user: true
    with: {command: "echo a"}
  - id: b
    uses: shell
    pin: p
    needs: [a]
    run_as_orker_user: true
    with: {command: "echo b"}
`

// TestIntegration_RecordPausePinHistory records a paused run's pin
// (pausePinFixture): a's run asks for the pause; the pin's clock stops (its
// timer canceled), b is held; the unpause re-arms the clock and b runs on the
// same pin. The session carries the outer bound.
func TestIntegration_RecordPausePinHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "pause-pin-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-pause-pin-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	runID := fmt.Sprintf("ork-pause-pin-%d", time.Now().UnixNano())
	var pause pauseOnce
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment,
		func(ctx context.Context, cs *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			if in.GetTaskId() == "a" {
				if err := pause.send(ctx, cs.control, runID); err != nil {
					return nil, fmt.Errorf("pause from a: %w", err)
				}
				if err := waitFor(ctx, "the run's pause", func() (bool, error) { return signaled(ctx, cs.control, runID) }); err != nil {
					return nil, err
				}
			}
			return instant(ctx, cs, orker, in)
		})
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	plan := rawPlan(s.orkerQueue)
	plan.CommitRoot = "mem://commits"
	pausePlan := engine.DefaultPausePlan(7*24*3600, 31*24*3600)
	pausePlan.Cascades, pausePlan.MaxDeferredRequests = false, 0
	plan.Pause = &pausePlan
	s.start(runID, compile(t, pausedPinDocument), nil, plan)
	must.OKf(t, waitFor(ctx, "the paused run going quiet", func() (bool, error) { return quietAfterTask(ctx, s.control, runID) }), "wait")
	must.OKf(t, s.control.SignalWorkflow(ctx, runID, "", engine.PauseSignal, pauseSignalFor(false)), "unpause")
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("the run closed %v (%v), want Completed", status, err)
	}
	recorded := s.history(runID, "")
	if versionMarkersOf(t, recorded)["pausable-pin-timeout"] == 0 {
		t.Fatal("the run recorded no pausable-pin-timeout marker")
	}
	must.OKf(t, checkPinClockShape(recorded), "the pause did not stop the pin's clock")
	must.OKf(t, replayRecorded(recorded, runID), "the history just recorded does not replay")
	writeRecording(t, "pause", pausePinFixture, recorded)
}

// quietAfterTask reports that the run has completed a task's activity and
// has no task pending.
func quietAfterTask(ctx context.Context, c client.Client, runID string) (bool, error) {
	if ok, err := quiet(ctx, c, runID); !ok || err != nil {
		return false, err
	}
	scheduled := map[int64]bool{}
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return false, fmt.Errorf("read the history of %s: %w", runID, err)
		}
		if isExecuteTaskScheduled(event) {
			scheduled[event.GetEventId()] = true
		}
		if done := event.GetActivityTaskCompletedEventAttributes(); done != nil && scheduled[done.GetScheduledEventId()] {
			return true, nil
		}
	}
	return false, nil
}
