package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

const pinComposedDocument = `
name: pin-composed
version: "1.0.0"
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [prep]
    with: {image: fixed}
  - id: compile
    uses: shell
    pin: build
    needs: [release]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: publish
    uses: shell
    needs: [compile]
    with: {command: "echo publish"}
  - id: announce
    uses: shell
    needs: [publish]
    with: {command: "echo announce"}
`

// TestContinuation_PinMemberNeedingComposedTaskDoesNotDeadlock: a quiesce
// that starts while a pin is open lets the composed task a member needs
// start its child run, so the pin closes and the run then switches.
func TestContinuation_PinMemberNeedingComposedTaskDoesNotDeadlock(t *testing.T) {
	wf := compile(t, pinComposedDocument)
	for _, task := range wf.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	plan := holdPlan()
	_, composed, _ := composedFixture(t, reproducedChildDocument)
	plan.Children = composed.Children
	g := runGenerations(t, "ork-pin-composed", wf, payload(t, nil), plan, okBehavior, func(h *harness, generation int) {
		pinSetup(h, generation)
		if generation == 0 {
			raiseHistoryWhen(h, 650, taskStatusIs("prep", orkstackv1.Status_STATUS_SUCCEEDED))
		}
	})
	generationsOK(t, g, 2)
	gen0 := strings.Join(sortedTaskOrder(g.harnesses[0]), ",")
	if !strings.Contains(gen0, "compile") || !strings.Contains(gen0, "prep") {
		t.Fatalf("generation 0 ran %q, want both pin members — the composed task between them started under the hold", gen0)
	}
	for _, h := range g.harnesses[1:] {
		for _, id := range h.taskOrder() {
			if id == "prep" || id == "compile" {
				t.Fatalf("pin member %s ran after the switch", id)
			}
		}
	}
	// The hold was real: generation 0 stopped with the unpinned tail still
	// to run, after the composed task between the members had finished.
	if got := strings.Join(g.harnesses[len(g.harnesses)-1].taskOrder(), ","); !strings.Contains(got, "announce") {
		t.Fatalf("the last generation ran %q, want the tail after the pin closed", got)
	}
	t.Logf("generation 0 ran %s; %d generations", gen0, len(g.harnesses))
}

// TestContinuation_FanOutResumesAtNextIndex: a fan-out held part-way
// resumes, in the next generation, at the first item its predecessor never
// dispatched, and every item runs exactly once.
func TestContinuation_FanOutResumesAtNextIndex(t *testing.T) {
	g := runGenerations(t, "ork-resume", compile(t, spreadDocument), spreadInput(t, 10), holdPlan(), itemBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				raiseHistoryAt(h, 2, 650)
			}
		})
	generationsOK(t, g, 2)
	first, second := itemsDispatched(g.harnesses[0]), itemsDispatched(g.harnesses[1])
	if len(first) == 0 || len(first) >= 10 || len(second) == 0 || second[0] != first[len(first)-1]+1 {
		t.Fatalf("generation 0 dispatched %v, generation 1 %v: want generation 1 to start at the next index", first, second)
	}
}

// TestContinuation_ResolutionAfterRestoreCancelsTheRecreatedTimer: an await
// open across a switch is resolved in the next generation, and its
// recreated deadline timer is cancelled, so the run never times it out.
func TestContinuation_ResolutionAfterRestoreCancelsTheRecreatedTimer(t *testing.T) {
	var canceled, scheduled int
	var mu sync.Mutex
	g := runGenerations(t, "ork-await-cancel", compile(t, awaitAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				return
			}
			h.env.SetOnTimerScheduledListener(func(string, time.Duration) { mu.Lock(); scheduled++; mu.Unlock() })
			h.env.SetOnTimerCanceledListener(func(string) { mu.Lock(); canceled++; mu.Unlock() })
			h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
		})
	run := generationsOK(t, g, 2)
	mu.Lock()
	defer mu.Unlock()
	if scheduled == 0 || canceled == 0 {
		t.Fatalf("%d recreated timers, %d canceled: want the resolution to cancel the deadline", scheduled, canceled)
	}
	if got := taskRunOf(t, run, "approve").GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("approve = %v", got)
	}
}

// TestContinuation_UnreadableCheckpointFailsUnavailableAfterTheRestoreTimeout:
// a saved state that is gone is retried by Temporal until the restore
// timeout, then the run ends RUN_STATE_UNAVAILABLE with a terminal event.
func TestContinuation_UnreadableCheckpointFailsUnavailableAfterTheRestoreTimeout(t *testing.T) {
	plan := smallChunkPlan(2, 8)
	plan.Continuation.Restore = engine.RestorePolicy{TimeoutSeconds: 60, AttemptSeconds: 5, BackoffInitialMillis: 1000, BackoffMaxSeconds: 10}
	s := firstSwitch(t, "ork-boot-gone", bigChainDocument, payload(t, nil), plan)
	s.store.mu.Lock()
	delete(s.store.objects, s.carry.GetCheckpoint().GetUri())
	s.store.mu.Unlock()
	h := s.next(t, nil)
	requireRunStateError(t, h.env.GetWorkflowError(), engine.RunStateUnavailableCode)
	if e := failedStart(t, h, s); !strings.Contains(e.GetMessage(), "could not be read from object storage for 1m0s") {
		t.Fatalf("terminal event %q", e.GetMessage())
	}
}

// holdNeverSpinsAfterCancel is TestContinuation_HoldNeverSpins' cancel case:
// a cancel while the run holds, under the production 1 s deadlock detector,
// ends the run without a spin.
func holdNeverSpinsAfterCancel(t *testing.T) {
	const doc = `
name: hold-cancel
tasks:
  - id: slow
    uses: shell
    needs: []
    with: {op: exec}
  - id: fast
    uses: shell
    needs: []
    with: {op: exec}
  - id: next
    uses: shell
    needs: [fast]
    with: {op: exec}
`
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "slow" {
			return runUntilCanceled(ctx, nil)
		}
		return okBehavior(ctx, in)
	})
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-spin-cancel"})
	var once sync.Once
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		if statusOf(run, "fast") == orkstackv1.Status_STATUS_SUCCEEDED {
			once.Do(func() {
				h.env.SetCurrentHistoryLength(650)
				h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Millisecond)
			})
		}
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-spin-cancel", compile(t, doc), payload(t, nil), holdPlan(),
		(*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); !temporal.IsCanceledError(errors.Unwrap(err)) && !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %v, want a cancellation", err)
	}
}

// TestContinuation_PlanWithoutBudgetIsByteIdentical: with no continuation
// budget, a plan that now also carries the in-flight cap at its default
// issues exactly the command stream of the plan every committed fixture was
// recorded with, which carries neither, in order: no version marker, no
// hold, no write. (The command halves of the replay tests prove the recorded
// streams themselves still replay.)
func TestContinuation_PlanWithoutBudgetIsByteIdentical(t *testing.T) {
	for _, path := range committedFixtures(t) {
		if strings.Contains(path, "-continued-") || strings.Contains(path, "in-flight-cap") {
			continue // this stage's own fixtures were recorded with the new plan fields
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			recorded := loadHistoryFile(t, path)
			runID, wf, input, plan := goldenWorkflowArgs(t, recorded)
			if plan.Continuation != nil || plan.InFlight != nil {
				t.Fatalf("the fixture's plan already carries continuation %v or in-flight %v", plan.Continuation, plan.InFlight)
			}
			inputs, results := recordedItemPayloads(t, recorded)
			signals, offsets := recordedResolutions(t, recorded)
			count := eachCount(inputs)
			commands := func(plan engine.RunPlan) []string {
				var suite testsuite.WorkflowTestSuite
				var current *harness
				h := newHarnessCtx(t, &suite, fixtureBehavior(results, &current))
				current = h
				enablePins(h, worker.Options{})
				r := &commandRecorder{}
				h.env.SetWorkerOptions(worker.Options{EnableSessionWorker: true, Interceptors: []interceptor.WorkerInterceptor{r}})
				h.countItems = func(*orkstackv1.ArtifactRef) (int32, error) { return count, nil }
				for i, sig := range signals {
					h.signalAt(offsets[i], sig)
				}
				h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
				h.env.ExecuteWorkflow(engine.RunWorkflowType, runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
				return r.list()
			}
			bare := commands(plan)
			capped := plan
			limit := engine.DefaultInFlightLimit(engine.DefaultRunMaxInFlight)
			capped.InFlight = &limit
			with := commands(capped)
			if len(bare) == 0 {
				t.Fatal("the fixture's definition issued no command")
			}
			if strings.Join(bare, "\n") != strings.Join(with, "\n") {
				t.Fatalf("the command streams differ:\nrecorded plan %v\nwith the cap  %v", bare, with)
			}
			t.Logf("%d commands, identical", len(bare))
		})
	}
}

// fixtureBehavior answers a committed fixture's task with its recorded
// result, and a child run's task (the recording holds only the parent's)
// with a stand-in. The values are put in the current harness's store under a
// reference to exactly those bytes, so a render reads them.
func fixtureBehavior(results map[string]*orkstackv1.ExecuteTaskResult, current **harness) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result, ok := results[scheduleKey(in)]
		if !ok {
			fallback := succeeded(in.GetTaskId(), map[string]any{"value": "v"})
			fallback.ValuesRef.Uri = "mem://values/" + in.GetRunId() + "/" + scheduleKey(in)
			result = fallback
		}
		result = proto.Clone(result).(*orkstackv1.ExecuteTaskResult)
		values := result.GetValuesJson()
		if len(values) == 0 {
			values = []byte(`{"value":"v"}`)
		}
		if result.GetValuesRef() != nil {
			result.ValuesRef = (*current).values.put(result.GetValuesRef().GetUri(), values)
			result.ValuesJson, result.ValuesInlined = values, true
		}
		return result, nil
	}
}
