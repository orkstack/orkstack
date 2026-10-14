package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// lineLogger records every log line's message, for a test that counts the
// writer's decisions. It satisfies log.Logger.
type lineLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *lineLogger) add(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, msg)
}
func (l *lineLogger) Debug(msg string, _ ...any) {}
func (l *lineLogger) Info(msg string, _ ...any)  { l.add(msg) }
func (l *lineLogger) Warn(msg string, _ ...any)  { l.add(msg) }
func (l *lineLogger) Error(msg string, _ ...any) { l.add(msg) }

var _ log.Logger = (*lineLogger)(nil)

func (l *lineLogger) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if line == msg {
			n++
		}
	}
	return n
}

// The writer's operator log lines (docs/DEPLOY.md §5).
const (
	logSwitchDue = "run will carry on into a fresh history at its next idle point"
	logQuiesce   = "run pausing new tasks so it can carry on into a fresh history"
	logGiveUp    = "run resumed new tasks: it cannot reach a moment with nothing running"
	logSwitched  = "run carried on into a fresh history"
	logRestored  = "run restored its saved state"
)

// raiseHistoryWhen moves a harness's raw history length to length the first
// time a snapshot satisfies when.
func raiseHistoryWhen(h *harness, length int, when func(*orkstackv1.Run) bool) {
	var once sync.Once
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		if when(run) {
			once.Do(func() { h.env.SetCurrentHistoryLength(length) })
		}
		return nil
	}
}

func generationsOK(t *testing.T, g *generations, atLeast int) *orkstackv1.Run {
	t.Helper()
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if len(g.harnesses) < atLeast {
		t.Fatalf("%d generations, want at least %d", len(g.harnesses), atLeast)
	}
	run := g.final(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	requireContiguous(t, g.events())
	return run
}

func pinSetup(h *harness, _ int) { enablePins(h, worker.Options{DeadlockDetectionTimeout: time.Second}) }

const pinOpenDocument = `
name: pin-open
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep]
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

// TestContinuation_PinOpenAtTriggerClosesBeforeSwitch: a run whose budget is
// crossed while a pin is open finishes the pin on its host and closes it
// first; it switches only at an idle point with nothing of the pin left —
// not while the pin is still closing, which is in flight.
func TestContinuation_PinOpenAtTriggerClosesBeforeSwitch(t *testing.T) {
	g := runGenerations(t, "ork-pin-open", compile(t, pinOpenDocument), payload(t, nil), continuedPlan(1), okBehavior, pinSetup)
	generationsOK(t, g, 2)
	gen0 := strings.Join(sortedTaskOrder(g.harnesses[0]), ",")
	if !strings.HasPrefix(gen0, "compile,prep") && gen0 != "compile,prep,publish" {
		t.Fatalf("generation 0 ran %q, want both pin members before the switch", gen0)
	}
	if got := len(g.harnesses[0].releases()); got != 1 {
		t.Fatalf("generation 0 released %d pins, want the one it opened", got)
	}
	for _, h := range g.harnesses[1:] {
		for _, id := range h.taskOrder() {
			if id == "prep" || id == "compile" {
				t.Fatalf("pin member %s ran after the switch", id)
			}
		}
	}
}

const pinFanOutDocument = `
name: pin-fan
pins:
  build: {}
inputs:
  parts: {type: array}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: spread
    uses: shell
    needs: [prep]
    each: {over: "{{ .inputs.parts }}", max_items: 100, max_parallel: 2}
    with: {op: exec}
  - id: compile
    uses: shell
    pin: build
    needs: [spread]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: publish
    uses: shell
    needs: [compile]
    with: {command: "echo publish"}
`

// TestContinuation_PinMemberDownstreamOfFanOutDoesNotDeadlock: a quiesce
// that starts while a pin is open lets the fan-out (its items included) that
// a member needs run, so the pin closes and the run then switches.
func TestContinuation_PinMemberDownstreamOfFanOutDoesNotDeadlock(t *testing.T) {
	g := runGenerations(t, "ork-pin-fan", compile(t, pinFanOutDocument), spreadInput(t, 6), holdPlan(), itemBehavior,
		func(h *harness, generation int) {
			pinSetup(h, generation)
			if generation == 0 {
				raiseHistoryWhen(h, 650, taskStatusIs("prep", orkstackv1.Status_STATUS_SUCCEEDED))
			}
		})
	generationsOK(t, g, 2)
	if got := len(itemsDispatched(g.harnesses[0])); got != 6 {
		t.Fatalf("generation 0 dispatched %d items of the pin's closure, want all 6", got)
	}
	if got := strings.Join(g.harnesses[len(g.harnesses)-1].taskOrder(), ","); got != "publish" {
		t.Fatalf("the last generation ran %q, want publish after the pin closed", got)
	}
}

const pinAwaitDocument = `
name: pin-await
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: approve
    needs: [prep]
    await: {timeout_seconds: 3600}
  - id: compile
    uses: shell
    pin: build
    needs: [approve]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: side1
    uses: shell
    needs: [prep]
    with: {command: "echo side"}
  - id: side2
    uses: shell
    needs: [side1]
    with: {command: "echo side"}
  - id: publish
    uses: shell
    needs: [compile, side2]
    with: {command: "echo publish"}
`

// TestContinuation_PinBlockedOnAwaitGivesTheHoldUp: an open pin whose
// remaining work waits on an await cannot reach idle until someone resolves
// it, so the quiesce is given up — the held work runs — and the run switches
// at its next natural idle point, after the pin closes.
func TestContinuation_PinBlockedOnAwaitGivesTheHoldUp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	logs := &lineLogger{}
	suite.SetLogger(logs)
	resolved := false
	g := runGenerationsIn(t, &suite, "ork-pin-await", compile(t, pinAwaitDocument), payload(t, nil), holdPlan(), okBehavior,
		func(h *harness, generation int) {
			pinSetup(h, generation)
			if generation == 0 {
				raiseHistoryWhen(h, 650, taskStatusIs("prep", orkstackv1.Status_STATUS_SUCCEEDED))
			}
			if !resolved {
				resolved = true
				h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
			}
		})
	generationsOK(t, g, 2)
	// Given up, and — the run still past its budget — tried again after its
	// retry spacing, and given up again, until the pin closes.
	if logs.count(logGiveUp) < 1 {
		t.Fatal("the hold was never given up")
	}
	gen0 := strings.Join(sortedTaskOrder(g.harnesses[0]), ",")
	if gen0 != "compile,prep,side1,side2" {
		t.Fatalf("generation 0 ran %q: the held side tasks run after the give-up, the pin closes, then the switch", gen0)
	}
}

const skipPropagationDocument = `
name: skip-propagation
pins:
  build: {}
inputs:
  enabled: {type: boolean, default: false}
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: optional
    uses: shell
    needs: [prep]
    condition: "{{ .inputs.enabled }}"
    with: {command: "echo optional"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep, optional]
    join: none_failed
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: publish
    uses: shell
    needs: [compile]
    with: {command: "echo publish"}
`

// TestContinuation_GiveUpWaitsForSkipPropagation: a pin open and quiescing,
// whose member becomes ready only because a need was skipped in the same
// pass. The idle check re-scans rather than give the hold up; the member
// dispatches, the pin closes, and the run switches at its natural idle point.
func TestContinuation_GiveUpWaitsForSkipPropagation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	logs := &lineLogger{}
	suite.SetLogger(logs)
	g := runGenerationsIn(t, &suite, "ork-skip", compile(t, skipPropagationDocument), payload(t, nil), holdPlan(), okBehavior,
		func(h *harness, generation int) {
			pinSetup(h, generation)
			if generation == 0 {
				raiseHistoryWhen(h, 650, taskStatusIs("prep", orkstackv1.Status_STATUS_SUCCEEDED))
			}
		})
	generationsOK(t, g, 2)
	if logs.count(logGiveUp) != 0 {
		t.Fatal("the hold was given up: the member was ready, one re-scan away")
	}
	if logs.count(logQuiesce) != 1 {
		t.Fatalf("%d quiesces, want one", logs.count(logQuiesce))
	}
	if got := strings.Join(sortedTaskOrder(g.harnesses[0]), ","); got != "compile,prep" {
		t.Fatalf("generation 0 ran %q", got)
	}
}

const awaitAcrossDocument = `
name: await-across
tasks:
  - id: approve
    needs: []
    await: {timeout_seconds: 3600}
  - id: a
    uses: shell
    needs: []
    with: {command: "echo a"}
  - id: b
    uses: shell
    needs: [a]
    with: {command: "echo b"}
  - id: c
    uses: shell
    needs: [b]
    with: {command: "echo c"}
  - id: merge
    uses: shell
    needs: [approve, c]
    with: {command: "echo merge"}
`

// TestContinuation_AwaitCrossesSwitchWithOriginalDeadline: an open await
// crosses a switch at its original instant — its deadline is absolute — and
// accepts a resolution afterwards; its timer is recreated with the time
// remaining, and resolving cancels it
// (ResolutionAfterRestoreCancelsTheRecreatedTimer).
func TestContinuation_AwaitCrossesSwitchWithOriginalDeadline(t *testing.T) {
	var canceled atomic.Int32
	var remaining []time.Duration
	var mu sync.Mutex
	g := runGenerations(t, "ork-await-across", compile(t, awaitAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				return
			}
			h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
				mu.Lock()
				remaining = append(remaining, d)
				mu.Unlock()
			})
			h.env.SetOnTimerCanceledListener(func(string) { canceled.Add(1) })
			h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
		})
	run := generationsOK(t, g, 2)
	opened := 0
	for _, e := range g.events() {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING {
			opened++
		}
	}
	if opened != 1 {
		t.Fatalf("the await opened %d times across generations, want once", opened)
	}
	deadline := taskRunOf(t, run, "approve").GetAwait().GetDeadline().AsTime()
	first := mustFinalRun(t, g.harnesses[0].store, g.runID)
	if want := taskRunOf(t, first, "approve").GetAwait().GetDeadline().AsTime(); !deadline.Equal(want) {
		t.Fatalf("deadline %v after the switch, %v before", deadline, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remaining) == 0 || remaining[0] <= 0 || remaining[0] > time.Hour {
		t.Fatalf("recreated timers %v, want one with the time remaining of the hour", remaining)
	}
	if canceled.Load() == 0 {
		t.Fatal("the resolution did not cancel the recreated deadline timer")
	}
}

// TestContinuation_AwaitDeadlinePassedDuringSwitchExpiresImmediately: a
// deadline that passed while the run was switching fires at once in the next
// generation, with the original deadline in its message.
func TestContinuation_AwaitDeadlinePassedDuringSwitchExpiresImmediately(t *testing.T) {
	var start time.Time
	g := runGenerations(t, "ork-await-late", compile(t, awaitAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				start = time.Now().UTC().Truncate(time.Second)
				h.env.SetStartTime(start)
				return
			}
			h.env.SetStartTime(start.Add(2 * time.Hour))
		})
	var app *temporal.ApplicationError
	if !errors.As(g.err, &app) || app.Type() != engine.AwaitTimedOutCode {
		t.Fatalf("run error = %v, want %s", g.err, engine.AwaitTimedOutCode)
	}
	if len(g.harnesses) < 2 {
		t.Fatal("the run never switched")
	}
	run := g.final(t)
	failure := taskRunOf(t, run, "approve").GetError()
	wantDeadline := start.Add(time.Hour).Format(time.RFC3339)
	if failure.GetCode() != engine.AwaitTimedOutCode || !strings.Contains(failure.GetMessage(), wantDeadline) {
		t.Fatalf("approve failed %v, want the original deadline %s", failure, wantDeadline)
	}
}

// TestContinuation_ResolutionQueuedAtSwitchIsAppliedOnce: a resolution
// delivered while the switch's writes block is applied by the switch's last
// check, which turns the switch back; the run then carries it forward, and
// it is applied exactly once.
func TestContinuation_ResolutionQueuedAtSwitchIsAppliedOnce(t *testing.T) {
	var sent atomic.Bool
	g := runGenerations(t, "ork-await-queued", compile(t, awaitAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation != 0 {
				return
			}
			h.onContinuation = func(kind string, _ int64) error {
				if kind == "checkpoint" && sent.CompareAndSwap(false, true) {
					h.env.SignalWorkflow(engine.ResolveAwaitSignal, completion("approve", `{"ok":true}`, "alice"))
				}
				return nil
			}
		})
	generationsOK(t, g, 2)
	resolved := 0
	for _, e := range g.events() {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED {
			resolved++
		}
	}
	if resolved != 1 {
		t.Fatalf("the resolution was applied %d times, want once", resolved)
	}
	if g.harnesses[0].continuationCount("checkpoint") < 2 {
		t.Fatal("the first switch was not turned back by the resolution")
	}
}

// TestContinuation_CancelDuringHoldEndsCanceled: an operator's cancel while
// the run holds new dispatch ends it RUN_CANCELED, its running work stopped.
func TestContinuation_CancelDuringHoldEndsCanceled(t *testing.T) {
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
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-hold-cancel"})
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
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-hold-cancel", compile(t, doc), payload(t, nil), holdPlan(),
		(*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); !temporal.IsCanceledError(errors.Unwrap(err)) && !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %v, want a cancellation", err)
	}
	run := mustFinalRun(t, h.store, "ork-hold-cancel")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || statusOf(run, "next") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run %v, next %v: want CANCELED", run.GetStatus(), statusOf(run, "next"))
	}
	if h.continuationCount("events") != 0 {
		t.Fatal("a canceled run switched")
	}
}

// TestContinuation_FailureDuringHoldTakesTheFailPath: a task that fails
// while the run holds takes the run's fail path as always; a run never
// switches from it.
func TestContinuation_FailureDuringHoldTakesTheFailPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach().GetIndex() == 3 {
			return nil, temporal.NewNonRetryableApplicationError("workload exited 1", "ActivityFailed", nil)
		}
		return itemBehavior(ctx, in)
	})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-hold-fail"})
	raiseHistoryAt(h, 2, 650)
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-hold-fail", compile(t, spreadDocument), spreadInput(t, 8), holdPlan(),
		(*orkstackv1.RunContinuation)(nil))
	err := h.env.GetWorkflowError()
	var can *workflow.ContinueAsNewError
	if err == nil || errors.As(err, &can) {
		t.Fatalf("workflow error = %v, want the item's failure", err)
	}
	run := mustFinalRun(t, h.store, "ork-hold-fail")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	if h.continuationCount("events") != 0 {
		t.Fatal("the run switched from its fail path")
	}
}

// TestContinuation_HoldNeverSpins proves a held, idle run always makes
// progress under the production 1 s deadlock detector: idle with an open
// await (it switches); after a failed write (it resumes); with an opening
// pin and a closing pin (it waits on them).
func TestContinuation_HoldNeverSpins(t *testing.T) {
	deadlock := worker.Options{DeadlockDetectionTimeout: time.Second}
	t.Run("idle with an open await", func(t *testing.T) {
		g := runGenerations(t, "ork-spin-await", compile(t, awaitAcrossDocument), payload(t, nil), holdPlan(), okBehavior,
			func(h *harness, generation int) {
				h.env.SetWorkerOptions(deadlock)
				if generation == 0 {
					raiseHistoryWhen(h, 650, taskStatusIs("a", orkstackv1.Status_STATUS_SUCCEEDED))
				} else {
					h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
				}
			})
		generationsOK(t, g, 2)
	})
	t.Run("after a failed write", func(t *testing.T) {
		failed := false
		g := runGenerations(t, "ork-spin-write", compile(t, spreadDocument), spreadInput(t, 8), holdPlan(), itemBehavior,
			func(h *harness, generation int) {
				h.env.SetWorkerOptions(deadlock)
				if generation != 0 {
					return
				}
				raiseHistoryAt(h, 2, 650)
				h.onContinuation = func(kind string, _ int64) error {
					if kind == "events" && !failed {
						failed = true
						return errors.New("object storage is unavailable")
					}
					return nil
				}
			})
		generationsOK(t, g, 1)
	})
	t.Run("with an opening and a closing pin", func(t *testing.T) {
		g := runGenerations(t, "ork-spin-pins", compile(t, pinOpenDocument), payload(t, nil), holdPlan(), okBehavior,
			func(h *harness, generation int) {
				enablePins(h, deadlock)
				if generation == 0 {
					raiseHistoryWhen(h, 650, taskStatusIs("prep", orkstackv1.Status_STATUS_RUNNING))
				}
			})
		generationsOK(t, g, 2)
	})
}

// TestContinuation_LogsHoldSwitchAndGiveUpOncePerDecision: each transition
// of the writer is logged once: one switch-due line per generation that
// crossed its budget, one quiesce, one switch and one restore per crossing.
func TestContinuation_LogsHoldSwitchAndGiveUpOncePerDecision(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	logs := &lineLogger{}
	suite.SetLogger(logs)
	g := runGenerationsIn(t, &suite, "ork-logs", compile(t, spreadDocument), spreadInput(t, 8), holdPlan(), itemBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				raiseHistoryAt(h, 2, 650)
			}
		})
	generationsOK(t, g, 2)
	switches := len(g.carries)
	if got := logs.count(logSwitchDue); got < switches || got > len(g.harnesses) {
		t.Fatalf("switch due logged %d times over %d generations and %d switches", got, len(g.harnesses), switches)
	}
	for msg, want := range map[string]int{logQuiesce: 1, logSwitched: switches, logRestored: switches, logGiveUp: 0} {
		if got := logs.count(msg); got != want {
			t.Fatalf("%q logged %d times, want %d", msg, got, want)
		}
	}
}

// TestContinuation_ComposedChildInFlightHoldsSwitch: a composed child in
// flight is in flight like an activity, so the run does not switch until it
// has finished; the switch then comes at the idle point right after it.
func TestContinuation_ComposedChildInFlightHoldsSwitch(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	// build's dispatch and the child's start cross a budget of 2 while the
	// child is in flight.
	budget := engine.DefaultContinuationBudget(2, 1<<30, time.Hour)
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes,
		Reserve: engine.DefaultHistoryReserve}
	plan.Continuation = &budget
	registerContinuationOn(h.env, h.snapshots)
	var childEnded atomic.Bool
	h.mockChild(func(_ workflow.Context, _ childStart) (*orkstackv1.Run, error) {
		childEnded.Store(true)
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	})
	h.run("ork-parent-child", parent, payload(t, nil), plan)
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("workflow error = %v, want the switch after the child", h.env.GetWorkflowError())
	}
	run := mustFinalRun(t, h.store, "ork-parent-child")
	if release := taskRunOfRun(run, "release"); !childEnded.Load() || release.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("release = %v at the switch, want the child finished first", release.GetStatus())
	}
	if announce := taskRunOfRun(run, "announce"); announce.GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Fatalf("announce = %v at the switch, want PENDING", announce.GetStatus())
	}
}

// registerContinuationOn registers the continuation activities on a bare
// test environment over store.
func registerContinuationOn(env *testsuite.TestWorkflowEnvironment, store *testSnapshotStore) {
	h := &harness{env: env, continuations: map[string]int{}}
	h.registerContinuationActivities(engine.NewSnapshotActivities(store))
}

// reproducedShape is one definition the switched and unswitched runs must
// agree on.
type reproducedShape struct {
	name   string
	doc    string
	input  *orkstackv1.Payload
	plan   func(engine.RunPlan) engine.RunPlan
	setup  func(h *harness, generation int)
	budget int64
}

var rfc3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`)

// comparableEvents is a run's stream as a switch must leave it: sequence,
// type, task, item, status and message, with timestamps (in messages too)
// dropped.
func comparableEvents(events []*orkstackv1.Event) []string {
	out := eventKeys(events)
	for i := range out {
		out[i] = rfc3339.ReplaceAllString(out[i], "<time>")
	}
	return out
}

// comparableInputs is every ExecuteTask input, ordered by task and item,
// with the pin's session id — one opening's, never the same twice — dropped.
func comparableInputs(t *testing.T, inputs []*orkstackv1.ExecuteTaskInput) []string {
	t.Helper()
	out := make([]string, 0, len(inputs))
	for _, in := range inputs {
		in = proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
		if in.GetPin() != nil {
			in.Pin.InstanceId = ""
		}
		data, err := protojson.MarshalOptions{}.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s#%d %s", in.GetTaskId(), in.GetEach().GetIndex(), compactJSON(data)))
	}
	slices.Sort(out)
	return out
}

func compactJSON(data []byte) string {
	var buf bytes.Buffer
	_ = json.Compact(&buf, data)
	return buf.String()
}

func reproducedShapes(t *testing.T) []reproducedShape {
	withPins := func(h *harness, _ int) { enablePins(h, worker.Options{}) }
	approveLater := func(h *harness, _ int) {
		h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "alice"))
	}
	composed := func(plan engine.RunPlan) engine.RunPlan {
		_, composedPlan, _ := composedFixture(t, reproducedChildDocument)
		plan.Children = composedPlan.Children
		return plan
	}
	flat := "name: stages\ntasks:\n"
	for stage := 0; stage < 3; stage++ {
		for i := 0; i < 3; i++ {
			needs := "[]"
			if stage > 0 {
				needs = fmt.Sprintf("[s%da, s%db, s%dc]", stage-1, stage-1, stage-1)
			}
			flat += fmt.Sprintf("  - id: s%d%c\n    uses: shell\n    needs: %s\n    with: {op: exec}\n", stage, 'a'+i, needs)
		}
	}
	return []reproducedShape{
		{name: "a chain", doc: continuationChainDocument, budget: 2},
		{name: "flat stages", doc: flat, budget: 3},
		{name: "a join with handlers", doc: guardHandlersDocument, budget: 1},
		{name: "a composed task", doc: reproducedComposedDocument, plan: composed, budget: 1},
		{name: "an await", doc: awaitAcrossDocument, setup: approveLater, budget: 2},
		{name: "a pin with an unpinned need", doc: reproducedPinDocument, setup: withPins, budget: 2},
	}
}

// reproducedComposedDocument composes a child whose input reads nothing
// upstream, so the harness's stand-in values never need a store.
const reproducedComposedDocument = `
name: parent
version: "1.0.0"
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
    with: {image: fixed}
  - id: announce
    uses: shell
    needs: [release]
    with: {op: exec}
  - id: tail
    uses: shell
    needs: [announce]
    with: {op: exec}
`

const reproducedChildDocument = `
name: release-block
version: "2.1.0"
inputs:
  image: {type: string, required: true}
tasks:
  - id: sign
    uses: shell
    with: {op: exec}
`

// reproducedPinDocument is a pin whose member needs an unpinned task, with
// work after the pin so a natural idle point follows its close.
const reproducedPinDocument = `
name: pinned-needs
pins:
  build: {}
tasks:
  - id: prep
    uses: shell
    pin: build
    needs: []
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: fetch
    uses: shell
    needs: []
    with: {command: "echo fetch"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep, fetch]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: t1
    uses: shell
    needs: [compile]
    with: {command: "echo t1"}
  - id: t2
    uses: shell
    needs: [t1]
    with: {command: "echo t2"}
  - id: t3
    uses: shell
    needs: [t2]
    with: {command: "echo t3"}
`

// TestContinuation_GenerationsReproduceTheUnswitchedRun: a run that switches
// at natural idle points only — a chain, flat stages, a join with handlers,
// a composed task, an await and a pin with an unpinned need — publishes the
// same events and dispatches the same ExecuteTask inputs as the same
// definition run without continuation, timestamps and session ids apart.
func TestContinuation_GenerationsReproduceTheUnswitchedRun(t *testing.T) {
	for _, shape := range reproducedShapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			wf := compile(t, shape.doc)
			if strings.Contains(shape.doc, "orkflow:release-block") {
				for _, task := range wf.GetTasks() {
					if task.GetOrkflow() != nil {
						task.Orkflow.DefinitionDigest = childDigest
					}
				}
			}
			input := shape.input
			if input == nil {
				input = payload(t, nil)
			}
			base := guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)
			if shape.plan != nil {
				base = shape.plan(base)
			}
			unswitched := base
			unswitched.Continuation = nil
			continued := base
			budget := engine.DefaultContinuationBudget(shape.budget, 1<<40, time.Hour)
			continued.Continuation = &budget
			setup := shape.setup

			var suite testsuite.WorkflowTestSuite
			logs := &lineLogger{}
			suite.SetLogger(logs)
			g := runGenerationsIn(t, &suite, "ork-reproduce", wf, input, continued, itemBehavior, setup)
			if g.err != nil {
				t.Fatalf("switched run failed: %v", g.err)
			}
			if len(g.carries) == 0 {
				t.Fatal("the run never switched")
			}
			if logs.count(logQuiesce) != 0 {
				t.Fatal("a hold occurred: this test switches at natural idle points only")
			}
			one := runGenerations(t, "ork-reproduce", wf, input, unswitched, itemBehavior, setup)
			if one.err != nil || len(one.harnesses) != 1 {
				t.Fatalf("unswitched run = %v over %d generations", one.err, len(one.harnesses))
			}
			gotEvents, wantEvents := comparableEvents(g.events()), comparableEvents(one.events())
			if strings.Join(gotEvents, "\n") != strings.Join(wantEvents, "\n") {
				t.Fatalf("events differ across %d switches:\n%s\nwant\n%s", len(g.carries),
					strings.Join(gotEvents, "\n"), strings.Join(wantEvents, "\n"))
			}
			gotInputs, wantInputs := comparableInputs(t, g.invocations()), comparableInputs(t, one.invocations())
			if strings.Join(gotInputs, "\n") != strings.Join(wantInputs, "\n") {
				t.Fatalf("ExecuteTask inputs differ:\n%s\nwant\n%s", strings.Join(gotInputs, "\n"), strings.Join(wantInputs, "\n"))
			}
		})
	}
}

// TestContinuation_HeldGenerationsReachTheSameOutcome: a shape that does
// hold — a fan-out with max_parallel — dispatches the same multiset of
// inputs, ends every task the same way and reaches the same outcome as the
// unswitched run.
func TestContinuation_HeldGenerationsReachTheSameOutcome(t *testing.T) {
	wf := compile(t, spreadDocument)
	input := spreadInput(t, 12)
	plan := holdPlan()
	g := runGenerations(t, "ork-held-same", wf, input, plan, itemBehavior, func(h *harness, generation int) {
		if generation == 0 {
			raiseHistoryAt(h, 2, 650)
		}
	})
	generationsOK(t, g, 2)
	unswitched := plan
	unswitched.Continuation = nil
	one := runGenerations(t, "ork-held-same", wf, input, unswitched, itemBehavior, nil)
	if one.err != nil {
		t.Fatal(one.err)
	}
	if got, want := comparableInputs(t, g.invocations()), comparableInputs(t, one.invocations()); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("inputs differ:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	final, want := g.final(t), one.final(t)
	if final.GetStatus() != want.GetStatus() {
		t.Fatalf("outcome %v, want %v", final.GetStatus(), want.GetStatus())
	}
	for _, row := range want.GetTaskRuns() {
		if got := taskRunOf(t, final, row.GetTaskId()); got.GetStatus() != row.GetStatus() || len(got.GetItems()) != len(row.GetItems()) {
			t.Fatalf("task %s = %v with %d items, want %v with %d", row.GetTaskId(), got.GetStatus(), len(got.GetItems()),
				row.GetStatus(), len(row.GetItems()))
		}
	}
}
