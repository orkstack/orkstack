package engine_test

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sleepingDocument is a → sleep s (15 min) → c.
const sleepingDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: s
    needs: [a]
    sleep: {seconds: 900}
  - id: c
    uses: shell
    needs: [s]
    with: {command: "echo c"}
`

const sleepingSiblingDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-sibling
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: s
    needs: [a]
    sleep: {seconds: 3600}
  - id: c
    uses: shell
    needs: [a]
    with: {command: "exit 1"}
`

const sleepingSkippedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-skipped
inputs:
  gate: {type: boolean, default: false}
tasks:
  - id: s
    condition: "{{ .inputs.gate }}"
    sleep: {seconds: 600}
  - id: c
    uses: shell
    needs: [s]
    with: {command: "echo c"}
`

const sleepingHandlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-handler
tasks:
  - id: deploy
    uses: shell
    with: {command: "exit 1"}
  - id: settle
    needs: [deploy]
    join: any_failed
    sleep: {seconds: 120}
  - id: rollback
    uses: shell
    needs: [settle]
    join: all_done
    with: {command: "echo rollback"}
`

const sleepOnlyDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleep-only
tasks:
  - id: first
    sleep: {seconds: 60}
  - id: second
    needs: [first]
    sleep: {seconds: 120}
`

// sleepingBesideDocument is a container and a sleep that both start at once.
const sleepingBesideDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-beside
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: s
    sleep: {seconds: 60}
`

func TestRunWorkflow_SleepWaitsWithoutAnActivityThenSucceeds(t *testing.T) {
	h := newHarness(t, nil, okTask)
	var timers []time.Duration
	h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) { timers = append(timers, d) })

	h.start(t, "run-sleep", sleepingDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if got := strings.Join(h.taskOrder(), ","); got != "a,c" {
		t.Fatalf("ExecuteTask called for %s, want a,c — a sleep starts no activity", got)
	}
	if len(timers) != 1 || timers[0] != 15*time.Minute {
		t.Fatalf("timers scheduled = %v, want one of 15m", timers)
	}

	events := eventsFor(h, "run-sleep", "s")
	if got := strings.Join(eventTypes(events), ","); got != "TASK_SLEEPING,TASK_SUCCEEDED" {
		t.Fatalf("s's events = %s", got)
	}
	sleeping, woke := events[0], events[1]
	if sleeping.GetStatus() != orkstackv1.Status_STATUS_SLEEPING || woke.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("statuses = %v, %v", sleeping.GetStatus(), woke.GetStatus())
	}
	run := runOf(t, h, "run-sleep")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	s := taskRunOf(t, run, "s")
	opened := sleeping.GetTimestamp().AsTime()
	wakeAt := s.GetSleep().GetWakeAt().AsTime()
	if !wakeAt.Equal(opened.Add(15 * time.Minute)) {
		t.Fatalf("wake_at %v, want the start instant + 15m (%v)", wakeAt, opened.Add(15*time.Minute))
	}
	if want := "task sleeping until " + wakeAt.UTC().Format(time.RFC3339) + " (15m0s)"; sleeping.GetMessage() != want {
		t.Fatalf("TASK_SLEEPING message = %q, want %q", sleeping.GetMessage(), want)
	}
	if woke.GetMessage() != "task woke after 15m0s" || !woke.GetTimestamp().AsTime().Equal(wakeAt) {
		t.Fatalf("wake event = %q at %v, want at %v", woke.GetMessage(), woke.GetTimestamp().AsTime(), wakeAt)
	}
	if s.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || s.GetSleep().GetSeconds() != 900 {
		t.Fatalf("s = %v", s)
	}
	if !s.GetScheduledAt().AsTime().Equal(opened) || !s.GetStartedAt().AsTime().Equal(opened) ||
		!s.GetFinishedAt().AsTime().Equal(wakeAt) {
		t.Fatalf("scheduled_at %v / started_at %v / finished_at %v, want the start, the start and the wake-up",
			s.GetScheduledAt().AsTime(), s.GetStartedAt().AsTime(), s.GetFinishedAt().AsTime())
	}
	if s.GetValuesRef() != nil || s.GetOutput() != nil || s.GetCommit() != nil || len(s.GetArtifacts()) > 0 ||
		s.GetPool() != "" || s.GetOrkerId() != "" || s.GetAttempt() != 0 || s.GetAwait() != nil {
		t.Fatalf("a sleep carries values, a commit or container fields: %v", s)
	}
	if taskRunOf(t, run, "a").GetSleep() != nil {
		t.Fatal("a container task carries a sleep")
	}

	in, ok := h.inputFor("c")
	if !ok {
		t.Fatal("c was never dispatched")
	}
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == "s" {
			t.Fatalf("c was handed a reference for the sleep: %v", ref)
		}
	}
	if _, inOutput := run.GetOutput().GetData().GetFields()["s"]; inOutput {
		t.Fatalf("the run output names the sleep: %v", run.GetOutput())
	}
	for _, e := range eventsFor(h, "run-sleep", "c") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED && e.GetTimestamp().AsTime().Before(wakeAt) {
			t.Fatalf("c was scheduled at %v, before s woke at %v", e.GetTimestamp().AsTime(), wakeAt)
		}
	}
}

func TestRunWorkflow_CancelWhileSleepingEndsCanceled(t *testing.T) {
	h := newHarness(t, nil, okTask)
	cancelled := 0
	h.env.SetOnTimerCanceledListener(func(string) { cancelled++ })
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
	h.start(t, "run-sleep-cancel", sleepingDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-sleep-cancel")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	s := taskRunOf(t, run, "s")
	if s.GetStatus() != orkstackv1.Status_STATUS_CANCELED || s.GetFinishedAt() == nil || s.GetSleep().GetWakeAt() == nil {
		t.Fatalf("s = %v", s)
	}
	if !s.GetFinishedAt().AsTime().Before(s.GetSleep().GetWakeAt().AsTime()) {
		t.Fatalf("s finished %v, not before its wake-up %v", s.GetFinishedAt().AsTime(), s.GetSleep().GetWakeAt().AsTime())
	}
	found := false
	for _, e := range eventsFor(h, "run-sleep-cancel", "s") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG &&
			e.GetMessage() == "task canceled: run ended while this task was sleeping" &&
			e.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
			found = true
		}
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED {
			t.Fatalf("a canceled sleep succeeded: %v", e)
		}
	}
	if !found {
		t.Fatalf("no cancellation sentence for s: %v", eventTypes(eventsFor(h, "run-sleep-cancel", "s")))
	}
	if cancelled == 0 {
		t.Fatal("the sleep's timer was never cancelled")
	}
	if statusOf(run, "c") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("c = %v, want CANCELED", statusOf(run, "c"))
	}
	if slices.Contains(h.taskOrder(), "c") {
		t.Fatal("c was dispatched in a canceled run")
	}
}

func TestRunWorkflow_SiblingFailureCancelsASleep(t *testing.T) {
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "c" {
			return nil, temporal.NewNonRetryableApplicationError("c exploded", "ActivityFailed", nil)
		}
		return okTask(in)
	})
	h.start(t, "run-sleep-sibling", sleepingSiblingDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-sleep-sibling")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "c exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	s := taskRunOf(t, run, "s")
	if s.GetStatus() != orkstackv1.Status_STATUS_CANCELED || s.GetFinishedAt() == nil {
		t.Fatalf("s = %v", s)
	}
	if !run.GetFinishedAt().AsTime().Before(s.GetSleep().GetWakeAt().AsTime()) {
		t.Fatalf("the run finished %v, not before the sleep's wake-up %v: it waited out the sleep",
			run.GetFinishedAt().AsTime(), s.GetSleep().GetWakeAt().AsTime())
	}
}

func TestRunWorkflow_SkippedSleepNeverStarts(t *testing.T) {
	h := newHarness(t, nil, okTask)
	timers := 0
	h.env.SetOnTimerScheduledListener(func(string, time.Duration) { timers++ })
	h.start(t, "run-sleep-skipped", sleepingSkippedDocument, nil)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	run := runOf(t, h, "run-sleep-skipped")
	s := taskRunOf(t, run, "s")
	if s.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || s.GetSleep().GetSeconds() != 600 || s.GetSleep().GetWakeAt() != nil {
		t.Fatalf("s = %v", s)
	}
	if statusOf(run, "c") != orkstackv1.Status_STATUS_SKIPPED {
		t.Fatalf("c = %v, want the upstream skip", statusOf(run, "c"))
	}
	if timers != 0 {
		t.Fatalf("%d timers scheduled for a skipped sleep", timers)
	}
	for _, e := range eventsOf(h, "run-sleep-skipped") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING {
			t.Fatalf("a skipped sleep emitted TASK_SLEEPING: %v", e)
		}
	}
}

// A sleeping failure handler sleeps out its time in the failing phase and
// succeeds; the run still ends FAILED with its original reason, after it.
func TestRunWorkflow_SleepingFailureHandlerWakesInTheFailingPhase(t *testing.T) {
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, deployFailure()
		}
		return okTask(in)
	})
	h.start(t, "run-sleep-handler", sleepingHandlerDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-sleep-handler")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "workload exited 1") {
		t.Fatalf("run = %v / %v, want FAILED with deploy's reason", run.GetStatus(), run.GetError())
	}
	settle := taskRunOf(t, run, "settle")
	if settle.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("settle = %v", settle)
	}
	if got := settle.GetFinishedAt().AsTime().Sub(settle.GetStartedAt().AsTime()); got != 2*time.Minute {
		t.Fatalf("settle slept %v, want 2m", got)
	}
	if statusOf(run, "rollback") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback = %v, want it to run after the sleep", statusOf(run, "rollback"))
	}
	if run.GetFinishedAt().AsTime().Before(settle.GetSleep().GetWakeAt().AsTime()) {
		t.Fatalf("the run finished %v, before its sleeping handler woke at %v",
			run.GetFinishedAt().AsTime(), settle.GetSleep().GetWakeAt().AsTime())
	}
}

// An operator's cancel during a sleeping failure handler ends the run
// canceled and the handler CANCELED, never SUCCEEDED.
func TestRunWorkflow_CancelDuringASleepingHandlerCancelsIt(t *testing.T) {
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "deploy" {
			return nil, deployFailure()
		}
		return okTask(in)
	})
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
	h.start(t, "run-sleep-handler-cancel", sleepingHandlerDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-sleep-handler-cancel")
	settle := taskRunOf(t, run, "settle")
	if settle.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("settle = %v, want CANCELED", settle.GetStatus())
	}
	if statusOf(run, "rollback") == orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatal("rollback ran after an operator's cancel")
	}
}

func TestRunWorkflow_SleepOnlyOrkflowSucceeds(t *testing.T) {
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Errorf("ExecuteTask called for %q in a sleep-only orkflow", in.TaskId)
		return okTask(in)
	})
	// No route at all: a sleep-only orkflow plans with empty Routes.
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-sleep-only", compile(t, sleepOnlyDocument), payload(t, nil),
		engine.RunPlan{Namespace: "default", Routes: map[string]string{}}, (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	run := runOf(t, h, "run-sleep-only")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if len(h.invocations()) != 0 {
		t.Fatalf("%d activities dispatched", len(h.invocations()))
	}
	first, second := taskRunOf(t, run, "first"), taskRunOf(t, run, "second")
	if !second.GetStartedAt().AsTime().Equal(first.GetFinishedAt().AsTime()) ||
		second.GetFinishedAt().AsTime().Sub(first.GetStartedAt().AsTime()) != 3*time.Minute {
		t.Fatalf("first %v→%v, second %v→%v: want back to back, 3m in all",
			first.GetStartedAt().AsTime(), first.GetFinishedAt().AsTime(),
			second.GetStartedAt().AsTime(), second.GetFinishedAt().AsTime())
	}
	if fields := run.GetOutput().GetData().GetFields(); len(fields) != 0 {
		t.Fatalf("run output = %v, want empty: a sleep has no values", run.GetOutput())
	}
}

// A sleep is never held by the in-flight cap: it starts nothing, so with the
// cap filled by a running activity it still starts in the same pass.
func TestRunWorkflow_SleepIsNotHeldByTheInFlightCap(t *testing.T) {
	h := newHarness(t, nil, okTask)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-sleep-cap", compile(t, sleepingBesideDocument), payload(t, nil),
		cappedPlan(1, 0), (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	var sleepingSeq, aDoneSeq int64
	for _, e := range eventsOf(h, "run-sleep-cap") {
		switch {
		case e.GetTaskId() == "s" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
			sleepingSeq = e.GetSequence()
		case e.GetTaskId() == "a" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
			aDoneSeq = e.GetSequence()
		}
	}
	if sleepingSeq == 0 || aDoneSeq == 0 || sleepingSeq > aDoneSeq {
		t.Fatalf("s started sleeping at event %d, a succeeded at %d: the sleep waited for the cap", sleepingSeq, aDoneSeq)
	}
}

// A run with no sleep snapshots no TaskRun.sleep.
func TestRunWorkflow_NoSleepNoSleepState(t *testing.T) {
	h := newHarness(t, nil, okTask)
	h.start(t, "run-no-sleep", linearDocument, nil)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	for _, tr := range runOf(t, h, "run-no-sleep").GetTaskRuns() {
		if tr.GetSleep() != nil {
			t.Fatalf("task %q carries sleep state", tr.GetTaskId())
		}
	}
}

// sleepAcrossDocument is awaitAcrossDocument with the await a sleep: a chain
// of containers grows the history past a one-event budget while the sleep
// sleeps, so the run switches with it open.
const sleepAcrossDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleep-across
tasks:
  - id: nap
    needs: []
    sleep: {seconds: 3600}
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
    needs: [nap, c]
    with: {command: "echo merge"}
`

// TestContinuation_SleepCrossesSwitchAtItsOriginalInstant: an open sleep
// crosses a switch — it is not in flight — and wakes at its original
// instant: the next generation recreates its timer with the time remaining,
// it starts sleeping once across the whole chain, and wakes once.
func TestContinuation_SleepCrossesSwitchAtItsOriginalInstant(t *testing.T) {
	var mu sync.Mutex
	var remaining []time.Duration
	g := runGenerations(t, "ork-sleep-across", compile(t, sleepAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				return
			}
			h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
				mu.Lock()
				remaining = append(remaining, d)
				mu.Unlock()
			})
		})
	run := generationsOK(t, g, 2)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	slept, woke := 0, 0
	var wokeAt time.Time
	for _, e := range g.events() {
		if e.GetTaskId() != "nap" {
			continue
		}
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
			slept++
		case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
			woke++
			wokeAt = e.GetTimestamp().AsTime()
		}
	}
	if slept != 1 || woke != 1 {
		t.Fatalf("the sleep started %d and woke %d times across generations, want once each", slept, woke)
	}
	first := mustFinalRun(t, g.harnesses[0].store, g.runID)
	before := taskRunOf(t, first, "nap")
	if before.GetStatus() != orkstackv1.Status_STATUS_SLEEPING {
		t.Fatalf("nap was %v when generation 0 switched, want SLEEPING", before.GetStatus())
	}
	wakeAt := before.GetSleep().GetWakeAt().AsTime()
	nap := taskRunOf(t, run, "nap")
	if !nap.GetSleep().GetWakeAt().AsTime().Equal(wakeAt) || !wokeAt.Equal(wakeAt) || !nap.GetFinishedAt().AsTime().Equal(wakeAt) {
		t.Fatalf("wake_at %v after the switch, %v before; woke at %v, finished %v",
			nap.GetSleep().GetWakeAt().AsTime(), wakeAt, wokeAt, nap.GetFinishedAt().AsTime())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remaining) == 0 || remaining[0] <= 0 || remaining[0] >= time.Hour {
		t.Fatalf("recreated timers %v, want one with the time remaining of the hour", remaining)
	}
	if statusOf(run, "merge") != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("merge = %v", statusOf(run, "merge"))
	}
}
