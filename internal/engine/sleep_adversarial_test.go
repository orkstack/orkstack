package engine_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The adversarial half of the sleep's tests: races, the failing phase, a
// resolution aimed at a sleep, joins over a failed need and many sleeps at
// once. Each fails if the code is wrong.

// napBesideLongDocument is a one-minute sleep beside an hour-long one, so the
// run is still going when nap wakes and an operator's cancel can land on
// either side of nap's wake-up.
const napBesideLongDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: nap-beside-long
tasks:
  - id: nap
    sleep: {seconds: 60}
  - id: long
    sleep: {seconds: 3600}
`

// terminalEventsOf counts the events that END a task: its TASK_SUCCEEDED or
// TASK_FAILED, and a TASK_LOG carrying a terminal status (markCanceled's).
func terminalEventsOf(h *harness, runID, taskID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range eventsFor(h, runID, taskID) {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
			out = append(out, e)
		case orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
			switch e.GetStatus() {
			case orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED:
				out = append(out, e)
			}
		}
	}
	return out
}

// runTerminalEvents counts RUN_SUCCEEDED, RUN_FAILED and RUN_CANCELED.
func runTerminalEvents(h *harness, runID string) int {
	n := 0
	for _, e := range eventsOf(h, runID) {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
			orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
			n++
		}
	}
	return n
}

// An operator's cancel racing a sleep's wake-up, before it, at the same
// instant and after it: the sleep ends exactly once, with exactly one
// terminal event whose status is the one recorded, and the run ends once,
// CANCELED.
func TestRunWorkflow_CancelRacingTheWakeEndsTheSleepOnce(t *testing.T) {
	cases := []struct {
		name string
		at   time.Duration
		want orkstackv1.Status // 0: either outcome, so long as it is one
	}{
		{"cancel a second before the wake", 59 * time.Second, orkstackv1.Status_STATUS_CANCELED},
		{"cancel at the wake's instant", 60 * time.Second, 0},
		{"cancel a second after the wake", 61 * time.Second, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, okTask)
			h.env.RegisterDelayedCallback(h.env.CancelWorkflow, tc.at)
			runID := fmt.Sprintf("run-sleep-race-%d", i)
			h.start(t, runID, napBesideLongDocument, nil)
			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			run := runOf(t, h, runID)
			if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
				t.Fatalf("run = %v / %v, want CANCELED", run.GetStatus(), run.GetError())
			}
			if n := runTerminalEvents(h, runID); n != 1 {
				t.Fatalf("%d terminal run events, want exactly one", n)
			}
			nap := taskRunOf(t, run, "nap")
			ends := terminalEventsOf(h, runID, "nap")
			if len(ends) != 1 {
				t.Fatalf("nap has %d terminal events (%v), want exactly one", len(ends), eventTypes(ends))
			}
			if ends[0].GetStatus() != nap.GetStatus() {
				t.Fatalf("nap's terminal event says %v, its record %v", ends[0].GetStatus(), nap.GetStatus())
			}
			if tc.want != 0 && nap.GetStatus() != tc.want {
				t.Fatalf("nap = %v, want %v", nap.GetStatus(), tc.want)
			}
			if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED && nap.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
				t.Fatalf("nap = %v, want SUCCEEDED or CANCELED", nap.GetStatus())
			}
			if nap.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED &&
				!nap.GetFinishedAt().AsTime().Equal(nap.GetSleep().GetWakeAt().AsTime()) {
				t.Fatalf("nap woke at %v, its wake-up is %v", nap.GetFinishedAt().AsTime(), nap.GetSleep().GetWakeAt().AsTime())
			}
			// The long sleep never wakes in a canceled run.
			if long := taskRunOf(t, run, "long"); long.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
				t.Fatalf("long = %v, want CANCELED", long.GetStatus())
			}
		})
	}
}

// sleepingAllDoneHandlerDocument: settle (join all_done) starts sleeping in
// the main loop as soon as a succeeds, and only then does b fail: the run
// enters its failing phase with a handler already sleeping.
const sleepingAllDoneHandlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-all-done
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: settle
    needs: [a]
    join: all_done
    sleep: {seconds: 120}
  - id: b
    uses: shell
    needs: [a]
    with: {command: "exit 1"}
  - id: c
    uses: shell
    needs: [b]
    with: {command: "echo c"}
`

// A failure handler that was already sleeping when the run started failing
// sleeps out its time and succeeds; the run ends FAILED with b's reason,
// after the handler woke, and the task the failure cut is CANCELED.
func TestRunWorkflow_AnAlreadySleepingHandlerOutlivesTheFailure(t *testing.T) {
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "b" {
			return nil, temporal.NewNonRetryableApplicationError("b exploded", "ActivityFailed", nil)
		}
		return okTask(in)
	})
	h.start(t, "run-sleep-all-done", sleepingAllDoneHandlerDocument, nil)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-sleep-all-done")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "b exploded") {
		t.Fatalf("run = %v / %v, want FAILED with b's reason", run.GetStatus(), run.GetError())
	}
	settle := taskRunOf(t, run, "settle")
	if settle.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
		settle.GetFinishedAt().AsTime().Sub(settle.GetStartedAt().AsTime()) != 2*time.Minute {
		t.Fatalf("settle = %v, want SUCCEEDED after 2m", settle)
	}
	if b := taskRunOf(t, run, "b"); settle.GetStartedAt().AsTime().After(b.GetFinishedAt().AsTime()) {
		t.Fatalf("settle started %v, after b failed at %v: the premise of this test does not hold",
			settle.GetStartedAt().AsTime(), b.GetFinishedAt().AsTime())
	}
	if statusOf(run, "c") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("c = %v, want CANCELED by the failure", statusOf(run, "c"))
	}
	if run.GetFinishedAt().AsTime().Before(settle.GetFinishedAt().AsTime()) {
		t.Fatalf("the run finished %v, before its sleeping handler woke at %v",
			run.GetFinishedAt().AsTime(), settle.GetFinishedAt().AsTime())
	}
	if ends := terminalEventsOf(h, "run-sleep-all-done", "settle"); len(ends) != 1 {
		t.Fatalf("settle has %d terminal events, want one", len(ends))
	}
}

// awaitBesideSleepDocument holds an await, so the run reads resolutions at
// all, and a sleep whose id a resolution is aimed at.
const awaitBesideSleepDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: await-beside-sleep
tasks:
  - id: gate
    await: {timeout_seconds: 3600}
  - id: nap
    sleep: {seconds: 600}
`

// A resolution aimed at a sleeping task's id — a completion, then a
// rejection — never ends it: the sleep wakes at its own instant, nothing
// records a resolution for it, and the await beside it is resolved normally
// (the positive control: the run was reading resolutions).
func TestRunWorkflow_AResolutionAimedAtASleepNeverCompletesIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		gate bool
	}{
		{"beside an await", awaitBesideSleepDocument, true},
		{"in a run with no await", "apiVersion: orkstack/v1\nkind: Orkflow\nname: lone-sleep\ntasks:\n  - id: nap\n    sleep: {seconds: 600}\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, okTask)
			h.signalAt(time.Minute, completion("nap", `{"woken":true}`, "mallory"))
			h.signalAt(2*time.Minute, &orkstackv1.AwaitSignal{TaskId: "nap", Reject: true, Reason: "wake up",
				Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK})
			if tc.gate {
				h.signalAt(15*time.Minute, completion("gate", `{}`, "alice"))
			}
			h.start(t, "run-sleep-resolved", tc.doc, nil)
			must.OKf(t, h.env.GetWorkflowError(), "workflow error")
			run := runOf(t, h, "run-sleep-resolved")
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
			}
			nap := taskRunOf(t, run, "nap")
			if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
				!nap.GetFinishedAt().AsTime().Equal(nap.GetSleep().GetWakeAt().AsTime()) ||
				nap.GetFinishedAt().AsTime().Sub(nap.GetStartedAt().AsTime()) != 10*time.Minute {
				t.Fatalf("nap = %v, want SUCCEEDED at its own wake-up, 10m after it started", nap)
			}
			if nap.GetValuesRef() != nil || nap.GetOutput() != nil || nap.GetAwait() != nil {
				t.Fatalf("a resolution's values or door reached the sleep: %v", nap)
			}
			for _, e := range eventsFor(h, "run-sleep-resolved", "nap") {
				if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED || e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
					t.Fatalf("a resolution aimed at the sleep was applied: %v", e)
				}
			}
			if tc.gate && taskRunOf(t, run, "gate").GetAwait().GetResolvedBy() != "alice" {
				t.Fatalf("gate = %v, want resolved by alice: the run was not reading resolutions", taskRunOf(t, run, "gate"))
			}
		})
	}
}

// joinOverFailedNeedDocument is a failing need and a sleep that needs it,
// under the join the test writes in.
func joinOverFailedNeedDocument(join string) string {
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: join-over-failed\ntasks:\n" +
		"  - id: deploy\n    uses: shell\n    with: {command: \"exit 1\"}\n" +
		"  - id: settle\n    needs: [deploy]\n    sleep: {seconds: 90}\n"
	if join != "" {
		doc += "    join: " + join + "\n"
	}
	return doc
}

// A sleep whose need failed, under each join: a policy that needs success
// never starts it (no timer, CANCELED with the run's failure); a failure
// handler's policy starts it and it sleeps its full time. The run is FAILED
// with deploy's reason in every case.
func TestRunWorkflow_ASleepOverAFailedNeedFollowsItsJoin(t *testing.T) {
	cases := []struct {
		join    string
		sleeps  bool
		outcome orkstackv1.Status
	}{
		{"", false, orkstackv1.Status_STATUS_CANCELED},
		{"all_succeeded", false, orkstackv1.Status_STATUS_CANCELED},
		{"none_failed", false, orkstackv1.Status_STATUS_CANCELED},
		{"any_failed", true, orkstackv1.Status_STATUS_SUCCEEDED},
		{"all_done", true, orkstackv1.Status_STATUS_SUCCEEDED},
	}
	for _, tc := range cases {
		t.Run("join "+tc.join, func(t *testing.T) {
			h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return nil, deployFailure()
			})
			var timers []time.Duration
			h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) { timers = append(timers, d) })
			h.start(t, "run-join-over-failed", joinOverFailedNeedDocument(tc.join), nil)
			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			run := runOf(t, h, "run-join-over-failed")
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "workload exited 1") {
				t.Fatalf("run = %v / %v, want FAILED with deploy's reason", run.GetStatus(), run.GetError())
			}
			settle := taskRunOf(t, run, "settle")
			if settle.GetStatus() != tc.outcome {
				t.Fatalf("settle = %v, want %v", settle.GetStatus(), tc.outcome)
			}
			slept := 0
			for _, d := range timers {
				if d == 90*time.Second {
					slept++
				}
			}
			if slept > 1 || tc.sleeps != (slept == 1) {
				t.Fatalf("settle's timer was scheduled %d times, want sleeps=%v", slept, tc.sleeps)
			}
			if tc.sleeps && settle.GetFinishedAt().AsTime().Sub(settle.GetStartedAt().AsTime()) != 90*time.Second {
				t.Fatalf("settle slept %v, want 90s", settle.GetFinishedAt().AsTime().Sub(settle.GetStartedAt().AsTime()))
			}
			if !tc.sleeps && (settle.GetStartedAt() != nil || settle.GetSleep().GetWakeAt() != nil) {
				t.Fatalf("settle never started, yet carries a start or a wake-up: %v", settle)
			}
		})
	}
}

// manySleepsDocument is n sleeps and one container, all ready at once.
func manySleepsDocument(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: many-sleeps\ntasks:\n")
	b.WriteString("  - id: work\n    uses: shell\n    with: {command: \"echo work\"}\n")
	for i := range n {
		fmt.Fprintf(&b, "  - id: s%d\n    sleep: {seconds: %d}\n", i, 60+i)
	}
	return b.String()
}

// Fifty sleeps beside one container under an in-flight cap of ONE, with the
// history guard on: every sleep starts in the same pass as the container's
// dispatch — none waits for the cap, none takes the slot — and each wakes at
// its own instant.
func TestRunWorkflow_ManySleepsTakeNoInFlightSlot(t *testing.T) {
	const n = 50
	h := newHarness(t, nil, okTask)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-many-sleeps", compile(t, manySleepsDocument(n)), payload(t, nil),
		cappedPlan(1, 0), (*orkstackv1.RunContinuation)(nil))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	run := runOf(t, h, "run-many-sleeps")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	var workDone int64
	sleeping := map[string]int64{}
	for _, e := range eventsOf(h, "run-many-sleeps") {
		switch {
		case e.GetTaskId() == "work" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
			workDone = e.GetSequence()
		case e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
			sleeping[e.GetTaskId()] = e.GetSequence()
		}
	}
	if len(sleeping) != n || workDone == 0 {
		t.Fatalf("%d sleeps started and work's success is event %d, want %d sleeps and a success", len(sleeping), workDone, n)
	}
	for id, seq := range sleeping {
		if seq > workDone {
			t.Fatalf("%s started sleeping at event %d, after work succeeded at %d: it waited for the cap", id, seq, workDone)
		}
	}
	for i := range n {
		s := taskRunOf(t, run, fmt.Sprintf("s%d", i))
		if got := s.GetFinishedAt().AsTime().Sub(s.GetStartedAt().AsTime()); got != time.Duration(60+i)*time.Second {
			t.Fatalf("s%d slept %v, want %ds", i, got, 60+i)
		}
	}
}

// sleepsAtTheSwitchDocument is nap, the sleep that must cross, beside tick, a
// sleep whose wake-up decides when the run reaches an idle moment with its
// budget long spent: tick wakes tickSeconds in, and `after` (a container)
// needs it.
func sleepsAtTheSwitchDocument(tickSeconds int) string {
	return fmt.Sprintf(`
apiVersion: orkstack/v1
kind: Orkflow
name: sleeps-at-the-switch
tasks:
  - id: nap
    sleep: {seconds: 60}
  - id: tick
    sleep: {seconds: %d}
  - id: after
    uses: shell
    needs: [tick]
    with: {command: "echo after"}
`, tickSeconds)
}

// wakeOf is the generation whose events hold taskID's wake (TASK_SUCCEEDED),
// and how many wakes and starts the whole chain recorded for it.
func wakeOf(g *generations, taskID string) (generation, wakes, starts int) {
	generation = -1
	for i, h := range g.harnesses {
		for _, e := range h.bus.Events(g.runID, 0) {
			if e.GetTaskId() != taskID {
				continue
			}
			switch e.GetType() {
			case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
				wakes++
				generation = i
			case orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
				starts++
			}
		}
	}
	return generation, wakes, starts
}

// §7.3 with only sleeps open: a run whose budget is spent switches at its
// next idle moment, and a sleep that crosses fires once, at its ORIGINAL
// instant, never restarted. tick wakes a second before nap, and the switch
// its wake allows lands inside nap's last second.
func TestContinuation_ASleepCrossingASwitchJustBeforeItsWakeFiresOnce(t *testing.T) {
	g := runGenerations(t, "ork-sleep-at-switch", compile(t, sleepsAtTheSwitchDocument(59)), payload(t, nil),
		continuedPlan(1), okBehavior, nil)
	run := generationsOK(t, g, 2)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	first := mustFinalRun(t, g.harnesses[0].store, g.runID)
	for _, id := range []string{"nap", "tick"} {
		_, wakes, starts := wakeOf(g, id)
		if wakes != 1 || starts != 1 {
			t.Fatalf("%s started sleeping %d and woke %d times across %d generations, want once each",
				id, starts, wakes, len(g.harnesses))
		}
		row := taskRunOf(t, run, id)
		wakeAt := taskRunOf(t, first, id).GetSleep().GetWakeAt().AsTime()
		if !row.GetSleep().GetWakeAt().AsTime().Equal(wakeAt) || !row.GetFinishedAt().AsTime().Equal(wakeAt) {
			t.Fatalf("%s: wake_at %v after the chain, %v in generation 0; woke at %v",
				id, row.GetSleep().GetWakeAt().AsTime(), wakeAt, row.GetFinishedAt().AsTime())
		}
	}
	napGen, _, _ := wakeOf(g, "nap")
	tickGen, _, _ := wakeOf(g, "tick")
	if napGen <= tickGen {
		t.Fatalf("nap woke in generation %d, tick in %d: no switch landed between tick's wake and nap's, "+
			"so this did not test a crossing just before the wake", napGen, tickGen)
	}
}

// The next generation starting a second before nap's wake-up, at it, and
// long past it (the cluster slow to start it): nap is restored SLEEPING with
// its original wake_at, and wakes once — at that instant when it is still
// ahead, at once (no new timer) when it is not — never waiting its hour
// again.
func TestContinuation_ANextGenerationAtOrPastTheWakeWakesTheSleepOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration // the next generation's start, from nap's wake-up
	}{
		{"a second before the wake", -time.Second},
		{"at the wake's instant", 0},
		{"two hours past the wake", 2 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var start time.Time
			var mu sync.Mutex
			var recreated []time.Duration
			g := runGenerations(t, "ork-sleep-late", compile(t, sleepAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
				func(h *harness, generation int) {
					if generation == 0 {
						start = time.Now().UTC().Truncate(time.Second)
						h.env.SetStartTime(start)
						return
					}
					h.env.SetStartTime(start.Add(time.Hour + tc.offset))
					h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
						mu.Lock()
						recreated = append(recreated, d)
						mu.Unlock()
					})
				})
			run := generationsOK(t, g, 2)
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
			}
			first := mustFinalRun(t, g.harnesses[0].store, g.runID)
			if got := taskRunOf(t, first, "nap").GetStatus(); got != orkstackv1.Status_STATUS_SLEEPING {
				t.Fatalf("nap was %v when generation 0 switched, want SLEEPING", got)
			}
			wakeAt := start.Add(time.Hour)
			nap := taskRunOf(t, run, "nap")
			if !nap.GetSleep().GetWakeAt().AsTime().Equal(wakeAt) {
				t.Fatalf("wake_at %v, want the original %v", nap.GetSleep().GetWakeAt().AsTime(), wakeAt)
			}
			woke := wakeAt
			if tc.offset > 0 {
				woke = wakeAt.Add(tc.offset)
			}
			if !nap.GetFinishedAt().AsTime().Equal(woke) {
				t.Fatalf("nap woke at %v, want %v", nap.GetFinishedAt().AsTime(), woke)
			}
			if _, wakes, starts := wakeOf(g, "nap"); wakes != 1 || starts != 1 {
				t.Fatalf("nap started sleeping %d and woke %d times, want once each", starts, wakes)
			}
			mu.Lock()
			defer mu.Unlock()
			// Every later generation of the chain starts at the same instant
			// here, and each recreates nap's timer for what remains of it.
			if tc.offset < 0 {
				if len(recreated) == 0 {
					t.Fatal("no generation recreated nap's timer")
				}
				for _, d := range recreated {
					if d != -tc.offset {
						t.Fatalf("recreated timers %v, want each the %v remaining", recreated, -tc.offset)
					}
				}
			}
			if tc.offset >= 0 && len(recreated) != 0 {
				t.Fatalf("recreated timers %v for a wake-up already due, want none: it wakes at once", recreated)
			}
		})
	}
}

// An operator's cancel that lands in a LATER generation, while the sleep it
// restored is open: the recreated timer is cancelled, the sleep ends once,
// CANCELED, never SUCCEEDED, and the run ends once, CANCELED.
func TestContinuation_ACancelInALaterGenerationEndsTheCarriedSleepOnce(t *testing.T) {
	var cancelled int
	g := runGenerations(t, "ork-sleep-cancel-later", compile(t, sleepAcrossDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				return
			}
			h.env.SetOnTimerCanceledListener(func(string) { cancelled++ })
			h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
		})
	if len(g.harnesses) < 2 {
		t.Fatalf("the run never switched: %d generation", len(g.harnesses))
	}
	if !temporal.IsCanceledError(g.err) {
		t.Fatalf("run error = %v, want a cancellation", g.err)
	}
	run := g.final(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v / %v, want CANCELED", run.GetStatus(), run.GetError())
	}
	nap := taskRunOf(t, run, "nap")
	if nap.GetStatus() != orkstackv1.Status_STATUS_CANCELED || !nap.GetFinishedAt().AsTime().Before(nap.GetSleep().GetWakeAt().AsTime()) {
		t.Fatalf("nap = %v, want CANCELED before its wake-up", nap)
	}
	var ends, runEnds int
	for _, e := range g.events() {
		switch {
		case e.GetTaskId() == "nap" && (e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED ||
			(e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetStatus() == orkstackv1.Status_STATUS_CANCELED)):
			ends++
		case e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED:
			runEnds++
		}
	}
	if ends != 1 || runEnds != 1 {
		t.Fatalf("nap ended %d times and the run %d times across the chain, want once each", ends, runEnds)
	}
	if cancelled == 0 {
		t.Fatal("the recreated timer was never cancelled")
	}
}
