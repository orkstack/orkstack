package engine

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/task"
)

// pinnedDocument is a pin of two members, m1 → m3, whose closure holds the
// unpinned x (m3 needs x, x needs m1), with a 30-second pin timeout.
const pinnedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: pinned
pins:
  p:
    timeout_seconds: 30
tasks:
  - id: m1
    uses: shell
    pin: p
    run_as_orker_user: true
    with: {op: exec}
  - id: x
    uses: shell
    needs: [m1]
    with: {op: exec}
  - id: m3
    uses: shell
    pin: p
    needs: [x]
    run_as_orker_user: true
    with: {op: exec}
`

// pinHarness is timeoutHarness with sessions enabled and the pin's release
// recorded, so the inputs that carry the pin's bound can be read back.
type pinHarness struct {
	*timeoutHarness
	mu       sync.Mutex
	releases []*orkstackv1.ReleasePinInput
}

func newPinHarness(t *testing.T, steps map[string]step) *pinHarness {
	t.Helper()
	h := &pinHarness{timeoutHarness: newTimeoutHarness(t)}
	h.env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ReleasePinInput) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.releases = append(h.releases, in)
		return nil
	}, activity.RegisterOptions{Name: task.ActivityReleasePin})
	h.behave(steps)
	return h
}

// pinPausePlan is pausePlan whose pins' clocks stop while the run is paused.
func pinPausePlan(t *testing.T) RunPlan {
	plan := pausePlan(t)
	plan.Pause.PinTimeoutPausable = true
	plan.Pause.PinOuterMarginSeconds = DefaultPinOuterMarginSeconds
	return plan
}

// scheduledAfter reports that task's TASK_SCHEDULED comes after the event of
// type after in the run's stream.
func (h *timeoutHarness) scheduledAfter(t *testing.T, id string, after orkstackv1.EventType) bool {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	mark := -1
	for i, e := range h.events {
		if e.GetType() == after && mark < 0 {
			mark = i
		}
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED && e.GetTaskId() == id {
			return mark >= 0 && i > mark
		}
	}
	t.Fatalf("task %s was never scheduled", id)
	return false
}

// A paused run starts nothing new, an open pin's closure and members
// included (U5, §20.2): x and m3 are scheduled only after the unpause.
func TestPause_HoldsPinMembersAndClosure(t *testing.T) {
	h := newPinHarness(t, map[string]step{"m1": {takes: 10 * time.Second}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(true, "req-1")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(false, "req-2")) }, time.Minute)
	if err := h.run(t, pinnedDocument, pinPausePlan(t)); err != nil {
		t.Fatalf("run ended %v, want success", err)
	}
	for _, id := range []string{"x", "m3"} {
		if !h.scheduledAfter(t, id, eventRunUnpaused) {
			t.Fatalf("%s was scheduled while the run was paused", id)
		}
	}
}

// A pin's clock stops while its run is paused: a 30-second pin held paused
// for a minute still completes, with no PIN_TIMEOUT.
func TestPause_PinClockStopsWhilePaused(t *testing.T) {
	h := newPinHarness(t, map[string]step{"m1": {takes: 20 * time.Second}, "m3": {takes: 5 * time.Second}})
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(true, "req-1")) }, 10*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(false, "req-2")) }, 70*time.Second)
	if err := h.run(t, pinnedDocument, pinPausePlan(t)); err != nil {
		t.Fatalf("run ended %v, want success: the pin's clock was stopped while paused", err)
	}
}

// The pin's working timeout is the workflow's own timer: a member that
// outlives it is stopped PIN_TIMEOUT, its clock never having stopped.
func TestPin_WorkingTimeoutFiresWithAMemberRunning(t *testing.T) {
	h := newPinHarness(t, map[string]step{"m1": {takes: time.Minute}})
	if err := h.run(t, pinnedDocument, pinPausePlan(t)); err == nil {
		t.Fatal("the run succeeded past its pin's working timeout")
	}
	event, run := h.last(t)
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the run ended %s, want RUN_FAILED: a pin that ran out is the platform's failure, never a cancel", event.GetType())
	}
	if got := rowOf(run, "m1").GetError(); got.GetCode() != "PIN_TIMEOUT" || !strings.Contains(got.GetMessage(), "p") {
		t.Fatalf("m1 error %v, want PIN_TIMEOUT naming pin p", got)
	}
	if len(h.releases) != 1 {
		t.Fatalf("%d releases, want the pin released on its host", len(h.releases))
	}
}

// Every site that carries the pin's bound to the session, the Orker and its
// lease carries the outer bound in the new path, and the pin's own timeout
// before it (§21.4).
func TestPin_TimeoutSitesAgree(t *testing.T) {
	for name, tc := range map[string]struct {
		plan RunPlan
		want int32
	}{
		"pausable": {plan: pinPausePlan(t), want: int32(30 + pinPausePlan(t).Pause.MaxTimeoutSeconds + DefaultPinOuterMarginSeconds)},
		"before":   {plan: pausePlan(t), want: 30},
	} {
		t.Run(name, func(t *testing.T) {
			h := newPinHarness(t, nil)
			var seen []int32
			if err := h.run(t, pinnedDocument, tc.plan); err != nil {
				t.Fatalf("run ended %v", err)
			}
			for _, r := range h.releases {
				seen = append(seen, r.GetPin().GetTimeoutSeconds())
			}
			if !slices.Equal(seen, []int32{tc.want}) {
				t.Fatalf("release bound %v, want [%d]", seen, tc.want)
			}
		})
	}
}

// pinDeadline's arithmetic over stop and run cycles.
func TestPinDeadline_StopAndRun(t *testing.T) {
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	d := newPinDeadline(30*time.Second, at)
	d.stop(at.Add(10 * time.Second))
	if d.remaining != 20*time.Second || d.running() {
		t.Fatalf("after a stop at 10s: remaining %s running %v, want 20s stopped", d.remaining, d.running())
	}
	d.stop(at.Add(40 * time.Second)) // a second stop changes nothing
	d.run(at.Add(70 * time.Second))
	if !d.running() || d.dueAt() != at.Add(90*time.Second) {
		t.Fatalf("after a run at 70s: due %s, want 90s", d.dueAt())
	}
	if d.stoppedFor(at.Add(90*time.Second)) != time.Minute {
		t.Fatalf("stopped for %s, want 1m", d.stoppedFor(at.Add(90*time.Second)))
	}
}
