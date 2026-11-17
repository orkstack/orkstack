package engine

import (
	"strconv"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const testParentRunID = "ork-parent"

// childPausePlan is pausePlan for a child run of testParentRunID.
func childPausePlan(t *testing.T) RunPlan {
	plan := pausePlan(t)
	plan.Origin = &RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: testParentRunID}
	return plan
}

// inheritedSignal is the parent's cascaded request; expiresIn is from now.
func (h *timeoutHarness) inheritedSignal(pause bool, seq int, expiresIn time.Duration) map[string]any {
	return map[string]any{"format": 1, "pause": pause, "inherited": true, "parentRunId": testParentRunID,
		"cascadeSeq": strconv.Itoa(seq), "depth": "1", "originExpiresAt": h.env.Now().Add(expiresIn).UTC().Format(time.RFC3339Nano)}
}

// A second pause of a paused run changes nothing: one RUN_PAUSED, and one
// unpause lifts it.
func TestPause_DuplicatePauseIsANoChange(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	must.OK(t, h.run(t, twoStepDocument, pausePlan(t)))
	if n := len(h.sawEvent(eventRunPaused)); n != 1 {
		t.Fatalf("%d RUN_PAUSED events, want 1", n)
	}
}

// An unpause of a run that is not paused says so and changes nothing.
func TestPause_UnpauseOfARunNotPaused(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, 5*time.Second)
	must.OK(t, h.run(t, twoStepDocument, pausePlan(t)))
	unpaused := h.sawEvent(eventRunUnpaused)
	if len(unpaused) != 1 || unpaused[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING ||
		!strings.Contains(unpaused[0].GetMessage(), "not paused") {
		t.Fatalf("RUN_UNPAUSED events %v, want one saying the run is not paused", unpaused)
	}
	if len(h.sawEvent(eventRunPaused)) != 0 {
		t.Fatal("an unpause paused the run")
	}
}

// A pause inherited from a parent that never lifts it ends the run at its
// backstop, the originating expiry plus the grace, RUN_PAUSE_TIMEOUT.
func TestPause_InheritedBackstopEndsTheRun(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(PauseSignal, h.inheritedSignal(true, 1, time.Minute))
	}, 5*time.Second)
	requireCode(t, h.run(t, twoStepDocument, childPausePlan(t)), RunPauseTimeoutCode)
	_, run := h.last(t)
	if !strings.Contains(run.GetError().GetMessage(), "paused because parent run "+testParentRunID+" was paused") {
		t.Fatalf("run error %v", run.GetError())
	}
	// 5s + 1m to the originating expiry, + 600s of grace for one level.
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took < 665*time.Second || took > 666*time.Second {
		t.Fatalf("the run ended %s after it started, want at its backstop, 11m5s", took)
	}
}

// A failing run whose inherited pause reaches its backstop keeps its own
// cause, and its held handler never starts.
func TestPause_InheritedBackstopWhileFailingKeepsCause(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(PauseSignal, h.inheritedSignal(true, 1, time.Minute))
	}, 5*time.Second)
	requireCode(t, h.run(t, handlerDocument, childPausePlan(t)), "TASK_FAILED")
	_, run := h.last(t)
	message := run.GetError().GetMessage()
	if run.GetError().GetCode() != "RUN_FAILED" || !strings.Contains(message, "inherited from parent run "+testParentRunID) ||
		!strings.Contains(message, "rollback did not start") {
		t.Fatalf("run error %v; want build's cause with the backstop as detail", run.GetError())
	}
	if rollback := rowOf(run, "rollback"); rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil {
		t.Fatalf("rollback %s scheduled=%v; want CANCELED, never dispatched", rollback.GetStatus(), rollback.GetScheduledAt())
	}
}

// The parent's unpause lifts an inherited pause of a failing run: its held
// handler then runs.
func TestPause_InheritedUnpauseLetsHandlersRun(t *testing.T) {
	h := newTimeoutHarness(t)
	h.behave(map[string]step{"build": {takes: time.Minute, fails: true}})
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(PauseSignal, h.inheritedSignal(true, 1, time.Hour))
	}, 5*time.Second)
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(PauseSignal, h.inheritedSignal(false, 2, 0))
	}, 30*time.Minute)
	requireCode(t, h.run(t, handlerDocument, childPausePlan(t)), "TASK_FAILED")
	_, run := h.last(t)
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
		rollback.GetScheduledAt().AsTime().Sub(run.GetStartedAt().AsTime()) < 30*time.Minute {
		t.Fatalf("rollback %s at %v; want SUCCEEDED, dispatched after the parent's unpause", rollback.GetStatus(), rollback.GetScheduledAt())
	}
}

// A run that ends while paused — canceled, or failed by a task that was
// running when it paused — records its pause as its last one, ended
// RUN_ENDED at the run's end, and carries no current pause. "expired" is a
// control: a pause that ends the run by expiring was already ended
// TIMED_OUT at its expiry, and the terminal record keeps that.
func TestPause_TerminalSnapshotRecordsHowThePauseEnded(t *testing.T) {
	t.Run("canceled while paused", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
		h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Hour)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		_, run := h.last(t)
		last := run.GetLastPause()
		if run.GetPause() != nil || last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED ||
			!last.GetEndedAt().AsTime().Equal(run.GetFinishedAt().AsTime()) || last.GetPausedBy() != "alice" {
			t.Fatalf("terminal pause %v, last pause %v; want alice's pause ended RUN_ENDED at the run's end", run.GetPause(), last)
		}
	})
	t.Run("failed while paused", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.behave(map[string]step{"build": {takes: 10 * time.Second, fails: true}})
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
		requireCode(t, h.run(t, twoStepDocument, pausePlan(t)), "TASK_FAILED")
		_, run := h.last(t)
		last := run.GetLastPause()
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetPause() != nil ||
			last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED ||
			!last.GetEndedAt().AsTime().Equal(run.GetFinishedAt().AsTime()) {
			t.Fatalf("run %s, terminal pause %v, last pause %v; want FAILED with its pause ended RUN_ENDED at the run's end",
				run.GetStatus(), run.GetPause(), last)
		}
	})
	t.Run("expired as the run failed", func(t *testing.T) {
		// The pause's timer and the failing task's answer are due at one
		// instant: the pause had lapsed, so it ended TIMED_OUT at its
		// expiry. A control: the test environment takes the timer first, so
		// it cannot put both in one workflow task as a cluster can.
		h := newTimeoutHarness(t)
		h.behave(map[string]step{"build": {takes: 65 * time.Second, fails: true}})
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		_, run := h.last(t)
		last := run.GetLastPause()
		if run.GetPause() != nil || last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT ||
			!last.GetEndedAt().AsTime().Equal(last.GetExpiresAt().AsTime()) {
			t.Fatalf("run %s, terminal pause %v, last pause %v; want it ended TIMED_OUT at its expiry",
				run.GetStatus(), run.GetPause(), last)
		}
	})
	t.Run("expired as the run was canceled", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Minute, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
		h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 65*time.Second)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		_, run := h.last(t)
		last := run.GetLastPause()
		if run.GetPause() != nil || last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT ||
			!last.GetEndedAt().AsTime().Equal(last.GetExpiresAt().AsTime()) {
			t.Fatalf("run %s, terminal pause %v, last pause %v; want it ended TIMED_OUT at its expiry",
				run.GetStatus(), run.GetPause(), last)
		}
	})
	t.Run("expired", func(t *testing.T) {
		h := newTimeoutHarness(t)
		h.taskTakes(10*time.Second, 0)
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
		_ = h.run(t, twoStepDocument, pausePlan(t))
		_, run := h.last(t)
		last := run.GetLastPause()
		if run.GetPause() != nil || last.GetEnd() != orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT ||
			!last.GetEndedAt().AsTime().Equal(last.GetExpiresAt().AsTime()) {
			t.Fatalf("terminal pause %v, last pause %v; want it ended TIMED_OUT at its expiry", run.GetPause(), last)
		}
	})
}
