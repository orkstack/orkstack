package engine

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// deferPausePlan is reservePausePlan with continuation on and deferral
// pinned: a pause request no generation-0 history fits is carried into the
// next generation and applied there first (D-S2-1, PR2).
func deferPausePlan(t *testing.T, maxDeferred int64) RunPlan {
	plan := reservePausePlan(t)
	plan.Pause.MaxDeferredRequests = maxDeferred
	budget := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
	plan.Continuation = &budget
	return plan
}

// namedPauseSignal is pauseSignal with its own request id.
func namedPauseSignal(pause bool, requestID string) map[string]any {
	s := pauseSignal(pause, "3600")
	s["requestId"] = requestID
	return s
}

// deferThenSwitch runs a three-task chain under deferPausePlan, sending the
// signals at their offsets, and returns the generation it carried on into.
func deferThenSwitch(t *testing.T, maxDeferred int64, signals map[time.Duration]map[string]any) (*timeoutHarness, *pausedGeneration) {
	t.Helper()
	h := newTimeoutHarness(t)
	h.carriesOn()
	h.taskTakes(10*time.Second, 0)
	for at, sig := range signals {
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, sig) }, at)
	}
	start := h.env.Now()
	err := h.run(t, chainOf(3), deferPausePlan(t, maxDeferred))
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("generation 0 ended %v, want it to carry the request on", err)
	}
	out := &pausedGeneration{store: h.store, at: h.env.Now(), start: start}
	var runID string
	var input *orkstackv1.Payload
	must.OK(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &runID, &out.wf, &input, &out.plan, &out.carry))
	// The next generation's history has room: its reserve is the ordinary one.
	out.plan.Pause.ApplyReserveEvents = pausePlan(t).Pause.ApplyReserveEvents
	return h, out
}

// A pause request that does not fit the history is not applied there: the
// run starts nothing more, carries the request on at its idle point, and the
// next generation applies it before anything else, so the run is paused, not
// stopped RUN_HISTORY_LIMIT.
func TestPause_OverReserveIsCarriedAcrossTheSwitch(t *testing.T) {
	h, g := deferThenSwitch(t, 64, map[time.Duration]map[string]any{5 * time.Second: namedPauseSignal(true, "req-1")})
	if n := len(h.sawEvent(eventRunPaused)); n != 0 {
		t.Fatalf("generation 0 emitted RUN_PAUSED %d times; the request did not fit there", n)
	}
	if !slices.Equal(h.tasks, []string{"c1"}) {
		t.Fatalf("generation 0 dispatched %v, want only c1: a carried request holds new work", h.tasks)
	}
	deferred := g.carry.GetDeferredPauseRequests()
	if len(deferred) != 1 || deferred[0].GetRequestId() != "req-1" || !deferred[0].GetPause() {
		t.Fatalf("carried requests %v, want req-1's pause", deferred)
	}
	next, err := g.next(t, func(h *timeoutHarness) {
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(false, "req-2")) }, time.Minute)
	})
	must.OK(t, err)
	paused := next.sawEvent(eventRunPaused)
	if len(paused) != 1 || paused[0].GetRequestId() != "req-1" {
		t.Fatalf("generation 1 RUN_PAUSED %v, want req-1's, applied first", paused)
	}
	if !slices.Equal(next.tasks, []string{"c2", "c3"}) {
		t.Fatalf("generation 1 dispatched %v, want c2 and c3 after the unpause", next.tasks)
	}
}

// Carried requests keep their order: a pause then an unpause carried
// together leave the next generation unpaused, having been paused.
func TestPause_CarriedRequestsKeepTheirOrder(t *testing.T) {
	_, g := deferThenSwitch(t, 64, map[time.Duration]map[string]any{
		5 * time.Second: namedPauseSignal(true, "req-1"),
		6 * time.Second: namedPauseSignal(false, "req-2"),
	})
	var ids []string
	for _, r := range g.carry.GetDeferredPauseRequests() {
		ids = append(ids, r.GetRequestId())
	}
	if !slices.Equal(ids, []string{"req-1", "req-2"}) {
		t.Fatalf("carried %v, want req-1 then req-2", ids)
	}
	next, err := g.next(t, nil)
	must.OK(t, err)
	if len(next.sawEvent(eventRunPaused)) != 1 || len(next.sawEvent(eventRunUnpaused)) != 1 {
		t.Fatalf("generation 1 paused %d, unpaused %d; want one of each, in order",
			len(next.sawEvent(eventRunPaused)), len(next.sawEvent(eventRunUnpaused)))
	}
}

// The carried requests are bounded by the plan: one past it stops the run
// RUN_HISTORY_LIMIT, explicitly, naming that request.
func TestPause_CarriedRequestsOverTheBoundStopTheRun(t *testing.T) {
	h := newTimeoutHarness(t)
	h.carriesOn()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(true, "req-1")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(false, "req-2")) }, 6*time.Second)
	err := h.run(t, chainOf(3), deferPausePlan(t, 1))
	requireCode(t, err, RunHistoryLimitCode)
	if !strings.Contains(err.Error(), "req-2") {
		t.Fatalf("refusal %q does not name the request past the bound", err)
	}
}

// A plan that pins no deferral keeps PR1's ending even with continuation on:
// RUN_HISTORY_LIMIT naming the request (a run submitted before PR2).
func TestPause_OverReserveWithoutDeferralStillStops(t *testing.T) {
	h := newTimeoutHarness(t)
	h.carriesOn()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, namedPauseSignal(true, "req-1")) }, 5*time.Second)
	err := h.run(t, chainOf(3), deferPausePlan(t, 0))
	requireCode(t, err, RunHistoryLimitCode)
}

// A carry whose requests exceed the plan's bound, or hold a request that is
// not a pause request, is refused at restore: RUN_STATE_*, never applied.
func TestAdoptPauseCarry_DeferredRequests(t *testing.T) {
	plan := RunPlan{Pause: deferPausePlan(t, 2).Pause}
	ok := &orkstackv1.RunPauseSignal{Format: 1, Pause: true, RequestId: "req-1", Principal: "alice"}
	for name, tc := range map[string]struct {
		requests []*orkstackv1.RunPauseSignal
		refused  bool
	}{
		"within the bound": {requests: []*orkstackv1.RunPauseSignal{ok, ok}},
		"over the bound":   {requests: []*orkstackv1.RunPauseSignal{ok, ok, ok}, refused: true},
		"unknown format":   {requests: []*orkstackv1.RunPauseSignal{{Format: 9, Pause: true}}, refused: true},
		"nil request":      {requests: []*orkstackv1.RunPauseSignal{nil}, refused: true},
		"no request id":    {requests: []*orkstackv1.RunPauseSignal{{Format: 1, Pause: true}}, refused: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := &runState{versions: map[string]workflow.Version{pauseCarryVersion: 1, pauseDeferredVersion: 1},
				pauses: &fakeReceiveChannel{}, plan: plan}
			got := s.adoptPauseCarry(&orkstackv1.RunContinuation{DeferredPauseRequests: tc.requests}, &orkstackv1.Run{})
			if (got != "") != tc.refused {
				t.Fatalf("adoptPauseCarry = %q, refused want %v", got, tc.refused)
			}
		})
	}
}

// An idle run carrying a pause request waits for a cascade send still live,
// which holds its switch, rather than giving the request up; with none live
// it is stranded, and with no carried request it scans again.
func TestIdleUnpaused_WaitsForALiveCascadeSend(t *testing.T) {
	for name, tc := range map[string]struct {
		hold, quiescing bool
		live            int
		want            idleAction
	}{
		"nothing carried":         {want: idleRescan},
		"nothing carried, a send": {live: 1, want: idleRescan},
		"carried, a send live":    {hold: true, live: 1, want: idleWait},
		"quiescing, a send live":  {quiescing: true, live: 1, want: idleWait},
		"carried, nothing live":   {hold: true, want: idleStranded},
	} {
		t.Run(name, func(t *testing.T) {
			s := &runState{deferHold: tc.hold, quiescing: tc.quiescing, cascade: cascadeState{live: tc.live}, pins: map[string]*pinState{}}
			if got := s.idleUnpaused(); got != tc.want {
				t.Fatalf("idleUnpaused = %d, want %d", got, tc.want)
			}
		})
	}
}
