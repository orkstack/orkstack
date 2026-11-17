package engine

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func pausePlan(t *testing.T) RunPlan {
	plan := guardPlan(t, 1_000_000, 1<<40)
	plan.Pause = &PausePlan{DefaultTimeoutSeconds: 7 * 24 * 3600, MaxTimeoutSeconds: 31 * 24 * 3600,
		ApplyReserveEvents: 20, CascadeEventsPerChild: 4, InheritedGraceSeconds: 600, InheritedGraceMaxLevels: 8}
	return plan
}

func pauseSignal(pause bool, timeoutSeconds string) map[string]any {
	return map[string]any{"format": 1, "pause": pause, "principal": "alice", "reason": "db maintenance",
		"requestId": "req-1", "timeoutSeconds": timeoutSeconds}
}

func (h *timeoutHarness) sawEvent(eventType orkstackv1.EventType) []*orkstackv1.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*orkstackv1.Event
	for _, e := range h.events {
		if e.GetType() == eventType {
			out = append(out, e)
		}
	}
	return out
}

// A paused run starts nothing new: ship waits while build finishes, and
// starts after the unpause; the run succeeds.
func TestPause_HoldsNewWorkUntilUnpaused(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	must.OK(t, h.run(t, twoStepDocument, pausePlan(t)))
	paused, unpaused := h.sawEvent(eventRunPaused), h.sawEvent(eventRunUnpaused)
	if len(paused) != 1 || paused[0].GetStatus() != statusPaused || !strings.Contains(paused[0].GetMessage(), "run paused by alice") {
		t.Fatalf("RUN_PAUSED events %v", paused)
	}
	if len(unpaused) != 1 {
		t.Fatalf("RUN_UNPAUSED events %v", unpaused)
	}
	_, run := h.last(t)
	ship := rowOf(run, "ship")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || ship.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s, ship %s", run.GetStatus(), ship.GetStatus())
	}
	if !ship.GetScheduledAt().AsTime().After(paused[0].GetTimestamp().AsTime().Add(30 * time.Minute)) {
		t.Fatalf("ship was dispatched at %s, before the unpause", ship.GetScheduledAt().AsTime())
	}
}

// A pause that is not lifted ends the run RUN_PAUSE_TIMEOUT, TimedOut.
func TestPause_ExpiryEndsTheRun(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
	err := h.run(t, twoStepDocument, pausePlan(t))
	requireCode(t, err, RunPauseTimeoutCode)
	_, run := h.last(t)
	if run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT ||
		!strings.Contains(run.GetError().GetMessage(), "not unpaused before") {
		t.Fatalf("run error %v", run.GetError())
	}
	if got := rowOf(run, "ship").GetStatus(); got != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("ship %s, want CANCELED: it never started", got)
	}
}

// A run whose plan lacks Pause has no channel: a request is never read.
func TestPause_NotPausableWithoutThePlan(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	must.OK(t, h.run(t, twoStepDocument, guardPlan(t, 1_000_000, 1<<40)))
	if len(h.sawEvent(eventRunPaused)) != 0 {
		t.Fatal("a run whose plan lacks Pause was paused")
	}
}

// A pause plan whose numbers cannot bound a pause is named, never used.
func TestPausePlan_Fault(t *testing.T) {
	good := PausePlan{DefaultTimeoutSeconds: 60, MaxTimeoutSeconds: 120, InheritedGraceSeconds: 10, InheritedGraceMaxLevels: 8}
	tests := []struct {
		name  string
		edit  func(*PausePlan)
		fault bool
	}{
		{"valid", func(*PausePlan) {}, false},
		{"default equals max", func(p *PausePlan) { p.DefaultTimeoutSeconds = 120 }, false},
		{"zero max", func(p *PausePlan) { p.MaxTimeoutSeconds = 0 }, true},
		{"zero default", func(p *PausePlan) { p.DefaultTimeoutSeconds = 0 }, true},
		{"default above max", func(p *PausePlan) { p.DefaultTimeoutSeconds = 121 }, true},
		{"negative grace", func(p *PausePlan) { p.InheritedGraceSeconds = -1 }, true},
		{"zero levels", func(p *PausePlan) { p.InheritedGraceMaxLevels = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := good
			tt.edit(&p)
			if got := p.fault(); (got != "") != tt.fault {
				t.Fatalf("fault() = %q, want fault %v", got, tt.fault)
			}
		})
	}
}

// A request the run cannot apply is named, so it is dropped with a warning.
func TestPauseRequest_Fault(t *testing.T) {
	s := &runState{plan: RunPlan{Pause: &PausePlan{DefaultTimeoutSeconds: 60, MaxTimeoutSeconds: 120,
		InheritedGraceSeconds: 10, InheritedGraceMaxLevels: 8}}, lastCascadeSeq: 2}
	tests := []struct {
		name  string
		req   pauseRequest
		fault bool
	}{
		{"own pause", pauseRequest{Format: 1, Pause: true, RequestID: "r"}, false},
		{"other format", pauseRequest{Format: 2, Pause: true, RequestID: "r"}, true},
		{"no request id", pauseRequest{Format: 1, Pause: true}, true},
		{"inherited, not the parent", pauseRequest{Format: 1, Pause: true, Inherited: true, ParentRunID: "ork-x", CascadeSeq: 3,
			OriginExpiresAt: "2026-11-13T00:00:00Z"}, true},
		{"inherited, stale sequence", pauseRequest{Format: 1, Pause: true, Inherited: true, CascadeSeq: 2,
			OriginExpiresAt: "2026-11-13T00:00:00Z"}, true},
		{"inherited pause, bad origin expiry", pauseRequest{Format: 1, Pause: true, Inherited: true, CascadeSeq: 3,
			OriginExpiresAt: "tomorrow"}, true},
		{"inherited pause, no origin expiry", pauseRequest{Format: 1, Pause: true, Inherited: true, CascadeSeq: 3}, true},
		{"inherited pause", pauseRequest{Format: 1, Pause: true, Inherited: true, CascadeSeq: 3,
			OriginExpiresAt: "2026-11-13T00:00:00Z"}, false},
		{"inherited unpause needs no expiry", pauseRequest{Format: 1, Inherited: true, CascadeSeq: 3}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.pauseRequestFault(&tt.req); (got != "") != tt.fault {
				t.Fatalf("pauseRequestFault() = %q, want fault %v", got, tt.fault)
			}
		})
	}
	bad := &runState{plan: RunPlan{Pause: &PausePlan{}}}
	if got := bad.pauseRequestFault(&pauseRequest{Format: 1, Pause: true, RequestID: "r"}); got == "" {
		t.Fatal("a request to a run whose pause plan is invalid was accepted")
	}
}

// A pause timeout names itself on never-started rows and the failing note,
// never the history guard's text.
func TestRunCauseText_PauseTimeout(t *testing.T) {
	s := &runState{runError: &orkstackv1.Error{Code: RunPauseTimeoutCode}}
	if got := s.runCauseText(); !strings.Contains(got, "pause") {
		t.Fatalf("runCauseText() = %q, want it to name the pause", got)
	}
}

// FuzzPauseRequest holds the signal decoder and its checks to their claims:
// no input panics, and a request they accept has format 1, a request id when
// it is the API's, and a parseable originating expiry when it is an
// inherited pause.
func FuzzPauseRequest(f *testing.F) {
	f.Add([]byte(`{"format":1,"pause":true,"principal":"alice","requestId":"req-1","timeoutSeconds":"3600"}`))
	f.Add([]byte(`{"format":1,"pause":false,"requestId":"req-2"}`))
	f.Add([]byte(`{"format":1,"pause":true,"inherited":true,"cascadeSeq":"3","originExpiresAt":"2026-11-13T00:00:00Z","depth":"2"}`))
	f.Add([]byte(`{"format":2}`))
	f.Add([]byte(`{"format":1,"timeoutSeconds":3600}`))
	f.Add([]byte(`not json`))
	s := &runState{plan: RunPlan{Pause: &PausePlan{DefaultTimeoutSeconds: 60, MaxTimeoutSeconds: 120,
		InheritedGraceSeconds: 10, InheritedGraceMaxLevels: 8}}}
	f.Fuzz(func(t *testing.T, data []byte) {
		req, err := parsePauseRequest(data)
		if err != nil || s.pauseRequestFault(req) != "" {
			return
		}
		if req.Format != 1 {
			t.Fatalf("accepted format %d", req.Format)
		}
		if !req.Inherited && req.RequestID == "" {
			t.Fatal("accepted an API request with no request id")
		}
		if req.Inherited && req.Pause {
			if _, err := time.Parse(time.RFC3339Nano, req.OriginExpiresAt); err != nil {
				t.Fatalf("accepted an inherited pause with expiry %q", req.OriginExpiresAt)
			}
		}
		if d := s.pauseTimeout(req); d <= 0 || d > 120*time.Second {
			t.Fatalf("pause length %s outside the plan's bound", d)
		}
	})
}
