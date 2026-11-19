package engine

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Who paused and who unpaused are marked as the request says: verified when
// OrkStack authenticated them, asserted otherwise, in the events' messages
// and on the run's last pause (AGENTS.md §7, Principal).
func TestPause_PrincipalsAreMarkedAsTheRequestSays(t *testing.T) {
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	paused := requestSignal(true, "req-1")
	paused["principal"], paused["principalVerified"] = "user:corp:ada", true
	unpaused := requestSignal(false, "req-2")
	unpaused["principal"] = "bob"
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, paused) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, unpaused) }, time.Hour)
	_ = h.run(t, twoStepDocument, pausePlan(t))
	messages := map[orkstackv1.EventType]string{}
	for _, typ := range []orkstackv1.EventType{eventRunPaused, eventRunUnpaused} {
		for _, ev := range h.sawEvent(typ) {
			messages[typ] = ev.GetMessage()
		}
	}
	if m := messages[eventRunPaused]; !strings.Contains(m, "run paused by user:corp:ada (verified)") {
		t.Errorf("RUN_PAUSED says %q", m)
	}
	if m := messages[eventRunUnpaused]; !strings.Contains(m, "run unpaused by bob (asserted)") {
		t.Errorf("RUN_UNPAUSED says %q", m)
	}
	_, run := h.last(t)
	last := run.GetLastPause()
	if last.GetPausedBy() != "user:corp:ada" || !last.GetPausedByVerified() || last.GetEndedBy() != "bob" || last.GetEndedByVerified() {
		t.Fatalf("last pause %v, want paused by ada (verified), unpaused by bob (asserted)", last)
	}
}
