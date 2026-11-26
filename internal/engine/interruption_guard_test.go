package engine_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestHistoryGuard_TwoHundredBouncesStayWithinTheBound: two hundred stop
// bounces of one task go through the history guard as new dispatch (design
// §4.10). With room, the task runs its 201st time and the run succeeds,
// never failing for the bounces; with the history pushed to its line part-way
// through, the guard stops the run RUN_HISTORY_LIMIT, explicitly, before the
// bounces are done, and nothing is dispatched after it.
func TestHistoryGuard_TwoHundredBouncesStayWithinTheBound(t *testing.T) {
	const bounces = 200
	script := func() *scriptedBehavior {
		steps := make([]error, bounces)
		for i := range steps {
			steps[i] = stopBounceError()
		}
		return newScript(map[string][]error{"work": steps})
	}
	plan := func() engine.RunPlan {
		p := guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)
		budgets := engine.DefaultInterruptionBudgets
		p.Interruptions = &budgets
		return p
	}

	t.Run("with room every bounce runs again", func(t *testing.T) {
		b := script()
		h := newHarnessCtx(t, nil, b.run)
		// A real cluster starts a new workflow task, and the guard forgets its
		// pending counts, at every activity completion; the test environment's
		// history length moves only when a test moves it, so it moves here at
		// each snapshot (one per bounce), as it would.
		var mu sync.Mutex
		length := 100
		h.onSnapshot = func(*orkstackv1.Run, int64) error {
			mu.Lock()
			defer mu.Unlock()
			length += 12
			h.raiseHistory(length)
			return nil
		}
		h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-bounces", compile(t, fmt.Sprintf(oneTaskDocument, "")),
			payload(t, nil), plan(), (*orkstackv1.RunContinuation)(nil))
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if got := b.count("work"); got != bounces+1 {
			t.Fatalf("work ran %d times, want %d", got, bounces+1)
		}
		row := taskRunOf(t, mustFinalRun(t, h.store, "ork-bounces"), "work")
		if st := row.GetInterruptions(); st.GetNeverStarted() != bounces || st.GetOwnAttemptsUsed() != 0 {
			t.Fatalf("interruptions = %v, want %d bounces and no own attempt", st, bounces)
		}
	})

	t.Run("at the line the guard stops the run", func(t *testing.T) {
		b := script()
		h := newHarnessCtx(t, nil, b.run)
		// Past the hundredth bounce the cluster's history is at its line.
		h.onSnapshot = onceAt(h, int(engine.DefaultHistoryMaxEvents)-10, func(run *orkstackv1.Run) bool {
			for _, tr := range run.GetTaskRuns() {
				if tr.GetTaskId() == "work" {
					return tr.GetInterruptions().GetNeverStarted() >= bounces/2
				}
			}
			return false
		})
		h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-bounces", compile(t, fmt.Sprintf(oneTaskDocument, "")),
			payload(t, nil), plan(), (*orkstackv1.RunContinuation)(nil))
		requireHistoryLimitError(t, h.env)
		got := b.count("work")
		if got != bounces/2 {
			t.Fatalf("work ran %d times, want the guard to stop it at bounce %d, before its next dispatch", got, bounces/2)
		}
		// The run failed, so the task waiting to run again is not run again.
		if row := taskRunOf(t, mustFinalRun(t, h.store, "ork-bounces"), "work"); row.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
			row.GetError().GetCode() != "ORKER_STOPPED" || !strings.Contains(row.GetError().GetMessage(), "not run again") {
			t.Fatalf("work = %v %v %q, want FAILED ORKER_STOPPED, not run again", row.GetStatus(), row.GetError().GetCode(), row.GetError().GetMessage())
		}
	})
}
