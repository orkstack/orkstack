package engine

import (
	"strconv"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestReadyLoopProbe_CountsFromWorkflowsRunningBesideEachOther: the probe's
// counters are written by the workflow code of every run that shares it, and
// a test that runs many beside one another (the 200 seeds of
// TestHistoryGuard_CompletionsLandingDuringThePassNeverTripTheGuard, which a
// -short run skips) must not make that a data race. Eight runs under one probe
// count every task's classification once, and the race detector, where it is
// built, sees no unsynchronized write.
func TestReadyLoopProbe_CountsFromWorkflowsRunningBesideEachOther(t *testing.T) {
	probe := &readyLoopProbe{}
	readyLoopProbeForTest = probe
	defer func() { readyLoopProbeForTest = nil }()
	const runs, tasks = 8, 6
	t.Run("runs", func(t *testing.T) {
		for i := range runs {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				h := newRefusalHarnessOver(t, newMemSnapshotStore())
				h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, flatDefinition(t, tasks),
					(*orkstackv1.Payload)(nil), guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes),
					(*orkstackv1.RunContinuation)(nil))
				if err := h.env.GetWorkflowError(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	if got := probe.classify.Load(); got != runs*tasks {
		t.Fatalf("%d classifications over %d runs of %d tasks, want each task's once (%d)", got, runs, tasks, runs*tasks)
	}
}
