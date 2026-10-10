//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_OrkerMaxConcurrentBoundsAFanOut is F-orker-storage-perf-1's
// end-to-end half: an Orker booted with orker.max_concurrent=2, its task
// worker built with the Orker's own Tuner exactly as cmd/orker builds it,
// serving a 4-item fan-out with no max_parallel, never has more than two
// ExecuteTask attempts in flight — every item is dispatched at once, and only
// the ceiling decides how many this Orker takes.
func TestIntegration_OrkerMaxConcurrentBoundsAFanOut(t *testing.T) {
	e := startEachE2E(t, true)
	refs, _ := e.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(e.store, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs,
		Pool: task.DefaultPool, OrkerID: "ceiling-1", WorkRoot: e.workRoot,
		MaxConcurrent: 2,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	t.Cleanup(activities.Close)
	tuner, err := activities.WorkerTuner()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	inFlight, peak, executed := 0, 0, 0
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true, Tuner: tuner,
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		inFlight++
		executed++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}()
		return activities.ExecuteTask(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	described, err := activities.DescribeOrker(e.ctx)
	if err != nil || described.GetMaxConcurrent() != 2 {
		t.Fatalf("DescribeOrker max_concurrent = %d (%v); want 2", described.GetMaxConcurrent(), err)
	}

	runID := fmt.Sprintf("ork-ceiling-%d", time.Now().UnixNano())
	// Each item sleeps long enough that an unbounded Orker would run all four
	// at once; max_parallel 0 is "absent".
	e.start(runID, e.eachDefinition("ceiling", 4, 0, "sleep 2 && "))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	mu.Lock()
	defer mu.Unlock()
	if peak > 2 {
		t.Fatalf("peak in-flight ExecuteTask = %d; the ceiling is 2", peak)
	}
	if peak < 2 {
		t.Fatalf("peak in-flight ExecuteTask = %d; four sleeping items never overlapped, so the bound was not exercised", peak)
	}
	proof.Write(t, "engine", "orker-max-concurrent.txt",
		[]byte(fmt.Sprintf("max_concurrent=2 items=4 max_parallel=absent executed=%d peak_in_flight=%d\n", executed, peak)))
}
