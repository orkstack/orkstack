//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_PinsAndCarriedGenerationsCarryTheFairnessKey: on a real
// cluster, a run whose plan carries a fairness selection and that carries on
// across generations records, in every generation's own history, the plan's
// key and weight on every task, every fan-out item, every pinned member and
// both pins' creation requests — the last scheduled through the run worker's
// interceptor (engine.RunWorkerInterceptors), as cmd/ork builds it.
func TestIntegration_PinsAndCarriedGenerationsCarryTheFairnessKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "fair-reach", false, canFixtureBehavior)
	budget := engine.DefaultContinuationBudget(40, 1<<20, 5*time.Minute)
	plan := s.plan(1000, &budget)
	plan.Fairness = &engine.FairnessSelection{Key: "e1-reach", Weight: 2, PinRequests: true}
	want := temporal.Priority{FairnessKey: "e1-reach", FairnessWeight: 2}
	runID := fmt.Sprintf("ork-fair-reach-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, carriedFairDocument), spreadInput(t, 8), plan), "StartRun")
	must.OKf(t, s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil), "the run failed")
	described, err := s.client.DescribeWorkflowExecution(ctx, runID, "")
	must.OK(t, err)
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("%d generations, want the run to carry on", len(chain))
	}
	tasks, creations, carriedTasks, carriedCreations := 0, 0, 0, 0
	for generation, execution := range chain {
		recorded := s.history(runID, execution)
		for _, attrs := range recordedScheduled(recorded, task.ActivityExecuteTask) {
			if got := recordedPriorityOf(attrs); got != want {
				t.Fatalf("generation %d scheduled ExecuteTask %s with %+v, want %+v", generation, attrs.GetActivityId(), got, want)
			}
			tasks++
			if generation > 0 {
				carriedTasks++
			}
		}
		for _, attrs := range recordedScheduled(recorded, sessionCreationActivityName) {
			if got := recordedPriorityOf(attrs); got != want {
				t.Fatalf("generation %d scheduled a pin creation request with %+v, want %+v", generation, got, want)
			}
			creations++
			if generation > 0 {
				carriedCreations++
			}
		}
		must.OKf(t, replayRecorded(recorded, runID), "generation %d does not replay", generation)
	}
	// a, gap, b, and eight items; two pins.
	if tasks != 11 || creations != 2 {
		t.Fatalf("the run scheduled %d task activities and %d pin creation requests, want 11 and 2", tasks, creations)
	}
	if carriedTasks == 0 || carriedCreations == 0 {
		t.Fatalf("carried generations scheduled %d task activities and %d pin creation requests; want both after a carry",
			carriedTasks, carriedCreations)
	}
	result := fmt.Sprintf("run %s: %d generations; %d task activities (%d after a carry) and %d pin creation requests "+
		"(%d after a carry), every one recorded with fairness key %q weight %v\n",
		runID, len(chain), tasks, carriedTasks, creations, carriedCreations, want.FairnessKey, want.FairnessWeight)
	proof.Write(t, "engine", "fair-reach.txt", []byte(result))
	t.Logf("EVIDENCE %s", result)
}
