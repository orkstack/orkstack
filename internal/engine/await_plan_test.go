package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// failingProbe fails the test the moment anything asks the cluster.
type failingProbe struct{ t *testing.T }

func (p failingProbe) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	p.t.Fatal("Plan probed a pool for an orkflow with no container task")
	return engine.PoolLiveness{}, nil
}

const mixedAwaitDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: mixed-await
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: gate
    needs: [crunch]
    await: {timeout_seconds: 60}
`

func TestPoolResolverPlan_SkipsAwaitTasks(t *testing.T) {
	t.Run("an await-only definition plans with empty routes and no probe", func(t *testing.T) {
		resolver := &engine.PoolResolver{Probe: failingProbe{t}}
		planned, plan, err := resolver.Plan(context.Background(), nil, compile(t, awaitOnlyDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan.Routes) != 0 {
			t.Fatalf("routes = %v, want none", plan.Routes)
		}
		for _, task := range planned.GetTasks() {
			if task.GetPool() != "" {
				t.Fatalf("await %q was given pool %q", task.GetId(), task.GetPool())
			}
		}
	})
	t.Run("a mixed definition routes only the container tasks", func(t *testing.T) {
		resolver := &engine.PoolResolver{Probe: everyPoolLive{}}
		planned, plan, err := resolver.Plan(context.Background(), nil, compile(t, mixedAwaitDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan.Routes) != 1 || plan.Routes["gpu"] == "" {
			t.Fatalf("routes = %v, want only gpu", plan.Routes)
		}
		if got := planned.GetTasks()[1].GetPool(); got != "" {
			t.Fatalf("the await was given pool %q", got)
		}
	})
	t.Run("CheckAllowedPools ignores the await", func(t *testing.T) {
		ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"batch"}}
		resolver := &engine.PoolResolver{}
		if err := resolver.CheckAllowedPools(ns, compile(t, awaitOnlyDocument)); err != nil {
			t.Fatalf("CheckAllowedPools refused an await-only orkflow: %v", err)
		}
	})
}

// A plan with no callback await encodes byte for byte as it did before
// RunPlan.Awaits existed: the golden history's recorded plan is compared
// against what current code would send.
func TestRunPlan_EncodesIdenticallyWithoutAwaits(t *testing.T) {
	plan := engine.RunPlan{
		Namespace:             "default",
		Routes:                map[string]string{"main": "ork-tasks"},
		CommitRoot:            "s3://orkstack-artifacts",
		TransferBudgetSeconds: 60,
	}
	got, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"namespace":"default","routes":{"main":"ork-tasks"},"commit_root":"s3://orkstack-artifacts",` +
		`"limits":{"values_max_bytes":0,"artifact_max_count":0,"artifact_max_bytes":0,` +
		`"log_max_bytes":0,"commit_max_bytes":0},"transfer_budget_seconds":60}`
	if string(got) != want {
		t.Fatalf("plan encodes as\n  %s\nwant the pre-awaits bytes\n  %s", got, want)
	}
	plan.Awaits = map[string]engine.AwaitCredential{"gate": {Salt: "aa", Digest: "bb"}}
	withAwait, _ := json.Marshal(plan)
	if string(withAwait) == want {
		t.Fatal("a plan with a callback await encodes like one without")
	}
}

func TestSimulate_AwaitIsAwaitsAndTakesAWave(t *testing.T) {
	wf := compile(t, awaitedDocument)
	plan := simulate(t, wf, nil, nil)
	a, b, c := planTaskOf(t, plan, "a"), planTaskOf(t, plan, "b"), planTaskOf(t, plan, "c")
	if a.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || a.GetAwait() != nil {
		t.Fatalf("a = %v", a)
	}
	if b.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS || b.GetWave() != 2 {
		t.Fatalf("b = %v, want AWAITS in wave 2", b)
	}
	if b.GetAwait().GetTimeoutSeconds() != 3600 || !b.GetAwait().GetCallback() {
		t.Fatalf("b's AwaitSpec = %v", b.GetAwait())
	}
	if c.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || c.GetWave() != 3 {
		t.Fatalf("c = %v, want RUNS in wave 3", c)
	}
	if plan.GetRuns() != 2 || plan.GetAwaits() != 1 || plan.GetWaveCount() != 3 {
		t.Fatalf("runs %d awaits %d waves %d, want 2/1/3", plan.GetRuns(), plan.GetAwaits(), plan.GetWaveCount())
	}
	if got := fmt.Sprint(plan.GetWaves()[1].GetTaskIds()); got != "[b]" {
		t.Fatalf("wave 2 = %s, want [b]", got)
	}

	// A skipped await still carries its spec.
	skipped := simulate(t, compile(t, awaitedSkippedDocument), nil, nil)
	if got := planTaskOf(t, skipped, "b"); got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED || got.GetAwait() == nil {
		t.Fatalf("skipped await = %v", got)
	}
	if skipped.GetAwaits() != 0 {
		t.Fatalf("awaits = %d for a skipped await", skipped.GetAwaits())
	}
}

func TestSimulate_AssumedOutputStubsAnAwaitsResolution(t *testing.T) {
	wf := compile(t, awaitedConditionDocument)
	no := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"b": {"approved": false}}))
	if got := planTaskOf(t, no, "c").GetOutcome(); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Fatalf("approved=false: c = %v", got)
	}
	yes := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"b": {"approved": true}}))
	if got := planTaskOf(t, yes, "c").GetOutcome(); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("approved=true: c = %v", got)
	}
	if len(yes.GetUnusedAssumedOutputs()) != 0 {
		t.Fatalf("an await's stub was reported unused: %v", yes.GetUnusedAssumedOutputs())
	}
}

// interpretAwaited is interpret with every open await completed by a delayed
// resolution carrying assumed[id] (or {}), so the agreement check extends to
// awaits: AWAITS ⇔ TASK_AWAITING, and the await ends SUCCEEDED.
func interpretAwaited(t *testing.T, runID string, wf *orkstackv1.Orkflow, assumed map[string]map[string]any) (*orkstackv1.Run, *harness) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		values := assumed[in.TaskId]
		if values == nil {
			values = map[string]any{}
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(values)}, nil
	})
	step := time.Minute
	for i, task := range wf.GetTasks() {
		if task.GetAwait() == nil {
			continue
		}
		values := assumed[task.GetId()]
		if values == nil {
			values = map[string]any{}
		}
		body := valuesJSON(values)
		h.signalAt(time.Duration(i+1)*step, &orkstackv1.AwaitSignal{
			TaskId:     task.GetId(),
			Values:     valuesRef(task.GetId(), body),
			ValuesJson: body,
			Via:        orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		})
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatalf("%s: workflow did not complete", runID)
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run, h
}

// TestSimulate_AgreesWithTheInterpreterOnAwaits is the plan/interpreter
// agreement gate extended to awaited tasks.
func TestSimulate_AgreesWithTheInterpreterOnAwaits(t *testing.T) {
	cases := []struct {
		name    string
		wf      *orkstackv1.Orkflow
		assumed map[string]map[string]any
	}{
		{"await then container", compile(t, awaitedDocument), map[string]map[string]any{"b": {"model_uri": "s3://m"}}},
		{"await approves", compile(t, awaitedConditionDocument), map[string]map[string]any{"b": {"approved": true}}},
		{"await declines", compile(t, awaitedConditionDocument), map[string]map[string]any{"b": {"approved": false}}},
		{"await only", compile(t, awaitOnlyDocument), nil},
		{"skipped await", compile(t, awaitedSkippedDocument), nil},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := fmt.Sprintf("run-simulate-await-%d", i)
			plan := simulate(t, tc.wf, nil, assumedPayloads(t, tc.assumed))
			run, h := interpretAwaited(t, runID, tc.wf, tc.assumed)
			awaiting := map[string]bool{}
			for _, e := range eventsOf(h, runID) {
				if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING {
					awaiting[e.GetTaskId()] = true
				}
			}
			for _, task := range plan.GetTasks() {
				id := task.GetTaskId()
				isAwaits := task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS
				if isAwaits != awaiting[id] {
					t.Errorf("%s: planned %v, TASK_AWAITING emitted = %v", id, task.GetOutcome(), awaiting[id])
				}
				if isAwaits && statusOf(run, id) != orkstackv1.Status_STATUS_SUCCEEDED {
					t.Errorf("%s: planned AWAITS, interpreter %v", id, statusOf(run, id))
				}
				if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS && statusOf(run, id) != orkstackv1.Status_STATUS_SUCCEEDED {
					t.Errorf("%s: planned RUNS, interpreter %v", id, statusOf(run, id))
				}
				if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED && statusOf(run, id) != orkstackv1.Status_STATUS_SKIPPED {
					t.Errorf("%s: planned SKIPPED, interpreter %v", id, statusOf(run, id))
				}
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Errorf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
			}
		})
	}
}
