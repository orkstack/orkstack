package engine_test

import (
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const profiledTaskDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted
tasks:
  - id: work
    uses: shell
    pool: edge
    devices: [sensor]
    with: {op: exec}
`

// TestRunWorkflow_AContractFaultRunsTheTaskAgainOnTheFaultBudget: a profiled
// task of a run that pinned contracts lists the contract faults on every
// dispatch, and one handed back PROFILE_CONTRACT_NOT_SERVED before it
// started runs again without spending its one attempt; a plain run's task
// lists none of them.
func TestRunWorkflow_AContractFaultRunsTheTaskAgainOnTheFaultBudget(t *testing.T) {
	plan := defaultPlan("edge")
	budgets := engine.DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	plan.ProfileIdentities = map[string]map[string]string{"edge": {"sensor": "d1"}}
	plan.ProfileContracts = map[string]map[string]engine.ProfileContracts{"edge": {"sensor": {
		Set: []engine.ContractPin{{Label: "1", Digests: []string{"d1"}}}}}}
	notServed := interruptionError("PROFILE_CONTRACT_NOT_SERVED", &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "orker-a"})
	b := newScript(map[string][]error{"work": {notServed}})
	_, capture, err := runInterrupted(t, profiledTaskDocument, plan, b, nil)
	must.OKf(t, err, "workflow error")
	if got := b.count("work"); got != 2 {
		t.Fatalf("work ran %d times, want 2", got)
	}
	options := capture.of(task.ActivityExecuteTask)
	for i, o := range options {
		types := o.RetryPolicy.NonRetryableErrorTypes
		if o.RetryPolicy.MaximumAttempts != 1 || !slices.Contains(types, "OrkInterruption:PROFILE_CHANGED") ||
			!slices.Contains(types, "OrkInterruption:PROFILE_CONTRACT_NOT_SERVED") {
			t.Fatalf("dispatch %d policy = %+v, want the contract faults listed on one attempt", i, o.RetryPolicy)
		}
	}

	plain := interruptionPlan(nil)
	b = newScript(nil)
	_, capture, err = runInterrupted(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: interrupted
tasks:
  - id: work
    uses: shell
    with: {op: exec}
`, plain, b, nil)
	must.OKf(t, err, "plain workflow error")
	for _, o := range capture.of(task.ActivityExecuteTask) {
		if slices.Contains(o.RetryPolicy.NonRetryableErrorTypes, "OrkInterruption:PROFILE_CONTRACT_NOT_SERVED") {
			t.Fatalf("a plain task listed a contract fault: %v", o.RetryPolicy.NonRetryableErrorTypes)
		}
	}
}
