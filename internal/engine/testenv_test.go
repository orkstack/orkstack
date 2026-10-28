package engine_test

import (
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
)

// testDeadlockDetectionTimeout is the deadlock detector's budget in the
// mocked-environment harnesses. Left unset, the SDK applies its production
// 1 s default to every workflow task, so a loaded CI runner — not the code
// under test — could fail a correctness assertion. The 1 s budget is
// proven where a test sets it explicitly (worker.Options{
// DeadlockDetectionTimeout: time.Second}), which still wins.
const testDeadlockDetectionTimeout = 60 * time.Second

// productionDeadlock is the deadlock budget a test proves a path within: the
// SDK's production 1 s, scaled under a race build (testenv_internal_test.go).
func productionDeadlock() time.Duration { return engine.ProductionDeadlockForTest() }

// generousDeadlock gives a fresh test environment the harnesses' deadlock
// budget.
func generousDeadlock(env *testsuite.TestWorkflowEnvironment) *testsuite.TestWorkflowEnvironment {
	env.SetWorkerOptions(testWorkerOptions(worker.Options{}))
	return env
}

// testWorkerOptions is options with the harnesses' deadlock budget unless
// options sets one. SetWorkerOptions replaces every option, so a test that
// sets others (interceptors, the session worker) passes them through here.
func testWorkerOptions(options worker.Options) worker.Options {
	if options.DeadlockDetectionTimeout == 0 {
		options.DeadlockDetectionTimeout = testDeadlockDetectionTimeout
	}
	return options
}

// TestGenerousDeadlock_DefaultsAndYields proves a harness environment
// tolerates a workflow task longer than the SDK's 1 s default, and that a
// test which sets the production 1 s budget explicitly still gets it.
func TestGenerousDeadlock_DefaultsAndYields(t *testing.T) {
	slow := func(workflow.Context) error {
		// Longer than 1 s of wall clock inside one workflow task.
		time.Sleep(1500 * time.Millisecond)
		return nil
	}
	t.Run("default", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := generousDeadlock(suite.NewTestWorkflowEnvironment())
		env.RegisterWorkflowWithOptions(slow, workflow.RegisterOptions{Name: "slow"})
		env.ExecuteWorkflow("slow")
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("slow workflow task under the harness budget: %v", err)
		}
	})
	t.Run("explicit 1 s", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := generousDeadlock(suite.NewTestWorkflowEnvironment())
		env.SetWorkerOptions(testWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second}))
		env.RegisterWorkflowWithOptions(slow, workflow.RegisterOptions{Name: "slow"})
		env.ExecuteWorkflow("slow")
		err := env.GetWorkflowError()
		if err == nil || !strings.Contains(err.Error(), "deadlock") {
			t.Fatalf("error = %v, want the deadlock detector's", err)
		}
	})
}
