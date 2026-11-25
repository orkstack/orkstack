package engine_test

import (
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// testDeadlockDetectionTimeout is the deadlock detector's budget in the
// mocked-environment harnesses. Left unset, the SDK applies its production
// 1 s default to every workflow task, so a loaded CI runner — not the code
// under test — could fail a correctness assertion. A test that proves a path
// within the production budget holds it on thread CPU time instead
// (engine.NewDeadlockBudgetForTest), which a host's load cannot move.
const testDeadlockDetectionTimeout = 60 * time.Second

// generousDeadlock gives a fresh test environment the harnesses' deadlock
// budget.
func generousDeadlock(env *testsuite.TestWorkflowEnvironment) *testsuite.TestWorkflowEnvironment {
	env.SetWorkerOptions(testWorkerOptions(worker.Options{}))
	return env
}

// testWorkerOptions is options with the harnesses' deadlock budget unless
// options sets one, and with the interceptors every production run worker
// has (PayloadInterceptor, RunWorkerInterceptors) after the test's own, so a test's interceptor
// sees what those set. SetWorkerOptions replaces every option, so a test that
// sets others (interceptors, the session worker) passes them through here.
func testWorkerOptions(options worker.Options) worker.Options {
	if options.DeadlockDetectionTimeout == 0 {
		options.DeadlockDetectionTimeout = testDeadlockDetectionTimeout
	}
	options.Interceptors = append(append([]interceptor.WorkerInterceptor(nil), options.Interceptors...), append([]interceptor.WorkerInterceptor{engine.PayloadInterceptor(nil)}, engine.RunWorkerInterceptors()...)...)
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
		must.OKf(t, env.GetWorkflowError(), "slow workflow task under the harness budget")
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
