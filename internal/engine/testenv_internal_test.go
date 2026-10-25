package engine

import (
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// testDeadlockDetectionTimeout is the deadlock detector's budget in the
// mocked-environment harnesses. Left unset, the SDK applies its production
// 1 s default to every workflow task, so a loaded CI runner — not the code
// under test — could fail a correctness assertion. The 1 s budget is
// proven where a test sets it explicitly (worker.Options{
// DeadlockDetectionTimeout: time.Second}), which still wins.
const testDeadlockDetectionTimeout = 60 * time.Second

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
