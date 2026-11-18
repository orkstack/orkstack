package engine

import (
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// testDeadlockDetectionTimeout is the deadlock detector's budget in the
// mocked-environment harnesses. Left unset, the SDK applies its production
// 1 s default to every workflow task, so a loaded CI runner — not the code
// under test — could fail a correctness assertion. A test that proves a path
// within the production budget holds it on thread CPU time instead
// (deadlockBudget), which a host's load cannot move.
const testDeadlockDetectionTimeout = 60 * time.Second

// generousDeadlock gives a fresh test environment the harnesses' deadlock
// budget.
func generousDeadlock(env *testsuite.TestWorkflowEnvironment) *testsuite.TestWorkflowEnvironment {
	env.SetWorkerOptions(testWorkerOptions(worker.Options{}))
	return env
}

// testWorkerOptions is options with the harnesses' deadlock budget unless
// options sets one, and with the interceptors every production run worker
// has (RunWorkerInterceptors) after the test's own, so a test's interceptor
// sees what those set. SetWorkerOptions replaces every option, so a test that
// sets others (interceptors, the session worker) passes them through here.
func testWorkerOptions(options worker.Options) worker.Options {
	if options.DeadlockDetectionTimeout == 0 {
		options.DeadlockDetectionTimeout = testDeadlockDetectionTimeout
	}
	options.Interceptors = append(append([]interceptor.WorkerInterceptor(nil), options.Interceptors...), RunWorkerInterceptors()...)
	return options
}

// productionDeadlock is the SDK's production deadlock budget, 1 s: the wall
// clock deadlockBudget falls back to where there is no thread clock. A race
// build's instrumentation slows CPU-bound workflow code several times over,
// so there the budget is scaled by raceSlowdown, never dropped, the way the
// wall-clock bounds beside it are.
func productionDeadlock() time.Duration {
	if raceBuild {
		return time.Second * raceSlowdown
	}
	return time.Second
}
