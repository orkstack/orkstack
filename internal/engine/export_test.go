package engine

// The interpreter's shared skip-reason helpers, exported to the external test
// package so TestClassify_UsesTheSharedSkipReasons can compare RunWorkflow's
// emitted events against them without a second harness.
var (
	SkipReasonUpstream  = skipReasonUpstream
	SkipReasonCondition = skipReasonCondition
)

// DropCacheContextForTest sets the test-only switch that makes cacheContextFor
// return nil, and returns the function that clears it.
func DropCacheContextForTest() (restore func()) {
	dropCacheContextForTest = true
	return func() { dropCacheContextForTest = false }
}

// JoinHandlersOnRunContextForTest sets the test-only switch that makes
// dispatchContext return runCtx for a failure handler, and returns the
// function that clears it.
func JoinHandlersOnRunContextForTest() (restore func()) {
	joinHandlersOnRunContextForTest = true
	return func() { joinHandlersOnRunContextForTest = false }
}
