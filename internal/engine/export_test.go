package engine

// The interpreter's shared skip-reason helpers, exported to the external test
// package so TestClassify_UsesTheSharedSkipReasons can compare RunWorkflow's
// emitted events against them without a second harness.
var (
	SkipReasonUpstream  = skipReasonUpstream
	SkipReasonCondition = skipReasonCondition
)
