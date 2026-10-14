package engine

import (
	"context"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

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

// SetRetentionClockForTest makes the store's terminal retention read now.
func (s *RunStore) SetRetentionClockForTest(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retention.now = now
}

// SetRetentionClockForTest makes the bus's terminal retention read now.
func (b *EventBus) SetRetentionClockForTest(now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retention.now = now
}

// RetainedRunsForTest is how many runs the store and the bus hold: the
// store's entries and order, and the bus's logs and dedupe maps.
func RetainedRunsForTest(s *RunStore, b *EventBus) (runs, order, logs, seen int) {
	s.mu.RLock()
	runs, order = len(s.runs), len(s.order)
	s.mu.RUnlock()
	b.mu.Lock()
	logs, seen = len(b.log), len(b.seen)
	b.mu.Unlock()
	return runs, order, logs, seen
}

// CompactRunResultForTest is compactRunResult: the projection of a recorded
// top-level result onto what current code returns, which is what the replay
// payload tests compare a re-executed result with.
func CompactRunResultForTest(run *orkstackv1.Run) *orkstackv1.Run {
	return compactRunResult(run)
}

// DisableHistoryGuardForTest sets the test-only switch that makes the history
// guard never check, and returns the function that clears it: the payload
// half of the history-limit fixture re-executes its prefix with it, so the
// guard never crosses in a test environment whose history length is static.
func DisableHistoryGuardForTest() (restore func()) {
	historyGuardDisabledForTest = true
	return func() { historyGuardDisabledForTest = false }
}

// IsHistoryLimitTerminationForTest is isHistoryLimitTermination: whether a
// termination reason is the cluster's own at its history limit, the text the
// reader's backstop matches (TestIntegration_ContinueAsNewServerContract, h).
func IsHistoryLimitTerminationForTest(reason string) bool { return isHistoryLimitTermination(reason) }

// ObserveDecisionStatesForTest sets the test-only hook that sees a run's
// decision state at each switch and after each restore, and returns the
// function that clears it.
func ObserveDecisionStatesForTest(fn func(phase string, state DecisionState)) (restore func()) {
	continuationStateHookForTest = fn
	return func() { continuationStateHookForTest = nil }
}

// ContinueAtEveryIdleForTest sets the test-only switch that makes every
// natural idle point after a generation's first command due, and returns the
// function that clears it.
func ContinueAtEveryIdleForTest() (restore func()) {
	continuationAtEveryIdleForTest = true
	return func() { continuationAtEveryIdleForTest = false }
}

// WithContinuationHooksForTest sets this instance's test-only hooks, run at
// the start of every continuation write (kind "events", "snapshot" or
// "checkpoint") and every saved-state read, in the local activity's own
// goroutine. A hook blocks on a channel its test owns and never returns
// because of its context.
func (a *SnapshotActivities) WithContinuationHooksForTest(
	beforeWrite func(ctx context.Context, kind string) error,
	beforeRead func(ctx context.Context, offset int64) error,
) *SnapshotActivities {
	a.beforeContinuationWrite = beforeWrite
	a.beforeContinuationRead = beforeRead
	return a
}

// ForceContinuationForTest makes every run decide with budget and limit
// instead of its plan's, and take the continue-as-new version WITHOUT
// asking workflow.GetVersion, and returns the function that clears it: a
// negative replay test proves a committed history replays only because of
// the gate.
func ForceContinuationForTest(budget ContinuationBudget, limit HistoryLimit) (restore func()) {
	continuationPlanForTest = &budget
	historyGuardPlanForTest = &limit
	continueAsNewForcedForTest = true
	return func() {
		continuationPlanForTest = nil
		historyGuardPlanForTest = nil
		continueAsNewForcedForTest = false
	}
}
