package engine

import (
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
