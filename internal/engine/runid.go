package engine

import (
	"fmt"
	"sync/atomic"
	"time"
)

// RunIDGenerator issues run ids of the documented shape
// `ork-{timestamp}-{sequence}` (AGENTS.md §1.5).
//
// A run id is the Temporal workflow id, so a duplicate is not cosmetic: the
// second start either fails or silently attaches to the first one's execution.
// Generation therefore has to be safe across engine replicas, not just across
// goroutines in one of them.
type RunIDGenerator struct {
	now     func() time.Time
	counter atomic.Uint32
}

// NewRunIDGenerator returns a generator driven by the wall clock.
func NewRunIDGenerator() *RunIDGenerator {
	return &RunIDGenerator{now: time.Now}
}

// NewRunID issues the next id. It runs in the API process, never inside
// workflow code.
func (g *RunIDGenerator) NewRunID() string {
	return fmt.Sprintf("ork-%d-%d", g.now().UTC().UnixNano(), g.counter.Add(1))
}

// processRunIDs is the generator every run id in this process comes from.
var processRunIDs = NewRunIDGenerator()

// NewRunID issues a run id from the process-wide generator.
func NewRunID() string { return processRunIDs.NewRunID() }
