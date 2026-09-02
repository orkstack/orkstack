package engine

import (
	"fmt"
	"sync/atomic"
	"time"
)

// activityIDGenerator issues the ids of the standalone activities this engine
// executes directly against a task queue — today the DescribeOrker probe
// (describeOrkerOn).
//
// A standalone activity's id is its identity on the cluster, so two of them
// must never be spelled the same way. The admin inventory probes its pools
// concurrently (`inParallel`, `internal/api`), so several goroutines ask for
// an id at once, and a host whose clock is coarser than a nanosecond hands
// them the same reading routinely: a clock reading alone is not enough.
//
// A clock reading is not a uniqueness token; the construction that is, and
// that this package already relies on for run ids, is a random nonce over a
// counter (`runid.go`, AGENTS.md §7):
//
//	sequence = (nonce << 32) | counter
//	nonce   : 32 bits from crypto/rand, drawn once per generator
//	counter : per-generator monotonic counter
//
// The timestamp stays, because an activity id is read by a human in a Temporal
// history and the time is what they are looking for. It no longer carries the
// uniqueness: freeze the clock and every id is still distinct, which is what
// TestActivityIDs_AreDistinctWhenTheClockDoesNotMove measures.
type activityIDGenerator struct {
	now     func() time.Time
	nonce   uint32
	counter atomic.Uint32
}

// newActivityIDGenerator returns a generator driven by the wall clock, with a
// fresh nonce. crypto/rand is read here, once: the nonce only has to
// distinguish this process from another one.
func newActivityIDGenerator() *activityIDGenerator {
	return &activityIDGenerator{now: time.Now, nonce: newRunIDNonce()}
}

// next returns the next id under prefix. It is safe for concurrent use.
//
// counter wraps after 2^32 ids from one generator, which cannot reissue an id:
// a wrap needs four billion activities, and they cannot share the nanosecond
// of the ids they would repeat.
func (g *activityIDGenerator) next(prefix string) string {
	sequence := uint64(g.nonce)<<32 | uint64(g.counter.Add(1))
	return fmt.Sprintf("%s-%d-%d", prefix, g.now().UTC().UnixNano(), sequence)
}

// processActivityIDs is where every standalone activity id in this process
// comes from, so one nonce covers the process however many TemporalStarters
// are wired inside it.
var processActivityIDs = newActivityIDGenerator()

// newActivityID issues a standalone activity id from the process-wide
// generator.
func newActivityID(prefix string) string { return processActivityIDs.next(prefix) }
