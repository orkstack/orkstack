package engine

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// RunIDGenerator issues run ids of the documented shape
// `ork-{timestamp}-{sequence}` (AGENTS.md §7).
//
// A run id is the Temporal workflow id, so a duplicate is not cosmetic: the
// second start either fails or silently attaches to the first one's execution.
// Generation therefore has to be safe across engine replicas, not just across
// goroutines in one of them, and a per-process counter is not — two replicas
// submitting in the same clock tick both count from 1.
//
// The sequence component carries the entropy that fixes it, as a uint64
// rendered in decimal:
//
//	sequence = (nonce << 32) | counter
//	nonce   : 32 bits from crypto/rand, drawn once per generator
//	counter : per-generator monotonic counter, the field starts at zero
//
// That satisfies the three constraints at once. The id still reads
// `ork-{timestamp}-{sequence}`. The sequence is still monotonic within a
// replica, because only the low half moves, so one replica's log is still an
// ordered series. And it never restarts at 1, because the high half is drawn
// fresh per process.
//
// Entropy alone is a probability, not a guarantee, so it is only half the
// answer: TemporalStarter.StartRun asks the server to reject a reused id
// outright (see ErrRunIDTaken), which is what turns "unlikely" into "proven".
type RunIDGenerator struct {
	now     func() time.Time
	nonce   uint32
	counter atomic.Uint32
}

// NewRunIDGenerator returns a generator driven by the wall clock, with a fresh
// nonce. crypto/rand is read here, once, and never on the submit path: a
// syscall per id would buy nothing, because the nonce only has to distinguish
// this process from another one.
func NewRunIDGenerator() *RunIDGenerator {
	return &RunIDGenerator{now: time.Now, nonce: newRunIDNonce()}
}

// NewRunID issues the next id. It runs in the API process, never inside
// workflow code, and is safe for concurrent use.
//
// counter wraps after 2^32 ids from one generator. That cannot reissue an id:
// a wrap needs four billion submissions, which cannot share one nanosecond
// with the ids it would repeat.
func (g *RunIDGenerator) NewRunID() string {
	sequence := uint64(g.nonce)<<32 | uint64(g.counter.Add(1))
	return fmt.Sprintf("ork-%d-%d", g.now().UTC().UnixNano(), sequence)
}

// newRunIDNonce draws the high half of the sequence.
//
// Zero is excluded so a fresh replica can never render its first sequence as
// "1", a misleadingly ordinal-looking id.
func newRunIDNonce() uint32 {
	var b [4]byte
	nonce := uint32(0)
	if _, err := rand.Read(b[:]); err == nil {
		nonce = binary.BigEndian.Uint32(b[:])
	} else {
		// crypto/rand.Read has not returned an error since Go 1.24, and
		// library code must not panic, so the fallback still has to differ
		// between two processes on one host.
		nonce = uint32(time.Now().UnixNano()) ^ uint32(os.Getpid())<<16
	}
	if nonce == 0 {
		nonce = 1
	}
	return nonce
}

// processRunIDs is the generator every run id in this process comes from, so
// one nonce covers the process however many Services are wired inside it.
var processRunIDs = NewRunIDGenerator()

// NewRunID issues a run id from the process-wide generator.
func NewRunID() string { return processRunIDs.NewRunID() }
