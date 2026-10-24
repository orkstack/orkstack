package engine

import (
	"math"
	"testing"
	"time"
)

// TestMaxRunIDLength_BoundsEveryIssuedID holds MaxRunIDLength to the
// longest id the generator can issue: the largest int64 nanosecond count and
// the largest sequence (nonce and counter both at MaxUint32).
func TestMaxRunIDLength_BoundsEveryIssuedID(t *testing.T) {
	g := &RunIDGenerator{now: func() time.Time { return time.Unix(0, math.MaxInt64) }, nonce: math.MaxUint32}
	g.counter.Store(math.MaxUint32 - 1)
	id := g.NewRunID()
	if len(id) != MaxRunIDLength {
		t.Fatalf("the longest id %q is %d bytes, MaxRunIDLength is %d", id, len(id), MaxRunIDLength)
	}
}
