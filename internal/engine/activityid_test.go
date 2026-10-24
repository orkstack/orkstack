package engine

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestActivityIDs_AreDistinctWhenTheClockDoesNotMove is the regression that
// matters, and it is deterministic.
//
// The admin pool inventory probes its pools together rather than one after
// another, so several goroutines ask for a `describe-orker` activity id at
// the same instant. An id derived from the clock alone is a collision waiting
// for a clock tick to be shared — a standalone activity's id is its identity
// on the cluster, so the second caller either fails or attaches to the first
// one's activity.
//
// Freezing the clock is how the property is measured rather than raced: a
// generator that derives its id from the clock produces ONE id here, whatever
// the host's timer resolution happens to be.
func TestActivityIDs_AreDistinctWhenTheClockDoesNotMove(t *testing.T) {
	frozen := time.Date(2026, 8, 22, 1, 17, 52, 0, time.UTC)
	g := &activityIDGenerator{now: func() time.Time { return frozen }, nonce: 7}

	const ids = 1024
	seen := make(map[string]struct{}, ids)
	for i := 0; i < ids; i++ {
		id := g.next("describe-orker")
		if _, dup := seen[id]; dup {
			t.Fatalf("id %q issued twice from one stopped clock after %d ids", id, i)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != ids {
		t.Fatalf("distinct ids = %d, want %d", len(seen), ids)
	}
}

// TestActivityIDs_AreDistinctAcrossAConcurrentPoolFanout runs the shape the
// caller actually has: storeFanout goroutines deep, all asking at once,
// through the process-wide generator the production call site uses.
func TestActivityIDs_AreDistinctAcrossAConcurrentPoolFanout(t *testing.T) {
	const (
		callers  = 16 // internal/api's storeFanout
		apiece   = 64
		expected = callers * apiece
	)
	issued := make([]string, expected)
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(callers)
	for c := 0; c < callers; c++ {
		go func(c int) {
			defer done.Done()
			start.Wait()
			for i := 0; i < apiece; i++ {
				issued[c*apiece+i] = newActivityID("describe-orker")
			}
		}(c)
	}
	start.Done()
	done.Wait()

	seen := make(map[string]struct{}, expected)
	for _, id := range issued {
		if !strings.HasPrefix(id, "describe-orker-") {
			t.Fatalf("id %q does not carry the prefix its call site asked for", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("id %q issued twice to a concurrent pool fan-out", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != expected {
		t.Fatalf("distinct ids = %d, want %d", len(seen), expected)
	}
}

// TestActivityIDs_TwoProcessesDoNotShareASequence is the other half of the run
// id argument (runid.go): a per-process counter alone makes two replicas count
// from 1 together, so the nonce is what separates them. Two generators stand
// in for two processes.
func TestActivityIDs_TwoProcessesDoNotShareASequence(t *testing.T) {
	frozen := time.Date(2026, 8, 22, 1, 17, 52, 0, time.UTC)
	now := func() time.Time { return frozen }
	a := newActivityIDGenerator()
	b := newActivityIDGenerator()
	a.now, b.now = now, now
	if a.nonce == b.nonce {
		t.Fatalf("two generators drew the same nonce %d; the draw is not random", a.nonce)
	}
	if got, other := a.next("describe-orker"), b.next("describe-orker"); got == other {
		t.Fatalf("two processes issued the same first id %q in one instant", got)
	}
}
