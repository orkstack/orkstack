package engine

import (
	"regexp"
	"strconv"
	"testing"
	"time"
)

// frozen returns a clock stuck at t, so the only thing that can distinguish
// two ids is the sequence component.
func frozen(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

var idShape = regexp.MustCompile(`^ork-(\d+)-(\d+)$`)

func TestRunIDGenerator_KeepsTheDocumentedShape(t *testing.T) {
	instant := time.Date(2026, 8, 17, 0, 53, 55, 252396515, time.UTC)
	g := NewRunIDGenerator()
	g.now = frozen(instant)

	id := g.NewRunID()
	parts := idShape.FindStringSubmatch(id)
	if parts == nil {
		t.Fatalf("run id = %q, want ork-{timestamp}-{sequence} (AGENTS.md §7)", id)
	}
	if parts[1] != strconv.FormatInt(instant.UnixNano(), 10) {
		t.Errorf("timestamp component = %q, want the clock's UnixNano %d",
			parts[1], instant.UnixNano())
	}
}

// TestRunIDGenerator_SequenceIsMonotonicWithinAReplica keeps the readable half
// of the contract: an operator following one replica's log sees an ordered
// series, even though the rendered sequence no longer starts at 1.
func TestRunIDGenerator_SequenceIsMonotonicWithinAReplica(t *testing.T) {
	g := NewRunIDGenerator()
	g.now = frozen(time.Unix(0, 1))

	previous := uint64(0)
	for i := range 100 {
		id := g.NewRunID()
		parts := idShape.FindStringSubmatch(id)
		if parts == nil {
			t.Fatalf("run id = %q, want ork-{timestamp}-{sequence}", id)
		}
		sequence, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			t.Fatalf("sequence of %q: %v", id, err)
		}
		if i > 0 && sequence != previous+1 {
			t.Fatalf("id %d sequence = %d, want %d (one more than the previous id's)",
				i, sequence, previous+1)
		}
		previous = sequence
	}
}

// TestRunIDGenerator_TwoReplicasSharingAClockIssueDifferentIDs is the
// in-process form of the cross-replica requirement: with the clock pinned to
// one instant, two generators stand in for two engine replicas submitting in
// the same nanosecond. A per-process counter makes both emit the identical
// series; the nonce is what keeps two replicas' ids apart.
func TestRunIDGenerator_TwoReplicasSharingAClockIssueDifferentIDs(t *testing.T) {
	instant := time.Unix(0, 1786928035776242000)
	replicaA, replicaB := NewRunIDGenerator(), NewRunIDGenerator()
	replicaA.now, replicaB.now = frozen(instant), frozen(instant)

	seen := map[string]string{}
	for i := range 1000 {
		for replica, g := range map[string]*RunIDGenerator{"A": replicaA, "B": replicaB} {
			id := g.NewRunID()
			if owner, clash := seen[id]; clash {
				t.Fatalf("replica %s issued %q at index %d, already issued by replica %s",
					replica, id, i, owner)
			}
			seen[id] = replica
		}
	}
}

// TestRunIDGenerator_SequenceDoesNotRestartPerProcess states the nonce's job
// directly: a fresh generator stands for a fresh replica, and its first id
// must not read as sequence 1, the way a per-process counter would make it.
func TestRunIDGenerator_SequenceDoesNotRestartPerProcess(t *testing.T) {
	instant := time.Unix(0, 1786928035776242000)
	firsts := map[string]int{}
	const replicas = 16
	for range replicas {
		g := NewRunIDGenerator()
		g.now = frozen(instant)
		id := g.NewRunID()
		parts := idShape.FindStringSubmatch(id)
		if parts == nil {
			t.Fatalf("run id = %q, want ork-{timestamp}-{sequence}", id)
		}
		if parts[2] == "1" {
			t.Errorf("a fresh replica issued %q: the sequence still restarts at 1", id)
		}
		firsts[parts[2]]++
	}
	if len(firsts) != replicas {
		t.Errorf("%d replicas produced %d distinct first sequences %v, want %d",
			replicas, len(firsts), firsts, replicas)
	}
}
