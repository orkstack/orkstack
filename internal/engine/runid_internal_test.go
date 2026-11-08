package engine

import (
	"regexp"
	"testing"
)

// idShape is the shape of a new run id: `ork-` and 26 characters of the TypeID
// alphabet. A test may hold an issued id to it; nothing in the engine may.
var idShape = regexp.MustCompile(`^ork-[0-7][0-9a-hjkmnp-tv-z]{25}$`)

func TestNewRunID_KeepsTheDocumentedShape(t *testing.T) {
	id := NewRunID()
	if !idShape.MatchString(id) {
		t.Fatalf("run id = %q, want %s (AGENTS.md §7)", id, idShape)
	}
	if len(id) != 30 {
		t.Fatalf("run id %q is %d characters, want 30", id, len(id))
	}
}

// TestNewRunID_IsMonotonicWithinAReplica keeps the readable half of the older
// contract: an operator following one replica's log sees an ordered series.
func TestNewRunID_IsMonotonicWithinAReplica(t *testing.T) {
	previous := NewRunID()
	for i := range 10_000 {
		next := NewRunID()
		if next <= previous {
			t.Fatalf("id %d = %q does not sort after %q", i, next, previous)
		}
		previous = next
	}
}
