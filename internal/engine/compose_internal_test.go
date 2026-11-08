package engine

import "testing"

// TestMaxRunIDLength_BoundsEveryIssuedID holds MaxRunIDLength to what the
// generator issues, because a run plan's size is measured with a placeholder of
// that length. A UUIDv7 in base32 is one fixed length.
func TestMaxRunIDLength_BoundsEveryIssuedID(t *testing.T) {
	for range 100 {
		if id := NewRunID(); len(id) != MaxRunIDLength {
			t.Fatalf("id %q is %d bytes, MaxRunIDLength is %d", id, len(id), MaxRunIDLength)
		}
	}
}
