package consent

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestEmpty_IsFrozen pins the empty table's consent digest under projection
// 1: the SHA-256 of its canonical bytes, {"tasks":[],"v":1}. Every schedule
// and trigger saved by this release records it, so it may never change; a
// release that covers more adds c2: and leaves c1: as it is.
func TestEmpty_IsFrozen(t *testing.T) {
	const golden = "c1:8273758e29b4114ebe3e0962bfd318248772032b0727a367a63c0c3c44195545"
	if got := Empty(); got != golden {
		t.Fatalf("Empty() = %s, want the frozen %s", got, golden)
	}
	if got := string(emptyBytes()); got != `{"tasks":[],"v":1}` {
		t.Fatalf("the empty table's canonical bytes are %q", got)
	}
	// The digest is of those bytes exactly: another spelling of the same
	// table is another digest, so a change in the canonical form shows here.
	sum := sha256.Sum256([]byte(`{"v":1,"tasks":[]}`))
	if Empty() == Prefix1+hex.EncodeToString(sum[:]) {
		t.Fatal("a non-canonical spelling digests to the frozen value")
	}
}

func TestValid(t *testing.T) {
	for _, tc := range []struct {
		digest string
		want   bool
	}{
		{Empty(), true},
		{"c1:" + strings.Repeat("0", 64), true},
		{"c2:" + strings.Repeat("0", 64), false},
		{"c1:" + strings.Repeat("0", 63), false},
		{"c1:" + strings.Repeat("A", 64), false},
		{"", false},
	} {
		if got := Valid(tc.digest); got != tc.want {
			t.Errorf("Valid(%q) = %v, want %v", tc.digest, got, tc.want)
		}
	}
}
