package servicehost

import (
	"math"
	"testing"
)

// TestAnswerBytes: the size docs/EVENT_SOURCE_IMAGES.md states, and never one
// that wrapped into something small.
func TestAnswerBytes(t *testing.T) {
	for _, tc := range []struct {
		lanes int32
		body  int64
		want  int64
	}{
		{1, 1 << 20, 2<<20 + 256<<10 + 1<<20}, {4, 1 << 20, 4*(2<<20+256<<10) + 1<<20}, {64, 8 << 20, 64*(16<<20+256<<10) + 1<<20},
		{0, 1 << 20, 1 << 20}, {-1, 1 << 20, 1 << 20}, {1, 0, 256<<10 + 1<<20}, {1, -1, 256<<10 + 1<<20},
		{64, math.MaxInt64, math.MaxInt64}, {1, math.MaxInt64 / 2, math.MaxInt64}, {2, math.MaxInt64 / 4, math.MaxInt64},
	} {
		if got := AnswerBytes(tc.lanes, tc.body); got != tc.want {
			t.Errorf("AnswerBytes(%d, %d) = %d, want %d", tc.lanes, tc.body, got, tc.want)
		}
	}
	if got := AnswerInodes(4); got != 128 {
		t.Errorf("AnswerInodes(4) = %d", got)
	}
}
