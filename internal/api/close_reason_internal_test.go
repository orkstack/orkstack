package api

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestStreamGapError_CloseReasonNamesEveryCauseWithinTheFrame: the reason a
// socket closes with at a gap fits a close frame (123 bytes, valid UTF-8)
// and names the cause of every gap it reached, whatever the number of
// ranges, their sizes and their mix: the closing clause goes first, then
// every range but the first of each cause, then the ranges themselves.
func TestStreamGapError_CloseReasonNamesEveryCauseWithinTheFrame(t *testing.T) {
	lost := func(first, last int64) engine.EventGap {
		return engine.EventGap{First: first, Last: last, Reason: "segment object missing"}
	}
	unpublished := func(first, last int64) engine.EventGap {
		return engine.EventGap{First: first, Last: last, Reason: engine.UnpublishedEventsReason}
	}
	const big = int64(1_000_000_000_000_000_000)
	for _, tc := range []struct {
		name string
		gaps []engine.EventGap
		want string
		// size is the reason's exact length in bytes, or 0 when unchecked.
		size int
	}{
		{"one range in 123 bytes", []engine.EventGap{lost(1, 1)},
			"events 1–1 of this run were lost from object storage before the run was archived; its outcome and other events are intact", 123},
		{"one range in 124 bytes", []engine.EventGap{lost(1, 12)},
			"events 1–12 of this run were lost from object storage before the run was archived", 0},
		{"one range never published", []engine.EventGap{unpublished(1, 1)},
			"events 1–1 of this run were never published; its outcome and other events are intact", 0},
		{"three ranges", []engine.EventGap{lost(1201, 2400), lost(3601, 4800), lost(6001, 7200)},
			"events 1201–2400, 3601–4800, 6001–7200 of this run were lost from object storage before the run was archived", 0},
		{"four ranges", []engine.EventGap{lost(1201, 2400), lost(3601, 4800), lost(6001, 7200), lost(8401, 9600)},
			"events 1201–2400 and 3 more ranges of this run were lost from object storage", 0},
		{"two long ranges", []engine.EventGap{lost(big, 2*big), lost(3*big, 4*big)},
			"events 1000000000000000000–2000000000000000000 and 1 more range of this run were lost from object storage", 0},
		{"never published, then lost", []engine.EventGap{unpublished(1, 1), lost(1201, 2400)},
			"events 1–1 of this run were never published, and 1201–2400 were lost from object storage", 0},
		{"never published, then four lost", []engine.EventGap{unpublished(1, 1), lost(1201, 2400), lost(3601, 4800),
			lost(6001, 7200), lost(8401, 9600)},
			"events 1–1 of this run were never published, and 1201–2400 and 3 more ranges were lost from object storage", 0},
		{"lost, then never published", []engine.EventGap{lost(1201, 2400), lost(3601, 4800), unpublished(5000, 5000)},
			"events 1201–2400 and 1 more range of this run were lost from object storage, and 5000–5000 were never published", 0},
		{"both causes, ranges too long to name", []engine.EventGap{unpublished(1, big-1), lost(big, 2*big-1)},
			"events of this run in 1 range were lost from object storage, in 1 range never published", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := (&streamGapError{runID: "ork-gap", gaps: tc.gaps}).closeReason()
			if got != tc.want {
				t.Fatalf("close reason = %q, want %q", got, tc.want)
			}
			if len(got) > maxCloseReason || !utf8.ValidString(got) {
				t.Fatalf("close reason of %d bytes (valid UTF-8 %v), want at most %d valid", len(got), utf8.ValidString(got), maxCloseReason)
			}
			if tc.size != 0 && len(got) != tc.size {
				t.Fatalf("close reason of %d bytes, want %d", len(got), tc.size)
			}
			for _, gap := range tc.gaps {
				cause := "lost from object storage"
				if gap.Unpublished() {
					cause = "never published"
				}
				if !strings.Contains(got, cause) {
					t.Fatalf("close reason %q does not say %q for %d–%d", got, cause, gap.First, gap.Last)
				}
			}
		})
	}
}
