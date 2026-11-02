package api

import (
	"encoding/binary"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/engine"
)

// wsCloseReasonMax is the most a WebSocket close frame's reason holds: RFC
// 6455 §5.5 allows a control frame 125 bytes of payload, two of them the
// status code.
const wsCloseReasonMax = 125 - 2

const (
	lostFromStorage = "lost from object storage"
	neverPublished  = "never published"
)

// requireCloseReasonProperties holds a close reason to what the bridge
// promises whatever the gaps: a sentence that fits a close frame and is valid
// UTF-8, that says for each gap why its events are missing and says nothing
// of a cause none of the gaps has — a gap the run never published is never
// called lost — and that never carries a gap's own reason text, which no
// frame can hold beside a range. It returns the reason.
func requireCloseReasonProperties(t testing.TB, gaps []engine.EventGap) string {
	t.Helper()
	got := (&streamGapError{runID: "ork-gap", gaps: gaps}).closeReason()
	if got == "" || len(got) > wsCloseReasonMax || !utf8.ValidString(got) {
		t.Fatalf("close reason %q: %d bytes (valid UTF-8 %v), want a sentence of at most %d valid bytes",
			got, len(got), utf8.ValidString(got), wsCloseReasonMax)
	}
	var lost, unpublished bool
	for _, gap := range gaps {
		if gap.Unpublished() {
			unpublished = true
		} else {
			lost = true
		}
		if gap.Reason != "" && strings.Contains(got, gap.Reason) {
			t.Fatalf("close reason %q carries the gap's own reason %q", got, gap.Reason)
		}
	}
	if lost != strings.Contains(got, lostFromStorage) {
		t.Fatalf("close reason %q: says lost from object storage %v, but a lost gap is present %v",
			got, strings.Contains(got, lostFromStorage), lost)
	}
	if unpublished != strings.Contains(got, neverPublished) {
		t.Fatalf("close reason %q: says never published %v, but an unpublished gap is present %v",
			got, strings.Contains(got, neverPublished), unpublished)
	}
	if again := (&streamGapError{runID: "ork-gap", gaps: gaps}).closeReason(); again != got {
		t.Fatalf("close reason differs between calls: %q then %q", got, again)
	}
	return got
}

// closeReasonCases are the gap lists the close reason is held to, each with
// the fragments the reason must and must not hold and, where the boundary of
// the frame is the point, its exact length. The same lists seed
// FuzzCloseReason.
var closeReasonCases = func() []struct {
	name  string
	gaps  []engine.EventGap
	names []string // fragments the reason must hold
	omits []string // fragments it must not hold
	size  int      // the reason's exact length in bytes, or 0 when unchecked
} {
	lost := func(first, last int64) engine.EventGap {
		return engine.EventGap{First: first, Last: last, Reason: "segment object missing"}
	}
	unpublished := func(first, last int64) engine.EventGap {
		return engine.EventGap{First: first, Last: last, Reason: engine.UnpublishedEventsReason}
	}
	const big = int64(1_000_000_000_000_000_000)
	const intact = "outcome and other events are intact"
	type row = struct {
		name  string
		gaps  []engine.EventGap
		names []string
		omits []string
		size  int
	}
	return []row{
		{name: "one range that fills the frame exactly", gaps: []engine.EventGap{lost(1, 1)},
			names: []string{"1–1", intact}, size: wsCloseReasonMax},
		{name: "one range a byte over the frame drops the closing clause", gaps: []engine.EventGap{lost(1, 12)},
			names: []string{"1–12"}, omits: []string{intact}},
		{name: "one range never published", gaps: []engine.EventGap{unpublished(1, 1)},
			names: []string{"1–1", intact}},
		{name: "three ranges are all named", gaps: []engine.EventGap{lost(1201, 2400), lost(3601, 4800), lost(6001, 7200)},
			names: []string{"1201–2400", "3601–4800", "6001–7200"}},
		{name: "four ranges name the first and count the rest",
			gaps:  []engine.EventGap{lost(1201, 2400), lost(3601, 4800), lost(6001, 7200), lost(8401, 9600)},
			names: []string{"1201–2400", "3 more ranges"}, omits: []string{"3601"}},
		{name: "two long ranges name the first and count the second",
			gaps:  []engine.EventGap{lost(big, 2*big), lost(3*big, 4*big)},
			names: []string{"1000000000000000000–2000000000000000000", "1 more range"}, omits: []string{"3000000000000000000"}},
		{name: "never published, then lost", gaps: []engine.EventGap{unpublished(1, 1), lost(1201, 2400)},
			names: []string{"1–1", "1201–2400"}},
		{name: "never published, then four lost",
			gaps:  []engine.EventGap{unpublished(1, 1), lost(1201, 2400), lost(3601, 4800), lost(6001, 7200), lost(8401, 9600)},
			names: []string{"1–1", "1201–2400", "3 more ranges"}, omits: []string{"3601"}},
		{name: "lost, then never published",
			gaps:  []engine.EventGap{lost(1201, 2400), lost(3601, 4800), unpublished(5000, 5000)},
			names: []string{"1201–2400", "5000–5000"}},
		{name: "both causes in ranges too long to name",
			gaps:  []engine.EventGap{unpublished(1, big-1), lost(big, 2*big-1)},
			omits: []string{"1000000000000000000"}},
		{name: "no gaps at all still closes with a sentence", gaps: nil},
	}
}()

// TestStreamGapError_CloseReasonFitsTheFrameAndNamesEveryCause: the reason a
// socket closes with at a gap holds the properties above for each list in
// closeReasonCases, and names the ranges the frame can hold, the closing
// clause only while it fits.
func TestStreamGapError_CloseReasonFitsTheFrameAndNamesEveryCause(t *testing.T) {
	for _, tc := range closeReasonCases {
		t.Run(tc.name, func(t *testing.T) {
			got := requireCloseReasonProperties(t, tc.gaps)
			for _, fragment := range tc.names {
				if !strings.Contains(got, fragment) {
					t.Errorf("close reason %q does not hold %q", got, fragment)
				}
			}
			for _, fragment := range tc.omits {
				if strings.Contains(got, fragment) {
					t.Errorf("close reason %q holds %q, which the frame cannot carry beside what it names", got, fragment)
				}
			}
			if tc.size != 0 && len(got) != tc.size {
				t.Errorf("close reason of %d bytes, want %d", len(got), tc.size)
			}
		})
	}
}

// gapRecord is the bytes of one gap in a fuzz input: its kind, then its first
// number and the distance to its last, each shifted two bits down on reading
// so the sum fits an int64 for any bytes.
const gapRecord = 1 + 8 + 8

func encodeGaps(gaps []engine.EventGap) []byte {
	var data []byte
	for _, gap := range gaps {
		record := make([]byte, gapRecord)
		if gap.Unpublished() {
			record[0] = 1
		}
		binary.BigEndian.PutUint64(record[1:], uint64(gap.First)<<2)
		binary.BigEndian.PutUint64(record[9:], uint64(gap.Last-gap.First)<<2)
		data = append(data, record...)
	}
	return data
}

func decodeGaps(data []byte) []engine.EventGap {
	var gaps []engine.EventGap
	for ; len(data) >= gapRecord; data = data[gapRecord:] {
		first := int64(binary.BigEndian.Uint64(data[1:]) >> 2)
		last := first + int64(binary.BigEndian.Uint64(data[9:])>>2)
		reason := "segment object missing"
		if data[0]&1 == 1 {
			reason = engine.UnpublishedEventsReason
		}
		gaps = append(gaps, engine.EventGap{First: first, Last: last, Reason: reason})
	}
	return gaps
}

// FuzzCloseReason: for any list of gaps, of any number, size and mix of
// causes, the close reason holds the properties of
// requireCloseReasonProperties.
func FuzzCloseReason(f *testing.F) {
	for _, tc := range closeReasonCases {
		f.Add(encodeGaps(tc.gaps))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		requireCloseReasonProperties(t, decodeGaps(data))
	})
}
