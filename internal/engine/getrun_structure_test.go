package engine_test

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// getRunRead is one live GetRun: its latency, what it read, and how much
// history the generation it read held.
type getRunRead struct {
	Seconds       float64 `json:"seconds"`
	Events        int64   `json:"history_events_read"`
	EventBytes    int64   `json:"history_event_bytes_read"`
	SnapshotReads int64   `json:"snapshot_reads"`
	SnapshotBytes int64   `json:"snapshot_bytes_read"`
	// History and HistoryBytes are the most the read could have scanned: the
	// events, and the bytes the cluster reports for them, of the generation
	// the run was in when the read ended, plus those of the generation before
	// it when the run switched during the read.
	History      int64 `json:"generation_history_events"`
	HistoryBytes int64 `json:"generation_history_bytes"`
	// MsPer1000Events is the read's wall-clock cost per thousand events it
	// scanned: evidence of where a slowdown is, never a bound (it is a
	// wall-clock number, and the reference host alone enforces those).
	MsPer1000Events float64 `json:"ms_per_1000_events"`
}

// costPerThousandEvents is a read's milliseconds per thousand events read.
func costPerThousandEvents(seconds float64, events int64) float64 {
	if events <= 0 {
		return 0
	}
	return math.Round(seconds*1000/(float64(events)/1000)*10) / 10
}

// The bounds on what one live GetRun reads. They are counts and sizes, so they
// hold on any host whatever its speed, and a miss of one fails the test on
// every runner: they are not timing checks (docs/PROOFS.md §12).
//
// CALIBRATION (2026-10-27, the 16-CPU reference host, with other work holding
// its load average between 20 and 50, so the wall-clock bounds sometimes
// missed and these always held): 160 live GetRuns, ten to a soak run, in
// sixteen soak runs of the three merge-gate tests: the fan-out test five times
// (two soak runs of 4,000 items each time, overlays off), the item-ceiling
// test three times (4,000 items, definition by reference, overlays on) and the
// task-ceiling test three times (1,000 tasks, overlays on), most of the reads
// in the second generation of their run. The history bytes were added after
// the first 120 reads and are calibrated on the last 40 (one run of each
// test).
//
//   - History events read, as a share of the events the generation held when
//     the read ended: 0.022 to 1.000. The read scans the current generation
//     once and a history only grows, so the share cannot pass 1; the low
//     shares are reads begun in the first seconds of a generation, which grew
//     past them while they ran. A read that scanned the history twice would
//     show 2. The bound is the generation's length plus
//     soakGetRunEventsMarginPercent (10%): a margin for what a Describe a
//     moment later may count, and far below any second pass over the history.
//   - History bytes read (the decoded size of the events handed out), as a
//     share of the bytes the cluster reports for the generation: 0.631 to
//     0.999. The counted size is the cluster's less its batch framing (about
//     0.4%), so a complete read is 0.995 or more. The bound is the same
//     one pass in bytes, with the same margin. It catches a reader that
//     fetches an event twice or loads more than the history, and it bounds
//     the decode work of a GetRun by the generation's size, which the soaks
//     hold under 80% of the cluster's limit.
//   - Saved-state objects read: 1 (a full snapshot) or 2 (an overlay and the
//     full snapshot it is written against); never more than 2 in the 160. The
//     bound, soakGetRunSnapshotReadsMax, is 4: twice the most observed. It
//     holds the reader's one bounded fallback (the newest snapshot missing:
//     that read, then the previous snapshot's overlay and its base, 3) and
//     fails a reader that walks the run's snapshots (30 objects at these
//     sizes). The bytes of those objects are recorded, not bounded: they
//     are the run's saved state, which grows with its items by design (0.29
//     to 3.6 MB at 4,000 items) and is bounded where it is written.
//
// What none of this bounds is the cost of a read per event or per byte: a
// decode that got twice as slow shows only in the wall-clock numbers
// (MsPer1000Events is that evidence), which the reference host enforces and a
// smaller CI runner records.
const (
	soakGetRunEventsMarginPercent = 10
	soakGetRunSnapshotReadsMax    = 4
)

// getRunStructuralMisses are the ways the live GetRuns read more than a
// reader that does its one job reads: the current generation's history once,
// in events and in bytes, and the run's saved state in a bounded number of
// objects. A read of no events at all is a miss too: the counting reader saw
// nothing, and a bound over nothing proves nothing; so is a generation the
// cluster reports no size for.
func getRunStructuralMisses(reads []getRunRead) []string {
	var misses []string
	for i, r := range reads {
		switch {
		case r.Events == 0:
			misses = append(misses, fmt.Sprintf("live GetRun %d read no history events: the counting reader saw nothing, and the structural bounds prove nothing", i+1))
		case r.Events*100 > r.History*(100+soakGetRunEventsMarginPercent):
			misses = append(misses, fmt.Sprintf("live GetRun %d read %d history events, and the generation it read holds %d (it may be read %d%% over, no more): it read more than the history once",
				i+1, r.Events, r.History, soakGetRunEventsMarginPercent))
		}
		switch {
		case r.Events == 0:
		case r.HistoryBytes <= 0:
			misses = append(misses, fmt.Sprintf("live GetRun %d: the cluster reported no history size for the generation it read, and the bytes bound proves nothing", i+1))
		case r.EventBytes*100 > r.HistoryBytes*(100+soakGetRunEventsMarginPercent):
			misses = append(misses, fmt.Sprintf("live GetRun %d read %d bytes of history, and the generation it read holds %d (it may be read %d%% over, no more): it read more than the history once",
				i+1, r.EventBytes, r.HistoryBytes, soakGetRunEventsMarginPercent))
		}
		if r.SnapshotReads > soakGetRunSnapshotReadsMax {
			misses = append(misses, fmt.Sprintf("live GetRun %d read %d saved-state objects, more than the %d it needs", i+1, r.SnapshotReads, soakGetRunSnapshotReadsMax))
		}
	}
	return misses
}

func TestGetRunStructuralMisses_AreCountsThatHoldOnAnyHost(t *testing.T) {
	// A read of events events (of the history events the generation held) that
	// decoded bytes (of the historyBytes it held) and loaded snaps objects.
	read := func(events, history, bytes, historyBytes, snaps int64) getRunRead {
		return getRunRead{Events: events, History: history, EventBytes: bytes, HistoryBytes: historyBytes, SnapshotReads: snaps}
	}
	ok := read(9000, 9100, 6_500_000, 6_700_000, 2)
	cases := []struct {
		name  string
		reads []getRunRead
		want  []string // a substring of each miss, in order
	}{
		{"every read within the generation", []getRunRead{ok, ok}, nil},
		{"a read of exactly the generation", []getRunRead{read(9100, 9100, 6_700_000, 6_700_000, 1)}, nil},
		{"a read a little over the generation, within the margin", []getRunRead{read(9101, 9100, 6_700_500, 6_700_000, 2)}, nil},
		{"a read at the margin", []getRunRead{read(10010, 9100, 7_370_000, 6_700_000, 2)}, nil},
		{"a read over the margin", []getRunRead{ok, read(10011, 9100, 6_700_000, 6_700_000, 2)},
			[]string{"live GetRun 2 read 10011 history events, and the generation it read holds 9100"}},
		{"a read that scanned the history twice", []getRunRead{read(18200, 9100, 13_400_000, 6_700_000, 2)},
			[]string{"read more than the history once", "read 13400000 bytes of history"}},
		{"a counting reader that saw nothing", []getRunRead{read(0, 9100, 0, 6_700_000, 2)},
			[]string{"read no history events"}},
		{"events of the right number and too many bytes", []getRunRead{read(9000, 9100, 7_370_001, 6_700_000, 2)},
			[]string{"live GetRun 1 read 7370001 bytes of history, and the generation it read holds 6700000"}},
		{"bytes at the margin", []getRunRead{read(9000, 9100, 7_370_000, 6_700_000, 2)}, nil},
		{"a cluster that reported no history size", []getRunRead{read(9000, 9100, 6_500_000, 0, 2)},
			[]string{"reported no history size"}},
		{"too many saved-state objects", []getRunRead{{Events: 9000, History: 9100, EventBytes: 6_500_000, HistoryBytes: 6_700_000, SnapshotReads: soakGetRunSnapshotReadsMax + 1}},
			[]string{fmt.Sprintf("read %d saved-state objects", soakGetRunSnapshotReadsMax+1)}},
		{"exactly the saved-state objects it may read", []getRunRead{read(9000, 9100, 6_500_000, 6_700_000, soakGetRunSnapshotReadsMax)}, nil},
		{"every bound in one read", []getRunRead{read(10011, 9100, 7_370_001, 6_700_000, soakGetRunSnapshotReadsMax+1)},
			[]string{"history events", "bytes of history", "saved-state objects"}},
		{"no read at all", nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := getRunStructuralMisses(c.reads)
			if len(got) != len(c.want) {
				t.Fatalf("misses = %q, want %d", got, len(c.want))
			}
			for i, want := range c.want {
				if !strings.Contains(got[i], want) {
					t.Errorf("miss %d = %q, want it to contain %q", i, got[i], want)
				}
			}
		})
	}
}

// The cost of a read per thousand events is evidence, not a bound: it is the
// number a slowdown shows in, and a reader of the evidence compares runs by it.
func TestGetRunRead_RecordsItsCostPerThousandEvents(t *testing.T) {
	for _, c := range []struct {
		seconds float64
		events  int64
		want    float64
	}{
		{1.5, 15000, 100},
		{0.25, 500, 500},
		{2, 0, 0},
	} {
		if got := costPerThousandEvents(c.seconds, c.events); got != c.want {
			t.Errorf("costPerThousandEvents(%g s, %d events) = %g, want %g", c.seconds, c.events, got, c.want)
		}
	}
}
