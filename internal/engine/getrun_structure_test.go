package engine_test

import (
	"fmt"
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
	// History is the most events the read could have scanned: the length of
	// the generation the run was in when the read ended, plus the length the
	// generation before it ended at when the run switched during the read.
	History int64 `json:"generation_history_events"`
}

// The bounds on what one live GetRun reads. They are counts, so they hold on
// any host whatever its speed, and a miss of one fails the test on every
// runner: they are not timing checks (docs/PROOFS.md §12).
//
// CALIBRATION (2026-10-27, the 16-CPU reference host, with other work holding
// its load average between 40 and 50, so the wall-clock bounds missed and
// these held): 60 live GetRuns, ten to a soak run: the fan-out test twice (two
// runs of 4,000 items each time, overlays off), the item-ceiling test once
// (4,000 items, definition by reference, overlays on) and the task-ceiling
// test once (1,000 tasks, overlays on), most of them in the second
// generation of their run.
//
//   - History events read, as a share of the events the generation held when
//     the read ended: 0.022 to 1.000. The read scans the current generation
//     once and a history only grows, so the share cannot pass 1; the low
//     shares are reads begun in the first seconds of a generation, which grew
//     past them while they ran. A read that scanned the history twice would
//     show 2. The bound is the generation's length plus
//     soakGetRunEventsMarginPercent (10%): a margin for what a Describe a
//     moment later may count, and far below any second pass over the history.
//   - Saved-state objects read: 1 (a full snapshot) or 2 (an overlay and the
//     full snapshot it is written against); never more than 2 in the 60. The
//     bound, soakGetRunSnapshotReadsMax, is 4: twice the most observed. It
//     holds the reader's one bounded fallback (the newest snapshot missing:
//     that read, then the previous snapshot's overlay and its base, 3) and
//     fails a reader that walks the run's snapshots (30 objects at these
//     sizes).
const (
	soakGetRunEventsMarginPercent = 10
	soakGetRunSnapshotReadsMax    = 4
)

// getRunStructuralMisses are the ways the live GetRuns read more than a
// reader that does its one job reads: the current generation's history once,
// and the run's saved state in a bounded number of objects. A read of no
// events at all is a miss too: the counting reader saw nothing, and a bound
// over nothing proves nothing.
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
		if r.SnapshotReads > soakGetRunSnapshotReadsMax {
			misses = append(misses, fmt.Sprintf("live GetRun %d read %d saved-state objects, more than the %d it needs", i+1, r.SnapshotReads, soakGetRunSnapshotReadsMax))
		}
	}
	return misses
}

func TestGetRunStructuralMisses_AreCountsThatHoldOnAnyHost(t *testing.T) {
	ok := getRunRead{Events: 9000, History: 9100, SnapshotReads: 2}
	cases := []struct {
		name  string
		reads []getRunRead
		want  []string // a substring of each miss, in order
	}{
		{"every read within the generation", []getRunRead{ok, ok}, nil},
		{"a read of exactly the generation", []getRunRead{{Events: 9100, History: 9100, SnapshotReads: 1}}, nil},
		{"a read a little over the generation, within the margin", []getRunRead{{Events: 9101, History: 9100, SnapshotReads: 2}}, nil},
		{"a read at the margin", []getRunRead{{Events: 10010, History: 9100, SnapshotReads: 2}}, nil},
		{"a read over the margin", []getRunRead{ok, {Events: 10011, History: 9100, SnapshotReads: 2}},
			[]string{"live GetRun 2 read 10011 history events, and the generation it read holds 9100"}},
		{"a read that scanned the history twice", []getRunRead{{Events: 18200, History: 9100, SnapshotReads: 2}},
			[]string{"read more than the history once"}},
		{"a counting reader that saw nothing", []getRunRead{{Events: 0, History: 9100, SnapshotReads: 2}},
			[]string{"read no history events"}},
		{"too many saved-state objects", []getRunRead{{Events: 9000, History: 9100, SnapshotReads: soakGetRunSnapshotReadsMax + 1}},
			[]string{fmt.Sprintf("read %d saved-state objects", soakGetRunSnapshotReadsMax+1)}},
		{"exactly the saved-state objects it may read", []getRunRead{{Events: 9000, History: 9100, SnapshotReads: soakGetRunSnapshotReadsMax}}, nil},
		{"both bounds in one read", []getRunRead{{Events: 10011, History: 9100, SnapshotReads: soakGetRunSnapshotReadsMax + 1}},
			[]string{"history events", "saved-state objects"}},
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
