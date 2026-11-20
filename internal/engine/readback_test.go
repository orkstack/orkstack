package engine

import (
	"testing"
	"time"
)

func TestPackReadback_ClosesACallAtItsBudgetOrItsEntryCap(t *testing.T) {
	const kib = 1024
	tun := defaultReadbackTunables()
	for _, tc := range []struct {
		name  string
		sizes []int64
		want  [][2]int
	}{
		{"nothing to read", nil, nil},
		{"one call", []int64{10, 20, 30}, [][2]int{{0, 3}}},
		// readback-single-max-entry: an entry of 127 KiB fits alone.
		{"a single maximal entry", []int64{127 * kib}, [][2]int{{0, 1}}},
		{"an entry that would overflow opens a call", []int64{2 * kib, 127 * kib, 5}, [][2]int{{0, 1}, {1, 3}}},
		{"the 129th entry opens a call", make([]int64, 130), [][2]int{{0, 128}, {128, 130}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := packReadback(tc.sizes, tun)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("calls = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("calls = %v, want %v", got, tc.want)
				}
			}
			// Every call fits its budget, framing counted.
			for _, c := range got {
				var sum int64
				for _, s := range tc.sizes[c[0]:c[1]] {
					sum += s
				}
				if sum+readbackFraming(c[1]-c[0]) > tun.Budget {
					t.Fatalf("call %v holds %d bytes with framing, over %d", c, sum+readbackFraming(c[1]-c[0]), tun.Budget)
				}
			}
		})
	}
}

func TestPackReadback_RefusesAnEntryNoCallCanHold(t *testing.T) {
	tun := defaultReadbackTunables()
	if _, err := packReadback([]int64{tun.Budget - readbackFraming(1) + 1}, tun); err == nil {
		t.Fatal("an entry over the budget was packed")
	}
	if _, err := packReadback([]int64{-1}, tun); err == nil {
		t.Fatal("a negative size was packed")
	}
}

func TestReadbackFraming_IsAtMostOneKiBFor128Entries(t *testing.T) {
	if f := readbackFraming(128); f > 1024 {
		t.Fatalf("framing for 128 entries = %d bytes, over 1 KiB", f)
	}
}

func TestReadbackCallTimeouts(t *testing.T) {
	tun := defaultReadbackTunables()
	start := time.Unix(1_800_000_000, 0)
	deadline := start.Add(tun.Deadline)
	for _, tc := range []struct {
		name     string
		wft      time.Duration
		now      time.Time
		maxRetry time.Duration
		left     time.Duration
		ok       bool
	}{
		{"the default task timeout keeps the 5 s ceiling", 10 * time.Second, start, 5 * time.Second, tun.Deadline, true},
		{"a short task timeout halves it", 4 * time.Second, start, 2 * time.Second, tun.Deadline, true},
		// A call re-executed after a lost task recomputes from the same
		// recorded deadline, so the phase never extends.
		{"a re-executed call gets what is left", 10 * time.Second, start.Add(7 * time.Minute), 5 * time.Second, 3 * time.Minute, true},
		{"past the deadline nothing is called", 10 * time.Second, deadline, 0, 0, false},
		{"no task timeout known", 0, start, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := readbackCallTimeouts(tun, tc.wft, tc.now, deadline)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (%+v)", ok, tc.ok, got)
			}
			if !ok {
				return
			}
			if got.maxInterval != tc.maxRetry || got.scheduleToClose != tc.left || got.startToClose != tun.CallTimeout {
				t.Fatalf("timeouts = %+v, want max retry %v, left %v", got, tc.maxRetry, tc.left)
			}
			if got.maxInterval >= tc.wft {
				t.Fatalf("the retry ceiling %v is not below the task timeout %v", got.maxInterval, tc.wft)
			}
		})
	}
}
