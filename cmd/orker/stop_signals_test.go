package main

import (
	"os"
	"syscall"
	"testing"
	"time"
)

// spaced is n arrivals a second and a half apart after start, so every
// signal is its own.
func spaced(start time.Time, n int) chan time.Time {
	stamps := make(chan time.Time, n)
	for i := 1; i <= n; i++ {
		stamps <- start.Add(time.Duration(i) * 1500 * time.Millisecond)
	}
	close(stamps)
	return stamps
}

// TestSignals_SecondEndsTheGraceThirdExits: after the first stop signal, a
// second runs the grace's end, and a third runs the exit; nothing runs twice.
func TestSignals_SecondEndsTheGraceThirdExits(t *testing.T) {
	start := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	var seconds, thirds int
	countStopSignals(spaced(start, 3), start, nil, func() { seconds++ }, func() { thirds++ })
	if seconds != 1 || thirds != 1 {
		t.Fatalf("second ran %d times and third %d, want 1 and 1", seconds, thirds)
	}

	seconds, thirds = 0, 0
	countStopSignals(spaced(start, 1), start, nil, func() { seconds++ }, func() { thirds++ })
	if seconds != 1 || thirds != 0 {
		t.Fatalf("one later signal ran second %d and third %d times, want 1 and 0", seconds, thirds)
	}
}

// TestSignals_OnesWithinASecondAreOneStop: a process manager that signals the
// process and its group, or sends two signals together, means one stop. The
// debounce is measured at arrival, so signals that arrived together still
// count as one however late they are read.
func TestSignals_OnesWithinASecondAreOneStop(t *testing.T) {
	start := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	stamps := make(chan time.Time, 4)
	for _, offset := range []time.Duration{100 * time.Millisecond, 900 * time.Millisecond, 2 * time.Second, 2500 * time.Millisecond} {
		stamps <- start.Add(offset)
	}
	close(stamps)
	var seconds, thirds, merged int
	countStopSignals(stamps, start, func() { merged++ }, func() { seconds++ }, func() { thirds++ })
	// 0.1 s and 0.9 s are the first signal's; 2 s is the second; 2.5 s is
	// within a second of it.
	if seconds != 1 || thirds != 0 || merged != 3 {
		t.Fatalf("second ran %d times, third %d, merged %d; want 1, 0 and 3", seconds, thirds, merged)
	}
}

// TestStampSignals_StampsAtArrival: a signal is stamped when it arrives, not
// when it is counted.
func TestStampSignals_StampsAtArrival(t *testing.T) {
	signals := make(chan os.Signal, 2)
	at := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	stamps := stampSignals(signals, func() time.Time { return at })
	signals <- syscall.SIGTERM
	if got := <-stamps; !got.Equal(at) {
		t.Fatalf("stamp = %v; want the arrival %v", got, at)
	}
	close(signals)
	if _, open := <-stamps; open {
		t.Fatal("the stamps did not end with the signals")
	}
}
