package main

import (
	"os"
	"syscall"
	"testing"
)

// TestSignals_SecondChangesNothingThirdExits: after the first stop signal, a
// second only says so, and a third runs the exit; nothing runs twice.
func TestSignals_SecondChangesNothingThirdExits(t *testing.T) {
	signals := make(chan os.Signal, 4)
	var seconds, thirds int
	signals <- syscall.SIGTERM
	signals <- syscall.SIGINT
	signals <- syscall.SIGTERM
	close(signals)
	countStopSignals(signals, func() { seconds++ }, func() { thirds++ })
	if seconds != 1 || thirds != 1 {
		t.Fatalf("second ran %d times and third %d, want 1 and 1", seconds, thirds)
	}

	signals = make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	close(signals)
	seconds, thirds = 0, 0
	countStopSignals(signals, func() { seconds++ }, func() { thirds++ })
	if seconds != 1 || thirds != 0 {
		t.Fatalf("one later signal ran second %d and third %d times, want 1 and 0", seconds, thirds)
	}
}
