package orker

import (
	"testing"
	"time"
)

// TestIdentityRefreshAfter: the refresh point is between a third of the
// token's life and a quarter of it later, at the draw's point, and never
// earlier than a third (the masker's arithmetic counts on it).
func TestIdentityRefreshAfter(t *testing.T) {
	life := 12 * time.Minute
	for _, tc := range []struct {
		draw float64
		want time.Duration
	}{
		{0, 4 * time.Minute},
		{0.5, 4*time.Minute + 90*time.Second},
		{1, 7 * time.Minute},
		{-1, 4 * time.Minute},
		{2, 7 * time.Minute},
	} {
		if got := identityRefreshAfter(life, tc.draw); got != tc.want {
			t.Errorf("draw %v: %v, want %v", tc.draw, got, tc.want)
		}
	}
	if got := identityRefreshAfter(0, 0.5); got != 0 {
		t.Errorf("no life: %v", got)
	}
}

// TestIdentityRetryDelay: the n-th delay after a failure is between half of
// b and b, b being retry_min doubled n times and capped at 60 s, and never
// below retry_min: 5-10 s, 10-20, 20-40, 30-60, then 30-60.
func TestIdentityRetryDelay(t *testing.T) {
	min := 5 * time.Second
	for _, tc := range []struct {
		n      int
		lo, hi time.Duration
	}{
		{1, 5 * time.Second, 10 * time.Second},
		{2, 10 * time.Second, 20 * time.Second},
		{3, 20 * time.Second, 40 * time.Second},
		{4, 30 * time.Second, 60 * time.Second},
		{9, 30 * time.Second, 60 * time.Second},
		{64, 30 * time.Second, 60 * time.Second},
	} {
		if got := identityRetryDelay(tc.n, min, 0); got != tc.lo {
			t.Errorf("n=%d draw 0: %v, want %v", tc.n, got, tc.lo)
		}
		if got := identityRetryDelay(tc.n, min, 1); got != tc.hi {
			t.Errorf("n=%d draw 1: %v, want %v", tc.n, got, tc.hi)
		}
	}
	// A retry_min so large that half of the cap is below it is held at it.
	if got := identityRetryDelay(4, 50*time.Second, 0); got != 50*time.Second {
		t.Errorf("retry_min 50s: %v", got)
	}
	if got := identityRetryDelay(0, min, 0); got != 5*time.Second {
		t.Errorf("n=0 is the first retry's: %v", got)
	}
}

// TestIdentityExpired: the watchdog fires at the earlier of the boot clock
// since the request was sent and the wall clock against issued_at.
func TestIdentityExpired(t *testing.T) {
	issued := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	life := 10 * time.Minute
	for _, tc := range []struct {
		name      string
		sinceSent time.Duration
		wall      time.Time
		want      bool
	}{
		{"fresh on both", time.Minute, issued.Add(time.Minute), false},
		{"boot clock past its life", 10 * time.Minute, issued.Add(time.Minute), true},
		{"a paused host: the wall clock is past", time.Minute, issued.Add(10 * time.Minute), true},
		{"both just before", 10*time.Minute - time.Nanosecond, issued.Add(10*time.Minute - time.Nanosecond), false},
	} {
		if got := identityExpired(tc.sinceSent, tc.wall, issued, life); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
