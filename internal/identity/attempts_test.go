package identity

import (
	"testing"
	"time"
)

// TestSlots_Formula: identity_slots is max(1, retry.maximum_attempts) plus
// extra, never below the retry policy, and over the deployment's most is
// refused with both numbers.
func TestSlots_Formula(t *testing.T) {
	for _, c := range []struct {
		name               string
		maxAttempts, extra int
		limit              int
		want               int
		over               bool
	}{
		{"no retry", 0, 0, 32, 1, false},
		{"retry 1", 1, 0, 32, 1, false},
		{"retry 3", 3, 0, 32, 3, false},
		{"retry 3, extra 2", 3, 2, 32, 5, false},
		{"no retry, extra 4", 0, 4, 32, 5, false},
		{"at the most", 30, 2, 32, 32, false},
		{"one over", 31, 2, 32, 33, true},
		{"a thousand retries", 1000, 0, 256, 1000, true},
	} {
		got, err := Slots(c.maxAttempts, c.extra, c.limit)
		if got != c.want || (err != nil) != c.over {
			t.Errorf("%s: Slots(%d, %d, %d) = %d, %v", c.name, c.maxAttempts, c.extra, c.limit, got, err)
		}
	}
}

// TestSlots_ExpiredInjectedClock: a slot outlives its attempt once more
// than attempt_deadline_seconds plus the skew have passed since it was
// taken, by the engine's clock.
func TestSlots_ExpiredInjectedClock(t *testing.T) {
	taken := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	deadline, skew := 300*time.Second, time.Minute
	for _, c := range []struct {
		after time.Duration
		want  bool
	}{
		{0, false},
		{deadline, false},
		{deadline + skew, false},
		{deadline + skew + time.Nanosecond, true},
		{-time.Hour, false},
	} {
		if got := SlotExpired(taken, deadline, skew, taken.Add(c.after)); got != c.want {
			t.Errorf("after %v: expired = %v", c.after, got)
		}
	}
}

// TestMint_TTLClamp: an identity's ttl is held between the deployment's
// min_ttl and max_ttl, and an unset one is the deployment's ttl.
func TestMint_TTLClamp(t *testing.T) {
	b := TTLBounds{Default: time.Hour, Min: 10 * time.Minute, Max: 12 * time.Hour}
	for _, c := range []struct {
		asked, want time.Duration
	}{
		{0, time.Hour},
		{time.Minute, 10 * time.Minute},
		{2 * time.Hour, 2 * time.Hour},
		{48 * time.Hour, 12 * time.Hour},
		{-time.Hour, 10 * time.Minute},
	} {
		if got := b.Clamp(c.asked); got != c.want {
			t.Errorf("Clamp(%v) = %v, want %v", c.asked, got, c.want)
		}
	}
}
