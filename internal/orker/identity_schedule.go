package orker

import "time"

// identityRetryCap is the longest retry delay of the one retry rule
// (workload-identity.md §7, "Refresh").
const identityRetryCap = 60 * time.Second

// identityRefreshAfter is when, after a token was asked for, it is refreshed:
// between a third of its life and a quarter of its life later than that, at
// draw (0 to 1, clamped), so the items of a fan-out do not refresh together.
// Never earlier than a third: the masker's arithmetic counts on it.
func identityRefreshAfter(life time.Duration, draw float64) time.Duration {
	if life <= 0 {
		return 0
	}
	draw = min(max(draw, 0), 1)
	return life/3 + time.Duration(draw*float64(life/4))
}

// identityRetryDelay is the n-th delay after a failure (n from 1): drawn at
// draw (0 to 1, clamped) between half of b and b, where b is retryMin doubled
// n times and capped at a minute, and never below retryMin. With the default
// 5 s: 5 to 10 s, then 10 to 20, 20 to 40, 30 to 60. One rule for identity
// tokens and providers alike.
func identityRetryDelay(n int, retryMin time.Duration, draw float64) time.Duration {
	n = max(n, 1)
	b := identityRetryCap
	if n < 62 && retryMin < identityRetryCap>>n {
		b = retryMin << n
	}
	draw = min(max(draw, 0), 1)
	return max(b/2+time.Duration(draw*float64(b-b/2)), retryMin)
}

// identityExpired is the watchdog's rule: what is on disk has expired once
// either clock says so, the boot clock since the request was sent (which
// keeps running while a host is suspended) or the wall clock against the
// engine's issued_at (which catches a paused virtual machine or a frozen
// cgroup).
func identityExpired(sinceSent time.Duration, wall, issuedAt time.Time, life time.Duration) bool {
	return sinceSent >= life || !wall.Before(issuedAt.Add(life))
}
