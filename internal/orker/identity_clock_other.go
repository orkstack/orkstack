//go:build !linux

package orker

import "time"

// bootClock: off Linux the process's monotonic clock stands in for the boot
// clock. It stops while the host sleeps, and the watchdog's wall-clock half
// catches that.
func bootClock() time.Duration { return monotonicSinceStart() }
