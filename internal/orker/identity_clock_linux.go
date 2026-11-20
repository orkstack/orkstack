//go:build linux

package orker

import (
	"time"

	"golang.org/x/sys/unix"
)

// bootClock is CLOCK_BOOTTIME: time since boot, suspended time included.
func bootClock() time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		// The kernel has had CLOCK_BOOTTIME since 2.6.39; a failure here is
		// not one a running Orker can meet. The monotonic clock is the
		// nearest the process has, and the watchdog's wall-clock half still
		// catches a pause.
		return monotonicSinceStart()
	}
	return time.Duration(ts.Nano())
}
