package orker

import (
	"context"
	"time"
)

// systemIdentityClock is the host's clocks: the wall clock, and a boot
// clock that keeps running while the host is suspended.
type systemIdentityClock struct{}

func (systemIdentityClock) Now() time.Time      { return time.Now() }
func (systemIdentityClock) Boot() time.Duration { return bootClock() }

// Sleep waits d or until ctx ends. A Go timer stops with the host, so the
// watchdog never relies on one alone: it ticks and re-reads both clocks.
func (systemIdentityClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// processStart anchors the monotonic stand-in for the boot clock.
var processStart = time.Now()

func monotonicSinceStart() time.Duration { return time.Since(processStart) }
