// Package bootwait is how an integration test waits for a process it started
// to become ready.
//
// A boot has a long tail. An engine reads the whole namespace registry and
// Orkflow store of its bucket before it serves, and at a higher PROOF_JOBS one
// boot in several hundred was measured stalling 50 s on its fresh bucket's
// first read while the object store grew and removed per-bucket volumes. A
// merge gate is never rerun, so a wait that gives up at 60 s turns that tail
// into a failed proof, and a wait that absorbs it silently hides a store that
// is slowly getting worse. Until therefore does three explicit things:
//
//   - it tolerates a slow boot up to Deadline (180 s), well past the measured
//     stall;
//   - it ends at once, with its own error, when the process has exited, instead
//     of polling a corpse until the deadline;
//   - it records a boot over WarnAfter (20 s) as a WARN line that names the
//     duration, which `proofrun overhead` lists under "Slow boots".
//
// A deadline, a timer and a select stand where a bare sleep would: nothing here
// waits a fixed time and hopes (AGENTS.md §11.1).
package bootwait

import (
	"context"
	"fmt"
	"time"
)

const (
	// Deadline is how long a boot may take before the wait fails.
	Deadline = 180 * time.Second
	// WarnAfter is how long a boot may take before the wait that saw it logs
	// a WARN line. A slow boot is recorded, never absorbed.
	WarnAfter = 20 * time.Second
	// Interval is the pause between two probes.
	Interval = 250 * time.Millisecond
)

// Wait describes one process a test is waiting for. A zero duration means the
// package default of the same name.
type Wait struct {
	// What names the process in every message: "ork", "ork-A at 127.0.0.1:9".
	What string
	// Exited, when non-nil, is closed when the process exits. Until stops at
	// once when it is closed and the process has not answered.
	Exited <-chan struct{}
	// Logf receives the wait's log lines, normally t.Logf. Nil logs nothing.
	Logf func(format string, args ...any)

	Deadline, WarnAfter, Interval time.Duration
}

// Until calls probe at once and then every Interval until it returns nil, and
// reports how long the process took to answer.
//
// It fails when Exited closes ("<What> exited before it became ready"), when
// the deadline passes ("<What> did not become ready within <deadline>") or when
// ctx ends (the error wraps ctx.Err()). The first two name the last probe's
// error; the probe's own context is bounded by the deadline, so a probe that
// hangs cannot outlast it. A probe that answers wins over an exit seen in the
// same instant, and a closed Exited is not probed again.
func (w Wait) Until(ctx context.Context, probe func(ctx context.Context) error) (time.Duration, error) {
	deadline, warnAfter, interval := w.Deadline, w.WarnAfter, w.Interval
	if deadline <= 0 {
		deadline = Deadline
	}
	if warnAfter <= 0 {
		warnAfter = WarnAfter
	}
	if interval <= 0 {
		interval = Interval
	}

	start := time.Now()
	bounded, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	var last error
	for {
		last = probe(bounded)
		if last == nil {
			took := time.Since(start)
			w.logf("%s became ready after %s", w.What, took.Round(time.Millisecond))
			if took > warnAfter {
				w.logf("WARN: %s took %s to become ready (over the %s warning threshold); a slow boot is recorded here, not absorbed",
					w.What, took.Round(time.Millisecond), warnAfter)
			}
			return took, nil
		}
		select {
		case <-w.Exited:
			return time.Since(start), fmt.Errorf("%s exited before it became ready, after %s (last probe: %w)",
				w.What, time.Since(start).Round(time.Millisecond), last)
		default:
		}

		pause := time.NewTimer(interval)
		select {
		case <-pause.C:
		case <-w.Exited:
			pause.Stop()
			return time.Since(start), fmt.Errorf("%s exited before it became ready, after %s (last probe: %w)",
				w.What, time.Since(start).Round(time.Millisecond), last)
		case <-bounded.Done():
			pause.Stop()
			if err := ctx.Err(); err != nil {
				return time.Since(start), fmt.Errorf("waiting for %s: %w (last probe: %v)", w.What, err, last)
			}
			return time.Since(start), fmt.Errorf("%s did not become ready within %s (last probe: %w)", w.What, deadline, last)
		}
	}
}

func (w Wait) logf(format string, args ...any) {
	if w.Logf != nil {
		w.Logf(format, args...)
	}
}
