package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// continuationConfig is engine.continuation.*: whether a run carries on into
// a fresh history past its budget, the budget, and how long its next history
// waits for its saved state (docs/DEPLOY.md §1). Pinned into every new
// top-level plan at submit.
type continuationConfig struct {
	enabled        bool
	events, bytes  int64
	restoreTimeout time.Duration
}

// The bounds continuationFromEnv refuses outside, naming the variable.
const (
	minContinuationEvents         int64 = 50
	minContinuationBytes          int64 = 16 << 10
	minContinuationRestoreTimeout       = time.Minute
	maxContinuationRestoreTimeout       = 8 * time.Hour
	minRunMaxInFlight             int32 = 16
	maxRunMaxInFlight             int32 = 1800
)

// continuationFromEnv reads ORK_CONTINUATION_ENABLED, _EVENTS, _BYTES and
// _RESTORE_TIMEOUT. The three numbers are validated only when continuation
// is enabled, so a deployment or a test that tunes engine.history.* low with
// continuation off is never refused over keys it does not use:
//
//   - 50 ≤ events ≤ history.max_events/4;
//   - 16 KiB ≤ bytes ≤ history.max_bytes/5 — bytes also bounds the saved
//     state a generation reads back, so its base, the 60% hold line and the
//     reserve fit under the limit;
//   - 1m ≤ restore_timeout ≤ 8h;
//   - the bootstrap's worst-case history (engine.RestoreHistoryBound of the
//     budget every run would carry) ≤ history.max_events/4, so a
//     generation's bootstrap plus its budget stay under half the limit,
//     below the hold line.
func continuationFromEnv(historyMaxEvents, historyMaxBytes int64) (continuationConfig, error) {
	cfg := continuationConfig{
		enabled:        engine.DefaultContinuationEnabled,
		events:         engine.DefaultContinuationEvents,
		bytes:          engine.DefaultContinuationBytes,
		restoreTimeout: engine.DefaultContinuationRestoreTimeout,
	}
	var errs []error
	if raw := env("ORK_CONTINUATION_ENABLED", ""); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("ORK_CONTINUATION_ENABLED=%q must be true or false: %w", raw, err))
		} else {
			cfg.enabled = enabled
		}
	}
	events, err := envInt64("ORK_CONTINUATION_EVENTS", engine.DefaultContinuationEvents)
	errs = append(errs, err)
	bytes, err := envInt64("ORK_CONTINUATION_BYTES", engine.DefaultContinuationBytes)
	errs = append(errs, err)
	timeout, err := envPositiveDuration("ORK_CONTINUATION_RESTORE_TIMEOUT", engine.DefaultContinuationRestoreTimeout)
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return cfg, err
	}
	cfg.events, cfg.bytes, cfg.restoreTimeout = events, bytes, timeout
	if !cfg.enabled {
		return cfg, nil
	}
	errs = nil
	if events < minContinuationEvents || events > historyMaxEvents/4 {
		errs = append(errs, fmt.Errorf("ORK_CONTINUATION_EVENTS=%d must be between %d and a quarter of "+
			"ORK_HISTORY_MAX_EVENTS (%d)", events, minContinuationEvents, historyMaxEvents/4))
	}
	if bytes < minContinuationBytes || bytes > historyMaxBytes/5 {
		errs = append(errs, fmt.Errorf("ORK_CONTINUATION_BYTES=%d must be between %d (16 KiB) and a fifth of "+
			"ORK_HISTORY_MAX_BYTES (%d)", bytes, minContinuationBytes, historyMaxBytes/5))
	}
	if timeout < minContinuationRestoreTimeout || timeout > maxContinuationRestoreTimeout {
		errs = append(errs, fmt.Errorf("ORK_CONTINUATION_RESTORE_TIMEOUT=%s must be between %s and %s",
			timeout, minContinuationRestoreTimeout, maxContinuationRestoreTimeout))
	}
	if len(errs) == 0 {
		if bound := engine.RestoreHistoryBound(engine.DefaultContinuationBudget(events, bytes, timeout)); bound > historyMaxEvents/4 {
			errs = append(errs, fmt.Errorf("engine.continuation.restore_timeout (ORK_CONTINUATION_RESTORE_TIMEOUT=%s) "+
				"with engine.continuation.bytes (ORK_CONTINUATION_BYTES=%d) lets a run's start from its saved state "+
				"append up to %d history events, more than a quarter of engine.history.max_events "+
				"(ORK_HISTORY_MAX_EVENTS=%d): shorten the timeout, lower the bytes, or raise the limit",
				timeout, bytes, bound, historyMaxEvents))
		}
	}
	return cfg, errors.Join(errs...)
}

// inFlightCapFromEnv reads ORK_RUN_MAX_IN_FLIGHT: the most activities, child
// runs and pins one run keeps pending. Always validated, 16 to 1,800: below
// 16 a run would serialise on its own bookkeeping, and 1,800 leaves 200
// below the cluster's pending-activity limit for publishes, pins and pin
// exemptions.
func inFlightCapFromEnv() (int32, error) {
	value, err := envInt32("ORK_RUN_MAX_IN_FLIGHT", engine.DefaultRunMaxInFlight)
	if err != nil {
		return engine.DefaultRunMaxInFlight, err
	}
	if value < minRunMaxInFlight || value > maxRunMaxInFlight {
		return engine.DefaultRunMaxInFlight, fmt.Errorf("ORK_RUN_MAX_IN_FLIGHT=%d must be between %d and %d",
			value, minRunMaxInFlight, maxRunMaxInFlight)
	}
	return value, nil
}
