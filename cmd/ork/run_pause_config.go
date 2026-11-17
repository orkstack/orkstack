package main

import (
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// minRunPauseTimeout is the shortest default pause: a pause any shorter
// would end before an operator could act on it.
const minRunPauseTimeout = time.Minute

// runPauseTimeoutsFromEnv reads ORK_RUN_PAUSE_TIMEOUT
// (engine.run_pause_timeout) and ORK_RUN_PAUSE_TIMEOUT_MAX
// (engine.run_pause_timeout_max): how long a pause lasts when its request
// names no timeout, and the longest it may last. Always validated: whole
// seconds, the maximum at most engine.DefaultRunPauseTimeoutMax (744h), the
// default from 1m to the maximum. Both are pinned, in seconds, into every
// run's plan (api.Service.pausePlan), so changing them moves only new runs.
func runPauseTimeoutsFromEnv() (timeout, maximum time.Duration, err error) {
	maximum, err = pauseDurationFromEnv("ORK_RUN_PAUSE_TIMEOUT_MAX", "engine.run_pause_timeout_max", engine.DefaultRunPauseTimeoutMax,
		minRunPauseTimeout, engine.DefaultRunPauseTimeoutMax)
	if err != nil {
		return engine.DefaultRunPauseTimeout, engine.DefaultRunPauseTimeoutMax, err
	}
	timeout, err = pauseDurationFromEnv("ORK_RUN_PAUSE_TIMEOUT", "engine.run_pause_timeout", engine.DefaultRunPauseTimeout,
		minRunPauseTimeout, maximum)
	if err != nil {
		return engine.DefaultRunPauseTimeout, engine.DefaultRunPauseTimeoutMax, err
	}
	return timeout, maximum, nil
}

// pauseDurationFromEnv reads one pause duration: whole seconds from low to
// high, fallback when unset. Every refusal names both the variable and its
// configuration-file key (configKey), so either way of setting it knows
// what to change.
func pauseDurationFromEnv(key, configKey string, fallback, low, high time.Duration) (time.Duration, error) {
	raw := env(key, "")
	if raw == "" {
		switch {
		case fallback > high:
			return 0, fmt.Errorf("%s is unset, and its default of %s is above %s: set %s (%s) to at most %s",
				key, fallback, high, key, configKey, high)
		case fallback < low:
			return 0, fmt.Errorf("%s is unset, and its default of %s is below %s: set %s (%s) to at least %s",
				key, fallback, low, key, configKey, low)
		}
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s=%q (%s) is not a duration (e.g. 168h, 24h): %w", key, raw, configKey, err)
	}
	if parsed < low || parsed > high {
		return 0, fmt.Errorf("%s=%s (%s) must be between %s and %s", key, raw, configKey, low, high)
	}
	if parsed%time.Second != 0 {
		return 0, fmt.Errorf("%s=%s (%s) must be a whole number of seconds", key, raw, configKey)
	}
	return parsed, nil
}
