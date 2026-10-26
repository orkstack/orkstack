package main

import (
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// The bounds of ORK_PIN_CREATION_TIMEOUT (engine.pin_creation_timeout). Below
// one second a pin could not outwait one of the SDK's own creation retries
// (a 1s backoff), so it would fail PIN_UNAVAILABLE on any momentary
// saturation; above a day a run would sit PENDING behind a pool that has had
// a full day to free a slot, which no Orkflow's author asked for. The plan
// carries whole seconds, so a fraction of a second is refused rather than
// rounded.
const (
	minPinCreationTimeout = time.Second
	maxPinCreationTimeout = 24 * time.Hour
)

// pinCreationTimeoutFromEnv reads ORK_PIN_CREATION_TIMEOUT: how long a pin
// waits for some pin host of its pool to have a free session slot before its
// first task fails PIN_UNAVAILABLE. Always validated, a duration of whole
// seconds from 1s to 24h; an unset key is engine.DefaultPinCreationTimeout.
// The value is pinned, in seconds, into every top-level plan at submit
// (api.Service.pinRunTreeLimits), so changing it moves only new runs.
func pinCreationTimeoutFromEnv() (time.Duration, error) {
	raw := env("ORK_PIN_CREATION_TIMEOUT", "")
	if raw == "" {
		return engine.DefaultPinCreationTimeout, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return engine.DefaultPinCreationTimeout, fmt.Errorf(
			"ORK_PIN_CREATION_TIMEOUT=%q is not a duration (e.g. 5m, 90s): %w", raw, err)
	}
	if parsed < minPinCreationTimeout || parsed > maxPinCreationTimeout {
		return engine.DefaultPinCreationTimeout, fmt.Errorf(
			"ORK_PIN_CREATION_TIMEOUT=%s must be between %s and %s", raw, minPinCreationTimeout, maxPinCreationTimeout)
	}
	if parsed%time.Second != 0 {
		return engine.DefaultPinCreationTimeout, fmt.Errorf(
			"ORK_PIN_CREATION_TIMEOUT=%s must be a whole number of seconds", raw)
	}
	return parsed, nil
}
