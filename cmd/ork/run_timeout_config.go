package main

import (
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// The bounds of ORK_RUN_TIMEOUT_HANDLER_GRACE
// (engine.run_timeout_handler_grace). Zero is allowed: a timed-out run then
// starts no failure handler. Above a day a run would keep its handlers going
// a full day past the limit its author declared. The plan carries whole
// seconds, so a fraction of a second is refused rather than rounded.
const maxRunTimeoutHandlerGrace = 24 * time.Hour

// runTimeoutHandlerGraceFromEnv reads ORK_RUN_TIMEOUT_HANDLER_GRACE: how long
// after a run's deadline its failure handlers may still run. Always
// validated, a duration of whole seconds from 0s to 24h; an unset key is
// engine.DefaultRunTimeoutHandlerGrace. The value is pinned, in seconds, into
// the plan of every run whose definition declares `timeout:`
// (api.Service.runTimeoutPlanFor), so changing it moves only new runs.
func runTimeoutHandlerGraceFromEnv() (time.Duration, error) {
	raw := env("ORK_RUN_TIMEOUT_HANDLER_GRACE", "")
	if raw == "" {
		return engine.DefaultRunTimeoutHandlerGrace, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return engine.DefaultRunTimeoutHandlerGrace, fmt.Errorf(
			"ORK_RUN_TIMEOUT_HANDLER_GRACE=%q is not a duration (e.g. 10m, 90s): %w", raw, err)
	}
	if parsed < 0 || parsed > maxRunTimeoutHandlerGrace {
		return engine.DefaultRunTimeoutHandlerGrace, fmt.Errorf(
			"ORK_RUN_TIMEOUT_HANDLER_GRACE=%s must be between 0s and %s", raw, maxRunTimeoutHandlerGrace)
	}
	if parsed%time.Second != 0 {
		return engine.DefaultRunTimeoutHandlerGrace, fmt.Errorf(
			"ORK_RUN_TIMEOUT_HANDLER_GRACE=%s must be a whole number of seconds", raw)
	}
	return parsed, nil
}
