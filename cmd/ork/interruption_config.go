package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// interruptionBound is one engine.interruptions.* count setting and its
// bounds (design §4.3).
type interruptionBound struct {
	env      string
	min, max int32
	into     *int32
}

// interruptionBudgetsFromEnv reads engine.interruptions.*: each run this
// engine accepts pins them in its plan at submit (api.Service), so a change
// moves only new runs. An unset key is its default
// (engine.DefaultInterruptionBudgets); a value that is not an integer, or
// outside its bounds, is refused at boot naming the setting, never clamped.
func interruptionBudgetsFromEnv() (engine.InterruptionBudgets, error) {
	b := engine.DefaultInterruptionBudgets
	var errs []error
	for _, s := range []interruptionBound{
		{"ORK_INTERRUPTIONS_LOST", 0, 10, &b.Lost},
		{"ORK_INTERRUPTIONS_LOST_RUN_CAP", 1, 200, &b.LostRunCap},
		{"ORK_INTERRUPTIONS_HANDED_BACK", 1, 50, &b.HandedBack},
		{"ORK_INTERRUPTIONS_NEVER_STARTED", 1, 100, &b.NeverStarted},
		{"ORK_INTERRUPTIONS_NEVER_STARTED_FAULTS", 1, 20, &b.NeverStartedFaults},
	} {
		raw := env(s.env, "")
		if raw == "" {
			continue
		}
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || int32(n) < s.min || int32(n) > s.max {
			errs = append(errs, fmt.Errorf("%s=%q must be a whole number from %d to %d", s.env, raw, s.min, s.max))
			continue
		}
		*s.into = int32(n)
	}
	if raw := env("ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF", ""); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < time.Second || d > 30*time.Second || d%time.Millisecond != 0 {
			errs = append(errs, fmt.Errorf("ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF=%q must be a duration from 1s to 30s", raw))
		} else {
			b.NeverStartedBackoffMillis = d.Milliseconds()
		}
	}
	if len(errs) > 0 {
		return engine.DefaultInterruptionBudgets, errors.Join(errs...)
	}
	return b, nil
}
