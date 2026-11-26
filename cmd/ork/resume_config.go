package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// The bounds of engine.resume.*. A confirm's wait bounds its reset's
// deadline, and the claim that holds one lives a day: the bounds keep them
// far apart.
const (
	minResumeWait    = time.Second
	maxResumeWait    = 10 * time.Minute
	maxResumeSeqGap  = 1_000_000
	maxResumeConfirm = 600
	minChildWaitHint = time.Minute
	maxChildWaitHint = 30 * 24 * time.Hour
)

// resumeSettingsFromEnv reads engine.resume.* (docs/DEPLOY.md §1).
func resumeSettingsFromEnv() (api.ResumeSettings, error) {
	out := api.DefaultResumeSettings()
	var errs []error
	durationIn := func(name string, into *time.Duration, fallback, least, most time.Duration) {
		raw := env(name, "")
		if raw == "" {
			*into = fallback
			return
		}
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("%s=%q is not a duration (e.g. 30s): %w", name, raw, err))
		case d < least || d > most:
			errs = append(errs, fmt.Errorf("%s=%s must be between %s and %s", name, d, least, most))
		default:
			*into = d
		}
	}
	duration := func(name string, into *time.Duration, fallback time.Duration) {
		durationIn(name, into, fallback, minResumeWait, maxResumeWait)
	}
	number := func(name string, fallback, most int32) int32 {
		n, err := envInt32(name, fallback)
		switch {
		case err != nil:
			errs = append(errs, err)
		case n < 1 || n > most:
			errs = append(errs, fmt.Errorf("%s=%d must be between 1 and %d", name, n, most))
		default:
			return n
		}
		return fallback
	}
	duration("ORK_RESUME_CONFIRM_TIMEOUT", &out.ConfirmTimeout, engine.DefaultResumeConfirmTimeout)
	duration("ORK_RESUME_VISIBILITY_LAG_FLOOR", &out.VisibilityLagFloor, engine.DefaultResumeVisibilityLagFloor)
	durationIn("ORK_RESUME_CHILD_WAIT_HINT", &out.ChildWaitHint, engine.DefaultResumeChildWaitHint, minChildWaitHint, maxChildWaitHint)
	out.MaxSequenceGap = int64(number("ORK_RESUME_MAX_SEQ_GAP", int32(engine.DefaultResumeMaxSequenceGap), maxResumeSeqGap))
	out.ConfirmsPerMinute = int(number("ORK_RESUME_CONFIRM_PER_RUN", engine.DefaultResumeConfirmsPerRun, maxResumeConfirm))
	return out, errors.Join(errs...)
}

// resumeSeam is how the engine's service resets a run to resume it: none
// while resume is not shipped, and the service then resumes no runs.
func resumeSeam(starter *engine.TemporalStarter) engine.ResumeCluster {
	if !engine.ResumeShipped {
		return nil
	}
	return starter
}
