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
)

// resumeSettingsFromEnv reads engine.resume.* (docs/DEPLOY.md §1).
func resumeSettingsFromEnv() (api.ResumeSettings, error) {
	out := api.DefaultResumeSettings()
	var errs []error
	duration := func(name string, into *time.Duration, fallback time.Duration) {
		d, err := envPositiveDuration(name, fallback)
		switch {
		case err != nil:
			errs = append(errs, err)
		case d < minResumeWait || d > maxResumeWait:
			errs = append(errs, fmt.Errorf("%s=%s must be between %s and %s", name, d, minResumeWait, maxResumeWait))
		default:
			*into = d
		}
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
