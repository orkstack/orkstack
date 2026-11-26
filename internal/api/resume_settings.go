package api

import (
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// ResumeSettings are engine.resume.* (docs/DEPLOY.md §1): what ResumeRun
// waits for and bounds. They decide no workflow command, so a run pins none.
type ResumeSettings struct {
	// VisibilityLagFloor is how long after the last ending below a run the
	// cluster's index of runs is trusted to show every run still open there.
	VisibilityLagFloor time.Duration
	// ChildWaitHint is how long a run below the base may have been open
	// before a refusal leads with ending it rather than waiting for it.
	ChildWaitHint time.Duration
	// MaxSequenceGap is how far a run's stored events may go on above its
	// ending before a resume asks its caller to accept them.
	MaxSequenceGap int64
	// ConfirmTimeout is how long a confirm waits for the run to carry on, and,
	// with resumeResetMargin, how long its reset may take to be applied.
	ConfirmTimeout time.Duration
	// ConfirmsPerMinute bounds how often one run's resume is confirmed, on
	// each engine replica.
	ConfirmsPerMinute int
}

// DefaultResumeSettings are the defaults of engine.resume.*.
func DefaultResumeSettings() ResumeSettings {
	return ResumeSettings{
		VisibilityLagFloor: engine.DefaultResumeVisibilityLagFloor, ChildWaitHint: engine.DefaultResumeChildWaitHint,
		MaxSequenceGap: engine.DefaultResumeMaxSequenceGap,
		ConfirmTimeout: engine.DefaultResumeConfirmTimeout, ConfirmsPerMinute: engine.DefaultResumeConfirmsPerRun,
	}
}

// resumeSettingsOf is set with every unset value its default.
func resumeSettingsOf(set ResumeSettings) ResumeSettings {
	if set.VisibilityLagFloor <= 0 {
		set.VisibilityLagFloor = DefaultResumeSettings().VisibilityLagFloor
	}
	if set.ChildWaitHint <= 0 {
		set.ChildWaitHint = DefaultResumeSettings().ChildWaitHint
	}
	if set.MaxSequenceGap <= 0 {
		set.MaxSequenceGap = DefaultResumeSettings().MaxSequenceGap
	}
	if set.ConfirmTimeout <= 0 {
		set.ConfirmTimeout = DefaultResumeSettings().ConfirmTimeout
	}
	if set.ConfirmsPerMinute <= 0 {
		set.ConfirmsPerMinute = DefaultResumeSettings().ConfirmsPerMinute
	}
	return set
}
