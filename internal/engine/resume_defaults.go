package engine

import "time"

// engine.resume.*'s defaults (docs/DEPLOY.md §1). ResumeRun reads the
// settings at each request: they decide no workflow command, so no run pins
// one.
const (
	// DefaultResumeConfirmTimeout is how long a confirmed resume waits for its
	// run to carry on.
	DefaultResumeConfirmTimeout = 30 * time.Second
	// DefaultResumeVisibilityLagFloor is how long after the last ending below
	// a run the cluster's index is trusted to show every run open there.
	DefaultResumeVisibilityLagFloor = 15 * time.Second
	// DefaultResumeChildWaitHint is how long a run below a resumed run may
	// have been open before the refusal leads with ending it rather than
	// waiting for it (design §28.12 F8).
	DefaultResumeChildWaitHint = 24 * time.Hour
	// DefaultResumeMaxSequenceGap is how far a run's stored events may go on
	// above its ending before a resume asks its caller to accept them.
	DefaultResumeMaxSequenceGap int64 = 4096
	// DefaultResumeConfirmsPerRun bounds one run's confirms a minute, on each
	// replica.
	DefaultResumeConfirmsPerRun = 6
)
