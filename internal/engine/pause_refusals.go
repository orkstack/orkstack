package engine

// The reasons PauseRun and UnpauseRun refuse a request with
// (docs/REASONS.md, "Pausing a run"). Each refusal's message begins with its
// reason, as an admission refusal's does.
const (
	// RunPauseUnsupportedCode: the run's plan carries no Pause, so it was
	// started by a release that cannot pause it.
	RunPauseUnsupportedCode = "RUN_PAUSE_UNSUPPORTED"
	// RunPauseNotRunningCode: the run has not started, is waiting for its
	// concurrency slot, or is resuming.
	RunPauseNotRunningCode = "RUN_PAUSE_NOT_RUNNING"
	// RunAlreadyPausedCode: a pause of a run that is already paused.
	RunAlreadyPausedCode = "RUN_ALREADY_PAUSED"
	// RunPauseInheritedCode: an unpause of a run its parent run's pause
	// holds.
	RunPauseInheritedCode = "RUN_PAUSE_INHERITED"
	// RunEndingCode: a pause of a failing run with no failure handler left
	// to start, or an unpause of a run whose pause has expired.
	RunEndingCode = "RUN_ENDING"
)
