package task

import orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

// interruptionPhases maps every phase an Orker's heartbeat names
// (internal/orker: Heartbeater.Phase, PhaseReported) to how far the attempt
// had got. It is the one table: a phase not in it is unknown, never guessed.
// TestInterruptionPhaseOf_CoversEveryPhaseTheOrkerEmits holds it to the
// Orker's source.
var interruptionPhases = map[string]orkstackv1.InterruptionPhase{
	// Preparing: nothing of the task's ran yet.
	"start":           orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"image-policy":    orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"profiles":        orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"commit-check":    orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"workspace":       orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"stage-inputs":    orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"memo":            orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"stage-artifacts": orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"image":           orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"render":          orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	"create":          orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START,
	// The container running, or being stopped at its timeout or a cancel.
	"run":     orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_RUNNING,
	"timeout": orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_RUNNING,
	"cancel":  orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_RUNNING,
	// The container ended; its outputs being checked and saved.
	"exit":     orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_AFTER_EXIT,
	"validate": orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_AFTER_EXIT,
	"commit":   orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_AFTER_EXIT,
	// The Orker reported how the attempt ended.
	"reported": orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_REPORTED,
}

// InterruptionPhaseOf is how far an attempt had got, from the last phase its
// heartbeat named: UNSPECIFIED for a phase this release does not know.
func InterruptionPhaseOf(phase string) orkstackv1.InterruptionPhase {
	return interruptionPhases[phase]
}
