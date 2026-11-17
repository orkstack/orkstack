package task

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// emittedPhases is every phase the Orker's own (non-test) source names: each
// Heartbeater.Phase("…") call, and PhaseReported.
func emittedPhases(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "orker", "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no Orker source found: %v", err)
	}
	call := regexp.MustCompile(`\.Phase\("([a-z-]+)"\)`)
	reported := regexp.MustCompile(`const PhaseReported = "([a-z-]+)"`)
	out := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range call.FindAllStringSubmatch(string(src), -1) {
			out[m[1]] = true
		}
		for _, m := range reported.FindAllStringSubmatch(string(src), -1) {
			out[m[1]] = true
		}
	}
	return out
}

// TestInterruptionPhaseOf_CoversEveryPhaseTheOrkerEmits: every phase the
// Orker's heartbeat names maps to a phase, none to UNSPECIFIED; the table
// names no phase the Orker does not emit; and an unknown phase is
// UNSPECIFIED, never a guess.
func TestInterruptionPhaseOf_CoversEveryPhaseTheOrkerEmits(t *testing.T) {
	emitted := emittedPhases(t)
	if len(emitted) < 15 || !emitted["run"] || !emitted["reported"] {
		t.Fatalf("found only %d phases in the Orker's source (%v): the scan is broken", len(emitted), emitted)
	}
	for phase := range emitted {
		if InterruptionPhaseOf(phase) == orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_UNSPECIFIED {
			t.Errorf("the Orker emits phase %q, which the table does not map", phase)
		}
	}
	for phase := range interruptionPhases {
		if !emitted[phase] {
			t.Errorf("the table maps %q, which the Orker never emits", phase)
		}
	}
	for _, unknown := range []string{"", "container", "frobnicate", "RUN"} {
		if got := InterruptionPhaseOf(unknown); got != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_UNSPECIFIED {
			t.Errorf("unknown phase %q = %v, want UNSPECIFIED", unknown, got)
		}
	}
	if InterruptionPhaseOf("run") != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_RUNNING ||
		InterruptionPhaseOf("create") != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START ||
		InterruptionPhaseOf("commit") != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_AFTER_EXIT ||
		InterruptionPhaseOf("reported") != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_REPORTED {
		t.Error("a phase maps to the wrong class")
	}
}
