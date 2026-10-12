package api

import (
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestCheckRunPlanSize_RefusesAPlanOverTheBoundWithoutChildren covers the run
// that composes nothing: mintChildren never measures its plan, so this check
// is the only one, and it must refuse past engine.MaxPlanBytes.
func TestCheckRunPlanSize_RefusesAPlanOverTheBoundWithoutChildren(t *testing.T) {
	small := engine.RunPlan{Namespace: "default"}
	if err := checkRunPlanSize(small); err != nil {
		t.Fatalf("a small plan was refused: %v", err)
	}
	big := engine.RunPlan{CommitRoot: strings.Repeat("r", engine.MaxPlanBytes+1)}
	err := checkRunPlanSize(big)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "the run's plan encodes to") {
		t.Fatalf("checkRunPlanSize(big) = %v, want FailedPrecondition naming the plan", err)
	}
}
