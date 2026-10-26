package engine

import (
	"testing"
	"time"

	"go.temporal.io/sdk/workflow"
)

// TestPinCreationTimeoutFor_AnswersFromTheRunsOwnDecisions: a generation that
// carries the decision of an earlier one answers from it and never asks
// again (version), so a run's pins wait the same number in every generation
// whichever build a later generation runs on. The context is never touched:
// a decision already made is returned before the SDK is consulted, which is
// exactly the property under test.
func TestPinCreationTimeoutFor_AnswersFromTheRunsOwnDecisions(t *testing.T) {
	cases := []struct {
		name     string
		decision workflow.Version
		seconds  int32
		want     time.Duration
	}{
		{name: "gated, a number", decision: 1, seconds: 30, want: 30 * time.Second},
		{name: "gated, no number", decision: 1, seconds: 0, want: 5 * time.Minute},
		{name: "legacy, a number is ignored", decision: workflow.DefaultVersion, seconds: 30, want: 5 * time.Minute},
		{name: "legacy, no number", decision: workflow.DefaultVersion, seconds: 0, want: 5 * time.Minute},
		{name: "gated, a day", decision: 1, seconds: 86400, want: 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &runState{
				plan:     RunPlan{PinCreationTimeoutSeconds: tc.seconds},
				versions: map[string]workflow.Version{PinCreationTimeoutChange: tc.decision},
			}
			if got := s.pinCreationTimeoutFor(nil); got != tc.want {
				t.Errorf("pinCreationTimeoutFor = %s, want %s", got, tc.want)
			}
			if got := s.versions[PinCreationTimeoutChange]; got != tc.decision {
				t.Errorf("the decision became %d, want it left at %d", got, tc.decision)
			}
		})
	}
}

// TestLegacyPinCreationTimeout_IsFiveMinutes freezes the number every build
// before the gate used. The DefaultVersion branch of a recorded history is
// only faithful while it stays exactly this; a release that changes the
// deployment's default (DefaultPinCreationTimeout) must not move it.
func TestLegacyPinCreationTimeout_IsFiveMinutes(t *testing.T) {
	if legacyPinCreationTimeout != 5*time.Minute {
		t.Fatalf("legacyPinCreationTimeout = %s: it is frozen at the five minutes recorded histories were built with",
			legacyPinCreationTimeout)
	}
	if DefaultPinCreationTimeout != 5*time.Minute {
		t.Errorf("DefaultPinCreationTimeout = %s, want 5m0s: the setting's default is what every build before it hardcoded",
			DefaultPinCreationTimeout)
	}
}

// TestChildStartPlan_CarriesTheRunTreesPinCreationTimeout: the pin creation
// timeout is a run-tree limit like the history ceiling, set once on the
// top-level plan, and a child run waits for its pins exactly as long as the
// run that started it — whatever the nested plan a submit built holds.
func TestChildStartPlan_CarriesTheRunTreesPinCreationTimeout(t *testing.T) {
	parent := &runState{runID: "ork-parent", plan: RunPlan{PinCreationTimeoutSeconds: 45}}
	planned := &ChildRunPlan{Plan: RunPlan{Namespace: "default"}}
	if got := parent.childStartPlan(planned).PinCreationTimeoutSeconds; got != 45 {
		t.Errorf("child plan PinCreationTimeoutSeconds = %d, want the parent's 45", got)
	}

	// A plan built before the field has none, and its children start with
	// none: byte-identical to what that build sent.
	old := &runState{runID: "ork-old", plan: RunPlan{}}
	if got := old.childStartPlan(planned).PinCreationTimeoutSeconds; got != 0 {
		t.Errorf("child plan PinCreationTimeoutSeconds = %d, want unset under a plan without it", got)
	}
	if planned.Plan.PinCreationTimeoutSeconds != 0 {
		t.Errorf("the planned child plan was mutated: %d", planned.Plan.PinCreationTimeoutSeconds)
	}
}
