package main

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestInterruptionBudgetsFromEnv: unset is the defaults; each setting is
// read; a value that is not an integer, or outside its bounds, is refused at
// boot naming the setting, never clamped.
func TestInterruptionBudgetsFromEnv(t *testing.T) {
	got, err := interruptionBudgetsFromEnv()
	if err != nil || got != engine.DefaultInterruptionBudgets {
		t.Fatalf("unset = %+v, %v; want the defaults", got, err)
	}
	t.Setenv("ORK_INTERRUPTIONS_LOST", "0")
	t.Setenv("ORK_INTERRUPTIONS_LOST_RUN_CAP", "200")
	t.Setenv("ORK_INTERRUPTIONS_HANDED_BACK", "7")
	t.Setenv("ORK_INTERRUPTIONS_NEVER_STARTED", "100")
	t.Setenv("ORK_INTERRUPTIONS_NEVER_STARTED_FAULTS", "1")
	t.Setenv("ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF", "30s")
	got, err = interruptionBudgetsFromEnv()
	want := engine.InterruptionBudgets{Lost: 0, LostRunCap: 200, HandedBack: 7, NeverStarted: 100, NeverStartedFaults: 1, NeverStartedBackoffMillis: 30000, NeverStartedFaultFloorMillis: engine.DefaultInterruptionBudgets.NeverStartedFaultFloorMillis}
	if err != nil || got != want {
		t.Fatalf("set = %+v, %v; want %+v", got, err, want)
	}
	for _, tc := range []struct{ env, value string }{
		{"ORK_INTERRUPTIONS_LOST", "11"},
		{"ORK_INTERRUPTIONS_LOST", "-1"},
		{"ORK_INTERRUPTIONS_LOST_RUN_CAP", "0"},
		{"ORK_INTERRUPTIONS_HANDED_BACK", "51"},
		{"ORK_INTERRUPTIONS_NEVER_STARTED", "lots"},
		{"ORK_INTERRUPTIONS_NEVER_STARTED_FAULTS", "21"},
		{"ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF", "500ms"},
		{"ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF", "31s"},
	} {
		t.Run(tc.env+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			if _, err := interruptionBudgetsFromEnv(); err == nil || !strings.Contains(err.Error(), tc.env) {
				t.Fatalf("%s=%s: err = %v, want a refusal naming it", tc.env, tc.value, err)
			}
		})
	}
}
