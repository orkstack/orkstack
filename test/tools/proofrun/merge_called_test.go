package main

import (
	"testing"
)

// TestMerge_ReadsACalledProofsJobsByTheirParts: a proof called from
// proof-label.yml is listed as `prove / <job>`. The merger reads each part from
// that name, and neither the called `proof` nor the verdict job named `proof` is
// a part, so a run with both merges as a run with one.
func TestMerge_ReadsACalledProofsJobsByTheirParts(t *testing.T) {
	for name, want := range map[string]string{
		"prove / fast":                 "fast",
		"prove / plan":                 "plan",
		"prove / integration (3)":      "integration:3",
		"prove / ui-e2e (1)":           "ui-e2e:1",
		"prove / activity-fixture (2)": "activity-fixture:2",
		"prove / proof":                "",
		"proof":                        "",
		"no proof asked for":           "",
	} {
		if got := partFromJobName(name); got != want {
			t.Errorf("partFromJobName(%q) = %q, want %q", name, got, want)
		}
	}
}
