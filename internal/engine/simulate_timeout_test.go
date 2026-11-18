package engine_test

import (
	"testing"
)

// TestSimulate_CarriesTheDefinitionsRunTimeout: a plan says the planned
// definition's own run timeout (docs/DSL.md "Run timeout"), and 0 for a
// definition that declares none, so `orkctl run plan` can print it.
func TestSimulate_CarriesTheDefinitionsRunTimeout(t *testing.T) {
	const doc = "apiVersion: orkstack/v1\nkind: Orkflow\nname: bounded\nversion: \"1\"\ntimeout: 1h30m\ntasks:\n  - id: soak\n    sleep:\n      seconds: 60\n"
	if got := simulate(t, compile(t, doc), nil, nil).GetTimeoutSeconds(); got != 5400 {
		t.Fatalf("timeout_seconds = %d, want 5400", got)
	}
	if got := simulate(t, compile(t, anyFailedSkippedDocument), nil, nil).GetTimeoutSeconds(); got != 0 {
		t.Fatalf("timeout_seconds of a definition with none = %d, want 0", got)
	}
}
