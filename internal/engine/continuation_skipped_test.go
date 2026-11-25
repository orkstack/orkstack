package engine_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// skippedFanOutDocument skips a fan-out by its condition, and another because
// what it needs was skipped, and then runs long enough to carry on.
const skippedFanOutDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: skipped-fan-out
inputs:
  xs: {type: array}
tasks:
  - id: spread
    uses: shell
    condition: "{{ eq 1 2 }}"
    each: {over: "{{ .inputs.xs }}", as: x, max_items: 5}
    with: {op: exec, command: "echo {{ .each.x }}"}
  - id: off
    uses: shell
    condition: "{{ eq 1 2 }}"
    with: {op: exec, command: "off"}
  - id: below
    uses: shell
    needs: [off]
    each: {over: "{{ .inputs.xs }}", as: x, max_items: 5}
    with: {op: exec, command: "echo {{ .each.x }}"}
  - id: first
    uses: shell
    with: {op: exec, command: "first"}
  - id: second
    uses: shell
    needs: [first]
    with: {op: exec, command: "second"}
  - id: third
    uses: shell
    needs: [second]
    with: {op: exec, command: "third"}
`

// TestContinuation_ASkippedFanOutCarriesOn: a run whose fan-out was skipped
// carries on past it, like one whose task was.
func TestContinuation_ASkippedFanOutCarriesOn(t *testing.T) {
	g := runGenerations(t, "ork-skipped-fan-out", compile(t, skippedFanOutDocument),
		payload(t, map[string]any{"xs": []any{"a"}}), continuedPlan(1), okBehavior, nil)
	run := generationsOK(t, g, 2)
	for _, id := range []string{"spread", "below"} {
		if row := taskRunOfRun(run, id); row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || len(row.GetItems()) != 0 {
			t.Errorf("%s = %v, want skipped", id, row)
		}
	}
	if row := taskRunOfRun(run, "third"); row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("third = %v", row)
	}
}

// skippedPinMemberDocument skips one member of a pin by its condition, and
// carries on before the others, whose needs are not in yet, open it.
const skippedPinMemberDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: skipped-pin-member
pins:
  host: {}
tasks:
  - id: off
    uses: shell
    pin: host
    run_as_orker_user: true
    condition: "{{ eq 1 2 }}"
    with: {command: "off"}
  - id: first
    uses: shell
    with: {op: exec, command: "first"}
  - id: second
    uses: shell
    needs: [first]
    with: {op: exec, command: "second"}
  - id: pinned
    uses: shell
    pin: host
    run_as_orker_user: true
    needs: [second]
    with: {command: "pinned"}
  - id: last
    uses: shell
    pin: host
    run_as_orker_user: true
    needs: [pinned]
    with: {command: "last"}
`

// TestContinuation_APinWithASkippedMemberCarriesOn: a skipped member opens no
// pin, so a run carries on before the members that will.
func TestContinuation_APinWithASkippedMemberCarriesOn(t *testing.T) {
	budget := engine.NewDeadlockBudgetForTest(t)
	g := runGenerations(t, "ork-skipped-pin-member", compile(t, skippedPinMemberDocument), payload(t, nil), continuedPlan(1), okBehavior,
		func(h *harness, _ int) { pinSetup(budget, h) })
	run := generationsOK(t, g, 2)
	budget.Check(t)
	for id, want := range map[string]orkstackv1.Status{"off": orkstackv1.Status_STATUS_SKIPPED,
		"pinned": orkstackv1.Status_STATUS_SUCCEEDED, "last": orkstackv1.Status_STATUS_SUCCEEDED} {
		if got := taskRunOfRun(run, id).GetStatus(); got != want {
			t.Errorf("%s = %v, want %v", id, got, want)
		}
	}
}
