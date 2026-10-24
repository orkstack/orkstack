package engine_test

import (
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// The plan half of join policies (docs/DSL.md "Joins and failure handlers"): Simulate decides a
// join with the interpreter's own predicate and sentences, plans assumed
// failures, and agrees with a run in which exactly those tasks fail.

func simulateAssuming(t *testing.T, wf *orkstackv1.Orkflow, input *orkstackv1.Payload,
	assumed map[string]*orkstackv1.Payload, assumeFailed []string,
) *orkstackv1.PlanRunResponse {
	t.Helper()
	plan, err := engine.Simulate(wf, input, assumed, assumeFailed)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	return plan
}

func outcomeOf(t *testing.T, plan *orkstackv1.PlanRunResponse, id string) orkstackv1.PlanOutcome {
	t.Helper()
	return planTaskOf(t, plan, id).GetOutcome()
}

func TestSimulate_NoneFailedRejoinAgreesWithTheInterpreter(t *testing.T) {
	wf := compile(t, rejoinDocument)
	for i, route := range []string{"canary", "full", "none"} {
		input := payload(t, map[string]any{"route": route})
		plan := simulate(t, wf, input, nil)
		verify := planTaskOf(t, plan, "verify")
		if verify.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED {
			t.Errorf("%s: PlanTask.join = %v", route, verify.GetJoin())
		}
		if route == "none" {
			if verify.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED ||
				verify.GetSkipReason() != "join none_failed: every need was skipped (canary, full)" || verify.GetSkippedBy() != "" {
				t.Errorf("verify = %v", verify)
			}
		} else if verify.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || verify.GetWave() != 3 {
			t.Errorf("%s: verify = %v", route, verify)
		}
		runID := fmt.Sprintf("run-simulate-rejoin-%d", i)
		run, h := interpret(t, runID, wf, input, nil)
		assertAgreement(t, "rejoin "+route, plan, run, h, runID)
	}
}

func TestSimulate_AnyFailedIsSkippedWithTheRunsOnlyWhenSentence(t *testing.T) {
	wf := compile(t, anyFailedSkippedDocument)
	plan := simulate(t, wf, nil, nil)
	rollback := planTaskOf(t, plan, "rollback")
	if rollback.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED ||
		rollback.GetSkipReason() != "join any_failed: none of its needs failed (deploy); it runs only when one of them fails" ||
		rollback.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED || rollback.GetWave() != 2 {
		t.Fatalf("rollback = %v", rollback)
	}
	run, h := interpret(t, "run-simulate-any-failed", wf, nil, nil)
	assertAgreement(t, "any_failed skipped", plan, run, h, "run-simulate-any-failed")
}

func TestSimulate_AssumeFailedPlansHandlersAndUnreachesDependentNonHandlers(t *testing.T) {
	plan := simulateAssuming(t, compile(t, joinFailDocument), nil, nil, []string{"deploy"})
	deploy := planTaskOf(t, plan, "deploy")
	if deploy.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED || deploy.GetWave() != 2 ||
		deploy.GetError().GetCode() != "ASSUMED_FAILED" || deploy.GetError().GetMessage() != "assumed to fail (assume_failed)" {
		t.Fatalf("deploy = %v", deploy)
	}
	announce := planTaskOf(t, plan, "announce")
	if announce.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED || announce.GetUnreachedBy() != "deploy" ||
		announce.GetWave() != 0 {
		t.Fatalf("announce = %v", announce)
	}
	for _, id := range []string{"rollback", "notify", "cleanup"} {
		if got := outcomeOf(t, plan, id); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
			t.Errorf("%s = %v, want RUNS", id, got)
		}
	}
	if plan.GetFailsAt() != "deploy" || plan.GetAssumedFailed() != 1 || len(plan.GetUnusedAssumeFailed()) != 0 {
		t.Fatalf("fails_at %q, assumed_failed %d, unused %v", plan.GetFailsAt(), plan.GetAssumedFailed(), plan.GetUnusedAssumeFailed())
	}
	// ASSUMED_FAILED takes a wave.
	found := false
	for _, w := range plan.GetWaves() {
		for _, id := range w.GetTaskIds() {
			found = found || id == "deploy"
		}
	}
	if !found {
		t.Errorf("waves %v do not list deploy", plan.GetWaves())
	}
}

func TestSimulate_AssumeFailedLeavesIndependentWorkPlannedAsRunning(t *testing.T) {
	plan := simulateAssuming(t, compile(t, joinFailDocument), nil, nil, []string{"deploy"})
	if got := outcomeOf(t, plan, "smoke"); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("smoke (independent of deploy) = %v, want RUNS", got)
	}
	if got := outcomeOf(t, plan, "build"); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("build = %v", got)
	}
}

func TestSimulate_UnreachedByNamesTheFailureThatCutIt(t *testing.T) {
	chain := compile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: chain
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec}
`)
	plan := simulateAssuming(t, chain, nil, nil, []string{"a"})
	for _, id := range []string{"b", "c"} {
		if p := planTaskOf(t, plan, id); p.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED || p.GetUnreachedBy() != "a" {
			t.Errorf("%s = %v", id, p)
		}
	}
	broken := simulate(t, compile(t, conditionErrorDocument), payload(t, map[string]any{"a": "x", "b": 1}), nil)
	for _, id := range []string{"after", "later"} {
		if p := planTaskOf(t, broken, id); p.GetUnreachedBy() != "broken" {
			t.Errorf("%s unreached_by = %q, want broken", id, p.GetUnreachedBy())
		}
	}
}

func TestSimulate_AssumeFailedAgreesWithARunWhereThatTaskFails(t *testing.T) {
	cases := []struct {
		name    string
		wf      *orkstackv1.Orkflow
		input   map[string]any
		assumed map[string]map[string]any
		fail    []string
	}{
		{"join-fail, deploy", compile(t, joinFailDocument), nil, nil, []string{"deploy"}},
		{"branch-and-recover, deploy", compileExample(t, "branch-and-recover.ork.yaml"),
			map[string]any{"route": "full"}, map[string]map[string]any{"decide": {"value": "full"}}, []string{"deploy"}},
		{"branch-and-recover, deploy and rollback", compileExample(t, "branch-and-recover.ork.yaml"),
			map[string]any{"route": "canary"}, map[string]map[string]any{"decide": {"value": "canary"}},
			[]string{"deploy", "rollback"}},
		{"escalate, rollback never runs", compile(t, escalateDocument), nil, nil, []string{"rollback"}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := payload(t, tc.input)
			plan := simulateAssuming(t, tc.wf, input, assumedPayloads(t, tc.assumed), tc.fail)
			runID := fmt.Sprintf("run-simulate-assume-%d", i)
			run, h := interpretFailing(t, runID, tc.wf, input, tc.assumed, tc.fail)
			assertAgreement(t, tc.name, plan, run, h, runID)
		})
	}
}

func TestSimulate_AssumeFailedOnASkippedTaskIsUnused(t *testing.T) {
	plan := simulateAssuming(t, compile(t, rejoinDocument), payload(t, map[string]any{"route": "canary"}), nil,
		[]string{"full", "decide"})
	if !reflect.DeepEqual(plan.GetUnusedAssumeFailed(), []string{"full"}) {
		t.Fatalf("unused_assume_failed = %v, want [full]", plan.GetUnusedAssumeFailed())
	}
	if plan.GetFailsAt() != "decide" || outcomeOf(t, plan, "full") != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED {
		t.Fatalf("fails_at = %q, full = %v", plan.GetFailsAt(), outcomeOf(t, plan, "full"))
	}
	only := simulateAssuming(t, compile(t, rejoinDocument), payload(t, map[string]any{"route": "canary"}), nil, []string{"full"})
	if only.GetFailsAt() != "" || only.GetAssumedFailed() != 0 || !reflect.DeepEqual(only.GetUnusedAssumeFailed(), []string{"full"}) {
		t.Fatalf("an assumption on a skipped task = fails_at %q, unused %v", only.GetFailsAt(), only.GetUnusedAssumeFailed())
	}
}

func TestSimulate_ConditionErrorPlansTheBrokenTasksHandlers(t *testing.T) {
	wf := compile(t, brokenConditionDocument+`
  - id: rollback
    uses: shell
    needs: [broken]
    join: any_failed
    with: {op: exec}
`)
	input := payload(t, map[string]any{"a": "x", "b": 1})
	plan := simulate(t, wf, input, nil)
	if p := planTaskOf(t, plan, "broken"); p.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS || p.GetError().GetCode() != "RUN_FAILED" {
		t.Fatalf("broken = %v", p)
	}
	if got := outcomeOf(t, plan, "rollback"); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("rollback = %v, want RUNS", got)
	}
	if got := outcomeOf(t, plan, "after"); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED {
		t.Fatalf("after = %v", got)
	}
	run, h := interpret(t, "run-simulate-condition-handler", wf, input, nil)
	if tr := taskRunOf(t, run, "broken"); tr.GetStatus() != orkstackv1.Status_STATUS_FAILED || tr.GetError().GetCode() != "CONDITION_FAILED" {
		t.Fatalf("broken in the run = %v", tr)
	}
	assertAgreement(t, "condition error with a handler", plan, run, h, "run-simulate-condition-handler")
}

func TestSimulate_AHandlersBrokenConditionAfterFailsAtCutsNothing(t *testing.T) {
	wf := compile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: two-fails
inputs:
  a: {type: string}
  b: {type: number}
tasks:
  - id: first
    uses: shell
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {op: exec}
  - id: h1
    uses: shell
    needs: [first]
    join: any_failed
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {op: exec}
  - id: h2
    uses: shell
    needs: [first]
    join: all_done
    with: {op: exec}
`)
	input := payload(t, map[string]any{"a": "x", "b": 1})
	plan := simulate(t, wf, input, nil)
	if plan.GetFailsAt() != "first" {
		t.Fatalf("fails_at = %q", plan.GetFailsAt())
	}
	if p := planTaskOf(t, plan, "h1"); p.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS || p.GetError().GetCode() != "CONDITION_FAILED" {
		t.Fatalf("h1 = %v", p)
	}
	if got := outcomeOf(t, plan, "h2"); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("h2 = %v, want RUNS: a handler's FAILS after fails_at cuts nothing", got)
	}
	run, h := interpret(t, "run-simulate-two-fails", wf, input, nil)
	assertAgreement(t, "two FAILS", plan, run, h, "run-simulate-two-fails")
}

// With no assumption, a definition with no join gets a plan whose new fields
// are all unset — byte-identical to before — save fails_at and unreached_by,
// which only a FAILS sets.
func TestSimulate_NoAssumptionIsProtoEqualToBefore(t *testing.T) {
	docs := []*orkstackv1.Orkflow{
		compile(t, diamondDocument), compile(t, wavesDocument), compile(t, smokeDocument),
		compileExample(t, "http-post.ork.yaml"), compileExample(t, "cached-pipeline.ork.yaml"),
	}
	for _, wf := range docs {
		withNil := simulateAssuming(t, wf, nil, nil, nil)
		withEmpty := simulateAssuming(t, wf, nil, nil, []string{})
		if !proto.Equal(withNil, withEmpty) {
			t.Errorf("%s: nil and empty assumptions differ", wf.GetName())
		}
		if withNil.GetFailsAt() != "" || withNil.GetAssumedFailed() != 0 || len(withNil.GetUnusedAssumeFailed()) != 0 {
			t.Errorf("%s: new response fields set: %v", wf.GetName(), withNil)
		}
		for _, p := range withNil.GetTasks() {
			if p.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED || p.GetUnreachedBy() != "" {
				t.Errorf("%s/%s: new task fields set: %v", wf.GetName(), p.GetTaskId(), p)
			}
		}
	}
}
