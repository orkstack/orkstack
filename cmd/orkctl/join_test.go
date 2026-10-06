package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// branchAndRecoverPath is relative on purpose: goldens stay machine-independent.
const branchAndRecoverPath = "../../examples/branch-and-recover.ork.yaml"

func applyBranchAndRecover(t *testing.T, f *fixture) {
	t.Helper()
	f.run("apply", branchAndRecoverPath).requireExit(t, exitOK)
}

func TestRunPlan_Golden_RejoinCanary(t *testing.T) {
	f := newFixture(t)
	applyBranchAndRecover(t, f)
	got := f.run("run", "plan", "branch-and-recover", "--input", "route=canary",
		"--output", `decide={"value":"canary"}`).requireExit(t, exitOK)
	requireGolden(t, "run-plan-rejoin-canary.golden", got.stdout)
	got.requireContains(t,
		`condition "{{ eq .tasks.decide.output.value \"full\" }}" evaluated false`,
		"join none_failed",
		"join any_failed: none of its needs failed (deploy); it runs only when one of them fails")
}

func TestRunPlan_Golden_AssumeFailedDeploy(t *testing.T) {
	f := newFixture(t)
	applyBranchAndRecover(t, f)
	got := f.run("run", "plan", "branch-and-recover", "--input", "route=canary",
		"--output", `decide={"value":"canary"}`, "--assume-failed", "deploy").requireExit(t, exitOK)
	requireGolden(t, "run-plan-assume-failed-deploy.golden", got.stdout)
	got.requireContains(t,
		"assumed_failed",
		"assumed to fail (--assume-failed)",
		"after deploy fails",
		"1 assumed failed, ",
		"The run would FAIL at deploy: assumed to fail.",
		"Failure handlers that would run: rollback, notify.")
	machine := f.run("--json", "run", "plan", "branch-and-recover", "--input", "route=canary",
		"--output", `decide={"value":"canary"}`, "--assume-failed", "deploy").requireExit(t, exitOK)
	machine.requireContains(t, `"failsAt": "deploy"`, `"assumedFailed": 1`, `"outcome": "ASSUMED_FAILED"`)
}

// An older engine discards assume_failed: the plan comes back with no
// assumption applied, and the CLI says so instead of presenting it as the
// failure path.
func TestRunPlan_ReportsAnAssumptionAnOlderServerIgnored(t *testing.T) {
	plan := &orkstackv1.PlanRunResponse{
		OrkflowName: "branch-and-recover", OrkflowVersion: "0.1.0", Namespace: "default",
		Tasks: []*orkstackv1.PlanTask{{TaskId: "deploy", Outcome: orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS, Wave: 1}},
		Runs:  1, WaveCount: 1,
	}
	var out bytes.Buffer
	if err := renderPlan(&out, plan, "", []string{"deploy"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "The server did not apply --assume-failed: it predates assumed failures.") {
		t.Fatalf("plan output:\n%s", out.String())
	}
	out.Reset()
	if err := renderPlan(&out, plan, "", nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "did not apply") {
		t.Fatalf("a plan with no assumption warns:\n%s", out.String())
	}
}

func TestRunPlan_AssertRunsOfAnAssumedFailedTaskExits4(t *testing.T) {
	f := newFixture(t)
	applyBranchAndRecover(t, f)
	base := []string{"run", "plan", "branch-and-recover", "--input", "route=canary",
		"--output", `decide={"value":"canary"}`, "--assume-failed", "deploy"}
	f.run(append(base, "--assert-runs", "rollback", "--assert-runs", "notify", "--assert-skipped", "escalate")...).
		requireExit(t, exitOK)
	f.run(append(base, "--assert-runs", "announce")...).requireExit(t, exitPlanAssertionFailed)
	failed := f.run(append(base, "--assert-runs", "deploy")...).requireExit(t, exitPlanAssertionFailed)
	want := "assert-runs deploy: planned ASSUMED_FAILED (assumed to fail (--assume-failed))"
	if !strings.Contains(failed.stderr, want+"\n") {
		t.Fatalf("stderr lacks %q:\n%s", want, failed.stderr)
	}
	f.run("run", "plan", "branch-and-recover", "--input", "route=canary", "--output", `decide={"value":"canary"}`,
		"--assume-failed", "deploy", "--assume-failed", "rollback", "--assert-runs", "escalate").requireExit(t, exitOK)
}

func TestRunPlan_AssertRunsIsOneTaskPerFlag(t *testing.T) {
	f := newFixture(t)
	applyBranchAndRecover(t, f)
	base := []string{"run", "plan", "branch-and-recover", "--input", "route=canary",
		"--output", `decide={"value":"canary"}`, "--assume-failed", "deploy"}
	joined := f.run(append(base, "--assert-runs", "rollback,notify")...).requireExit(t, exitError)
	if !strings.Contains(joined.stderr, `task "rollback,notify" is not in this orkflow`) {
		t.Fatalf("stderr:\n%s", joined.stderr)
	}
	f.run(append(base, "--assert-runs", "rollback", "--assert-runs", "notify")...).requireExit(t, exitOK)
}

func TestOrkflowGet_Golden_JoinTable(t *testing.T) {
	f := newFixture(t)
	applyBranchAndRecover(t, f)
	got := f.run("orkflow", "get", "branch-and-recover").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-join.golden", got.stdout)
	got.requireContains(t, "JOIN", "verify    none_failed  canary,full", "rollback  any_failed   deploy")
}

func TestRunGet_Golden_FailingRunAndJoinTable(t *testing.T) {
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(fixedNow.Add(time.Duration(seconds) * time.Second))
	}
	f := newFixture(t)
	runID := "ork-1790000000000000000-4294967301"
	deployErr := &orkstackv1.Error{Code: "CONTAINER_NONZERO_EXIT", Message: "container exited with status 1",
		Details: `task "deploy"`, Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED}
	f.runs.Apply(&orkstackv1.Run{
		Id: runID, OrkflowName: "branch-and-recover", OrkflowVersion: "0.1.0",
		Status: orkstackv1.Status_STATUS_RUNNING, CreatedAt: at(0), StartedAt: at(0),
		Error: &orkstackv1.Error{Code: "RUN_FAILED", Message: "container exited with status 1", Details: `task "deploy"`},
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "verify", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1", Attempt: 1,
				ScheduledAt: at(1), StartedAt: at(1), FinishedAt: at(2), Join: orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED},
			{TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED, Pool: "main", OrkerId: "orker-1", Attempt: 2,
				ExitCode: 1, ScheduledAt: at(2), StartedAt: at(4), FinishedAt: at(5), Error: deployErr},
			{TaskId: "rollback", Status: orkstackv1.Status_STATUS_RUNNING, Pool: "main", OrkerId: "orker-1", Attempt: 1,
				ScheduledAt: at(6), StartedAt: at(6), Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED},
		},
	})
	got := f.run("run", "get", runID).requireExit(t, exitOK)
	requireGolden(t, "run-get-failing.golden", got.stdout)
	got.requireContains(t, "RUNNING (failing)", "JOIN", "verify    none_failed", "rollback  any_failed")
	machine := f.run("--json", "run", "get", runID).requireExit(t, exitOK)
	machine.requireContains(t, `"join": "ANY_FAILED"`)
}

// handlerDocYAML is the task-run fixture's orkflow with a failure handler of
// produce: the same name, so --file may replay a run of it.
func handlerDocYAML(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "handler.ork.yaml")
	doc := taskRunFixtureYAML + `  - id: tidy
    uses: shell
    needs: [produce, side]
    join: any_failed
    with:
      op: exec
      command: ["sh", "-c", "cat /orkstack/input/task.json"]
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTaskRun_NeedFlagRules(t *testing.T) {
	fx := newTaskRunFixture(t)
	file := handlerDocYAML(t)
	rt := &fakeTaskRuntime{}

	// Only for a failure handler.
	notHandler := fx.runTaskRun(rt, "--file", file, "--task", "produce", "--need", "side=failed").requireExit(t, exitError)
	if !strings.Contains(notHandler.stderr, `--need applies to a failure handler only; task "produce" (join all_succeeded) is not one`) {
		t.Fatalf("stderr:\n%s", notHandler.stderr)
	}
	// Only a direct need.
	indirect := fx.runTaskRun(rt, "--file", file, "--task", "tidy", "--need", "consume=failed").requireExit(t, exitError)
	if !strings.Contains(indirect.stderr, `"consume" is not a direct need of "tidy"`) {
		t.Fatalf("stderr:\n%s", indirect.stderr)
	}
	// Without --from-run, every direct need.
	missing := fx.runTaskRun(rt, "--file", file, "--task", "tidy", "--need", "produce=failed").requireExit(t, exitError)
	if !strings.Contains(missing.stderr, "missing: side") {
		t.Fatalf("stderr:\n%s", missing.stderr)
	}
	bad := fx.runTaskRun(rt, "--file", file, "--task", "tidy", "--need", "produce=broken").requireExit(t, exitError)
	if !strings.Contains(bad.stderr, `invalid --need "produce=broken"`) {
		t.Fatalf("stderr:\n%s", bad.stderr)
	}
	both := fx.runTaskRun(rt, "--file", file, "--task", "tidy", "--render-only",
		"--need", "produce=failed", "--need", "side=succeeded").requireExit(t, exitOK)
	both.requireContains(t, `"policy": "any_failed"`, `"taskId": "produce"`, `"status": "FAILED"`, `"message": "supplied with --need"`)

	// With --from-run, --need overrides the recorded outcome.
	override := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--file", file, "--task", "tidy", "--render-only",
		"--need", "produce=failed").requireExit(t, exitOK)
	override.requireContains(t, `"status": "FAILED"`, `"taskId": "side"`)
	// And without a --need, a handler replays the run's own outcomes.
	recorded := fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--file", file, "--task", "tidy", "--render-only").
		requireExit(t, exitOK)
	recorded.requireContains(t, `"policy": "any_failed"`)
}
