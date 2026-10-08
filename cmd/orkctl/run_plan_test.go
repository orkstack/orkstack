package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// advancedDemoPath is relative on purpose: the file-form header prints the
// path it was given, and a relative one keeps the golden machine-independent.
const advancedDemoPath = "../../examples/advanced-demo.ork.yaml"

const planFailsDocument = `name: plan-fails
version: "1.0.0"
inputs:
  a:
    type: string
    default: "x"
  b:
    type: number
    default: 1
tasks:
  - id: first
    uses: shell
    with: {command: "echo"}
  - id: broken
    uses: shell
    needs: [first]
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {command: "echo"}
  - id: after
    uses: shell
    needs: [broken]
    with: {command: "echo"}
`

func writePlanFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func applyAdvancedDemo(t *testing.T, f *fixture) {
	t.Helper()
	f.run("apply", advancedDemoPath).requireExit(t, exitOK)
}

func TestRunPlanGolden(t *testing.T) {
	f := newFixture(t)
	applyAdvancedDemo(t, f)

	human := f.run("run", "plan", "advanced-demo", "--input", "enable_secret_check=false").requireExit(t, exitOK)
	requireGolden(t, "run-plan.golden", human.stdout)

	machine := f.run("run", "plan", "advanced-demo", "--input", "enable_secret_check=false", "--json").requireExit(t, exitOK)
	requireGolden(t, "run-plan.json.golden", machine.stdout)
	for _, fragment := range []string{`"outcome": "RUNS"`, `"outcome": "SKIPPED"`} {
		if !strings.Contains(machine.stdout, fragment) {
			t.Errorf("--json output lacks %s", fragment)
		}
	}
}

func TestRunPlanFileGolden(t *testing.T) {
	f := newFixture(t)
	out := f.run("run", "plan", "--file", advancedDemoPath).requireExit(t, exitOK)
	requireGolden(t, "run-plan-file.golden", out.stdout)
	if !strings.Contains(out.stdout, "(from "+advancedDemoPath+", not applied)") {
		t.Errorf("file-form header does not say it was not applied:\n%s", out.stdout)
	}
	f.run("orkflow", "get", "advanced-demo").requireExit(t, exitError)
}

func TestRunPlanFailsGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "plan-fails.ork.yaml", planFailsDocument)
	f.run("apply", path).requireExit(t, exitOK)
	out := f.run("run", "plan", "plan-fails").requireExit(t, exitOK)
	requireGolden(t, "run-plan-fails.golden", out.stdout)
}

func TestRunPlan_AssertionsSetTheExitCode(t *testing.T) {
	f := newFixture(t)
	applyAdvancedDemo(t, f)

	failed := f.run("run", "plan", "advanced-demo", "--assert-runs", "guarded").requireExit(t, exitPlanAssertionFailed)
	requireGolden(t, "run-plan-assert-failed.golden", failed.stdout)
	wantLine := `assert-runs guarded: planned SKIPPED (condition "{{ .inputs.enable_secret_check }}" evaluated false)`
	if !strings.Contains(failed.stderr, wantLine+"\n") {
		t.Fatalf("stderr does not carry the assertion line %q:\n%s", wantLine, failed.stderr)
	}

	f.run("run", "plan", "advanced-demo", "--assert-skipped", "warmup").requireExit(t, exitPlanAssertionFailed)
	f.run("run", "plan", "advanced-demo", "--assert-runs", "warmup", "--assert-skipped", "guarded").requireExit(t, exitOK)
	unknown := f.run("run", "plan", "advanced-demo", "--assert-runs", "nope").requireExit(t, exitError)
	if !strings.Contains(unknown.stderr, `task "nope" is not in this orkflow`) {
		t.Fatalf("unknown assertion stderr:\n%s", unknown.stderr)
	}
	// Assertions apply under --json too, with nothing but JSON on stdout.
	jsonFailed := f.run("run", "plan", "advanced-demo", "--json", "--assert-runs", "guarded").requireExit(t, exitPlanAssertionFailed)
	if !strings.HasPrefix(jsonFailed.stdout, "{") || strings.Contains(jsonFailed.stdout, "assert-runs") {
		t.Fatalf("--json stdout carries more than the response:\n%s", jsonFailed.stdout)
	}
}

func TestRunPlan_OutputFlagsBuildAssumedOutputs(t *testing.T) {
	file := writePlanFile(t, "outputs.json", `{"flaky": {"exitCode": 1}, "warmup": {"ok": true}}`)
	got, err := buildAssumedOutputs([]string{`flaky={"exitCode": 0}`}, file)
	if err != nil {
		t.Fatalf("buildAssumedOutputs: %v", err)
	}
	if code := got["flaky"].GetData().GetFields()["exitCode"].GetNumberValue(); code != 0 {
		t.Errorf("flaky exitCode = %v, want the --output pair's 0", code)
	}
	if !got["warmup"].GetData().GetFields()["ok"].GetBoolValue() {
		t.Errorf("warmup from the file lost: %v", got["warmup"])
	}
	if len(got) != 2 {
		t.Errorf("assumed outputs = %v, want two", got)
	}

	if _, err := buildAssumedOutputs([]string{`flaky=[1]`}, ""); err == nil ||
		err.Error() != `invalid --output "flaky=[1]": value must be a JSON object` {
		t.Errorf("non-object pair = %v", err)
	}
	if _, err := buildAssumedOutputs([]string{`flaky`}, ""); err == nil {
		t.Error("a pair without = was accepted")
	}
	malformed := writePlanFile(t, "bad.json", `{"flaky": `)
	if _, err := buildAssumedOutputs(nil, malformed); err == nil || !strings.Contains(err.Error(), malformed) {
		t.Errorf("malformed file = %v, want an error naming %s", err, malformed)
	}
	nonObject := writePlanFile(t, "list.json", `{"flaky": 3}`)
	if _, err := buildAssumedOutputs(nil, nonObject); err == nil || !strings.Contains(err.Error(), `"flaky"`) {
		t.Errorf("non-object file value = %v", err)
	}
	if none, err := buildAssumedOutputs(nil, ""); err != nil || none != nil {
		t.Errorf("no flags = %v, %v, want nil, nil", none, err)
	}
}

func TestRunPlan_FileAndNameAreExclusive(t *testing.T) {
	f := newFixture(t)
	both := f.run("run", "plan", "advanced-demo", "--file", advancedDemoPath).requireExit(t, exitError)
	neither := f.run("run", "plan").requireExit(t, exitError)
	withVersion := f.run("run", "plan", "--file", advancedDemoPath, "--version", "1").requireExit(t, exitError)
	if !strings.Contains(both.stderr, "exactly one of <orkflow> and --file") ||
		!strings.Contains(neither.stderr, "exactly one of <orkflow> and --file") {
		t.Errorf("exclusivity stderr: %q / %q", both.stderr, neither.stderr)
	}
	if !strings.Contains(withVersion.stderr, "orkflow_version cannot be set with yaml") {
		t.Errorf("--version with --file stderr: %q", withVersion.stderr)
	}
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("requests crossed the wire before the refusal: %v", got)
	}
}

func TestRunPlan_NeverSubmits(t *testing.T) {
	f := newFixture(t)
	applyAdvancedDemo(t, f)
	outputs := writePlanFile(t, "outputs.json", `{"flaky": {"exitCode": 0}}`)
	f.run("run", "plan", "advanced-demo", "--version", "1.0.1",
		"--input", "enable_secret_check=true", "--input-json", `{"log_lines": 3}`,
		"--output", `warmup={"ok": true}`, "--outputs-file", outputs,
		"--assert-runs", "summarize", "--assert-skipped", "guarded").requireExit(t, exitPlanAssertionFailed)
	f.run("run", "plan", "--file", advancedDemoPath, "--json").requireExit(t, exitOK)

	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a plan started runs: %v", started)
	}
	t.Logf("C7 stubStarter.started after every plan flag: %v", f.starter.startedRuns())
	list := f.run("run", "list", "--json").requireExit(t, exitOK)
	if strings.Contains(list.stdout, `"id"`) {
		t.Fatalf("run list shows a run after plans:\n%s", list.stdout)
	}
	for _, path := range f.requests() {
		if strings.Contains(path, "SubmitRun") {
			t.Fatalf("a plan reached SubmitRun: %v", f.requests())
		}
	}
}

// planRows puts wave-0 rows last and otherwise keeps wave, then declaration, order.
func TestRunPlan_RowsOrderWaveThenDeclarationWithWaveZeroLast(t *testing.T) {
	plan := &orkstackv1.PlanRunResponse{Tasks: []*orkstackv1.PlanTask{
		{TaskId: "late", Wave: 2},
		{TaskId: "failed", Wave: 0, Outcome: orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS},
		{TaskId: "early", Wave: 1},
		{TaskId: "also-late", Wave: 2},
		{TaskId: "gone", Wave: 0, Outcome: orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED},
	}}
	var got []string
	for _, row := range planRows(plan) {
		got = append(got, row.GetTaskId())
	}
	want := []string{"early", "late", "also-late", "failed", "gone"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("planRows = %v, want %v", got, want)
	}
}
