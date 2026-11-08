package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A Dependabot pull request's full proof waits for the ci:proof label. `proof`
// is the required check and GitHub treats a skipped job as passing one, so the
// rules here hold four things: an unlabelled Dependabot pull request's `proof`
// fails and never skips; every other event's guard is what it was; proof.yml
// never takes `labeled`, which would cancel a running proof and report the
// check again on any label of any pull request; and only the one event that
// asks for a proof can report a check named `proof` from proof-label.yml.

// guardBeforeTheLabelGate is the guard as it stood before this rule, which
// must still decide every pull request Dependabot did not open.
const guardBeforeTheLabelGate = `github.event_name != 'pull_request' || (!github.event.pull_request.draft && (github.event.repository.private == false || contains(github.event.pull_request.labels.*.name, 'ci:proof')))`

// proofLabelGate is the one event proof-label.yml starts a proof on.
const proofLabelGate = `github.event.label.name == 'ci:proof' && github.event.pull_request.user.login == 'dependabot[bot]' && github.event.sender.type == 'User' && !github.event.pull_request.draft`

const (
	labelsContext = "github.event.pull_request.labels.*.name"
	authorContext = "github.event.pull_request.user.login"
	draftContext  = "github.event.pull_request.draft"
)

// prContext is a pull_request event's context. A boolean that is false is null,
// which the evaluator reads as GitHub reads false.
func prContext(author, labels string, draft, private bool) map[string]string {
	ctx := map[string]string{"github.event_name": "pull_request", authorContext: author, labelsContext: labels}
	if draft {
		ctx[draftContext] = "true"
	}
	if private {
		ctx["github.event.repository.private"] = "true"
	}
	return ctx
}

func mustEval(t *testing.T, expr string, ctx map[string]string) bool {
	t.Helper()
	got, err := evalGHExpr(expr, ctx)
	if err != nil {
		t.Fatalf("%q: %v", expr, err)
	}
	return got
}

// runStep runs a step's script with exactly the environment given.
func runStep(t *testing.T, script string, env map[string]string) (bool, string) {
	t.Helper()
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, isExit := err.(*exec.ExitError); !isExit {
			t.Fatal(err)
		}
	}
	return err == nil, string(out)
}

// guardStepEnv is the guard step's environment for one pull request, spelled as
// GitHub renders each expression: a boolean as true or false, the labels joined.
func guardStepEnv(author, labels string, draft, private bool) map[string]string {
	return map[string]string{
		"DRAFT": fmt.Sprint(draft), "PRIVATE": fmt.Sprint(private), "LABELS": labels, "AUTHOR": author,
	}
}

// TestWorkflows_ADependabotPullRequestWaitsForTheLabel is the whole table of
// pull requests, the guard every other job carries and the step that makes
// `proof` fail: they agree on every row, so a pull request that runs nothing
// never reaches a passing `proof`, and only a Dependabot row changed.
func TestWorkflows_ADependabotPullRequestWaitsForTheLabel(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	guardStep := w.Jobs["proof"].Steps[0]
	for _, author := range []string{"alice", "dependabot[bot]", "dependabot"} {
		for _, labels := range []string{"", "dependencies", "ci:proof", "dependencies,ci:proof", "ci:proof-not", "not-ci:proof"} {
			for _, draft := range []bool{false, true} {
				for _, private := range []bool{false, true} {
					name := fmt.Sprintf("author %s, labels %q, draft %v, private %v", author, labels, draft, private)
					ctx := prContext(author, labels, draft, private)
					labelled := labels == "ci:proof" || labels == "dependencies,ci:proof"
					want := !draft && labelled || !draft && !private && author != "dependabot[bot]"

					if got := mustEval(t, proofGuard, ctx); got != want {
						t.Errorf("%s: the guard says %v, want %v", name, got, want)
					}
					if !mustEval(t, normalizeIf(guardStep.If), ctx) {
						t.Errorf("%s: the guard step does not run", name)
					}
					passed, out := runStep(t, guardStep.Run, guardStepEnv(author, labels, draft, private))
					if passed != want {
						t.Errorf("%s: the guard step passed = %v, and the jobs run = %v, so `proof` would end on neither: %s", name, passed, want, out)
					}
					if author != "dependabot[bot]" {
						if before := mustEval(t, guardBeforeTheLabelGate, ctx); before != want {
							t.Errorf("%s: a pull request Dependabot did not open changed: %v, was %v", name, want, before)
						}
					}
					if author == "dependabot[bot]" && !draft && !labelled && !strings.Contains(out, "ci:proof") {
						t.Errorf("%s: the refusal does not name the label: %s", name, out)
					}
				}
			}
		}
	}
}

// TestWorkflows_TheLabelGateIsOnlyAPullRequestRule: a merge group, a validate
// push, a dispatch and a call run as they did, whoever authored the commit, and
// `proof`'s first step does not run for them.
func TestWorkflows_TheLabelGateIsOnlyAPullRequestRule(t *testing.T) {
	first := normalizeIf(loadWorkflow(t, "proof.yml").Jobs["proof"].Steps[0].If)
	for _, event := range []string{"merge_group", "push", "workflow_dispatch", "schedule"} {
		// A pull_request payload is not there on these events; the author row
		// shows that one would change nothing.
		for _, ctx := range []map[string]string{
			{"github.event_name": event},
			{"github.event_name": event, authorContext: "dependabot[bot]"},
		} {
			if !mustEval(t, proofGuard, ctx) {
				t.Errorf("%s (%v): the guard refuses", event, ctx)
			}
			if mustEval(t, first, ctx) {
				t.Errorf("%s: proof's guard step runs", event)
			}
		}
	}
}

// labelGateProblems holds proof-label.yml to what makes it safe: it fires on
// every label of every pull request, so nothing but the one event may start a
// proof or report a check named `proof`, and the check it reports says what the
// proof it ran said.
func labelGateProblems(w *wfFile) []string {
	var p []string
	if len(w.On) != 1 {
		p = append(p, fmt.Sprintf("proof-label.yml triggers on %v, want pull_request alone", w.On))
	}
	if types, ok := w.onTypes("pull_request"); !ok || strings.Join(types, ",") != "labeled" {
		p = append(p, fmt.Sprintf("pull_request types = %v (present %v), want [labeled]", types, ok))
	}
	if len(w.Jobs) != 2 || w.Jobs["prove"] == nil || w.Jobs["verdict"] == nil {
		return append(p, fmt.Sprintf("jobs = %v, want prove and verdict: a job named proof would report the required check on every label", w.jobNames()))
	}
	prove, verdict := w.Jobs["prove"], w.Jobs["verdict"]
	if prove.Uses != "./.github/workflows/proof.yml" {
		p = append(p, fmt.Sprintf("jobs.prove uses %q, want this commit's proof.yml", prove.Uses))
	}
	if fmt.Sprint(prove.With["tier"]) != "full" {
		p = append(p, fmt.Sprintf("jobs.prove runs tier %v, want the merge gate's, full", prove.With["tier"]))
	}
	if got := normalizeIf(prove.If); got != proofLabelGate {
		p = append(p, fmt.Sprintf("jobs.prove.if = %q, want %q", got, proofLabelGate))
	}
	// A called workflow's job never gets more than its caller grants, and
	// GitHub refuses to start one that asks for more.
	for perm, level := range map[string]string{"contents": "read", "actions": "write", "checks": "read"} {
		if prove.Permissions[perm] != level {
			p = append(p, fmt.Sprintf("jobs.prove.permissions.%s = %q, want %s", perm, prove.Permissions[perm], level))
		}
	}
	wantName := "(" + proofLabelGate + ") && 'proof' || 'no proof asked for'"
	if got := normalizeIf(verdict.Name); got != wantName {
		p = append(p, fmt.Sprintf("jobs.verdict.name = %q, want %q", got, wantName))
	}
	if got := normalizeIf(verdict.If); got != "always() && ("+proofLabelGate+")" {
		p = append(p, fmt.Sprintf("jobs.verdict.if = %q: without always() a failed proof skips it, and a skipped required job reports success", got))
	}
	if strings.Join(verdict.needs(), ",") != "prove" {
		p = append(p, fmt.Sprintf("jobs.verdict needs %v, want prove", verdict.needs()))
	}
	if len(verdict.Steps) != 1 || envString(verdict.Steps[0], "VERDICT") != "${{ needs.prove.outputs.proof_verdict }}" {
		p = append(p, "jobs.verdict does not read the verdict of the proof it needs from proof_verdict")
	}
	for _, s := range verdict.Steps {
		if isCheckout(s) || s.Uses != "" || s.ContinueOnError != nil {
			p = append(p, "jobs.verdict runs something other than its own step, or lets it fail")
		}
	}
	return p
}

func TestWorkflows_TheLabelGateHoldsItsShape(t *testing.T) {
	expectNoProblems(t, "proof-label.yml", labelGateProblems(loadWorkflow(t, "proof-label.yml")))
}

func TestWorkflows_TheLabelGateRefusesEachWeakening(t *testing.T) {
	expectEachMutationRefused(t, "proof-label.yml", labelGateProblems, []mutation{
		{"a job named proof", func(w *wfFile) { w.Jobs["proof"] = w.Jobs["verdict"]; delete(w.Jobs, "verdict") }},
		{"the verdict always named proof", func(w *wfFile) { w.Jobs["verdict"].Name = "proof" }},
		{"the verdict named proof by a looser test", func(w *wfFile) {
			w.Jobs["verdict"].Name = "${{ github.event.label.name == 'ci:proof' && 'proof' || 'no proof asked for' }}"
		}},
		{"the verdict skipped when the proof failed", func(w *wfFile) { w.Jobs["verdict"].If = "${{ " + proofLabelGate + " }}" }},
		{"any label starts a proof", func(w *wfFile) {
			w.Jobs["prove"].If = strings.ReplaceAll(w.Jobs["prove"].If, "github.event.label.name == 'ci:proof' && ", "")
		}},
		{"any author's pull request", func(w *wfFile) {
			w.Jobs["prove"].If = strings.ReplaceAll(w.Jobs["prove"].If, "github.event.pull_request.user.login == 'dependabot[bot]' && ", "")
		}},
		{"a bot's label counts", func(w *wfFile) {
			w.Jobs["prove"].If = strings.ReplaceAll(w.Jobs["prove"].If, "github.event.sender.type == 'User' && ", "")
		}},
		{"a draft is proved", func(w *wfFile) {
			w.Jobs["prove"].If = strings.ReplaceAll(w.Jobs["prove"].If, " && !github.event.pull_request.draft", "")
		}},
		{"unlabeled too", func(w *wfFile) {
			w.On["pull_request"] = map[string]any{"types": []any{"labeled", "unlabeled"}}
		}},
		{"also on a dispatch", func(w *wfFile) { w.On["workflow_dispatch"] = nil }},
		{"another tier", func(w *wfFile) { w.Jobs["prove"].With["tier"] = "nightly" }},
		{"another workflow", func(w *wfFile) { w.Jobs["prove"].Uses = "./.github/workflows/ci.yml" }},
		{"a caller that grants less than proof.yml asks", func(w *wfFile) { w.Jobs["prove"].Permissions["actions"] = "read" }},
		{"the verdict not of the proof", func(w *wfFile) {
			w.Jobs["verdict"].Steps[0].Env["VERDICT"] = "${{ needs.prove.result }}"
		}},
		{"the verdict checks out the candidate", func(w *wfFile) {
			w.Jobs["verdict"].Steps = append(w.Jobs["verdict"].Steps, wfStep{Uses: "actions/checkout@x"})
		}},
	})
}

// TestWorkflows_OnlyTheAskingEventReportsAProofCheck is the trap: GitHub treats
// a skipped job as passing a required check. Across every label event, a job
// that could be reported as `proof` runs and reads the proof's verdict, and on
// any other event it is neither named `proof` nor started. Removing a label does
// not fire this workflow at all.
func TestWorkflows_OnlyTheAskingEventReportsAProofCheck(t *testing.T) {
	w := loadWorkflow(t, "proof-label.yml")
	name, cond, proveIf := normalizeIf(w.Jobs["verdict"].Name), normalizeIf(w.Jobs["verdict"].If), normalizeIf(w.Jobs["prove"].If)
	asked := 0
	for _, label := range []string{"ci:proof", "dependencies", "ci:feature", "CI:PROOF", "ci:proof-not"} {
		for _, author := range []string{"dependabot[bot]", "alice", "dependabot"} {
			for _, sender := range []string{"User", "Bot"} {
				for _, draft := range []bool{false, true} {
					ctx := prContext(author, label, draft, false)
					ctx["github.event.label.name"] = label
					ctx["github.event.sender.type"] = sender
					rendered, err := evalGHString(name, ctx)
					if err != nil {
						t.Fatal(err)
					}
					want := strings.EqualFold(label, "ci:proof") && author == "dependabot[bot]" && sender == "User" && !draft
					where := fmt.Sprintf("label %q, author %s, sender %s, draft %v", label, author, sender, draft)
					if got := rendered == "proof"; got != want {
						t.Errorf("%s: the job is named %q; want proof = %v", where, rendered, want)
					}
					if got := mustEval(t, cond, ctx); got != want {
						t.Errorf("%s: the job runs = %v, want %v", where, got, want)
					}
					if got := mustEval(t, proveIf, ctx); got != want {
						t.Errorf("%s: the proof starts = %v, want %v", where, got, want)
					}
					if want {
						asked++
					}
				}
			}
		}
	}
	if asked != 2 {
		t.Errorf("%d events asked for a proof, want the label in each spelling GitHub compares alike", asked)
	}
}

// TestWorkflows_TheLabelGatesVerdictIsTheProofs: the check passes only when the
// proof's own last step did. A run whose proof failed, was cancelled, never got
// there or left nothing fails, and so does one that says something else.
func TestWorkflows_TheLabelGatesVerdictIsTheProofs(t *testing.T) {
	script := loadWorkflow(t, "proof-label.yml").Jobs["verdict"].Steps[0].Run
	for verdict, want := range map[string]bool{
		"success": true, "failure": false, "cancelled": false, "skipped": false, "": false, "successful": false, "success ": false,
	} {
		if passed, out := runStep(t, script, map[string]string{"VERDICT": verdict}); passed != want {
			t.Errorf("VERDICT=%q: passed = %v, want %v: %s", verdict, passed, want, out)
		}
	}
}

// TestWorkflows_ProofYMLSaysItsVerdictToACaller: the verdict is the last step's
// outcome, carried out of the job and the called workflow. It is an output, not
// the called job's result, because an excused runner-size miss leaves a shard
// red while the proof passes (docs/PROOFS.md §12).
func TestWorkflows_ProofYMLSaysItsVerdictToACaller(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	steps := w.Jobs["proof"].Steps
	last := steps[len(steps)-1]
	if last.Name != "verdict" || last.ID != "verdict" {
		t.Errorf("jobs.proof's last step is %q with id %q, want the verdict, id verdict", last.Name, last.ID)
	}
	if got := w.Jobs["proof"].Outputs["verdict"]; got != "${{ steps.verdict.outcome }}" {
		t.Errorf("jobs.proof.outputs.verdict = %q, want the verdict step's outcome", got)
	}
	call, _ := w.On["workflow_call"].(map[string]any)
	outputs, _ := call["outputs"].(map[string]any)
	output, _ := outputs["proof_verdict"].(map[string]any)
	if got := fmt.Sprint(output["value"]); got != "${{ jobs.proof.outputs.verdict }}" {
		t.Errorf("workflow_call's proof_verdict = %q, want the proof job's verdict", got)
	}
}

// TestWorkflows_NothingElseReportsAProofCheck: `proof` is reported by proof.yml's
// proof job and proof-label.yml's verdict, and by no other job of any workflow.
func TestWorkflows_NothingElseReportsAProofCheck(t *testing.T) {
	for _, file := range allWorkflows {
		w := loadWorkflow(t, file)
		for _, id := range w.jobNames() {
			named := w.Jobs[id].Name
			if named == "" {
				named = id
			}
			if !strings.Contains(named, "proof") || (file == "proof.yml" && id == "proof") || (file == "proof-label.yml" && id == "verdict") {
				continue
			}
			if named == "proof" || strings.Contains(named, "'proof'") {
				t.Errorf("%s: job %s reports a check named proof", file, id)
			}
		}
	}
}

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

// TestDependabot_NeverAddsTheLabel: Dependabot adds the labels its configuration
// names. The label is someone's decision to spend a proof, so Dependabot may not
// add it for them, whatever else its entries label.
func TestDependabot_NeverAddsTheLabel(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "dependabot.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Updates []struct {
			Ecosystem string   `yaml:"package-ecosystem"`
			Labels    []string `yaml:"labels"`
		} `yaml:"updates"`
	}
	if err := yaml.Unmarshal(b, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Updates) == 0 {
		t.Fatal(".github/dependabot.yml has no updates")
	}
	for _, u := range config.Updates {
		for _, label := range u.Labels {
			if strings.EqualFold(label, "ci:proof") {
				t.Errorf("the %s entry adds the ci:proof label itself, so every one of its pull requests would start a proof", u.Ecosystem)
			}
		}
	}
}

// TestWorkflows_TheProofNamesNoSecret: a labelled Dependabot pull request's new
// head runs proof.yml as Dependabot, with no Actions secret, and the label's run
// runs a dependency's code with whatever the run has. The proof needs none.
func TestWorkflows_TheProofNamesNoSecret(t *testing.T) {
	for _, file := range []string{"proof.yml", "proof-label.yml"} {
		if text := workflowFile(t, file); strings.Contains(text, "secrets.") || strings.Contains(text, "secrets:") {
			t.Errorf("%s names a secret", file)
		}
	}
}
