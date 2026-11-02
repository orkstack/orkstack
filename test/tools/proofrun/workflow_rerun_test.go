package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

// .github/workflows/proof-rerun.yml runs a completed proof again, automatically,
// when nothing in it recorded a result (proofrun rerun-plan decides, rerunplan.go).
// A rerun is a request to GitHub for a new attempt; the merger judges the
// attempts, so the workflow can make a proof ineligible by a wrong rerun and can
// never pass one. What it must never do is run on anything but a proof of a
// candidate branch of this repository or a nightly of its default branch, run a
// stranger's code with its token, or run more than twice for one run. These
// rules pin that shape, each with the weakenings it must refuse.

const proofRerunFile = "proof-rerun.yml"

// rerunGuardCase is one completed run the job's `if:` is asked about, as the
// event's context describes it, and whether the job may start for it. A proof
// of a candidate is a push to validate/full/**; a nightly is a schedule or a
// dispatch on the default branch, the only runs whose proof is main's nightly
// (docs/PROOFS.md §6). Each refused case differs from the first admitted one
// by one thing a stranger, a fork, a feature branch or a cap could change.
type rerunGuardCase struct {
	name  string
	set   map[string]string // the context, over the admitted candidate run's
	admit bool
}

func rerunGuardCases() []rerunGuardCase {
	const (
		path     = "github.event.workflow_run.path"
		event    = "github.event.workflow_run.event"
		branch   = "github.event.workflow_run.head_branch"
		attempt  = "github.event.workflow_run.run_attempt"
		proof    = ".github/workflows/proof.yml"
		nightly  = ".github/workflows/nightly.yml"
		validate = "validate/full/some-change"
	)
	return []rerunGuardCase{
		{"a candidate's failed proof, on its first attempt", nil, true},
		{"a candidate's failed proof, on the last attempt the cap allows", map[string]string{attempt: fmt.Sprint(defaultMaxRunAttempts - 1)}, true},
		{"a nightly's schedule on the default branch", map[string]string{path: nightly, event: "schedule", branch: "main"}, true},
		{"a nightly's dispatch on the default branch", map[string]string{path: nightly, event: "workflow_dispatch", branch: "main"}, true},

		{"the switch off", map[string]string{"vars.PROOF_AUTO_RERUN": ""}, false},
		{"the switch on for something else", map[string]string{"vars.PROOF_AUTO_RERUN": "yes"}, false},
		{"a proof that passed", map[string]string{"github.event.workflow_run.conclusion": "success"}, false},
		{"a proof that was cancelled", map[string]string{"github.event.workflow_run.conclusion": "cancelled"}, false},
		{"a fork's run", map[string]string{"github.event.workflow_run.head_repository.full_name": "stranger/r"}, false},
		{"the attempt that reaches the cap", map[string]string{attempt: fmt.Sprint(defaultMaxRunAttempts)}, false},
		{"an attempt past the cap", map[string]string{attempt: fmt.Sprint(defaultMaxRunAttempts + 1)}, false},
		{"a proof of a feature branch", map[string]string{branch: "feature/x"}, false},
		{"a proof of the default branch", map[string]string{branch: "main"}, false},
		{"a proof of a branch that only starts like validate", map[string]string{branch: "validate/other"}, false},
		{"a proof run by a pull request", map[string]string{event: "pull_request"}, false},
		{"a proof run by a dispatch", map[string]string{event: "workflow_dispatch"}, false},
		{"another workflow's run on a candidate branch", map[string]string{path: ".github/workflows/ci.yml"}, false},
		{"a run of nothing but a candidate's branch and event", map[string]string{path: ""}, false},
		{"a nightly of a feature branch", map[string]string{path: nightly, event: "schedule", branch: "feature/x"}, false},
		{"a nightly pushed", map[string]string{path: nightly, event: "push", branch: "main"}, false},
		{"a nightly of a pull request", map[string]string{path: nightly, event: "pull_request", branch: "main"}, false},
		{"a nightly with the candidate's branch and event", map[string]string{path: nightly, event: "push", branch: validate}, false},
		{"the candidate's workflow on a schedule", map[string]string{path: proof, event: "schedule", branch: "main"}, false},
		{"another workflow on a schedule", map[string]string{path: ".github/workflows/ci.yml", event: "schedule", branch: "main"}, false},
		{"a run of an event the guard does not name", map[string]string{event: "workflow_run"}, false},
		// An `||` the guard grew outside its terms admits what they refuse.
		{"every term failing, from a dispatch", map[string]string{
			"vars.PROOF_AUTO_RERUN": "", "github.event.workflow_run.conclusion": "success",
			"github.event.workflow_run.head_repository.full_name": "stranger/r", attempt: "9",
			path: ".github/workflows/ci.yml", event: "pull_request", branch: "feature/x", "github.event_name": "workflow_dispatch",
		}, false},
	}
}

// rerunGuardContext is the context of the first admitted case: a candidate's
// failed proof, pushed to a validate/full branch of this repository.
func rerunGuardContext(over map[string]string) map[string]string {
	ctx := map[string]string{
		"vars.PROOF_AUTO_RERUN":                               "true",
		"github.repository":                                   "o/r",
		"github.event.repository.default_branch":              "main",
		"github.event_name":                                   "workflow_run",
		"github.event.workflow_run.conclusion":                "failure",
		"github.event.workflow_run.head_repository.full_name": "o/r",
		"github.event.workflow_run.run_attempt":               "1",
		"github.event.workflow_run.path":                      ".github/workflows/proof.yml",
		"github.event.workflow_run.event":                     "push",
		"github.event.workflow_run.head_branch":               "validate/full/some-change",
	}
	for k, v := range over {
		ctx[k] = v
	}
	return ctx
}

func proofRerunProblems(w *wfFile) []string {
	var p []string
	// The one trigger: a completed proof or nightly, nothing a stranger can
	// start. The nightly calls proof.yml as a reusable workflow, which raises
	// no workflow_run of its own.
	if len(w.On) != 1 {
		p = append(p, fmt.Sprintf("proof-rerun.yml has %d triggers, want workflow_run alone", len(w.On)))
	}
	run, ok := w.On["workflow_run"].(map[string]any)
	if !ok {
		return append(p, "proof-rerun.yml does not trigger on workflow_run")
	}
	if got := fmt.Sprint(run["workflows"]); got != "[proof nightly]" {
		p = append(p, fmt.Sprintf("workflow_run.workflows = %s, want [proof, nightly]", got))
	}
	if types, _ := w.onTypes("workflow_run"); strings.Join(types, ",") != "completed" {
		p = append(p, fmt.Sprintf("workflow_run.types = %v, want [completed]", types))
	}
	if len(run) != 2 {
		p = append(p, fmt.Sprintf("workflow_run has keys %v, want workflows and types alone (a branch filter here would hide the guard below)", run))
	}

	// The token: read everywhere, written only for the one job, and only to
	// re-run workflow runs.
	if len(w.Permissions) != 1 || w.Permissions["contents"] != "read" {
		p = append(p, fmt.Sprintf("top-level permissions = %v, want contents: read alone", w.Permissions))
	}
	if len(w.Jobs) != 1 || w.Jobs["rerun"] == nil {
		return append(p, fmt.Sprintf("proof-rerun.yml has jobs %v, want rerun alone (never one named proof, the required check)", w.jobNames()))
	}
	job := w.Jobs["rerun"]
	want := map[string]string{"actions": "write", "contents": "read", "checks": "read"}
	if len(job.Permissions) != len(want) {
		p = append(p, fmt.Sprintf("jobs.rerun.permissions = %v, want exactly %v", job.Permissions, want))
	}
	for perm, lvl := range want {
		if job.Permissions[perm] != lvl {
			p = append(p, fmt.Sprintf("jobs.rerun.permissions.%s = %q, want %q", perm, job.Permissions[perm], lvl))
		}
	}

	// The guard: every term, ANDed, no `||` that would admit what the rest
	// excludes, and the attempt cap the planner's own default.
	cond := normalizeIf(job.If)
	if or, balanced := topLevelOr(cond); or || !balanced {
		p = append(p, fmt.Sprintf("jobs.rerun.if = %q has a top-level ||, or unbalanced parentheses", job.If))
	}
	// What the guard admits, run by run, rather than how it is written. The
	// cap is the planner's own default: at most two automatic reruns.
	for _, c := range rerunGuardCases() {
		got, err := evalGHExpr(cond, rerunGuardContext(c.set))
		if err != nil {
			p = append(p, fmt.Sprintf("jobs.rerun.if cannot be evaluated: %v", err))
			break
		}
		if got != c.admit {
			verb := "refuses"
			if !c.admit {
				verb = "admits"
			}
			p = append(p, fmt.Sprintf("jobs.rerun.if %s %s", verb, c.name))
		}
	}
	if strings.Contains(cond, "!=") {
		p = append(p, "jobs.rerun.if compares with !=, which admits what it does not name")
	}

	// A group per run, never cancelled: two completions of one run cannot
	// both ask for a rerun, and a rerun in flight is not torn down.
	group := fmt.Sprint(w.Concurrency["group"])
	if !strings.Contains(group, "github.event.workflow_run.id") {
		p = append(p, fmt.Sprintf("concurrency group %q is not per run", group))
	}
	if got := fmt.Sprint(w.Concurrency["cancel-in-progress"]); got != "false" {
		p = append(p, fmt.Sprintf("cancel-in-progress = %s, want false", got))
	}

	// The checkout: the default branch, never the run's own commit, with no
	// credential left on disk; the repository's code that runs is the default
	// branch's.
	checkouts := 0
	for i, s := range job.Steps {
		if !isCheckout(s) {
			continue
		}
		checkouts++
		if ref := withString(s, "ref"); ref != "${{ github.event.repository.default_branch }}" {
			p = append(p, fmt.Sprintf("jobs.rerun %s checks out %q, want the default branch", s.label(i), ref))
		}
		if withString(s, "persist-credentials") != "false" {
			p = append(p, fmt.Sprintf("jobs.rerun %s does not set persist-credentials: false", s.label(i)))
		}
	}
	if checkouts != 1 {
		p = append(p, fmt.Sprintf("jobs.rerun has %d checkouts, want one", checkouts))
	}
	for i, s := range job.Steps {
		for _, text := range []string{s.Run, withString(s, "ref"), withString(s, "repository"), withString(s, "path")} {
			if strings.Contains(text, "workflow_run.head_") || strings.Contains(text, "pull_request") {
				p = append(p, fmt.Sprintf("jobs.rerun %s reaches for the run's own head (%q)", s.label(i), text))
			}
		}
		if strings.Contains(s.Run, "${{") {
			p = append(p, fmt.Sprintf("jobs.rerun %s splices an expression into its shell; pass it through env:", s.label(i)))
		}
	}

	// The planner step: the event values it reads reach it through env, its
	// output is the step's output, and nothing else decides.
	var plan, rerun *wfStep
	for i := range job.Steps {
		switch s := &job.Steps[i]; {
		case strings.Contains(s.Run, "proofrun rerun-plan"):
			plan = s
		case strings.Contains(s.Run, "gh run rerun"):
			rerun = s
		}
	}
	if plan == nil {
		return append(p, "jobs.rerun never runs `proofrun rerun-plan`")
	}
	if plan.ID != "plan" {
		p = append(p, fmt.Sprintf("the planner step has id %q, want plan", plan.ID))
	}
	for key, val := range map[string]string{
		"REPO": "${{ github.repository }}", "RUN_ID": "${{ github.event.workflow_run.id }}",
		"RUN_ATTEMPT": "${{ github.event.workflow_run.run_attempt }}", "GH_TOKEN": "${{ github.token }}",
	} {
		if got := envString(*plan, key); got != val {
			p = append(p, fmt.Sprintf("the planner step's env %s = %q, want %q", key, got, val))
		}
	}
	for _, flag := range []string{`-repo "$REPO"`, `-run "$RUN_ID"`, `-attempt "$RUN_ATTEMPT"`, `>>"$GITHUB_OUTPUT"`} {
		if !strings.Contains(plan.Run, flag) && !(flag == `>>"$GITHUB_OUTPUT"` && strings.Contains(plan.Run, `tee -a "$GITHUB_OUTPUT"`)) {
			p = append(p, fmt.Sprintf("the planner step does not pass %s", flag))
		}
	}
	if !strings.Contains(plan.Run, "set -euo pipefail") {
		p = append(p, "the planner step does not run under set -euo pipefail")
	}
	if strings.Contains(plan.Run, "-max-attempts") {
		p = append(p, "the planner step overrides -max-attempts; the cap is the workflow's and the planner's default")
	}

	// The rerun: only failed jobs, only when the planner said so, never the
	// whole run (a part that recorded a result would get a second attempt).
	if rerun == nil {
		return append(p, "jobs.rerun never runs `gh run rerun`")
	}
	if normalizeIf(rerun.If) != "steps.plan.outputs.rerun == 'true'" {
		p = append(p, fmt.Sprintf("the rerun step runs under %q, want only when the planner said rerun=true", rerun.If))
	}
	if !regexp.MustCompile(`(?m)^\s*gh run rerun "\$RUN_ID" --repo "\$REPO" --failed\s*$`).MatchString(rerun.Run) {
		p = append(p, "the rerun step is not exactly `gh run rerun \"$RUN_ID\" --repo \"$REPO\" --failed`")
	}
	if strings.Count(rerun.Run, "gh run rerun") != 1 {
		p = append(p, "the rerun step re-runs more than once")
	}
	for key, val := range map[string]string{"REPO": "${{ github.repository }}", "RUN_ID": "${{ github.event.workflow_run.id }}", "GH_TOKEN": "${{ github.token }}"} {
		if got := envString(*rerun, key); got != val {
			p = append(p, fmt.Sprintf("the rerun step's env %s = %q, want %q", key, got, val))
		}
	}
	// The planner comes first.
	pi, ri := -1, -1
	for i := range job.Steps {
		if &job.Steps[i] == plan {
			pi = i
		}
		if &job.Steps[i] == rerun {
			ri = i
		}
	}
	if pi > ri {
		p = append(p, "the rerun step runs before the planner")
	}
	return p
}

func TestWorkflows_TheAutomaticRerunIsGuardedToACandidatesCompletedProof(t *testing.T) {
	step := func(w *wfFile, contains string) *wfStep {
		for i := range w.Jobs["rerun"].Steps {
			if strings.Contains(w.Jobs["rerun"].Steps[i].Run, contains) {
				return &w.Jobs["rerun"].Steps[i]
			}
		}
		t.Fatalf("jobs.rerun has no step running %q", contains)
		return nil
	}
	dropTerm := func(term string) mutation {
		return mutation{"the guard without " + term, func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, term, "true", 1)
		}}
	}
	expectEachMutationRefused(t, proofRerunFile, proofRerunProblems, []mutation{
		dropTerm("vars.PROOF_AUTO_RERUN == 'true'"),
		dropTerm("github.event.workflow_run.conclusion == 'failure'"),
		dropTerm("github.event.workflow_run.event == 'push'"),
		dropTerm("startsWith(github.event.workflow_run.head_branch, 'validate/full/')"),
		dropTerm("github.event.workflow_run.head_repository.full_name == github.repository"),
		dropTerm("github.event.workflow_run.run_attempt < 3"),
		dropTerm("github.event.workflow_run.path == '.github/workflows/proof.yml'"),
		dropTerm("github.event.workflow_run.path == '.github/workflows/nightly.yml'"),
		dropTerm("github.event.workflow_run.head_branch == github.event.repository.default_branch"),
		{"every validate branch", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "'validate/full/'", "'validate/'", 1)
		}},
		{"a nightly from a push", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "github.event.workflow_run.event == 'schedule'", "github.event.workflow_run.event == 'push'", 1)
		}},
		{"a nightly of a pull request", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "github.event.workflow_run.event == 'workflow_dispatch'", "github.event.workflow_run.event == 'pull_request'", 1)
		}},
		{"a nightly of any branch", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "github.event.workflow_run.head_branch == github.event.repository.default_branch", "github.event.workflow_run.head_branch != ''", 1)
		}},
		{"the candidate's rule for the nightly's file", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "'.github/workflows/proof.yml'", "'.github/workflows/nightly.yml'", 1)
		}},
		{"the which-runs term OR-ed with a third", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "default_branch))", "default_branch) || github.event.workflow_run.event == 'workflow_run')", 1)
		}},
		{"a cap of four attempts", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "run_attempt < 3", "run_attempt < 4", 1)
		}},
		{"a guard OR-ed with a dispatch", func(w *wfFile) { w.Jobs["rerun"].If += " || github.event_name == 'workflow_dispatch'" }},
		{"a guard that names what it excludes", func(w *wfFile) { w.Jobs["rerun"].If += " && github.event.workflow_run.event != 'pull_request'" }},
		{"a second trigger", func(w *wfFile) { w.On["pull_request_target"] = map[string]any{} }},
		{"a push trigger", func(w *wfFile) { w.On["push"] = map[string]any{} }},
		{"the wrong workflow", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"ci"} }},
		{"a third workflow", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"proof", "nightly", "ci"} }},
		{"the candidate's proof alone", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"proof"} }},
		{"the release's gate", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"proof", "release"} }},
		{"more event types", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["types"] = []any{"completed", "requested"} }},
		{"a branch filter", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["branches"] = []any{"**"} }},
		{"workflow-level contents: write", func(w *wfFile) { w.Permissions["contents"] = "write" }},
		{"workflow-level actions: write", func(w *wfFile) { w.Permissions["actions"] = "write" }},
		{"no actions: write", func(w *wfFile) { delete(w.Jobs["rerun"].Permissions, "actions") }},
		{"contents: write", func(w *wfFile) { w.Jobs["rerun"].Permissions["contents"] = "write" }},
		{"no checks: read", func(w *wfFile) { delete(w.Jobs["rerun"].Permissions, "checks") }},
		{"pull-requests: write", func(w *wfFile) { w.Jobs["rerun"].Permissions["pull-requests"] = "write" }},
		{"a second job", func(w *wfFile) { w.Jobs["other"] = &wfJob{} }},
		{"the job named proof", func(w *wfFile) { w.Jobs["proof"] = w.Jobs["rerun"]; delete(w.Jobs, "rerun") }},
		{"a group per workflow", func(w *wfFile) { w.Concurrency["group"] = "proof-rerun" }},
		{"cancelling a rerun in flight", func(w *wfFile) { w.Concurrency["cancel-in-progress"] = true }},
		{"the checkout of the run's commit", func(w *wfFile) {
			for i := range w.Jobs["rerun"].Steps {
				if s := &w.Jobs["rerun"].Steps[i]; isCheckout(*s) {
					s.With["ref"] = "${{ github.event.workflow_run.head_sha }}"
				}
			}
		}},
		{"the checkout of the run's branch", func(w *wfFile) {
			for i := range w.Jobs["rerun"].Steps {
				if s := &w.Jobs["rerun"].Steps[i]; isCheckout(*s) {
					s.With["ref"] = "${{ github.event.workflow_run.head_branch }}"
				}
			}
		}},
		{"a checkout of the fork", func(w *wfFile) {
			for i := range w.Jobs["rerun"].Steps {
				if s := &w.Jobs["rerun"].Steps[i]; isCheckout(*s) {
					s.With["repository"] = "${{ github.event.workflow_run.head_repository.full_name }}"
				}
			}
		}},
		{"no ref", func(w *wfFile) {
			for i := range w.Jobs["rerun"].Steps {
				if s := &w.Jobs["rerun"].Steps[i]; isCheckout(*s) {
					delete(s.With, "ref")
				}
			}
		}},
		{"a checkout that keeps the token", func(w *wfFile) {
			for i := range w.Jobs["rerun"].Steps {
				if s := &w.Jobs["rerun"].Steps[i]; isCheckout(*s) {
					s.With["persist-credentials"] = true
				}
			}
		}},
		{"an expression in the shell", func(w *wfFile) {
			s := step(w, "proofrun rerun-plan")
			s.Run = strings.Replace(s.Run, `"$RUN_ID"`, `"${{ github.event.workflow_run.id }}"`, 1)
		}},
		{"the head branch in the shell", func(w *wfFile) {
			s := step(w, "gh run rerun")
			s.Run += "\necho ${{ github.event.workflow_run.head_branch }}"
		}},
		{"the run id not from env", func(w *wfFile) { delete(step(w, "proofrun rerun-plan").Env, "RUN_ID") }},
		{"the attempt not from env", func(w *wfFile) { delete(step(w, "proofrun rerun-plan").Env, "RUN_ATTEMPT") }},
		{"a planner that overrides the cap", func(w *wfFile) { step(w, "proofrun rerun-plan").Run += " -max-attempts 99" }},
		{"a planner whose output goes nowhere", func(w *wfFile) {
			s := step(w, "proofrun rerun-plan")
			s.Run = strings.Replace(s.Run, `| tee -a "$GITHUB_OUTPUT"`, "", 1)
		}},
		{"a planner that can fail silently", func(w *wfFile) {
			s := step(w, "proofrun rerun-plan")
			s.Run = strings.Replace(s.Run, "set -euo pipefail", "set -u", 1)
		}},
		{"a rerun on every completion", func(w *wfFile) { step(w, "gh run rerun").If = "" }},
		{"a rerun on any planner output", func(w *wfFile) { step(w, "gh run rerun").If = "steps.plan.outputs.reason" }},
		{"a rerun of every job", func(w *wfFile) {
			s := step(w, "gh run rerun")
			s.Run = strings.Replace(s.Run, " --failed", "", 1)
		}},
		{"a rerun before the planner", func(w *wfFile) {
			job := w.Jobs["rerun"]
			var plan, rerun, rest []wfStep
			for _, s := range job.Steps {
				switch {
				case strings.Contains(s.Run, "proofrun rerun-plan"):
					plan = append(plan, s)
				case strings.Contains(s.Run, "gh run rerun"):
					rerun = append(rerun, s)
				default:
					rest = append(rest, s)
				}
			}
			job.Steps = append(append(rest, rerun...), plan...)
		}},
		{"a rerun without the run id", func(w *wfFile) {
			s := step(w, "gh run rerun")
			s.Run = strings.Replace(s.Run, `"$RUN_ID" `, "", 1)
		}},
		{"two reruns", func(w *wfFile) { step(w, "gh run rerun").Run += "\ngh run rerun \"$RUN_ID\" --repo \"$REPO\" --failed" }},
	})
}

// The variable that turns the workflow on is not set by this change, and the
// workflow explains the one permission beyond the two a rerun needs.
func TestWorkflows_TheAutomaticRerunIsOptInAndExplainsItsChecksPermission(t *testing.T) {
	b, err := os.ReadFile(workflowPath(proofRerunFile))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, want := range []string{"PROOF_AUTO_RERUN", "checks: read", "annotations"} {
		if !strings.Contains(text, want) {
			t.Errorf("proof-rerun.yml does not mention %q in its comments", want)
		}
	}
	var comment []string
	for _, line := range strings.Split(text, "\n") {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "#") {
			comment = append(comment, trimmed)
		}
	}
	if joined := strings.Join(comment, "\n"); !strings.Contains(joined, "checks: read") || !strings.Contains(joined, "annotations") {
		t.Error("proof-rerun.yml's comments do not say why the job reads checks (check-run annotations)")
	}
}
