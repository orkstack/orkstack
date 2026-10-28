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
// candidate branch of this repository, run a stranger's code with its token, or
// run more than twice for one run. These rules pin that shape, each with the
// weakenings it must refuse.

const proofRerunFile = "proof-rerun.yml"

// rerunGuardTerms are the terms the job's `if:` ANDs together.
var rerunGuardTerms = []string{
	"vars.PROOF_AUTO_RERUN == 'true'",
	"github.event.workflow_run.conclusion == 'failure'",
	"github.event.workflow_run.event == 'push'",
	"startsWith(github.event.workflow_run.head_branch, 'validate/full/')",
	"github.event.workflow_run.head_repository.full_name == github.repository",
}

var rerunCapRE = regexp.MustCompile(`github\.event\.workflow_run\.run_attempt < ([0-9]+)`)

func proofRerunProblems(w *wfFile) []string {
	var p []string
	// The one trigger: a completed proof, nothing a stranger can start.
	if len(w.On) != 1 {
		p = append(p, fmt.Sprintf("proof-rerun.yml has %d triggers, want workflow_run alone", len(w.On)))
	}
	run, ok := w.On["workflow_run"].(map[string]any)
	if !ok {
		return append(p, "proof-rerun.yml does not trigger on workflow_run")
	}
	if got := fmt.Sprint(run["workflows"]); got != "[proof]" {
		p = append(p, fmt.Sprintf("workflow_run.workflows = %s, want [proof]", got))
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
	for _, term := range rerunGuardTerms {
		if !strings.Contains(cond, term) {
			p = append(p, fmt.Sprintf("jobs.rerun.if lacks %q", term))
		}
	}
	if m := rerunCapRE.FindStringSubmatch(cond); m == nil {
		p = append(p, "jobs.rerun.if has no cap on github.event.workflow_run.run_attempt")
	} else if m[1] != fmt.Sprint(defaultMaxRunAttempts) {
		p = append(p, fmt.Sprintf("jobs.rerun.if caps the run attempt below %s, and the planner's -max-attempts defaults to %d: at most two automatic reruns", m[1], defaultMaxRunAttempts))
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
		{"every validate branch", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "'validate/full/'", "'validate/'", 1)
		}},
		{"a cap of four attempts", func(w *wfFile) {
			w.Jobs["rerun"].If = strings.Replace(w.Jobs["rerun"].If, "run_attempt < 3", "run_attempt < 4", 1)
		}},
		{"a guard OR-ed with a dispatch", func(w *wfFile) { w.Jobs["rerun"].If += " || github.event_name == 'workflow_dispatch'" }},
		{"a guard that names what it excludes", func(w *wfFile) { w.Jobs["rerun"].If += " && github.event.workflow_run.event != 'pull_request'" }},
		{"a second trigger", func(w *wfFile) { w.On["pull_request_target"] = map[string]any{} }},
		{"a push trigger", func(w *wfFile) { w.On["push"] = map[string]any{} }},
		{"the wrong workflow", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"ci"} }},
		{"a second workflow", func(w *wfFile) { w.On["workflow_run"].(map[string]any)["workflows"] = []any{"proof", "ci"} }},
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
