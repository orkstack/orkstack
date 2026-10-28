package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The workflows are the merge gate's mechanism. These tests parse them as
// YAML and pin the shape the plan fixes (plan §2.8, §3): the real proof on
// pull requests, `always()` alone on the one required check, the draft and
// ci:proof guard on every other job, one commit checked out everywhere,
// per-job permissions compared by level, artifact-name outputs, a step
// timeout under every job timeout, exact pins, and no workflow input
// interpolated into a shell. Each rule is a function that returns its
// problems, so a companion test can show that it bites on a mutated copy.

// ---- the model --------------------------------------------------------------

type wfStep struct {
	Name    string         `yaml:"name"`
	ID      string         `yaml:"id"`
	If      string         `yaml:"if"`
	Uses    string         `yaml:"uses"`
	Run     string         `yaml:"run"`
	With    map[string]any `yaml:"with"`
	Env     map[string]any `yaml:"env"`
	Timeout any            `yaml:"timeout-minutes"`
	// ContinueOnError is read only so that a rule can refuse it.
	ContinueOnError any `yaml:"continue-on-error"`
}

func (s wfStep) label(i int) string {
	switch {
	case s.Name != "":
		return fmt.Sprintf("step %d (%s)", i+1, s.Name)
	case s.Uses != "":
		return fmt.Sprintf("step %d (%s)", i+1, s.Uses)
	}
	return fmt.Sprintf("step %d", i+1)
}

type wfJob struct {
	Needs       any               `yaml:"needs"`
	If          string            `yaml:"if"`
	Uses        string            `yaml:"uses"`
	With        map[string]any    `yaml:"with"`
	Timeout     any               `yaml:"timeout-minutes"`
	Permissions map[string]string `yaml:"permissions"`
	Concurrency map[string]any    `yaml:"concurrency"`
	Outputs     map[string]string `yaml:"outputs"`
	Steps       []wfStep          `yaml:"steps"`
	// ContinueOnError is read only so that a rule can refuse it.
	ContinueOnError any `yaml:"continue-on-error"`
}

func (j *wfJob) needs() []string {
	switch v := j.Needs.(type) {
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, n := range v {
			out = append(out, fmt.Sprint(n))
		}
		return out
	}
	return nil
}

type wfFile struct {
	On          map[string]any    `yaml:"on"`
	Permissions map[string]string `yaml:"permissions"`
	Concurrency map[string]any    `yaml:"concurrency"`
	Jobs        map[string]*wfJob `yaml:"jobs"`
}

func (w *wfFile) jobNames() []string {
	var out []string
	for name := range w.Jobs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// onTypes is the `types:` list of one trigger, and whether the trigger exists.
func (w *wfFile) onTypes(event string) ([]string, bool) {
	v, ok := w.On[event]
	if !ok {
		return nil, false
	}
	m, _ := v.(map[string]any)
	var out []string
	if list, ok := m["types"].([]any); ok {
		for _, t := range list {
			out = append(out, fmt.Sprint(t))
		}
	}
	return out, true
}

var allWorkflows = []string{"proof.yml", "ci.yml", "nightly.yml", "release.yml", "site.yml", "proof-rerun.yml"}

func workflowPath(name string) string {
	return filepath.Join("..", "..", "..", ".github", "workflows", name)
}

func workflowFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(workflowPath(name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// loadWorkflow parses one workflow afresh, so a test may mutate its copy.
func loadWorkflow(t *testing.T, name string) *wfFile {
	t.Helper()
	var w wfFile
	if err := yaml.Unmarshal([]byte(workflowFile(t, name)), &w); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(w.Jobs) == 0 {
		t.Fatalf("%s declares no jobs", name)
	}
	return &w
}

// compositeActions are the composite actions under .github/actions the rules
// cover, each read by compositeSteps.
var compositeActions = []string{"proof-host", "proof-fetch"}

// compositeSteps are one composite action's steps.
func compositeSteps(t *testing.T, name string) []wfStep {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "actions", name, "action.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var a struct {
		Runs struct {
			Steps []wfStep `yaml:"steps"`
		} `yaml:"runs"`
	}
	if err := yaml.Unmarshal(b, &a); err != nil {
		t.Fatalf("%s action: %v", name, err)
	}
	if len(a.Runs.Steps) == 0 {
		t.Fatalf("the %s action has no steps", name)
	}
	return a.Runs.Steps
}

// proofHostSteps are the proof-host composite action's steps.
func proofHostSteps(t *testing.T) []wfStep { return compositeSteps(t, "proof-host") }

func isCheckout(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/checkout@") }

// proofFetchAction is the composite action every proof.yml download of a
// run's own artifact goes through: two bounded attempts, logged
// (.github/actions/proof-fetch).
const proofFetchAction = "./.github/actions/proof-fetch"

// isArtifactFetch reports whether a step downloads one of the run's own
// artifacts: through proof-fetch (`artifact:` or `pattern:`), or through
// actions/download-artifact, which proof.yml no longer uses but the rules
// still recognise so a step that fell back to it is held to the same shape.
func isArtifactFetch(s wfStep) bool {
	if strings.HasPrefix(s.Uses, "actions/download-artifact@") {
		return true
	}
	return s.Uses == proofFetchAction && (withString(s, "artifact") != "" || withString(s, "pattern") != "")
}

// fetchedArtifact is the artifact name a fetch step downloads, or "" for a
// pattern download of several.
func fetchedArtifact(s wfStep) string {
	if s.Uses == proofFetchAction {
		return withString(s, "artifact")
	}
	return withString(s, "name")
}

func withString(s wfStep, key string) string {
	if s.With == nil {
		return ""
	}
	v, ok := s.With[key]
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func envString(s wfStep, key string) string {
	if s.Env == nil {
		return ""
	}
	v, ok := s.Env[key]
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// permLevel orders a GitHub permission level; -1 is a word GitHub does not
// accept.
func permLevel(s string) int {
	switch s {
	case "none":
		return 0
	case "read":
		return 1
	case "write":
		return 2
	}
	return -1
}

// timeoutMinutes reads a `timeout-minutes` value. An expression counts as the
// largest number it can yield, so a sum over expression-valued steps is never
// an underestimate.
func timeoutMinutes(v any) (int, bool, error) {
	switch x := v.(type) {
	case nil:
		return 0, false, nil
	case int:
		return x, true, nil
	case float64:
		return int(x), true, nil
	case string:
		s := strings.TrimSpace(x)
		if n, err := strconv.Atoi(s); err == nil {
			return n, true, nil
		}
		if strings.HasPrefix(s, "${{") && strings.HasSuffix(s, "}}") {
			max := -1
			for _, lit := range regexp.MustCompile(`\b[0-9]+\b`).FindAllString(s, -1) {
				n, _ := strconv.Atoi(lit)
				if n > max {
					max = n
				}
			}
			if max < 0 {
				return 0, true, fmt.Errorf("expression %q yields no number", s)
			}
			return max, true, nil
		}
		return 0, true, fmt.Errorf("%q is neither a number nor an expression", s)
	}
	return 0, true, fmt.Errorf("unreadable timeout %v", v)
}

func expectNoProblems(t *testing.T, what string, problems []string) {
	t.Helper()
	if len(problems) > 0 {
		t.Errorf("%s:\n  %s", what, strings.Join(problems, "\n  "))
	}
}

// mutation is one weakening a rule must refuse.
type mutation struct {
	name   string
	mutate func(w *wfFile)
}

func expectEachMutationRefused(t *testing.T, file string, rule func(*wfFile) []string, cases []mutation) {
	t.Helper()
	if p := rule(loadWorkflow(t, file)); len(p) > 0 {
		t.Fatalf("%s already fails the rule:\n  %s", file, strings.Join(p, "\n  "))
	}
	for _, c := range cases {
		w := loadWorkflow(t, file)
		c.mutate(w)
		if p := rule(w); len(p) == 0 {
			t.Errorf("%s: the rule accepts the weakening %q", file, c.name)
		}
	}
}

// ---- the required check -------------------------------------------------------

// proofGuard is the guard every proof.yml job but `proof` carries (plan §3.2):
// a draft runs nothing heavy, and while the repository is private a pull
// request needs the ci:proof label. A job's `if:` is the guard in parentheses
// alone, or a condition ANDed with it: `<prefix> && (<guard>)`, the prefix
// holding no `||` outside parentheses (which would bind looser than the AND).
const proofGuard = `github.event_name != 'pull_request' || (!github.event.pull_request.draft && (github.event.repository.private == false || contains(github.event.pull_request.labels.*.name, 'ci:proof')))`

// normalizeIf strips the expression delimiters and collapses whitespace.
func normalizeIf(cond string) string {
	c := strings.TrimSpace(cond)
	if strings.HasPrefix(c, "${{") && strings.HasSuffix(c, "}}") {
		c = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(c, "${{"), "}}"))
	}
	return strings.Join(strings.Fields(c), " ")
}

// topLevelOr reports whether an expression has `||` outside every
// parenthesis and quoted string, and whether its parentheses balance.
func topLevelOr(expr string) (or, balanced bool) {
	depth, quoted := 0, false
	for i := 0; i < len(expr); i++ {
		switch c := expr[i]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return or, false
			}
		case c == '|' && depth == 0 && i+1 < len(expr) && expr[i+1] == '|':
			or = true
		}
	}
	return or, depth == 0 && !quoted
}

// guardProblem is why a job's `if:` is not the proof guard, or "".
func guardProblem(cond string) string {
	c := normalizeIf(cond)
	g := "(" + proofGuard + ")"
	if c == g {
		return ""
	}
	if !strings.HasSuffix(c, " && "+g) {
		return "is not the guard, nor a condition ANDed with it as `<prefix> && (<guard>)`"
	}
	prefix := strings.TrimSuffix(c, " && "+g)
	if or, balanced := topLevelOr(prefix); or || !balanced {
		return "has a prefix with an unparenthesized `||`, which binds looser than the AND with the guard"
	}
	return ""
}

func proofCheckProblems(w *wfFile) []string {
	var p []string
	// A pull request triggers exactly these types: never `labeled`, whose
	// event would land in the concurrency group and cancel a running proof.
	want := []string{"opened", "ready_for_review", "reopened", "synchronize"}
	if types, ok := w.onTypes("pull_request"); !ok {
		p = append(p, "proof.yml does not trigger on pull_request")
	} else {
		got := append([]string(nil), types...)
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			p = append(p, fmt.Sprintf("pull_request types = %v, want exactly %v", types, want))
		}
	}
	if types, ok := w.onTypes("merge_group"); !ok || strings.Join(types, ",") != "checks_requested" {
		p = append(p, fmt.Sprintf("merge_group types = %v (present %v), want [checks_requested]", types, ok))
	}
	if _, ok := w.On["workflow_call"]; !ok {
		p = append(p, "proof.yml is not callable (workflow_call)")
	}

	proof, ok := w.Jobs["proof"]
	if !ok {
		return append(p, "proof.yml has no proof job")
	}
	// A skipped required job reports success: `always()` and nothing else.
	if cond := strings.TrimSpace(proof.If); cond != "always()" && cond != "${{ always() }}" {
		p = append(p, fmt.Sprintf("jobs.proof.if = %q, want always() alone", proof.If))
	}
	needs := map[string]bool{}
	for _, n := range proof.needs() {
		needs[n] = true
	}
	for _, name := range w.jobNames() {
		if name != "proof" && !needs[name] {
			p = append(p, "jobs.proof does not need "+name+": the merger could miss its part")
		}
	}
	for _, perm := range []string{"contents", "actions", "checks"} {
		if permLevel(proof.Permissions[perm]) < 1 {
			p = append(p, fmt.Sprintf("jobs.proof.permissions.%s = %q, want at least read", perm, proof.Permissions[perm]))
		}
	}
	// On a draft, or unlabelled while private, `proof` fails rather than skips.
	if len(proof.Steps) == 0 {
		p = append(p, "jobs.proof has no steps")
	} else {
		first := proof.Steps[0]
		if !strings.Contains(first.If, "github.event_name == 'pull_request'") ||
			!strings.Contains(first.Run, "a draft runs no full proof") ||
			!strings.Contains(first.Run, "the ci:proof label") ||
			!strings.Contains(first.Run, "exit 1") {
			p = append(p, "jobs.proof's first step does not fail a draft or an unlabelled private pull request")
		}
	}

	for _, name := range w.jobNames() {
		if name == "proof" {
			continue
		}
		job := w.Jobs[name]
		if why := guardProblem(job.If); why != "" {
			p = append(p, fmt.Sprintf("jobs.%s.if %s: %q", name, why, job.If))
		}
		if strings.Contains(job.If, "github.event_name == 'merge_group'") {
			p = append(p, fmt.Sprintf("jobs.%s narrows itself to merge_group: the pull request's proof would be trivial", name))
		}
	}

	// Every part tests one commit: every checkout names github.sha.
	checkouts := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !isCheckout(s) {
				continue
			}
			checkouts++
			if ref := withString(s, "ref"); ref != "${{ github.sha }}" {
				p = append(p, fmt.Sprintf("jobs.%s %s checks out %q, want ${{ github.sha }}", name, s.label(i), ref))
			}
		}
	}
	if checkouts == 0 {
		p = append(p, "proof.yml has no checkout")
	}

	// The queue mode's variable reaches the step that reads it.
	readers := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.Contains(s.Run, "MERGE_GATE_MODE") {
				continue
			}
			readers++
			if got := envString(s, "MERGE_GATE_MODE"); got != "${{ vars.MERGE_GATE_MODE }}" {
				p = append(p, fmt.Sprintf("jobs.%s %s reads $MERGE_GATE_MODE, and its env sets it to %q, not ${{ vars.MERGE_GATE_MODE }}", name, s.label(i), got))
			}
		}
	}
	if readers == 0 {
		p = append(p, "no step reads MERGE_GATE_MODE: the containment check has no queue-mode switch")
	}
	return p
}

// TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests is the shape the
// merge gate's precondition 5 runs (`make github-merge-gate`).
func TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests(t *testing.T) {
	expectNoProblems(t, "proof.yml", proofCheckProblems(loadWorkflow(t, "proof.yml")))
}

func TestWorkflows_TheProofCheckRefusesEachWeakening(t *testing.T) {
	firstCheckout := func(w *wfFile, job string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if isCheckout(w.Jobs[job].Steps[i]) {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no checkout", job)
		return nil
	}
	expectEachMutationRefused(t, "proof.yml", proofCheckProblems, []mutation{
		{"proof without if: always()", func(w *wfFile) { w.Jobs["proof"].If = "" }},
		{"proof skipped with plan", func(w *wfFile) { w.Jobs["proof"].If = "always() && needs.plan.result != 'skipped'" }},
		{"proof without actions: read", func(w *wfFile) { delete(w.Jobs["proof"].Permissions, "actions") }},
		{"proof with checks: none", func(w *wfFile) { w.Jobs["proof"].Permissions["checks"] = "none" }},
		{"proof does not need integration", func(w *wfFile) { w.Jobs["proof"].Needs = []any{"base", "plan", "build"} }},
		{"proof's draft step removed", func(w *wfFile) { w.Jobs["proof"].Steps = w.Jobs["proof"].Steps[1:] }},
		{"pull_request on labeled", func(w *wfFile) {
			w.On["pull_request"] = map[string]any{"types": []any{"opened", "synchronize", "reopened", "ready_for_review", "labeled"}}
		}},
		{"pull_request without ready_for_review", func(w *wfFile) {
			w.On["pull_request"] = map[string]any{"types": []any{"opened", "synchronize", "reopened"}}
		}},
		{"no merge_group", func(w *wfFile) { delete(w.On, "merge_group") }},
		{"base without the guard", func(w *wfFile) { w.Jobs["base"].If = "" }},
		{"plan without the guard", func(w *wfFile) { w.Jobs["plan"].If = "" }},
		{"build without the label", func(w *wfFile) { w.Jobs["build"].If = "${{ !github.event.pull_request.draft }}" }},
		{"integration only on merge_group", func(w *wfFile) {
			w.Jobs["integration"].If += " && github.event_name == 'merge_group'"
		}},
		{"a guard OR-ed in front", func(w *wfFile) {
			w.Jobs["fast"].If = "${{ inputs.tier == 'x' || (" + proofGuard + ") }}"
		}},
		{"a prefix whose || binds looser than the guard", func(w *wfFile) {
			w.Jobs["fast"].If = "${{ inputs.tier == 'x' || inputs.tier == 'y' && (" + proofGuard + ") }}"
		}},
		{"the guard's draft and label terms swapped", func(w *wfFile) {
			w.Jobs["plan"].If = "${{ (github.event_name != 'pull_request' || (github.event.repository.private == false || (!github.event.pull_request.draft && contains(github.event.pull_request.labels.*.name, 'ci:proof')))) }}"
		}},
		{"the guard ANDed with a looser branch", func(w *wfFile) {
			w.Jobs["build"].If = "${{ (" + proofGuard + ") || github.event_name == 'push' }}"
		}},
		{"a checkout without ref", func(w *wfFile) { delete(firstCheckout(w, "integration").With, "ref") }},
		{"a checkout of the head ref", func(w *wfFile) {
			firstCheckout(w, "plan").With["ref"] = "${{ github.event.pull_request.head.sha }}"
		}},
		{"MERGE_GATE_MODE not exported", func(w *wfFile) {
			for i := range w.Jobs["base"].Steps {
				delete(w.Jobs["base"].Steps[i].Env, "MERGE_GATE_MODE")
			}
		}},
	})
}

// TestWorkflows_ProofDoesNotTriggerOnLabels: a `labeled` event for any label
// would land in the pull request's concurrency group, cancel a running proof
// and report the required check again on the same head (plan §3.2).
func TestWorkflows_ProofDoesNotTriggerOnLabels(t *testing.T) {
	types, ok := loadWorkflow(t, "proof.yml").onTypes("pull_request")
	if !ok || len(types) == 0 {
		t.Fatal("proof.yml has no pull_request types")
	}
	if contains(types, "labeled") || contains(types, "unlabeled") {
		t.Errorf("proof.yml triggers on a label event: %v", types)
	}
}

// verdictArms parses the verdict step's `case "$tier" in` into its arms, keyed
// by pattern. ok is false when the step has no such case.
func verdictArms(run string) (arms map[string]string, ok bool) {
	arms = map[string]string{}
	armStart := regexp.MustCompile(`^\s*(full|release|nightly|\*)\)\s*$`)
	in, cur := false, ""
	for _, line := range strings.Split(run, "\n") {
		t := strings.TrimSpace(line)
		switch {
		case !in && t == `case "$tier" in`:
			in, ok = true, true
		case !in:
		case t == "esac":
			return arms, ok
		case cur == "" && armStart.MatchString(line):
			cur = strings.TrimSuffix(t, ")")
			arms[cur] = ""
		case cur != "" && t == ";;":
			cur = ""
		case cur != "":
			arms[cur] += line + "\n"
		}
	}
	return arms, ok
}

// verdictProblems holds the last step of `proof` to the plan's per-tier rule:
// a full proof must be merge-eligible, a release proof is judged by `proofrun
// verify -release`, the rule the tag applies, a nightly by its result and
// eligible_but_for_tier, and any other tier fails. The step runs whenever the
// job does and is never allowed to continue past a failure.
func verdictProblems(w *wfFile) []string {
	proof, ok := w.Jobs["proof"]
	if !ok || len(proof.Steps) == 0 {
		return []string{"jobs.proof has no steps"}
	}
	last := proof.Steps[len(proof.Steps)-1]
	var p []string
	if last.Name != "verdict" {
		return append(p, fmt.Sprintf("the last step of jobs.proof is %q, want the verdict", last.Name))
	}
	if last.If != "" {
		p = append(p, fmt.Sprintf("the verdict step runs under %q; it must run whenever the job does", last.If))
	}
	if last.ContinueOnError != nil {
		p = append(p, "the verdict step sets continue-on-error")
	}
	if !strings.Contains(last.Run, "set -euo pipefail") {
		p = append(p, "the verdict step does not run under set -euo pipefail")
	}
	if !strings.Contains(last.Run, `tier="$(jq -r .tier "$proof/proof.json")"`) {
		p = append(p, "the verdict step does not read the tier from proof.json")
	}
	arms, ok := verdictArms(last.Run)
	if !ok {
		return append(p, `the verdict step has no case "$tier" in`)
	}
	want := map[string][]string{
		"full":    {`jq -e '.result == "pass" and .merge_eligible == true' "$proof/proof.json"`},
		"release": {`go run ./test/tools/proofrun verify -release -proof "$proof"`},
		"nightly": {`jq -e '.result == "pass" and .eligible_but_for_tier == true' "$proof/proof.json"`},
		"*":       {"exit 1"},
	}
	for arm, body := range arms {
		if _, known := want[arm]; !known {
			p = append(p, fmt.Sprintf("the verdict case has an unexpected arm %q", arm))
		}
		for _, bad := range []string{"|| true", "|| :", "exit 0"} {
			if strings.Contains(body, bad) {
				p = append(p, fmt.Sprintf("the verdict's %s arm contains %q: it cannot fail", arm, bad))
			}
		}
	}
	for arm, parts := range want {
		body, present := arms[arm]
		if !present {
			p = append(p, fmt.Sprintf("the verdict case has no %s arm", arm))
			continue
		}
		for _, part := range parts {
			if !strings.Contains(body, part) {
				p = append(p, fmt.Sprintf("the verdict's %s arm lacks %q", arm, part))
			}
		}
	}
	if strings.Contains(arms["full"], "eligible_but_for_tier") {
		p = append(p, "the verdict's full arm accepts a proof that is merge-ineligible but for its tier")
	}
	return p
}

func TestWorkflows_TheVerdictJudgesEachTierByItsOwnRule(t *testing.T) {
	verdict := func(w *wfFile) *wfStep {
		steps := w.Jobs["proof"].Steps
		return &steps[len(steps)-1]
	}
	// swap rewrites within one arm only, so a mutation cannot be satisfied by
	// the same text sitting in another arm.
	swap := func(w *wfFile, arm, from, to string) {
		v := verdict(w)
		arms, _ := verdictArms(v.Run)
		body, ok := arms[arm]
		if !ok || !strings.Contains(body, from) {
			t.Fatalf("the verdict's %s arm has no %q to weaken", arm, from)
		}
		v.Run = strings.Replace(v.Run, body, strings.Replace(body, from, to, 1), 1)
	}
	expectEachMutationRefused(t, "proof.yml", verdictProblems, []mutation{
		{"verdict under a condition", func(w *wfFile) { verdict(w).If = "success()" }},
		{"verdict continues on error", func(w *wfFile) { verdict(w).ContinueOnError = true }},
		{"verdict not last", func(w *wfFile) {
			job := w.Jobs["proof"]
			job.Steps = append(job.Steps, wfStep{Name: "after", Run: "true"})
		}},
		{"full judged by its result alone", func(w *wfFile) {
			swap(w, "full", ` and .merge_eligible == true`, ``)
		}},
		{"full's merge_eligible moved to the nightly arm", func(w *wfFile) {
			swap(w, "full", `.result == "pass" and .merge_eligible == true`, `.result == "pass"`)
			swap(w, "nightly", `.eligible_but_for_tier == true`, `.merge_eligible == true and .eligible_but_for_tier == true`)
		}},
		{"full accepts a tier-ineligible proof", func(w *wfFile) {
			swap(w, "full", `.merge_eligible == true`, `.merge_eligible == true or .eligible_but_for_tier == true`)
		}},
		{"release judged by its result", func(w *wfFile) {
			swap(w, "release", `go run ./test/tools/proofrun verify -release -proof "$proof"`, `jq -e '.result == "pass"' "$proof/proof.json"`)
		}},
		{"release without -release", func(w *wfFile) {
			swap(w, "release", `verify -release -proof`, `verify -proof`)
		}},
		{"nightly judged by its result alone", func(w *wfFile) {
			swap(w, "nightly", ` and .eligible_but_for_tier == true`, ``)
		}},
		{"nightly without a failing exit", func(w *wfFile) {
			swap(w, "nightly", `jq -e '`, `jq '`)
		}},
		{"unknown tier passes", func(w *wfFile) { swap(w, "*", "exit 1", "exit 0") }},
		{"a verdict that cannot fail", func(w *wfFile) {
			swap(w, "full", `"$proof/proof.json"`, `"$proof/proof.json" || true`)
		}},
		{"a tier read from elsewhere", func(w *wfFile) {
			v := verdict(w)
			v.Run = strings.Replace(v.Run, `tier="$(jq -r .tier "$proof/proof.json")"`, `tier=full`, 1)
		}},
		{"no case", func(w *wfFile) {
			v := verdict(w)
			v.Run = strings.Replace(v.Run, `case "$tier" in`, `case "$other" in`, 1)
		}},
	})
}

// TestWorkflows_TheSummaryCarriesTheQuarantineWarnings: the seven-day
// quarantine WARNING reaches the job summary, where a reviewer reads it.
func TestWorkflows_TheSummaryCarriesTheQuarantineWarnings(t *testing.T) {
	for _, s := range loadWorkflow(t, "proof.yml").Jobs["proof"].Steps {
		if s.Name == "summary" {
			if !strings.Contains(s.Run, ".quarantine_warnings[]?") {
				t.Error("the summary step does not print proof.json's quarantine_warnings")
			}
			if s.If != "always()" {
				t.Errorf("the summary step runs under %q, want always()", s.If)
			}
			return
		}
	}
	t.Fatal("jobs.proof has no summary step")
}

// jobRecordProgram is the jq program the proof job's "job records" step runs
// on each of GitHub's job objects.
func jobRecordProgram(t *testing.T) string {
	t.Helper()
	for _, s := range loadWorkflow(t, "proof.yml").Jobs["proof"].Steps {
		if s.Name != "job records" {
			continue
		}
		const open = `--arg err "$err" '`
		i := strings.Index(s.Run, open)
		j := -1
		if i >= 0 {
			if k := strings.Index(s.Run[i:], `' <<<"$job"`); k >= 0 {
				j = i + k
			}
		}
		if i < 0 || j < 0 {
			t.Fatalf("the job records step has no per-job jq program:\n%s", s.Run)
		}
		for _, want := range []string{"check-runs/$check/annotations", "actions/jobs/$id/logs", "attempt-$n.json", "--paginate"} {
			if !strings.Contains(s.Run, want) {
				t.Errorf("the job records step lacks %q", want)
			}
		}
		return s.Run[i+len(open) : j]
	}
	t.Fatal("jobs.proof has no job records step")
	return ""
}

// TestWorkflows_JobRecordsCarryWhatTheMergerClassifies: the record the proof
// job writes for a part job decodes into proofrun's githubJob with its steps,
// annotations, log and read error, and a job whose runner GitHub lost is
// classified as lost — the one attempt that may run again. Without the
// annotations no lost runner is ever recognised.
func TestWorkflows_JobRecordsCarryWhatTheMergerClassifies(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is not installed")
	}
	program := jobRecordProgram(t)
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	logFile := write("log", "2026-10-21T00:00:00Z setting up\n")
	annFile := write("ann", `["The runner has received a shutdown signal. This can happen when the runner service is stopped."]`)
	job := `{"id":5,"name":"integration (3)","status":"completed","conclusion":"failure",` +
		`"check_run_url":"https://api.github.com/repos/o/r/check-runs/987",` +
		`"steps":[{"name":"Set up job","conclusion":"success"},{"name":"integration shard","conclusion":"failure"}]}`
	decode := func(errText string) githubJob {
		t.Helper()
		cmd := exec.Command(jq, "-c", "--rawfile", "log", logFile, "--slurpfile", "ann", annFile, "--arg", "err", errText, program)
		cmd.Stdin = strings.NewReader(job)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("the job records program failed: %v", err)
		}
		var a githubAttempt
		if err := json.Unmarshal([]byte(`{"attempt":1,"jobs":[`+strings.TrimSpace(string(out))+`]}`), &a); err != nil {
			t.Fatalf("the record does not decode as a githubAttempt: %v\n%s", err, out)
		}
		return a.Jobs[0]
	}

	j := decode("")
	if j.Name != "integration (3)" || partFromJobName(j.Name) != "integration:3" {
		t.Errorf("name = %q, which names no part", j.Name)
	}
	if j.CheckRunID != 987 || j.Conclusion != "failure" || j.FailedStep != "integration shard" {
		t.Errorf("record = %+v", j)
	}
	if len(j.Steps) != 2 || j.Steps[1] != (githubStep{Name: "integration shard", Conclusion: "failure"}) {
		t.Errorf("steps = %+v", j.Steps)
	}
	if len(j.Annotations) != 1 || !strings.Contains(j.Log, "setting up") || j.ReadError != "" {
		t.Errorf("annotations %q, log %q, read_error %q", j.Annotations, j.Log, j.ReadError)
	}
	if class, why := classify(&j, partReading{}); class != attemptLost {
		t.Errorf("a lost runner's record is classified %v (%s), want lost", class, why)
	}

	// The log is best effort: GitHub keeps none for a job whose runner it lost,
	// so a missing one must not read as an unreadable record.
	for _, s := range loadWorkflow(t, "proof.yml").Jobs["proof"].Steps {
		if s.Name != "job records" {
			continue
		}
		if strings.Contains(s.Run, "log: the API call failed") {
			t.Error("a failed log fetch sets read_error; a runner-lost job has no log")
		}
		if !strings.Contains(s.Run, `annotations: the API call failed`) || !strings.Contains(s.Run, `unreadable: listing attempt`) {
			t.Error("the job listing or the annotations fetch no longer sets read_error")
		}
		// Annotations are fetched as one JSON string per message and slurped, so
		// a multi-line message stays one element.
		m := regexp.MustCompile(`check-runs/\$check/annotations" --jq '([^']+)'`).FindStringSubmatch(s.Run)
		if m == nil || !strings.Contains(s.Run, `jq -s . "$work/ann-lines"`) || strings.Contains(s.Run, "jq -R") {
			t.Fatalf("the annotations are not fetched as JSON strings:\n%s", s.Run)
		}
		page := `[{"path":".github","annotation_level":"failure","message":"lost communication\nwith the server"},{"message":"second"}]`
		lines := exec.Command(jq, "-r", m[1])
		lines.Stdin = strings.NewReader(page)
		out, err := lines.Output()
		if err != nil {
			t.Fatalf("the annotation filter failed: %v", err)
		}
		slurp := exec.Command(jq, "-c", "-s", ".")
		slurp.Stdin = strings.NewReader(string(out))
		arr, err := slurp.Output()
		if err != nil {
			t.Fatalf("slurping the annotations failed: %v", err)
		}
		if got := strings.TrimSpace(string(arr)); got != `["lost communication\nwith the server","second"]` {
			t.Errorf("annotations = %s, want one element per message", got)
		}
	}

	j = decode("annotations: the API call failed")
	if j.ReadError == "" {
		t.Fatal("a failed fetch leaves no read_error")
	}
	if class, _ := classify(&j, partReading{}); class != attemptResult {
		t.Errorf("an unreadable record is classified %v, want a result (fails closed)", class)
	}
}

// The merger's step names (proofStepNames, setupStepNames) are pinned to
// proof.yml in workflow_setup_test.go.

// ---- a red part cannot turn green ------------------------------------------------

func setsContinueOnError(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return v != nil // an expression, or any word but a literal false
}

// continueOnErrorProblems refuses `continue-on-error` anywhere: a job or a
// step that continues past its failure turns a red proof into a green required
// check, and nothing in these workflows has a reason to.
func continueOnErrorProblems(file string, w *wfFile, extra ...wfStep) []string {
	var p []string
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		if setsContinueOnError(job.ContinueOnError) {
			p = append(p, fmt.Sprintf("%s › %s sets continue-on-error", file, name))
		}
		for i, s := range job.Steps {
			if setsContinueOnError(s.ContinueOnError) {
				p = append(p, fmt.Sprintf("%s › %s %s sets continue-on-error", file, name, s.label(i)))
			}
		}
	}
	for i, s := range extra {
		if setsContinueOnError(s.ContinueOnError) {
			p = append(p, fmt.Sprintf("%s %s sets continue-on-error", file, s.label(i)))
		}
	}
	return p
}

func TestWorkflows_NothingContinuesOnError(t *testing.T) {
	for _, file := range allWorkflows {
		expectNoProblems(t, file, continueOnErrorProblems(file, loadWorkflow(t, file)))
	}
	for _, a := range compositeActions {
		expectNoProblems(t, "the "+a+" action", continueOnErrorProblems("the "+a+" action", &wfFile{}, compositeSteps(t, a)...))
	}

	stepNamed := func(w *wfFile, job, name string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if w.Jobs[job].Steps[i].Name == name {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no step %q", job, name)
		return nil
	}
	rule := func(file string) func(*wfFile) []string {
		return func(w *wfFile) []string { return continueOnErrorProblems(file, w) }
	}
	expectEachMutationRefused(t, "proof.yml", rule("proof.yml"), []mutation{
		{"the proof job continues on error", func(w *wfFile) { w.Jobs["proof"].ContinueOnError = true }},
		{"a shard job continues on error", func(w *wfFile) { w.Jobs["integration"].ContinueOnError = "${{ true }}" }},
		{"the merge step continues", func(w *wfFile) { stepNamed(w, "proof", "merge").ContinueOnError = true }},
		{"the verdict step continues", func(w *wfFile) { stepNamed(w, "proof", "verdict").ContinueOnError = true }},
		{"the job records step continues", func(w *wfFile) { stepNamed(w, "proof", "job records").ContinueOnError = true }},
		{"the parts download continues", func(w *wfFile) {
			for i := range w.Jobs["proof"].Steps {
				if st := &w.Jobs["proof"].Steps[i]; st.With["pattern"] != nil {
					st.ContinueOnError = true
				}
			}
		}},
		{"a shard's proof step continues", func(w *wfFile) { stepNamed(w, "integration", "integration shard").ContinueOnError = true }},
	})
	expectEachMutationRefused(t, "release.yml", rule("release.yml"), []mutation{
		{"the tag job continues", func(w *wfFile) { w.Jobs["tag"].ContinueOnError = true }},
		{"the release verify continues", func(w *wfFile) { stepNamed(w, "tag", "verify the release proof").ContinueOnError = true }},
	})
}

// uploadProblems: a stuck or failing shard must still upload its part, so the
// merger reads what it left; that is every upload step running `if: always()`.
func uploadProblems(file string, w *wfFile) []string {
	var p []string
	seen := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.HasPrefix(s.Uses, "actions/upload-artifact@") {
				continue
			}
			seen++
			if c := strings.TrimSpace(s.If); c != "always()" && c != "${{ always() }}" {
				p = append(p, fmt.Sprintf("%s › %s %s runs under %q, want always()", file, name, s.label(i), s.If))
			}
		}
	}
	if seen == 0 {
		p = append(p, file+" uploads nothing")
	}
	return p
}

func TestWorkflows_EveryProofUploadRunsEvenWhenTheJobFailed(t *testing.T) {
	for _, file := range []string{"proof.yml", "ci.yml"} {
		expectNoProblems(t, file, uploadProblems(file, loadWorkflow(t, file)))
	}
	uploadIn := func(w *wfFile, job string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if strings.HasPrefix(w.Jobs[job].Steps[i].Uses, "actions/upload-artifact@") {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s uploads nothing", job)
		return nil
	}
	rule := func(w *wfFile) []string { return uploadProblems("proof.yml", w) }
	expectEachMutationRefused(t, "proof.yml", rule, []mutation{
		{"a shard's upload without if", func(w *wfFile) { uploadIn(w, "integration").If = "" }},
		{"the plan's upload without if", func(w *wfFile) { uploadIn(w, "plan").If = "" }},
		{"the proof job's upload on success only", func(w *wfFile) { uploadIn(w, "proof").If = "success()" }},
		{"the fast part's upload on failure only", func(w *wfFile) { uploadIn(w, "fast").If = "failure()" }},
	})
}

// partIdentityProblems holds every step that runs a CI part to what the merger
// requires of that part (merge.go checkIdentity): a part that uses the images
// records the plan hash, so its step passes --plan, its job needs `plan` and
// downloads the plan artifact to where --plan reads it; a UI part also records
// the UI plan hash (--ui-plan). partUsesImages is the merger's own predicate.
func partIdentityProblems(w *wfFile) []string {
	var p []string
	partRE := regexp.MustCompile(`--part\s+"?([^"\s\\]+)"?`)
	seen := 0
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		for i, s := range job.Steps {
			if !strings.Contains(s.Run, "scripts/proof.sh --part") {
				continue
			}
			m := partRE.FindStringSubmatch(s.Run)
			if m == nil {
				p = append(p, fmt.Sprintf("jobs.%s %s names no part", name, s.label(i)))
				continue
			}
			seen++
			part := strings.NewReplacer("${{ matrix.shard }}", "1", "${{ matrix.activity }}", "shell").Replace(m[1])
			if !partUsesImages(part) {
				continue
			}
			where := fmt.Sprintf("jobs.%s %s (part %s)", name, s.label(i), part)
			if !strings.Contains(s.Run, `--plan "$RUNNER_TEMP/ci/plan/integration-plan.json"`) {
				p = append(p, where+" passes no --plan: the merger finds no plan hash and the proof is ineligible")
			}
			if strings.HasPrefix(part, "ui-e2e:") && !strings.Contains(s.Run, `--ui-plan "$RUNNER_TEMP/ci/plan/ui-plan.json"`) {
				p = append(p, where+" passes no --ui-plan: the merger finds no UI plan hash")
			}
			if !contains(job.needs(), "plan") {
				p = append(p, fmt.Sprintf("jobs.%s does not need plan, whose artifact its --plan reads", name))
			}
			downloaded := false
			for _, d := range job.Steps {
				if isArtifactFetch(d) &&
					fetchedArtifact(d) == "${{ needs.plan.outputs.plan_artifact }}" &&
					withString(d, "path") == "${{ runner.temp }}/ci/plan" {
					downloaded = true
				}
			}
			if !downloaded {
				p = append(p, fmt.Sprintf("jobs.%s never downloads the plan artifact to $RUNNER_TEMP/ci/plan", name))
			}
		}
	}
	if seen == 0 {
		p = append(p, "no step runs scripts/proof.sh --part")
	}
	return p
}

func TestWorkflows_EveryImageUsingPartPassesThePlanTheMergerRequires(t *testing.T) {
	// The merger's predicate agrees with the parts this workflow runs.
	for part, want := range map[string]bool{"build": true, "observability": true, "integration:1": true,
		"ui-e2e:1": true, "activity-fixture:shell": true, "plan": false, "fast": false} {
		if partUsesImages(part) != want {
			t.Errorf("partUsesImages(%q) = %v, want %v: update this test with merge.go", part, !want, want)
		}
	}
	stepOf := func(w *wfFile, job, contains string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if strings.Contains(w.Jobs[job].Steps[i].Run, contains) {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no step running %q", job, contains)
		return nil
	}
	drop := func(job, part, flag string) mutation {
		return mutation{"no " + flag + " on " + job, func(w *wfFile) {
			s := stepOf(w, job, "--part "+part)
			var kept []string
			for _, line := range strings.Split(s.Run, "\n") {
				if !strings.Contains(line, flag+" ") {
					kept = append(kept, line)
				}
			}
			s.Run = strings.Join(kept, "\n")
		}}
	}
	expectEachMutationRefused(t, "proof.yml", partIdentityProblems, []mutation{
		drop("build", "build", "--plan"),
		drop("integration", `"integration:`, "--plan"),
		drop("ui-e2e", `"ui-e2e:`, "--plan"),
		drop("ui-e2e", `"ui-e2e:`, "--ui-plan"),
		drop("activity-fixture", `"activity-fixture:`, "--plan"),
		drop("observability", "observability", "--plan"),
		{"activity-fixture does not need plan", func(w *wfFile) { w.Jobs["activity-fixture"].Needs = []any{"base", "build"} }},
		{"observability never downloads the plan", func(w *wfFile) {
			job := w.Jobs["observability"]
			var kept []wfStep
			for _, s := range job.Steps {
				if fetchedArtifact(s) != "${{ needs.plan.outputs.plan_artifact }}" {
					kept = append(kept, s)
				}
			}
			job.Steps = kept
		}},
	})
}

// attemptProblems holds every step that runs a CI part to naming its run
// attempt explicitly: the uploads and the plan step's directory carry
// $GITHUB_RUN_ATTEMPT, so a part that kept proof.sh's default of 1 would, on a
// lost-runner rerun, write -a1 where the upload looks for -a2.
func attemptProblems(w *wfFile) []string {
	var p []string
	seen := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.Contains(s.Run, "scripts/proof.sh --part") {
				continue
			}
			seen++
			if !strings.Contains(s.Run, `--attempt "$GITHUB_RUN_ATTEMPT"`) {
				p = append(p, fmt.Sprintf(`jobs.%s %s does not pass --attempt "$GITHUB_RUN_ATTEMPT"`, name, s.label(i)))
			}
		}
	}
	if seen == 0 {
		p = append(p, "no step runs scripts/proof.sh --part")
	}
	return p
}

func TestWorkflows_EveryPartStepPassesItsRunAttempt(t *testing.T) {
	stepOf := func(w *wfFile, job, contains string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if strings.Contains(w.Jobs[job].Steps[i].Run, contains) {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no step running %q", job, contains)
		return nil
	}
	dropAttempt := func(job, part string) mutation {
		return mutation{"no --attempt on " + job, func(w *wfFile) {
			s := stepOf(w, job, "--part "+part)
			s.Run = strings.ReplaceAll(s.Run, `--attempt "$GITHUB_RUN_ATTEMPT"`, "")
		}}
	}
	expectEachMutationRefused(t, "proof.yml", attemptProblems, []mutation{
		dropAttempt("plan", "plan"),
		dropAttempt("build", "build"),
		dropAttempt("fast", "fast"),
		dropAttempt("integration", `"integration:`),
		dropAttempt("ui-e2e", `"ui-e2e:`),
		dropAttempt("activity-fixture", `"activity-fixture:`),
		dropAttempt("observability", "observability"),
		{"a fixed attempt", func(w *wfFile) {
			s := stepOf(w, "build", "--part build")
			s.Run = strings.ReplaceAll(s.Run, `--attempt "$GITHUB_RUN_ATTEMPT"`, "--attempt 1")
		}},
	})
}

// observabilityProblems pins how the plan job decides whether the
// observability job runs: from the plan's own record of what it requires, the
// field the merger reads. observability-paths.txt lists touched telemetry
// paths, which never contain the word "observability", so grepping it for the
// word skips the job the merger then expects.
func observabilityProblems(w *wfFile) []string {
	var p []string
	job := w.Jobs["plan"]
	if job == nil {
		return []string{"proof.yml has no plan job"}
	}
	found := false
	for i, s := range job.Steps {
		for _, line := range strings.Split(s.Run, "\n") {
			if !strings.Contains(line, "observability=") {
				continue
			}
			found = true
			if strings.Contains(line, "observability-paths.txt") {
				p = append(p, fmt.Sprintf("jobs.plan %s decides from observability-paths.txt, a list of paths", s.label(i)))
			}
			if !strings.Contains(line, `jq -r '.observability_required // false' "$dir/integration-plan.json"`) {
				p = append(p, fmt.Sprintf("jobs.plan %s does not read observability_required from the plan", s.label(i)))
			}
		}
	}
	if !found {
		p = append(p, "jobs.plan sets no observability output")
	}
	return p
}

func TestWorkflows_TheObservabilityPartFollowsThePlansRecord(t *testing.T) {
	expectEachMutationRefused(t, "proof.yml", observabilityProblems, []mutation{
		{"revert to grepping the paths file", func(w *wfFile) {
			for i := range w.Jobs["plan"].Steps {
				s := &w.Jobs["plan"].Steps[i]
				s.Run = strings.ReplaceAll(s.Run,
					`$(jq -r '.observability_required // false' "$dir/integration-plan.json")`,
					`$(grep -q 'observability' "$dir/observability-paths.txt" 2>/dev/null && echo true || echo false)`)
			}
		}},
		{"read another plan field", func(w *wfFile) {
			for i := range w.Jobs["plan"].Steps {
				s := &w.Jobs["plan"].Steps[i]
				s.Run = strings.ReplaceAll(s.Run, ".observability_required", ".tier")
			}
		}},
		{"no observability output", func(w *wfFile) {
			for i := range w.Jobs["plan"].Steps {
				s := &w.Jobs["plan"].Steps[i]
				s.Run = strings.ReplaceAll(s.Run, "observability=", "obs=")
			}
		}},
	})
}

// workflowFilesOnDisk lists every workflow and composite action under
// .github, as paths relative to it.
func workflowFilesOnDisk(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "..", ".github")
	var out []string
	for _, pattern := range []string{"workflows/*.yml", "workflows/*.yaml", "actions/*/action.yml", "actions/*/action.yaml"} {
		m, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range m {
			rel, _ := filepath.Rel(root, f)
			out = append(out, filepath.ToSlash(rel))
		}
	}
	sort.Strings(out)
	return out
}

// The rules above run over allWorkflows and the composite action; a workflow
// added to .github without being listed would escape every one of them.
func TestWorkflows_TheRulesCoverEveryWorkflowOnDisk(t *testing.T) {
	var covered []string
	for _, f := range allWorkflows {
		covered = append(covered, "workflows/"+f)
	}
	for _, a := range compositeActions {
		covered = append(covered, "actions/"+a+"/action.yml")
	}
	sort.Strings(covered)
	if got := workflowFilesOnDisk(t); strings.Join(got, "\n") != strings.Join(covered, "\n") {
		t.Errorf("the workflow rules cover\n  %s\nbut .github holds\n  %s\nadd the new file to allWorkflows (and its rules) or remove it",
			strings.Join(covered, "\n  "), strings.Join(got, "\n  "))
	}
}

// ---- timeouts ------------------------------------------------------------------

// timeoutProblems holds every job to the plan's rule (§2.6 rule 2, §3.2): a
// job has a limit, at least the sum of its steps' own limits plus five
// minutes; every step before a `scripts/proof.sh` step has a limit of its
// own; a job that only calls a reusable workflow is exempt because that
// workflow's jobs carry theirs. A stuck shard must therefore stop at its proof
// step's limit, with its `if: always()` upload still to run, never at the
// job's — which would take the upload with it.
func timeoutProblems(file string, w *wfFile) []string {
	var p []string
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		if job.Uses != "" {
			continue
		}
		jt, set, err := timeoutMinutes(job.Timeout)
		if err != nil {
			p = append(p, fmt.Sprintf("%s › %s: %v", file, name, err))
			continue
		}
		if !set {
			p = append(p, fmt.Sprintf("%s › %s has no timeout-minutes", file, name))
			continue
		}
		sum, proofAt := 0, -1
		for i, s := range job.Steps {
			n, _, err := timeoutMinutes(s.Timeout)
			if err != nil {
				p = append(p, fmt.Sprintf("%s › %s %s: %v", file, name, s.label(i), err))
			}
			sum += n
			if proofAt < 0 && strings.Contains(s.Run, "scripts/proof.sh") {
				proofAt = i
			}
		}
		if proofAt >= 0 {
			for i, s := range job.Steps[:proofAt] {
				if _, set, _ := timeoutMinutes(s.Timeout); !set {
					p = append(p, fmt.Sprintf("%s › %s %s runs before the proof step and has no timeout-minutes", file, name, s.label(i)))
				}
			}
		}
		if jt < sum+5 {
			p = append(p, fmt.Sprintf("%s › %s times out at %d, and its steps sum to %d (needs %d)", file, name, jt, sum, sum+5))
		}
	}
	return p
}

func TestWorkflows_EveryProofStepTimesOutBeforeItsJob(t *testing.T) {
	for _, file := range allWorkflows {
		expectNoProblems(t, file, timeoutProblems(file, loadWorkflow(t, file)))
	}
}

func TestWorkflows_TheTimeoutRuleRefusesEachWeakening(t *testing.T) {
	rule := func(w *wfFile) []string { return timeoutProblems("proof.yml", w) }
	stepNamed := func(w *wfFile, job, name string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if w.Jobs[job].Steps[i].Name == name {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no step %q", job, name)
		return nil
	}
	expectEachMutationRefused(t, "proof.yml", rule, []mutation{
		{"a job without timeout-minutes", func(w *wfFile) { w.Jobs["build"].Timeout = nil }},
		{"a job limit under its steps", func(w *wfFile) { w.Jobs["integration"].Timeout = 100 }},
		{"an expression step limit over the job", func(w *wfFile) {
			stepNamed(w, "integration", "integration shard").Timeout = "${{ env.TIER == 'nightly' && 140 || 45 }}"
		}},
		{"an unreadable step limit", func(w *wfFile) { stepNamed(w, "fast", "fast").Timeout = "soon" }},
		{"a run-first step before the proof step without a limit", func(w *wfFile) {
			job := w.Jobs["fast"]
			job.Steps = append([]wfStep{{Run: "echo first"}}, job.Steps...)
		}},
	})
}

// ---- artifacts, credentials and permissions --------------------------------------

func TestWorkflows_ShardsFindThePlanAndBuildByTheirOutputs(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	if got := w.Jobs["plan"].Outputs["plan_artifact"]; got != "plan-a${{ github.run_attempt }}" {
		t.Errorf("jobs.plan.outputs.plan_artifact = %q", got)
	}
	if got := w.Jobs["build"].Outputs["build_artifact"]; got != "part-build-a${{ github.run_attempt }}" {
		t.Errorf("jobs.build.outputs.build_artifact = %q", got)
	}
	downloads := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !isArtifactFetch(s) {
				continue
			}
			art := fetchedArtifact(s)
			if art == "" {
				continue // a pattern download of every part
			}
			downloads++
			if strings.Contains(art, "github.run_attempt") {
				t.Errorf("jobs.%s %s downloads %q, a name built from this attempt: a re-run job would miss attempt 1's", name, s.label(i), art)
			}
			if art != "${{ needs.plan.outputs.plan_artifact }}" && art != "${{ needs.build.outputs.build_artifact }}" {
				t.Errorf("jobs.%s %s downloads %q, not a needs output", name, s.label(i), art)
			}
		}
	}
	if downloads == 0 {
		t.Error("no job downloads the plan or the build by name")
	}
}

func credentialProblems(file string, w *wfFile) []string {
	var p []string
	checkouts := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !isCheckout(s) {
				continue
			}
			checkouts++
			if withString(s, "persist-credentials") != "false" {
				p = append(p, fmt.Sprintf("%s › %s %s does not set persist-credentials: false", file, name, s.label(i)))
			}
		}
	}
	if checkouts == 0 {
		p = append(p, file+" has no checkout to check")
	}
	return p
}

func TestWorkflows_PullRequestCheckoutsDoNotPersistCredentials(t *testing.T) {
	for _, file := range allWorkflows {
		expectNoProblems(t, file, credentialProblems(file, loadWorkflow(t, file)))
	}
	expectEachMutationRefused(t, "ci.yml", func(w *wfFile) []string { return credentialProblems("ci.yml", w) }, []mutation{
		{"a named checkout that persists", func(w *wfFile) {
			s := &w.Jobs["feature"].Steps[0]
			s.Name = "checkout"
			s.With["persist-credentials"] = true
		}},
		{"a checkout with no with:", func(w *wfFile) { w.Jobs["check"].Steps[0].With = nil }},
		{"no checkout at all", func(w *wfFile) {
			for _, j := range w.Jobs {
				j.Steps = nil
			}
		}},
	})
}

// calleeNeeds is the highest level any job of proof.yml, or its top level,
// asks for, by permission.
func calleeNeeds(w *wfFile) map[string]int {
	need := map[string]int{}
	add := func(perms map[string]string) {
		for p, lvl := range perms {
			if l := permLevel(lvl); l > need[p] {
				need[p] = l
			}
		}
	}
	add(w.Permissions)
	for _, job := range w.Jobs {
		add(job.Permissions)
	}
	return need
}

func callerProblems(file string, caller, callee *wfFile) []string {
	var p []string
	need := calleeNeeds(callee)
	inputs := map[string]bool{}
	if call, ok := callee.On["workflow_call"].(map[string]any); ok {
		if in, ok := call["inputs"].(map[string]any); ok {
			for name := range in {
				inputs[name] = true
			}
		}
	}
	calls := 0
	for _, name := range caller.jobNames() {
		job := caller.Jobs[name]
		if job.Uses != "./.github/workflows/proof.yml" {
			continue
		}
		calls++
		for perm, lvl := range need {
			if lvl == 0 {
				continue
			}
			got := permLevel(job.Permissions[perm])
			if got < lvl {
				p = append(p, fmt.Sprintf("%s › %s grants %s: %q, and proof.yml's jobs need %s", file, name, perm,
					job.Permissions[perm], []string{"none", "read", "write"}[lvl]))
			}
		}
		for input := range job.With {
			if !inputs[input] {
				p = append(p, fmt.Sprintf("%s › %s passes input %q, which proof.yml does not declare", file, name, input))
			}
		}
	}
	if calls == 0 {
		p = append(p, file+" does not call proof.yml")
	}
	return p
}

// TestWorkflows_CallersGrantWhatTheirCalleesNeed: a reusable workflow's jobs
// cannot get a permission level their caller did not grant, and a caller
// cannot pass an input the callee does not declare. Without this, nightly.yml
// and release.yml would fail at startup, and nothing local would notice.
func TestWorkflows_CallersGrantWhatTheirCalleesNeed(t *testing.T) {
	callee := loadWorkflow(t, "proof.yml")
	need := calleeNeeds(callee)
	for _, p := range []string{"contents", "actions", "checks"} {
		if need[p] < 1 {
			t.Errorf("proof.yml's jobs need no %s: read; the comparison would be vacuous", p)
		}
	}
	for _, file := range []string{"nightly.yml", "release.yml"} {
		expectNoProblems(t, file, callerProblems(file, loadWorkflow(t, file), callee))
		rule := func(w *wfFile) []string { return callerProblems(file, w, callee) }
		caller := map[string]string{"nightly.yml": "nightly", "release.yml": "gate"}[file]
		expectEachMutationRefused(t, file, rule, []mutation{
			{"actions: none", func(w *wfFile) { w.Jobs[caller].Permissions["actions"] = "none" }},
			{"checks dropped", func(w *wfFile) { delete(w.Jobs[caller].Permissions, "checks") }},
			{"contents misspelt", func(w *wfFile) { w.Jobs[caller].Permissions["contents"] = "reed" }},
			{"an undeclared input", func(w *wfFile) { w.Jobs[caller].With["shard"] = 3 }},
		})
	}
}

// TestWorkflows_OnlyPullRequestRunsShareAConcurrencyGroup: a group per tier
// and event, and only a pull request cancels a run. Anything else — a merge
// group, a dispatch, a nightly, a release — is a group of its own, so nothing
// displaces a proof that is already running.
func TestWorkflows_OnlyPullRequestRunsShareAConcurrencyGroup(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	group := fmt.Sprint(w.Concurrency["group"])
	if !strings.Contains(group, "inputs.tier") ||
		!strings.Contains(group, "github.event_name") ||
		!strings.Contains(group, "github.event.pull_request.number || github.run_id") {
		t.Errorf("concurrency group %q does not name the tier, the event and a per-run fallback", group)
	}
	if got := fmt.Sprint(w.Concurrency["cancel-in-progress"]); got != "${{ github.event_name == 'pull_request' }}" {
		t.Errorf("cancel-in-progress = %q, want only pull requests", got)
	}
}

// ---- triggers --------------------------------------------------------------------

// TestWorkflows_CheckRunsOnEveryPush: ci.yml's `check` runs on every push and
// pull request (plan §3.1, §3.4; CONTRIBUTING.md and docs/PROOFS.md say so),
// and `feature` narrows a push to validate/feature/**.
func TestWorkflows_CheckRunsOnEveryPush(t *testing.T) {
	rule := func(w *wfFile) []string {
		var p []string
		push, ok := w.On["push"]
		if !ok {
			p = append(p, "ci.yml does not trigger on push")
		} else if m, _ := push.(map[string]any); len(m) > 0 {
			p = append(p, fmt.Sprintf("ci.yml filters its push trigger: %v", m))
		}
		if _, ok := w.On["pull_request"]; !ok {
			p = append(p, "ci.yml does not trigger on pull_request")
		}
		if check, ok := w.Jobs["check"]; !ok || check.If != "" {
			p = append(p, "ci.yml's check job is missing or conditional")
		}
		feature, ok := w.Jobs["feature"]
		if !ok {
			return append(p, "ci.yml has no feature job")
		}
		const narrowed = "github.event_name == 'push' && startsWith(github.ref, 'refs/heads/validate/feature/')"
		pushes := strings.Count(feature.If, "github.event_name == 'push'")
		if pushes == 0 || pushes != strings.Count(feature.If, narrowed) {
			p = append(p, fmt.Sprintf("jobs.feature.if runs on a push outside validate/feature/**: %s", feature.If))
		}
		return p
	}
	expectEachMutationRefused(t, "ci.yml", rule, []mutation{
		{"push filtered to validate branches", func(w *wfFile) {
			w.On["push"] = map[string]any{"branches": []any{"validate/feature/**"}}
		}},
		{"no push", func(w *wfFile) { delete(w.On, "push") }},
		{"feature on every push", func(w *wfFile) {
			w.Jobs["feature"].If = "${{ github.event_name == 'push' || github.event_name == 'workflow_dispatch' }}"
		}},
	})
}

// featureBaseProblems: the feature proof diffs from the commit the change
// branched from, whatever shape a validate/feature push has, so the checkout
// holds the history a merge-base needs.
func featureBaseProblems(w *wfFile) []string {
	feature, ok := w.Jobs["feature"]
	if !ok {
		return []string{"ci.yml has no feature job"}
	}
	var p []string
	for _, s := range feature.Steps {
		if isCheckout(s) && withString(s, "fetch-depth") != "0" {
			p = append(p, fmt.Sprintf("the feature checkout has fetch-depth %q, and a merge-base needs the history (0)", withString(s, "fetch-depth")))
		}
		if s.Name != "base" {
			continue
		}
		if !strings.Contains(s.Run, `git merge-base HEAD origin/main`) {
			p = append(p, "the feature base is not the merge base with origin/main")
		}
		if strings.Contains(s.Run, "base=\"$(git rev-parse HEAD^1)\"\n          echo") {
			p = append(p, "the feature base is HEAD^1, which a non-merge push does not satisfy")
		}
		return p
	}
	return append(p, "the feature job has no base step")
}

func TestWorkflows_TheFeatureBaseIsTheMergeBase(t *testing.T) {
	expectEachMutationRefused(t, "ci.yml", featureBaseProblems, []mutation{
		{"HEAD^1 as the base", func(w *wfFile) {
			for i := range w.Jobs["feature"].Steps {
				if s := &w.Jobs["feature"].Steps[i]; s.Name == "base" {
					s.Run = `set -euo pipefail
          base="$(git rev-parse HEAD^1)"
          echo "base=$base" >>"$GITHUB_OUTPUT"`
				}
			}
		}},
		{"a shallow checkout", func(w *wfFile) {
			for i := range w.Jobs["feature"].Steps {
				if isCheckout(w.Jobs["feature"].Steps[i]) {
					w.Jobs["feature"].Steps[i].With["fetch-depth"] = 2
				}
			}
		}},
	})
}

// TestWorkflows_TheFlakesJobReadsTonightsProof: `flakes` waits for `nightly`,
// whatever its result, so the window it reads includes that night's proof.
func TestWorkflows_TheFlakesJobReadsTonightsProof(t *testing.T) {
	rule := func(w *wfFile) []string {
		flakes, ok := w.Jobs["flakes"]
		if !ok {
			return []string{"nightly.yml has no flakes job"}
		}
		var p []string
		if !contains(flakes.needs(), "nightly") {
			p = append(p, fmt.Sprintf("jobs.flakes.needs = %v, want nightly", flakes.needs()))
		}
		if !strings.Contains(flakes.If, "always()") {
			p = append(p, "jobs.flakes.if lacks always(): a red nightly would skip the flake report")
		}
		return p
	}
	expectEachMutationRefused(t, "nightly.yml", rule, []mutation{
		{"no needs", func(w *wfFile) { w.Jobs["flakes"].Needs = nil }},
		{"no always()", func(w *wfFile) { w.Jobs["flakes"].If = "${{ github.event_name == 'workflow_dispatch' }}" }},
	})
}

// ---- release ---------------------------------------------------------------------

// releaseVersionRE is plan §3.1's version rule.
const releaseVersionRE = `^v[0-9]+\.[0-9]+\.[0-9]+$`

func releaseProblems(w *wfFile) []string {
	var p []string
	validate, ok := w.Jobs["validate"]
	if !ok {
		return []string{"release.yml has no validate job"}
	}
	if len(validate.needs()) > 0 || validate.If != "" {
		p = append(p, "jobs.validate waits on something or is conditional; it must run first, on every dispatch")
	}
	for _, c := range []struct {
		job   string
		needs []string
	}{{"gate", []string{"validate"}}, {"tag", []string{"validate", "gate"}}} {
		job, ok := w.Jobs[c.job]
		if !ok {
			p = append(p, "release.yml has no "+c.job+" job")
			continue
		}
		for _, n := range c.needs {
			if !contains(job.needs(), n) {
				p = append(p, fmt.Sprintf("jobs.%s does not need %s", c.job, n))
			}
		}
	}
	var check *wfStep
	for i := range validate.Steps {
		if strings.Contains(validate.Steps[i].Run, "=~") {
			check = &validate.Steps[i]
		}
	}
	if check == nil {
		return append(p, "jobs.validate never matches the version")
	}
	if got := envString(*check, "VERSION"); got != "${{ inputs.version }}" {
		p = append(p, fmt.Sprintf("jobs.validate reads the version from %q, not the env", got))
	}
	m := regexp.MustCompile(`=~\s*(\S+)\s*\]\]`).FindStringSubmatch(check.Run)
	if m == nil {
		return append(p, "jobs.validate's match has no pattern")
	}
	if m[1] != releaseVersionRE {
		p = append(p, fmt.Sprintf("jobs.validate matches %s, want %s", m[1], releaseVersionRE))
	} else {
		re := regexp.MustCompile(m[1])
		for v, want := range map[string]bool{
			"v1.2.3": true, "v10.0.11": true,
			"1.2.3": false, "v1.2": false, "v1.2.3-rc1": false, "v1.2.3; rm -rf /": false, "": false,
		} {
			if re.MatchString(v) != want {
				p = append(p, fmt.Sprintf("the version pattern on %q = %v, want %v", v, !want, want))
			}
		}
	}
	if !strings.Contains(check.Run, "exit 1") || !strings.Contains(check.Run, `version=$VERSION" >>"$GITHUB_OUTPUT"`) {
		p = append(p, "jobs.validate does not refuse, or does not publish, the version")
	}
	// A tag is created from main and nowhere else: validate refuses a tagging
	// dispatch on any other ref, and the tag job runs only on main's ref.
	var refuse *wfStep
	for i := range validate.Steps {
		if strings.Contains(validate.Steps[i].Run, "refs/heads/main") {
			refuse = &validate.Steps[i]
		}
	}
	if refuse == nil {
		p = append(p, "jobs.validate never refuses a tagging dispatch off refs/heads/main")
	} else {
		if envString(*refuse, "REF") != "${{ github.ref }}" || envString(*refuse, "WANT_TAG") != "${{ inputs.tag }}" {
			p = append(p, "jobs.validate's main check does not read github.ref and inputs.tag from its env")
		}
		if !strings.Contains(refuse.Run, "exit 1") || !strings.Contains(refuse.Run, `"$WANT_TAG" = true`) ||
			!strings.Contains(refuse.Run, `"$REF" != refs/heads/main`) {
			p = append(p, "jobs.validate's main check does not fail a tagging dispatch off main")
		}
	}
	if tag, ok := w.Jobs["tag"]; ok && !strings.Contains(tag.If, "github.ref == 'refs/heads/main'") {
		p = append(p, fmt.Sprintf("jobs.tag.if = %q does not require github.ref == 'refs/heads/main'", tag.If))
	}
	// Concurrency comes from the validated value, never the raw input.
	if w.Concurrency != nil {
		p = append(p, "release.yml has a workflow-level concurrency group, decided before validation")
	}
	for _, name := range []string{"gate", "tag"} {
		job, ok := w.Jobs[name]
		if !ok {
			continue
		}
		group := fmt.Sprint(job.Concurrency["group"])
		if !strings.Contains(group, "needs.validate.outputs.version") || strings.Contains(group, "inputs.") {
			p = append(p, fmt.Sprintf("jobs.%s's concurrency group %q is not the validated version", name, group))
		}
	}
	return p
}

func TestWorkflows_ReleaseValidatesTheVersionFirst(t *testing.T) {
	expectEachMutationRefused(t, "release.yml", releaseProblems, []mutation{
		{"no validate job", func(w *wfFile) { delete(w.Jobs, "validate") }},
		{"validate only when tagging", func(w *wfFile) { w.Jobs["validate"].If = "${{ inputs.tag }}" }},
		{"gate does not wait for validate", func(w *wfFile) { w.Jobs["gate"].Needs = nil }},
		{"tag does not wait for validate", func(w *wfFile) { w.Jobs["tag"].Needs = "gate" }},
		{"a looser pattern", func(w *wfFile) {
			s := &w.Jobs["validate"].Steps[0]
			s.Run = strings.Replace(s.Run, releaseVersionRE, `^v[0-9.]+`, 1)
		}},
		{"concurrency from the raw input", func(w *wfFile) {
			w.Jobs["gate"].Concurrency["group"] = "release-${{ inputs.version }}"
		}},
		{"workflow-level concurrency", func(w *wfFile) {
			w.Concurrency = map[string]any{"group": "release-${{ inputs.version }}"}
		}},
		{"no refusal of a tag off main", func(w *wfFile) {
			v := w.Jobs["validate"]
			v.Steps = v.Steps[:1]
		}},
		{"the refusal never fails", func(w *wfFile) {
			for i := range w.Jobs["validate"].Steps {
				s := &w.Jobs["validate"].Steps[i]
				if strings.Contains(s.Run, "refs/heads/main") {
					s.Run = strings.Replace(s.Run, "exit 1", "exit 0", 1)
				}
			}
		}},
		{"the refusal reads the ref only on tag false", func(w *wfFile) {
			for i := range w.Jobs["validate"].Steps {
				s := &w.Jobs["validate"].Steps[i]
				if strings.Contains(s.Run, "refs/heads/main") {
					s.Env["WANT_TAG"] = "false"
				}
			}
		}},
		{"the tag job on any ref", func(w *wfFile) { w.Jobs["tag"].If = "${{ inputs.tag }}" }},
	})
}

// gitTagRE matches `git tag`, with or without `-c key=value` options before it.
var gitTagRE = regexp.MustCompile(`git( -c '?[^ ]+'?)* tag\b`)

func tagProblems(w *wfFile) []string {
	tag, ok := w.Jobs["tag"]
	if !ok {
		return []string{"release.yml has no tag job"}
	}
	var p []string
	verifyAt, tagAt, pushAt := -1, -1, -1
	for i, s := range tag.Steps {
		if verifyAt < 0 && strings.Contains(s.Run, "proofrun verify -release") {
			verifyAt = i
		}
		touches := gitTagRE.MatchString(s.Run) || strings.Contains(s.Run, "git push")
		if touches && (verifyAt < 0 || i <= verifyAt) {
			p = append(p, fmt.Sprintf("%s touches a tag before the proof is verified", s.label(i)))
		}
		if tagAt < 0 && gitTagRE.MatchString(s.Run) {
			tagAt = i
			// A runner has no committer identity: an annotated tag needs one.
			if !strings.Contains(s.Run, "user.name=") || !strings.Contains(s.Run, "user.email=") {
				p = append(p, fmt.Sprintf("%s creates an annotated tag with no explicit committer identity", s.label(i)))
			}
		}
		if strings.Contains(s.Run, "git push") {
			pushAt = i
		}
	}
	if verifyAt < 0 {
		p = append(p, "the tag job never runs `proofrun verify -release`")
	}
	if tagAt < 0 {
		p = append(p, "the tag job never creates a tag")
	}
	if pushAt < 0 || pushAt < tagAt {
		p = append(p, "the tag job never pushes the tag it created")
	}
	for i, s := range tag.Steps {
		if isCheckout(s) && withString(s, "ref") != "${{ github.sha }}" {
			p = append(p, fmt.Sprintf("%s checks out %q, not the commit the gate proved", s.label(i), withString(s, "ref")))
		}
		// Repository code runs in this job (`go run`): no checkout leaves the
		// token on disk for it, and only the step that pushes is handed it.
		if isCheckout(s) && withString(s, "persist-credentials") != "false" {
			p = append(p, fmt.Sprintf("%s persists credentials while the proof is verified", s.label(i)))
		}
		if i != pushAt && (envString(s, "GH_TOKEN") != "" || envString(s, "GITHUB_TOKEN") != "") {
			p = append(p, fmt.Sprintf("%s is handed a token and does not push", s.label(i)))
		}
	}
	return p
}

// TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst: release.yml creates and
// pushes a tag only after `proofrun verify -release` accepted a passing
// release proof of the commit it checked out; no earlier step touches a tag.
func TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst(t *testing.T) {
	expectEachMutationRefused(t, "release.yml", tagProblems, []mutation{
		{"a tag without a committer identity", func(w *wfFile) {
			for i := range w.Jobs["tag"].Steps {
				s := &w.Jobs["tag"].Steps[i]
				s.Run = strings.ReplaceAll(s.Run, "git -c user.name='github-actions[bot]' -c user.email='41898282+github-actions[bot]@users.noreply.github.com' tag", "git tag")
			}
		}},
		{"a push before verify", func(w *wfFile) {
			job := w.Jobs["tag"]
			job.Steps = append([]wfStep{{Name: "early", Run: "git push origin v0.0.0"}}, job.Steps...)
		}},
		{"a tag before verify", func(w *wfFile) {
			job := w.Jobs["tag"]
			job.Steps = append([]wfStep{{Name: "early", Run: `git tag -a "$VERSION" -m x`}}, job.Steps...)
		}},
		{"verify removed", func(w *wfFile) {
			for i := range w.Jobs["tag"].Steps {
				w.Jobs["tag"].Steps[i].Run = strings.ReplaceAll(w.Jobs["tag"].Steps[i].Run, "proofrun verify -release", "true")
			}
		}},
		{"verify and tag in one step", func(w *wfFile) {
			steps := w.Jobs["tag"].Steps
			last := steps[len(steps)-1]
			for i := range steps {
				if strings.Contains(steps[i].Run, "proofrun verify -release") {
					steps[i].Run += "\n" + last.Run
				}
			}
		}},
		{"checkout of the branch tip", func(w *wfFile) { delete(w.Jobs["tag"].Steps[0].With, "ref") }},
		{"a checkout that keeps the token", func(w *wfFile) { w.Jobs["tag"].Steps[0].With["persist-credentials"] = true }},
		{"the verify step handed the token", func(w *wfFile) {
			for i := range w.Jobs["tag"].Steps {
				if s := &w.Jobs["tag"].Steps[i]; strings.Contains(s.Run, "proofrun verify -release") {
					s.Env["GH_TOKEN"] = "${{ github.token }}"
				}
			}
		}},
	})
}

// ---- the site ----------------------------------------------------------------------

// writePermissions lists the permissions a job or workflow asks above read.
func writePermissions(perms map[string]string) []string {
	var out []string
	for perm, lvl := range perms {
		if permLevel(lvl) > 1 {
			out = append(out, perm+": "+lvl)
		}
	}
	sort.Strings(out)
	return out
}

// siteProblems holds site.yml to what its header promises. A pull request
// builds the site with read-only permissions and publishes nothing; only a push
// to main deploys, in the one job that holds write permissions, and that job
// checks out and runs no repository code, since it holds the Pages and OIDC
// tokens.
func siteProblems(w *wfFile) []string {
	var p []string
	for _, trigger := range []string{"pull_request_target", "workflow_run", "issue_comment"} {
		if _, ok := w.On[trigger]; ok {
			p = append(p, "site.yml triggers on "+trigger+", which runs with the repository's permissions on a stranger's behalf")
		}
	}
	if _, ok := w.On["pull_request"]; !ok {
		p = append(p, "site.yml does not build pull requests")
	}
	if push, _ := w.On["push"].(map[string]any); fmt.Sprint(push["branches"]) != "[main]" {
		p = append(p, fmt.Sprintf("site.yml's push trigger is %v, want branches: [main] alone", w.On["push"]))
	}
	if len(w.Permissions) != 1 || w.Permissions["contents"] != "read" {
		p = append(p, fmt.Sprintf("site.yml's top-level permissions are %v, want contents: read alone", w.Permissions))
	}
	writers := 0
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		for perm, lvl := range job.Permissions {
			if permLevel(lvl) < 0 {
				p = append(p, fmt.Sprintf("jobs.%s.permissions.%s = %q is not a level GitHub accepts", name, perm, lvl))
			}
		}
		if len(writePermissions(job.Permissions)) == 0 {
			continue
		}
		writers++
		where := "jobs." + name
		if got, want := strings.Join(writePermissions(job.Permissions), ", "), "id-token: write, pages: write"; got != want || len(job.Permissions) != 2 {
			p = append(p, fmt.Sprintf("%s.permissions = %v, want exactly pages: write and id-token: write", where, job.Permissions))
		}
		cond := normalizeIf(job.If)
		for _, want := range []string{"github.event_name != 'pull_request'", "github.ref == 'refs/heads/main'"} {
			if !strings.Contains(cond, want) {
				p = append(p, fmt.Sprintf("%s.if = %q lacks %q: a pull request or another branch must never deploy", where, job.If, want))
			}
		}
		if or, balanced := topLevelOr(cond); or || !balanced {
			p = append(p, fmt.Sprintf("%s.if = %q has a top-level ||, which would admit a run the other terms exclude", where, job.If))
		}
		if !contains(job.needs(), "build") {
			p = append(p, where+" does not need build: it would publish what was never built")
		}
		for i, s := range job.Steps {
			if isCheckout(s) || s.Run != "" {
				p = append(p, fmt.Sprintf("%s %s runs repository code in the job that holds the Pages and OIDC tokens", where, s.label(i)))
			}
		}
	}
	if writers != 1 {
		p = append(p, fmt.Sprintf("%d jobs of site.yml hold write permissions, want exactly the one that deploys", writers))
	}
	return p
}

func TestWorkflows_OnlyTheSiteDeployJobWritesAndItRunsNoRepositoryCode(t *testing.T) {
	expectEachMutationRefused(t, "site.yml", siteProblems, []mutation{
		{"a pull_request_target trigger", func(w *wfFile) { w.On["pull_request_target"] = map[string]any{} }},
		{"no pull request build", func(w *wfFile) { delete(w.On, "pull_request") }},
		{"a push to any branch", func(w *wfFile) { w.On["push"] = map[string]any{} }},
		{"a push to main and another branch", func(w *wfFile) { w.On["push"] = map[string]any{"branches": []any{"main", "dev"}} }},
		{"workflow-level contents: write", func(w *wfFile) { w.Permissions["contents"] = "write" }},
		{"workflow-level pages: write", func(w *wfFile) { w.Permissions["pages"] = "write" }},
		{"no workflow-level permissions", func(w *wfFile) { w.Permissions = nil }},
		{"the build job writes pages", func(w *wfFile) { w.Jobs["build"].Permissions = map[string]string{"pages": "write"} }},
		{"a misspelt level", func(w *wfFile) { w.Jobs["build"].Permissions = map[string]string{"contents": "reed"} }},
		{"the deploy job also writes contents", func(w *wfFile) { w.Jobs["deploy"].Permissions["contents"] = "write" }},
		{"the deploy job without id-token", func(w *wfFile) { delete(w.Jobs["deploy"].Permissions, "id-token") }},
		{"the deploy job also reads contents", func(w *wfFile) { w.Jobs["deploy"].Permissions["contents"] = "read" }},
		{"deploy on a pull request", func(w *wfFile) { w.Jobs["deploy"].If = "github.ref == 'refs/heads/main'" }},
		{"deploy from any ref", func(w *wfFile) { w.Jobs["deploy"].If = "github.event_name != 'pull_request'" }},
		{"deploy OR a dispatch", func(w *wfFile) {
			w.Jobs["deploy"].If += " || github.event_name == 'workflow_dispatch'"
		}},
		{"deploy without build", func(w *wfFile) { w.Jobs["deploy"].Needs = nil }},
		{"deploy checks out the repository", func(w *wfFile) {
			job := w.Jobs["deploy"]
			job.Steps = append([]wfStep{{Uses: "actions/checkout@v7.0.1"}}, job.Steps...)
		}},
		{"deploy runs a script", func(w *wfFile) {
			job := w.Jobs["deploy"]
			job.Steps = append(job.Steps, wfStep{Name: "after", Run: "./site/deploy.sh"})
		}},
		{"a second job that writes", func(w *wfFile) {
			w.Jobs["announce"] = &wfJob{Permissions: map[string]string{"pages": "write", "id-token": "write"}, If: w.Jobs["deploy"].If, Needs: "build"}
		}},
	})
}

// ---- shells and pins ---------------------------------------------------------------

// untrustedInShell names the expressions a `run:` may never splice into its
// script: a workflow input or event field is text a person typed, and the
// shell would read it as code. They reach a step through `env:`.
var untrustedInShell = regexp.MustCompile(`\$\{\{\s*(inputs\.|github\.event\.|github\.event_name|github\.head_ref|github\.base_ref|github\.ref_name|github\.actor)`)

func TestWorkflows_RunStepsNeverInterpolateInputs(t *testing.T) {
	check := func(where string, steps []wfStep) {
		for i, s := range steps {
			if m := untrustedInShell.FindString(s.Run); m != "" {
				t.Errorf("%s %s splices %q into its shell; pass it through env:", where, s.label(i), m)
			}
		}
	}
	for _, file := range allWorkflows {
		w := loadWorkflow(t, file)
		for _, name := range w.jobNames() {
			check(file+" › "+name, w.Jobs[name].Steps)
		}
	}
	for _, a := range compositeActions {
		check("the "+a+" action", compositeSteps(t, a))
	}
	for _, expr := range []string{"inputs.version", "github.event.pull_request.title", "github.event_name", "github.head_ref",
		"github.base_ref", "github.ref_name", "github.actor"} {
		if untrustedInShell.FindString(`run: echo "${{ `+expr+` }}"`) == "" {
			t.Errorf("the pattern misses an interpolated %s", expr)
		}
	}
}

var (
	exactActionRef = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@v[0-9]+\.[0-9]+\.[0-9]+$`)
	exactVersion   = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+$`)
)

// pinKeys are the `with:` keys that name a toolchain; each is an exact version.
// cosign is no longer one: proof-host downloads its binary by URL and hash
// through proof-fetch, and TestProofHost_CosignIsPinnedByReleaseAndByHash and
// proofHostProblems pin the pair.
var pinKeys = []string{"go-version", "bun-version", "node-version"}

// pinProblems holds steps to the pin rule: no floating ref. Every action is
// named by an exact release, and every toolchain in pinKeys by an exact
// version, Go's also on go.mod's minor line. seen counts the pins it judged by
// key, so a caller can refuse a check that judged nothing.
func pinProblems(where string, steps []wfStep, goMinor string, seen map[string]int) []string {
	var p []string
	for i, s := range steps {
		if s.Uses != "" && !strings.HasPrefix(s.Uses, "./") {
			seen["uses"]++
			if !exactActionRef.MatchString(s.Uses) {
				p = append(p, fmt.Sprintf("%s %s uses %q, not an exact vMAJOR.MINOR.PATCH release", where, s.label(i), s.Uses))
			}
		}
		for _, key := range pinKeys {
			v := withString(s, key)
			if v == "" {
				continue
			}
			seen[key]++
			if !exactVersion.MatchString(v) {
				p = append(p, fmt.Sprintf("%s %s pins %s %q, not an exact version", where, s.label(i), key, v))
			}
			if key == "go-version" && !strings.HasPrefix(v, goMinor+".") {
				p = append(p, fmt.Sprintf("%s %s pins Go %s, and go.mod says go %s", where, s.label(i), v, goMinor))
			}
		}
	}
	return p
}

func goModMinor(t *testing.T) string {
	t.Helper()
	goLine := regexp.MustCompile(`(?m)^go ([0-9]+\.[0-9]+)`).FindStringSubmatch(readRepoFileForPins(t, "go.mod"))
	if goLine == nil {
		t.Fatal("go.mod has no go line")
	}
	return goLine[1]
}

// TestWorkflows_ActionsAndToolchainsArePinnedExactly: no floating ref. Every
// action is named by an exact release of its latest major (Dependabot moves
// them, one pull request and one proof at a time), and every toolchain by an
// exact version: the Go patch of go.mod's line, Bun's own release, the Node
// release the site is built on, cosign's.
func TestWorkflows_ActionsAndToolchainsArePinnedExactly(t *testing.T) {
	goMinor := goModMinor(t)
	seen := map[string]int{}
	for _, file := range allWorkflows {
		w := loadWorkflow(t, file)
		for _, name := range w.jobNames() {
			expectNoProblems(t, file, pinProblems(file+" › "+name, w.Jobs[name].Steps, goMinor, seen))
		}
	}
	for _, a := range compositeActions {
		expectNoProblems(t, "the "+a+" action", pinProblems("the "+a+" action", compositeSteps(t, a), goMinor, seen))
	}
	for _, key := range append([]string{"uses"}, pinKeys...) {
		if seen[key] == 0 {
			t.Errorf("no step names %s: the pin check is vacuous", key)
		}
	}
}

func TestWorkflows_ThePinRuleRefusesFloatingRefsAndVersions(t *testing.T) {
	goMinor := goModMinor(t)
	stepUsing := func(w *wfFile, job, action string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if strings.HasPrefix(w.Jobs[job].Steps[i].Uses, action+"@") {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s uses no %s", job, action)
		return nil
	}
	rule := func(file string) func(*wfFile) []string {
		return func(w *wfFile) []string {
			var p []string
			for _, name := range w.jobNames() {
				p = append(p, pinProblems(file+" › "+name, w.Jobs[name].Steps, goMinor, map[string]int{})...)
			}
			return p
		}
	}
	expectEachMutationRefused(t, "site.yml", rule("site.yml"), []mutation{
		{"node on a major", func(w *wfFile) { stepUsing(w, "build", "actions/setup-node").With["node-version"] = "26" }},
		{"node on a wildcard", func(w *wfFile) { stepUsing(w, "build", "actions/setup-node").With["node-version"] = "26.x" }},
		{"node on lts", func(w *wfFile) { stepUsing(w, "build", "actions/setup-node").With["node-version"] = "lts/*" }},
		{"node on latest", func(w *wfFile) { stepUsing(w, "build", "actions/setup-node").With["node-version"] = "latest" }},
		{"bun on latest", func(w *wfFile) { stepUsing(w, "build", "oven-sh/setup-bun").With["bun-version"] = "latest" }},
		{"an action on a major", func(w *wfFile) { stepUsing(w, "build", "actions/checkout").Uses = "actions/checkout@v7" }},
		{"an action on a branch", func(w *wfFile) { stepUsing(w, "deploy", "actions/deploy-pages").Uses = "actions/deploy-pages@main" }},
		{"the pages upload on a branch", func(w *wfFile) {
			stepUsing(w, "build", "actions/upload-pages-artifact").Uses = "actions/upload-pages-artifact@main"
		}},
	})
	expectEachMutationRefused(t, "ci.yml", rule("ci.yml"), []mutation{
		{"go on another minor line", func(w *wfFile) { stepUsing(w, "check", "actions/setup-go").With["go-version"] = "1.26.0" }},
		{"go on a minor", func(w *wfFile) { stepUsing(w, "check", "actions/setup-go").With["go-version"] = goMinor }},
	})
}

func readRepoFileForPins(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ---- the artifacts carry records, not binaries ----------------------------------

// TestWorkflows_TheArtifactsCarryRecordsNotBinaries: a part's directory is
// mostly its bin/ — the stack binaries, proofrun and the adopted test
// binaries, about 500 MB a shard against 100 KB of records — and the proof
// job once copied every part whole into the merged proof, which made its
// upload 4.6 GB and timed it out at the step's limit after every part had
// passed (run 40365261654). Every part upload but the plan's now leaves its
// bin/ out (the plan's holds the compiled test binaries every shard adopts),
// and the merged proof is assembled by scripts/proof-collect.sh — each
// part's records, the events and logs only of parts that did not pass, and
// GitHub's job records — after the merger has read the parts
// (TestRepositoryProofCollect pins what the script keeps).
func TestWorkflows_TheArtifactsCarryRecordsNotBinaries(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	var merge *wfStep
	for i := range w.Jobs["proof"].Steps {
		if w.Jobs["proof"].Steps[i].Name == "merge" {
			merge = &w.Jobs["proof"].Steps[i]
		}
	}
	if merge == nil {
		t.Fatal("jobs.proof has no merge step")
	}
	if strings.Contains(merge.Run, `cp -r "$RUNNER_TEMP/ci/parts/."`) || strings.Contains(merge.Run, "rsync ") {
		t.Error("jobs.proof merge copies the parts itself; scripts/proof-collect.sh decides what the merged proof carries")
	}
	if !strings.Contains(merge.Run, `scripts/proof-collect.sh "$RUNNER_TEMP/ci/parts" "$RUNNER_TEMP/ci/jobs" "$RUNNER_TEMP/ci/proof"`) {
		t.Error("jobs.proof merge does not collect the parts' records and GitHub's job records with scripts/proof-collect.sh")
	}
	if at := strings.Index(merge.Run, "proofrun merge"); at < 0 || strings.Index(merge.Run, "scripts/proof-collect.sh") < at {
		t.Error("jobs.proof merge collects the parts before the merger has read them")
	}
	uploads := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.HasPrefix(s.Uses, "actions/upload-artifact@") {
				continue
			}
			uploads++
			var lines []string
			for _, l := range strings.Split(withString(s, "path"), "\n") {
				if l = strings.TrimSpace(l); l != "" {
					lines = append(lines, l)
				}
			}
			switch name {
			case "plan":
				if len(lines) != 1 || strings.HasPrefix(lines[0], "!") {
					t.Errorf("jobs.plan %s uploads %q; the plan's bin/ holds the test binaries the shards adopt", s.label(i), lines)
				}
			case "proof":
				if len(lines) != 1 || !strings.HasSuffix(lines[0], "/ci/proof") {
					t.Errorf("jobs.proof %s uploads %q, not the merged proof alone", s.label(i), lines)
				}
			default:
				if len(lines) != 2 || !strings.HasPrefix(lines[0], "proof/") || !strings.HasSuffix(lines[0], "/") ||
					lines[1] != "!"+lines[0]+"bin/**" {
					t.Errorf("jobs.%s %s uploads %q; want its directory and !<directory>bin/**", name, s.label(i), lines)
				}
			}
		}
	}
	if uploads < 8 {
		t.Errorf("%d upload steps checked; the proof job and every part upload", uploads)
	}
}
