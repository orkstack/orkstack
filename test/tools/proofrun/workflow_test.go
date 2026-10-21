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

var allWorkflows = []string{"proof.yml", "ci.yml", "nightly.yml", "release.yml"}

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

// proofHostSteps are the composite action's steps.
func proofHostSteps(t *testing.T) []wfStep {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "actions", "proof-host", "action.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var a struct {
		Runs struct {
			Steps []wfStep `yaml:"steps"`
		} `yaml:"runs"`
	}
	if err := yaml.Unmarshal(b, &a); err != nil {
		t.Fatalf("proof-host action: %v", err)
	}
	if len(a.Runs.Steps) == 0 {
		t.Fatal("the proof-host action has no steps")
	}
	return a.Runs.Steps
}

func isCheckout(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/checkout@") }

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

// The guard every proof.yml job but `proof` carries (plan §3.2): a draft runs
// nothing heavy, and while the repository is private a pull request needs the
// ci:proof label.
var draftGuardParts = []string{
	"github.event_name != 'pull_request'",
	"!github.event.pull_request.draft",
	"github.event.repository.private == false",
	"contains(github.event.pull_request.labels.*.name, 'ci:proof')",
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
		for _, part := range draftGuardParts {
			if !strings.Contains(job.If, part) {
				p = append(p, fmt.Sprintf("jobs.%s.if lacks the draft and ci:proof guard (%s)", name, part))
				break
			}
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

// TestWorkflows_TheVerdictJudgesEachTierByItsOwnRule: a full proof must be
// merge-eligible; a release proof is judged by `proofrun verify -release`,
// the rule the tag applies; a nightly by its result.
func TestWorkflows_TheVerdictJudgesEachTierByItsOwnRule(t *testing.T) {
	steps := loadWorkflow(t, "proof.yml").Jobs["proof"].Steps
	if len(steps) == 0 {
		t.Fatal("jobs.proof has no steps")
	}
	last := steps[len(steps)-1]
	if last.If != "" {
		t.Errorf("the verdict step runs under %q; it must run whenever the job does", last.If)
	}
	for _, want := range []string{
		`.result == "pass" and .merge_eligible == true`,
		"proofrun verify -release -proof",
		`nightly)`,
		`.result == "pass" and .eligible_but_for_tier == true`,
		"exit 1",
	} {
		if !strings.Contains(last.Run, want) {
			t.Errorf("the verdict step lacks %q:\n%s", want, last.Run)
		}
	}
	if strings.Contains(last.Run, `else .result == "pass" end`) {
		t.Error("the verdict step still judges release by its result alone")
	}
	if strings.Contains(last.Run, `jq -e '.result == "pass"' `) {
		t.Error("the verdict step judges a tier by its result alone")
	}
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

	j = decode("annotations: the API call failed")
	if j.ReadError == "" {
		t.Fatal("a failed fetch leaves no read_error")
	}
	if class, _ := classify(&j, partReading{}); class != attemptResult {
		t.Errorf("an unreadable record is classified %v, want a result (fails closed)", class)
	}
}

// TestWorkflows_EveryPartsProofStepIsOneTheMergerKnows: the merger decides
// whether a part's proof step started by the step's name, so every step that
// runs `scripts/proof.sh --part` carries a name it knows.
func TestWorkflows_EveryPartsProofStepIsOneTheMergerKnows(t *testing.T) {
	w := loadWorkflow(t, "proof.yml")
	seen := 0
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.Contains(s.Run, "scripts/proof.sh --part") {
				continue
			}
			seen++
			if !proofStepNames[s.Name] {
				t.Errorf("jobs.%s %s runs a part under a name the merger does not know", name, s.label(i))
			}
		}
	}
	if seen == 0 {
		t.Fatal("no step runs scripts/proof.sh --part")
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
			if !strings.HasPrefix(s.Uses, "actions/download-artifact@") {
				continue
			}
			art := withString(s, "name")
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
			// release.yml's tag job is the one checkout that pushes.
			if file == "release.yml" && name == "tag" {
				continue
			}
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
	})
}

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
		touches := strings.Contains(s.Run, "git tag") || strings.Contains(s.Run, "git push")
		if touches && (verifyAt < 0 || i <= verifyAt) {
			p = append(p, fmt.Sprintf("%s touches a tag before the proof is verified", s.label(i)))
		}
		if tagAt < 0 && strings.Contains(s.Run, "git tag") {
			tagAt = i
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
	}
	return p
}

// TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst: release.yml creates and
// pushes a tag only after `proofrun verify -release` accepted a passing
// release proof of the commit it checked out; no earlier step touches a tag.
func TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst(t *testing.T) {
	expectEachMutationRefused(t, "release.yml", tagProblems, []mutation{
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
	})
}

// ---- shells and pins ---------------------------------------------------------------

// untrustedInShell names the expressions a `run:` may never splice into its
// script: a workflow input or event field is text a person typed, and the
// shell would read it as code. They reach a step through `env:`.
var untrustedInShell = regexp.MustCompile(`\$\{\{\s*(inputs\.|github\.event\.|github\.head_ref)`)

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
	check("the proof-host action", proofHostSteps(t))
	if untrustedInShell.FindString(`run: echo "${{ inputs.version }}"`) == "" {
		t.Error("the pattern misses an interpolated input")
	}
}

var (
	exactActionRef = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@v[0-9]+\.[0-9]+\.[0-9]+$`)
	exactVersion   = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+$`)
)

// TestWorkflows_ActionsAndToolchainsArePinnedExactly: no floating ref. Every
// action is named by an exact release of its latest major (Dependabot moves
// them, one pull request and one proof at a time), and every toolchain by an
// exact version: the Go patch of go.mod's line, Bun's own release, cosign's.
func TestWorkflows_ActionsAndToolchainsArePinnedExactly(t *testing.T) {
	gomod := readRepoFileForPins(t, "go.mod")
	goLine := regexp.MustCompile(`(?m)^go ([0-9]+\.[0-9]+)`).FindStringSubmatch(gomod)
	if goLine == nil {
		t.Fatal("go.mod has no go line")
	}
	seen := map[string]int{}
	check := func(where string, steps []wfStep) {
		for i, s := range steps {
			if s.Uses != "" && !strings.HasPrefix(s.Uses, "./") {
				seen["uses"]++
				if !exactActionRef.MatchString(s.Uses) {
					t.Errorf("%s %s uses %q, not an exact vMAJOR.MINOR.PATCH release", where, s.label(i), s.Uses)
				}
			}
			for _, key := range []string{"go-version", "bun-version", "cosign-release"} {
				v := withString(s, key)
				if v == "" {
					continue
				}
				seen[key]++
				if !exactVersion.MatchString(v) {
					t.Errorf("%s %s pins %s %q, not an exact version", where, s.label(i), key, v)
				}
				if key == "go-version" && !strings.HasPrefix(v, goLine[1]+".") {
					t.Errorf("%s %s pins Go %s, and go.mod says go %s", where, s.label(i), v, goLine[1])
				}
			}
		}
	}
	for _, file := range allWorkflows {
		w := loadWorkflow(t, file)
		for _, name := range w.jobNames() {
			check(file+" › "+name, w.Jobs[name].Steps)
		}
	}
	check("the proof-host action", proofHostSteps(t))
	for _, key := range []string{"uses", "go-version", "bun-version", "cosign-release"} {
		if seen[key] == 0 {
			t.Errorf("no step names %s: the pin check is vacuous", key)
		}
	}
}

func readRepoFileForPins(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
