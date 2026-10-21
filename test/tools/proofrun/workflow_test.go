package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The workflows are the merge gate's mechanism. These tests pin the shape the
// plan fixes: the real proof on pull requests, `always()` on the one required
// check, per-job permissions, artifact-name outputs, and a step timeout under
// every job timeout (plan §2.8, §3.2).

func workflowFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests(t *testing.T) {
	s := workflowFile(t, "proof.yml")
	for _, want := range []string{
		"pull_request:",
		"merge_group:",
		"workflow_call:",
		"if: always()",
		"actions: read",
		"checks: read",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("proof.yml lacks %q", want)
		}
	}
	// The required check runs on every pull request, draft guard aside, and
	// on every merge group. A heavy job may not narrow itself to one event.
	heavy := regexp.MustCompile(`(?m)^  (plan|build|fast|integration|ui-e2e|activity-fixture|observability):`)
	if !heavy.MatchString(s) {
		t.Fatal("proof.yml holds no heavy job")
	}
	if strings.Contains(s, "if: github.event_name == 'merge_group'") {
		t.Error("a heavy job runs only on merge_group")
	}
	// A pull request must not trigger on `labeled`: a label event would land
	// in the concurrency group and cancel a running proof.
	prTypes := regexp.MustCompile(`(?s)pull_request:\s*\n\s*types:\s*\[([^\]]*)\]`).FindStringSubmatch(s)
	if prTypes == nil {
		t.Fatal("proof.yml has no pull_request types")
	}
	if strings.Contains(prTypes[1], "labeled") {
		t.Errorf("proof.yml triggers on labeled: %s", prTypes[1])
	}
}

// TestWorkflows_EveryProofStepTimesOutBeforeItsJob holds every job to the
// plan's rule (§2.6 rule 2, §3.2): a job's limit is at least the sum of its
// steps' own limits plus five minutes, every step before a `scripts/proof.sh`
// step has a limit of its own, and a job that only calls a reusable workflow
// is exempt because that workflow's jobs carry theirs. A stuck shard must
// therefore stop at its proof step's limit, with its `if: always()` upload
// still to run, never at the job's — which would take the upload with it.
func TestWorkflows_EveryProofStepTimesOutBeforeItsJob(t *testing.T) {
	type step struct {
		uses    string
		run     string
		timeout int
		line    int
	}
	type job struct {
		name     string
		timeout  int
		steps    []step
		reusable bool
	}
	for _, name := range []string{"proof.yml", "ci.yml", "nightly.yml", "release.yml"} {
		lines := strings.Split(workflowFile(t, name), "\n")
		var jobs []job
		var cur *job
		var curStep *step
		inSteps := false
		flushStep := func() {
			if curStep != nil && cur != nil {
				cur.steps = append(cur.steps, *curStep)
				curStep = nil
			}
		}
		flushJob := func() {
			flushStep()
			if cur != nil {
				jobs = append(jobs, *cur)
				cur = nil
			}
			inSteps = false
		}
		jobHead := regexp.MustCompile(`^  ([a-zA-Z0-9_-]+):\s*$`)
		stepHead := regexp.MustCompile(`^      - (?:uses|name):\s*(.+)$`)
		stepTimeout := regexp.MustCompile(`^        timeout-minutes:\s*([0-9]+)`)
		jobTimeout := regexp.MustCompile(`^    timeout-minutes:\s*([0-9]+)`)
		stepUses := regexp.MustCompile(`^        uses:\s*(.+)$`)
		stepRun := regexp.MustCompile(`^        run:\s*(.*)$`)
		for n, line := range lines {
			if m := jobHead.FindStringSubmatch(line); m != nil {
				flushJob()
				cur = &job{name: m[1]}
				continue
			}
			if cur == nil {
				continue
			}
			if strings.HasPrefix(line, "    steps:") {
				flushStep()
				inSteps = true
				continue
			}
			if !inSteps {
				if m := jobTimeout.FindStringSubmatch(line); m != nil {
					cur.timeout, _ = strconv.Atoi(m[1])
				}
				if strings.HasPrefix(line, "    uses: ./.github/workflows/") {
					cur.reusable = true
				}
				continue
			}
			if m := stepHead.FindStringSubmatch(line); m != nil {
				flushStep()
				curStep = &step{line: n + 1}
				continue
			}
			if curStep == nil {
				continue
			}
			if m := stepTimeout.FindStringSubmatch(line); m != nil {
				curStep.timeout, _ = strconv.Atoi(m[1])
			}
			if m := stepUses.FindStringSubmatch(line); m != nil {
				curStep.uses = strings.TrimSpace(m[1])
			}
			if m := stepRun.FindStringSubmatch(line); m != nil {
				curStep.run = strings.TrimSpace(m[1])
				continue
			}
			if curStep.run != "" {
				if indent := len(line) - len(strings.TrimLeft(line, " ")); indent >= 10 && strings.TrimSpace(line) != "" {
					curStep.run += "\n" + strings.TrimSpace(line)
				}
			}
		}
		flushJob()
		for _, j := range jobs {
			if j.reusable {
				continue
			}
			sum, proofAt := 0, -1
			for i, s := range j.steps {
				sum += s.timeout
				if proofAt < 0 && strings.Contains(s.run, "scripts/proof.sh") {
					proofAt = i
				}
			}
			if proofAt >= 0 {
				for _, s := range j.steps[:proofAt] {
					if s.timeout == 0 {
						t.Errorf("%s: job %s's step at line %d runs before the proof step and has no timeout-minutes",
							name, j.name, s.line)
					}
				}
			}
			if j.timeout == 0 || sum == 0 {
				continue
			}
			if j.timeout < sum+5 {
				t.Errorf("%s: job %s times out at %d, and its steps sum to %d (needs %d)",
					name, j.name, j.timeout, sum, sum+5)
			}
		}
	}
}

func TestWorkflows_ShardsFindThePlanAndBuildByTheirOutputs(t *testing.T) {
	s := workflowFile(t, "proof.yml")
	for _, want := range []string{
		"plan_artifact: plan-a${{ github.run_attempt }}",
		"build_artifact: part-build-a${{ github.run_attempt }}",
		"name: ${{ needs.plan.outputs.plan_artifact }}",
		"name: ${{ needs.build.outputs.build_artifact }}",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("proof.yml lacks %q", want)
		}
	}
}

func TestWorkflows_PullRequestCheckoutsDoNotPersistCredentials(t *testing.T) {
	for _, name := range []string{"proof.yml", "ci.yml"} {
		s := workflowFile(t, name)
		blocks := regexp.MustCompile(`(?s)- uses: actions/checkout@[^\n]*\n(.*?)(\n      - |\n  [a-z]|\z)`).FindAllStringSubmatch(s, -1)
		for _, b := range blocks {
			if !strings.Contains(b[1], "persist-credentials: false") {
				t.Errorf("%s: a checkout does not set persist-credentials: false:\n%s", name, b[0])
			}
		}
	}
}

// callerWorkflow is one workflow that calls another: the inputs it declares,
// its jobs and the permissions each job grants.
type callerWorkflow struct {
	On struct {
		WorkflowCall struct {
			Inputs map[string]any `yaml:"inputs"`
		} `yaml:"workflow_call"`
	} `yaml:"on"`
	Jobs map[string]struct {
		Uses        string            `yaml:"uses"`
		Permissions map[string]string `yaml:"permissions"`
		With        map[string]any    `yaml:"with"`
	} `yaml:"jobs"`
}

func parseWorkflow(t *testing.T, name string) callerWorkflow {
	t.Helper()
	var w callerWorkflow
	if err := yaml.Unmarshal([]byte(workflowFile(t, name)), &w); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return w
}

// TestWorkflows_CallersGrantWhatTheirCalleesNeed: a reusable workflow's jobs
// cannot ask for a permission their caller did not grant, and a caller cannot
// pass an input the callee does not declare. Without this, nightly.yml and
// release.yml would fail at startup on a permission or an input typo, and
// nothing local would notice.
func TestWorkflows_CallersGrantWhatTheirCalleesNeed(t *testing.T) {
	var callee struct {
		On struct {
			WorkflowCall struct {
				Inputs map[string]any `yaml:"inputs"`
			} `yaml:"workflow_call"`
		} `yaml:"on"`
		Jobs map[string]struct {
			Permissions map[string]string `yaml:"permissions"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(workflowFile(t, "proof.yml")), &callee); err != nil {
		t.Fatal(err)
	}
	needed := map[string]bool{}
	for _, job := range callee.Jobs {
		for p := range job.Permissions {
			needed[p] = true
		}
	}
	if len(needed) == 0 {
		t.Fatal("proof.yml declares no job permissions")
	}
	inputs := map[string]bool{}
	for name := range callee.On.WorkflowCall.Inputs {
		inputs[name] = true
	}
	for _, name := range []string{"nightly.yml", "release.yml"} {
		w := parseWorkflow(t, name)
		calls := 0
		for jobName, job := range w.Jobs {
			if job.Uses != "./.github/workflows/proof.yml" {
				continue
			}
			calls++
			for p := range needed {
				if _, ok := job.Permissions[p]; !ok {
					t.Errorf("%s › %s calls proof.yml without granting %s", name, jobName, p)
				}
			}
			for input := range job.With {
				if !inputs[input] {
					t.Errorf("%s › %s passes input %q, which proof.yml does not declare", name, jobName, input)
				}
			}
		}
		if calls == 0 {
			t.Errorf("%s does not call proof.yml", name)
		}
	}
}

// TestWorkflows_OnlyPullRequestRunsShareAConcurrencyGroup: a group per tier
// and event, and only a pull request cancels a run. Anything else — a merge
// group, a dispatch, a nightly, a release — is a group of its own, so nothing
// displaces a proof that is already running.
func TestWorkflows_OnlyPullRequestRunsShareAConcurrencyGroup(t *testing.T) {
	var w struct {
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress string `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
	}
	if err := yaml.Unmarshal([]byte(workflowFile(t, "proof.yml")), &w); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(w.Concurrency.Group, "inputs.tier") ||
		!strings.Contains(w.Concurrency.Group, "github.event_name") ||
		!strings.Contains(w.Concurrency.Group, "github.event.pull_request.number || github.run_id") {
		t.Errorf("concurrency group %q does not name the tier, the event and a per-run fallback", w.Concurrency.Group)
	}
	if w.Concurrency.CancelInProgress != "${{ github.event_name == 'pull_request' }}" {
		t.Errorf("cancel-in-progress = %q, want only pull requests", w.Concurrency.CancelInProgress)
	}
}

// TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst: release.yml creates and
// pushes a tag only after `proofrun verify -release` accepted a passing
// release proof, and no earlier step touches a tag.
func TestWorkflows_TheTagJobVerifiesTheReleaseProofFirst(t *testing.T) {
	var w struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string `yaml:"name"`
				Uses string `yaml:"uses"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(workflowFile(t, "release.yml")), &w); err != nil {
		t.Fatal(err)
	}
	tag, ok := w.Jobs["tag"]
	if !ok {
		t.Fatal("release.yml has no tag job")
	}
	verifyAt, tagAt := -1, -1
	for i, step := range tag.Steps {
		switch {
		case strings.Contains(step.Run, "proofrun verify -release"):
			verifyAt = i
		case strings.Contains(step.Run, "git tag"):
			tagAt = i
		}
		if tagAt >= 0 && strings.Contains(step.Run, "git push") && i < verifyAt {
			t.Errorf("step %q pushes a tag before the proof is verified", step.Name)
		}
	}
	if verifyAt < 0 {
		t.Fatal("the tag job never runs `proofrun verify -release`")
	}
	if tagAt < 0 || tagAt < verifyAt {
		t.Fatalf("the tag job creates the tag at step %d, and verifies the proof at %d", tagAt, verifyAt)
	}
	if !strings.Contains(strings.Join([]string{tag.Steps[tagAt].Run, stepsRun(tag.Steps)}, "\n"), "git push") {
		t.Error("the tag job never pushes the tag it created")
	}
}

func stepsRun(steps []struct {
	Name string `yaml:"name"`
	Uses string `yaml:"uses"`
	Run  string `yaml:"run"`
}) string {
	var out []string
	for _, s := range steps {
		out = append(out, s.Run)
	}
	return strings.Join(out, "\n")
}
