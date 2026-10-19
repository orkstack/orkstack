package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
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

func TestWorkflows_EveryProofStepTimesOutBeforeItsJob(t *testing.T) {
	for _, name := range []string{"proof.yml", "ci.yml", "nightly.yml", "release.yml"} {
		s := workflowFile(t, name)
		lines := strings.Split(s, "\n")
		job, jobTimeout, stepSum, inSteps := "", 0, 0, false
		finish := func() {
			if job == "" || jobTimeout == 0 || stepSum == 0 {
				return
			}
			if jobTimeout < stepSum+5 {
				t.Errorf("%s: job %s times out at %d, and its steps sum to %d (needs %d)", name, job, jobTimeout, stepSum, stepSum+5)
			}
		}
		for _, line := range lines {
			if m := regexp.MustCompile(`^  ([a-zA-Z0-9_-]+):\s*$`).FindStringSubmatch(line); m != nil {
				finish()
				job, jobTimeout, stepSum, inSteps = m[1], 0, 0, false
				continue
			}
			if strings.HasPrefix(line, "    steps:") {
				inSteps = true
				continue
			}
			if m := regexp.MustCompile(`^    timeout-minutes:\s*([0-9]+)`).FindStringSubmatch(line); m != nil {
				jobTimeout, _ = strconv.Atoi(m[1])
				continue
			}
			if inSteps {
				if m := regexp.MustCompile(`^        timeout-minutes:\s*([0-9]+)`).FindStringSubmatch(line); m != nil {
					n, _ := strconv.Atoi(m[1])
					stepSum += n
				}
				if strings.HasPrefix(line, "      - uses: ./.github/workflows/") {
					// A reusable workflow's own jobs carry their limits.
					jobTimeout = 0
				}
			}
		}
		finish()
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
