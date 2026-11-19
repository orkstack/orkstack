package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// proofWorkflow is .github/workflows/proof.yml, decoded enough to read a
// job's steps.
type proofWorkflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Name string            `yaml:"name"`
			Uses string            `yaml:"uses"`
			With map[string]string `yaml:"with"`
			Run  string            `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func readProofWorkflow(t *testing.T) proofWorkflow {
	t.Helper()
	root, err := repoRoot()
	must.OK(t, err)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "proof.yml"))
	must.OK(t, err)
	var w proofWorkflow
	must.OK(t, yaml.Unmarshal(raw, &w))
	return w
}

// TestProofWorkflow_MergeJobHasTheBaseCommit: the merge job checks the
// base's copy of the GPU-host list against the base commit itself
// (gpuBaseCopyProblems), so its checkout holds the whole history; a shallow
// one fails every proof whose tree carries the list.
func TestProofWorkflow_MergeJobHasTheBaseCommit(t *testing.T) {
	w := readProofWorkflow(t)
	job, ok := w.Jobs["proof"]
	if !ok {
		t.Fatal("proof.yml has no proof job")
	}
	for _, step := range job.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") {
			if step.With["fetch-depth"] != "0" {
				t.Fatalf("the proof job's checkout has fetch-depth %q; want 0, the whole history", step.With["fetch-depth"])
			}
			return
		}
	}
	t.Fatal("the proof job checks out nothing")
}

// TestProofWorkflow_PushBaseIsTheMergeBaseWithMain runs the base job's own
// script, for a push, in a repository where the pushed branch is three
// commits on main: the base is main's commit, never the pushed commit's
// parent.
func TestProofWorkflow_PushBaseIsTheMergeBaseWithMain(t *testing.T) {
	w := readProofWorkflow(t)
	var script string
	for _, step := range w.Jobs["base"].Steps {
		if step.Name == "base" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("proof.yml's base job has no base step")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t", "GIT_CONFIG_GLOBAL=/dev/null")
		out, err := cmd.CombinedOutput()
		must.OKf(t, err, "git %v: %s", args, out)
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "--no-gpg-sign", "-m", "main")
	mainSHA := git("rev-parse", "HEAD")
	git("update-ref", "refs/remotes/origin/main", mainSHA)
	for _, m := range []string{"one", "two", "three"} {
		git("commit", "-q", "--allow-empty", "--no-gpg-sign", "-m", m)
	}
	out := filepath.Join(dir, "github-output")
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GITHUB_EVENT_NAME=push", "GITHUB_OUTPUT="+out, "BASE_INPUT=main", "GIT_CONFIG_GLOBAL=/dev/null")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the base step failed: %v\n%s", err, b)
	}
	got, err := os.ReadFile(out)
	must.OK(t, err)
	if !strings.Contains(string(got), "base="+mainSHA+"\n") {
		t.Fatalf("the base step wrote\n%s\nwant base=%s, main's commit, for a push three commits on it", got, mainSHA)
	}
}

// TestGPUBaseCopyProblems_MissingCommitFailsClosed: a base commit the
// repository does not hold is a problem naming it, never a copy taken on
// trust.
func TestGPUBaseCopyProblems_MissingCommitFailsClosed(t *testing.T) {
	copyFile := filepath.Join(t.TempDir(), gpuBaseFileName)
	must.OK(t, os.WriteFile(copyFile, nil, 0o644))
	const absent = "0123456789abcdef0123456789abcdef01234567"
	problems := gpuBaseCopyProblems(copyFile, absent)
	if len(problems) != 1 || !strings.Contains(problems[0], absent) || !strings.Contains(problems[0], "not in this repository") {
		t.Fatalf("problems = %q; want one naming the missing commit", problems)
	}
}
