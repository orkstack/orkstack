package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A part whose runner measured below the floor stops before any gate and
// leaves its job red. On GitHub it also leaves one annotation of fixed words,
// the only way the rerun planner (rerunplan.go) can tell that job from a failed
// one without downloading its artifact. The annotation is evidence for a
// decision to run again, never for a verdict: the merger re-derives
// degradation from runner-health.json, so a forged annotation can cost one
// wasted attempt and nothing else.

// belowFloor runs scripts/proof-part.sh's part_runner_health for part, with a
// proofrun stand-in that measured exit status code, in an environment that
// sets GITHUB_ACTIONS to github ("" leaves it unset). It returns the
// function's standard output and exit status.
func belowFloor(t *testing.T, part, github string, code int) (string, int) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	dir := t.TempDir()
	pr := filepath.Join(dir, "proofrun")
	script := "#!/bin/sh\nexit " + string(rune('0'+code)) + "\n"
	if err := os.WriteFile(pr, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, "-c", `source "$PART_SH" && part_runner_health`)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "PART_SH=" + filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"),
		"PR=" + pr, "DIR=" + dir, "PART=" + part,
	}
	if github != "" {
		cmd.Env = append(cmd.Env, "GITHUB_ACTIONS="+github)
	}
	cmd.Dir = dir
	out, err := cmd.Output()
	status := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		status = ee.ExitCode()
	}
	return string(out), status
}

func TestProofPart_ABelowFloorStackPartOnGitHubAnnotatesWithFixedWords(t *testing.T) {
	for _, part := range []string{"integration:1", "ui-e2e:3", "activity-fixture:git", "observability"} {
		out, status := belowFloor(t, part, "true", 1)
		if status != 1 {
			t.Errorf("%s: part_runner_health exits %d, want 1 (the part stops)", part, status)
		}
		want := "::error::" + runnerBelowFloorAnnotation + "\n"
		if strings.Count(out, "::error::") != 1 || !strings.Contains(out, "\n"+want) && !strings.HasPrefix(out, want) {
			t.Errorf("%s: output has no line %q:\n%s", part, strings.TrimSuffix(want, "\n"), out)
		}
	}
}

func TestProofPart_TheAnnotationIsOnGitHubAndInTheStoppingBranchOnly(t *testing.T) {
	for _, c := range []struct {
		name         string
		part, github string
		code         int
		status       int
	}{
		{"a local proof", "integration:1", "", 1, 1},
		{"GITHUB_ACTIONS false", "integration:1", "false", 1, 1},
		{"a part the floor does not bind", "plan", "true", 1, 0},
		{"the build part", "build", "true", 1, 0},
		{"the fast part", "fast", "true", 1, 0},
		{"a runner above the floor", "integration:1", "true", 0, 0},
		{"a measurement that failed", "integration:1", "true", 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, status := belowFloor(t, c.part, c.github, c.code)
			if status != c.status {
				t.Errorf("exits %d, want %d:\n%s", status, c.status, out)
			}
			if strings.Contains(out, "::error") || strings.Contains(out, "::warning") || strings.Contains(out, runnerBelowFloorAnnotation) {
				t.Errorf("a workflow command or the marker in the output:\n%s", out)
			}
		})
	}
}

// The annotation is the only workflow command the script prints, and it
// carries no variable text: the planner matches the whole message.
func TestProofPart_TheAnnotationIsTheOnlyWorkflowCommandAndHasNoVariableText(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	if n := strings.Count(text, "::error"); n != 1 {
		t.Errorf("scripts/proof-part.sh prints %d ::error commands, want the one", n)
	}
	for _, other := range []string{"::warning", "::notice", "::set-output", "::add-mask", "::stop-commands", "::group"} {
		if strings.Contains(text, other) {
			t.Errorf("scripts/proof-part.sh prints %s", other)
		}
	}
	if strings.ContainsAny(runnerBelowFloorAnnotation, "$`\\%\n") {
		t.Errorf("the annotation %q has variable or escaped text", runnerBelowFloorAnnotation)
	}
}
