package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A CI part's gate writes everything it prints to its log file, so the job's
// own log, which GitHub keeps when the runner is lost and the part's
// artifact is not, held none of the integration gate's `proofrun: start` and
// `proofrun: outcome` lines: the lines the merger and the rerun planner read
// a lost or cancelled shard from (classify, logProgress). Run 40965510239's
// cancelled integration (1) was twelve minutes into
// TestIntegration_SoakTasks, and the merger, reading neither a line nor an
// event, said it was "cancelled before any test started". The integration
// gate now also prints those lines to the job's log as they happen.

// partRun runs scripts/proof-part.sh's part_run for the integration gate, as
// part_main calls it, over a command that prints script, in a fresh part
// directory. It returns the function's standard output, its exit status and
// the directory.
func partRun(t *testing.T, script string) (string, int, string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	dir := t.TempDir()
	gate := filepath.Join(t.TempDir(), "gate.sh")
	if err := os.WriteFile(gate, []byte(script+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	call := partIntegrationCall(t)
	cmd := exec.Command(bash, "-c", `source "$PART_SH" && `+call+` sh "$GATE"`)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "PART_SH=" + filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"),
		"DIR=" + dir, "GATE=" + gate,
	}
	out, err := cmd.Output()
	status := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		status = ee.ExitCode()
	}
	return string(out), status, dir
}

// partIntegrationCall is how part_main runs the integration gate, up to the
// command: whatever it sets before part_run, and part_run's gate and log.
func partIntegrationCall(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*((?:[A-Z_]+='[^']*' )*part_run integration integration\.log) env `).FindStringSubmatch(string(b))
	if m == nil {
		t.Fatal("scripts/proof-part.sh runs no integration gate through part_run")
	}
	return m[1]
}

func TestProofPart_TheIntegrationGatePrintsItsProgressToTheJobLog(t *testing.T) {
	out, status, dir := partRun(t, `echo "go: downloading something"
echo "proofrun: start example.com/m TestA"
echo "start engine-001 slot 1: example.com/m TestA (estimated 3s, timeout 10m0s)"
echo "proofrun: outcome pass example.com/m TestA 1.00"
echo "proofrun: start example.com/m TestB"`)
	if status != 0 {
		t.Fatalf("part_run exits %d:\n%s", status, out)
	}
	for _, want := range []string{"proofrun: start example.com/m TestA\n", "proofrun: outcome pass example.com/m TestA 1.00\n", "proofrun: start example.com/m TestB\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("the job log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "go: downloading") || strings.Contains(out, "slot 1") {
		t.Errorf("the job log carries the gate's other output, which stays in its log file:\n%s", out)
	}
	if failed, unfinished := logProgress(out); len(failed) != 0 || len(unfinished) != 1 || unfinished[0] != "example.com/m.TestB" {
		t.Errorf("the merger reads failed %v unfinished %v from the job log, want TestB unfinished", failed, unfinished)
	}
	b, err := os.ReadFile(filepath.Join(dir, "integration.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "go: downloading something") || !strings.Contains(string(b), "proofrun: start example.com/m TestB") {
		t.Errorf("integration.log does not hold the gate's whole output:\n%s", b)
	}
	if g, _ := os.ReadFile(filepath.Join(dir, "gates.tsv")); !strings.HasPrefix(string(g), "integration\tpass\t") {
		t.Errorf("gates.tsv = %q", g)
	}
}

// The gate's own exit status is the gate's result, whatever the progress
// filter matched.
func TestProofPart_TheIntegrationGateKeepsItsExitStatus(t *testing.T) {
	for _, c := range []struct {
		script string
		status int
	}{
		{`echo "proofrun: outcome fail example.com/m TestA 1.00"; exit 1`, 1},
		{`echo "nothing the merger reads"; exit 3`, 3},
		{`echo "nothing the merger reads"`, 0},
	} {
		out, status, dir := partRun(t, c.script)
		if status != c.status {
			t.Errorf("%q: part_run exits %d, want %d:\n%s", c.script, status, c.status, out)
		}
		g, _ := os.ReadFile(filepath.Join(dir, "gates.tsv"))
		want := "integration\tpass\t"
		if c.status != 0 {
			want = "integration\tfail\t"
		}
		if !strings.HasPrefix(string(g), want) {
			t.Errorf("%q: gates.tsv = %q, want %q", c.script, g, want)
		}
	}
}
