package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
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
	return partRunAs(t, "", partIntegrationCall(t), script)
}

// partRunAs is partRun with prelude run first in the shell that sources the
// part (the shell options scripts/proof.sh sets before it does), and call in
// place of part_main's own call of part_run.
func partRunAs(t *testing.T, prelude, call, script string) (string, int, string) {
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
	cmd := exec.Command(bash, "-c", prelude+`source "$PART_SH" && `+call+` sh "$GATE"`)
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
// filter matched: a failure, a signal, or a pass whose filter matched no line
// (grep's exit 1). It holds with scripts/proof.sh's shell options too, which
// the part runs under, and without them.
func TestProofPart_TheIntegrationGateKeepsItsExitStatus(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	const proofOptions = "set -uo pipefail"
	if !strings.Contains(string(b), "\n"+proofOptions+"\n") {
		t.Fatalf("scripts/proof.sh no longer sets %q; this test runs the part under it", proofOptions)
	}
	for _, prelude := range []string{"", proofOptions + "; "} {
		for _, c := range []struct {
			script string
			status int
		}{
			{`echo "proofrun: outcome fail example.com/m TestA 1.00"; exit 1`, 1},
			{`echo "nothing the merger reads"; exit 3`, 3},
			{`exit 1`, 1},
			{`echo "proofrun: start example.com/m TestA"; kill -KILL $$`, 128 + int(syscall.SIGKILL)},
			{`echo "nothing the merger reads"`, 0},
			{`echo "proofrun: start example.com/m TestA"`, 0},
		} {
			out, status, dir := partRunAs(t, prelude, partIntegrationCall(t), c.script)
			if status != c.status {
				t.Errorf("%q%q: part_run exits %d, want %d:\n%s", prelude, c.script, status, c.status, out)
			}
			g, _ := os.ReadFile(filepath.Join(dir, "gates.tsv"))
			want := "integration\tpass\t"
			if c.status != 0 {
				want = "integration\tfail\t"
			}
			if !strings.HasPrefix(string(g), want) {
				t.Errorf("%q%q: gates.tsv = %q, want %q", prelude, c.script, g, want)
			}
		}
	}
}

// A stream filter that cannot run loses the lines the job's log exists to
// keep, so it fails a gate that passed, as a log tee cannot write does: the
// gate's result is never a pass the part could not stream.
func TestProofPart_TheIntegrationGateFailsWhenItsStreamCannotRun(t *testing.T) {
	// An extended regular expression grep refuses (exit 2).
	call := strings.Replace(partIntegrationCall(t), "PART_STREAM='^proofrun: (start|outcome) '", "PART_STREAM='('", 1)
	if call == partIntegrationCall(t) {
		t.Fatal("scripts/proof-part.sh's integration gate no longer sets the PART_STREAM this test replaces")
	}
	for _, script := range []string{`exit 0`, `echo "proofrun: start example.com/m TestA"`} {
		out, status, dir := partRunAs(t, "", call, script)
		if status == 0 {
			t.Errorf("%q: part_run passes a gate whose stream filter could not run:\n%s", script, out)
		}
		if g, _ := os.ReadFile(filepath.Join(dir, "gates.tsv")); !strings.HasPrefix(string(g), "integration\tfail\t") {
			t.Errorf("%q: gates.tsv = %q, want the gate failed", script, g)
		}
	}
}

// startPartRun starts part_run for the integration gate over script in its
// own process group, with dir as the part directory, and returns the command
// and a channel of the lines it prints, closed at their end.
func startPartRun(t *testing.T, dir, script string, env ...string) (*exec.Cmd, <-chan string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	gate := filepath.Join(t.TempDir(), "gate.sh")
	if err := os.WriteFile(gate, []byte(script+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, "-c", `source "$PART_SH" && `+partIntegrationCall(t)+` sh "$GATE"`)
	cmd.Env = append([]string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "PART_SH=" + filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"),
		"DIR=" + dir, "GATE=" + gate,
	}, env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 64)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	return cmd, lines
}

// awaitLine waits, up to a deadline, for a line that contains want.
func awaitLine(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("part_run's output ended without %q", want)
			}
			if strings.Contains(l, want) {
				return
			}
		case <-deadline:
			t.Fatalf("no line %q within 20s", want)
		}
	}
}

// awaitFile waits, up to a deadline, for path to exist.
func awaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear within 20s", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A progress line reaches the job's log while the gate still runs: a pipe
// would otherwise hold it until the gate's end, which a cancelled or lost
// shard never reaches.
func TestProofPart_TheIntegrationGateStreamsEachProgressLineAsItHappens(t *testing.T) {
	dir := t.TempDir()
	release := filepath.Join(t.TempDir(), "release")
	cmd, lines := startPartRun(t, dir, `echo "proofrun: start example.com/m TestA"
while [ ! -f "$RELEASE" ]; do sleep 0.05; done
echo "proofrun: outcome pass example.com/m TestA 1.00"`, "RELEASE="+release)
	awaitLine(t, lines, "proofrun: start example.com/m TestA")
	if err := os.WriteFile(release, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	awaitLine(t, lines, "proofrun: outcome pass example.com/m TestA")
	if err := cmd.Wait(); err != nil {
		t.Fatalf("part_run: %v", err)
	}
}

// The part forwards a signal to its whole process group, and proofrun
// integration shuts down on one by finishing its jobs and writing its events
// and ledger: the stream's filters must outlive the signal, or the gate dies
// of a broken pipe before it has written what it was shutting down to write.
func TestProofPart_TheIntegrationGateOutlivesASignalToItsGroup(t *testing.T) {
	dir := t.TempDir()
	tmp := t.TempDir()
	ready, done := filepath.Join(tmp, "ready"), filepath.Join(tmp, "done")
	cmd, lines := startPartRun(t, dir, `trap 'echo "proofrun: outcome fail example.com/m TestA 3.00"; echo shutting down; : >"$DONE"; exit 1' TERM
echo "proofrun: start example.com/m TestA"
: >"$READY"
while :; do sleep 0.05; done`, "READY="+ready, "DONE="+done)
	awaitLine(t, lines, "proofrun: start example.com/m TestA")
	awaitFile(t, ready)
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	awaitFile(t, done)
	_ = cmd.Wait()
	deadline := time.Now().Add(20 * time.Second)
	for {
		b, _ := os.ReadFile(filepath.Join(dir, "integration.log"))
		if strings.Contains(string(b), "shutting down") {
			if !strings.Contains(string(b), "proofrun: outcome fail example.com/m TestA") {
				t.Errorf("integration.log lacks the outcome the gate wrote as it shut down:\n%s", b)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("integration.log lacks what the gate wrote as it shut down:\n%s", b)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A NUL byte in the gate's output (a test that prints one) must not end the
// stream: grep would otherwise call the rest binary and print none of it.
func TestProofPart_TheIntegrationGateStreamsPastABinaryByte(t *testing.T) {
	// The gate passes, so part_run prints no tail of its log: what the job
	// log holds is the stream alone.
	out, status, _ := partRun(t, `printf 'junk\000junk\nproofrun: start example.com/m TestA\nproofrun: outcome fail example.com/m TestA 1.00\n'`)
	if status != 0 || !strings.Contains(out, "proofrun: start example.com/m TestA\n") || !strings.Contains(out, "proofrun: outcome fail example.com/m TestA 1.00\n") {
		t.Errorf("exit %d, the job log:\n%q", status, out)
	}
}

// A log the gate's output cannot be written to fails the gate, as the
// redirect it replaced did, whatever the gate itself exited.
func TestProofPart_TheIntegrationGateFailsWhenItsLogCannotBeWritten(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "integration.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, "-c", `source "$PART_SH" && `+partIntegrationCall(t)+` sh -c 'echo "proofrun: start example.com/m TestA"'`)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "PART_SH=" + filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"), "DIR=" + dir}
	err = cmd.Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
		t.Errorf("part_run exits %v, want a failure", err)
	}
	if g, _ := os.ReadFile(filepath.Join(dir, "gates.tsv")); !strings.HasPrefix(string(g), "integration\tfail\t") {
		t.Errorf("gates.tsv = %q, want the gate failed", g)
	}
}
