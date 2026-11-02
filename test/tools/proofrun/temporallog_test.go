package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A proof part's own Temporal is a container started with --rm, so stopping
// it at the part's end threw its server log away: a history-read error such
// as "branch tokens don't match" could not be traced to the server. The part
// now keeps that log, its end bounded and gzip-compressed, beside its records,
// as a local proof keeps its own Temporal's (`proofrun temporal-log`).

// fakeDockerLogs puts a docker on PATH whose `logs NAME` prints out on
// standard output and errText on standard error and exits code, and which
// appends every invocation to calls.
func fakeDockerLogs(t *testing.T, out, errText string, code int) (calls string) {
	t.Helper()
	dir := t.TempDir()
	calls = filepath.Join(dir, "calls")
	for name, text := range map[string]string{"out": out, "err": errText} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/bin/sh\necho \"docker $*\" >>'" + calls + "'\n" +
		"[ \"$1\" = logs ] || exit 0\n" +
		"cat '" + filepath.Join(dir, "out") + "'\ncat '" + filepath.Join(dir, "err") + "' >&2\nexit " + itoa(code) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return calls
}

func gunzipFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("%s is not gzip: %v", path, err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTemporalLog_KeepsBothStreamsOfTheServersLogCompressed(t *testing.T) {
	quiet(t)
	calls := fakeDockerLogs(t, "", `{"level":"error","msg":"branch tokens don't match"}`+"\n", 0)
	out := filepath.Join(t.TempDir(), "temporal-server.log.gz")
	if code := cmdTemporalLog([]string{"-container", "orkstack-proof-p1-temporal", "-out", out}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if got := gunzipFile(t, out); got != `{"level":"error","msg":"branch tokens don't match"}`+"\n" {
		t.Errorf("the kept log = %q", got)
	}
	// The file is uploaded with the proof's other records, by a user who is
	// not necessarily the one that wrote it.
	if fi, err := os.Stat(out); err != nil || fi.Mode().Perm() != 0o644 {
		t.Errorf("the kept log's mode is %v (%v), want 0644", fi.Mode().Perm(), err)
	}
	if b, _ := os.ReadFile(calls); strings.TrimSpace(string(b)) != "docker logs orkstack-proof-p1-temporal" {
		t.Errorf("docker was asked %q", b)
	}
	calls = fakeDockerLogs(t, "stdout line\n", "stderr line\n", 0)
	if code := cmdTemporalLog([]string{"-container", "c1", "-out", out}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if got := gunzipFile(t, out); !strings.Contains(got, "stdout line\n") || !strings.Contains(got, "stderr line\n") {
		t.Errorf("the kept log = %q, want both of the container's streams", got)
	}
	if b, _ := os.ReadFile(calls); strings.TrimSpace(string(b)) != "docker logs c1" {
		t.Errorf("docker was asked %q", b)
	}
}

func TestTemporalLog_KeepsTheEndOfALogBeyondItsBound(t *testing.T) {
	quiet(t)
	var log strings.Builder
	for i := 0; log.Len() < 50000; i++ {
		log.WriteString("line " + itoa(i) + " of a long-running server\n")
	}
	fakeDockerLogs(t, log.String(), "", 0)
	out := filepath.Join(t.TempDir(), "temporal-server.log.gz")
	if code := cmdTemporalLog([]string{"-container", "c1", "-out", out, "-max-bytes", "1000"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	got := gunzipFile(t, out)
	head, kept, ok := strings.Cut(got, "\n")
	if !ok || head != "[proofrun temporal-log: the log's first "+itoa(log.Len()-1000)+" bytes left out]" {
		t.Errorf("the first line = %q, want how much was left out", head)
	}
	if kept != log.String()[log.Len()-1000:] {
		t.Errorf("kept %d bytes, want the log's last 1000", len(kept))
	}
}

func TestTemporalLog_WritesNothingWhenDockerHasNoLog(t *testing.T) {
	quiet(t)
	fakeDockerLogs(t, "", "Error response from daemon: No such container: c1\n", 1)
	dir := t.TempDir()
	out := filepath.Join(dir, "temporal-server.log.gz")
	if code := cmdTemporalLog([]string{"-container", "c1", "-out", out}); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a failed read left %v", entries)
	}
}

// A part's cleanup and a local proof's exit keep the log with TERM, INT and
// HUP ignored, before they stop the container: a `docker logs` that does not
// answer must not hold the stop, or a cancel leaves the Temporal running. It
// gives up after its timeout, even on a docker whose child still holds the
// pipes, and writes nothing.
func TestTemporalLog_GivesUpOnADockerThatDoesNotAnswer(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "sleep.pid")
	script := "#!/bin/sh\nsleep 120 &\necho $! >'" + pidfile + "'\nwait\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Cleanup(func() {
		if b, err := os.ReadFile(pidfile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	outDir := t.TempDir()
	out := filepath.Join(outDir, "temporal-server.log.gz")
	started := time.Now()
	code := cmdTemporalLog([]string{"-container", "c1", "-out", out, "-timeout", "1s"})
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Errorf("temporal-log took %v with a 1s timeout", elapsed)
	}
	if code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if entries, _ := os.ReadDir(outDir); len(entries) != 0 {
		t.Errorf("a read that timed out left %v", entries)
	}
}

func TestTemporalLog_RefusesAnInvalidInvocation(t *testing.T) {
	quiet(t)
	calls := fakeDockerLogs(t, "x\n", "", 0)
	out := filepath.Join(t.TempDir(), "l.gz")
	for _, args := range [][]string{
		nil,
		{"-container", "c1"},
		{"-out", out},
		{"-container", "--help", "-out", out},
		{"-container", "c1;rm", "-out", out},
		{"-container", "c1", "-out", out, "-max-bytes", "0"},
		{"-container", "c1", "-out", out, "-timeout", "0s"},
		{"-container", "c1", "-out", out, "extra"},
	} {
		if code := cmdTemporalLog(args); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
	if _, err := os.Stat(calls); err == nil {
		t.Error("an invalid invocation ran docker")
	}
}

// part_cleanup keeps the part's Temporal's log before it stops the container,
// which --rm then removes with its log; a part that started no Temporal asks
// for none.
func TestProofPart_TheCleanupKeepsTheTemporalServersLogBeforeItStopsIt(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	for _, ran := range []string{"1", ""} {
		dir := t.TempDir()
		calls := fakeDockerLogs(t, "", "", 0)
		pr := filepath.Join(dir, "proofrun")
		if err := os.WriteFile(pr, []byte("#!/bin/sh\necho \"proofrun $*\" >>'"+calls+"'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bash, "-c", `source "$PART_SH" && false; part_cleanup`)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "PART_SH=" + filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"),
			"PR=" + pr, "DIR=" + dir, "REPO=" + mustRepoRoot(), "PART_TEMPORAL_RAN=" + ran, "TEMPORAL_NAME=orkstack-proof-p1-temporal",
		}
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		_ = cmd.Run()
		b, _ := os.ReadFile(calls)
		got := strings.TrimSpace(string(b))
		want := "proofrun temporal-log -container orkstack-proof-p1-temporal -out " + filepath.Join(dir, "temporal-server.log.gz") +
			"\ndocker stop orkstack-proof-p1-temporal"
		if ran == "" {
			want = ""
		}
		if got != want {
			t.Errorf("PART_TEMPORAL_RAN=%q: the cleanup ran\n%s\nwant\n%s\n%s", ran, got, want, out.String())
		}
	}
}

// A local proof's own Temporal is the same container; its exit (on_exit, run
// under bash with a docker and a proofrun that record their calls) keeps the
// log in the proof's directory the same way, before it stops the container,
// then stops the gate's browser server, and ends with the exit status it was
// entered with. A proof that started no Temporal asks for no log and stops
// none; a log that cannot be kept never keeps the container running.
func TestProofScript_TheLocalProofKeepsItsTemporalsLogBeforeItStopsIt(t *testing.T) {
	needBash(t)
	onExit := proofScriptFunctions(t, "on_exit")
	const name = "orkstack-proof-pr-me-temporal"
	for _, c := range []struct {
		name         string
		started      string
		logFails     bool
		wantVerbs    []string
		keepsTheLog  bool
		saysNotKept  bool
		stopsTheTemp bool
	}{
		{"a proof that started its Temporal", "1", false,
			[]string{"proofrun temporal-log", "docker stop", "docker ps", "docker stop"}, true, false, true},
		{"a proof that started none", "", false,
			[]string{"docker ps", "docker stop"}, false, false, false},
		{"a log that cannot be kept", "1", true,
			[]string{"proofrun temporal-log", "docker stop", "docker ps", "docker stop"}, true, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			tools := installFakeDocker(t, []fakeDockerContainer{
				{Name: "browser-server-1", Labels: map[string]string{"orkstack.proof": "pr-me", "orkstack.e2e": "browser-server"}},
				{Name: "browser-server-of-another-proof", Labels: map[string]string{"orkstack.proof": "pr-other", "orkstack.e2e": "browser-server"}},
			})
			pr := tools.recorder("proofrun")
			if c.logFails {
				t.Setenv("FAKE_TOOL_FAIL", "proofrun")
			}
			out, err := runProofShell(t, dir, onExit+
				"\nSAMPLER= IMAGES_BUILT= TEMPORAL_STARTED="+c.started+" PR="+pr+" DIR="+dir+" PROOF_ID=pr-me TEMPORAL_NAME="+name+
				"\nfalse\non_exit")
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 1 {
				t.Errorf("on_exit ended %v, want the exit status 1 it was entered with:\n%s", err, out)
			}
			if got := tools.verbs(); !slices.Equal(got, c.wantVerbs) {
				t.Fatalf("on_exit ran %q, want %q:\n%s", got, c.wantVerbs, out)
			}
			calls := tools.calls()
			if c.keepsTheLog {
				want := []string{"proofrun", "temporal-log", "-container", name, "-out", filepath.Join(dir, "temporal-server.log.gz")}
				if !slices.Equal(calls[0], want) {
					t.Errorf("the log was kept with %q, want %q", calls[0], want)
				}
			}
			if c.stopsTheTemp && !slices.Equal(calls[1], []string{"docker", "stop", name}) {
				t.Errorf("the container was stopped with %q", calls[1])
			}
			for _, call := range calls {
				if call[1] == "stop" && slices.Contains(call, name) && !c.stopsTheTemp {
					t.Errorf("a proof that started no Temporal stopped %q", call)
				}
			}
			last := calls[len(calls)-1]
			if last[len(last)-1] != "browser-server-1" {
				t.Errorf("the gate's browser server was stopped as %q, want browser-server-1 and not another proof's", last)
			}
			if said := strings.Contains(out, "could not be kept"); said != c.saysNotKept {
				t.Errorf("on_exit said the log could not be kept: %v, want %v:\n%s", said, c.saysNotKept, out)
			}
		})
	}
}
