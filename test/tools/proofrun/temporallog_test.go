package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
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

// A local proof's own Temporal is the same container; its exit keeps the log
// in the proof's directory the same way, before it stops the container.
func TestProofScript_TheLocalProofKeepsItsTemporalsLogBeforeItStopsIt(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	start := strings.Index(text, "\non_exit() {")
	end := strings.Index(text[start+1:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("scripts/proof.sh has no on_exit function")
	}
	body := text[start : start+1+end]
	keep := strings.Index(body, `"$PR" temporal-log -container "$TEMPORAL_NAME" -out "$DIR/temporal-server.log.gz"`)
	stop := strings.Index(body, `docker stop "$TEMPORAL_NAME"`)
	if keep < 0 || stop < 0 || keep > stop {
		t.Errorf("on_exit does not keep the Temporal server's log before it stops the container:\n%s", body)
	}
	if !strings.Contains(body[:stop], `if [ -n "$TEMPORAL_STARTED" ]`) {
		t.Error("on_exit keeps a Temporal log for a proof that started no Temporal of its own")
	}
}
