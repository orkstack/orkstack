package shellplugin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// logRecorder collects what a plugin streamed, with the instant each line
// arrived. The instants are the point: "visible while it runs" is a claim
// about WHEN, and a plugin that streamed everything at exit would satisfy any
// test that only checked what.
type logRecorder struct {
	mu    sync.Mutex
	lines []string
	at    []time.Time
}

func (r *logRecorder) logger() plugin.Logger {
	return func(format string, args ...any) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.lines = append(r.lines, fmt.Sprintf(format, args...))
		r.at = append(r.at, time.Now())
	}
}

func (r *logRecorder) snapshot() ([]string, []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lines...), append([]time.Time(nil), r.at...)
}

func (r *logRecorder) contains(want string) bool {
	lines, _ := r.snapshot()
	for _, line := range lines {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

// The milestone's claim, at the plugin: a shell task's output reaches the log
// stream WHILE the command runs. The command emits a marker and then sleeps
// well past the poll window, so an implementation that only streams at exit
// cannot pass.
func TestShellPlugin_StreamsStdoutWhileTheCommandIsStillRunning(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{
		"command": "echo early-marker; sleep 2; echo late-marker",
	})
	ec.Log = recorder.logger()

	done := make(chan error, 1)
	go func() {
		_, err := New().Execute(context.Background(), ec)
		done <- err
	}()

	deadline := time.Now().Add(90 * time.Second)
	for !recorder.contains("early-marker") && time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("the command finished (err=%v) before its first line was streamed; "+
				"output is still only visible afterwards", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !recorder.contains("early-marker") {
		lines, _ := recorder.snapshot()
		t.Fatalf("the first line never arrived while the command ran; streamed %q", lines)
	}
	if recorder.contains("late-marker") {
		t.Fatal("the second line arrived before the command slept; the test is not measuring streaming")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("Execute never returned")
	}
	if !recorder.contains("late-marker") {
		lines, _ := recorder.snapshot()
		t.Fatalf("the trailing line was never streamed; streamed %q", lines)
	}
}

func TestShellPlugin_StreamsStderrAsWellAsStdout(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{
		"command": "echo out-line; echo err-line 1>&2",
	})
	ec.Log = recorder.logger()

	if _, err := New().Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, want := range []string{"out-line", "err-line"} {
		if !recorder.contains(want) {
			lines, _ := recorder.snapshot()
			t.Errorf("streamed %q, want it to carry %q", lines, want)
		}
	}
}

// The existing artifacts are a contract of their own and must not change
// because a second consumer appeared: the capture files still hold the whole
// stream, byte for byte.
func TestShellPlugin_StreamingLeavesTheCaptureArtifactsUnchanged(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{
		"command":           "for i in 1 2 3; do echo out-$i; echo err-$i 1>&2; done",
		"capture_stdout_as": "out",
		"capture_stderr_as": "err",
	})
	ec.Log = recorder.logger()

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Artifacts) != 2 {
		t.Fatalf("produced %d artifacts, want the two capture files", len(result.Artifacts))
	}
	if got := readArtifact(t, result.Artifacts, "out"); got != "out-1\nout-2\nout-3\n" {
		t.Errorf("the stdout artifact holds %q, want the whole stream", got)
	}
	if got := readArtifact(t, result.Artifacts, "err"); got != "err-1\nerr-2\nerr-3\n" {
		t.Errorf("the stderr artifact holds %q, want the whole stream", got)
	}
	// And the payload strings are still what they were.
	if stdout, _ := result.Output["stdout"].(string); stdout != "out-1\nout-2\nout-3\n" {
		t.Errorf("output.stdout = %q, want the captured head", stdout)
	}
}

// A log line is data, not a format string. A task printing a percent sign must
// not have it interpreted — that is how "100%" becomes "100%!(NOVERB)".
func TestShellPlugin_StreamedLinesAreDataNotFormatStrings(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{"command": `echo 'progress: 100% (%d of %s)'`})
	ec.Log = recorder.logger()

	if _, err := New().Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !recorder.contains("progress: 100% (%d of %s)") {
		lines, _ := recorder.snapshot()
		t.Errorf("streamed %q, want the line verbatim", lines)
	}
}

// A final line with no trailing newline is still somebody's output.
func TestShellPlugin_StreamsATrailingPartialLine(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{"command": `printf 'no-newline-at-end'`})
	ec.Log = recorder.logger()

	if _, err := New().Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !recorder.contains("no-newline-at-end") {
		lines, _ := recorder.snapshot()
		t.Errorf("streamed %q, want the unterminated final line", lines)
	}
}

// One line longer than the tee's own limit is emitted in pieces rather than
// buffered whole: the plugin is the first place a runaway line could exhaust
// memory, before the Orker's sink ever sees it.
func TestShellPlugin_SplitsAnUnterminatedRunOfBytes(t *testing.T) {
	recorder := &logRecorder{}
	ec := execContext(t, map[string]any{
		"command": fmt.Sprintf(`printf '%%0.sx' $(seq 1 %d)`, maxStreamedLine*2),
	})
	ec.Log = recorder.logger()

	if _, err := New().Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	lines, _ := recorder.snapshot()
	var xs, total int
	for _, line := range lines {
		if strings.Trim(line, "x") != "" {
			continue
		}
		xs++
		total += len(line)
		if len(line) > maxStreamedLine {
			t.Errorf("a streamed piece is %d bytes, above the %d-byte limit", len(line), maxStreamedLine)
		}
	}
	if xs < 2 {
		t.Fatalf("a %d-byte unterminated run produced %d pieces; it was not split",
			maxStreamedLine*2, xs)
	}
	if total != maxStreamedLine*2 {
		t.Errorf("the split pieces total %d bytes, want %d", total, maxStreamedLine*2)
	}
}

// A plugin with no logger configured still runs. Nothing about streaming may
// make a task fail.
func TestShellPlugin_RunsWithNoLogger(t *testing.T) {
	ec := execContext(t, map[string]any{"command": "echo fine"})
	ec.Log = nil
	if _, err := New().Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute with no logger: %v", err)
	}
}

// readArtifact returns the bytes of a produced artifact by name.
func readArtifact(t *testing.T, produced []plugin.ProducedArtifact, name string) string {
	t.Helper()
	for _, artifact := range produced {
		if artifact.Name != name {
			continue
		}
		contents, err := os.ReadFile(artifact.LocalPath)
		if err != nil {
			t.Fatalf("read artifact %q: %v", name, err)
		}
		return string(contents)
	}
	t.Fatalf("no artifact named %q among %d produced", name, len(produced))
	return ""
}
