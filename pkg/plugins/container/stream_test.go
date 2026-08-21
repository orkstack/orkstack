package containerplugin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// M11.1: what the application inside the container writes reaches the task's
// LIVE log stream, while the container runs — not only the capture artifacts
// after it exits.
//
// These tests drive a stand-in Docker CLI whose `run` writes a body the test
// supplies, so the streaming path is observable with no daemon. The real
// daemon is exercised by container_integration_test.go.

// streamingDockerScript is a stand-in CLI that streams whatever body the test
// wrote. It answers `image inspect` as "present", so no pull is attempted.
const streamingDockerScript = `#!/bin/sh
set -u
case "${1:-}" in
  image) exit 0 ;;
  kill|rm) exit 0 ;;
  run)
    . "$STREAM_BODY"
    exit "${STREAM_EXIT:-0}"
    ;;
esac
exit 0
`

// newStreamingDocker installs the stand-in CLI plus the body its `run` writes,
// and returns the plugin pointed at it.
func newStreamingDocker(t *testing.T, body string, exitCode int) *Plugin {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(streamingDockerScript), 0o755); err != nil {
		t.Fatalf("write stand-in docker: %v", err)
	}
	bodyPath := filepath.Join(dir, "body.sh")
	if err := os.WriteFile(bodyPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write stand-in run body: %v", err)
	}
	t.Setenv("STREAM_BODY", bodyPath)
	t.Setenv("STREAM_EXIT", fmt.Sprint(exitCode))
	return &Plugin{DockerBin: bin}
}

// newStreamingContext builds the ExecContext, recording into logs. A nil logs
// is a plugin with no logger configured.
func newStreamingContext(t *testing.T, config map[string]any, logs *logCapture) *plugin.ExecContext {
	t.Helper()
	ec := &plugin.ExecContext{
		TaskID:  "build",
		Config:  config,
		WorkDir: t.TempDir(),
	}
	if logs != nil {
		ec.Log = func(format string, args ...any) { logs.add(fmt.Sprintf(format, args...)) }
	}
	return ec
}

// The milestone's claim at the plugin boundary: both of the application's
// streams reach plugin.Logger, framed into lines, with stderr marked — and
// every line exactly once, however the reads were split.
//
// The body writes partial lines across separate writes (each printf is its own
// write, and the sleeps force separate reads on the plugin's side), a percent
// sign, a CR redraw, and a final line on each stream with no newline at all.
func TestContainerPlugin_StreamsBothStreamsThroughTheLoggerWithMarkedStderr(t *testing.T) {
	rec := &logCapture{}
	p := newStreamingDocker(t, `
printf 'out-1\n'
printf 'err-1\n' >&2
printf 'split-'
sleep 0.1
printf 'across-reads\n'
printf 'progress: 100%% (%%d of %%s)\n'
printf 'redraw-1\rredraw-2\n'
printf 'err-split-' >&2
sleep 0.1
printf 'across-reads\n' >&2
printf 'no-newline-at-end'
printf 'err-no-newline-at-end' >&2
`, 0)
	ec := newStreamingContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": "irrelevant",
	}, rec)

	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// stdout travels bare, stderr carries the marker, and both are framed.
	for _, want := range []string{
		"out-1",
		"split-across-reads",
		"progress: 100% (%d of %s)",
		"redraw-1",
		"redraw-2",
		"no-newline-at-end",
		plugin.StderrMarker + "err-1",
		plugin.StderrMarker + "err-split-across-reads",
		plugin.StderrMarker + "err-no-newline-at-end",
	} {
		if got := rec.count(want); got != 1 {
			t.Errorf("streamed %q %d time(s), want exactly 1; the stream was:\n  %s",
				want, got, strings.Join(rec.all(), "\n  "))
		}
	}

	// And the marker is not sprayed onto stdout: exactly the three stderr
	// lines carry it.
	marked := 0
	for _, line := range rec.all() {
		if strings.HasPrefix(line, plugin.StderrMarker) {
			marked++
		}
	}
	if marked != 3 {
		t.Errorf("%d streamed line(s) carry %q, want the 3 stderr lines",
			marked, plugin.StderrMarker)
	}
}

// The claim a post-mortem read cannot satisfy: a line written early by a
// long-lived application is readable BEFORE the container exits.
func TestContainerPlugin_StreamsWhileTheContainerIsStillRunning(t *testing.T) {
	rec := &logCapture{}
	p := newStreamingDocker(t, `
printf 'early-marker\n'
sleep 2
printf 'late-marker\n'
`, 0)
	ec := newStreamingContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": "irrelevant",
	}, rec)

	done := make(chan error, 1)
	go func() {
		_, err := p.Execute(context.Background(), ec)
		done <- err
	}()

	deadline := time.Now().Add(60 * time.Second)
	for !rec.holds("early-marker") && time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("the container finished (err=%v) before its first line was streamed; "+
				"the application's output is still only visible afterwards", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rec.holds("early-marker") {
		t.Fatalf("the first line never arrived while the container ran; streamed %q", rec.all())
	}
	if rec.holds("late-marker") {
		t.Fatal("the second line arrived before the container slept; the test is not measuring streaming")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Execute never returned")
	}
	if !rec.holds("late-marker") {
		t.Fatalf("the trailing line was never streamed; streamed %q", rec.all())
	}
}

// Nothing the plugin already does may regress because a third consumer of the
// same bytes appeared: the capture artifacts are still the byte-exact streams,
// and the output payload still carries every field it carried.
func TestContainerPlugin_StreamingLeavesTheArtifactsAndPayloadUnchanged(t *testing.T) {
	rec := &logCapture{}
	p := newStreamingDocker(t, `
printf 'out-1\nout-2\r\nout-3'
printf 'err-1\nerr-2' >&2
`, 0)
	ec := newStreamingContext(t, map[string]any{
		"image":             "alpine:3.20",
		"command":           "irrelevant",
		"capture_stdout_as": "out",
		"capture_stderr_as": "err",
	}, rec)

	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Byte-exact, terminators and all: the artifact is the authoritative copy,
	// and the log's CR framing must not have reached it.
	if got := readStreamArtifact(t, result.Artifacts, "out"); got != "out-1\nout-2\r\nout-3" {
		t.Errorf("the stdout artifact holds %q, want the stream byte for byte", got)
	}
	if got := readStreamArtifact(t, result.Artifacts, "err"); got != "err-1\nerr-2" {
		t.Errorf("the stderr artifact holds %q, want the stream byte for byte", got)
	}

	for field, want := range map[string]any{
		"exit_code":    float64(0),
		"stdout":       "out-1\nout-2\r\nout-3",
		"stderr":       "err-1\nerr-2",
		"image":        "alpine:3.20",
		"image_pulled": false,
		"timed_out":    false,
	} {
		if got := result.Output[field]; got != want {
			t.Errorf("output[%q] = %#v, want %#v", field, got, want)
		}
	}
	for _, field := range []string{"container", "duration_ms"} {
		if _, ok := result.Output[field]; !ok {
			t.Errorf("output has no %q", field)
		}
	}
	// The two streams stay legible in the log, marker and all.
	if !rec.holds(plugin.StderrMarker+"err-2") || !rec.holds("out-3") {
		t.Errorf("the unterminated final lines never streamed; streamed %q", rec.all())
	}
}

// A failing container's last words are what a person reads to find out why.
// The streams are flushed BEFORE the exit code is interpreted, so they are
// there whatever the plugin decides about the failure.
func TestContainerPlugin_StreamsTheFinalLineOfAFailingContainer(t *testing.T) {
	rec := &logCapture{}
	p := newStreamingDocker(t, `
printf 'dying-words-on-stdout'
printf 'dying-words-on-stderr' >&2
`, 3)
	ec := newStreamingContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": "irrelevant",
	}, rec)

	_, err := p.Execute(context.Background(), ec)
	if err == nil {
		t.Fatal("Execute succeeded; a container exiting 3 must fail the task")
	}
	if rec.count("dying-words-on-stdout") != 1 {
		t.Errorf("the failing container's stdout line was not streamed exactly once: %q",
			rec.all())
	}
	if rec.count(plugin.StderrMarker+"dying-words-on-stderr") != 1 {
		t.Errorf("the failing container's stderr line was not streamed exactly once: %q",
			rec.all())
	}
}

// A container that writes nothing produces an empty stream, not a hung one:
// the plugin returns, and no phantom line is invented for it.
func TestContainerPlugin_AContainerThatWritesNothingStreamsNothing(t *testing.T) {
	rec := &logCapture{}
	p := newStreamingDocker(t, "true\n", 0)
	ec := newStreamingContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": "irrelevant",
	}, rec)

	done := make(chan error, 1)
	go func() {
		_, err := p.Execute(context.Background(), ec)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Execute never returned for a container that wrote nothing")
	}

	// Only the plugin's own lifecycle lines, every one of them named.
	for _, line := range rec.all() {
		if !strings.HasPrefix(line, "container: ") {
			t.Errorf("a container that wrote nothing produced the log line %q", line)
		}
	}
}

// Nothing about streaming may make a task fail, so a plugin with no logger
// configured still runs and still captures.
func TestContainerPlugin_StreamsNowhereWithNoLogger(t *testing.T) {
	p := newStreamingDocker(t, "printf 'ignored\\n'\n", 0)
	ec := newStreamingContext(t, map[string]any{
		"image":             "alpine:3.20",
		"command":           "irrelevant",
		"capture_stdout_as": "out",
	}, nil)

	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute with no logger: %v", err)
	}
	if got := readStreamArtifact(t, result.Artifacts, "out"); got != "ignored\n" {
		t.Errorf("the stdout artifact holds %q with no logger configured", got)
	}
}

// A chatty application costs a bounded number of log lines and a bounded
// amount of Orker memory: one framed line per line written, never a write per
// byte, and never a growing buffer.
func TestContainerPlugin_AChattyApplicationStreamsOneLinePerLine(t *testing.T) {
	const lines = 5000
	rec := &logCapture{}
	p := newStreamingDocker(t, fmt.Sprintf(`
i=0
while [ $i -lt %d ]; do
  printf 'chatty-%%d\n' "$i"
  i=$((i + 1))
done
`, lines), 0)
	ec := newStreamingContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": "irrelevant",
	}, rec)

	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	streamed := 0
	for _, line := range rec.all() {
		if strings.HasPrefix(line, "chatty-") {
			streamed++
		}
	}
	if streamed != lines {
		t.Fatalf("%d lines written produced %d streamed line(s); a per-byte or "+
			"per-read sink would not match", lines, streamed)
	}
	if got := rec.count("chatty-0"); got != 1 {
		t.Errorf("the first line was streamed %d time(s), want exactly 1", got)
	}
	if got := rec.count(fmt.Sprintf("chatty-%d", lines-1)); got != 1 {
		t.Errorf("the last line was streamed %d time(s), want exactly 1", got)
	}
}

func readStreamArtifact(t *testing.T, produced []plugin.ProducedArtifact, name string) string {
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
