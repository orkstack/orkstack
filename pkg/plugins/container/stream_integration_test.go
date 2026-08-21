//go:build integration

package containerplugin

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// M11.1 against a real daemon: what an application inside a real container
// writes reaches plugin.Logger WHILE the container runs.
//
// The plugin-level half of the milestone. The end-to-end half — the same lines
// read back through StreamTaskLogs from a replica that started nothing, before
// the run reaches a terminal state — lives in
// cmd/orkctl/tasklogs_two_engine_integration_test.go.

// TestIntegration_ContainerApplicationOutputStreamsWhileTheContainerRuns is the
// claim a post-mortem read cannot satisfy.
//
// The container prints one line a second on each stream for five seconds. The
// assertion is about TIME: the first lines must be in the logger while
// Execute has not returned, so an implementation that handed the capture files
// over at the end fails here.
func TestIntegration_ContainerApplicationOutputStreamsWhileTheContainerRuns(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	logs := &logCapture{}
	ec := newIntegrationContext(t, "stream-live", map[string]any{
		"image": image,
		"command": `for i in 1 2 3 4 5; do
  echo "app-out-$i"
  echo "app-err-$i" 1>&2
  sleep 1
done
printf 'app-final-line-without-a-newline'`,
		"capture_stdout_as": "out",
		"capture_stderr_as": "err",
		"timeout_seconds":   120,
	})
	ec.Log = func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		logs.add(line)
		t.Log(line)
	}
	defer assertNoLeftoverContainers(t, ec.TaskID)

	type outcome struct {
		result *plugin.Result
		err    error
	}
	done := make(chan outcome, 1)
	started := time.Now()
	go func() {
		result, err := New().Execute(context.Background(), ec)
		done <- outcome{result: result, err: err}
	}()

	// Both streams, live. The image pull is already done, so the only thing
	// between here and the first line is the container's own start-up.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if logs.holds("app-out-1") && logs.holds(plugin.StderrMarker+"app-err-1") {
			break
		}
		select {
		case got := <-done:
			t.Fatalf("the container finished (err=%v) before its first lines were streamed; "+
				"the application's output is still only visible afterwards. streamed:\n  %s",
				got.err, strings.Join(logs.all(), "\n  "))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no application line arrived while the container ran; streamed:\n  %s",
				strings.Join(logs.all(), "\n  "))
		}
		time.Sleep(50 * time.Millisecond)
	}
	firstLineAfter := time.Since(started)
	if logs.holds("app-out-5") {
		t.Fatal("the last line arrived before the container had slept for it; " +
			"the test is not measuring streaming")
	}
	t.Logf("the first lines of both streams were readable %s in, with the container still running",
		firstLineAfter.Round(time.Millisecond))

	var got outcome
	select {
	case got = <-done:
	case <-time.After(3 * time.Minute):
		t.Fatal("Execute never returned")
	}
	if got.err != nil {
		t.Fatalf("Execute: %v", got.err)
	}

	// Every line, exactly once, stderr marked, the unterminated final line
	// flushed.
	for i := 1; i <= 5; i++ {
		if n := logs.count(fmt.Sprintf("app-out-%d", i)); n != 1 {
			t.Errorf("streamed app-out-%d %d time(s), want exactly 1", i, n)
		}
		want := fmt.Sprintf("%sapp-err-%d", plugin.StderrMarker, i)
		if n := logs.count(want); n != 1 {
			t.Errorf("streamed %q %d time(s), want exactly 1", want, n)
		}
	}
	if n := logs.count("app-final-line-without-a-newline"); n != 1 {
		t.Errorf("the final line with no newline was streamed %d time(s), want exactly 1", n)
	}

	// And nothing regressed: the artifacts are still the whole streams.
	wantOut := "app-out-1\napp-out-2\napp-out-3\napp-out-4\napp-out-5\napp-final-line-without-a-newline"
	if stdout := readStreamArtifact(t, got.result.Artifacts, "out"); stdout != wantOut {
		t.Errorf("the stdout artifact holds %q, want %q", stdout, wantOut)
	}
	wantErr := "app-err-1\napp-err-2\napp-err-3\napp-err-4\napp-err-5\n"
	if stderr := readStreamArtifact(t, got.result.Artifacts, "err"); stderr != wantErr {
		t.Errorf("the stderr artifact holds %q, want %q", stderr, wantErr)
	}
	if code, _ := got.result.Output["exit_code"].(float64); code != 0 {
		t.Errorf("output.exit_code = %v, want 0", got.result.Output["exit_code"])
	}
	if _, ok := got.result.Output["duration_ms"]; !ok {
		t.Error("output has no duration_ms")
	}
}

// A real container that writes nothing produces an empty stream, not a hung
// one: the plugin returns promptly and invents no line for it.
func TestIntegration_ContainerThatWritesNothingStreamsNothing(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	logs := &logCapture{}
	ec := newIntegrationContext(t, "stream-silent", map[string]any{
		"image":           image,
		"command":         "true",
		"timeout_seconds": 120,
	})
	ec.Log = func(format string, args ...any) { logs.add(fmt.Sprintf(format, args...)) }
	defer assertNoLeftoverContainers(t, ec.TaskID)

	done := make(chan error, 1)
	go func() {
		_, err := New().Execute(context.Background(), ec)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("Execute never returned for a container that wrote nothing")
	}

	for _, line := range logs.all() {
		if !strings.HasPrefix(line, "container: ") {
			t.Errorf("a container that wrote nothing produced the log line %q", line)
		}
	}
}
