package orker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A docker CLI that exits 0 succeeded, even when its output pipe stays open a
// moment past its exit: a helper that inherited the pipe, or a loaded host
// that schedules the drain late. The 100 ms the runner once allowed for that
// drain turned a successful `docker create` on a loaded CI runner into
// RUNTIME_REFUSED ("exec: WaitDelay expired before I/O complete", CI run
// 40740645720), and a task with one attempt failed its run.
func TestRunWithin_AnOutputPipeHeldPastExitIsStillASuccess(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "docker")
	// The background subshell inherits stdout and holds it for half a second
	// after the CLI itself has printed its answer and exited 0.
	script := "#!/bin/sh\necho c0ffee\n( sleep 0.5 ) &\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	d := &DockerRuntime{Bin: bin}
	answer, overran, err := d.runWithin(context.Background(), time.Minute, "create", "busybox")
	if err != nil || answer.code != 0 || overran {
		t.Fatalf("runWithin = code %d, overran %v, err %v: a CLI that exited 0 failed", answer.code, overran, err)
	}
	if got := strings.TrimSpace(answer.stdout); got != "c0ffee" {
		t.Fatalf("output = %q, want the CLI's whole answer", got)
	}
}
