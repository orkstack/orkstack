//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// slowStartCLI is a stand-in for the container CLI that passes everything to
// the real one, except that while armed exists the `start` of a container
// never returns: it records its pid and that it was asked, and sleeps. With
// lateStart also present it first hands the real start to a background shell
// that runs it fifteen seconds on (inside the 60 s the Orker waits for a container to leave created, and long enough for a slow host to kill the Orker first), so the daemon starts the container only after
// the Orker that asked is dead. It is the shape of newGatedCLI
// (container_removal_integration_test.go), for the start step: the daemon is
// the real one, and only the CLI's timing is ours, because no real daemon can
// be made slow to start a container at a chosen instant.
type slowStartCLI struct {
	path, armed, lateStart, asked, pidFile string
}

func newSlowStartCLI(t *testing.T) *slowStartCLI {
	t.Helper()
	realCLI := containerCLI(t)
	dir := t.TempDir()
	g := &slowStartCLI{
		path:      filepath.Join(dir, "docker"),
		armed:     filepath.Join(dir, "armed"),
		lateStart: filepath.Join(dir, "late-start"),
		asked:     filepath.Join(dir, "start-asked"),
		pidFile:   filepath.Join(dir, "start.pid"),
	}
	script := `#!/bin/sh
if [ "$1" = start ] && [ -e '` + g.armed + `' ]; then
  echo $$ > '` + g.pidFile + `'
  if [ -e '` + g.lateStart + `' ]; then
    ( sleep 15; '` + realCLI + `' "$@" ) >/dev/null 2>&1 </dev/null &
  fi
  : > '` + g.asked + `'
  exec sleep 600
fi
exec '` + realCLI + `' "$@"
`
	must.OK(t, os.WriteFile(g.path, []byte(script), 0o755))
	// The hung start belongs to a killed Orker and nothing waits on it: it is
	// stopped here, by the pid it recorded, once it is still the sleep it
	// became.
	t.Cleanup(func() {
		raw, err := os.ReadFile(g.pidFile)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || pid <= 1 {
			return
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil || !bytes.Equal(cmdline, []byte("sleep\x00600\x00")) {
			return
		}
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
	return g
}

// TestIntegration_KillMidStartLeavesAReapableContainer is E92 R8-2: an Orker
// killed while a container start is in flight (the hard bound of its stop, a
// third signal or its process manager's SIGKILL) leaves what the start had
// reached, a created or a started container with its labels, and the next
// Orker of the same identity finds it by those labels at boot and removes it.
//
// Two shapes of the same kill. In the first the start has not reached the
// daemon when the Orker dies: the container stays created and never runs. In
// the second the daemon starts the container after the Orker that asked is
// dead, so the reap finds a running one. In both, the container is observed
// to exist, after the kill and before the replacement boots, which is what
// makes an empty list afterwards a removal and not an absence.
func TestIntegration_KillMidStartLeavesAReapableContainer(t *testing.T) {
	requireDocker(t)
	for _, tt := range []struct {
		name string
		// late: the daemon starts the container after its asker is dead.
		late      bool
		wantState string
	}{
		{name: "created-never-started", wantState: "created"},
		{name: "started-after-the-kill", late: true, wantState: "running"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			gate := newSlowStartCLI(t)
			arm(t, gate.armed)
			if tt.late {
				arm(t, gate.lateStart)
			}
			// The held Orker is a child process and inherits this: its runtime
			// and this test's own reads go through the shim, which passes
			// everything but the armed start to the real CLI.
			t.Setenv(orker.DockerBinVar, gate.path)
			id := proof.OrkerID("kill-mid-start-" + tt.name)
			workRoot := removalWorkRoot(t, "kill-mid-start-"+tt.name)
			in := removalInput(t, "midstart-"+tt.name, "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)

			kill, exited, output := spawnHeldOrker(t, id, workRoot, in)
			// The start is in flight: its CLI was asked, and the container the
			// Orker created for it is there, not yet running.
			waitCtx, cancelWait := context.WithTimeout(ctx, 2*time.Minute)
			defer cancelWait()
			for {
				if _, err := os.Stat(gate.asked); err == nil {
					break
				}
				select {
				case <-exited:
					t.Fatalf("the Orker exited before it asked to start its container: %s", output.String())
				case <-waitCtx.Done():
					t.Fatalf("the start was never asked for: %v: %s", waitCtx.Err(), output.String())
				case <-time.After(20 * time.Millisecond):
				}
			}
			container := awaitLabelled(waitCtx, t, "ork.run="+in.GetRunId())
			if got := containerState(ctx, t, container); got != "created" {
				t.Fatalf("the container is %q while its start is in flight, want created", got)
			}

			kill()
			// Positive control: after the kill and before any boot, the
			// container is there, and carries the identity the reap goes by.
			awaitState(waitCtx, t, container, tt.wantState)
			if owned := containersLabelled(ctx, t, "ork.orker="+id); !slices.Contains(owned, container) {
				t.Fatalf("containers labelled with the killed Orker's identity: %v, want %s among them", owned, container)
			}
			t.Logf("observed: container %s is %s after its Orker was killed mid-start, carrying ork.orker=%s", container, tt.wantState, id)

			// The replacement's boot reaps by the identity. The shim is no longer
			// armed, so its own probe containers start as they would anywhere.
			must.OK(t, os.Remove(gate.armed))
			removalLife(ctx, t, id, workRoot, orker.NewDockerRuntime())
			if left := containersLabelled(ctx, t, "ork.orker="+id); len(left) != 0 {
				t.Fatalf("the replacement's boot left its predecessor's containers: %v", left)
			}
			if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
				t.Fatalf("containers of the killed attempt after the replacement booted: %v", left)
			}
		})
	}
}
