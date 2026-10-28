//go:build integration

package orker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// slowDockerShim is the real docker CLI behind a shim that makes the daemon
// slow to start a container, the way a daemon under load is. Everything the
// shim does not intercept reaches the real daemon, so the probe that does pass
// binds a real directory with a real container.
//
// Flags (files under dir): hang (its content N: the first N `docker run`
// invocations never answer, and the shim waits to be killed), delay (seconds
// every later `run` waits before it reaches the daemon), fail (a `run` that
// the daemon refuses, as it refuses an unshared path). `calls` records every
// invocation and `runs` counts the `run`s.
type slowDockerShim struct {
	t   *testing.T
	dir string
	bin string
}

func newSlowDockerShim(t *testing.T, realDocker string) *slowDockerShim {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
S='` + dir + `'
echo "$*" >> "$S/calls"
if [ "$1" = run ]; then
  n=0; if [ -e "$S/runs" ]; then n=$(cat "$S/runs"); fi
  n=$((n+1)); echo "$n" > "$S/runs"
  if [ -e "$S/hang" ] && [ "$n" -le "$(cat "$S/hang")" ]; then exec sleep 300; fi
  if [ -e "$S/delay" ]; then sleep "$(cat "$S/delay")"; fi
  if [ -e "$S/fail" ]; then echo "docker: Error response from daemon: bind source path does not exist" >&2; exit 125; fi
fi
exec '` + realDocker + `' "$@"
`
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return &slowDockerShim{t: t, dir: dir, bin: bin}
}

func (s *slowDockerShim) set(flag, content string) {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(s.dir, flag), []byte(content), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

func (s *slowDockerShim) runs() int {
	s.t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.dir, "runs"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		s.t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		s.t.Fatal(err)
	}
	return n
}

// probeContainers returns, in order, the --name of every probe `docker run`
// and the container every `docker rm --force` named.
func (s *slowDockerShim) probeContainers() (ran, removed []string) {
	s.t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.dir, "calls"))
	if err != nil {
		s.t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		switch {
		case len(fields) > 0 && fields[0] == "run":
			for i, field := range fields {
				if field == "--name" && i+1 < len(fields) {
					ran = append(ran, fields[i+1])
				}
			}
		case len(fields) > 2 && fields[0] == "rm" && strings.HasPrefix(fields[len(fields)-1], "ork-bindprobe-"):
			removed = append(removed, fields[len(fields)-1])
		}
	}
	return ran, removed
}

// TestIntegration_BootBindProbeSeparatesASlowDaemonFromAnUnbindableRoot boots
// a real Orker (real Activities.Boot, real DockerRuntime, real daemon and a
// real bind mount) behind a docker CLI that is slow to start containers.
//
// Under load a daemon took more than the old 30 s control timeout to start the
// probe's container, the probe was killed, and the Orker refused to start
// saying its work root was not bind-mountable. A work root is bindable or it is
// not; a slow daemon says nothing about it. The cases are the three answers
// the probe now has: a slow start that finishes in its budget boots, one that
// overruns once boots on the retry, one that never answers refuses boot with
// its own reason, and a container that ran and was refused is still the work
// root's fault and is not retried.
func TestIntegration_BootBindProbeSeparatesASlowDaemonFromAnUnbindableRoot(t *testing.T) {
	requireDocker(t)
	realDocker, err := exec.LookPath("docker")
	if err != nil {
		infraAbsent(t, "the docker CLI is not on PATH: %v", err)
	}
	// The probe image is made local here, so that the step each case makes
	// slow is the run and no case waits on a registry.
	setup, cancelSetup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSetup()
	if err := exec.CommandContext(setup, realDocker, "image", "inspect", "alpine:3.20").Run(); err != nil {
		if out, pullErr := exec.CommandContext(setup, realDocker, "pull", "--quiet", "alpine:3.20").CombinedOutput(); pullErr != nil {
			infraAbsent(t, "the bind probe image alpine:3.20 is not local and cannot be pulled: %v: %s", pullErr, out)
		}
	}
	workParent := "../../.data/work"
	if err := os.MkdirAll(workParent, 0o755); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		flags      map[string]string
		budget     time.Duration
		wantRuns   int
		wantReason string // "" boots
		// wantRemoved is how many probe containers Boot force-removed: one
		// per attempt that overran its budget.
		wantRemoved int
	}{
		{
			name:  "a start slower than usual but inside its budget boots without a retry",
			flags: map[string]string{"delay": "2"}, budget: time.Minute, wantRuns: 1,
		},
		{
			name:  "a start that overruns once boots on the retry",
			flags: map[string]string{"hang": "1"}, budget: 10 * time.Second, wantRuns: 2, wantRemoved: 1,
		},
		{
			name:  "a daemon that never starts it refuses boot as slow, not as unbindable",
			flags: map[string]string{"hang": "9"}, budget: 2 * time.Second,
			wantRuns: 2, wantReason: orker.ReasonBindProbeTimedOut, wantRemoved: 2,
		},
		{
			name:  "a container the daemon refused is the work root's fault and is not retried",
			flags: map[string]string{"fail": ""}, budget: time.Minute,
			wantRuns: 1, wantReason: orker.ReasonWorkdirNotBindable,
		},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			shim := newSlowDockerShim(t, realDocker)
			for flag, content := range tt.flags {
				shim.set(flag, content)
			}
			workRoot, err := os.MkdirTemp(workParent, proof.WorkRootPattern("ork-bindprobe"))
			if err != nil {
				t.Fatal(err)
			}
			// The Orker hands this path to the daemon as a bind source, and
			// Docker requires it absolute.
			if workRoot, err = filepath.Abs(workRoot); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(workRoot) })
			store, err := artifacts.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			activities := orker.NewActivitiesWithOptions(store, orker.Options{
				Runtime:  &orker.DockerRuntime{Bin: shim.bin, BindProbeTimeout: tt.budget},
				OrkerID:  proof.OrkerID(fmt.Sprintf("bindprobe-%d", i)),
				WorkRoot: workRoot,
			})
			t.Cleanup(activities.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			err = activities.Boot(ctx)

			switch {
			case tt.wantReason == "" && err != nil:
				t.Fatalf("Boot = %v; want it to boot", err)
			case tt.wantReason != "" && (err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+":")):
				t.Fatalf("Boot = %v; want a refusal starting %s:", err, tt.wantReason)
			}
			if tt.wantReason == orker.ReasonBindProbeTimedOut {
				for _, want := range []string{orker.BindProbeTimeoutVar, "daemon", tt.budget.String()} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Boot = %v; want the refusal to contain %q", err, want)
					}
				}
				if strings.Contains(err.Error(), orker.ReasonWorkdirNotBindable) {
					t.Errorf("Boot = %v; a slow daemon must not be called an unbindable work root", err)
				}
			}
			if got := shim.runs(); got != tt.wantRuns {
				t.Errorf("the probe ran its container %d times; want %d", got, tt.wantRuns)
			}
			ran, removed := shim.probeContainers()
			if len(removed) != tt.wantRemoved {
				t.Errorf("force-removed %q after runs of %q; want %d removals", removed, ran, tt.wantRemoved)
			}
			for j, name := range removed {
				if name != ran[j] {
					t.Errorf("removal #%d was of %q; want the overrun attempt's container %q", j, name, ran[j])
				}
			}
		})
	}
}
