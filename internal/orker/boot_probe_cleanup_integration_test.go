//go:build integration

package orker_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// probeContainersOf lists the containers, in any state, labelled with the
// Orker identity id: by its label, never host-wide, because other tests and
// other proofs start and stop containers on the same daemon (AGENTS.md
// §11.1).
func probeContainersOf(ctx context.Context, t *testing.T, cli, id string) []string {
	t.Helper()
	out, err := exec.CommandContext(ctx, cli, "ps", "--all", "--quiet", "--filter", "label=ork.orker="+id).CombinedOutput()
	if err != nil {
		t.Fatalf("%s ps --filter label=ork.orker=%s: %v: %s", cli, id, err, out)
	}
	return strings.Fields(string(out))
}

// TestIntegration_ABootStoppedMidProbeLeavesNothingItsNextBootMisses is the
// boot probe's own leftover on a real daemon. A `run` of the SIGTERM probe's
// container cut off between the daemon's create and its start leaves a
// container created and never started, which the daemon's --rm never removes
// (it fires on exit): four such ork-sigprobe-* containers were found on a
// proof host, labelled with nothing but their task, so neither the boot
// reaper nor `proofrun clean` could tell whose they were.
//
// The CLI here creates that container for real and then hangs, as a daemon
// that has not started it yet; the Orker is stopped there. First, the
// container is the Orker's by its label (the positive control: the scenario
// made what it claims). Then either the stopping Orker removes it, or — when
// the CLI refuses every removal of it, as an Orker killed before it could
// remove anything would leave it — it stays, created, labelled and mounted
// from this test's work root, and the Orker's next boot, under the same
// identity, reaps it.
func TestIntegration_ABootStoppedMidProbeLeavesNothingItsNextBootMisses(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	setup, cancelSetup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSetup()
	requireProbeImage(setup, t, realDocker)

	cases := []struct {
		name string
		dies bool
	}{
		{name: "the stopping Orker removes the container"},
		{name: "the next boot reaps the container a dead Orker left", dies: true},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancelAll := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancelAll()
			dir := t.TempDir()
			dead := ""
			if tt.dies {
				dead = `case "$1" in rm|kill) case "$*" in *ork-sigprobe-*) echo "the Orker died before it removed this" >&2; exit 1 ;; esac ;; esac
`
			}
			shim := filepath.Join(dir, "docker")
			script := `#!/bin/sh
D='` + dir + `'
` + dead + `if [ "$1" = run ]; then
  case "$*" in *ork.task=sigprobe*)
    shift
    prev="" name=""
    for a do
      shift
      [ "$prev" = --name ] && name=$a
      prev=$a
      case "$a" in --detach|--rm) ;; *) set -- "$@" "$a" ;; esac
    done
    printf '%s' "$name" > "$D/name"
    '` + realDocker + `' create "$@" > "$D/created.tmp" 2>&1 || { cat "$D/created.tmp" >&2; exit 125; }
    mv "$D/created.tmp" "$D/created"
    exec sleep 600 ;;
  esac
fi
exec '` + realDocker + `' "$@"
`
			must.OK(t, os.WriteFile(shim, []byte(script), 0o755))
			id := proof.OrkerID(fmt.Sprintf("probe-left-%d", i))
			root := postureWorkRoot(t, "ork-probe-left")
			t.Cleanup(func() {
				if name, err := os.ReadFile(filepath.Join(dir, "name")); err == nil && len(name) > 0 {
					_ = exec.Command(realDocker, "rm", "--force", string(name)).Run()
				}
			})
			newLife := func(cli string) *orker.Activities {
				store, err := artifacts.NewLocalStore(t.TempDir())
				must.OK(t, err)
				t.Cleanup(func() { _ = store.Close() })
				return orker.NewActivitiesWithOptions(store, orker.Options{
					Runtime:  &orker.DockerRuntime{Bin: cli, BindProbeTimeout: 2 * time.Minute},
					OrkerID:  id,
					WorkRoot: root,
				})
			}

			first := newLife(shim)
			t.Cleanup(first.Close)
			bootCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- first.Boot(bootCtx) }()
			created := filepath.Join(dir, "created")
			for {
				if _, err := os.Stat(created); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("Boot returned %v before the SIGTERM probe's container was created", err)
				case <-ctx.Done():
					t.Fatal("the SIGTERM probe's container was not created within 5 minutes")
				case <-time.After(50 * time.Millisecond):
				}
			}
			name, err := os.ReadFile(filepath.Join(dir, "name"))
			if err != nil || len(name) == 0 {
				t.Fatalf("the shim recorded no container name: %v", err)
			}
			if got := probeContainersOf(ctx, t, realDocker, id); len(got) != 1 {
				t.Fatalf("containers labelled ork.orker=%s while the probe's %s is created: %q; want it alone: "+
					"a probe container that does not carry its Orker's identity is one no reaper finds", id, name, got)
			}

			stop()
			select {
			case err = <-done:
			case <-ctx.Done():
				t.Fatal("Boot did not return after it was stopped")
			}
			first.Close()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Boot = %v; want the context's own error", err)
			}
			left := probeContainersOf(ctx, t, realDocker, id)
			if !tt.dies {
				if len(left) != 0 {
					t.Fatalf("the stopped Orker left %q labelled ork.orker=%s; want its probe's container removed", left, id)
				}
				return
			}

			// What a dead Orker leaves: the container, never started, mounted
			// from this test's work root, which `proofrun clean` attributes to
			// this process once it is gone.
			if len(left) != 1 {
				t.Fatalf("containers labelled ork.orker=%s after the Orker died: %q; want its probe's one", id, left)
			}
			out, err := exec.CommandContext(ctx, realDocker, "container", "inspect", "--format",
				`{{.State.Status}}{{range .Mounts}} {{.Source}}{{end}}`, string(name)).CombinedOutput()
			if err != nil {
				t.Fatalf("inspect %s: %v: %s", name, err, out)
			}
			fields := strings.Fields(string(out))
			if len(fields) < 2 || fields[0] != "created" {
				t.Fatalf("the left container %s is %q; want created and never started, with its mounts", name, out)
			}
			for _, source := range fields[1:] {
				if !strings.HasPrefix(source, root+string(filepath.Separator)) {
					t.Errorf("the left container mounts %s, outside this test's work root %s", source, root)
				}
			}

			second := newLife(realDocker)
			t.Cleanup(second.Close)
			if err := second.Boot(ctx); err != nil {
				t.Fatalf("the next Boot = %v", err)
			}
			if left := probeContainersOf(ctx, t, realDocker, id); len(left) != 0 {
				t.Fatalf("the next boot under ork.orker=%s left %q; want the dead Orker's probe container reaped", id, left)
			}
		})
	}
}
