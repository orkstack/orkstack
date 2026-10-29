//go:build integration

package orker_test

import (
	"context"
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
)

// TestIntegration_BootProbeRefusesARuntimeThatDropsSIGTERM is the boot
// probe's SIGTERM check on a real daemon. Its container is built as a task's
// is, its workload traps SIGTERM, and it is cancelled as a cancelled task's
// container is. The first case is the positive control: on this host's
// engine the workload hears SIGTERM and the Orker boots, which is what rootful
// Podman under AppArmor 5 did not do. In the second, the CLI drops every
// SIGTERM on its way to the daemon, as that host's AppArmor profile dropped
// it on its way from the container's init to the workload, and the real
// container, still running at the end of its grace, refuses boot as
// RUNTIME_POSTURE_REFUSED. Either way the daemon removes the probe's
// container once it has ended.
func TestIntegration_BootProbeRefusesARuntimeThatDropsSIGTERM(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	setup, cancelSetup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSetup()
	requireProbeImage(setup, t, realDocker)

	cases := []struct {
		name string
		drop bool
	}{
		{name: "an engine that delivers SIGTERM boots"},
		{name: "a CLI that drops SIGTERM is refused", drop: true},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			drop := ""
			if tt.drop {
				drop = `if [ "$1" = kill ] && [ "$3" = TERM ]; then exit 0; fi
`
			}
			shim := filepath.Join(dir, "docker")
			script := "#!/bin/sh\necho \"$*\" >> '" + dir + "/calls'\n" + drop + "exec '" + realDocker + "' \"$@\"\n"
			if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			store, err := artifacts.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			activities := orker.NewActivitiesWithOptions(store, orker.Options{
				Runtime:  &orker.DockerRuntime{Bin: shim, BindProbeTimeout: 90 * time.Second},
				OrkerID:  proof.OrkerID(fmt.Sprintf("sigprobe-%d", i)),
				WorkRoot: postureWorkRoot(t, "ork-sigprobe"),
			})
			t.Cleanup(activities.Close)
			const grace = 3 * time.Second
			orker.SetSignalProbeGrace(activities, grace)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()

			started := time.Now()
			err = activities.Boot(ctx)
			elapsed := time.Since(started)

			raw, readErr := os.ReadFile(filepath.Join(dir, "calls"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			name, terms := "", 0
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Fields(line)
				if len(fields) > 0 && fields[0] == "run" && strings.Contains(line, "ork.task=sigprobe") {
					for j, field := range fields {
						if field == "--name" && j+1 < len(fields) {
							name = fields[j+1]
						}
					}
				}
				if strings.HasPrefix(line, "kill --signal TERM ") {
					terms++
				}
			}
			if name == "" || terms != 1 {
				t.Fatalf("the probe created %q and sent %d SIGTERMs; want its container, signalled once:\n%s", name, terms, raw)
			}
			// The daemon removes it once it has exited, which may be just
			// after Boot returns.
			for deadline := time.Now().Add(time.Minute); ; {
				out, inspectErr := exec.Command(realDocker, "container", "inspect", "--format", "{{.State.Status}}", name).CombinedOutput()
				if inspectErr != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Errorf("the SIGTERM probe's container %s is still there a minute after Boot: %s", name, out)
					break
				}
				time.Sleep(200 * time.Millisecond)
			}

			if !tt.drop {
				if err != nil {
					t.Fatalf("Boot = %v; want it to boot on an engine that delivers SIGTERM", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), orker.ReasonRuntimePostureRefused+":") {
				t.Fatalf("Boot = %v; want a refusal starting %s:", err, orker.ReasonRuntimePostureRefused)
			}
			for _, want := range []string{"never heard SIGTERM", "its trap had not run at the end of the grace"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Boot = %v; want it to say %q", err, want)
				}
			}
			t.Logf("refused after %s: %v", elapsed, err)
			if elapsed < grace {
				t.Errorf("Boot refused after %s, before the %s grace had passed", elapsed, grace)
			}
		})
	}
}
