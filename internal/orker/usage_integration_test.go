//go:build integration

package orker_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_UsageLocatorFindsTheContainersCgroup holds the usage
// sampler's cgroup lookup to a real engine: Docker in the merge gate, and
// rootless and rootful Podman, by its CLI and by Docker's CLI on its socket, in
// the podman workflow. Each engine names a container's cgroup leaf its own way
// (docker-<id>.scope, /docker/<id>, libpod-<id>.scope, its container child),
// so the parser's table is only as good as the layouts these hosts really
// have. On a host that gives the container a cgroup, the locator finds it by
// the engine's pid, reads CPU time that grows and the memory limit the
// container was given, and a read after the container is removed fails rather
// than reporting the last numbers again. A rootless engine on cgroup v1 gives
// the container no cgroup to read, and the locator says so.
func TestIntegration_UsageLocatorFindsTheContainersCgroup(t *testing.T) {
	requireDocker(t)
	cli := containerCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	requireProbeImage(ctx, t, cli)
	info := runtimeInfo(t)
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	v2 := err == nil

	const limit = 64 << 20
	label := "ork.orker=" + proof.OrkerID("usage-locator")
	out, err := exec.CommandContext(ctx, cli, "run", "--detach", "--init", "--pids-limit", "64",
		"--memory", "64m", "--cpus", "0.5", "--network", "none", "--label", label,
		orker.BindProbeImage, "/bin/sh", "-c", "while :; do :; done").Output()
	must.OKf(t, err, "start the container")
	id := strings.TrimSpace(string(out))
	removed := false
	t.Cleanup(func() {
		if !removed {
			_ = exec.Command(cli, "rm", "--force", id).Run()
		}
	})

	read, method, err := orker.LocateUsage(ctx, orker.NewDockerRuntime(), orker.ContainerID(id))
	if info.Rootless && !v2 {
		if !errors.Is(err, orker.ErrNoCgroup) {
			t.Fatalf("LocateUsage on rootless %s, cgroup v1 = (%q, %v); want ErrNoCgroup", info.Name, method, err)
		}
		t.Logf("rootless %s on cgroup v1: no readable cgroup, as expected", info.Name)
		return
	}
	if err != nil {
		t.Fatalf("LocateUsage on %s (cli %s, rootless %v, cgroup v2 %v) = %v; /proc/<pid>/cgroup:\n%s",
			info.Name, info.CLI, info.Rootless, v2, err, procCgroupOf(ctx, t, cli, id))
	}
	if want := map[bool]string{true: "cgroup-v2", false: "cgroup-v1"}[v2]; method != want {
		t.Fatalf("method = %q on a host whose cgroup v2 is %v; want %q", method, v2, want)
	}

	first, err := read()
	must.OKf(t, err, "first read")
	if first.Limit != limit {
		t.Fatalf("limit = %d; want the container's own %d", first.Limit, limit)
	}
	if first.Memory <= 0 {
		t.Fatalf("memory = %d; a running container uses some", first.Memory)
	}
	// The busy loop's CPU time grows; a reader stuck on another cgroup, or on
	// a parent that a short-lived container never charged, would not.
	deadline := time.Now().Add(30 * time.Second)
	for {
		now, err := read()
		must.OKf(t, err, "read while running")
		if now.CPUSeconds > first.CPUSeconds {
			t.Logf("%s (cli %s, rootless %v): %s; cpu %.3fs → %.3fs, memory %d, peak %d (kernel's %v), limit %d",
				info.Name, info.CLI, info.Rootless, method, first.CPUSeconds, now.CPUSeconds, now.Memory, now.MemoryPeak, now.PeakRead, now.Limit)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cpu stayed at %.3fs for 30 s of a busy loop", now.CPUSeconds)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}

	must.OKf(t, exec.CommandContext(ctx, cli, "rm", "--force", id).Run(), "remove the container")
	removed = true
	deadline = time.Now().Add(30 * time.Second)
	for {
		if _, err := read(); err != nil {
			t.Logf("a read after removal fails: %v", err)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("reads still succeed 30 s after the container was removed")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// procCgroupOf is the container's /proc/<pid>/cgroup, for a failure message
// that shows the layout the parser did not know.
func procCgroupOf(ctx context.Context, t *testing.T, cli, id string) string {
	t.Helper()
	pid, err := exec.CommandContext(ctx, cli, "inspect", "--format", "{{.State.Pid}}", id).Output()
	if err != nil {
		return "(no pid: " + err.Error() + ")"
	}
	list, err := os.ReadFile("/proc/" + strings.TrimSpace(string(pid)) + "/cgroup")
	if err != nil {
		return "(" + err.Error() + ")"
	}
	return string(list)
}
