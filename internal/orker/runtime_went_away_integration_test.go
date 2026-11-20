//go:build integration

package orker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// nestedUnixDaemon is nestedDaemon (container_removal_integration_test.go) on
// a unix socket instead of a published TCP port: a real dockerd in a container
// of this test's own, never given the host's Docker socket, reached through a
// socket it creates in a directory of the test's own, bound at /ork-nd inside
// the container. Not at its host path: the dind image mounts a tmpfs over
// /tmp, which would hide a directory bound there, and dockerd then cannot
// create its socket (measured 2026-11-20). Nothing listens on the host's network, so a
// daemon that answers root-equivalent requests is not on a port other proofs
// on this host could reach.
//
// The socket is made reachable by the test's user by a chmod 0666 the daemon's
// own loop runs once the daemon answers on it (dockerd's --group would need a
// gid the test cannot assume, and chmod before the daemon has finished
// creating the socket would be undone by it). The loop is the daemon's process
// manager, as nestedDaemon's is: killing dockerd is a restart, on the same
// state, and the socket is removed and made again each time.
//
// --network none was weighed and not used: the nested daemon pulls the task's
// pinned image itself (the test image is made local through the first
// baseInput, as in the restart test), which needs egress, and loading it with
// save and load cannot be proven to keep its digest name under both image
// stores. So the daemon listens on its unix socket alone, never a TCP host,
// publishes no port, and sits on a user-defined bridge of the test's own, not
// the default one, so no other container on this host shares its network.
// The container and the bridge are removed by their own ids, and the
// container's outer timeout ends it even if this process is killed before the
// cleanup.
//
// daemonPID is the pid dockerd has inside the daemon's container now, empty
// while none runs: the proof that a daemon did, or did not, restart.
func nestedUnixDaemon(ctx context.Context, t *testing.T, workRoot string) (host string, signalDaemon func(sig string), daemonPID func() string) {
	t.Helper()
	cli := containerCLI(t)
	sockDir, err := os.MkdirTemp("", "ork-nd-")
	must.OK(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "docker.sock")
	const inner = "/ork-nd/docker.sock"
	loop := fmt.Sprintf(`while :; do rm -f %[1]s; `+
		`( until docker -H unix://%[1]s version >/dev/null 2>&1; do sleep 0.1; done; chmod 0666 %[1]s ) & `+
		// One --host, a unix socket. The pinned entrypoint adds its own unix
		// socket and a TCP listener (tcp://0.0.0.0:2375) only when given no
		// command or flags alone; it is given `dockerd`, so it adds neither.
		`dockerd-entrypoint.sh dockerd --host=unix://%[1]s; sleep 0.2; done`, inner)
	hostEnv := os.Environ()
	bridge, err := exec.CommandContext(ctx, cli, "network", "create", "--driver", "bridge",
		"ork-nd-"+strings.TrimPrefix(filepath.Base(sockDir), "ork-nd-")).Output()
	if err != nil {
		infraAbsent(t, "create the nested daemon's own bridge: %v", err)
	}
	bridgeID := strings.TrimSpace(string(bridge))
	t.Cleanup(func() {
		cmd := exec.Command(cli, "network", "rm", bridgeID)
		cmd.Env = hostEnv
		_ = cmd.Run()
	})
	args := []string{"run", "--rm", "--detach", "--privileged", "--network", bridgeID}
	{
		servers := upstreamNameservers(t)
		resolv := filepath.Join(t.TempDir(), "resolv.conf")
		var conf strings.Builder
		for _, ns := range servers {
			conf.WriteString("nameserver " + ns + "\n")
		}
		must.OK(t, os.WriteFile(resolv, []byte(conf.String()), 0o644))
		args = append(args, "--volume", resolv+":/etc/resolv.conf:ro")
	}
	out, err := exec.CommandContext(ctx, cli, append(args,
		"--volume", workRoot+":"+workRoot, "--volume", sockDir+":/ork-nd",
		"--env", "DOCKER_TLS_CERTDIR=", "--entrypoint", "timeout", dindImage,
		"-s", "KILL", "900", "sh", "-c", loop)...).Output()
	if err != nil {
		infraAbsent(t, "start a nested daemon: %v", err)
	}
	id := strings.TrimSpace(string(out))
	// hostEnv, read before the test points DOCKER_HOST at the nested daemon,
	// reaches the daemon that holds its container and bridge. The container's
	// cleanup runs before the bridge's (cleanups run last first), so the
	// bridge is never removed while the container is on it.
	t.Cleanup(func() {
		// The daemon's own account of a failure, before its removal takes
		// the log with it.
		if t.Failed() {
			logs := exec.Command(cli, "logs", "--tail", "60", id)
			logs.Env = hostEnv
			if out, err := logs.CombinedOutput(); err == nil {
				t.Logf("the nested daemon's log:\n%s", out)
			}
		}
		cmd := exec.Command(cli, "rm", "--force", "--volumes", id)
		cmd.Env = hostEnv
		_ = cmd.Run()
	})
	signalDaemon = func(sig string) {
		cmd := exec.CommandContext(ctx, cli, "exec", id, "pkill", "-"+sig, "-x", "dockerd")
		cmd.Env = hostEnv
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("signal the nested daemon: %v: %s", err, out)
		}
	}
	daemonPID = func() string {
		cmd := exec.CommandContext(ctx, cli, "exec", id, "pgrep", "-x", "dockerd")
		cmd.Env = hostEnv
		out, err := cmd.Output()
		if err != nil {
			return ""
		}
		return strings.Join(strings.Fields(string(out)), ",")
	}
	host = "unix://" + sock
	awaitNestedDaemon(ctx, t, host)
	return host, signalDaemon, daemonPID
}

// asApplicationError is err as the error an attempt reports to Temporal.
func asApplicationError(t *testing.T, err error) *temporal.ApplicationError {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("the attempt ended with %v, which is not an application error", err)
	}
	return app
}

// TestIntegration_ARuntimeThatWentAwayIsAnInterruption: a container stopped
// from outside whose runtime then ends the Orker's event stream by itself is an
// interruption the engine runs again, even though the Orker is not stopping
// (AGENTS.md §3.2, §4). The same stop through a daemon that stays up is an
// operator's and stays the task's own, retryable failure under its policy.
//
// The two cases share one real daemon, the positive control first: it is the
// stop that leaves the daemon running, so the case that restarts it proves
// the difference is the runtime's going away and not the stop.
func TestIntegration_ARuntimeThatWentAwayIsAnInterruption(t *testing.T) {
	requireDocker(t)
	if onPodman(t) {
		t.Log("Podman has no daemon to restart: its API service holds no containers, and each container has a monitor of its own")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cli := containerCLI(t)
	workRoot := removalWorkRoot(t, "went-away")
	host, signalDaemon, daemonPID := nestedUnixDaemon(ctx, t, workRoot)
	// The Orker and this test's own reads now reach the nested daemon.
	t.Setenv("DOCKER_HOST", host)
	activities := removalLife(ctx, t, proof.OrkerID("went-away"), workRoot, orker.NewDockerRuntime())
	// The engine lists the interruption: this Orker may emit it only when the
	// stop is the stop's doing, or the runtime is seen to have gone away.
	orker.ListInterruptions(activities.Dispatcher(), orker.InterruptionType(orker.ReasonContainerStoppedByRuntime))
	const ticking = `while :; do echo tick; sleep 1; done`

	// attempt runs one task, lets stop act on its container, and returns how
	// the attempt ended and the run id its containers are labelled with.
	attempt := func(t *testing.T, name string, stop func(container string)) (string, error) {
		t.Helper()
		in := baseInput(t, "went-away", "/bin/sh", "-c", ticking)
		in.RunId = removalRunID("went-away-" + name)
		in.CommitUri = "mem://commits/" + in.GetRunId() + "/commit.json"
		in.IdempotencyKey = "default/" + in.GetRunId() + "/went-away"
		t.Cleanup(func() { removeByLabel(t, "ork.run="+in.GetRunId()) })
		done := make(chan error, 1)
		go func() {
			_, err := activities.Dispatcher().Execute(ctx, in)
			done <- err
		}()
		awaitCtx, cancelAwait := context.WithTimeout(ctx, 2*time.Minute)
		defer cancelAwait()
		container := awaitLabelled(awaitCtx, t, "ork.run="+in.GetRunId())
		awaitTicks(awaitCtx, t, container, 1)
		stop(container)
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("the attempt succeeded though its container was stopped from outside")
			}
			return in.GetRunId(), err
		case <-ctx.Done():
			t.Fatalf("the attempt never ended: %v", ctx.Err())
		}
		return "", nil
	}
	// settled brings the daemon back, takes what the attempt left, and checks
	// that nothing of the run is left.
	settled := func(t *testing.T, runID string) {
		t.Helper()
		awaitNestedDaemon(ctx, t, host)
		activities.RetryRemovals(ctx)
		if left := containersLabelled(ctx, t, "ork.run="+runID); len(left) != 0 {
			t.Fatalf("containers left after the daemon was back and a retry ran: %v", left)
		}
	}

	t.Run("a stop through a daemon that stays up is not an interruption", func(t *testing.T) {
		before := daemonPID()
		if before == "" {
			t.Fatal("the nested daemon runs no dockerd; the case proves nothing")
		}
		runID, err := attempt(t, "stop", func(container string) {
			if out, err := exec.CommandContext(ctx, cli, "stop", "--time", "1", container).CombinedOutput(); err != nil {
				t.Fatalf("docker stop %s: %v: %s", container, err, out)
			}
		})
		app := asApplicationError(t, err)
		if app.Type() != orker.CategoryInfrastructureError || app.NonRetryable() {
			t.Errorf("the attempt ended type %q non-retryable=%v, want a retryable %s: the engine's policy decides it, not an interruption",
				app.Type(), app.NonRetryable(), orker.CategoryInfrastructureError)
		}
		if !strings.Contains(err.Error(), orker.ReasonContainerStoppedByRuntime) {
			t.Errorf("the attempt ended %v, want %s", err, orker.ReasonContainerStoppedByRuntime)
		}
		if strings.Contains(err.Error(), "the runtime itself went away") {
			t.Errorf("the attempt says its runtime went away though the daemon stayed up: %v", err)
		}
		// Positive control: the window passed with the daemon running, the very
		// process that was running before the stop.
		if after := daemonPID(); after != before {
			t.Fatalf("dockerd was %q before the stop and %q after it; the daemon did not stay up, so the case proves nothing", before, after)
		}
		settled(t, runID)
	})

	t.Run("a daemon that restarts under the container is an interruption", func(t *testing.T) {
		before := daemonPID()
		restarted := time.Now()
		runID, err := attempt(t, "restart", func(string) {
			// What `systemctl restart docker` sends.
			signalDaemon("TERM")
		})
		t.Logf("observed: the attempt ended %s after the daemon was told to stop, with: %v",
			time.Since(restarted).Round(time.Millisecond), err)
		app := asApplicationError(t, err)
		if want := orker.InterruptionType(orker.ReasonContainerStoppedByRuntime); app.Type() != want || !app.NonRetryable() {
			t.Errorf("the attempt ended type %q non-retryable=%v, want %q non-retryable: the Orker was not stopping, but its runtime went away",
				app.Type(), app.NonRetryable(), want)
		}
		if !strings.Contains(err.Error(), "the runtime itself went away") {
			t.Errorf("the attempt ended %v, want it to say the runtime itself went away", err)
		}
		var detail *orkstackv1.ActivityFailureDetail
		if err := app.Details(&detail); err != nil || detail.GetReasonCode() != orker.ReasonContainerStoppedByRuntime {
			t.Errorf("the failure detail's reason = %q (%v), want %s", detail.GetReasonCode(), err, orker.ReasonContainerStoppedByRuntime)
		}
		settled(t, runID)
		// Positive control: the daemon did restart, a new dockerd answers.
		if after := daemonPID(); after == "" || after == before {
			t.Errorf("dockerd was %q before the signal and %q after the daemon was back; it never restarted", before, after)
		}
	})
}

// upstreamNameservers is the resolvers written into the nested daemon's own
// /etc/resolv.conf, so it can pull the pinned images it runs. On a
// user-defined bridge a container resolves through Docker's embedded DNS at
// 127.0.0.11, reached by DNAT rules in the container's own network namespace,
// and a dockerd running inside it rewrites that namespace's iptables, which
// leaves every lookup failing with "server misbehaving" (measured 2026-11-20);
// --dns would not help, since it only changes where 127.0.0.11 forwards. So
// the container is given the upstream resolvers directly: those behind
// systemd-resolved's stub (/run/systemd/resolve/resolv.conf), else the
// non-loopback ones the host names. A host that names none fails the test:
// the nested daemon could resolve nothing, and its pulls would fail for that.
func upstreamNameservers(t *testing.T) []string {
	t.Helper()
	for _, path := range []string{"/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var out []string
		seen := map[string]bool{}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 || fields[0] != "nameserver" {
				continue
			}
			ip := net.ParseIP(fields[1])
			if ip == nil || ip.IsLoopback() || ip.To4() == nil || seen[fields[1]] {
				continue
			}
			seen[fields[1]] = true
			out = append(out, fields[1])
		}
		if len(out) > 0 {
			t.Logf("the nested daemon resolves through %v, from %s", out, path)
			return out
		}
	}
	t.Fatalf("no non-loopback IPv4 nameserver in /run/systemd/resolve/resolv.conf or /etc/resolv.conf: " +
		"the nested daemon would have no resolver it can reach")
	return nil
}
