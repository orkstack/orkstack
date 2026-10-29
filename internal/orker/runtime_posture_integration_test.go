//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// argvShim is the real docker CLI behind a shim that rewrites a `docker run`'s
// arguments before the real daemon sees them, the way a runtime that ignores,
// weakens or adds to what it was asked for behaves. Everything else, and every
// argument it does not rewrite, reaches the real daemon, so the container the
// probe judges is a real one.
//
// Rewrites (files under dir): replace (lines "FROM<TAB>TO": an argument equal
// to FROM becomes TO), prepend (lines: arguments added right after `run`) and
// sub (one line "FROM<TAB>TO": FROM becomes TO inside every bind mount, and
// the rewritten source is created, as Podman's Docker-compatible socket
// creates a bind source that does not exist). `calls` records every
// invocation.
type argvShim struct {
	t   *testing.T
	dir string
	bin string
}

func newArgvShim(t *testing.T, realDocker string) *argvShim {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		infraAbsent(t, "bash is not on PATH, and the argument-rewriting shim needs its arrays: %v", err)
	}
	dir := t.TempDir()
	script := `#!/usr/bin/env bash
S='` + dir + `'
real='` + realDocker + `'
echo "$*" >> "$S/calls"
if [ "$1" != run ]; then exec "$real" "$@"; fi
shift
args=(run)
if [ -e "$S/prepend" ]; then
  while IFS= read -r line; do [ -n "$line" ] && args+=("$line"); done < "$S/prepend"
fi
for a in "$@"; do
  if [ -e "$S/replace" ]; then
    while IFS=$'\t' read -r from to; do
      if [ "$a" = "$from" ]; then a=$to; fi
    done < "$S/replace"
  fi
  if [ -e "$S/sub" ]; then
    IFS=$'\t' read -r from to < "$S/sub"
    case "$a" in
      type=bind,source=*)
        a=${a//"$from"/"$to"}
        src=${a#type=bind,source=}; src=${src%%,destination=*}
        mkdir -p "$src"
        case "$a" in *destination=/orkstack/input,*) mkdir -p "$src/secrets" ;; esac ;;
    esac
  fi
  args+=("$a")
done
exec "$real" "${args[@]}"
`
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return &argvShim{t: t, dir: dir, bin: bin}
}

func (s *argvShim) set(flag, content string) {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(s.dir, flag), []byte(content), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

// probeRuns returns every `docker run` of the boot probe's first container
// the shim saw, before its rewrites.
func (s *argvShim) probeRuns() []string {
	s.t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.dir, "calls"))
	if err != nil {
		s.t.Fatal(err)
	}
	var runs []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "run ") && strings.Contains(line, "ork.task=bindprobe") {
			runs = append(runs, line)
		}
	}
	return runs
}

// postureWorkRoot is a fresh work root the daemon can bind, under the
// repository's .data/work as every Orker integration test's is.
func postureWorkRoot(t *testing.T, name string) string {
	t.Helper()
	parent := "../../.data/work"
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, proof.WorkRootPattern(name))
	if err != nil {
		t.Fatal(err)
	}
	if root, err = filepath.Abs(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// TestIntegration_BootProbeJudgesATasksContainerOnTheRealDaemon is G16, G24
// and G10 on a real daemon. The boot probe runs a container built exactly as
// a task's is, and judges what the daemon gave it from inside and from the
// host; each case has the CLI weaken one thing on its way to the daemon, and
// Boot refuses with the reason that names it. The first case is the positive
// control: the same probe, unrewritten, boots, which shows every other
// refusal is the rewrite's and not the host's.
//
//   - G16: a runtime that keeps capabilities, applies no seccomp filter, lets
//     privileges be gained, leaves /run writable, sets no pids limit or puts
//     a proxy variable into the container's environment is refused at boot as
//     RUNTIME_POSTURE_REFUSED, naming what was weaker and never a proxy's
//     value, which can carry a credential;
//   - G24: a runtime that satisfies a bind by creating a directory of its own
//     (Podman's Docker-compatible socket does) is refused as
//     WORKDIR_NOT_BINDABLE: the nonces the Orker and the container wrote did
//     not reach each other;
//   - G10: a runtime that runs the "as the Orker" user as another host user
//     boots, unless every task is forced to run that way, in which case it is
//     refused as RUN_AS_ORKER_UNAVAILABLE.
func TestIntegration_BootProbeJudgesATasksContainerOnTheRealDaemon(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	setup, cancelSetup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSetup()
	requireProbeImage(setup, t, realDocker)
	orkerUser := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	// The Podman engine is given no seccomp option, which it fills with its
	// own default, so a weaker filter is asked for rather than rewritten;
	// and its /run is a read-only tmpfs of its own, so a writable root
	// leaves /run read-only there.
	info := runtimeInfo(t)
	podman := info.Name == orker.CLIPodman
	// The Orker's own user lands as the Orker unless a rootless engine maps
	// it elsewhere and the CLI cannot keep it: the docker CLI against a
	// rootless Podman socket, run by a non-root Orker (the Orker as root
	// inside the runtime is that shape's supported form).
	ownUserLands := os.Geteuid() == 0 || !info.Rootless || info.CLI == orker.CLIPodman
	// Without --pids-limit a container gets its engine's default: no limit
	// on Docker and on a rootful Podman socket ("max", which the probe names),
	// and on a rootless Podman socket a limit of 1, under which the probe's
	// init cannot even fork. Either is refused, in the probe's words or the
	// runtime's; which one depends on the host's engine, not on the probe.
	noPidsSays := []string{"pids.max"}
	if podman && info.CLI == orker.CLIDocker {
		noPidsSays = []string{"pids.max|failed to fork"}
	}
	noSeccomp := struct{ replace, prepend string }{replace: "seccomp=" + orker.SeccompBuiltin + "\tseccomp=unconfined\n"}
	writableRoot := []string{"/ is writable", "/run is writable", "/var/tmp is writable"}
	if podman {
		noSeccomp = struct{ replace, prepend string }{prepend: "--security-opt\nseccomp=unconfined\n"}
		writableRoot = []string{"/ is writable"}
	}
	// As root the Orker reads any file, so a container user that lands as
	// someone else still leaves files it can read: the hatch does what it is
	// for, and the probe says so.
	hatchLands := os.Geteuid() == 0

	cases := []struct {
		name      string
		replace   string // FROM<TAB>TO lines
		prepend   string
		elsewhere bool // bind the probe's sources from another root, created on the way
		force     bool
		// wantReason is "" for a boot; wantSays are in the refusal (one of
		// the |-separated alternatives each), and mustNotSay never is.
		wantReason string
		wantSays   []string
		mustNotSay string
	}{
		{name: "the probe as it is boots"},
		{name: "capabilities kept", replace: "--cap-drop=ALL\t--cap-drop=NET_RAW\n",
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: []string{"CapBnd"}},
		{name: "no seccomp filter", replace: noSeccomp.replace, prepend: noSeccomp.prepend,
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: []string{"Seccomp"}},
		{name: "privileges can be gained", replace: "no-new-privileges\tno-new-privileges=false\n",
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: []string{"NoNewPrivs"}},
		{name: "a writable root, and with it /run", replace: "--read-only\t--read-only=false\n",
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: writableRoot},
		{name: "no pids limit", replace: "--pids-limit\t--label\n",
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: noPidsSays},
		{name: "a proxy the task never set", prepend: "--env\nHTTP_PROXY=http://user:hunter2@proxy.invalid:3128\n",
			wantReason: orker.ReasonRuntimePostureRefused, wantSays: []string{"HTTP_PROXY"}, mustNotSay: "hunter2"},
		{name: "a bind source the runtime made itself", elsewhere: true,
			wantReason: orker.ReasonWorkdirNotBindable, wantSays: []string{"did not bind"}},
		{name: "the Orker's user landing as another, not forced", replace: orkerUser + "\t12345:12345\n"},
		{name: "the Orker's user landing as another, forced", replace: orkerUser + "\t12345:12345\n", force: true,
			wantReason: map[bool]string{false: orker.ReasonRunAsOrkerUnavailable, true: ""}[hatchLands],
			wantSays:   map[bool][]string{false: {orker.ForceUserVar, "uid"}, true: nil}[hatchLands]},
		{name: "the Orker's own user, forced", force: true,
			wantReason: map[bool]string{false: orker.ReasonRunAsOrkerUnavailable, true: ""}[ownUserLands]},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			shim := newArgvShim(t, realDocker)
			if tt.replace != "" {
				shim.set("replace", tt.replace)
			}
			if tt.prepend != "" {
				shim.set("prepend", tt.prepend)
			}
			workRoot := postureWorkRoot(t, "ork-posture")
			if tt.elsewhere {
				shim.set("sub", workRoot+"\t"+postureWorkRoot(t, "ork-posture-elsewhere")+"\n")
			}
			store, err := artifacts.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			policy := orker.SecurityPolicyFromEnv()
			policy.ForceUser = tt.force
			var logs bytes.Buffer
			activities := orker.NewActivitiesWithOptions(store, orker.Options{
				Runtime:  &orker.DockerRuntime{Bin: shim.bin, BindProbeTimeout: 90 * time.Second},
				OrkerID:  proof.OrkerID(fmt.Sprintf("posture-%d", i)),
				WorkRoot: workRoot,
				Policy:   &policy,
				Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
			})
			t.Cleanup(activities.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()

			err = activities.Boot(ctx)

			runs := shim.probeRuns()
			if len(runs) != 1 {
				t.Fatalf("the probe ran %d containers (%q); want one", len(runs), runs)
			}
			flags := []string{
				"--init", "--cap-drop=ALL", "--read-only",
				"--tmpfs /tmp:rw,nosuid,nodev,noexec", "--pids-limit", "--memory", "--cpus",
				"--user " + orkerUser, orker.BindProbeImage,
			}
			if podman {
				flags = append(flags, "--tmpfs /run:ro,size=1m", "--tmpfs /var/tmp:ro,size=1m")
			} else {
				flags = append(flags, "--security-opt seccomp="+orker.SeccompBuiltin)
			}
			for _, flag := range flags {
				if !strings.Contains(runs[0], flag) {
					t.Errorf("the probe's container was asked for without %q: %s", flag, runs[0])
				}
			}
			if tt.wantReason == "" {
				if err != nil {
					t.Fatalf("Boot = %v; want it to boot", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+":") {
				t.Fatalf("Boot = %v; want a refusal starting %s:", err, tt.wantReason)
			}
			for _, want := range tt.wantSays {
				if !slices.ContainsFunc(strings.Split(want, "|"), func(alternative string) bool {
					return strings.Contains(err.Error(), alternative)
				}) {
					t.Errorf("Boot = %v; want the refusal to name %q", err, want)
				}
			}
			if tt.mustNotSay != "" && strings.Contains(err.Error()+logs.String(), tt.mustNotSay) {
				t.Errorf("the refusal or the log quotes %q, a value the probe must never repeat:\n%v\n%s", tt.mustNotSay, err, logs.String())
			}
		})
	}
}

// TestIntegration_RemoveOfARunningContainerThatIgnoresTERMIsPrompt is G23 on
// a real daemon. Podman's `rm --force`, behind its CLI and its Docker socket
// alike, stops a running container and waits out its stop timeout before it
// removes it: 30 s for a workload that ignores SIGTERM, which is the whole of
// the control timeout a removal runs under. The shim gives the real docker CLI
// exactly that behaviour, and Remove still finishes at once, because it kills
// the container before it asks for the removal.
func TestIntegration_RemoveOfARunningContainerThatIgnoresTERMIsPrompt(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	requireProbeImage(ctx, t, realDocker)

	dir := t.TempDir()
	shim := filepath.Join(dir, "docker")
	script := `#!/bin/sh
if [ "$1" = rm ]; then
  for id in "$@"; do :; done
  "` + realDocker + `" stop --time 30 "$id" >/dev/null 2>&1
fi
exec '` + realDocker + `' "$@"
`
	if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	label := "ork.orker=" + proof.OrkerID("remove-term-ignorer")
	// A pids limit, as every Orker container has: the Podman socket's default
	// is one process, which leaves its init unable to fork.
	out, err := exec.CommandContext(ctx, realDocker, "run", "--detach", "--init", "--pids-limit", "64", "--label", label,
		"--network", "none", orker.BindProbeImage, "/bin/sh", "-c", `trap "" TERM; while :; do sleep 1; done`).Output()
	if err != nil {
		t.Fatalf("start the TERM-ignoring container: %v", err)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command(realDocker, "rm", "--force", id).Run() })
	running, err := exec.CommandContext(ctx, realDocker, "inspect", "--format", "{{.State.Running}}", id).Output()
	if err != nil || strings.TrimSpace(string(running)) != "true" {
		t.Fatalf("the container is not running before the removal (%q, %v); the case proves nothing", running, err)
	}

	started := time.Now()
	err = (&orker.DockerRuntime{Bin: shim}).Remove(ctx, orker.ContainerID(id))
	elapsed := time.Since(started)

	if err != nil {
		t.Fatalf("Remove = %v", err)
	}
	if elapsed > 15*time.Second {
		t.Fatalf("Remove took %s; a removal that waits out the stop timeout takes 30 s", elapsed)
	}
	if err := exec.CommandContext(ctx, realDocker, "inspect", id).Run(); err == nil {
		t.Fatalf("container %s still exists after Remove", id)
	}
	t.Logf("removed a running TERM-ignorer in %s behind a CLI whose rm waits out the stop timeout", elapsed)
}

// TestIntegration_ACLIThatWritesOnStderrStillRunsATask is G21 on a real
// daemon: the docker CLI behind a shim that writes the podman-docker shim's
// own line on stderr before every command. The runtime read every answer from
// the merged streams, so the image inspect no longer decoded and every task
// failed IMAGE_PULL_FAILED. Every answer is now read from stdout alone, and
// the task runs and commits as it does on the plain CLI.
func TestIntegration_ACLIThatWritesOnStderrStillRunsATask(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	dir := t.TempDir()
	shim := filepath.Join(dir, "docker")
	script := `#!/bin/sh
echo "Emulate Docker CLI using podman. Create /etc/containers/nodocker to quiet msg." >&2
exec '` + realDocker + `' "$@"
`
	if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := newDispatcher(t)
	dispatcher.Runtime = &orker.DockerRuntime{Bin: shim}
	in := baseInput(t, "noisy-cli", "/bin/sh", "-c", `printf '{"ok":true}' > /orkstack/output/values.json`)
	in.CommitUri = "mem://commits/" + in.RunId + "/noisy-cli/commit.json"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := dispatcher.Execute(ctx, in)

	if err != nil {
		t.Fatalf("Execute behind a CLI that writes on stderr = %v", err)
	}
	if got := string(result.GetValuesJson()); got != `{"ok":true}` {
		t.Fatalf("values = %q; want the workload's", got)
	}
	if !strings.HasPrefix(result.GetImage().GetPlatform(), "linux/") {
		t.Fatalf("image platform = %q; want the inspect's answer, read from stdout", result.GetImage().GetPlatform())
	}
}
