package orker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// countingDocker is a stand-in docker CLI that appends every invocation to a
// calls file and reads its behaviour from flag files in its state directory,
// so a test can count how many processes one runtime call spawns
// (F-orker-storage-perf-5, -9, -10) without a daemon.
//
// Flags (files under dir): present (the image is local), pullfail, oom,
// exitcode (Wait's status), created (`inspect {{.State.Status}}` answers
// "created" while it exists), rmfail-<id>, psfail, versionfail, runfail,
// slowruns (its content N: the first N `run` invocations hang for 30 s, as a
// daemon that is slow to start a container does), timeoutruns (its content N:
// the first N `run` invocations answer at once with the daemon's own deadline
// error, as a daemon whose containerd cannot start the shim does), runother (a
// refusal that is neither a bind nor a deadline), runexit (the container ran
// and exited 3), runfailsrc (a refusal that quotes the bind source path),
// inspecthang (`image inspect` hangs for 30 s), rmfailall (every `rm` is
// refused), noise (every command first writes a line of its own on stderr, as
// the podman-docker shim does) and killnotrunning (`kill` answers that the
// container is not running). For the Podman dialects: podmanengine
// (`version`'s components name the Podman engine), rootless (the content
// `info {{.Host.Security.Rootless}}` answers), infofail (`info` fails),
// versionhang and infohang (`version` or `info` hangs for 30 s) and state
// (the content `inspect {{.State.Status}}` answers). `run` also counts
// itself into the runs file and, once counted, touches runstarted.
//
// A `run` that succeeds behaves as the boot probe's container does on a
// runtime that gives it a task's posture: it reads the nonce the Orker wrote
// into the input bind and prints it, writes its own nonce (its last argument)
// into the output bind, and prints the facts file, which newCountingDocker
// fills with a posture that passes for SecurityPolicyFromEnv. Three more flags
// make it the probe of a runtime that does not: wrongdir (the container sees
// an input directory with nothing in it), nowrite (its writes do not reach the
// Orker's output directory) and asorkerwrong (the file it writes as the
// Orker's user does not hold what it wrote).
//
// A `run --detach` of the boot probe's SIGTERM container (label
// ork.task=sigprobe) is not counted with the other runs and behaves as its
// workload does where SIGTERM is delivered: it writes the ready file into the
// output bind, with the label file's content (unconfined by default) and the
// signals file's (a shell that traps SIGTERM), and a later `kill --signal
// TERM` writes the file its trap writes. deaf makes the workload never hear
// SIGTERM, neverready makes it never say it is ready, termrefused makes the
// runtime refuse SIGTERM itself, and sigrunslow, sigruntimeout and sigrunother
// make that run hang for 30 s, answer the daemon's own deadline error, or be
// refused for another reason. Every `kill --signal TERM` touches termsent.
type countingDocker struct {
	t   *testing.T
	dir string
	bin string
}

func newCountingDocker(t *testing.T) *countingDocker {
	t.Helper()
	// A stand-in of the docker CLI, whatever CLI the environment points the
	// suite's real runtime at (ORK_DOCKER_CLI=podman in the Podman lane): a
	// test that wants the podman CLI's grammar says so on the runtime.
	t.Setenv(CLIVar, "")
	dir := t.TempDir()
	script := `#!/bin/sh
S=` + shellQuote(dir) + `
echo "$*" >> "$S/calls"
if [ -e "$S/noise" ]; then echo "Emulate Docker CLI using podman. Create /etc/containers/nodocker to quiet msg." >&2; fi
case "$1" in
  image)
    if [ -e "$S/inspecthang" ]; then exec sleep 30; fi
    if [ -e "$S/present" ]; then
      echo '{"Id":"sha256:abc","Os":"linux","Architecture":"amd64"}'
      exit 0
    fi
    echo "Error: No such image" >&2; exit 1 ;;
  pull)
    if [ -e "$S/pullfail" ]; then echo "pull access denied" >&2; exit 1; fi
    touch "$S/present"; exit 0 ;;
  inspect)
    case "$3" in
      *OOMKilled*) if [ -e "$S/oom" ]; then echo true; else echo false; fi ;;
      *) if [ -e "$S/state" ]; then cat "$S/state"; elif [ -e "$S/created" ]; then echo created; else echo running; fi ;;
    esac ;;
  create) echo cid-0123 ;;
  kill)
    if [ -e "$S/killnotrunning" ]; then echo "Error response from daemon: container $4 is not running" >&2; exit 1; fi
    if [ "$3" = TERM ]; then touch "$S/termsent"; fi
    if [ "$3" = TERM ] && [ -e "$S/termrefused" ]; then echo "Error response from daemon: cannot kill container: permission denied" >&2; exit 1; fi
    if [ "$3" = TERM ] && [ -e "$S/sigout" ] && [ ! -e "$S/deaf" ]; then echo heard > "$(cat "$S/sigout")/probe-heard-term"; fi
    exit 0 ;;
  start) sleep 5 ;;
  wait) if [ -e "$S/exitcode" ]; then cat "$S/exitcode"; else echo 0; fi ;;
  ps)
    if [ -e "$S/psfail" ]; then echo "daemon unreachable" >&2; exit 1; fi
    echo c1; echo c2; echo c3 ;;
  rm)
    if [ -e "$S/rmfailall" ]; then echo "Error response from daemon: removal of container $4 is already in progress" >&2; exit 1; fi
    if [ -e "$S/rmfail-$4" ]; then echo "removal of $4 is already in progress" >&2; exit 1; fi
    exit 0 ;;
  version)
    if [ -e "$S/versionhang" ]; then exec sleep 30; fi
    if [ -e "$S/versionfail" ]; then echo "Cannot connect to the Docker daemon" >&2; exit 1; fi
    case "$3" in
      *Components*) if [ -e "$S/podmanengine" ]; then echo '[{"Name":"Podman Engine","Version":"5.8.8"}]'; else echo '[{"Name":"Engine","Version":"27.0.1"}]'; fi ;;
      *) echo 27.0.1 ;;
    esac ;;
  info)
    if [ -e "$S/infohang" ]; then exec sleep 30; fi
    if [ -e "$S/infofail" ]; then echo "Error: cannot read host info" >&2; exit 125; fi
    case "$3" in
      *Host.Security.Rootless*) if [ -e "$S/rootless" ]; then cat "$S/rootless"; else echo false; fi ;;
      *) echo "[name=seccomp,profile=builtin]" ;;
    esac ;;
  run)
    case "$*" in
      *ork.task=sigprobe*)
        if [ -e "$S/sigrunslow" ]; then exec sleep 30; fi
        if [ -e "$S/sigruntimeout" ]; then
          echo "docker: Error response from daemon: failed to create task for container: failed to create shim task: context deadline exceeded: unknown." >&2; exit 125
        fi
        if [ -e "$S/sigrunother" ]; then echo "docker: Error response from daemon: no space left on device" >&2; exit 125; fi
        out=""
        for a in "$@"; do
          case "$a" in type=bind,source=*,destination=/orkstack/output) out=${a#type=bind,source=}; out=${out%%,destination=*} ;; esac
        done
        printf '%s' "$out" > "$S/sigout"
        if [ ! -e "$S/neverready" ]; then
          { echo '@@label'; if [ -e "$S/label" ]; then cat "$S/label"; else echo unconfined; fi
            echo '@@signals'; if [ -e "$S/signals" ]; then cat "$S/signals"; else printf 'SigBlk:\t0000000000000000\nSigIgn:\t0000000000000000\nSigCgt:\t0000000000004000\n'; fi
          } > "$out/probe-ready"
        fi
        echo 0123456789ab; exit 0 ;;
    esac
    n=0; if [ -e "$S/runs" ]; then n=$(cat "$S/runs"); fi
    n=$((n+1)); echo "$n" > "$S/runs"
    touch "$S/runstarted"
    if [ -e "$S/runfail" ]; then echo "docker: Error response from daemon: bind source path does not exist" >&2; exit 125; fi
    if [ -e "$S/runother" ]; then echo "docker: Error response from daemon: no space left on device" >&2; exit 125; fi
    if [ -e "$S/runexit" ]; then exit 3; fi
    if [ -e "$S/runfailsrc" ]; then
      src=""
      for a in "$@"; do case "$a" in type=bind,source=*) src=${a#type=bind,source=}; src=${src%%,destination=*} ;; esac; done
      echo "docker: Error response from daemon: error while creating mount source path '$src': permission denied" >&2; exit 125
    fi
    if [ -e "$S/timeoutruns" ] && [ "$n" -le "$(cat "$S/timeoutruns")" ]; then
      echo "docker: Error response from daemon: failed to create task for container: failed to create shim task: context deadline exceeded: unknown." >&2; exit 125
    fi
    if [ -e "$S/slowruns" ] && [ "$n" -le "$(cat "$S/slowruns")" ]; then exec sleep 30; fi
    in=""; out=""; last=""
    for a in "$@"; do
      case "$a" in
        type=bind,source=*,destination=/orkstack/input,readonly) in=${a#type=bind,source=}; in=${in%%,destination=*} ;;
        type=bind,source=*,destination=/orkstack/output) out=${a#type=bind,source=}; out=${out%%,destination=*} ;;
      esac
      last=$a
    done
    echo '@@nonce'
    if [ -n "$in" ] && [ ! -e "$S/wrongdir" ]; then cat "$in/probe-nonce"; fi
    echo
    if [ -n "$out" ] && [ ! -e "$S/nowrite" ]; then
      printf '%s' "$last" > "$out/probe-nonce"
      if [ -e "$S/asorkerwrong" ]; then printf 'other' > "$out/probe-as-orker"; else printf '%s' "$last" > "$out/probe-as-orker"; fi
    fi
    if [ -e "$S/facts" ]; then cat "$S/facts"; fi
    exit 0 ;;
  *) exit 1 ;;
esac
`
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	fake := &countingDocker{t: t, dir: dir, bin: bin}
	fake.set("facts", GoodBootProbeFacts(SecurityPolicyFromEnv()))
	return fake
}

func (c *countingDocker) set(flag, content string) {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.dir, flag), []byte(content), 0o644); err != nil {
		c.t.Fatal(err)
	}
}

func (c *countingDocker) clear(flag string) {
	c.t.Helper()
	if err := os.Remove(filepath.Join(c.dir, flag)); err != nil && !errors.Is(err, os.ErrNotExist) {
		c.t.Fatal(err)
	}
}

// calls returns every invocation so far, one argv per line, and forgets them.
func (c *countingDocker) calls() []string {
	c.t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.dir, "calls"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		c.t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(c.dir, "calls"))
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func countPrefix(calls []string, prefix string) int {
	n := 0
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			n++
		}
	}
	return n
}

const countingPinned = "registry.example/tool@sha256:" + "0000000000000000000000000000000000000000000000000000000000000000"

// F-orker-storage-perf-5 (a): a cached image costs ONE inspect, and the
// platform facts come from it.
func TestDockerRuntime_PullOfACachedImageInspectsOnce(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	info, err := runtime.Pull(context.Background(), countingPinned)
	if err != nil {
		t.Fatal(err)
	}
	calls := fake.calls()
	if got := countPrefix(calls, "image inspect"); got != 1 {
		t.Fatalf("image inspects = %d; want 1: %q", got, calls)
	}
	if countPrefix(calls, "pull") != 0 {
		t.Fatalf("a cached image was pulled: %q", calls)
	}
	if !info.Cached || info.PlatformDigest != "sha256:abc" || info.Platform != "linux/amd64" {
		t.Fatalf("info = %+v", info)
	}
}

// F-orker-storage-perf-5 (a): an absent image is pulled and then inspected
// once more, for the facts of what was actually pulled.
func TestDockerRuntime_PullOfAnAbsentImageReinspectsAfterThePull(t *testing.T) {
	fake := newCountingDocker(t)
	runtime := &DockerRuntime{Bin: fake.bin}
	info, err := runtime.Pull(context.Background(), countingPinned)
	if err != nil {
		t.Fatal(err)
	}
	calls := fake.calls()
	if countPrefix(calls, "image inspect") != 2 || countPrefix(calls, "pull") != 1 {
		t.Fatalf("calls = %q; want inspect, pull, inspect", calls)
	}
	if info.Cached || info.PlatformDigest != "sha256:abc" {
		t.Fatalf("info = %+v", info)
	}
}

// F-orker-storage-perf-5 (b): the OOM inspect runs only for a non-zero exit,
// the only status that reads it, and is still reported there.
func TestDockerRuntime_WaitInspectsOOMOnlyForANonZeroExit(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("oom", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	status, err := runtime.Wait(context.Background(), "c-ok")
	if err != nil || status.Code != 0 {
		t.Fatalf("Wait = %+v, %v", status, err)
	}
	if calls := fake.calls(); countPrefix(calls, "inspect --format {{.State.OOMKilled}}") != 0 {
		t.Fatalf("a zero exit inspected OOMKilled: %q", calls)
	}
	fake.set("exitcode", "137\n")
	status, err = runtime.Wait(context.Background(), "c-oom")
	if err != nil || status.Code != 137 || !status.OOMKilled {
		t.Fatalf("Wait = %+v, %v; want 137 OOMKilled", status, err)
	}
}

// F-orker-storage-perf-5 (c): a container that stays created for a while is
// observed with a backoff, not a fixed 20 ms poll.
func TestDockerRuntime_StartBacksOffWhileTheContainerIsCreated(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("created", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	go func() {
		time.Sleep(700 * time.Millisecond)
		fake.clear("created")
	}()
	var stdout, stderr bytes.Buffer
	if err := runtime.Start(context.Background(), "c-slow", &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	polls := countPrefix(fake.calls(), "inspect --format {{.State.Status}}")
	// A fixed 20 ms poll is ~35 observations in 700 ms; 20 ms doubling to
	// 200 ms is under ten.
	if polls > 10 {
		t.Fatalf("observed the created state %d times in 700 ms; want a backoff (≤ 10)", polls)
	}
}

// F-orker-storage-perf-10: the runtime's identity is read once per process;
// a failure is not remembered.
func TestDockerRuntime_DescribeCachesOnlyASuccess(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("versionfail", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	if _, err := runtime.Describe(context.Background()); err == nil {
		t.Fatal("Describe against an unreachable daemon succeeded")
	}
	fake.clear("versionfail")
	fake.calls()
	for i := 0; i < 2; i++ {
		info, err := runtime.Describe(context.Background())
		if err != nil || info.Version != "27.0.1" || info.Name != "docker" {
			t.Fatalf("Describe #%d = %+v, %v", i, info, err)
		}
	}
	if calls := fake.calls(); len(calls) != 3 {
		t.Fatalf("two Describes after a success spawned %d processes: %q; want 3 (the engine, version, info) in total", len(calls), calls)
	}
}

// F-orker-storage-perf-9 (a): one orphan that cannot be removed does not stop
// the others from being removed, and is reported as a partial reap.
func TestDockerRuntime_ReapRemovesEveryContainerItCan(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("rmfail-c2", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	removed, err := runtime.Reap(context.Background(), "orker-x")
	if removed != 2 {
		t.Fatalf("removed = %d; want 2", removed)
	}
	var partial *ReapIncompleteError
	if !errors.As(err, &partial) || len(partial.Failures) != 1 || !strings.Contains(partial.Failures[0].Error(), "c2") {
		t.Fatalf("err = %v; want a ReapIncompleteError naming c2", err)
	}
	if got := countPrefix(fake.calls(), "rm --force"); got != 3 {
		t.Fatalf("rm calls = %d; want 3", got)
	}
}

// F-orker-storage-perf-9 (a), boot half: a partial reap is logged and boot
// proceeds; a reap that could not even list fails boot.
func TestBoot_APartialReapIsLoggedNotFatal(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("rmfail-c2", "")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	newBooting := func(id string) *Activities {
		a := NewActivitiesWithOptions(nil, Options{
			OrkerID: id, WorkRoot: t.TempDir(), Runtime: &DockerRuntime{Bin: fake.bin}, Logger: logger,
		})
		t.Cleanup(a.Close)
		return a
	}
	if err := newBooting("orker-partial-reap-" + strings.ReplaceAll(t.Name(), "/", "-")).Boot(context.Background()); err != nil {
		t.Fatalf("Boot with one unremovable orphan: %v", err)
	}
	if !strings.Contains(logs.String(), "c2") || !strings.Contains(logs.String(), "orphan") {
		t.Fatalf("the unremovable orphan was not logged:\n%s", logs.String())
	}
	fake.set("psfail", "")
	err := newBooting("orker-list-fails-" + strings.ReplaceAll(t.Name(), "/", "-")).Boot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "daemon unreachable") {
		t.Fatalf("Boot with an unlistable daemon = %v; want a refusal", err)
	}
}

// F-orker-storage-perf-9 (b): the probe image is pulled under the pull
// timeout when absent, and a pull failure is its own error rather than a
// claim that the work root is unbindable.
func TestDockerRuntime_RunBootProbeSeparatesTheImageFromTheBind(t *testing.T) {
	fake := newCountingDocker(t)
	runtime := &DockerRuntime{Bin: fake.bin}
	spec := probeSpecUnder(t, t.TempDir())

	fake.set("pullfail", "")
	_, err := runtime.RunBootProbe(context.Background(), spec)
	if !errors.Is(err, ErrProbeImageUnavailable) || errors.Is(err, ErrNotBindable) {
		t.Fatalf("pull failure = %v; want ErrProbeImageUnavailable and not ErrNotBindable", err)
	}
	if !strings.Contains(err.Error(), BindProbeImage) {
		t.Fatalf("the refusal %q does not name the probe image", err)
	}

	fake.clear("pullfail")
	fake.calls()
	if _, err := runtime.RunBootProbe(context.Background(), spec); err != nil {
		t.Fatalf("probe after a successful pull: %v", err)
	}
	calls := fake.calls()
	if countPrefix(calls, "pull") != 1 || countPrefix(calls, "run") != 1 {
		t.Fatalf("calls = %q; want one pull then one run", calls)
	}

	fake.set("runfail", "")
	_, err = runtime.RunBootProbe(context.Background(), spec)
	if !errors.Is(err, ErrNotBindable) || errors.Is(err, ErrProbeImageUnavailable) {
		t.Fatalf("bind failure = %v; want ErrNotBindable", err)
	}
	if calls := fake.calls(); countPrefix(calls, "pull") != 0 {
		t.Fatalf("a present probe image was pulled again: %q", calls)
	}
}

// TestDockerRuntime_ParsesStdoutAndQuotesStderr is G21: a CLI that writes a
// line of its own on stderr for every command — the podman-docker shim's
// "Emulate Docker CLI using podman" is the common one — must not change what
// the runtime reads. Every command's answer is read from stdout only, and a
// failure quotes what the CLI said on stderr.
func TestDockerRuntime_ParsesStdoutAndQuotesStderr(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("noise", "")
	fake.set("present", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	ctx := context.Background()

	info, err := runtime.Pull(ctx, countingPinned)
	if err != nil || info.PlatformDigest != "sha256:abc" || info.Platform != "linux/amd64" {
		t.Errorf("Pull = %+v, %v; want the image's facts read from stdout alone", info, err)
	}
	id, err := runtime.Create(ctx, validSpec())
	if err != nil || id != "cid-0123" {
		t.Errorf("Create = %q, %v; want the container id read from stdout alone", id, err)
	}
	removed, err := runtime.Reap(ctx, "orker-x")
	if err != nil || removed != 3 {
		t.Errorf("Reap = %d, %v; want the three listed containers and nothing else", removed, err)
	}
	described, err := runtime.Describe(ctx)
	if err != nil || described.Version != "27.0.1" {
		t.Errorf("Describe = %+v, %v; want the version read from stdout alone", described, err)
	}

	fake.clear("present")
	fake.set("pullfail", "")
	_, err = runtime.Pull(ctx, countingPinned)
	if err == nil || !strings.Contains(err.Error(), "pull access denied") {
		t.Errorf("a refused pull = %v; want it to quote what the CLI said on stderr", err)
	}
}

// TestDockerRuntime_RemoveKillsBeforeItRemoves is G23: `rm --force` on a
// running container waits out the container's stop timeout on some runtimes
// (30 s on Podman, which is the whole control timeout), so Remove kills the
// container first. A kill the runtime answers with "not running", or with
// anything else, does not stop the removal: only the removal's own answer
// decides Remove's.
func TestDockerRuntime_RemoveKillsBeforeItRemoves(t *testing.T) {
	fake := newCountingDocker(t)
	runtime := &DockerRuntime{Bin: fake.bin}

	if err := runtime.Remove(context.Background(), "c-running"); err != nil {
		t.Fatalf("Remove = %v", err)
	}
	calls := fake.calls()
	want := []string{"kill --signal KILL c-running", "rm --force --volumes c-running"}
	if strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls = %q; want %q", calls, want)
	}

	fake.set("killnotrunning", "")
	if err := runtime.Remove(context.Background(), "c-exited"); err != nil {
		t.Fatalf("Remove of a container that is not running = %v; a kill's refusal must not stop the removal", err)
	}
	if got := countPrefix(fake.calls(), "rm --force --volumes c-exited"); got != 1 {
		t.Fatalf("the container was removed %d times; want once", got)
	}
}
