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
// inspecthang (`image inspect` hangs for 30 s) and rmfailall (every `rm` is
// refused). `run` also counts itself into the runs file and, once counted,
// touches runstarted.
type countingDocker struct {
	t   *testing.T
	dir string
	bin string
}

func newCountingDocker(t *testing.T) *countingDocker {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
S=` + shellQuote(dir) + `
echo "$*" >> "$S/calls"
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
      *) if [ -e "$S/created" ]; then echo created; else echo running; fi ;;
    esac ;;
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
    if [ -e "$S/versionfail" ]; then echo "Cannot connect to the Docker daemon" >&2; exit 1; fi
    echo 27.0.1 ;;
  info) echo "[name=seccomp,profile=builtin]" ;;
  run)
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
    exit 0 ;;
  *) exit 1 ;;
esac
`
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return &countingDocker{t: t, dir: dir, bin: bin}
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
	if calls := fake.calls(); len(calls) != 2 {
		t.Fatalf("two Describes after a success spawned %d processes: %q; want 2 (version, info) in total", len(calls), calls)
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
func TestDockerRuntime_ProbeBindableSeparatesTheImageFromTheBind(t *testing.T) {
	fake := newCountingDocker(t)
	runtime := &DockerRuntime{Bin: fake.bin}
	root := t.TempDir()

	fake.set("pullfail", "")
	err := runtime.ProbeBindable(context.Background(), root)
	if !errors.Is(err, ErrProbeImageUnavailable) || errors.Is(err, ErrNotBindable) {
		t.Fatalf("pull failure = %v; want ErrProbeImageUnavailable and not ErrNotBindable", err)
	}
	if !strings.Contains(err.Error(), bindProbeImage) {
		t.Fatalf("the refusal %q does not name the probe image", err)
	}

	fake.clear("pullfail")
	fake.calls()
	if err := runtime.ProbeBindable(context.Background(), root); err != nil {
		t.Fatalf("probe after a successful pull: %v", err)
	}
	calls := fake.calls()
	if countPrefix(calls, "pull") != 1 || countPrefix(calls, "run") != 1 {
		t.Fatalf("calls = %q; want one pull then one run", calls)
	}

	fake.set("runfail", "")
	err = runtime.ProbeBindable(context.Background(), root)
	if !errors.Is(err, ErrNotBindable) || errors.Is(err, ErrProbeImageUnavailable) {
		t.Fatalf("bind failure = %v; want ErrNotBindable", err)
	}
	if calls := fake.calls(); countPrefix(calls, "pull") != 0 {
		t.Fatalf("a present probe image was pulled again: %q", calls)
	}
}
