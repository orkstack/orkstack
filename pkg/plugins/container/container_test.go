package containerplugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// These tests drive a stand-in Docker CLI so behavior is observable without a
// daemon: every invocation is recorded argv-by-argv, and the stand-in's exit
// code, streams and delay are set per test. The real daemon is exercised by
// container_integration_test.go.

const fakeDockerScript = `#!/bin/sh
set -u
{
  for arg in "$@"; do printf '%s\n' "$arg"; done
  printf '%s\n' '--END--'
} >> "$FAKE_DOCKER_LOG"

case "${1:-}" in
  image)
    exit "${FAKE_DOCKER_INSPECT_EXIT:-0}"
    ;;
  pull)
    printf 'pulled %s\n' "${2:-}"
    exit "${FAKE_DOCKER_PULL_EXIT:-0}"
    ;;
  kill|rm)
    exit 0
    ;;
  run)
    if [ -n "${FAKE_DOCKER_STAT:-}" ]; then
      if [ -d "$FAKE_DOCKER_STAT" ]; then printf 'scratch:present\n'; else printf 'scratch:absent\n'; fi
    fi
    if [ -n "${FAKE_DOCKER_STDOUT:-}" ]; then printf '%s' "$FAKE_DOCKER_STDOUT"; fi
    if [ -n "${FAKE_DOCKER_STDERR:-}" ]; then printf '%s' "$FAKE_DOCKER_STDERR" >&2; fi
    if [ -n "${FAKE_DOCKER_SLEEP:-}" ]; then exec sleep "$FAKE_DOCKER_SLEEP"; fi
    exit "${FAKE_DOCKER_RUN_EXIT:-0}"
    ;;
esac
exit 0
`

type fakeDocker struct {
	t   *testing.T
	bin string
	log string
}

func newFakeDocker(t *testing.T) *fakeDocker {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatalf("write fake docker: %v", err)
	}
	log := filepath.Join(dir, "invocations.txt")
	t.Setenv("FAKE_DOCKER_LOG", log)
	return &fakeDocker{t: t, bin: bin, log: log}
}

// invocations returns every recorded command, one argv slice per invocation.
func (f *fakeDocker) invocations() [][]string {
	f.t.Helper()
	raw, err := os.ReadFile(f.log)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatalf("read fake docker log: %v", err)
	}
	var all [][]string
	var current []string
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if line == "--END--" {
			all = append(all, current)
			current = nil
			continue
		}
		current = append(current, line)
	}
	return all
}

// only returns the single invocation of the named docker subcommand.
func (f *fakeDocker) only(sub string) []string {
	f.t.Helper()
	var found [][]string
	for _, argv := range f.invocations() {
		if len(argv) > 0 && argv[0] == sub {
			found = append(found, argv)
		}
	}
	if len(found) != 1 {
		f.t.Fatalf("want exactly one `docker %s`, got %d in %v", sub, len(found), f.invocations())
	}
	return found[0]
}

func (f *fakeDocker) count(sub string) int {
	f.t.Helper()
	n := 0
	for _, argv := range f.invocations() {
		if len(argv) > 0 && argv[0] == sub {
			n++
		}
	}
	return n
}

// stubResolver stands in for the Orker's upstream artifact resolver.
type stubResolver struct {
	paths map[string]string
	err   error
	asked []string
}

func (s *stubResolver) Resolve(_ context.Context, ref string) (string, error) {
	s.asked = append(s.asked, ref)
	if s.err != nil {
		return "", s.err
	}
	path, ok := s.paths[ref]
	if !ok {
		return "", fmt.Errorf("no artifact %q", ref)
	}
	return path, nil
}

type execOption func(*plugin.ExecContext)

func withResolver(r plugin.ArtifactResolver) execOption {
	return func(ec *plugin.ExecContext) { ec.Artifacts = r }
}

// logCapture records what a plugin streamed.
//
// It locks because a container's stdout and stderr reach plugin.Logger from
// the two goroutines exec.Cmd runs to drain the pipes (M11.1), so concurrent
// lines are the normal case rather than an edge one.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) add(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
}

func (c *logCapture) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *logCapture) text() string { return strings.Join(c.all(), "\n") }

// count reports how many streamed lines are EXACTLY want. Exactness is what
// makes a drop and a duplicate both visible.
func (c *logCapture) count(want string) int {
	n := 0
	for _, line := range c.all() {
		if line == want {
			n++
		}
	}
	return n
}

func (c *logCapture) holds(want string) bool {
	for _, line := range c.all() {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

func newExecContext(t *testing.T, config map[string]any, opts ...execOption) (*plugin.ExecContext, *logCapture) {
	t.Helper()
	logs := &logCapture{}
	ec := &plugin.ExecContext{
		TaskID:  "build",
		Config:  config,
		WorkDir: t.TempDir(),
		Log: func(format string, args ...any) {
			logs.add(fmt.Sprintf(format, args...))
		},
	}
	for _, opt := range opts {
		opt(ec)
	}
	return ec, logs
}

func argvAfter(t *testing.T, argv []string, flag string) []string {
	t.Helper()
	var values []string
	for i := 0; i < len(argv)-1; i++ {
		if argv[i] == flag {
			values = append(values, argv[i+1])
		}
	}
	return values
}

// containerArgs returns everything after the image reference: the command the
// container is asked to run.
func containerArgs(t *testing.T, argv []string, image string) []string {
	t.Helper()
	for i, arg := range argv {
		if arg == image {
			return argv[i+1:]
		}
	}
	t.Fatalf("argv %v does not name image %q", argv, image)
	return nil
}

func assertPermanent(t *testing.T, err error) {
	t.Helper()
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Fatalf("error %v is not marked permanent", err)
	}
}

func assertRetryable(t *testing.T, err error) {
	t.Helper()
	var retryable *plugin.RetryableError
	if errors.As(err, &retryable) && !retryable.Retryable {
		t.Fatalf("error %v is marked permanent, want retryable", err)
	}
}

func TestContainerPlugin_DescribesTheGenericOpSelectorAndRejectsAnUnknownOp(t *testing.T) {
	p := New()

	properties, _ := p.Describe().InputSchema["properties"].(map[string]any)
	opSchema, _ := properties["op"].(map[string]any)
	if opSchema == nil {
		t.Fatalf("container does not document an `op` field in its input schema")
	}
	enum, _ := opSchema["enum"].([]any)
	if len(enum) != 2 || fmt.Sprintf("%v", enum[0]) != "run" || fmt.Sprintf("%v", enum[1]) != "pull" {
		t.Errorf("advertised ops = %v, want [run pull]", enum)
	}

	ec, _ := newExecContext(t, map[string]any{"op": "frobnicate", "image": "alpine:3.20"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil {
		t.Fatalf("container accepted op=frobnicate, want a permanent error")
	}
	if !strings.Contains(err.Error(), `unknown op "frobnicate"`) {
		t.Errorf("error %q does not report the unknown op", err)
	}
	assertPermanent(t, err)
}

func TestContainerPlugin_RequiresAnImage(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{"command": "true"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "`image` is required") {
		t.Fatalf("error = %v, want `image` is required", err)
	}
	assertPermanent(t, err)
	if got := len(docker.invocations()); got != 0 {
		t.Errorf("docker was invoked %d time(s) for a config that cannot run", got)
	}
}

// The run argv is the whole contract with Docker, so it is asserted directly:
// a reader of a task can predict the command, and a value carrying spaces
// stays one argument.
func TestContainerPlugin_RendersTheRunCommandAsArgvWithEnvMountsAndWorkdir(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	artifactPath := filepath.Join(t.TempDir(), "body.json")
	if err := os.WriteFile(artifactPath, []byte(`{"ok":true}`), 0o644); err != nil {
		t.Fatalf("write upstream artifact: %v", err)
	}
	resolver := &stubResolver{paths: map[string]string{"fetch/body": artifactPath}}

	ec, _ := newExecContext(t, map[string]any{
		"image":   "alpine:3.20",
		"workdir": "/work",
		"command": "echo hello world",
		"env": map[string]any{
			"ZULU":    "last",
			"ALPHA":   "first",
			"MESSAGE": "two words",
		},
		"mounts": []any{
			map[string]any{"artifact": "fetch/body", "path": "/input/body.json"},
			map[string]any{"ephemeral": "scratch", "path": "/scratch"},
		},
	}, withResolver(resolver))

	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	argv := docker.only("run")
	if argv[1] != "--rm" {
		t.Errorf("argv[1] = %q, want --rm so a finished container is not left behind", argv[1])
	}
	names := argvAfter(t, argv, "--name")
	if len(names) != 1 || !strings.HasPrefix(names[0], "ork-build-") {
		t.Errorf("--name = %v, want one name derived from the task id", names)
	}
	if labels := argvAfter(t, argv, "--label"); len(labels) != 1 || labels[0] != "ork.task=build" {
		t.Errorf("--label = %v, want [ork.task=build]", labels)
	}
	if dirs := argvAfter(t, argv, "--workdir"); len(dirs) != 1 || dirs[0] != "/work" {
		t.Errorf("--workdir = %v, want [/work]", dirs)
	}

	wantEnv := []string{"ALPHA=first", "MESSAGE=two words", "ZULU=last"}
	if got := argvAfter(t, argv, "--env"); strings.Join(got, "|") != strings.Join(wantEnv, "|") {
		t.Errorf("--env = %v, want %v (sorted, one argv element each)", got, wantEnv)
	}

	scratch := filepath.Join(ec.WorkDir, internalDir, ephemeralDir, "scratch")
	// `--mount type=bind`, never `--volume`: an unshared host path must make
	// the run fail loudly instead of quietly mounting an empty directory.
	wantMounts := []string{
		"type=bind,source=" + artifactPath + ",target=/input/body.json,readonly",
		"type=bind,source=" + scratch + ",target=/scratch",
	}
	if got := argvAfter(t, argv, "--mount"); strings.Join(got, "|") != strings.Join(wantMounts, "|") {
		t.Errorf("--mount = %v, want %v", got, wantMounts)
	}
	if got := argvAfter(t, argv, "--volume"); len(got) != 0 {
		t.Errorf("--volume = %v, want none: --volume hides an unshared host path", got)
	}

	if got := containerArgs(t, argv, "alpine:3.20"); strings.Join(got, "|") != "sh|-c|echo hello world" {
		t.Errorf("container command = %v, want the one-liner handed to the image's sh -c", got)
	}
	if len(resolver.asked) != 1 || resolver.asked[0] != "fetch/body" {
		t.Errorf("resolver asked for %v, want [fetch/body]", resolver.asked)
	}
}

func TestContainerPlugin_RunsACommandListAsArgvWithoutAShell(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{
		"image":   "alpine:3.20",
		"command": []any{"/bin/echo", "a b", "c;d"},
	})
	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := containerArgs(t, docker.only("run"), "alpine:3.20")
	if strings.Join(got, "|") != "/bin/echo|a b|c;d" {
		t.Errorf("container command = %v, want the list used verbatim as argv", got)
	}
}

func TestContainerPlugin_ExpandsEnvironmentVariablesBetweenEachOther(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{
		"image": "alpine:3.20",
		"env": map[string]any{
			// Rendered by the engine's template layer before the plugin sees
			// it; here the plugin resolves the remaining shell-style refs.
			"RELEASE":  "v1.4.2",
			"ARCHIVE":  "orkstack-${RELEASE}.tar.gz",
			"BANNER":   "build $RELEASE",
			"LITERAL":  "cost: $$5",
			"UNKNOWN":  "[${NOT_DECLARED}]",
			"NUMERIC":  "7",
			"DERIVED":  "$NUMERIC-$NUMERIC",
			"NOEXPAND": "plain",
		},
	})
	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := map[string]string{}
	for _, pair := range argvAfter(t, docker.only("run"), "--env") {
		key, value, _ := strings.Cut(pair, "=")
		got[key] = value
	}
	want := map[string]string{
		"RELEASE":  "v1.4.2",
		"ARCHIVE":  "orkstack-v1.4.2.tar.gz",
		"BANNER":   "build v1.4.2",
		"LITERAL":  "cost: $5",
		"UNKNOWN":  "[]",
		"NUMERIC":  "7",
		"DERIVED":  "7-7",
		"NOEXPAND": "plain",
	}
	for key, wantValue := range want {
		if got[key] != wantValue {
			t.Errorf("env %s = %q, want %q", key, got[key], wantValue)
		}
	}
}

func TestContainerPlugin_CapturesStdoutAndStderrAsArtifacts(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	t.Setenv("FAKE_DOCKER_STDOUT", "line one\nline two\n")
	t.Setenv("FAKE_DOCKER_STDERR", "a warning\n")

	ec, _ := newExecContext(t, map[string]any{
		"image":             "alpine:3.20",
		"command":           "echo",
		"capture_stdout_as": "build-log",
		"capture_stderr_as": "build-warnings",
	})
	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Artifacts) != 2 {
		t.Fatalf("produced %d artifacts, want 2", len(result.Artifacts))
	}
	byName := map[string]plugin.ProducedArtifact{}
	for _, a := range result.Artifacts {
		byName[a.Name] = a
	}
	for name, want := range map[string]string{
		"build-log":      "line one\nline two\n",
		"build-warnings": "a warning\n",
	} {
		produced, ok := byName[name]
		if !ok {
			t.Fatalf("no artifact named %q (got %v)", name, result.Artifacts)
		}
		raw, err := os.ReadFile(produced.LocalPath)
		if err != nil {
			t.Fatalf("read artifact %q: %v", name, err)
		}
		if string(raw) != want {
			t.Errorf("artifact %q = %q, want the exact stream %q", name, raw, want)
		}
	}

	if result.Output["stdout"] != "line one\nline two\n" {
		t.Errorf("output.stdout = %q, want the stream head", result.Output["stdout"])
	}
	if result.Output["stderr"] != "a warning\n" {
		t.Errorf("output.stderr = %q, want the stream head", result.Output["stderr"])
	}
	if result.Output["exit_code"] != float64(0) {
		t.Errorf("output.exit_code = %v, want 0", result.Output["exit_code"])
	}
	if result.Output["timed_out"] != false {
		t.Errorf("output.timed_out = %v, want false", result.Output["timed_out"])
	}
}

func TestContainerPlugin_TruncatesTheOutputPayloadButNotTheArtifact(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	full := strings.Repeat("x", MaxCapturedOutput+512)
	t.Setenv("FAKE_DOCKER_STDOUT", full)

	ec, _ := newExecContext(t, map[string]any{
		"image":             "alpine:3.20",
		"capture_stdout_as": "log",
	})
	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := result.Output["stdout"].(string); len(got) != MaxCapturedOutput {
		t.Errorf("output.stdout is %d bytes, want it truncated to %d", len(got), MaxCapturedOutput)
	}
	raw, err := os.ReadFile(result.Artifacts[0].LocalPath)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if len(raw) != len(full) {
		t.Errorf("artifact is %d bytes, want the full %d-byte stream", len(raw), len(full))
	}
}

func TestContainerPlugin_CreatesEphemeralMountsBeforeTheRunAndRemovesThemAfter(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, logs := newExecContext(t, map[string]any{
		"image": "alpine:3.20",
		"mounts": []any{
			map[string]any{"ephemeral": "scratch", "path": "/scratch"},
		},
	})
	scratch := filepath.Join(ec.WorkDir, internalDir, ephemeralDir, "scratch")
	// The stand-in reports, from inside the run, whether the directory the
	// plugin promised actually exists.
	t.Setenv("FAKE_DOCKER_STAT", scratch)

	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stdout := result.Output["stdout"].(string); !strings.Contains(stdout, "scratch:present") {
		t.Errorf("the container did not see the ephemeral mount: stdout = %q", stdout)
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ephemeral mount %s still exists after the run (stat err = %v)", scratch, err)
	}
	if !strings.Contains(logs.text(), `removed ephemeral "scratch"`) {
		t.Errorf("the run did not report removing the ephemeral mount: %v", logs.all())
	}
}

func TestContainerPlugin_RemovesEphemeralMountsAfterAFailedRun(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	t.Setenv("FAKE_DOCKER_RUN_EXIT", "3")

	ec, _ := newExecContext(t, map[string]any{
		"image": "alpine:3.20",
		"mounts": []any{
			map[string]any{"ephemeral": "scratch", "path": "/scratch"},
		},
	})
	scratch := filepath.Join(ec.WorkDir, internalDir, ephemeralDir, "scratch")

	if _, err := p.Execute(context.Background(), ec); err == nil {
		t.Fatalf("Execute succeeded, want the failing exit code to fail the task")
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ephemeral mount %s survived a failed run (stat err = %v)", scratch, err)
	}
}

func TestContainerPlugin_AlwaysRemovesTheContainerItStarted(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{"image": "alpine:3.20"})
	if _, err := p.Execute(context.Background(), ec); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	name := argvAfter(t, docker.only("run"), "--name")[0]
	rm := docker.only("rm")
	if strings.Join(rm, " ") != fmt.Sprintf("rm --force --volumes %s", name) {
		t.Errorf("cleanup ran %v, want `rm --force --volumes %s`", rm, name)
	}
}

// A failing container fails the task with ITS exit code, and with none of its
// output.
//
// The stderr assertion is inverted from what it was before M11.1's repair
// round, and deliberately: the error crosses into Temporal history and onto
// the run's event stream (internal/engine/workflow.go copies it onto
// TaskRun.Error, TASK_FAILED, Run.Error and RUN_FAILED), and a task's output
// does not go there (AGENTS.md §1.4). What replaces the quote is a pointer to
// the log stream, which carries every byte of both streams, live and
// afterwards.
func TestContainerPlugin_FailsTheTaskWithTheContainerExitCodeAndNoneOfItsOutput(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	t.Setenv("FAKE_DOCKER_RUN_EXIT", "42")
	t.Setenv("FAKE_DOCKER_STDERR", "compilation failed\n")

	ec, logs := newExecContext(t, map[string]any{"image": "alpine:3.20", "command": "false"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil {
		t.Fatalf("Execute succeeded, want a failure carrying the exit code")
	}
	if !strings.Contains(err.Error(), "exited with code 42") {
		t.Errorf("error %q does not report the exit code", err)
	}
	if strings.Contains(err.Error(), "compilation failed") {
		t.Errorf("error %q carries the container's stderr; that error is written into "+
			"Temporal history and onto the event stream, where a task's output may not go", err)
	}
	if !strings.Contains(err.Error(), "orkctl run logs") {
		t.Errorf("error %q does not say where the container's output is", err)
	}
	// And it is there: the log stream carries what the error no longer quotes.
	if !logs.holds(plugin.StderrMarker + "compilation failed") {
		t.Errorf("the container's stderr is neither in the error nor in the log: %q", logs.all())
	}
	assertPermanent(t, err)
}

// Docker reserves 125/126/127 for its own failures, and only 125 (the daemon
// could not start the container at all) can be fixed by trying again.
func TestContainerPlugin_SeparatesDockerFailuresFromContainerFailures(t *testing.T) {
	for _, tc := range []struct {
		label     string
		exit      string
		permanent bool
		message   string
	}{
		{label: "daemon refused the run", exit: "125", permanent: false, message: "could not start"},
		{label: "entrypoint not executable", exit: "126", permanent: true, message: "cannot run the requested command"},
		{label: "entrypoint not found", exit: "127", permanent: true, message: "cannot run the requested command"},
		{label: "container failed", exit: "1", permanent: true, message: "exited with code 1"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			docker := newFakeDocker(t)
			p := &Plugin{DockerBin: docker.bin}
			t.Setenv("FAKE_DOCKER_RUN_EXIT", tc.exit)

			ec, _ := newExecContext(t, map[string]any{"image": "alpine:3.20"})
			_, err := p.Execute(context.Background(), ec)
			if err == nil {
				t.Fatalf("exit %s succeeded, want a failure", tc.exit)
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("error %q does not contain %q", err, tc.message)
			}
			if tc.permanent {
				assertPermanent(t, err)
			} else {
				assertRetryable(t, err)
			}
		})
	}
}

func TestContainerPlugin_KillsAContainerThatOutlivesItsTimeout(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin, KillGrace: 300 * time.Millisecond}
	t.Setenv("FAKE_DOCKER_SLEEP", "60")

	ec, logs := newExecContext(t, map[string]any{
		"image":           "alpine:3.20",
		"command":         "sleep 60",
		"timeout_seconds": 0.2,
	})

	started := time.Now()
	_, err := p.Execute(context.Background(), ec)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatalf("Execute succeeded, want the timeout to fail the task")
	}
	if elapsed > 30*time.Second {
		t.Fatalf("Execute took %s, want it bounded by the timeout", elapsed)
	}
	if !strings.Contains(err.Error(), "exceeded its timeout") {
		t.Errorf("error %q does not report the timeout", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("exit code %d", exitKilled)) {
		t.Errorf("error %q does not report the killed exit code %d", err, exitKilled)
	}
	assertPermanent(t, err)

	name := argvAfter(t, docker.only("run"), "--name")[0]
	kill := docker.only("kill")
	if strings.Join(kill, " ") != fmt.Sprintf("kill --signal KILL %s", name) {
		t.Errorf("kill invocation = %v, want the daemon asked to kill %s", kill, name)
	}
	if docker.count("rm") != 1 {
		t.Errorf("a killed container was removed %d time(s), want 1", docker.count("rm"))
	}
	if !strings.Contains(logs.text(), "exceeded its 200ms budget") {
		t.Errorf("the run did not log the expired budget: %v", logs.all())
	}
}

func TestContainerPlugin_AppliesThePullPolicy(t *testing.T) {
	for _, tc := range []struct {
		label       string
		policy      string
		imageLocal  bool
		wantPulls   int
		wantInspect int
		wantPulled  bool
		wantErr     string
	}{
		{label: "missing and local: no pull", policy: "", imageLocal: true, wantInspect: 1},
		{label: "missing and absent: pull", policy: "missing", wantInspect: 1, wantPulls: 1, wantPulled: true},
		{label: "always: pull without asking", policy: "always", imageLocal: true, wantPulls: 1, wantPulled: true},
		{label: "never and local: no pull", policy: "never", imageLocal: true, wantInspect: 1},
		{label: "never and absent: refuse", policy: "never", wantInspect: 1, wantErr: "`pull: never`"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			docker := newFakeDocker(t)
			p := &Plugin{DockerBin: docker.bin}
			if !tc.imageLocal {
				t.Setenv("FAKE_DOCKER_INSPECT_EXIT", "1")
			}

			config := map[string]any{"image": "alpine:3.20"}
			if tc.policy != "" {
				config["pull"] = tc.policy
			}
			ec, _ := newExecContext(t, config)
			result, err := p.Execute(context.Background(), ec)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
				}
				assertPermanent(t, err)
				if docker.count("run") != 0 {
					t.Errorf("the container ran despite a missing image")
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := docker.count("pull"); got != tc.wantPulls {
				t.Errorf("pulled %d time(s), want %d", got, tc.wantPulls)
			}
			if got := docker.count("image"); got != tc.wantInspect {
				t.Errorf("inspected the local image %d time(s), want %d", got, tc.wantInspect)
			}
			if result.Output["image_pulled"] != tc.wantPulled {
				t.Errorf("output.image_pulled = %v, want %v", result.Output["image_pulled"], tc.wantPulled)
			}
		})
	}
}

func TestContainerPlugin_RejectsAnUnknownPullPolicy(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{"image": "alpine:3.20", "pull": "sometimes"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), `unknown pull policy "sometimes"`) {
		t.Fatalf("error = %v, want an unknown pull policy", err)
	}
	assertPermanent(t, err)
}

func TestContainerPlugin_PullOpFetchesTheImageAndRunsNoContainer(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{"op": "pull", "image": "alpine:3.20"})
	result, err := p.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if pull := docker.only("pull"); strings.Join(pull, " ") != "pull alpine:3.20" {
		t.Errorf("pull invocation = %v", pull)
	}
	if docker.count("run") != 0 {
		t.Errorf("`op: pull` started a container")
	}
	if result.Output["image_pulled"] != true {
		t.Errorf("output.image_pulled = %v, want true", result.Output["image_pulled"])
	}
}

func TestContainerPlugin_ReportsAFailedPull(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	t.Setenv("FAKE_DOCKER_INSPECT_EXIT", "1")
	t.Setenv("FAKE_DOCKER_PULL_EXIT", "1")

	ec, _ := newExecContext(t, map[string]any{"image": "example.invalid/nope:1"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "pull example.invalid/nope:1 failed") {
		t.Fatalf("error = %v, want a reported pull failure", err)
	}
	// A registry that is briefly unreachable is exactly what retries are for.
	assertRetryable(t, err)
	if docker.count("run") != 0 {
		t.Errorf("the container ran without its image")
	}
}

func TestContainerPlugin_RejectsMountsThatCannotMeanOneThing(t *testing.T) {
	for _, tc := range []struct {
		label  string
		mount  map[string]any
		errsIn string
	}{
		{
			label:  "two sources",
			mount:  map[string]any{"artifact": "fetch/body", "ephemeral": "scratch", "path": "/x"},
			errsIn: "exactly one source",
		},
		{
			label:  "no source",
			mount:  map[string]any{"path": "/x"},
			errsIn: "needs a source",
		},
		{
			label:  "no path",
			mount:  map[string]any{"ephemeral": "scratch"},
			errsIn: "needs a container `path`",
		},
		{
			label:  "relative path",
			mount:  map[string]any{"ephemeral": "scratch", "path": "scratch"},
			errsIn: "must be absolute",
		},
		{
			label:  "container root",
			mount:  map[string]any{"ephemeral": "scratch", "path": "/"},
			errsIn: "may not be the container root",
		},
		{
			label:  "colon in path",
			mount:  map[string]any{"ephemeral": "scratch", "path": "/a:b"},
			errsIn: "may not contain ':' or ','",
		},
		{
			label:  "comma in path",
			mount:  map[string]any{"ephemeral": "scratch", "path": "/a,b"},
			errsIn: "may not contain ':' or ','",
		},
		{
			label:  "dot segments",
			mount:  map[string]any{"ephemeral": "scratch", "path": "/a/../b"},
			errsIn: "must be a clean path",
		},
		{
			label:  "writable artifact",
			mount:  map[string]any{"artifact": "fetch/body", "path": "/in", "read_only": false},
			errsIn: "upstream artifacts are immutable",
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			docker := newFakeDocker(t)
			p := &Plugin{DockerBin: docker.bin}

			ec, _ := newExecContext(t, map[string]any{
				"image":  "alpine:3.20",
				"mounts": []any{tc.mount},
			})
			_, err := p.Execute(context.Background(), ec)
			if err == nil || !strings.Contains(err.Error(), tc.errsIn) {
				t.Fatalf("error = %v, want one containing %q", err, tc.errsIn)
			}
			assertPermanent(t, err)
			if docker.count("run") != 0 {
				t.Errorf("a container ran despite an unusable mount")
			}
		})
	}
}

func TestContainerPlugin_RejectsTwoMountsOnTheSameContainerPath(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{
		"image": "alpine:3.20",
		"mounts": []any{
			map[string]any{"ephemeral": "a", "path": "/data"},
			map[string]any{"ephemeral": "b", "path": "/data"},
		},
	})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), `both mount "/data"`) {
		t.Fatalf("error = %v, want the collision reported", err)
	}
	assertPermanent(t, err)
}

func TestContainerPlugin_ReportsAnUnresolvableArtifactMount(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	resolver := &stubResolver{err: plugin.Permanent(errors.New(
		`artifact "fetch/body": task "fetch" is not an upstream of this task`))}

	ec, _ := newExecContext(t, map[string]any{
		"image":  "alpine:3.20",
		"mounts": []any{map[string]any{"artifact": "fetch/body", "path": "/in"}},
	}, withResolver(resolver))

	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "is not an upstream of this task") {
		t.Fatalf("error = %v, want the resolver's explanation", err)
	}
	if docker.count("run") != 0 {
		t.Errorf("a container ran without the input it asked for")
	}
}

func TestContainerPlugin_ReportsAMissingDockerCLIAsAHostProblem(t *testing.T) {
	p := &Plugin{DockerBin: "definitely-not-a-real-docker-cli"}

	ec, _ := newExecContext(t, map[string]any{"image": "alpine:3.20"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "docker CLI") {
		t.Fatalf("error = %v, want a missing docker CLI reported", err)
	}
	if !strings.Contains(err.Error(), DockerBinVar) {
		t.Errorf("error %q does not name the %s override", err, DockerBinVar)
	}
	assertPermanent(t, err)
}

func TestContainerPlugin_RejectsAnImageThatWouldBeReadAsADockerFlag(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{"image": "--privileged"})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "may not start with '-'") {
		t.Fatalf("error = %v, want the flag-shaped image rejected", err)
	}
	assertPermanent(t, err)
	if len(docker.invocations()) != 0 {
		t.Errorf("docker was invoked with a flag-shaped image: %v", docker.invocations())
	}
}

func TestContainerPlugin_RejectsAnEnvironmentNameDockerCannotCarry(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}

	ec, _ := newExecContext(t, map[string]any{
		"image": "alpine:3.20",
		"env":   map[string]any{"A=B": "c"},
	})
	_, err := p.Execute(context.Background(), ec)
	if err == nil || !strings.Contains(err.Error(), "may not contain '=' or whitespace") {
		t.Fatalf("error = %v, want the malformed env name rejected", err)
	}
	assertPermanent(t, err)
}

// A daemon in a VM (colima, Lima, Rancher, a remote host) sees only the paths
// it was told to share. `--mount` makes that refusal visible; the plugin turns
// the daemon's wording into the knob an operator can actually turn.
func TestContainerPlugin_ExplainsAHostPathTheDaemonCannotSee(t *testing.T) {
	docker := newFakeDocker(t)
	p := &Plugin{DockerBin: docker.bin}
	t.Setenv("FAKE_DOCKER_RUN_EXIT", fmt.Sprintf("%d", exitDockerFailed))
	t.Setenv("FAKE_DOCKER_STDERR",
		`docker: Error response from daemon: invalid mount config for type "bind": `+
			`bind source path does not exist: /var/folders/tmp/ork-build-1/.artifacts/fetch/body`)

	ec, _ := newExecContext(t, map[string]any{
		"image":  "alpine:3.20",
		"mounts": []any{map[string]any{"ephemeral": "scratch", "path": "/scratch"}},
	})
	_, err := p.Execute(context.Background(), ec)
	if err == nil {
		t.Fatalf("Execute succeeded, want the unshared bind source to fail the task")
	}
	if !strings.Contains(err.Error(), "cannot see a path this task mounts") {
		t.Errorf("error %q does not explain what went wrong", err)
	}
	if !strings.Contains(err.Error(), workDirHint) {
		t.Errorf("error %q does not name the %s knob", err, workDirHint)
	}
	// Sharing a directory is an operator action; no retry performs it.
	assertPermanent(t, err)
}
