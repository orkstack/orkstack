package orker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// validSpec is a container specification that satisfies the whole §9.2
// baseline, so a test can weaken exactly one thing and see it refused.
func validSpec() ContainerSpec {
	return ContainerSpec{
		Image:   "ghcr.io/acme/tool@sha256:" + strings.Repeat("a", 64),
		WorkDir: ContainerRoot,
		Mounts: []Mount{
			{Source: "/work/attempt/input", Destination: ContainerInput, ReadOnly: true},
			{Source: "/work/attempt/output", Destination: ContainerOutput},
		},
		TmpfsPath:           ContainerScratchTmp,
		TmpfsBytes:          1 << 20,
		Network:             NetworkNone,
		CPUs:                "2",
		Memory:              "4Gi",
		PidsLimit:           512,
		Labels:              map[string]string{"ork.run": "run-1", "ork.task": "build", "ork.orker": "orker-1"},
		ReadOnlyRootfs:      true,
		DropAllCapabilities: true,
		NoNewPrivileges:     true,
		SeccompProfile:      SeccompBuiltin,
		Init:                true,
	}
}

func argsString(t *testing.T, spec ContainerSpec) string {
	t.Helper()
	args, err := dockerCreateArgs(spec)
	if err != nil {
		t.Fatalf("dockerCreateArgs: %v", err)
	}
	return strings.Join(args, " ")
}

// TestDockerCreateArgs_AppliesTheWholeEnforcedProfile asserts every line of
// §9.2 is actually on the command line.
//
// It is a rendering test rather than a daemon test on purpose: this is where a
// flag would go missing, and a missing flag is invisible in a container that
// starts and runs fine.
func TestDockerCreateArgs_AppliesTheWholeEnforcedProfile(t *testing.T) {
	rendered := argsString(t, validSpec())

	required := []string{
		"--read-only",
		"--cap-drop=ALL",
		"--security-opt no-new-privileges",
		"--security-opt seccomp=" + SeccompBuiltin,
		"--init",
		"--workdir " + ContainerRoot,
		"--network " + NetworkNone,
		// Rendered in the spelling the runtime accepts, not the contract's:
		// docker refuses "4Gi" outright (invalid suffix), so the byte count
		// is what goes on the command line.
		"--cpus 2",
		"--memory " + strconv.FormatInt(4<<30, 10),
		"--pids-limit 512",
		"--label ork.orker=orker-1",
		"--label ork.run=run-1",
		"--label ork.task=build",
		"--tmpfs /tmp:rw,nosuid,nodev,size=1048576",
		"type=bind,source=/work/attempt/input,destination=" + ContainerInput + ",readonly",
		"type=bind,source=/work/attempt/output,destination=" + ContainerOutput,
	}
	for _, want := range required {
		if !strings.Contains(rendered, want) {
			t.Errorf("the enforced profile is missing %q:\n%s", want, rendered)
		}
	}
}

// TestDockerCreateArgs_CannotExpressAnythingForbidden is §9.1 held by
// construction.
//
// The point is not that these are rejected: it is that there is no field on
// ContainerSpec that could produce them, so a fully populated valid spec still
// renders none of them. A guarantee by construction is stronger than
// validation, and this test is what keeps it true as the spec grows.
func TestDockerCreateArgs_CannotExpressAnythingForbidden(t *testing.T) {
	spec := validSpec()
	spec.User = "1000:1000"
	spec.Command = []string{"/bin/sh", "-c", "echo hi"}
	spec.Env = map[string]string{"GREETING": "hello"}
	spec.StopTimeoutSeconds = 30
	rendered := argsString(t, spec)

	forbidden := []string{
		"--privileged",
		"--pid=host", "--ipc=host", "--uts=host", "--userns=host",
		"--network=host", "--net=host",
		"--cap-add",
		"--device",
		"seccomp=unconfined", "apparmor=unconfined", "label=disable",
		"/var/run/docker.sock",
		"--volumes-from",
		"--cgroup-parent", "--cgroupns=host",
		"--sysctl",
		"--restart",
	}
	for _, banned := range forbidden {
		if strings.Contains(rendered, banned) {
			t.Errorf("a valid spec rendered the forbidden %q:\n%s", banned, rendered)
		}
	}
}

// TestDockerCreateArgs_RefusesASpecThatWeakensTheBaseline covers the second
// line of defence: a spec assembled by hand, or by future code, that turns one
// of the mandatory flags off.
func TestDockerCreateArgs_RefusesASpecThatWeakensTheBaseline(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ContainerSpec)
		wantErr string
	}{
		{"writable root filesystem", func(s *ContainerSpec) { s.ReadOnlyRootfs = false }, "security profile"},
		{"capabilities retained", func(s *ContainerSpec) { s.DropAllCapabilities = false }, "security profile"},
		{"privilege escalation allowed", func(s *ContainerSpec) { s.NoNewPrivileges = false }, "security profile"},
		{"no init, so no zombie reaping or signal forwarding",
			func(s *ContainerSpec) { s.Init = false }, "security profile"},
		{"a network mode that is neither default nor none",
			func(s *ContainerSpec) { s.Network = "host" }, "network mode"},
		{"unlimited cpu", func(s *ContainerSpec) { s.CPUs = "" }, "always be bounded"},
		{"unlimited memory", func(s *ContainerSpec) { s.Memory = "" }, "always be bounded"},
		{"unlimited pids", func(s *ContainerSpec) { s.PidsLimit = 0 }, "always be bounded"},
		// docker reads a zero limit as NO limit, so a quantity that parses to
		// one is refused here instead of reaching the daemon as a flag value.
		{"zero cpu", func(s *ContainerSpec) { s.CPUs = "0" }, "always be bounded"},
		{"zero millicpu", func(s *ContainerSpec) { s.CPUs = "0m" }, "always be bounded"},
		{"negative cpu", func(s *ContainerSpec) { s.CPUs = "-1" }, "always be bounded"},
		{"NaN cpu", func(s *ContainerSpec) { s.CPUs = "NaN" }, "always be bounded"},
		{"zero memory", func(s *ContainerSpec) { s.Memory = "0" }, "always be bounded"},
		{"zero memory with a unit", func(s *ContainerSpec) { s.Memory = "0Gi" }, "always be bounded"},
		{"less than a byte of memory", func(s *ContainerSpec) { s.Memory = "0.5" }, "always be bounded"},
		{"negative memory", func(s *ContainerSpec) { s.Memory = "-1" }, "always be bounded"},
		{"NaN memory", func(s *ContainerSpec) { s.Memory = "NaN" }, "always be bounded"},
		{"no image", func(s *ContainerSpec) { s.Image = "" }, "image is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			tc.mutate(&spec)
			args, err := dockerCreateArgs(spec)
			if err == nil {
				t.Fatalf("dockerCreateArgs accepted a weakened spec: %v", args)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

// TestDockerCreateArgs_RefusesTheDockerSocketAsSourceAndAsTarget is §9.1's
// explicit two-sided rule.
//
// The TARGET half is the one an implementation forgets: target validation
// historically accepted any clean absolute path, so nothing stopped a mount
// landing at a container path that shadows a runtime socket.
func TestDockerCreateArgs_RefusesTheDockerSocketAsSourceAndAsTarget(t *testing.T) {
	tests := []struct {
		name  string
		mount Mount
	}{
		{"as a source", Mount{Source: "/var/run/docker.sock", Destination: "/orkstack/sock"}},
		{"as a target", Mount{Source: "/work/attempt/sock", Destination: "/var/run/docker.sock"}},
		{"as a target under /run", Mount{Source: "/work/attempt/sock", Destination: "/run/docker.sock"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			spec.Mounts = append(spec.Mounts, tc.mount)
			if _, err := dockerCreateArgs(spec); err == nil {
				t.Fatal("dockerCreateArgs accepted a docker socket mount")
			} else if !strings.Contains(err.Error(), "forbidden without exception") {
				t.Errorf("error %q does not say the mount is forbidden", err)
			}
		})
	}
}

// TestDockerCreateArgs_RefusesAMountSourceThatIsNotAnAbsolutePath keeps mount
// sources to Orker-created paths: a relative source would be resolved by the
// daemon against something this process does not control.
func TestDockerCreateArgs_RefusesAMountSourceThatIsNotAnAbsolutePath(t *testing.T) {
	spec := validSpec()
	spec.Mounts = append(spec.Mounts, Mount{Source: "relative/path", Destination: "/orkstack/extra"})
	if _, err := dockerCreateArgs(spec); err == nil {
		t.Fatal("dockerCreateArgs accepted a relative mount source")
	}
}

// TestDockerCreateArgs_EnvAndLabelsAreOrderedDeterministically matters because
// argv is compared in tests and read by humans; map iteration order is not.
func TestDockerCreateArgs_EnvAndLabelsAreOrderedDeterministically(t *testing.T) {
	spec := validSpec()
	spec.Env = map[string]string{"C": "3", "A": "1", "B": "2"}

	// The container NAME is deliberately unique per call — a stray container
	// has to be traceable to its attempt, and two attempts must not collide —
	// so it is excluded from the comparison and asserted separately.
	names := map[string]struct{}{}
	first := ""
	for range 20 {
		args, err := dockerCreateArgs(spec)
		if err != nil {
			t.Fatalf("dockerCreateArgs: %v", err)
		}
		for i, arg := range args {
			if arg == "--name" {
				names[args[i+1]] = struct{}{}
				args = append(append([]string{}, args[:i]...), args[i+2:]...)
				break
			}
		}
		rendered := strings.Join(args, " ")
		if first == "" {
			first = rendered
			continue
		}
		if rendered != first {
			t.Fatalf("argv is not deterministic:\n%s\n%s", first, rendered)
		}
	}
	if len(names) != 20 {
		t.Errorf("20 calls produced %d distinct container names; a name collision makes two "+
			"attempts indistinguishable to the reaper", len(names))
	}
	if a, b := strings.Index(first, "A=1"), strings.Index(first, "B=2"); a > b {
		t.Errorf("env is not sorted:\n%s", first)
	}
}

// TestIsUnbindable_RecognizesTheDaemonsOwnWords separates the host
// misconfiguration §3.1 predicts (not retryable) from an ordinary runtime
// refusal (retryable). Getting this wrong means retrying forever against a
// machine that will never bind the path.
func TestIsUnbindable_RecognizesTheDaemonsOwnWords(t *testing.T) {
	unbindable := []string{
		"docker: Error response from daemon: bind source path does not exist: /var/folders/x/y",
		"Mounts denied: The path /var/folders/x is not shared from the host",
	}
	for _, message := range unbindable {
		if !isUnbindable(message) {
			t.Errorf("isUnbindable(%q) = false; this is the WORKSPACE_NOT_BINDABLE case", message)
		}
	}
	ordinary := []string{
		"docker: Error response from daemon: no such image",
		"failed to create task for container: OCI runtime create failed",
	}
	for _, message := range ordinary {
		if isUnbindable(message) {
			t.Errorf("isUnbindable(%q) = true; this is an ordinary RUNTIME_REFUSED", message)
		}
	}
}

// TestDockerMemoryAndCPUs_TranslateTheContractSpelling covers the boundary
// where the Kubernetes spelling meets a runtime that does not speak it.
//
// The contract spells memory "4Gi" and cpu "500m" (§9.3). Docker rejects
// "4Gi" — `invalid suffix: 'gi'` — and its own b/k/m/g suffixes do not carry
// the same binary/decimal meanings, so translating suffix-to-suffix would
// quietly change the number. The byte count and the decimal are unambiguous.
func TestDockerMemoryAndCPUs_TranslateTheContractSpelling(t *testing.T) {
	memory := []struct{ quantity, want string }{
		{"4Gi", strconv.FormatInt(4<<30, 10)},
		{"512Mi", strconv.FormatInt(512<<20, 10)},
		{"1Gi", strconv.FormatInt(1<<30, 10)},
		{"1073741824", "1073741824"},
	}
	for _, tc := range memory {
		if got := dockerMemory(tc.quantity); got != tc.want {
			t.Errorf("dockerMemory(%q) = %q, want %q", tc.quantity, got, tc.want)
		}
	}
	cpus := []struct{ quantity, want string }{
		{"2", "2"},
		{"0.5", "0.5"},
		{"500m", "0.5"},
		{"1500m", "1.5"},
	}
	for _, tc := range cpus {
		if got := dockerCPUs(tc.quantity); got != tc.want {
			t.Errorf("dockerCPUs(%q) = %q, want %q", tc.quantity, got, tc.want)
		}
	}
	// An unparseable quantity is passed through so the daemon reports it,
	// rather than being replaced by a limit nobody asked for.
	if got := dockerMemory("lots"); got != "lots" {
		t.Errorf("dockerMemory(\"lots\") = %q; an unparseable quantity must reach the daemon", got)
	}
}

// fakeDockerBinary writes a stand-in `docker` CLI that lets a test control
// the timing of `start --attach` independently of `wait`, without a real
// daemon. `inspect` always reports the container as already running (so
// Start returns as soon as it is called, the same as a container that starts
// instantly), `start --attach` sleeps for attachDelay before printing output
// so its copy provably outlives a `wait` that returns immediately, and `wait`
// reports exit code 0 with no delay of its own — reproducing the two
// independent, unsynchronized docker invocations described in
// DockerRuntime.Wait's doc comment.
func fakeDockerBinary(t *testing.T, attachDelay time.Duration, output string) string {
	t.Helper()
	script := "#!/bin/sh\nset -eu\ncase \"$1\" in\n" +
		"  inspect) echo running ;;\n" +
		"  start) sleep " + strconv.FormatFloat(attachDelay.Seconds(), 'f', -1, 64) +
		"; printf '%s' " + shellQuote(output) + " ;;\n" +
		"  wait) echo 0 ;;\n" +
		"  *) exit 1 ;;\n" +
		"esac\n"
	path := filepath.Join(t.TempDir(), "docker")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker binary: %v", err)
	}
	return path
}

// shellQuote wraps s in single quotes for embedding in the fake docker
// script's own source, escaping any single quote s itself contains.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TestDockerRuntime_WaitDrainsTheAttachBeforeReporting is the regression test
// for a truncated durable stdout.log on a task that reports full success.
//
// `docker wait` and `docker start --attach`'s own stdout/stderr copy are two
// independent processes (Start's doc comment). For a container that writes a
// burst of output and exits almost immediately, `docker wait` can return
// before the attach has finished draining what it buffered — and Wait used
// to report the exit status the moment `docker wait` returned, with nothing
// joining the attach at all. The caller (OCIDispatcher.runContainer) then
// closes the durable log file, silently cutting off whatever the attach copy
// had not yet delivered. This test's fake `wait` always returns instantly
// while the fake attach sleeps first, so a Wait that does not join the
// attach returns fast; one that does takes at least attachDelay.
func TestDockerRuntime_WaitDrainsTheAttachBeforeReporting(t *testing.T) {
	const attachDelay = 300 * time.Millisecond
	const wantOutput = "the quick brown fox jumps over the lazy dog"
	bin := fakeDockerBinary(t, attachDelay, wantOutput)
	runtime := &DockerRuntime{Bin: bin}

	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	if err := runtime.Start(ctx, "fake-container", &stdout, &stderr); err != nil {
		t.Fatalf("Start: %v", err)
	}

	began := time.Now()
	status, err := runtime.Wait(ctx, "fake-container")
	elapsed := time.Since(began)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if status.Code != 0 {
		t.Errorf("exit code = %d, want 0", status.Code)
	}

	// A generous margin under attachDelay: the point is distinguishing "Wait
	// returned instantly, like the fake `wait` command does on its own" from
	// "Wait actually blocked on the attach", not measuring exact timing.
	if wantMinDelay := attachDelay * 2 / 3; elapsed < wantMinDelay {
		t.Errorf("Wait returned after %s, less than the %s the attach needed to finish draining "+
			"its output; Wait is not joining the attach and would let the caller close the durable "+
			"log before every byte lands in it", elapsed, wantMinDelay)
	}
	if stdout.String() != wantOutput {
		t.Errorf("stdout = %q, want %q (the durable log would be silently truncated)",
			stdout.String(), wantOutput)
	}
}

// TestDockerRuntime_StartCancelledWhileTheContainerRunsReportsItStarted is the
// regression test for a cancellation that skipped SIGTERM and the grace
// period. Start polls the daemon until the container leaves created; a
// cancellation that arrived between two observations returned as a start
// failure even when the workload was already running, and the dispatcher then
// force-removed the running container instead of signalling it (found by
// TestIntegration_CancellationSignalsThenKills once it cancelled as soon as
// the workload ran: the container died 751ms after cancellation, inside its 2s
// grace period). Here the fake daemon reports created until the test marks the
// container running, and the test cancels right after marking it: Start must
// report the container started, so the caller's cancellation path runs.
func TestDockerRuntime_StartCancelledWhileTheContainerRunsReportsItStarted(t *testing.T) {
	dir := t.TempDir()
	running := filepath.Join(dir, "running")
	observed := filepath.Join(dir, "observed-created")
	script := "#!/bin/sh\nset -eu\ncase \"$1\" in\n" +
		"  inspect) if [ -e " + shellQuote(running) + " ]; then echo running; else touch " + shellQuote(observed) + "; echo created; fi ;;\n" +
		"  start) sleep 2 ;;\n" +
		"  *) exit 1 ;;\n" +
		"esac\n"
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker binary: %v", err)
	}
	runtime := &DockerRuntime{Bin: bin}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		result <- runtime.Start(ctx, "fake-container", &stdout, &stderr)
	}()
	// Let Start observe "created" at least once, so the cancellation lands in
	// its polling loop rather than before it.
	awaitFile(t, observed, 30*time.Second)
	if err := os.WriteFile(running, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Start = %v for a container the daemon reports running; a cancellation "+
				"while observing it must leave it to the caller's SIGTERM-then-grace path, "+
				"not fail the start and force-remove it", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return after the cancellation")
	}
}

// TestDockerRuntime_StartCancelledBeforeTheContainerRunsIsACancelledStart:
// the other half — a container still in created when the cancellation lands
// was never started, and Start reports the cancellation.
func TestDockerRuntime_StartCancelledBeforeTheContainerRunsIsACancelledStart(t *testing.T) {
	dir := t.TempDir()
	observed := filepath.Join(dir, "observed-created")
	script := "#!/bin/sh\nset -eu\ncase \"$1\" in\n" +
		"  inspect) touch " + shellQuote(observed) + "; echo created ;;\n" +
		"  start) sleep 2 ;;\n" +
		"  *) exit 1 ;;\n" +
		"esac\n"
	bin := filepath.Join(dir, "docker")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker binary: %v", err)
	}
	runtime := &DockerRuntime{Bin: bin}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		result <- runtime.Start(ctx, "fake-container", &stdout, &stderr)
	}()
	awaitFile(t, observed, 30*time.Second)
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v, want the cancellation for a container that never left created", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return after the cancellation")
	}
}

// awaitFile waits, against a deadline, for path to exist.
func awaitFile(t *testing.T, path string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear within %s", path, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
