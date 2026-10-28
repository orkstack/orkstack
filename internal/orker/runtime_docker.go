// This file is the ONLY place in the repository that may name a Docker
// concept. AGENTS.md §5 requires the OCI runtime boundary to stay clean enough
// for a future Podman, containerd, Kubernetes Jobs, ECS or Nomad
// implementation, and the way that requirement is held is that every other
// file talks to ContainerRuntime and nothing else.
package orker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DockerBinVar names the environment variable that overrides the docker CLI to
// drive. An explicit path is used verbatim so a test can point the runtime at
// a stand-in.
const DockerBinVar = "ORK_DOCKER_BIN"

// PullTimeoutVar overrides how long an image pull may take.
const PullTimeoutVar = "ORK_DOCKER_PULL_TIMEOUT"

// BindProbeTimeoutVar overrides how long the boot bind probe may wait for the
// daemon to run its container, per attempt.
const BindProbeTimeoutVar = "ORK_DOCKER_BIND_PROBE_TIMEOUT"

const (
	// controlTimeout bounds the short bookkeeping commands — inspect, kill,
	// remove. They must NOT inherit the run's cancelled context or its
	// budget: cleaning up after a kill is exactly when the context is already
	// gone.
	controlTimeout = 30 * time.Second
	// DefaultPullTimeout bounds one image pull. Exported so
	// internal/config's Schema can bind ORK_DOCKER_PULL_TIMEOUT's
	// documented default to the same constant pullTimeout falls back to.
	DefaultPullTimeout = 5 * time.Minute
	// DefaultBindProbeTimeout bounds one attempt of the boot bind probe's
	// `docker run`. It is deliberately longer than controlTimeout: that is a
	// bound for bookkeeping, and a daemon under load has needed more than
	// 30 s to start even a container that exits at once (the shim connected
	// at 32 s). Exported for the same reason as DefaultPullTimeout.
	DefaultBindProbeTimeout = 2 * time.Minute
	// DefaultDockerBin is the CLI name resolved on PATH when neither Bin
	// nor ORK_DOCKER_BIN names one. Exported so internal/config's Schema
	// can bind ORK_DOCKER_BIN's documented default to the same constant
	// bin() falls back to.
	DefaultDockerBin = "docker"
)

// DockerRuntime drives the Docker CLI. It is the one ContainerRuntime that
// ships.
//
// It shells out rather than linking the daemon SDK: the CLI is the stable
// interface, it is what an operator debugging a container will type, and it
// keeps the dependency surface at zero.
type DockerRuntime struct {
	// Bin is the CLI to drive. Empty reads ORK_DOCKER_BIN, then DefaultDockerBin.
	Bin string
	// PullTimeout bounds one pull. Zero reads ORK_DOCKER_PULL_TIMEOUT, then
	// DefaultPullTimeout.
	PullTimeout time.Duration
	// BindProbeTimeout bounds one attempt of the boot bind probe's container.
	// Zero reads ORK_DOCKER_BIND_PROBE_TIMEOUT, then DefaultBindProbeTimeout.
	BindProbeTimeout time.Duration

	// mu guards attaches and described.
	mu sync.Mutex
	// attaches tracks, per container, the `docker start --attach` process
	// Start span. Wait consumes and removes its entry so it can block on the
	// SAME attach draining fully before reporting the exit status — see
	// dockerAttach and Wait for why that join is load-bearing.
	attaches map[ContainerID]*dockerAttach
	// described is the first successful Describe, kept for the process's
	// lifetime. A failure is never kept.
	described *RuntimeInfo
}

var _ ContainerRuntime = (*DockerRuntime)(nil)

// NewDockerRuntime returns a runtime with environment-derived defaults.
func NewDockerRuntime() *DockerRuntime { return &DockerRuntime{} }

func (d *DockerRuntime) bin() (string, error) {
	name := strings.TrimSpace(d.Bin)
	if name == "" {
		name = strings.TrimSpace(os.Getenv(DockerBinVar))
	}
	if name == "" {
		name = DefaultDockerBin
	}
	if strings.ContainsRune(name, os.PathSeparator) {
		return name, nil
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("docker CLI %q not found on PATH (set %s): %w", name, DockerBinVar, err)
	}
	return resolved, nil
}

// control runs a short bookkeeping command on a context of its own. Only a
// failure to START the CLI is an error; a non-zero exit is data the caller
// interprets.
func (d *DockerRuntime) control(ctx context.Context, args ...string) (int, string, error) {
	return d.controlWithin(ctx, controlTimeout, args...)
}

func (d *DockerRuntime) controlWithin(ctx context.Context, within time.Duration, args ...string) (int, string, error) {
	code, out, _, err := d.runWithin(ctx, within, args...)
	return code, out, err
}

// runWithin is controlWithin that also reports whether the command overran
// its own budget: `within` elapsed while ctx was still live, and the CLI was
// killed for it.
//
// controlWithin cannot say so. A CLI killed by its deadline is an exit error
// with status -1 — the same "data" as any non-zero exit — so a caller that
// must tell "the daemon was too slow to answer" from "the command failed"
// cannot recover the difference from the code. A command stopped because ctx
// itself ended is not an overrun: that is its caller's own stop.
func (d *DockerRuntime) runWithin(
	ctx context.Context, within time.Duration, args ...string,
) (code int, out string, overran bool, err error) {
	bin, err := d.bin()
	if err != nil {
		return 0, "", false, err
	}
	cmdCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()

	var combined bytes.Buffer
	cmd := exec.CommandContext(cmdCtx, bin, args...)
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	cmd.WaitDelay = 100 * time.Millisecond
	runErr := cmd.Run()
	overran = runErr != nil && errors.Is(cmdCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	switch {
	case runErr == nil:
		return 0, combined.String(), false, nil
	case isExitErr(runErr):
		return exitCodeOf(runErr), combined.String(), overran, nil
	case cmdCtx.Err() != nil:
		return 0, combined.String(), overran, fmt.Errorf("docker %s: %w", args[0], cmdCtx.Err())
	default:
		return 0, combined.String(), false, fmt.Errorf("docker %s: %w", args[0], runErr)
	}
}

// Pull makes a digest-addressed image available and reports what it resolved.
func (d *DockerRuntime) Pull(ctx context.Context, pinned string) (PullInfo, error) {
	if !strings.Contains(pinned, "@sha256:") {
		// A running execution must never silently switch images because a tag
		// moved (AGENTS.md §5). The runtime refuses a tag rather than
		// resolving one: resolution is apply-time work with an audit record,
		// not something to do quietly at dispatch.
		return PullInfo{}, fmt.Errorf("%w: image %q is not digest-addressed", errNotPinned, pinned)
	}

	// One inspect answers both questions — is the image local, and what are
	// its platform facts — so a cached image (the common case) costs one
	// docker process rather than two. A non-zero exit is "absent".
	info, present, err := d.lookupImage(ctx, pinned)
	if err != nil {
		return PullInfo{}, err
	}
	if !present {
		code, out, err := d.controlWithin(ctx, d.pullTimeout(), "pull", "--quiet", pinned)
		if err != nil {
			return PullInfo{}, fmt.Errorf("pull %s: %w", pinned, err)
		}
		if code != 0 {
			return PullInfo{}, fmt.Errorf("pull %s: docker exited %d: %s", pinned, code, strings.TrimSpace(out))
		}
		// Re-inspected only after an actual pull, for the facts of what was
		// pulled.
		if info, err = d.inspectImage(ctx, pinned); err != nil {
			return PullInfo{}, err
		}
	}
	info.Cached = present
	info.Delivery = DeliveryRegistry
	return info, nil
}

var errNotPinned = errors.New("image reference is not digest-addressed")

// LoadLayout imports an image from a local OCI image layout (§4.6).
//
// `docker load` accepts an OCI layout tarball (§14.12), but it VERIFIES
// NOTHING about the root object (§14.15) — a tampered layout loads happily.
// So the caller hashes the root blob against the pinned digest BEFORE calling
// this, and that check, not the load, is what makes the fallback safe.
func (d *DockerRuntime) LoadLayout(ctx context.Context, layoutPath, digest string) (PullInfo, error) {
	if _, err := os.Stat(layoutPath); err != nil {
		return PullInfo{}, fmt.Errorf("%w: layout %s: %w", ErrNotInLayout, layoutPath, err)
	}
	code, out, err := d.controlWithin(ctx, d.pullTimeout(), "load", "--quiet", "--input", layoutPath)
	if err != nil {
		return PullInfo{}, fmt.Errorf("load layout %s: %w", layoutPath, err)
	}
	if code != 0 {
		return PullInfo{}, fmt.Errorf("%w: load %s: docker exited %d: %s",
			ErrNotInLayout, layoutPath, code, strings.TrimSpace(out))
	}
	info, err := d.inspectImage(ctx, digest)
	if err != nil {
		return PullInfo{}, fmt.Errorf("%w: layout %s does not hold %s: %w",
			ErrNotInLayout, layoutPath, digest, err)
	}
	info.Cached = false
	info.Delivery = DeliveryEmbedded
	return info, nil
}

func (d *DockerRuntime) imagePresent(ctx context.Context, ref string) (bool, error) {
	code, _, err := d.control(ctx, "image", "inspect", "--format", "{{.Id}}", ref)
	if err != nil {
		return false, err
	}
	return code == 0, nil
}

// inspectImage reads the platform facts the commit records as audit data. An
// absent image is an error.
func (d *DockerRuntime) inspectImage(ctx context.Context, ref string) (PullInfo, error) {
	info, present, err := d.lookupImage(ctx, ref)
	if err != nil {
		return PullInfo{}, err
	}
	if !present {
		return PullInfo{}, fmt.Errorf("image inspect %s: the image is not present", ref)
	}
	return info, nil
}

// lookupImage is one `image inspect`: whether ref is local, and if it is, its
// platform facts. A non-zero exit means absent; only a CLI that could not run,
// or output that does not decode, is an error.
func (d *DockerRuntime) lookupImage(ctx context.Context, ref string) (PullInfo, bool, error) {
	code, out, err := d.control(ctx, "image", "inspect", "--format", "{{json .}}", ref)
	if err != nil {
		return PullInfo{}, false, err
	}
	if code != 0 {
		return PullInfo{}, false, nil
	}
	var raw struct {
		ID           string   `json:"Id"`
		RepoDigests  []string `json:"RepoDigests"`
		Os           string   `json:"Os"`
		Architecture string   `json:"Architecture"`
		Variant      string   `json:"Variant"`
	}
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		return PullInfo{}, false, fmt.Errorf("image inspect %s: decode: %w", ref, err)
	}
	platform := raw.Os + "/" + raw.Architecture
	if raw.Variant != "" {
		platform += "/" + raw.Variant
	}
	return PullInfo{PlatformDigest: raw.ID, Platform: platform}, true, nil
}

func (d *DockerRuntime) pullTimeout() time.Duration {
	if d.PullTimeout > 0 {
		return d.PullTimeout
	}
	if raw := strings.TrimSpace(os.Getenv(PullTimeoutVar)); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return DefaultPullTimeout
}

// bindProbeTimeout is the budget of one attempt of the boot bind probe: the
// field, then ORK_DOCKER_BIND_PROBE_TIMEOUT, then DefaultBindProbeTimeout. A
// value that is not a positive duration is ignored, as pullTimeout ignores it,
// rather than read as no budget.
func (d *DockerRuntime) bindProbeTimeout() time.Duration {
	if d.BindProbeTimeout > 0 {
		return d.BindProbeTimeout
	}
	if raw := strings.TrimSpace(os.Getenv(BindProbeTimeoutVar)); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return DefaultBindProbeTimeout
}

// Create builds the container. Every §9.2 constraint is materialized here as a
// flag, and nothing from §9.1 is expressible: there is no branch that could
// add a capability, mount a socket, or select a host namespace.
func (d *DockerRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	args, err := dockerCreateArgs(spec)
	if err != nil {
		return "", err
	}
	code, out, err := d.control(ctx, args...)
	if err != nil {
		return "", err
	}
	if code != 0 {
		trimmed := strings.TrimSpace(out)
		if isUnbindable(trimmed) {
			return "", fmt.Errorf("%w: %s", ErrNotBindable, trimmed)
		}
		return "", fmt.Errorf("create container: docker exited %d: %s", code, trimmed)
	}
	id := strings.TrimSpace(out)
	if id == "" {
		return "", errors.New("create container: docker returned no container id")
	}
	return ContainerID(id), nil
}

// dockerCreateArgs renders a spec to argv. It is a pure function so a test can
// assert the whole security posture without a daemon.
func dockerCreateArgs(spec ContainerSpec) ([]string, error) {
	switch {
	case spec.Image == "":
		return nil, errors.New("create container: image is required")
	case !spec.ReadOnlyRootfs, !spec.DropAllCapabilities, !spec.NoNewPrivileges, !spec.Init:
		// A spec that weakens the §9.2 baseline is refused at construction
		// rather than executed. These are not options.
		return nil, fmt.Errorf("create container: spec weakens the mandatory security profile "+
			"(readonly=%t cap-drop-all=%t no-new-privileges=%t init=%t)",
			spec.ReadOnlyRootfs, spec.DropAllCapabilities, spec.NoNewPrivileges, spec.Init)
	case spec.Network != NetworkNone && spec.Network != NetworkDefault:
		return nil, fmt.Errorf("create container: network mode %q is not %q or %q",
			spec.Network, NetworkDefault, NetworkNone)
	case spec.CPUs == "" || spec.Memory == "" || spec.PidsLimit <= 0:
		// No workload ever receives unlimited resources, including one that
		// requested nothing (§9.3).
		return nil, fmt.Errorf("create container: resources must always be bounded "+
			"(cpus=%q memory=%q pids=%d)", spec.CPUs, spec.Memory, spec.PidsLimit)
	}

	args := []string{
		"create",
		"--name", containerName(spec.Labels["ork.task"]),
		"--read-only",
		"--cap-drop=ALL",
		"--security-opt", "no-new-privileges",
		"--security-opt", "seccomp=" + spec.SeccompProfile,
		"--init",
		"--workdir", spec.WorkDir,
		"--network", spec.Network,
		"--cpus", dockerCPUs(spec.CPUs),
		"--memory", dockerMemory(spec.Memory),
		"--pids-limit", strconv.FormatInt(spec.PidsLimit, 10),
	}
	if spec.StopTimeoutSeconds > 0 {
		args = append(args, "--stop-timeout", strconv.Itoa(spec.StopTimeoutSeconds))
	}
	if spec.User != "" {
		args = append(args, "--user", spec.User)
	}
	if spec.TmpfsPath != "" {
		args = append(args, "--tmpfs",
			fmt.Sprintf("%s:rw,nosuid,nodev,size=%d", spec.TmpfsPath, spec.TmpfsBytes))
	}

	for _, key := range sortedKeys(spec.Labels) {
		args = append(args, "--label", key+"="+spec.Labels[key])
	}
	for _, key := range sortedKeys(spec.Env) {
		args = append(args, "--env", key+"="+spec.Env[key])
	}
	for _, m := range spec.Mounts {
		if err := checkMount(m); err != nil {
			return nil, err
		}
		mount := fmt.Sprintf("type=bind,source=%s,destination=%s", m.Source, m.Destination)
		if m.ReadOnly {
			mount += ",readonly"
		}
		args = append(args, "--mount", mount)
	}

	args = append(args, spec.Image)
	args = append(args, spec.Command...)
	return args, nil
}

// forbiddenMountTargets are container paths nothing may be mounted at.
//
// The docker socket is listed as a TARGET rule as well as a source rule
// because today's target validation accepts any clean absolute path: nothing
// may mount anything at a container path that would shadow a runtime socket
// (§9.1).
var forbiddenMountTargets = []string{
	"/var/run/docker.sock",
	"/run/docker.sock",
}

func checkMount(m Mount) error {
	if m.Source == "" || !strings.HasPrefix(m.Source, "/") {
		return fmt.Errorf("mount source %q must be an absolute Orker-created path", m.Source)
	}
	if !strings.HasPrefix(m.Destination, "/") {
		return fmt.Errorf("mount destination %q must be absolute", m.Destination)
	}
	for _, forbidden := range forbiddenMountTargets {
		if m.Source == forbidden {
			return fmt.Errorf("mount source %q is forbidden without exception (§9.1)", m.Source)
		}
		if m.Destination == forbidden {
			return fmt.Errorf("mount destination %q is forbidden without exception (§9.1)", m.Destination)
		}
	}
	return nil
}

// Start runs the container and attaches its two streams.
//
// stdout and stderr are captured independently and byte-exact, never merged on
// disk (§3.6): `docker start --attach` writes each to its own pipe, so the two
// writers receive exactly what the workload wrote to each descriptor.
//
// Start does NOT return until the container has actually left the `created`
// state, and that wait is load-bearing rather than tidy. `docker wait` on a
// container that has not been started RETURNS 0 IMMEDIATELY — measured on
// Docker 29.1.5 — so a Wait that raced ahead of the attach would report exit
// status 0 for a container that never ran. The dispatcher would then validate
// outputs, find none, and blame the image; and a workload quick enough to
// write its values.json first would have a fabricated success committed
// durably. Found by TestIntegration_DispatcherRunsAReferenceImageEndToEnd.
//
// Start deliberately returns before the container exits (the interface
// promises exactly that), which means the caller's Wait — a SEPARATE `docker
// wait` invocation — can observe the container's exit before this attach's
// own stdout/stderr copy has drained a buffered burst of output through the
// daemon -> CLI -> Go io.Writer pipeline. A fast, high-output container could
// exit before its own output finished landing in the durable log, truncating
// it silently. So the attach is registered here and Wait joins on it before
// reporting: see dockerAttach and Wait. Found via a truncated stdout.log on a
// task that wrote ~10k lines in under a second.
func (d *DockerRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	bin, err := d.bin()
	if err != nil {
		return err
	}
	// The CLI's own diagnostics arrive on the same stderr the workload uses,
	// so a small copy is kept to explain a refusal. It is bounded because a
	// workload's stderr is unbounded.
	diagnostics := &boundedBuffer{limit: 4096}

	// NOT exec.CommandContext, and that is the point. Binding the attach to
	// the activity context kills `docker start --attach` the instant the
	// activity is cancelled — which is exactly when the workload is running
	// its TERM handler and printing what it cleaned up. The container's LAST
	// WORDS were being dropped from the live stream and from the durable
	// stdout.log, so a cancelled attempt's logs ended mid-flight and §3.6's
	// "retain them and associate them with the attempt" quietly did not hold
	// for the one case an operator most wants to read.
	//
	// The attach instead ends when the CONTAINER does, which is guaranteed:
	// cancellation runs SIGTERM -> grace -> SIGKILL, so the process cannot
	// outlive the container it is reading. Found by `orkctl activity test`,
	// whose cancellation assertion watches for the workload's own TERM-trap
	// marker and never saw it.
	cmd := exec.Command(bin, "start", "--attach", string(id))
	cmd.Stdout = stdout
	cmd.Stderr = io.MultiWriter(stderr, diagnostics)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start container %s: %w", id, err)
	}
	// The attach process outlives Start deliberately: Start returns once the
	// container is running, and Wait is what blocks on the exit. Registered
	// before awaitLeftCreated runs so Wait can find it even if this container
	// exits (and Wait is called) before Start itself returns.
	attach := newDockerAttach(cmd)
	d.trackAttach(id, attach)

	return d.awaitLeftCreated(ctx, id, attach, diagnostics)
}

// dockerAttach tracks the goroutine draining `docker start --attach`'s
// stdout/stderr copy. done closes once cmd.Wait() returns, at which point
// every byte the container wrote has either reached the writers passed to
// Start or failed trying — never left mid-copy. Unlike a value channel, done
// can be observed by more than one reader (awaitLeftCreated and, later,
// Wait) without either consuming the other's signal.
type dockerAttach struct {
	done chan struct{}
	err  error
}

func newDockerAttach(cmd *exec.Cmd) *dockerAttach {
	a := &dockerAttach{done: make(chan struct{})}
	go func() {
		a.err = cmd.Wait()
		close(a.done)
	}()
	return a
}

func (d *DockerRuntime) trackAttach(id ContainerID, attach *dockerAttach) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.attaches == nil {
		d.attaches = make(map[ContainerID]*dockerAttach)
	}
	d.attaches[id] = attach
}

// takeAttach returns and forgets the attach registered for id, so a second
// Start for a reused (or, in tests, coincidentally repeated) id cannot join a
// stale one.
func (d *DockerRuntime) takeAttach(id ContainerID) *dockerAttach {
	d.mu.Lock()
	defer d.mu.Unlock()
	attach := d.attaches[id]
	delete(d.attaches, id)
	return attach
}

// awaitLeftCreated blocks until the daemon reports the container is no longer
// merely created.
//
// It polls a STATE rather than sleeping a guessed interval, and it watches the
// attach process at the same time: a `docker start` that fails outright exits
// immediately, and reporting that at once is far better than discovering it at
// the deadline. The deadline remains, for a daemon that accepts the start and
// then never runs it.
func (d *DockerRuntime) awaitLeftCreated(
	ctx context.Context,
	id ContainerID,
	attach *dockerAttach,
	diagnostics *boundedBuffer,
) error {
	deadline := time.Now().Add(startStateTimeout)
	poll := startStatePoll
	for {
		started, err := d.leftCreated(ctx, id)
		if err != nil {
			if ctx.Err() != nil {
				return d.startedDespiteCancel(ctx, id)
			}
			return err
		}
		if started {
			return nil
		}

		select {
		case <-attach.done:
			// The attach process is gone. Re-observe once: a container that
			// ran and exited very quickly is a success, not a refusal.
			started, err := d.leftCreated(ctx, id)
			if err != nil {
				return err
			}
			if started {
				return nil
			}
			return fmt.Errorf("start container %s: the runtime exited without running it: %v: %s",
				id, attach.err, strings.TrimSpace(diagnostics.String()))
		case <-ctx.Done():
			return d.startedDespiteCancel(ctx, id)
		case <-time.After(poll):
		}
		// Back off: a container that starts at once is seen on the first or
		// second observation, and one the daemon is slow to start (a large
		// image's layers, a busy host) is not observed fifty times a second.
		if poll *= 2; poll > startStatePollMax {
			poll = startStatePollMax
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("start container %s: it was still in the created state after %s: %s",
				id, startStateTimeout, strings.TrimSpace(diagnostics.String()))
		}
	}
}

// startedDespiteCancel decides what a cancellation that arrives while Start is
// still observing the container means. The workload may already be running —
// the daemon starts it before this loop's next observation — and a running
// workload is owed the cancellation sequence (SIGTERM, the grace period, then
// SIGKILL: AGENTS.md §3.3), which only the caller's cancellation path
// delivers. Returning the cancellation as a start failure instead sent the
// caller to its deferred force-remove, which killed a running container with
// no SIGTERM and no grace. So the state is observed once more, on a context
// that survives the cancellation: a container that has left created is
// reported started, and only one that never did is a cancelled start.
func (d *DockerRuntime) startedDespiteCancel(ctx context.Context, id ContainerID) error {
	observeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlTimeout)
	defer cancel()
	if started, err := d.leftCreated(observeCtx, id); err == nil && started {
		return nil
	}
	return ctx.Err()
}

// leftCreated reports whether the daemon has moved the container out of the
// created state.
func (d *DockerRuntime) leftCreated(ctx context.Context, id ContainerID) (bool, error) {
	code, out, err := d.control(ctx, "inspect", "--format", "{{.State.Status}}", string(id))
	if err != nil {
		return false, fmt.Errorf("start container %s: observe its state: %w", id, err)
	}
	if code != 0 {
		return false, nil
	}
	switch status := strings.TrimSpace(out); status {
	case "created":
		return false, nil
	case "":
		return false, fmt.Errorf("start container %s: the daemon reported no state", id)
	default:
		// running, exited, paused, dead — all mean the container was started,
		// which is what Wait needs to be true.
		return true, nil
	}
}

// boundedBuffer keeps at most limit bytes, so a workload's stderr cannot make
// a diagnostic copy unbounded.
type boundedBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := b.limit - len(b.buf); room > 0 {
		if len(p) < room {
			room = len(p)
		}
		b.buf = append(b.buf, p[:room]...)
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

const (
	// startStateTimeout bounds how long the daemon may take to move a
	// container out of `created`.
	startStateTimeout = 60 * time.Second
	// startStatePoll is the first gap between state observations; each later
	// gap doubles, up to startStatePollMax. It is a poll of a real condition
	// with a deadline, not a sleep standing in for synchronization.
	startStatePoll    = 20 * time.Millisecond
	startStatePollMax = 200 * time.Millisecond
)

// Signal delivers sig to the container's PID 1.
func (d *DockerRuntime) Signal(ctx context.Context, id ContainerID, sig Signal) error {
	code, out, err := d.control(ctx, "kill", "--signal", string(sig), string(id))
	if err != nil {
		return err
	}
	if code != 0 {
		trimmed := strings.TrimSpace(out)
		if strings.Contains(trimmed, "is not running") || strings.Contains(trimmed, "No such container") {
			// Signalling a container that already exited is the normal race
			// between the grace timer and a well-behaved workload, not a
			// failure.
			return nil
		}
		return fmt.Errorf("signal %s %s: docker exited %d: %s", sig, id, code, trimmed)
	}
	return nil
}

// Wait blocks until the container exits.
//
// `docker wait` (below) reports PID 1's exit the instant it happens. That is
// a DIFFERENT process from the `docker start --attach` Start registered,
// draining stdout/stderr on its own schedule, and for a fast, high-output
// container the two can finish in either order (see Start's doc comment).
// So before reporting the exit status, Wait joins the attach registered for
// id — the one belonging to THIS Start/Wait pair, via the map keyed by id —
// which is what guarantees every byte the container wrote already reached
// the writers Start was given by the time a caller acts on this result.
func (d *DockerRuntime) Wait(ctx context.Context, id ContainerID) (ExitStatus, error) {
	bin, err := d.bin()
	if err != nil {
		return ExitStatus{}, err
	}
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, "wait", string(id))
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return ExitStatus{}, fmt.Errorf("wait for container %s: %w", id, err)
	}
	code, err := strconv.Atoi(strings.TrimSpace(out.String()))
	if err != nil {
		return ExitStatus{}, fmt.Errorf("wait for container %s: unreadable status %q: %w",
			id, strings.TrimSpace(out.String()), err)
	}
	status := ExitStatus{Code: int32(code)}
	// Only a non-zero status reads OOMKilled (the dispatcher names an OOM kill
	// in a CONTAINER_EXIT_NONZERO message), so a successful container costs no
	// extra inspect.
	if status.Code != 0 {
		status.OOMKilled = d.oomKilled(ctx, id)
	}

	if attach := d.takeAttach(id); attach != nil {
		select {
		case <-attach.done:
			// `docker start --attach` exits with the container's own exit
			// code when attached, so an *exec.ExitError here is simply the
			// workload's non-zero exit reported a second time — already
			// captured in status above — not a failure to drain the copy.
			var exitErr *exec.ExitError
			if attach.err != nil && !errors.As(attach.err, &exitErr) {
				return status, fmt.Errorf("wait for container %s: drain its attached output: %w", id, attach.err)
			}
		case <-ctx.Done():
			return status, ctx.Err()
		}
	}

	return status, nil
}

func (d *DockerRuntime) oomKilled(ctx context.Context, id ContainerID) bool {
	code, out, err := d.control(ctx, "inspect", "--format", "{{.State.OOMKilled}}", string(id))
	if err != nil || code != 0 {
		return false
	}
	return strings.TrimSpace(out) == "true"
}

// Remove force-removes the container. Callers pass a DETACHED context: cleanup
// must survive the cancellation that caused it.
func (d *DockerRuntime) Remove(ctx context.Context, id ContainerID) error {
	code, out, err := d.control(ctx, "rm", "--force", "--volumes", string(id))
	if err != nil {
		return err
	}
	if code != 0 && !strings.Contains(out, "No such container") {
		return fmt.Errorf("remove container %s: docker exited %d: %s", id, code, strings.TrimSpace(out))
	}
	return nil
}

// Reap force-removes every container this Orker labelled in a previous life.
//
// This is §5.5 measure 2. It matters because an Orker crash does NOT stop its
// containers: after SIGKILL of the managing process the labelled container was
// still running three seconds later, and so was the --rm control, because --rm
// is daemon-side but fires on container EXIT and a container whose client was
// killed has not exited (§14.17).
func (d *DockerRuntime) Reap(ctx context.Context, orkerID string) (int, error) {
	if orkerID == "" {
		return 0, nil
	}
	code, out, err := d.control(ctx, "ps", "--all", "--quiet",
		"--filter", "label=ork.orker="+orkerID)
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return 0, fmt.Errorf("reap: list containers: docker exited %d: %s", code, strings.TrimSpace(out))
	}
	var (
		removed  int
		failures []error
	)
	for _, id := range strings.Fields(out) {
		// One container that cannot be removed (a removal already in
		// progress, a daemon hiccup) must not leave every other orphan
		// running, so every one is tried and the failures are reported
		// together.
		if err := d.Remove(ctx, ContainerID(id)); err != nil {
			failures = append(failures, fmt.Errorf("reap container %s: %w", id, err))
			continue
		}
		removed++
	}
	if len(failures) > 0 {
		return removed, &ReapIncompleteError{Failures: failures}
	}
	return removed, nil
}

// Describe reports the runtime's identity for DescribeOrker.
//
// The answer is process-lifetime data — the daemon's version and whether it is
// rootless do not change under a running Orker — and DescribeOrker is the pool
// liveness probe, asked several times per submit. So the first SUCCESSFUL
// answer is kept and every later call returns it without spawning a process. A
// failure is not kept: a daemon that comes up after the Orker did is still seen
// on the next call.
func (d *DockerRuntime) Describe(ctx context.Context) (RuntimeInfo, error) {
	d.mu.Lock()
	if d.described != nil {
		info := *d.described
		d.mu.Unlock()
		return info, nil
	}
	d.mu.Unlock()

	info := RuntimeInfo{Name: "docker"}
	code, out, err := d.control(ctx, "version", "--format", "{{.Server.Version}}")
	if err != nil {
		return info, err
	}
	if code != 0 {
		return info, fmt.Errorf("docker version: exited %d: %s", code, strings.TrimSpace(out))
	}
	info.Version = strings.TrimSpace(out)

	// Rootless is a daemon fact, not a flag: the security context reports it.
	if code, out, err := d.control(ctx, "info", "--format", "{{.SecurityOptions}}"); err == nil && code == 0 {
		info.Rootless = strings.Contains(out, "rootless")
	}
	d.mu.Lock()
	kept := info
	d.described = &kept
	d.mu.Unlock()
	return info, nil
}

// ProbeBindable verifies the daemon can bind-mount a host path, by doing it.
//
// §3.1 requires this at BOOT rather than at first task: on macOS with a
// VM-hosted daemon $HOME is shared and /var/folders is not, so an Orker
// pointed at the wrong workdir root fails every task later with an error that
// names the container and not the cause.
//
// The probe has three distinct ways to fail, and each is reported as its own
// thing: the root cannot be bound (ErrNotBindable: the probe container ran
// and exited non-zero, or no probe directory could be made), the probe's image
// is not available (ErrProbeImageUnavailable), and the daemon did not start
// the probe container within the probe's budget (ErrProbeTimedOut, after one
// retry). An Orker that is stopped meanwhile gets the context's own error.
func (d *DockerRuntime) ProbeBindable(ctx context.Context, root string) error {
	probe, err := os.MkdirTemp(root, "ork-bindprobe-*")
	if err != nil {
		return fmt.Errorf("%w: cannot create a probe directory under %s: %w", ErrNotBindable, root, err)
	}
	defer func() { _ = os.RemoveAll(probe) }()

	// The probe image is fetched first, under the PULL timeout rather than the
	// probe run's budget an implicit pull would spend, and a failure to fetch
	// it is its own error: a slow or rate-limited registry is not a work
	// root the daemon cannot bind, and naming it as one sends an operator to
	// the wrong fix.
	present, err := d.imagePresent(ctx, bindProbeImage)
	if err != nil {
		return fmt.Errorf("%w: the bind probe image %s: %w", ErrProbeImageUnavailable, bindProbeImage, err)
	}
	if !present {
		code, out, err := d.controlWithin(ctx, d.pullTimeout(), "pull", "--quiet", bindProbeImage)
		if err != nil {
			return fmt.Errorf("%w: the bind probe image %s could not be pulled: %w",
				ErrProbeImageUnavailable, bindProbeImage, err)
		}
		if code != 0 {
			return fmt.Errorf("%w: the bind probe image %s could not be pulled: docker exited %d: %s",
				ErrProbeImageUnavailable, bindProbeImage, code, strings.TrimSpace(out))
		}
	}

	// The run is the one step of the probe that waits on the daemon to create,
	// start and tear down a container, and under load that has taken longer
	// than the control timeout (a daemon needed 32 s to start a container
	// that exits at once). It therefore has a budget of its own, and an
	// attempt that overruns it is tried once more: whether the daemon is slow
	// is a fact about the moment, and whether the root binds is not.
	// A container that RAN and exited non-zero is the only answer that says
	// the root cannot be bound, and it is final.
	budget := d.bindProbeTimeout()
	for attempt := 1; attempt <= bindProbeAttempts; attempt++ {
		name := containerName("bindprobe")
		code, out, overran, err := d.runWithin(ctx, budget, "run", "--rm",
			"--name", name,
			"--network", "none",
			"--read-only",
			"--cap-drop=ALL",
			"--security-opt", "no-new-privileges",
			"--mount", fmt.Sprintf("type=bind,source=%s,destination=/probe,readonly", probe),
			"--entrypoint", "/bin/true",
			bindProbeImage)
		switch {
		case err == nil && code == 0:
			return nil
		case ctx.Err() != nil:
			// The Orker is stopping, which is neither a slow daemon nor an
			// unbindable root; Boot reports it as the interruption it is.
			return fmt.Errorf("probe bind of %s interrupted: %w", root, ctx.Err())
		case overran:
			// The killed CLI may have left its container half-made, or the
			// daemon may still be starting it. Remove it on a context of its
			// own so the retry does not run beside it.
			_ = d.Remove(context.WithoutCancel(ctx), ContainerID(name))
		case err != nil:
			return fmt.Errorf("%w: probe bind of %s: %w", ErrNotBindable, root, err)
		default:
			return fmt.Errorf("%w: probe bind of %s: docker exited %d: %s",
				ErrNotBindable, root, code, strings.TrimSpace(out))
		}
	}
	return fmt.Errorf("%w: the container daemon did not start the probe container within %s on each of %d attempts. "+
		"That does not show that %s cannot be bound, only that the daemon is slow or unresponsive; "+
		"raise %s if it is only slow",
		ErrProbeTimedOut, budget, bindProbeAttempts, root, BindProbeTimeoutVar)
}

// bindProbeAttempts is how many times the boot bind probe runs its container
// when the daemon overruns the probe's budget: the first try and one retry.
const bindProbeAttempts = 2

// bindProbeImage is the image the boot-time bind probe runs. It is pinned to a
// tag rather than a digest deliberately: it is Orker-local infrastructure that
// runs no workload code and produces no commit, so the immutability rule that
// governs activity images (AGENTS.md §5) does not apply to it.
const bindProbeImage = "alpine:3.20"

func containerName(taskID string) string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("ork-%s-%d", safeNameComponent(taskID), time.Now().UnixNano())
	}
	return fmt.Sprintf("ork-%s-%s", safeNameComponent(taskID), hex.EncodeToString(suffix[:]))
}

// safeNameComponent renders a task id into Docker's name grammar
// ([a-zA-Z0-9][a-zA-Z0-9_.-]*), so a stray container is traceable to its task.
func safeNameComponent(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	cleaned := strings.TrimLeft(b.String(), "-._")
	if len(cleaned) > 40 {
		cleaned = cleaned[:40]
	}
	if cleaned == "" {
		return "task"
	}
	return cleaned
}

// isUnbindable recognizes the daemon's own words for a source path it cannot
// bind. It separates WORKSPACE_NOT_BINDABLE (a host misconfiguration retrying
// will not fix) from RUNTIME_REFUSED (which is retryable).
func isUnbindable(message string) bool {
	lowered := strings.ToLower(message)
	return strings.Contains(lowered, "bind source path does not exist") ||
		strings.Contains(lowered, "mounts denied") ||
		strings.Contains(lowered, "is not shared from the host")
}

func isExitErr(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr)
}

func exitCodeOf(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// dockerMemory renders a resource quantity in the spelling `docker --memory`
// accepts.
//
// The CONTRACT spells memory the Kubernetes way — "4Gi", "512Mi" (§9.3,
// ResourceRequest) — and Docker does not understand it: `--memory 4Gi` is
// rejected outright with `invalid suffix: 'gi'`. Docker's own suffixes are
// b/k/m/g, and its binary/decimal meanings do not line up with Kubernetes'
// either, so translating suffix-to-suffix would quietly change the number.
//
// Rendering the exact BYTE COUNT avoids both problems: a plain integer is
// unambiguous to Docker and is exactly what the request asked for. Found by
// TestIntegration_DispatcherRunsAReferenceImageEndToEnd, where every container
// was refused before it started.
//
// This translation belongs here and nowhere else. AGENTS.md §5 requires the
// OCI runtime boundary to stay clean, and a runtime's flag grammar is the
// runtime's business: a future Podman or containerd implementation renders the
// same contract quantity its own way.
func dockerMemory(quantity string) string {
	bytes, err := parseMemory(quantity)
	if err != nil || bytes <= 0 {
		// The policy already validated this, so a parse failure here means
		// the two disagree. Pass it through and let the daemon say so rather
		// than silently substituting a limit nobody asked for.
		return quantity
	}
	return strconv.FormatInt(int64(bytes), 10)
}

// dockerCPUs renders a CPU quantity as the decimal `docker --cpus` accepts.
//
// The contract also allows the Kubernetes millicpu spelling ("500m"), which
// Docker does not parse. 500m is 0.5 CPUs.
func dockerCPUs(quantity string) string {
	cpus, err := parseCPU(quantity)
	if err != nil || cpus <= 0 {
		return quantity
	}
	return strconv.FormatFloat(cpus, 'f', -1, 64)
}
