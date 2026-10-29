// This file and runtime_dialect.go are the runtime files: the ONLY places in
// the repository that may name a Docker or Podman concept. AGENTS.md §5
// requires the OCI runtime boundary to stay clean enough for a future
// containerd, Kubernetes Jobs, ECS or Nomad implementation, and the way that
// requirement is held is that every other file talks to ContainerRuntime and
// nothing else.
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
	"path/filepath"
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
	// pipeDrainDelay is how long a docker CLI's output may stay open after
	// the CLI exited, or after its deadline killed it, before the runner
	// stops reading (exec.Cmd.WaitDelay). It bounds a pipe a helper process
	// inherited, never the command itself. At 100 ms a loaded host's late
	// drain read as a failure of a CLI that had exited 0: a successful
	// `docker create` became RUNTIME_REFUSED.
	pipeDrainDelay = 5 * time.Second
	// removeKillTimeout bounds the kill Remove sends before it removes. It
	// is short because its answer decides nothing: a kill that does not
	// finish in time leaves the removal to stop the container, as it did
	// before the kill came first.
	removeKillTimeout = 5 * time.Second
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

// DockerRuntime drives a Docker-compatible CLI: the docker CLI, against Docker
// or against Podman's Docker-compatible socket, or the podman CLI. It is the
// one ContainerRuntime that ships. What differs between them is a dialect, a
// value of spellings (runtime_dialect.go), on two axes: the CLI's flag
// grammar, which is configured (CLI, ORK_DOCKER_CLI), and the engine behind
// it, which is asked (`version`) and verified by the boot probe.
//
// It shells out rather than linking the daemon SDK: the CLI is the stable
// interface, it is what an operator debugging a container will type, and it
// keeps the dependency surface at zero.
type DockerRuntime struct {
	// Bin is the CLI to drive. Empty reads ORK_DOCKER_BIN, then the CLI's own
	// name (DefaultDockerBin, or "podman" for the podman CLI) on PATH.
	Bin string
	// CLI is the flag grammar Bin speaks: CLIDocker or CLIPodman. Empty reads
	// ORK_DOCKER_CLI, then DefaultDockerCLI.
	CLI string
	// PullTimeout bounds one pull. Zero reads ORK_DOCKER_PULL_TIMEOUT, then
	// DefaultPullTimeout.
	PullTimeout time.Duration
	// BindProbeTimeout bounds one attempt of the boot bind probe's container.
	// Zero reads ORK_DOCKER_BIND_PROBE_TIMEOUT, then DefaultBindProbeTimeout.
	BindProbeTimeout time.Duration

	// mu guards attaches, described and spoken.
	mu sync.Mutex
	// attaches tracks, per container, the `docker start --attach` process
	// Start span. Wait consumes and removes its entry so it can block on the
	// SAME attach draining fully before reporting the exit status — see
	// dockerAttach and Wait for why that join is load-bearing.
	attaches map[ContainerID]*dockerAttach
	// described is the first successful Describe, kept for the process's
	// lifetime. A failure is never kept.
	described *RuntimeInfo
	// spoken is the dialect, once the engine behind the CLI has answered.
	// The engine does not change under a running Orker; a failure to ask is
	// never kept.
	spoken *dialect
}

var _ ContainerRuntime = (*DockerRuntime)(nil)

// NewDockerRuntime returns a runtime with environment-derived defaults.
func NewDockerRuntime() *DockerRuntime { return &DockerRuntime{} }

func (d *DockerRuntime) bin() (string, error) {
	cli, err := d.cli()
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(d.Bin)
	if name == "" {
		name = strings.TrimSpace(os.Getenv(DockerBinVar))
	}
	if name == "" {
		name = DefaultDockerBin
		if cli == CLIPodman {
			name = CLIPodman
		}
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

// cli is the flag grammar the runtime's CLI speaks: the field, then
// ORK_DOCKER_CLI, then DefaultDockerCLI. A value that names no known CLI is an
// error on every command, never a guess.
func (d *DockerRuntime) cli() (string, error) {
	if raw := strings.TrimSpace(d.CLI); raw != "" {
		return parseCLI(raw)
	}
	return parseCLI(os.Getenv(CLIVar))
}

// grammar is the dialect as far as the CLI setting alone decides it, for the
// spellings that do not depend on the engine (the CLI's own refusals).
func (d *DockerRuntime) grammar() dialect {
	d.mu.Lock()
	spoken := d.spoken
	d.mu.Unlock()
	if spoken != nil {
		return *spoken
	}
	cli, err := d.cli()
	if err != nil {
		return dockerDialect
	}
	return dialect{cli: cli, engine: cli}
}

// dialect is the CLI's grammar and the engine behind it, asked once and kept.
// The podman CLI only ever drives the Podman engine; the docker CLI drives
// whichever engine answers its socket, and says which in `version`.
func (d *DockerRuntime) dialect(ctx context.Context) (dialect, error) {
	spoken, _, err := d.dialectWithin(ctx, controlTimeout)
	return spoken, err
}

// dialectWithin is dialect with the engine's whole answer — which engine it
// is, and whether it is rootless — bounded by one within, and overran
// reporting that the engine did not answer in time.
func (d *DockerRuntime) dialectWithin(ctx context.Context, within time.Duration) (spoken dialect, overran bool, err error) {
	d.mu.Lock()
	if d.spoken != nil {
		spoken = *d.spoken
		d.mu.Unlock()
		return spoken, false, nil
	}
	d.mu.Unlock()

	cli, err := d.cli()
	if err != nil {
		return dialect{}, false, err
	}
	// One budget for both questions, so identifying the engine is one step
	// of whatever bound its caller states.
	askCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	timedOut := func() bool { return askCtx.Err() != nil && ctx.Err() == nil }
	spoken = dialect{cli: cli, engine: cli}
	if cli == CLIDocker {
		answer, _, err := d.runWithin(askCtx, within, "version", "--format", "{{json .Server.Components}}")
		switch {
		case timedOut():
			return dialect{}, true, fmt.Errorf("the container engine did not answer `docker version` within %s", within)
		case err != nil:
			return dialect{}, false, fmt.Errorf("identify the container engine: %w", err)
		case answer.code != 0:
			hint := ""
			if containsFold(answer.said(), "can't evaluate field") {
				// The podman CLI installed as `docker` (the podman-docker
				// shim) has no such field: it speaks the podman grammar,
				// which only the setting can say.
				hint = fmt.Sprintf("; a podman CLI installed as docker (the podman-docker shim) speaks the podman grammar: set %s=%s",
					CLIVar, CLIPodman)
			}
			return dialect{}, false, fmt.Errorf("identify the container engine: docker version exited %d: %s%s",
				answer.code, answer.said(), hint)
		}
		if spoken.engine, err = engineFromComponents(answer.stdout); err != nil {
			return dialect{}, false, fmt.Errorf("identify the container engine: %w", err)
		}
	}
	// Rootless is a daemon fact, not a flag, and one this CLI must answer: a
	// detection that failed is an error, never "not rootless".
	answer, _, err := d.runWithin(askCtx, within, "info", "--format", spoken.rootlessQuery())
	switch {
	case timedOut():
		return dialect{}, true, fmt.Errorf("the container engine did not answer `%s info` within %s", cli, within)
	case err != nil:
		return dialect{}, false, fmt.Errorf("ask whether the container engine is rootless: %w", err)
	case answer.code != 0:
		return dialect{}, false, fmt.Errorf("ask whether the container engine is rootless: %s info --format %s exited %d: %s",
			cli, spoken.rootlessQuery(), answer.code, answer.said())
	}
	if spoken.rootless, err = spoken.isRootless(answer.stdout); err != nil {
		return dialect{}, false, fmt.Errorf("ask whether the container engine is rootless: %w", err)
	}
	d.mu.Lock()
	kept := spoken
	d.spoken = &kept
	d.mu.Unlock()
	return spoken, false, nil
}

// cliAnswer is what one CLI invocation reported: its exit status and its two
// streams, kept apart.
//
// They are kept apart because only stdout is the command's answer. A CLI may
// write lines of its own on stderr while succeeding — the podman-docker shim's
// "Emulate Docker CLI using podman" on every command, a rootless runtime's
// WARN lines — and a merged buffer handed those to every parser: the image
// inspect no longer decoded, so every task failed IMAGE_PULL_FAILED, and the
// created container's id carried the warning with it.
type cliAnswer struct {
	code   int
	stdout string
	stderr string
}

// said is what the CLI told an operator: its stderr, or its stdout when it
// wrote nothing on stderr. It is for messages only, never for parsing.
func (a cliAnswer) said() string {
	if text := strings.TrimSpace(a.stderr); text != "" {
		return text
	}
	return strings.TrimSpace(a.stdout)
}

// control runs a short bookkeeping command on a context of its own. Only a
// failure to START the CLI is an error; a non-zero exit is data the caller
// interprets.
func (d *DockerRuntime) control(ctx context.Context, args ...string) (cliAnswer, error) {
	return d.controlWithin(ctx, controlTimeout, args...)
}

func (d *DockerRuntime) controlWithin(ctx context.Context, within time.Duration, args ...string) (cliAnswer, error) {
	answer, _, err := d.runWithin(ctx, within, args...)
	return answer, err
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
) (answer cliAnswer, overran bool, err error) {
	bin, err := d.bin()
	if err != nil {
		return cliAnswer{}, false, err
	}
	cmdCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(cmdCtx, bin, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = pipeDrainDelay
	runErr := cmd.Run()
	overran = runErr != nil && errors.Is(cmdCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	answer = cliAnswer{stdout: stdout.String(), stderr: stderr.String()}
	switch {
	case runErr == nil:
		return answer, false, nil
	case isExitErr(runErr):
		answer.code = exitCodeOf(runErr)
		return answer, overran, nil
	case cmdCtx.Err() != nil:
		return answer, overran, fmt.Errorf("docker %s: %w", args[0], cmdCtx.Err())
	default:
		return answer, false, fmt.Errorf("docker %s: %w", args[0], runErr)
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
		answer, err := d.controlWithin(ctx, d.pullTimeout(), "pull", "--quiet", pinned)
		if err != nil {
			return PullInfo{}, fmt.Errorf("pull %s: %w", pinned, err)
		}
		if answer.code != 0 {
			return PullInfo{}, fmt.Errorf("pull %s: docker exited %d: %s", pinned, answer.code, answer.said())
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
	answer, err := d.controlWithin(ctx, d.pullTimeout(), "load", "--quiet", "--input", layoutPath)
	if err != nil {
		return PullInfo{}, fmt.Errorf("load layout %s: %w", layoutPath, err)
	}
	if answer.code != 0 {
		return PullInfo{}, fmt.Errorf("%w: load %s: docker exited %d: %s",
			ErrNotInLayout, layoutPath, answer.code, answer.said())
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

// imagePresent asks the daemon whether ref is local, giving it `within` to
// answer. overran reports that it did not: a daemon that is silent has not
// said the image is absent, and the caller must not read it as saying so.
func (d *DockerRuntime) imagePresent(ctx context.Context, within time.Duration, ref string) (present, overran bool, err error) {
	answer, overran, err := d.runWithin(ctx, within, "image", "inspect", "--format", "{{.Id}}", ref)
	if overran || err != nil {
		return false, overran, err
	}
	return answer.code == 0, false, nil
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
	answer, err := d.control(ctx, "image", "inspect", "--format", "{{json .}}", ref)
	if err != nil {
		return PullInfo{}, false, err
	}
	if answer.code != 0 {
		return PullInfo{}, false, nil
	}
	var raw struct {
		ID           string   `json:"Id"`
		RepoDigests  []string `json:"RepoDigests"`
		Os           string   `json:"Os"`
		Architecture string   `json:"Architecture"`
		Variant      string   `json:"Variant"`
	}
	if err := json.Unmarshal([]byte(answer.stdout), &raw); err != nil {
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
	spoken, err := d.dialect(ctx)
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}
	args, err := createArgs(spec, spoken)
	if err != nil {
		return "", err
	}
	answer, err := d.control(ctx, args...)
	if err != nil {
		return "", err
	}
	if answer.code != 0 {
		said := answer.said()
		if spoken.unbindable(said) {
			return "", fmt.Errorf("%w: %s", ErrNotBindable, said)
		}
		return "", fmt.Errorf("create container: docker exited %d: %s", answer.code, said)
	}
	id := strings.TrimSpace(answer.stdout)
	if id == "" {
		return "", errors.New("create container: docker returned no container id")
	}
	return ContainerID(id), nil
}

// dockerCreateArgs renders a spec to argv for the docker CLI against Docker.
// It is a pure function so a test can assert the whole security posture
// without a daemon.
func dockerCreateArgs(spec ContainerSpec) ([]string, error) {
	return createArgs(spec, dockerDialect)
}

// createArgs renders a spec to argv in a dialect.
func createArgs(spec ContainerSpec, spoken dialect) ([]string, error) {
	flags, err := dockerContainerFlags(spec, containerName(spec.Labels["ork.task"]), spoken)
	if err != nil {
		return nil, err
	}
	return append([]string{"create"}, flags...), nil
}

// dockerContainerFlags is everything after the verb: the flags, the image and
// the command of a container named name, in a dialect. `create` and the boot
// probe's `run` take the same list, which is what makes the probe's
// container a task's.
func dockerContainerFlags(spec ContainerSpec, name string, spoken dialect) ([]string, error) {
	switch {
	case spec.Image == "":
		return nil, errors.New("create container: image is required")
	case !plainArgument(spec.Image):
		// The image is the first argument after the flags: one that begins
		// with "-" would be read as a flag, and one with a space or a control
		// character is not a reference at all. The engine pins a reference
		// to the distribution grammar (dsl.ParseReference); this is the
		// boundary that does not rely on it.
		return nil, fmt.Errorf("create container: image %q is not a reference the runtime can take as an argument", spec.Image)
	case !spec.ReadOnlyRootfs, !spec.DropAllCapabilities, !spec.NoNewPrivileges, !spec.Init:
		// A spec that weakens the §9.2 baseline is refused at construction
		// rather than executed. These are not options.
		return nil, fmt.Errorf("create container: spec weakens the mandatory security profile "+
			"(readonly=%t cap-drop-all=%t no-new-privileges=%t init=%t)",
			spec.ReadOnlyRootfs, spec.DropAllCapabilities, spec.NoNewPrivileges, spec.Init)
	case spec.Network != NetworkNone && spec.Network != NetworkDefault:
		return nil, fmt.Errorf("create container: network mode %q is not %q or %q",
			spec.Network, NetworkDefault, NetworkNone)
	case spec.CPUs == "" || spec.Memory == "" || spec.PidsLimit <= 0 ||
		cpuReadsAsUnlimited(spec.CPUs) || memoryReadsAsUnlimited(spec.Memory):
		// No workload ever receives unlimited resources, including one that
		// requested nothing (§9.3). A quantity that parses to zero, a
		// negative or a non-number is unlimited to the daemon, which reads
		// `--cpus 0` and `--memory 0` as no limit, so it is refused here.
		return nil, fmt.Errorf("create container: resources must always be bounded "+
			"(cpus=%q memory=%q pids=%d)", spec.CPUs, spec.Memory, spec.PidsLimit)
	}

	seccomp, err := spoken.seccompFlags(spec.SeccompProfile)
	if err != nil {
		return nil, err
	}
	args := []string{
		"--name", name,
		"--read-only",
		"--cap-drop=ALL",
		"--security-opt", "no-new-privileges",
	}
	args = append(args, seccomp...)
	args = append(args,
		"--init",
		"--workdir", spec.WorkDir,
		"--network", spec.Network,
		"--cpus", dockerCPUs(spec.CPUs),
		"--memory", dockerMemory(spec.Memory),
		"--pids-limit", strconv.FormatInt(spec.PidsLimit, 10),
	)
	args = append(args, spoken.cliFlags()...)
	if spec.StopTimeoutSeconds > 0 {
		args = append(args, "--stop-timeout", strconv.Itoa(spec.StopTimeoutSeconds))
	}
	if spec.RunAsOrker {
		// The §3.1 escape hatch in this dialect's spelling. Under a user
		// namespace the dialect cannot keep (a rootless Docker daemon,
		// userns-remap) the uid is another host user, which the boot probe
		// finds.
		args = append(args, spoken.runAsOrkerFlags()...)
	}
	if spec.TmpfsPath != "" {
		// noexec is spelled rather than left to the runtime: Docker adds it to
		// a --tmpfs by itself and Podman does not.
		args = append(args, "--tmpfs",
			fmt.Sprintf("%s:rw,nosuid,nodev,noexec,size=%d", spec.TmpfsPath, spec.TmpfsBytes))
	}
	// /run and /var/tmp get a read-only tmpfs of their own only on an engine
	// whose --read-only leaves them writable (readOnlyTmpfsFlags says why not
	// on Docker), and the boot probe checks both on every engine.
	args = append(args, spoken.readOnlyTmpfsFlags()...)

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

// plainArgument reports whether s can sit among the runtime's arguments as an
// operand: it does not begin with "-" and holds no space or control character.
func plainArgument(s string) bool {
	if s == "" || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// plainMountPath reports whether a path can sit in a --mount value, which is a
// comma-separated list of key=value options: a comma would add an option
// (readonly=false, a second source) and a control character would end it.
func plainMountPath(path string) bool {
	for i := 0; i < len(path); i++ {
		if path[i] < ' ' || path[i] == 0x7f || path[i] == ',' {
			return false
		}
	}
	return true
}

func checkMount(m Mount) error {
	if m.Source == "" || !strings.HasPrefix(m.Source, "/") {
		return fmt.Errorf("mount source %q must be an absolute Orker-created path", m.Source)
	}
	if !strings.HasPrefix(m.Destination, "/") {
		return fmt.Errorf("mount destination %q must be absolute", m.Destination)
	}
	if !plainMountPath(m.Source) || !plainMountPath(m.Destination) {
		return fmt.Errorf("mount %q to %q: a path holds no comma or control character, which would add an option to the mount",
			m.Source, m.Destination)
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
	answer, err := d.control(ctx, "inspect", "--format", "{{.State.Status}}", string(id))
	if err != nil {
		return false, fmt.Errorf("start container %s: observe its state: %w", id, err)
	}
	if answer.code != 0 {
		return false, nil
	}
	// Named on both sides (runtime_dialect.go). A state on neither list is
	// not read as started: a Wait that raced a container that never started
	// fabricates a success, so an unknown state keeps the gate polling to
	// its deadline.
	status := strings.TrimSpace(answer.stdout)
	return startedStates[status] && !preStartStates[status], nil
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
	answer, err := d.control(ctx, "kill", "--signal", string(sig), string(id))
	if err != nil {
		return err
	}
	if answer.code != 0 {
		said := answer.said()
		if d.grammar().notRunning(said) {
			// Signalling a container that already exited is the normal race
			// between the grace timer and a well-behaved workload, not a
			// failure.
			return nil
		}
		return fmt.Errorf("signal %s %s: docker exited %d: %s", sig, id, answer.code, said)
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
	said := &boundedBuffer{limit: 4096}
	cmd := exec.CommandContext(ctx, bin, "wait", string(id))
	cmd.Stdout = &out
	cmd.Stderr = said
	if err := cmd.Run(); err != nil {
		return ExitStatus{}, fmt.Errorf("wait for container %s: %w: %s", id, err, strings.TrimSpace(said.String()))
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
	answer, err := d.control(ctx, "inspect", "--format", "{{.State.OOMKilled}}", string(id))
	if err != nil || answer.code != 0 {
		return false
	}
	return strings.TrimSpace(answer.stdout) == "true"
}

// Remove force-removes the container. Callers pass a DETACHED context: cleanup
// must survive the cancellation that caused it.
//
// It kills the container before it removes it. `rm --force` on a running
// container is not a kill on every runtime: Podman's, behind its CLI and its
// Docker-compatible socket alike, stops the container and waits out its stop
// timeout first — 30 s for a workload that ignores SIGTERM, which is the
// whole of controlTimeout — so a removal could time out, and a boot reap of
// such an orphan cost 30 s each. The kill's own answer decides nothing: a
// container that is not running, or already gone, is what most removals
// meet, and whatever the kill said, the removal's answer is the one reported.
//
// The kill has a short budget of its own (removeKillTimeout) and the removal
// keeps the whole controlTimeout it had before the kill came first: under
// load a kill that ate into a shared budget left the removal too little, and
// a removal cut off at its deadline leaves the daemon removing a container its
// caller has already given up on.
func (d *DockerRuntime) Remove(ctx context.Context, id ContainerID) error {
	killCtx, cancel := context.WithTimeout(ctx, removeKillTimeout)
	_, _ = d.control(killCtx, "kill", "--signal", string(SIGKILL), string(id))
	cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	answer, err := d.control(ctx, "rm", "--force", "--volumes", string(id))
	if err != nil {
		return err
	}
	if answer.code != 0 && !d.grammar().gone(answer.said()) {
		return fmt.Errorf("remove container %s: docker exited %d: %s", id, answer.code, answer.said())
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
	answer, err := d.control(ctx, "ps", "--all", "--quiet",
		"--filter", "label=ork.orker="+orkerID)
	if err != nil {
		return 0, err
	}
	if answer.code != 0 {
		return 0, fmt.Errorf("reap: list containers: docker exited %d: %s", answer.code, answer.said())
	}
	var (
		removed  int
		failures []error
	)
	for _, id := range strings.Fields(answer.stdout) {
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

// endpointVars point a CLI at a daemon or at a container store: the docker
// CLI's host, context and configuration directory (which holds its current
// context), and the podman CLI's remote connection and storage.
var endpointVars = []string{"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG",
	"CONTAINER_HOST", "CONTAINER_CONNECTION", "CONTAINERS_STORAGE_CONF"}

// Endpoint names, without asking it, the daemon this runtime's commands
// reach: the CLI's grammar and binary, and every variable that points either
// CLI elsewhere. `orkctl task run` keys the containers of a run that is gone
// on it, so one run never settles the leftovers of a run on another daemon.
// A setting that does not parse is named as it was given.
func (d *DockerRuntime) Endpoint() string {
	cli, err := d.cli()
	if err != nil {
		cli = "unparsed:" + d.CLI + "|" + os.Getenv(CLIVar)
	}
	bin, err := d.bin()
	if err != nil {
		bin = "unparsed:" + d.Bin + "|" + os.Getenv(DockerBinVar)
	}
	parts := []string{"cli=" + cli, "bin=" + bin}
	for _, name := range endpointVars {
		parts = append(parts, name+"="+os.Getenv(name))
	}
	return strings.Join(parts, "\n")
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

	spoken, err := d.dialect(ctx)
	if err != nil {
		// What the CLI setting alone says, so a transient failure does not
		// change what a pool reads about this Orker's posture.
		grammar := d.grammar()
		return RuntimeInfo{Name: grammar.engine, CLI: grammar.cli, Seccomp: grammar.seccompName()}, err
	}
	// The engine's name, not the CLI's: a docker CLI against Podman's socket
	// runs every container on Podman.
	info := RuntimeInfo{Name: spoken.engine, CLI: spoken.cli, Rootless: spoken.rootless, Seccomp: spoken.seccompName()}
	answer, err := d.control(ctx, "version", "--format", "{{.Server.Version}}")
	if err != nil {
		return info, err
	}
	if answer.code != 0 {
		return info, fmt.Errorf("%s version: exited %d: %s", spoken.cli, answer.code, answer.said())
	}
	info.Version = strings.TrimSpace(answer.stdout)
	d.mu.Lock()
	kept := info
	d.described = &kept
	d.mu.Unlock()
	return info, nil
}

// RunBootProbe runs the boot probe's container once, to its end, and returns
// what it wrote on stdout. The spec is built by the dispatcher exactly as a
// task's is (bootProbeSpec), and its flags are rendered by the same function
// a task's `create` is, so what the daemon refuses or weakens here it would
// refuse or weaken for every task. What the output says is judged by the
// caller, not here: that judgement is about Linux, not about this CLI.
//
// It has four ways to fail, each its own thing: the probe's image is not
// available (ErrProbeImageUnavailable); the daemon was too slow
// (ErrProbeTimedOut): it did not answer the image inspect within the probe's
// budget, or did not start the container within it on either of two
// attempts, whether it said nothing or said its own deadline had passed; the
// daemon refused a bind in its own words (ErrNotBindable); and the daemon
// refused the container for any other reason within the budget, or the
// container exited non-zero (ErrPostureRefused). An Orker that is stopped
// meanwhile gets the context's own error.
func (d *DockerRuntime) RunBootProbe(ctx context.Context, spec ContainerSpec) (string, error) {
	// Every step of the probe that waits on the daemon is bounded by the
	// probe's budget and not by controlTimeout, which is meant for
	// bookkeeping: under load a daemon has needed more than 30 s to answer
	// even the cheapest question, and a daemon that is silent has told the
	// probe nothing about the image or the root.
	budget := d.bindProbeTimeout()
	image := spec.Image

	// The engine first, because the argument vector depends on it. A CLI
	// setting that names no CLI, or an engine that cannot be identified, is
	// a runtime that cannot be given a task's posture.
	spoken, overran, err := d.dialectWithin(ctx, budget)
	switch {
	case ctx.Err() != nil:
		return "", fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
	case overran:
		return "", probeTimedOut(fmt.Sprintf("say which engine it is and whether it is rootless (`version`, `info`) within %s", budget), nil, nil)
	case err != nil:
		return "", fmt.Errorf("%w: %w", ErrPostureRefused, err)
	}
	// A spec this CLI cannot render is refused before anything waits on the
	// daemon: no retry changes it.
	if _, err := dockerContainerFlags(spec, containerName(spec.Labels["ork.task"]), spoken); err != nil {
		return "", fmt.Errorf("%w: %w", ErrPostureRefused, err)
	}

	// The probe image is fetched first, under the PULL timeout rather than the
	// probe run's budget an implicit pull would spend, and a failure to fetch
	// it is its own error: a slow or rate-limited registry is not a work
	// root the daemon cannot bind, and naming it as one sends an operator to
	// the wrong fix.
	present, overran, err := d.imagePresent(ctx, budget, image)
	switch {
	case ctx.Err() != nil:
		return "", fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
	case overran:
		return "", probeTimedOut(fmt.Sprintf("answer `docker image inspect %s` within %s", image, budget), nil, nil)
	case err != nil:
		return "", fmt.Errorf("%w: the bind probe image %s: %w", ErrProbeImageUnavailable, image, err)
	}
	if !present {
		answer, err := d.controlWithin(ctx, d.pullTimeout(), "pull", "--quiet", image)
		if err != nil {
			return "", fmt.Errorf("%w: the bind probe image %s could not be pulled: %w",
				ErrProbeImageUnavailable, image, err)
		}
		if answer.code != 0 {
			return "", fmt.Errorf("%w: the bind probe image %s could not be pulled: docker exited %d: %s",
				ErrProbeImageUnavailable, image, answer.code, answer.said())
		}
	}

	// The daemon quotes the paths it was given, and none of them is a word
	// about the daemon's own deadlines: neither a bind's source nor the probe
	// directory and the work root it lies in.
	var sources []string
	for _, m := range spec.Mounts {
		sources = append(sources, m.Source, filepath.Dir(m.Source), filepath.Dir(filepath.Dir(m.Source)))
	}

	// The run is the one step of the probe that waits on the daemon to create,
	// start and tear down a container. An attempt that overruns the budget, or
	// that the daemon ends with a deadline error of its own, is tried once
	// more: whether the daemon is slow is a fact about the moment, and whether
	// the root binds is not. Any other failure within the budget is the
	// daemon's answer, and it is final.
	var attempts, leftovers []string
	for attempt := 1; attempt <= bindProbeAttempts; attempt++ {
		name := containerName(spec.Labels["ork.task"])
		flags, _ := dockerContainerFlags(spec, name, spoken)
		answer, overran, err := d.runWithin(ctx, budget, append([]string{"run", "--rm"}, flags...)...)
		said := truncateString(answer.said(), 2000)
		switch {
		case err == nil && answer.code == 0:
			return answer.stdout, nil
		case ctx.Err() != nil:
			// The Orker is stopping, which is neither a slow daemon nor an
			// unbindable root; Boot reports it as the interruption it is.
			return "", fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
		case overran:
			attempts = append(attempts, fmt.Sprintf("attempt %d: no answer within %s", attempt, budget))
		case err != nil:
			return "", fmt.Errorf("%w: run the boot probe's container: %w", ErrPostureRefused, err)
		case spoken.unbindable(said):
			return "", fmt.Errorf("%w: docker exited %d: %s", ErrNotBindable, answer.code, said)
		case saysDeadline(said, sources...):
			attempts = append(attempts, fmt.Sprintf("attempt %d: the daemon answered %q", attempt, truncateString(said, 300)))
		default:
			return "", fmt.Errorf("%w: the runtime would not run the boot probe's container, which is built as a task's is: "+
				"docker exited %d: %s", ErrPostureRefused, answer.code, said)
		}
		// The attempt may have left its container half-made, or the daemon may
		// still be starting it. Removing it is best effort, on a context of
		// its own, and a removal the daemon refuses is reported with the
		// refusal: that container is what an operator looks for.
		if err := d.Remove(context.WithoutCancel(ctx), ContainerID(name)); err != nil {
			leftovers = append(leftovers, err.Error())
		}
	}
	return "", probeTimedOut(fmt.Sprintf("start the probe container within %s on each of %d attempts", budget, bindProbeAttempts),
		attempts, leftovers)
}

// probeTimedOut is the refusal of a daemon that was too slow: what it did not
// do, what each attempt saw, and any container that could not be removed. It
// says in the same breath that the work root is not the finding, and names the
// setting that raises the budget.
func probeTimedOut(didNot string, attempts, leftovers []string) error {
	var msg strings.Builder
	fmt.Fprintf(&msg, "the container daemon did not %s", didNot)
	if len(attempts) > 0 {
		fmt.Fprintf(&msg, " (%s)", strings.Join(attempts, "; "))
	}
	fmt.Fprintf(&msg, ". That does not show that the work root cannot be bound, only that the daemon is slow or unresponsive; "+
		"raise %s if it is only slow", BindProbeTimeoutVar)
	if len(leftovers) > 0 {
		fmt.Fprintf(&msg, ". A probe container could not be removed and may remain: %s", strings.Join(leftovers, "; "))
	}
	return fmt.Errorf("%w: %s", ErrProbeTimedOut, msg.String())
}

// saysDeadline reports whether the daemon's answer to the probe's run says that
// a deadline of its own passed (its containerd could not start the shim in
// time, a socket timed out), which is a slow daemon and not a finding about
// the root. The daemon quotes the paths it was given, so they are cut out
// before the words are looked for: a work root named "timeouts" is not a
// daemon that timed out.
func saysDeadline(answer string, paths ...string) bool {
	for _, path := range paths {
		if path != "" {
			answer = strings.ReplaceAll(answer, path, "")
		}
	}
	lowered := strings.ToLower(answer)
	for _, words := range []string{"deadline exceeded", "timed out", "timeout"} {
		if strings.Contains(lowered, words) {
			return true
		}
	}
	return false
}

// bindProbeAttempts is how many times the boot bind probe runs its container
// when the daemon overruns the probe's budget: the first try and one retry.
const bindProbeAttempts = 2

// BindProbeImage is the image the boot probe runs: alpine 3.20, the base of
// the first-party images, at the index digest they pin.
//
// It is qualified with its registry because a short name is resolved by the
// runtime's own search list, and Podman's enforcing short-name mode with more
// than one search registry refuses one, which refused boot. It is pinned to a
// digest because it is what the probe's checks were measured with: the probe
// needs a POSIX shell, cat, sed and env, and a tag that moved could take any
// of them away. It runs no workload code and produces no commit.
const BindProbeImage = "docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc"

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
	if err != nil || !isByteCount(bytes) {
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
	if err != nil || !isPositiveFinite(cpus) {
		return quantity
	}
	return strconv.FormatFloat(cpus, 'f', -1, 64)
}

// cpuReadsAsUnlimited reports whether a cpu quantity parses to a number that
// is no limit to the runtime: zero, negative, NaN or infinite. A quantity
// that does not parse is not judged here; the daemon refuses it by name.
func cpuReadsAsUnlimited(quantity string) bool {
	cpus, err := parseCPU(quantity)
	return err == nil && !isPositiveFinite(cpus)
}

// memoryReadsAsUnlimited is cpuReadsAsUnlimited for memory: a quantity that
// parses to less than one byte, or to more than an int64 holds, is not a
// byte count the runtime can be handed.
func memoryReadsAsUnlimited(quantity string) bool {
	bytes, err := parseMemory(quantity)
	return err == nil && !isByteCount(bytes)
}
