package containerplugin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// DockerBinVar names the environment variable that overrides which Docker CLI
// the plugin drives. Empty means `docker` from PATH.
const DockerBinVar = "ORK_DOCKER_BIN"

// Docker's own exit codes, distinguished from the container's. `docker run`
// exits with the container's status unless the container never ran, so these
// three values decide whether a failure is the orkflow author's or the host's.
const (
	// exitDockerFailed means the daemon refused the request: it is down, the
	// image is unreachable, or a flag was rejected. Usually transient.
	exitDockerFailed = 125
	// exitCommandNotExecutable means the entrypoint exists but cannot be run.
	exitCommandNotExecutable = 126
	// exitCommandNotFound means the entrypoint is not in the image.
	exitCommandNotFound = 127
	// exitKilled is the conventional status of a process killed by SIGKILL
	// (128 + 9). The plugin reports it when it kills a container that outlived
	// its budget, even if the CLI never got to report a status.
	exitKilled = 137
)

// controlCommandTimeout bounds the short bookkeeping commands (kill, remove,
// inspect). They must not inherit the run's cancelled context or its budget:
// cleaning up after a kill is exactly when the context is already gone.
const controlCommandTimeout = 30 * time.Second

// pullImageTimeout is the maximum duration allowed for docker pull operations.
// Large images may take several minutes to download; 5 minutes is a reasonable
// default that applications can override via timeout_seconds or the env var.
const pullImageTimeout = 5 * time.Minute

// pullImageTimeoutVar names the environment variable that overrides the default
// pull timeout. It's a duration string (e.g. "10m", "600s").
const pullImageTimeoutVar = "ORK_DOCKER_PULL_TIMEOUT"

// dockerBin resolves the CLI to drive. An explicit path is used verbatim so a
// test can point the plugin at a stand-in; a bare name is looked up on PATH.
func (p *Plugin) dockerBin() (string, error) {
	bin := strings.TrimSpace(p.DockerBin)
	if bin == "" {
		bin = strings.TrimSpace(os.Getenv(DockerBinVar))
	}
	if bin == "" {
		bin = "docker"
	}
	if strings.ContainsRune(bin, os.PathSeparator) {
		return bin, nil
	}
	resolved, err := exec.LookPath(bin)
	if err != nil {
		// A missing CLI is the host's configuration, not a transient fault:
		// retrying on the same Orker cannot install Docker.
		return "", plugin.Permanent(fmt.Errorf(
			"container: docker CLI %q not found on PATH (set %s): %w", bin, DockerBinVar, err))
	}
	return resolved, nil
}

// control runs a short docker command and returns its exit code plus combined
// output. Only a failure to start the CLI is an error; a non-zero exit is data.
// Failures to start the CLI are errors; exit codes are data.
func (p *Plugin) control(ctx context.Context, args ...string) (int, string, error) {
	bin, err := p.dockerBin()
	if err != nil {
		return 0, "", err
	}
	ctx, cancel := context.WithTimeout(ctx, controlCommandTimeout)
	defer cancel()

	var combined bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	cmd.WaitDelay = 100 * time.Millisecond // prevent blocking on inherited pipes
	switch err := cmd.Run(); {
	case err == nil:
		return 0, combined.String(), nil
	case isExitError(err):
		return exitCodeOf(err), combined.String(), nil
	case ctx.Err() != nil:
		return 0, combined.String(), fmt.Errorf("container: docker %s: %w", args[0], ctx.Err())
	default:
		return 0, combined.String(), fmt.Errorf("container: docker %s: %w", args[0], err)
	}
}

// imagePresent reports whether the daemon already holds the image locally.
func (p *Plugin) imagePresent(ctx context.Context, image string) (bool, error) {
	code, _, err := p.control(ctx, "image", "inspect", "--format", "{{.Id}}", image)
	if err != nil {
		return false, err
	}
	return code == 0, nil
}

// pullImageWithTimeout fetches the image from its registry with an explicit
// timeout. If the timeout is zero, uses the default pull timeout.
// A pull failure stays retryable: registries rate-limit and networks blink.
func (p *Plugin) pullImageWithTimeout(ctx context.Context, image string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = pullImageTimeout
		if env := strings.TrimSpace(os.Getenv(pullImageTimeoutVar)); env != "" {
			if parsed, err := time.ParseDuration(env); err == nil {
				timeout = parsed
			}
		}
	}

	pullCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	code, output, err := p.controlWithContext(pullCtx, "pull", image)
	if err != nil {
		return err
	}
	if code != 0 {
		if pullCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("container: pull %s timed out after %v", image, timeout)
		}
		return fmt.Errorf("container: pull %s failed (docker exited %d): %s",
			image, code, strings.TrimSpace(output))
	}
	return nil
}

// pullImage fetches the image from its registry using the default timeout.
// A pull failure stays retryable: registries rate-limit and networks blink.
func (p *Plugin) pullImage(ctx context.Context, image string) error {
	return p.pullImageWithTimeout(ctx, image, 0)
}

// controlWithContext is like control() but does NOT override the context timeout;
// it uses the supplied context directly. This allows callerslike pullImageWithTimeout
// to impose a custom deadline.
func (p *Plugin) controlWithContext(ctx context.Context, args ...string) (int, string, error) {
	bin, err := p.dockerBin()
	if err != nil {
		return 0, "", err
	}

	var combined bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	cmd.WaitDelay = 100 * time.Millisecond // prevent blocking on inherited pipes
	switch err := cmd.Run(); {
	case err == nil:
		return 0, combined.String(), nil
	case isExitError(err):
		return exitCodeOf(err), combined.String(), nil
	case ctx.Err() != nil:
		return 0, combined.String(), fmt.Errorf("container: docker %s: %w", args[0], ctx.Err())
	default:
		return 0, combined.String(), fmt.Errorf("container: docker %s: %w", args[0], err)
	}
}

// removeContainer is the cleanup safety net. `docker run --rm` already removes
// a container that exited on its own; this catches the ones that did not, so a
// killed or abandoned run leaves nothing behind. A container that is already
// gone is not an error.
func (p *Plugin) removeContainer(ctx context.Context, name string) {
	_, _, _ = p.control(ctx, "rm", "--force", "--volumes", name)
}

// killContainer stops a container that outlived its budget. It deliberately
// runs on a context of its own: the run context is about to be abandoned.
func (p *Plugin) killContainer(name string) (int, string, error) {
	return p.control(context.Background(), "kill", "--signal", "KILL", name)
}

// runRequest is one `docker run` invocation.
type runRequest struct {
	name    string
	image   string
	command []string
	workDir string
	env     map[string]string
	mounts  []mount
	labels  map[string]string
}

// argv renders the request as one argv slice. Nothing is ever concatenated
// into a shell command line, so a value carrying spaces, quotes or newlines
// reaches the container exactly as written (AGENTS.md §9). Ordering is fixed —
// env and labels sorted by key, mounts in declaration order — so the command
// a task runs is reproducible and reviewable.
func (r runRequest) argv() []string {
	args := []string{"run", "--rm", "--name", r.name}
	for _, key := range sortedKeys(r.labels) {
		args = append(args, "--label", key+"="+r.labels[key])
	}
	if r.workDir != "" {
		args = append(args, "--workdir", r.workDir)
	}
	for _, key := range sortedKeys(r.env) {
		args = append(args, "--env", key+"="+r.env[key])
	}
	for _, m := range r.mounts {
		args = append(args, "--mount", m.bindSpec())
	}
	args = append(args, r.image)
	return append(args, r.command...)
}

// runOutcome is what one container run produced, whatever its fate.
type runOutcome struct {
	exitCode int
	timedOut bool
	duration time.Duration
}

// runContainer runs the container to completion, to its timeout, or to the
// cancellation of ctx, streaming both output streams to the writers.
//
// The timeout is enforced by the daemon, not by killing the CLI: a killed CLI
// leaves the container running. On expiry the plugin asks the daemon to
// SIGKILL the container and only drops the CLI if the daemon does not answer
// within the kill grace, so `docker run` still reports the container's status.
func (p *Plugin) runContainer(
	ctx context.Context,
	req runRequest,
	stdout, stderr io.Writer,
	timeout time.Duration,
	log plugin.Logger,
) (runOutcome, error) {
	bin, err := p.dockerBin()
	if err != nil {
		return runOutcome{}, err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	cmd := exec.CommandContext(runCtx, bin, req.argv()...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = p.killGrace()

	done := make(chan struct{})
	timedOut := make(chan struct{})
	// watcherDone is how the run joins its own timeout watcher. Nothing this
	// function starts may outlive it: the logger it writes to belongs to a
	// task attempt that is about to finish.
	watcherDone := make(chan struct{})
	if timeout <= 0 {
		close(watcherDone)
	} else {
		go func() {
			defer close(watcherDone)
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-time.After(timeout):
			}
			close(timedOut)
			log("container: %s exceeded its %s budget; killing it", req.name, timeout)
			if code, output, err := p.killContainer(req.name); err != nil {
				log("container: kill %s: %v", req.name, err)
			} else if code != 0 {
				log("container: kill %s exited %d: %s", req.name, code, strings.TrimSpace(output))
			}
			// The daemon normally makes `docker run` return within
			// milliseconds of the kill. If it does not, stop waiting for it.
			select {
			case <-done:
			case <-time.After(p.killGrace()):
				log("container: %s did not stop within the kill grace; dropping the docker client", req.name)
				cancelRun()
			}
		}()
	}

	started := time.Now()
	runErr := cmd.Run()
	close(done)
	elapsed := time.Since(started)
	<-watcherDone

	killed := false
	select {
	case <-timedOut:
		killed = true
	default:
	}

	// Cleanup runs on a live context: ctx may already be cancelled, and that is
	// precisely when a container is most likely to be left behind.
	p.removeContainer(context.WithoutCancel(ctx), req.name)

	switch {
	case runErr == nil:
		return runOutcome{exitCode: 0, duration: elapsed}, nil
	case !isExitError(runErr):
		return runOutcome{duration: elapsed}, fmt.Errorf("container: run docker: %w", runErr)
	}

	code := exitCodeOf(runErr)
	if killed {
		// A CLI dropped mid-flight reports no status of its own; the container
		// was SIGKILLed, so report what a SIGKILLed process reports.
		if code <= 0 {
			code = exitKilled
		}
		return runOutcome{exitCode: code, timedOut: true, duration: elapsed}, nil
	}
	if ctx.Err() != nil {
		return runOutcome{exitCode: code, duration: elapsed},
			fmt.Errorf("container: run cancelled: %w", ctx.Err())
	}
	return runOutcome{exitCode: code, duration: elapsed}, nil
}

// killGrace is how long a killed container may take to disappear before the
// docker client itself is dropped.
func (p *Plugin) killGrace() time.Duration {
	if p.KillGrace > 0 {
		return p.KillGrace
	}
	return DefaultKillGrace
}

// containerName builds a name that is unique per attempt and legal for Docker
// (`[a-zA-Z0-9][a-zA-Z0-9_.-]*`), so a stray container is traceable to its task.
func containerName(taskID string) string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		// crypto/rand does not fail in practice; a clock-derived suffix keeps
		// the run going if it ever does.
		return fmt.Sprintf("ork-%s-%d", safeNameComponent(taskID), time.Now().UnixNano())
	}
	return fmt.Sprintf("ork-%s-%s", safeNameComponent(taskID), hex.EncodeToString(suffix[:]))
}

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

func isExitError(err error) bool {
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
