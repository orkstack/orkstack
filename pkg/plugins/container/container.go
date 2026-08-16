// Package containerplugin implements the builtin container plugin: it runs one
// task inside a Docker container and captures what that container produced.
//
// `with`:
//
//	op                  run | pull (default run)
//	image               image reference, e.g. alpine:3.20 (required)
//	command             one-liner run by the image's `sh -c`, or a list used as argv
//	workdir             working directory inside the container
//	env                 map of environment variables
//	mounts              list of bind mounts (below)
//	pull                missing | always | never (default missing)
//	capture_stdout_as   artifact name for the full stdout stream
//	capture_stderr_as   artifact name for the full stderr stream
//	timeout_seconds     wall-clock budget; an over-running container is killed
//
// `op` is the generic operation selector every OrkStack plugin shares (see
// docs/PLUGIN_DESIGN.md); an unknown `op` fails the task permanently.
//
//	run     the container is the work. A non-zero exit fails the task.
//	pull    only fetch the image, so a later task starts without the download.
//
// A mount carries a container `path` plus exactly one source:
//
//	mounts:
//	  - artifact: "fetch/body"   # an upstream artifact, always read-only
//	    path: /input/body.json
//	  - ephemeral: scratch       # a fresh directory, created before the run
//	    path: /scratch           # and removed after it
//
// `output`: exit_code, stdout, stderr, image, image_pulled, container,
// timed_out, duration_ms. The stdout/stderr strings are truncated to
// MaxCapturedOutput bytes; the full streams are available as artifacts through
// capture_stdout_as / capture_stderr_as.
//
// Security (AGENTS.md §9): the plugin drives the Docker CLI through an argv
// slice and never builds a shell command line, so no config value can inject a
// flag or a second command. `command` given as a string is the one place a
// shell is involved, and that shell is the container's own, inside the
// container's isolation boundary.
package containerplugin

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

const (
	// MaxCapturedOutput bounds the stdout/stderr strings placed on the task
	// output payload. Bytes never belong inline, so the full streams are only
	// available as artifacts.
	MaxCapturedOutput = 8 << 10

	// DefaultKillGrace is how long a container killed for exceeding its budget
	// may take to disappear before the Docker client is dropped too.
	DefaultKillGrace = 10 * time.Second

	// internalDir holds the plugin's own files inside the task workdir: the
	// captured streams and the ephemeral mounts.
	internalDir = ".ork-container"

	// The operations `op` selects.
	opRun  = "run"
	opPull = "pull"

	// Pull policies.
	pullMissing = "missing"
	pullAlways  = "always"
	pullNever   = "never"

	// invisibleBindSource is what the daemon says when it cannot see a bind
	// mount's host path. It is the one docker-level failure worth translating,
	// because the cause (a work directory the daemon does not share) is far
	// from the message.
	invisibleBindSource = "bind source path does not exist"
	// workDirHint names the variable that moves the Orker's work directory.
	// It is spelled out rather than imported: pkg/plugins must not depend on
	// internal/orker.
	workDirHint = "ORK_ORKER_WORKDIR"
)

// Plugin runs a task inside a Docker container.
type Plugin struct {
	// DockerBin overrides which Docker CLI is driven. Empty means
	// ORK_DOCKER_BIN, then `docker` from PATH.
	DockerBin string
	// KillGrace overrides how long a killed container may take to disappear
	// before the Docker client is dropped. Zero means DefaultKillGrace.
	KillGrace time.Duration
}

// New creates the container plugin.
func New() *Plugin { return &Plugin{} }

// Name is the DSL `plugin:` identifier.
func (p *Plugin) Name() string { return "container" }

// Describe returns discovery metadata.
func (p *Plugin) Describe() plugin.Descriptor {
	return plugin.Descriptor{
		Name:        "container",
		Version:     "0.1.0",
		Description: "Run a task inside a Docker container and capture its output streams as artifacts.",
		InputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"op":      map[string]any{"type": "string", "enum": []any{opRun, opPull}, "default": opRun},
				"image":   map[string]any{"type": "string"},
				"command": map[string]any{"type": []any{"string", "array"}, "items": map[string]any{"type": "string"}},
				"workdir": map[string]any{"type": "string"},
				"env":     map[string]any{"type": "object"},
				"mounts": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"artifact":  map[string]any{"type": "string"},
							"ephemeral": map[string]any{"type": "string"},
							"path":      map[string]any{"type": "string"},
							"read_only": map[string]any{"type": "boolean"},
						},
						"required": []any{"path"},
					},
				},
				"pull":              map[string]any{"type": "string", "enum": []any{pullMissing, pullAlways, pullNever}, "default": pullMissing},
				"capture_stdout_as": map[string]any{"type": "string"},
				"capture_stderr_as": map[string]any{"type": "string"},
				"timeout_seconds":   map[string]any{"type": "number"},
			},
			"required": []any{"image"},
		},
		OutputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"exit_code":    map[string]any{"type": "number"},
				"stdout":       map[string]any{"type": "string"},
				"stderr":       map[string]any{"type": "string"},
				"image":        map[string]any{"type": "string"},
				"image_pulled": map[string]any{"type": "boolean"},
				"container":    map[string]any{"type": "string"},
				"timed_out":    map[string]any{"type": "boolean"},
				"duration_ms":  map[string]any{"type": "number"},
			},
		},
	}
}

// Execute dispatches on `op`.
func (p *Plugin) Execute(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	op := strings.TrimSpace(configString(ec.Config, "op"))
	if op == "" {
		op = opRun
	}
	switch strings.ToLower(op) {
	case opRun:
		return p.execRun(ctx, ec)
	case opPull:
		return p.execPull(ctx, ec)
	default:
		return nil, plugin.Permanent(fmt.Errorf(
			"container: unknown op %q (want %s or %s)", op, opRun, opPull))
	}
}

// execPull fetches an image and stops there, so the task that runs it later
// starts without paying for the download.
func (p *Plugin) execPull(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	image, err := requiredImage(ec.Config)
	if err != nil {
		return nil, err
	}
	seconds, err := configSeconds(ec.Config, "timeout_seconds")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	timeout := time.Duration(seconds * float64(time.Second))

	log := logger(ec)
	log("container: pulling %s", image)
	if err := p.pullImageWithTimeout(ctx, image, timeout); err != nil {
		return nil, err
	}
	return &plugin.Result{Output: plugin.Payload{
		"image":        image,
		"image_pulled": true,
	}}, nil
}

// execRun is the whole task lifecycle: acquire the image, prepare the mounts,
// run the container under its budget, capture both streams, and release
// everything the run created.
func (p *Plugin) execRun(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	image, err := requiredImage(ec.Config)
	if err != nil {
		return nil, err
	}
	command, err := configCommand(ec.Config)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	declaredEnv, err := configStringMap(ec.Config, "env")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	if err := validateEnv(declaredEnv); err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	env := expandEnv(declaredEnv)
	policy, err := pullPolicy(ec.Config)
	if err != nil {
		return nil, err
	}
	mountSpecs, err := parseMounts(ec.Config)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	seconds, err := configSeconds(ec.Config, "timeout_seconds")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("container: %w", err))
	}
	timeout := time.Duration(seconds * float64(time.Second))

	log := logger(ec)

	pulled, err := p.acquireImage(ctx, image, policy, timeout, log)
	if err != nil {
		return nil, err
	}

	scratchRoot := filepath.Join(ec.WorkDir, internalDir, ephemeralDir)
	mounts, err := resolveMounts(ctx, ec, mountSpecs, scratchRoot)
	// Scratch directories are released even when a later mount fails to
	// resolve, so a half-built run leaves nothing behind.
	defer cleanEphemeral(mounts, log)
	if err != nil {
		return nil, err
	}

	streams, err := newStreams(ec.WorkDir)
	if err != nil {
		return nil, err
	}
	defer streams.close()

	req := runRequest{
		name:    containerName(ec.TaskID),
		image:   image,
		command: command,
		workDir: strings.TrimSpace(configString(ec.Config, "workdir")),
		env:     env,
		mounts:  mounts,
		labels:  map[string]string{"ork.task": ec.TaskID},
	}
	for _, m := range mounts {
		access := "rw"
		if m.readOnly {
			access = "ro"
		}
		log("container: mount %s -> %s (%s, %s)", m.hostPath, m.containerPath, access, m.source)
	}
	log("container: running %s as %s", image, req.name)

	outcome, err := p.runContainer(ctx, req, streams.stdout(), streams.stderr(), timeout, log)
	if flushErr := streams.flush(); flushErr != nil && err == nil {
		return nil, flushErr
	}
	if err != nil {
		return nil, err
	}

	output := plugin.Payload{
		"exit_code":    float64(outcome.exitCode),
		"stdout":       streams.stdoutHead.String(),
		"stderr":       streams.stderrHead.String(),
		"image":        image,
		"image_pulled": pulled,
		"container":    req.name,
		"timed_out":    outcome.timedOut,
		"duration_ms":  float64(outcome.duration.Milliseconds()),
	}

	if outcome.exitCode != 0 {
		return nil, p.runFailure(req, outcome, streams)
	}

	produced := make([]plugin.ProducedArtifact, 0, 2)
	if name := strings.TrimSpace(configString(ec.Config, "capture_stdout_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: streams.stdoutPath})
	}
	if name := strings.TrimSpace(configString(ec.Config, "capture_stderr_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: streams.stderrPath})
	}

	log("container: %s exited 0 in %s with %d artifact(s)", req.name, outcome.duration, len(produced))
	return &plugin.Result{Output: output, Artifacts: produced}, nil
}

// runFailure explains a non-zero exit and decides whether retrying could ever
// change it. Docker reserves three codes for its own failures; everything else
// is the container's own status.
func (p *Plugin) runFailure(req runRequest, outcome runOutcome, streams *streamSet) error {
	tail := strings.TrimSpace(streams.stderrHead.String())
	if tail == "" {
		tail = strings.TrimSpace(streams.stdoutHead.String())
	}
	if tail == "" {
		tail = "(no output)"
	}

	if outcome.timedOut {
		// A budget the container blew once it will blow again: the task is the
		// author's to fix, not the scheduler's to retry.
		return plugin.Permanent(fmt.Errorf(
			"container: %s exceeded its timeout after %s and was killed (exit code %d): %s",
			req.image, outcome.duration.Round(time.Millisecond), outcome.exitCode, tail))
	}
	switch outcome.exitCode {
	case exitDockerFailed:
		if strings.Contains(tail, invisibleBindSource) {
			// The daemon runs somewhere that cannot see the Orker's workdir —
			// a VM sharing only part of the host, or a remote daemon. No retry
			// fixes that, and the operator needs to hear which knob moves it.
			return plugin.Permanent(fmt.Errorf(
				"container: the docker daemon cannot see a path this task mounts, so it refused to start %s. "+
					"The Orker's work directory must be on a filesystem the daemon shares; "+
					"point %s at one. Docker said: %s",
				req.image, workDirHint, tail))
		}
		// The container never started: the daemon is down, the image is
		// unreachable, or the host is out of resources. Leave it retryable.
		return fmt.Errorf("container: docker could not start %s (exit code %d): %s",
			req.image, outcome.exitCode, tail)
	case exitCommandNotExecutable, exitCommandNotFound:
		return plugin.Permanent(fmt.Errorf(
			"container: %s cannot run the requested command (exit code %d): %s",
			req.image, outcome.exitCode, tail))
	default:
		return plugin.Permanent(fmt.Errorf("container: %s exited with code %d: %s",
			req.image, outcome.exitCode, tail))
	}
}

// acquireImage applies the pull policy and reports whether a pull happened.
func (p *Plugin) acquireImage(
	ctx context.Context,
	image, policy string,
	timeout time.Duration,
	log plugin.Logger,
) (bool, error) {
	if policy == pullAlways {
		log("container: pulling %s (pull: always)", image)
		if err := p.pullImageWithTimeout(ctx, image, timeout); err != nil {
			return false, err
		}
		return true, nil
	}

	present, err := p.imagePresent(ctx, image)
	if err != nil {
		return false, err
	}
	if present {
		log("container: using local image %s", image)
		return false, nil
	}
	if policy == pullNever {
		return false, plugin.Permanent(fmt.Errorf(
			"container: image %s is not present locally and `pull: never` forbids fetching it", image))
	}
	log("container: %s is not present locally; pulling it", image)
	if err := p.pullImageWithTimeout(ctx, image, timeout); err != nil {
		return false, err
	}
	return true, nil
}

func requiredImage(config map[string]any) (string, error) {
	image := strings.TrimSpace(configString(config, "image"))
	switch {
	case image == "":
		return "", plugin.Permanent(fmt.Errorf("container: `image` is required"))
	case strings.HasPrefix(image, "-"):
		// Docker would read this as one of its own flags.
		return "", plugin.Permanent(fmt.Errorf("container: `image` %q may not start with '-'", image))
	}
	return image, nil
}

func pullPolicy(config map[string]any) (string, error) {
	policy := strings.ToLower(strings.TrimSpace(configString(config, "pull")))
	switch policy {
	case "":
		return pullMissing, nil
	case pullMissing, pullAlways, pullNever:
		return policy, nil
	default:
		return "", plugin.Permanent(fmt.Errorf(
			"container: unknown pull policy %q (want %s, %s or %s)",
			policy, pullMissing, pullAlways, pullNever))
	}
}

func logger(ec *plugin.ExecContext) plugin.Logger {
	if ec.Log != nil {
		return ec.Log
	}
	return func(string, ...any) {}
}

// streamSet holds the container's output streams: a file each, so the captured
// artifact is the exact bytes, plus a bounded head for the output payload.
type streamSet struct {
	stdoutPath string
	stderrPath string
	stdoutFile *os.File
	stderrFile *os.File
	stdoutHead *headBuffer
	stderrHead *headBuffer
}

func newStreams(workDir string) (*streamSet, error) {
	dir := filepath.Join(workDir, internalDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("container: create stream dir: %w", err)
	}
	set := &streamSet{
		stdoutPath: filepath.Join(dir, "stdout"),
		stderrPath: filepath.Join(dir, "stderr"),
		stdoutHead: &headBuffer{limit: MaxCapturedOutput},
		stderrHead: &headBuffer{limit: MaxCapturedOutput},
	}
	var err error
	if set.stdoutFile, err = os.Create(set.stdoutPath); err != nil {
		return nil, fmt.Errorf("container: create stdout file: %w", err)
	}
	if set.stderrFile, err = os.Create(set.stderrPath); err != nil {
		_ = set.stdoutFile.Close()
		return nil, fmt.Errorf("container: create stderr file: %w", err)
	}
	return set, nil
}

func (s *streamSet) stdout() io.Writer { return io.MultiWriter(s.stdoutFile, s.stdoutHead) }
func (s *streamSet) stderr() io.Writer { return io.MultiWriter(s.stderrFile, s.stderrHead) }

func (s *streamSet) flush() error {
	if err := s.stdoutFile.Sync(); err != nil {
		return fmt.Errorf("container: flush stdout: %w", err)
	}
	if err := s.stderrFile.Sync(); err != nil {
		return fmt.Errorf("container: flush stderr: %w", err)
	}
	return nil
}

func (s *streamSet) close() {
	_ = s.stdoutFile.Close()
	_ = s.stderrFile.Close()
}
