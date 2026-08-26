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
// `output`: exit_code, image, image_pulled, container, timed_out, duration_ms.
// IT CARRIES NO BYTES THE CONTAINER WROTE. An output payload is a Temporal
// payload — the activity result, the workflow's own state, the run's stored
// task output — and a task's output travels as artifacts, never inline
// (AGENTS.md §1.2). What the container printed reaches a reader two other
// ways, both of them complete where a truncated string never was: the live
// log stream, and the byte-exact capture artifacts named by
// capture_stdout_as / capture_stderr_as.
//
// Live output: both streams reach the task's log stream WHILE the container
// runs, framed into lines, with stderr carrying plugin.StderrMarker so the two
// stay apart in one stream (see streamSet and docs/PLUGINS.md).
//
// A FAILURE carries no payload and quotes nothing the container wrote, for the
// same reason and one worse: an error travels into Temporal history AND onto
// the event stream, once per attempt. It names the exit code and where the
// output is instead (runFailure). It DOES carry the captures the run produced
// — the output of a failing container is exactly what an engineer opens the
// run to read, and the Orker uploads them before it fails the task.
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
	// MaxDiagnosticHead bounds the head of each stream the plugin keeps in
	// memory.
	//
	// It is NOT a payload field and never leaves this package: the only reader
	// is runFailure, which recognizes one daemon-level cause from it
	// (invisibleBindSource) and quotes none of it. The bytes themselves go to
	// the capture files and the log stream, which is where a task's output
	// belongs (AGENTS.md §1.2).
	MaxDiagnosticHead = 8 << 10

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
		// No stdout / stderr here, and none on the payload: an output payload
		// is a Temporal payload, and a task's output travels as artifacts
		// (AGENTS.md §1.2). A downstream task reads a container's output
		// through `artifact "<task>/<capture-name>"`, and a person reads it
		// with `orkctl run logs`.
		OutputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"exit_code":    map[string]any{"type": "number"},
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

	streams, err := newStreams(ec.WorkDir, ec.Log)
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
	// Flush each stream's final unterminated line BEFORE the exit code is
	// interpreted, so a failing container's last words are in the log stream
	// whatever the plugin decides about the failure.
	streams.closeLive()
	if flushErr := streams.flush(); flushErr != nil && err == nil {
		return nil, flushErr
	}
	if err != nil {
		return nil, err
	}

	// The captures are named BEFORE the exit code is interpreted, because a
	// failing container's output is exactly the output somebody opens the run
	// to read. Building them after the failure returned is what lost them:
	// the files existed, the author had named them, and the Orker's deferred
	// workdir cleanup removed them unread.
	produced := make([]plugin.ProducedArtifact, 0, 2)
	if name := strings.TrimSpace(configString(ec.Config, "capture_stdout_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: streams.stdoutPath})
	}
	if name := strings.TrimSpace(configString(ec.Config, "capture_stderr_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: streams.stderrPath})
	}

	if outcome.exitCode != 0 {
		// A result AND an error: the error fails the task and is all that
		// reaches history, while the captures still reach the store. The
		// Orker uploads them before it returns the failure
		// (internal/orker/activity.go).
		log("container: %s exited %d in %s with %d artifact(s)",
			req.name, outcome.exitCode, outcome.duration, len(produced))
		return &plugin.Result{Artifacts: produced}, p.runFailure(req, outcome, streams)
	}

	// No stdout / stderr: this payload is a Temporal payload, and a task's
	// output travels as artifacts (AGENTS.md §1.2). Everything here is
	// something only the plugin knows.
	output := plugin.Payload{
		"exit_code":    float64(outcome.exitCode),
		"image":        image,
		"image_pulled": pulled,
		"container":    req.name,
		"timed_out":    outcome.timedOut,
		"duration_ms":  float64(outcome.duration.Milliseconds()),
	}
	log("container: %s exited 0 in %s with %d artifact(s)", req.name, outcome.duration, len(produced))
	return &plugin.Result{Output: output, Artifacts: produced}, nil
}

// runFailure explains a non-zero exit and decides whether retrying could ever
// change it. Docker reserves three codes for its own failures; everything else
// is the container's own status.
//
// IT QUOTES NOTHING THE CONTAINER WROTE. The error this returns is the one
// thing here that leaves the Orker: Temporal records it as the activity's
// failure, the workflow copies it onto TaskRun.Error and the TASK_FAILED
// event, and the run's own failure copies it again onto Run.Error and
// RUN_FAILED (internal/engine/workflow.go) — history and the event stream
// both, once per attempt. A task's output is bytes, and bytes travel as stored
// objects (AGENTS.md §1.4): the whole of both streams is in the task's log
// stream, live and afterwards, and byte-exact in the capture artifacts. So a
// failure names what only it knows — the image, the exit code, the kill, the
// one daemon-level cause worth translating — and says where the output is.
//
// That is not less actionable than quoting a head of stderr was: the docker
// CLI's own diagnostics are written to the same stderr the plugin streams, so
// "docker could not start it" and the daemon's sentence about why are one
// `orkctl run logs` apart, and the artifacts hold every byte rather than the
// first 8 KiB.
func (p *Plugin) runFailure(req runRequest, outcome runOutcome, streams *streamSet) error {
	// The head buffers are READ here and never quoted: the daemon-level cause
	// below is recognized from them, and what leaves this function is the
	// plugin's own sentence about it.
	said := streams.stderrHead.String() + streams.stdoutHead.String()

	if outcome.timedOut {
		// A budget the container blew once it will blow again: the task is the
		// author's to fix, not the scheduler's to retry.
		return plugin.Permanent(fmt.Errorf(
			"container: %s exceeded its timeout after %s and was killed (exit code %d). %s",
			req.image, outcome.duration.Round(time.Millisecond), outcome.exitCode, whereTheOutputIs))
	}
	switch outcome.exitCode {
	case exitDockerFailed:
		if strings.Contains(said, invisibleBindSource) {
			// The daemon runs somewhere that cannot see the Orker's workdir —
			// a VM sharing only part of the host, or a remote daemon. No retry
			// fixes that, and the operator needs to hear which knob moves it.
			return plugin.Permanent(fmt.Errorf(
				"container: the docker daemon cannot see a path this task mounts, so it refused to start %s. "+
					"The Orker's work directory must be on a filesystem the daemon shares; "+
					"point %s at one. %s",
				req.image, workDirHint, whereTheOutputIs))
		}
		// The container never started: the daemon is down, the image is
		// unreachable, or the host is out of resources. Leave it retryable.
		return fmt.Errorf("container: docker could not start %s (exit code %d). %s",
			req.image, outcome.exitCode, whereTheOutputIs)
	case exitCommandNotExecutable, exitCommandNotFound:
		return plugin.Permanent(fmt.Errorf(
			"container: %s cannot run the requested command (exit code %d). %s",
			req.image, outcome.exitCode, whereTheOutputIs))
	default:
		return plugin.Permanent(fmt.Errorf("container: %s exited with code %d. %s",
			req.image, outcome.exitCode, whereTheOutputIs))
	}
}

// whereTheOutputIs ends every failure message. A failure a person cannot act
// on is no better than one that carries the bytes it must not carry, so the
// sentence that replaces the quote is the one that says where to look.
const whereTheOutputIs = "What it printed is in the task's log " +
	"(`orkctl run logs <run-id> --task <task-id>`; stderr lines carry the `[stderr] ` marker), " +
	"and byte-exact in its capture_stdout_as / capture_stderr_as artifacts when the task names them."

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

// streamSet holds the container's output streams. Each is written three ways,
// and every one of them is load-bearing:
//
//   - a FILE, so the captured artifact is the container's exact bytes;
//   - a bounded HEAD, which the plugin reads to recognize one daemon-level
//     failure and never puts on a payload or in an error (MaxDiagnosticHead);
//   - a LINE WRITER, so the application's own output reaches the task's live
//     log stream WHILE the container runs rather than only afterwards (M11.1).
//
// The live writer is a third reader of the same bytes, never a replacement for
// either of the other two. Both fds feed ONE logger — the live view is the
// interleaved console a person expects — and stderr carries
// plugin.StderrMarker per line so the two are still told apart. The byte-exact
// separation callers rely on stays where it always was: the two artifacts.
type streamSet struct {
	stdoutPath string
	stderrPath string
	stdoutFile *os.File
	stderrFile *os.File
	stdoutHead *headBuffer
	stderrHead *headBuffer
	stdoutLive *plugin.LineWriter
	stderrLive *plugin.LineWriter
}

// newStreams opens the capture files and builds the live writers. A nil log is
// a plugin with nowhere to stream to, which the writers accept and discard.
func newStreams(workDir string, log plugin.Logger) (*streamSet, error) {
	dir := filepath.Join(workDir, internalDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("container: create stream dir: %w", err)
	}
	set := &streamSet{
		stdoutPath: filepath.Join(dir, "stdout"),
		stderrPath: filepath.Join(dir, "stderr"),
		stdoutHead: &headBuffer{limit: MaxDiagnosticHead},
		stderrHead: &headBuffer{limit: MaxDiagnosticHead},
		stdoutLive: plugin.NewLineWriter(log, ""),
		stderrLive: plugin.NewLineWriter(log, plugin.StderrMarker),
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

func (s *streamSet) stdout() io.Writer {
	return io.MultiWriter(s.stdoutFile, s.stdoutHead, s.stdoutLive)
}

func (s *streamSet) stderr() io.Writer {
	return io.MultiWriter(s.stderrFile, s.stderrHead, s.stderrLive)
}

// closeLive emits each stream's final line, the one an application ended
// without a terminator. It is idempotent, so the explicit call before the exit
// code is read and the one on the way out are the same call.
func (s *streamSet) closeLive() {
	s.stdoutLive.Close()
	s.stderrLive.Close()
}

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
	s.closeLive()
	_ = s.stdoutFile.Close()
	_ = s.stderrFile.Close()
}
