// Package shellplugin implements the builtin shell plugin.
//
// `with`:
//
//	op                  run | eval (default run)
//	command             shell command run by `sh -c` in WorkDir (required)
//	env                 map of extra environment variables
//	capture_stdout_as   artifact name for the full stdout stream
//	capture_stderr_as   artifact name for the full stderr stream
//	capture_paths       list of globs, relative to WorkDir, captured as artifacts
//
// `op` is the generic operation selector every OrkStack plugin shares (see
// docs/PLUGIN_DESIGN.md); an unknown `op` fails the task permanently. The two
// ops differ in how they treat a non-zero exit:
//
//	run    the command is the work. A non-zero exit fails the task.
//	eval   the command is a question. Any exit succeeds; the answer is
//	       output.value (stdout with surrounding whitespace trimmed) and the
//	       caller branches on output.exit_code.
//
// `output`: exit_code, stdout, stderr, plus value for `eval`. The
// stdout/stderr strings are truncated to MaxCapturedOutput bytes; the full
// streams are available through capture_stdout_as / capture_stderr_as.
package shellplugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

const (
	// MaxCapturedOutput bounds the stdout/stderr strings placed on the task
	// output payload. Bytes never belong inline, so the full streams are only
	// available as artifacts.
	MaxCapturedOutput = 8 << 10

	// internalDir holds the plugin's own stream files. It is a dot-directory
	// and a directory, so capture_paths globs never pick it up.
	internalDir = ".ork-shell"

	// killDelay is how long a cancelled command may take to exit before its
	// pipes are closed and the process is killed.
	killDelay = 5 * time.Second

	// opRun and opEval are the operations `op` selects.
	opRun  = "run"
	opEval = "eval"
)

// Plugin runs a shell command in the task workdir.
type Plugin struct{}

// New creates the shell plugin.
func New() *Plugin { return &Plugin{} }

// Name is the DSL `plugin:` identifier.
func (p *Plugin) Name() string { return "shell" }

// Describe returns discovery metadata.
func (p *Plugin) Describe() plugin.Descriptor {
	return plugin.Descriptor{
		Name:        "shell",
		Version:     "0.1.0",
		Description: "Run a shell command in the task work directory and capture its output.",
		InputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"op":                map[string]any{"type": "string", "enum": []any{"run", "eval"}, "default": opRun},
				"command":           map[string]any{"type": "string"},
				"env":               map[string]any{"type": "object"},
				"capture_stdout_as": map[string]any{"type": "string"},
				"capture_stderr_as": map[string]any{"type": "string"},
				"capture_paths":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required": []any{"command"},
		},
		OutputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"exit_code": map[string]any{"type": "number"},
				"stdout":    map[string]any{"type": "string"},
				"stderr":    map[string]any{"type": "string"},
				"value":     map[string]any{"type": "string"},
			},
		},
	}
}

// Execute dispatches on `op`. Streams are written to files (for artifact
// capture) while the first MaxCapturedOutput bytes are kept for the payload.
func (p *Plugin) Execute(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	op := strings.TrimSpace(configString(ec.Config, "op"))
	if op == "" {
		op = opRun
	}
	switch op {
	case opRun, opEval:
	default:
		return nil, plugin.Permanent(fmt.Errorf(
			"shell: unknown op %q (want %s or %s)", op, opRun, opEval))
	}

	command := configString(ec.Config, "command")
	if strings.TrimSpace(command) == "" {
		return nil, plugin.Permanent(errors.New("shell: `command` is required"))
	}
	environ, err := configStringMap(ec.Config, "env")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("shell: %w", err))
	}
	globs, err := configStringSlice(ec.Config, "capture_paths")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("shell: %w", err))
	}

	streamDir := filepath.Join(ec.WorkDir, internalDir)
	if err := os.MkdirAll(streamDir, 0o755); err != nil {
		return nil, fmt.Errorf("shell: create stream dir: %w", err)
	}
	stdoutPath := filepath.Join(streamDir, "stdout")
	stderrPath := filepath.Join(streamDir, "stderr")
	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("shell: create stdout file: %w", err)
	}
	defer func() { _ = stdoutFile.Close() }()
	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		return nil, fmt.Errorf("shell: create stderr file: %w", err)
	}
	defer func() { _ = stderrFile.Close() }()

	stdoutHead := &headBuffer{limit: MaxCapturedOutput}
	stderrHead := &headBuffer{limit: MaxCapturedOutput}

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = ec.WorkDir
	cmd.Env = commandEnv(ec.WorkDir, environ)
	cmd.Stdout = io.MultiWriter(stdoutFile, stdoutHead)
	cmd.Stderr = io.MultiWriter(stderrFile, stderrHead)
	cmd.WaitDelay = killDelay

	if ec.Log != nil {
		ec.Log("shell: running command in %s", ec.WorkDir)
	}
	runErr := cmd.Run()
	if err := stdoutFile.Sync(); err != nil {
		return nil, fmt.Errorf("shell: flush stdout: %w", err)
	}
	if err := stderrFile.Sync(); err != nil {
		return nil, fmt.Errorf("shell: flush stderr: %w", err)
	}

	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		switch {
		case ctx.Err() != nil:
			return nil, fmt.Errorf("shell: command cancelled: %w", runErr)
		case errors.As(runErr, &exitErr):
			exitCode = exitErr.ExitCode()
		default:
			return nil, fmt.Errorf("shell: start command: %w", runErr)
		}
	}

	output := plugin.Payload{
		"exit_code": float64(exitCode),
		"stdout":    stdoutHead.String(),
		"stderr":    stderrHead.String(),
	}
	if op == opEval {
		output["value"] = strings.TrimSpace(stdoutHead.String())
	}

	// `run` treats a non-zero exit as failure; `eval` reports it as data.
	if exitCode != 0 && op == opRun {
		if ec.Log != nil {
			ec.Log("shell: command exited %d", exitCode)
		}
		return nil, plugin.Permanent(fmt.Errorf("shell: command exited with code %d: %s",
			exitCode, strings.TrimSpace(stderrHead.String())))
	}

	produced := make([]plugin.ProducedArtifact, 0, 2+len(globs))
	if name := strings.TrimSpace(configString(ec.Config, "capture_stdout_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: stdoutPath})
	}
	if name := strings.TrimSpace(configString(ec.Config, "capture_stderr_as")); name != "" {
		produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: stderrPath})
	}
	captured, err := capturePaths(ec.WorkDir, globs)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("shell: %w", err))
	}
	produced = append(produced, captured...)

	if ec.Log != nil {
		ec.Log("shell: %s exited %d with %d artifact(s)", op, exitCode, len(produced))
	}
	return &plugin.Result{Output: output, Artifacts: produced}, nil
}

// capturePaths resolves globs against the workdir. Each matching regular file
// becomes one artifact named by its workdir-relative slash path.
func capturePaths(workDir string, globs []string) ([]plugin.ProducedArtifact, error) {
	var produced []plugin.ProducedArtifact
	seen := map[string]struct{}{}
	for _, pattern := range globs {
		if pattern == "" {
			continue
		}
		if filepath.IsAbs(pattern) || escapes(pattern) {
			return nil, fmt.Errorf("capture_paths pattern %q must stay inside the work directory", pattern)
		}
		matches, err := filepath.Glob(filepath.Join(workDir, pattern))
		if err != nil {
			return nil, fmt.Errorf("capture_paths pattern %q: %w", pattern, err)
		}
		sort.Strings(matches)
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			rel, err := filepath.Rel(workDir, match)
			if err != nil {
				continue
			}
			name := filepath.ToSlash(rel)
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			seen[name] = struct{}{}
			produced = append(produced, plugin.ProducedArtifact{Name: name, LocalPath: match})
		}
	}
	return produced, nil
}

func escapes(pattern string) bool {
	cleaned := filepath.Clean(pattern)
	return cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator))
}

// commandEnv builds a minimal environment: the command sees PATH and a HOME
// pointing at its own workdir, plus whatever the task declared. The Orker's own
// environment is not inherited.
func commandEnv(workDir string, extra map[string]string) []string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/local/bin:/usr/bin:/bin"
	}
	env := map[string]string{
		"PATH":   path,
		"HOME":   workDir,
		"PWD":    workDir,
		"TMPDIR": workDir,
	}
	for key, value := range extra {
		env[key] = value
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}
	return out
}

// headBuffer keeps the first limit bytes written to it and discards the rest.
type headBuffer struct {
	limit int
	buf   bytes.Buffer
}

func (h *headBuffer) Write(p []byte) (int, error) {
	if room := h.limit - h.buf.Len(); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		h.buf.Write(p[:room])
	}
	return len(p), nil
}

func (h *headBuffer) String() string { return h.buf.String() }

func configString(config map[string]any, key string) string {
	value, ok := config[key]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func configStringMap(config map[string]any, key string) (map[string]string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return nil, nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("`%s` must be a map, got %T", key, value)
	}
	out := make(map[string]string, len(raw))
	for name, item := range raw {
		if item == nil {
			out[name] = ""
			continue
		}
		if s, ok := item.(string); ok {
			out[name] = s
			continue
		}
		out[name] = fmt.Sprintf("%v", item)
	}
	return out, nil
}

func configStringSlice(config map[string]any, key string) ([]string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case string:
		return []string{typed}, nil
	case []any:
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("`%s`[%d] must be a string, got %T", key, i, item)
			}
			out = append(out, s)
		}
		return out, nil
	case []string:
		return typed, nil
	default:
		return nil, fmt.Errorf("`%s` must be a list of strings, got %T", key, value)
	}
}
