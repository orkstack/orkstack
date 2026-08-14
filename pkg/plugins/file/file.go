// Package fileplugin implements the builtin file plugin.
//
// `with`:
//
//	op          write | read | copy | delete (required)
//	path        target for write, read and delete
//	content     file contents for write
//	from, to    source and destination for copy
//	capture_as  artifact name override (default: the file's workdir-relative path)
//
// `output`: path, size_bytes.
//
// write, read and copy each produce one artifact holding the resulting file, so
// a written result and a materialized upstream artifact both flow onward. All
// paths resolve inside the task work directory (which includes artifacts
// materialized by the `artifact` template helper); a path escaping it fails the
// task. delete is idempotent and produces no artifact.
package fileplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// Plugin performs a file operation in the task work directory.
type Plugin struct{}

// New creates the file plugin.
func New() *Plugin { return &Plugin{} }

// Name is the DSL `plugin:` identifier.
func (p *Plugin) Name() string { return "file" }

// Describe returns discovery metadata.
func (p *Plugin) Describe() plugin.Descriptor {
	return plugin.Descriptor{
		Name:        "file",
		Version:     "0.1.0",
		Description: "Write, read, copy or delete a file in the task work directory.",
		InputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"op":         map[string]any{"type": "string", "enum": []any{"write", "read", "copy", "delete"}},
				"path":       map[string]any{"type": "string"},
				"content":    map[string]any{"type": "string"},
				"from":       map[string]any{"type": "string"},
				"to":         map[string]any{"type": "string"},
				"capture_as": map[string]any{"type": "string"},
			},
			"required": []any{"op"},
		},
		OutputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"path":       map[string]any{"type": "string"},
				"size_bytes": map[string]any{"type": "number"},
			},
		},
	}
}

// Execute dispatches on `op`.
func (p *Plugin) Execute(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	op := strings.TrimSpace(configString(ec.Config, "op"))
	switch op {
	case "write":
		return p.write(ec)
	case "read":
		return p.read(ec)
	case "copy":
		return p.copy(ec)
	case "delete":
		return p.remove(ec)
	case "":
		return nil, plugin.Permanent(errors.New("file: `op` is required (write, read, copy or delete)"))
	default:
		return nil, plugin.Permanent(fmt.Errorf(
			"file: unknown op %q (want write, read, copy or delete)", op))
	}
}

func (p *Plugin) write(ec *plugin.ExecContext) (*plugin.Result, error) {
	target, err := resolve(ec.WorkDir, configString(ec.Config, "path"), "path")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: %w", err))
	}
	content := configString(ec.Config, "content")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return nil, fmt.Errorf("file: write %s: %w", target, err)
	}
	if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
		return nil, fmt.Errorf("file: write %s: %w", target, err)
	}
	if ec.Log != nil {
		ec.Log("file: wrote %d bytes to %s", len(content), target)
	}
	return result(ec, target, int64(len(content)))
}

func (p *Plugin) read(ec *plugin.ExecContext) (*plugin.Result, error) {
	target, err := resolve(ec.WorkDir, configString(ec.Config, "path"), "path")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: %w", err))
	}
	info, err := os.Stat(target)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: read %s: %w", target, err))
	}
	if !info.Mode().IsRegular() {
		return nil, plugin.Permanent(fmt.Errorf("file: read %s: not a regular file", target))
	}
	if ec.Log != nil {
		ec.Log("file: read %d bytes from %s", info.Size(), target)
	}
	return result(ec, target, info.Size())
}

func (p *Plugin) copy(ec *plugin.ExecContext) (*plugin.Result, error) {
	source, err := resolve(ec.WorkDir, configString(ec.Config, "from"), "from")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: %w", err))
	}
	target, err := resolve(ec.WorkDir, configString(ec.Config, "to"), "to")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: %w", err))
	}
	in, err := os.Open(source)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: copy %s: %w", source, err))
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return nil, fmt.Errorf("file: copy to %s: %w", target, err)
	}
	out, err := os.Create(target)
	if err != nil {
		return nil, fmt.Errorf("file: copy to %s: %w", target, err)
	}
	size, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return nil, fmt.Errorf("file: copy %s -> %s: %w", source, target, copyErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("file: copy to %s: %w", target, closeErr)
	}
	if ec.Log != nil {
		ec.Log("file: copied %d bytes %s -> %s", size, source, target)
	}
	return result(ec, target, size)
}

// remove deletes the path. Deleting an absent path succeeds with size 0 so a
// retried attempt behaves like the first.
func (p *Plugin) remove(ec *plugin.ExecContext) (*plugin.Result, error) {
	target, err := resolve(ec.WorkDir, configString(ec.Config, "path"), "path")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("file: %w", err))
	}
	var size int64
	if info, statErr := os.Stat(target); statErr == nil {
		size = info.Size()
	}
	if err := os.RemoveAll(target); err != nil {
		return nil, fmt.Errorf("file: delete %s: %w", target, err)
	}
	if ec.Log != nil {
		ec.Log("file: deleted %s", target)
	}
	return &plugin.Result{Output: plugin.Payload{
		"path":       displayPath(ec.WorkDir, target),
		"size_bytes": float64(size),
	}}, nil
}

// result builds the standard output payload plus the produced artifact.
func result(ec *plugin.ExecContext, target string, size int64) (*plugin.Result, error) {
	name := strings.TrimSpace(configString(ec.Config, "capture_as"))
	if name == "" {
		name = displayPath(ec.WorkDir, target)
	}
	return &plugin.Result{
		Output: plugin.Payload{
			"path":       displayPath(ec.WorkDir, target),
			"size_bytes": float64(size),
		},
		Artifacts: []plugin.ProducedArtifact{{Name: name, LocalPath: target}},
	}, nil
}

// resolve turns a configured path into an absolute path inside the workdir.
func resolve(workDir, raw, field string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("`%s` is required", field)
	}
	target := raw
	if !filepath.IsAbs(target) {
		target = filepath.Join(workDir, target)
	}
	target = filepath.Clean(target)
	root := filepath.Clean(workDir)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("`%s` %q escapes the task work directory", field, raw)
	}
	return target, nil
}

// displayPath reports paths relative to the workdir, which is scratch space
// with a per-attempt random name.
func displayPath(workDir, target string) string {
	rel, err := filepath.Rel(filepath.Clean(workDir), target)
	if err != nil {
		return target
	}
	return filepath.ToSlash(rel)
}

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
