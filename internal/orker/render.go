package orker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/template"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// EnvAllowlistVar names the environment variable holding the comma-separated
// list of variable names the `env` template helper may read. It defaults to
// empty, so no variable is readable unless an operator opts in.
const EnvAllowlistVar = "ORK_ORKER_ENV_ALLOWLIST"

// noValueMarker is what text/template prints for a present-but-nil value.
// Rendering it would silently corrupt a command or URL, so it is an error.
const noValueMarker = "<no value>"

// renderer renders one task's raw `with` block. It mirrors the DSL's
// compile-time traversal (internal/dsl/template.go): string leaves are
// rendered, maps and slices are walked, every other value is preserved
// verbatim, and map keys are never templated.
type renderer struct {
	taskID    string
	data      map[string]any
	resolver  plugin.ArtifactResolver
	allowlist map[string]struct{}
}

// newRenderer builds the render context documented in docs/DSL.md:
// `.inputs.<name>` from the run input and `.tasks.<id>.output.<field>` plus
// `.tasks.<id>.artifacts` from the upstream results.
func newRenderer(
	taskID string,
	inputs plugin.Payload,
	upstream map[string]task.UpstreamResult,
	resolver plugin.ArtifactResolver,
	allowlist map[string]struct{},
) *renderer {
	tasks := make(map[string]any, len(upstream))
	for id, result := range upstream {
		refs := make([]any, 0, len(result.Artifacts))
		for _, ref := range result.Artifacts {
			refs = append(refs, map[string]any{
				"name":         ref.Name,
				"uri":          ref.URI,
				"content_type": ref.ContentType,
				"size_bytes":   ref.SizeBytes,
				"sha256":       ref.SHA256,
			})
		}
		tasks[id] = map[string]any{
			"output":    payloadMap(result.Output),
			"artifacts": refs,
		}
	}
	return &renderer{
		taskID:    taskID,
		data:      map[string]any{"inputs": payloadMap(inputs), "tasks": tasks},
		resolver:  resolver,
		allowlist: allowlist,
	}
}

// payloadMap always yields a non-nil map so a missing key is an error rather
// than the zero value text/template returns when indexing a nil map.
func payloadMap(p plugin.Payload) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

// render walks the raw config and returns a rendered copy. The input is never
// mutated.
func (r *renderer) render(ctx context.Context, with map[string]any) (map[string]any, error) {
	value, err := r.renderValue(ctx, "with", with)
	if err != nil {
		return nil, err
	}
	rendered, _ := value.(map[string]any)
	if rendered == nil {
		rendered = map[string]any{}
	}
	return rendered, nil
}

func (r *renderer) renderValue(ctx context.Context, path string, value any) (any, error) {
	switch typed := value.(type) {
	case string:
		return r.renderString(ctx, path, typed)
	case map[string]any:
		out := make(map[string]any, len(typed))
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			child, err := r.renderValue(ctx, path+"."+k, typed[k])
			if err != nil {
				return nil, err
			}
			out[k] = child
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			child, err := r.renderValue(ctx, fmt.Sprintf("%s[%d]", path, i), item)
			if err != nil {
				return nil, err
			}
			out[i] = child
		}
		return out, nil
	default:
		return value, nil
	}
}

func (r *renderer) renderString(ctx context.Context, path, text string) (string, error) {
	if !strings.Contains(text, "{{") {
		return text, nil
	}
	name := r.taskID + "/" + path
	tmpl, err := template.New(name).
		Option("missingkey=error").
		Funcs(r.funcs(ctx)).
		Parse(text)
	if err != nil {
		return "", fmt.Errorf("task %q: %s: template parse error: %w", r.taskID, path, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, r.data); err != nil {
		return "", fmt.Errorf("task %q: %s: render: %w", r.taskID, path, err)
	}
	out := buf.String()
	if strings.Contains(out, noValueMarker) {
		return "", fmt.Errorf("task %q: %s: render: template resolved to %s", r.taskID, path, noValueMarker)
	}
	return out, nil
}

func (r *renderer) funcs(ctx context.Context) template.FuncMap {
	return template.FuncMap{
		"artifact": func(ref string) (string, error) {
			if r.resolver == nil {
				return "", fmt.Errorf("artifact %q: no artifact resolver is configured", ref)
			}
			return r.resolver.Resolve(ctx, ref)
		},
		"env": func(name string) (string, error) {
			if _, ok := r.allowlist[name]; !ok {
				return "", fmt.Errorf(
					"env %q is not allowlisted on this orker (set %s)", name, EnvAllowlistVar)
			}
			return os.Getenv(name), nil
		},
	}
}

// parseAllowlist reads a comma-separated allowlist. Blank entries are ignored.
func parseAllowlist(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}
