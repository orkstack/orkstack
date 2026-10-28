package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/template"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// EnvAllowlistVar names the environment variable holding the comma-separated
// list of variable names the `env` template helper may read. It defaults to
// empty, so no variable is readable unless an operator opts in.
const EnvAllowlistVar = "ORK_ORKER_ENV_ALLOWLIST"

// renderer renders one task's raw `with` block. It mirrors the DSL's
// compile-time traversal (internal/dsl/template.go): string leaves are
// rendered, maps and slices are walked, every other value is preserved
// verbatim, and map keys are never templated.
//
// Rendering stays in the ORKER rather than the engine, and that is the whole
// reason the wire carries the RAW `with:` block (OQ-1): `artifact
// "<task>/<name>"` resolves to a path only the Orker knows, after staging.
// The rendered result is materialized to /orkstack/input/config.json.
type renderer struct {
	taskID string
	data   map[string]any
	// paths maps "<producer>/<name>" to the CONTAINER path of a staged input
	// artifact. The helper returns a container path because that is the only
	// path the workload can open — the host path would be meaningless inside
	// the container.
	paths     map[string]string
	allowlist map[string]struct{}
}

// newRenderer builds the render context documented in docs/DSL.md:
// `.inputs.<name>` from the run input and `.tasks.<id>.output.<field>` plus
// `.tasks.<id>.artifacts` from the upstream results, and — for an item of a
// fan-out only — `.each.<as>`, `.each.index` and `.each.count`.
func newRenderer(
	taskID string,
	inputs map[string]any,
	upstreamValues map[string]map[string]any,
	upstreamArtifacts map[string][]*orkstackv1.ArtifactRef,
	paths map[string]string,
	allowlist map[string]struct{},
	each map[string]any,
) *renderer {
	tasks := make(map[string]any, len(upstreamArtifacts))
	ids := make(map[string]struct{}, len(upstreamArtifacts)+len(upstreamValues))
	for id := range upstreamArtifacts {
		ids[id] = struct{}{}
	}
	for id := range upstreamValues {
		ids[id] = struct{}{}
	}
	for id := range ids {
		refs := make([]any, 0, len(upstreamArtifacts[id]))
		for _, ref := range upstreamArtifacts[id] {
			refs = append(refs, map[string]any{
				"name":         ref.GetName(),
				"uri":          ref.GetUri(),
				"content_type": ref.GetContentType(),
				"size_bytes":   ref.GetSizeBytes(),
				"sha256":       ref.GetSha256(),
			})
		}
		tasks[id] = map[string]any{
			"output":    nonNilMap(upstreamValues[id]),
			"artifacts": refs,
		}
	}
	data := map[string]any{"inputs": nonNilMap(inputs), "tasks": tasks}
	// .each exists only while rendering an item of a fan-out: {<as>: the
	// item, "index": i, "count": n}. Absent otherwise, so a stray .each in a
	// task that is not an item is a missing key, RENDER_FAILED.
	if each != nil {
		data["each"] = each
	}
	return &renderer{
		taskID:    taskID,
		data:      data,
		paths:     paths,
		allowlist: allowlist,
	}
}

// nonNilMap always yields a non-nil map so a missing key is an error rather
// than the zero value text/template returns when indexing a nil map.
func nonNilMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
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
	out, err := dsl.ExecuteChecked(text, r.data, func(text string) (*template.Template, error) {
		return template.New(name).
			Option("missingkey=error").
			Funcs(r.funcs(ctx)).
			Parse(text)
	})
	var failure *dsl.TemplateError
	switch {
	case errors.As(err, &failure) && failure.Stage == dsl.StageParse:
		return "", fmt.Errorf("task %q: %s: template parse error: %w", r.taskID, path, failure.Err)
	case errors.As(err, &failure):
		return "", fmt.Errorf("task %q: %s: render: %w", r.taskID, path, failure.Err)
	case err != nil:
		return "", err
	}
	return out, nil
}

func (r *renderer) funcs(_ context.Context) template.FuncMap {
	return template.FuncMap{
		"artifact": func(ref string) (string, error) {
			path, ok := r.paths[ref]
			if !ok {
				return "", fmt.Errorf(
					"artifact %q is not an input of this task (staged: %s)",
					ref, joinOrNone(sortedKeys(r.paths)))
			}
			return path, nil
		},
		"env": func(name string) (string, error) {
			if _, ok := r.allowlist[name]; !ok {
				return "", fmt.Errorf(
					"env %q is not allowlisted on this orker (set %s)", name, EnvAllowlistVar)
			}
			return os.Getenv(name), nil
		},
		// sh wraps a value in POSIX single quotes, escaping any embedded '
		// as '\''.
		//
		// AGENTS.md §6 requires that shell commands must not interpolate
		// untrusted input without the templating layer's escaping, and until
		// now NEITHER FuncMap had a quoting function of any kind. This is the
		// escaping that sentence names.
		//
		// It is the SECONDARY mitigation. The primary one is that a
		// first-party `command` is an argv array rather than a shell string
		// (§10.2), so nothing rendered becomes code in the first place.
		"sh": func(value any) string {
			return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", `'\''`) + "'"
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
