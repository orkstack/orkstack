package dsl

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"text/template"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// noValueMarker is what text/template prints for a present-but-nil value.
// Rendering it would silently corrupt a value, so it is an error — the rule
// internal/orker/render.go holds a container's with: to.
const noValueMarker = "<no value>"

// RenderValues renders every string leaf of value as a text/template against
// {"inputs": inputs, "tasks": {<id>: {"output": outputs[id]}}} with
// missingkey=error and NO helper functions; maps and slices are walked, every
// other value is kept verbatim, and map keys are never templated — the
// traversal internal/orker/render.go uses. A result containing "<no value>"
// is an error (render.go's noValueMarker rule). label prefixes every error
// ("task \"release\": with" gives "task \"release\": with.image: …";
// "outputs" gives "outputs.digest: …"). Pure.
func RenderValues(label string, value any, inputs map[string]any, outputs map[string]map[string]any) (any, error) {
	tasks := make(map[string]any, len(outputs))
	for id, values := range outputs {
		output := make(map[string]any, len(values))
		for k, v := range values {
			output[k] = v
		}
		tasks[id] = map[string]any{"output": output}
	}
	in := make(map[string]any, len(inputs))
	for k, v := range inputs {
		in[k] = v
	}
	data := map[string]any{"inputs": in, "tasks": tasks}
	return renderValue(label, "", value, data)
}

func renderValue(label, path string, value any, data map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return renderString(label, path, typed, data)
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(typed))
		for _, k := range keys {
			child, err := renderValue(label, joinPath(path, k), typed[k], data)
			if err != nil {
				return nil, err
			}
			out[k] = child
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			child, err := renderValue(label, fmt.Sprintf("%s[%d]", path, i), item, data)
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

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// where renders "label.path" (or label alone at the root) for an error.
func where(label, path string) string {
	switch {
	case path == "":
		return label
	case label == "":
		return path
	case strings.HasPrefix(path, "["):
		return label + path
	default:
		return label + "." + path
	}
}

func renderString(label, path, text string, data map[string]any) (string, error) {
	if !strings.Contains(text, "{{") {
		return text, nil
	}
	at := where(label, path)
	tmpl, err := template.New(at).Option("missingkey=error").Parse(text)
	if err != nil {
		return "", fmt.Errorf("%s: template parse error: %w", at, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("%s: render: %w", at, err)
	}
	out := buf.String()
	if strings.Contains(out, noValueMarker) {
		return "", fmt.Errorf("%s: render: template resolved to %s", at, noValueMarker)
	}
	return out, nil
}

// ReferencedUpstreamIn lists, sorted and deduplicated, the task ids every
// string leaf of value reads through .tasks. It walks the parse tree with the
// walker ReferencedUpstream uses, and it differs from that function in one
// respect that is the reason it exists: it is AUTHORITATIVE.
// ReferencedUpstream is advisory ("never to decide anything") because a
// dynamic index can read a task it does not list; rule 28 forbids that index
// in an engine-rendered template, and here one is an error rather than a skip,
// so the engine may read exactly the listed values and nothing else. Pure.
func ReferencedUpstreamIn(value any) ([]string, error) {
	c := &templateChecker{refs: map[string]struct{}{}, engine: true}
	var walk func(path string, v any) error
	walk = func(path string, v any) error {
		switch typed := v.(type) {
		case string:
			if !strings.Contains(typed, "{{") {
				return nil
			}
			tmpl, err := Parse("reference", typed)
			if err != nil {
				return fmt.Errorf("%s: template parse error: %w", path, err)
			}
			return c.walkTemplate(path, tmpl)
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for k := range typed {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if err := walk(joinPath(path, k), typed[k]); err != nil {
					return err
				}
			}
		case map[string]string:
			keys := make([]string, 0, len(typed))
			for k := range typed {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if err := walk(joinPath(path, k), typed[k]); err != nil {
					return err
				}
			}
		case []any:
			for i, item := range typed {
				if err := walk(fmt.Sprintf("%s[%d]", path, i), item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk("", value); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(c.refs))
	for id := range c.refs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// OutputReference is one literal read of a task's output key in a document.
type OutputReference struct {
	Field  string // "tasks[2].with.body", "tasks[3].condition", "outputs.digest"
	TaskID string
	Key    string
}

// OutputReferences lists every .tasks.<id>.output.<key> read — the field
// spelling and the index spelling with literal arguments — in every template
// of wf: each task's with: and condition:, and outputs:. Document order,
// then field path. A dynamic key is not a reference and is skipped. Pure.
func OutputReferences(wf *orkstackv1.Orkflow) ([]OutputReference, error) {
	var out []OutputReference
	collect := func(field, text string, into *[]OutputReference) error {
		if !strings.Contains(text, "{{") {
			return nil
		}
		tmpl, err := Parse("reference", text)
		if err != nil {
			return fmt.Errorf("%s: template parse error: %w", field, err)
		}
		seen := map[[2]string]bool{}
		c := &templateChecker{
			refs: map[string]struct{}{},
			outputRef: func(taskID, key string) {
				k := [2]string{taskID, key}
				if seen[k] {
					return
				}
				seen[k] = true
				*into = append(*into, OutputReference{Field: field, TaskID: taskID, Key: key})
			},
		}
		return c.walkTemplate(field, tmpl)
	}
	var walk func(field string, v any, into *[]OutputReference) error
	walk = func(field string, v any, into *[]OutputReference) error {
		switch typed := v.(type) {
		case string:
			return collect(field, typed, into)
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for k := range typed {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if err := walk(field+"."+k, typed[k], into); err != nil {
					return err
				}
			}
		case []any:
			for i, item := range typed {
				if err := walk(fmt.Sprintf("%s[%d]", field, i), item, into); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i, t := range wf.GetTasks() {
		var refs []OutputReference
		anchor := fmt.Sprintf("tasks[%d]", i)
		if err := collect(anchor+".condition", t.GetCondition(), &refs); err != nil {
			return nil, err
		}
		if t.GetWith() != nil {
			if err := walk(anchor+".with", t.GetWith().AsMap(), &refs); err != nil {
				return nil, err
			}
		}
		sort.SliceStable(refs, func(a, b int) bool { return refs[a].Field < refs[b].Field })
		out = append(out, refs...)
	}
	keys := make([]string, 0, len(wf.GetOutputs()))
	for k := range wf.GetOutputs() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := collect("outputs."+k, wf.GetOutputs()[k], &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}
