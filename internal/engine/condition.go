package engine

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/orkstack/orkstack/internal/task"
)

// falsyValues are the rendered condition results treated as "do not run".
var falsyValues = map[string]struct{}{
	"":            {},
	"false":       {},
	"0":           {},
	"no":          {},
	"off":         {},
	"null":        {},
	"<no value>":  {},
	"[]":          {},
	"map[]":       {},
	"<nil>":       {},
	"unspecified": {},
}

// evaluateCondition renders a task `condition` against the deterministic part
// of the template context (run inputs plus already-completed upstream results)
// and reports whether the task should run.
//
// The engine deliberately renders nothing else: task `with` values and the
// `artifact`/`env` helpers are the Orker's job (see internal/task package doc),
// and resolving them here would require filesystem/network access inside
// deterministic workflow code. A condition that uses those helpers is
// therefore rejected rather than silently treated as empty.
func evaluateCondition(expression string, inputs map[string]any, upstream map[string]task.UpstreamResult) (bool, error) {
	if strings.TrimSpace(expression) == "" {
		return true, nil
	}

	funcs := template.FuncMap{
		"artifact": func(string) (string, error) {
			return "", fmt.Errorf("the artifact helper is not available in a condition: it resolves on the Orker")
		},
		"env": func(string) (string, error) {
			return "", fmt.Errorf("the env helper is not available in a condition: it resolves on the Orker")
		},
	}

	tmpl, err := template.New("condition").Option("missingkey=zero").Funcs(funcs).Parse(expression)
	if err != nil {
		return false, fmt.Errorf("parse condition %q: %w", expression, err)
	}

	tasks := make(map[string]any, len(upstream))
	for id, result := range upstream {
		tasks[id] = map[string]any{
			"output":    map[string]any(result.Output),
			"artifacts": result.Artifacts,
		}
	}

	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, map[string]any{
		"inputs": inputs,
		"tasks":  tasks,
	}); err != nil {
		return false, fmt.Errorf("evaluate condition %q: %w", expression, err)
	}

	value := strings.ToLower(strings.TrimSpace(rendered.String()))
	_, falsy := falsyValues[value]
	return !falsy, nil
}
