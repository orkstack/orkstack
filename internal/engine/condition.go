package engine

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
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
func evaluateCondition(expression string, inputs map[string]any, upstream map[string]map[string]any) (bool, error) {
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

	// A condition sees each completed ancestor's decoded values.json as
	// .tasks.<id>.output. It does NOT see .artifacts: an artifact is a
	// reference to bytes in the store, and a condition that branched on one
	// would be asking a question only the Orker can answer.
	tasks := make(map[string]any, len(upstream))
	for id, values := range upstream {
		tasks[id] = map[string]any{"output": values}
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

// skipReasonUpstream is the reason recorded when a direct need was skipped.
// classify and Simulate both spell it through this function, so a plan's
// skip_reason and the interpreter's "task skipped: " event cannot drift.
func skipReasonUpstream(need string) string {
	return fmt.Sprintf("upstream task %q was skipped", need)
}

// skipReasonCondition is the reason recorded when a condition rendered falsy.
func skipReasonCondition(expression string) string {
	return fmt.Sprintf("condition %q evaluated false", expression)
}
