package dsl

import (
	"fmt"
	"strconv"
	"strings"
	"text/template"
)

// ConcurrencyKeyRun is the run metadata a concurrency key template may read
// (§3.2 of the limits' design; ValidateConcurrency's roots): what is the same
// on every retry of one submit.
type ConcurrencyKeyRun struct {
	Orkflow    string
	Version    string
	OriginKind string
	OriginID   string
}

// RenderConcurrencyKey renders a run's key at submit (docs/DSL.md
// "Concurrency limits"): template with missingkey=error and no function,
// against the run's inputs (defaults applied) and run, then held to
// CheckConcurrencyKey. A key that does not render, or renders to a key the
// rule refuses, is CONCURRENCY_KEY_INVALID, naming the template. An empty
// template is not a key: the caller uses the default key instead. Pure.
func RenderConcurrencyKey(text string, inputs map[string]any, run ConcurrencyKeyRun) (string, error) {
	if text == "" {
		return "", fmt.Errorf("%s: the key template is empty", ConcurrencyKeyInvalidCode)
	}
	key := text
	if strings.Contains(text, "{{") {
		in := make(map[string]any, len(inputs))
		for k, v := range inputs {
			in[k] = v
		}
		data := map[string]any{
			"inputs": in,
			"run": map[string]any{
				"orkflow": run.Orkflow,
				"version": run.Version,
				"origin":  map[string]any{"kind": run.OriginKind, "id": run.OriginID},
			},
		}
		rendered, err := ExecuteChecked(text, data, func(text string) (*template.Template, error) {
			return template.New("concurrency.key").Option("missingkey=error").Parse(text)
		})
		if err != nil {
			return "", fmt.Errorf("%s: the key template %s did not render: %w", ConcurrencyKeyInvalidCode, strconv.Quote(text), err)
		}
		key = rendered
	}
	if err := CheckConcurrencyKey(key, KeyAuthored); err != nil {
		return "", fmt.Errorf("%w (from the key template %s)", err, strconv.Quote(text))
	}
	return key, nil
}
