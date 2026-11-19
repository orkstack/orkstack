package dsl

import (
	"strings"
	"testing"
)

var renderRun = ConcurrencyKeyRun{Orkflow: "migrate", Version: "v1", OriginKind: "SUBMIT", OriginID: ""}

func TestRenderConcurrencyKey(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		inputs     map[string]any
		want       string
		refused    string // a substring of the refusal; "" renders
	}{
		{name: "literal", text: "deploy", want: "deploy"},
		{name: "input", text: "db:{{ .inputs.env }}", inputs: map[string]any{"env": "prod"}, want: "db:prod"},
		{name: "run metadata", text: "{{ .run.orkflow }}@{{ .run.version }}/{{ .run.origin.kind }}", want: "migrate@v1/SUBMIT"},
		{name: "missing input", text: "db:{{ .inputs.env }}", refused: "did not render"},
		{name: "nil input", text: "db:{{ .inputs.env }}", inputs: map[string]any{"env": nil}, refused: "did not render"},
		{name: "renders a bad byte", text: "db:{{ .inputs.env }}", inputs: map[string]any{"env": "a b"}, refused: "offset 4"},
		{name: "renders the reserved prefix", text: "{{ .inputs.k }}", inputs: map[string]any{"k": "ork:x"}, refused: "ork:"},
		{name: "renders empty", text: "{{ .inputs.k }}", inputs: map[string]any{"k": ""}, refused: "is empty"},
		{name: "renders too long", text: "{{ .inputs.k }}", inputs: map[string]any{"k": strings.Repeat("a", 129)}, refused: "at most 128"},
		{name: "empty template", text: "", refused: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderConcurrencyKey(tc.text, tc.inputs, renderRun)
			if tc.refused != "" {
				if err == nil || !strings.HasPrefix(err.Error(), "CONCURRENCY_KEY_INVALID") || !strings.Contains(err.Error(), tc.refused) {
					t.Fatalf("RenderConcurrencyKey(%q) = %q, %v; want a CONCURRENCY_KEY_INVALID refusal naming %q", tc.text, got, err, tc.refused)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("RenderConcurrencyKey(%q) = %q, %v; want %q", tc.text, got, err, tc.want)
			}
		})
	}
}

// FuzzRenderConcurrencyKey: whatever a submit's input holds, the render
// never panics, a key it returns passes CheckConcurrencyKey, and every
// refusal is CONCURRENCY_KEY_INVALID.
func FuzzRenderConcurrencyKey(f *testing.F) {
	f.Add("db:{{ .inputs.env }}", "prod")
	f.Add("{{ .inputs.env }}", "a b")
	f.Add("{{ .inputs.env }}", "ork:x")
	f.Add("{{ .run.orkflow }}-{{ .inputs.env }}", "")
	f.Add("deploy", "")
	f.Fuzz(func(t *testing.T, text, value string) {
		if checkConcurrencyKeyTemplate(text) != nil && strings.Contains(text, "{{") {
			return // validate refuses it; a stored definition never holds it
		}
		key, err := RenderConcurrencyKey(text, map[string]any{"env": value}, renderRun)
		if err != nil {
			if !strings.HasPrefix(err.Error(), "CONCURRENCY_KEY_INVALID") {
				t.Fatalf("refusal %v is not CONCURRENCY_KEY_INVALID", err)
			}
			return
		}
		if err := CheckConcurrencyKey(key); err != nil {
			t.Fatalf("rendered key %q fails the rule: %v", key, err)
		}
	})
}
