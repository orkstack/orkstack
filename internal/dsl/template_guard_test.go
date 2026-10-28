package dsl_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// The no-value rule refuses a nil value that would print as "<no value>". It
// must not refuse data or an author's text that merely spells those characters.
func TestRenderValues_ADataValueThatSpellsTheMarkerRendersAsItself(t *testing.T) {
	inputs := map[string]any{"note": "see <no value> here", "list": []any{"<no value>"}, "nested": map[string]any{"k": "<no value>"}}
	for _, tc := range []struct{ template, want string }{
		{"{{ .inputs.note }}", "see <no value> here"},
		{"x {{ .inputs.note }} y", "x see <no value> here y"},
		{"{{ index .inputs.list 0 }}", "<no value>"},
		{"{{ .inputs.nested.k }}", "<no value>"},
		{"{{ len .inputs.note }}", "19"},
		{"literal <no value> text {{ .inputs.note }}", "literal <no value> text see <no value> here"},
		{`{{ "<no value>" }}`, "<no value>"},
	} {
		got, err := dsl.RenderValues("with", map[string]any{"v": tc.template}, inputs, nil)
		if err != nil {
			t.Errorf("%q: %v", tc.template, err)
			continue
		}
		if value := got.(map[string]any)["v"]; value != tc.want {
			t.Errorf("%q rendered %q, want %q", tc.template, value, tc.want)
		}
	}
}

func TestRenderValues_ANilValueIsStillRefusedAndNamed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		template string
		inputs   map[string]any
	}{
		{"a nil value", "{{ .inputs.missing }}", map[string]any{"missing": nil}},
		{"a nil beside data that spells the marker", "{{ .inputs.missing }} {{ .inputs.note }}", map[string]any{"missing": nil, "note": "see <no value> here"}},
		{"a nil after data that is the marker", "{{ .inputs.note }} then {{ .inputs.missing }}", map[string]any{"missing": nil, "note": "<no value>"}},
		{"a key a map does not hold, read through index", `{{ index .inputs.m "gone" }}`, map[string]any{"m": map[string]any{}}},
		{"a nil element of a list", "{{ range .inputs.list }}[{{ . }}]{{ end }}", map[string]any{"list": []any{"a", nil}}},
		{"a nil inside a with block", "{{ with .inputs.m }}{{ .k }}{{ end }}", map[string]any{"m": map[string]any{"k": nil}}},
		{"a nil kept in a variable", "{{ $v := .inputs.missing }}{{ $v }}", map[string]any{"missing": nil}},
		{"a nil printed from a branch", "{{ if true }}{{ .inputs.missing }}{{ else }}x{{ end }}", map[string]any{"missing": nil}},
		{"a nil printed from a template that is defined", `{{ define "n" }}{{ .inputs.missing }}{{ end }}{{ template "n" . }}`, map[string]any{"missing": nil}},
		{"a nil piped through a function that returns it", "{{ .inputs.missing | and }}", map[string]any{"missing": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.RenderValues("with", map[string]any{"v": tc.template}, tc.inputs, nil)
			if err == nil || !strings.Contains(err.Error(), "template resolved to <no value>") {
				t.Errorf("%q over %v: err = %v, want the nil value refused as <no value>", tc.template, tc.inputs, err)
			}
		})
	}
}

// A nil that is only looked at, never printed, was never refused and is not.
func TestRenderValues_ANilThatIsNotPrintedIsNotRefused(t *testing.T) {
	inputs := map[string]any{"missing": nil, "note": "<no value>"}
	for _, tc := range []struct{ template, want string }{
		{"{{ if .inputs.missing }}yes{{ else }}no{{ end }}", "no"},
		{"{{ $v := .inputs.missing }}{{ if not $v }}unset{{ end }}", "unset"},
		{"{{ with .inputs.missing }}x{{ else }}empty{{ end }} {{ .inputs.note }}", "empty <no value>"},
		{`{{ or .inputs.missing "fallback" }}`, "fallback"},
	} {
		got, err := dsl.RenderValues("with", map[string]any{"v": tc.template}, inputs, nil)
		if err != nil {
			t.Errorf("%q: %v", tc.template, err)
			continue
		}
		if value := got.(map[string]any)["v"]; value != tc.want {
			t.Errorf("%q rendered %q, want %q", tc.template, value, tc.want)
		}
	}
}

// What a template prints is what it computes from the real data, however the
// characters of the marker came together: across the text and a value, across
// two values, inside one action, or past a function that quotes or compares.
func TestRenderValues_TheMarkerAssembledFromPiecesRendersAsItself(t *testing.T) {
	inputs := map[string]any{"a": "<no", "b": " value>", "v": "value", "w": "value>", "whole": "see <no value> here", "mask": "<no\x01value>"}
	for _, tc := range []struct{ name, template, want string }{
		{"two values", "{{ .inputs.a }}{{ .inputs.b }}", "<no value>"},
		{"text then a value", "<no {{ .inputs.w }}", "<no value>"},
		{"a value then text", "{{ .inputs.a }} value>", "<no value>"},
		{"a value between two texts", "see <no {{ .inputs.v }}> here", "see <no value> here"},
		{"printf joining two values", `{{ printf "%s%s" .inputs.a .inputs.b }}`, "<no value>"},
		{"print joining two values", `{{ print .inputs.a .inputs.b }}`, "<no value>"},
		{"a value quoted by printf %q", `{{ .inputs.whole }}|{{ printf "%q" .inputs.whole }}`, `see <no value> here|"see <no value> here"`},
		{"a value compared with a string built by the template", `{{ .inputs.whole }}|{{ if eq .inputs.whole (print "see <no" " value> here") }}Y{{ else }}N{{ end }}`, "see <no value> here|Y"},
		{"a value that holds the old mask", "{{ .inputs.mask }}", "<no\x01value>"},
		{"the old mask and the marker side by side", "{{ .inputs.mask }}{{ .inputs.whole }}", "<no\x01value>see <no value> here"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dsl.RenderValues("with", map[string]any{"v": tc.template}, inputs, nil)
			if err != nil {
				t.Fatalf("%q: %v", tc.template, err)
			}
			if value := got.(map[string]any)["v"]; value != tc.want {
				t.Errorf("%q rendered %q, want %q", tc.template, value, tc.want)
			}
		})
	}
}
