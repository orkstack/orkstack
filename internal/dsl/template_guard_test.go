package dsl_test

import (
	"errors"
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
		template string
		inputs   map[string]any
	}{
		{"{{ .inputs.missing }}", map[string]any{"missing": nil}},
		{"{{ .inputs.missing }} {{ .inputs.note }}", map[string]any{"missing": nil, "note": "see <no value> here"}},
		{"{{ .inputs.note }} then {{ .inputs.missing }}", map[string]any{"missing": nil, "note": "<no value>"}},
	} {
		_, err := dsl.RenderValues("with", map[string]any{"v": tc.template}, tc.inputs, nil)
		if err == nil || !strings.Contains(err.Error(), "template resolved to <no value>") {
			t.Errorf("%q over %v: err = %v, want the nil value refused as <no value>", tc.template, tc.inputs, err)
		}
	}
}

func TestExecuteChecked_RefusesATemplateOrDataHoldingTheReservedMask(t *testing.T) {
	_, err := dsl.RenderValues("with", map[string]any{"v": "{{ .inputs.a }}{{ .inputs.b }}"},
		map[string]any{"a": "<no value>", "b": "<no\x01value>"}, nil)
	if err == nil || errors.Is(err, dsl.ErrNoValue) || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("err = %v, want the reserved sequence refused, and not as a nil value", err)
	}
}
