package dsl_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// ReferencedUpstream shares the checker's walker, so every spelling the
// checker accepts for a .tasks reference is one this collector sees. The
// table names each spelling the checker's checkField and checkIndex accept.
func TestReferencedUpstream_SeesEverySpellingTheCheckerAccepts(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []string
	}{
		{"field", `{{ .tasks.a.output.x }}`, []string{"a"}},
		{"index", `{{ index .tasks "b" "output" "y" }}`, []string{"b"}},
		{"if branch and body", `{{ if .tasks.a.output.ok }}{{ .tasks.c.output.z }}{{ end }}`, []string{"a", "c"}},
		{"inputs only", `{{ eq .inputs.region "eu" }}`, nil},
		{"dynamic index ignored", `{{ index .tasks .inputs.which "output" }}`, nil},
		{"root variable", `{{ $.tasks.d.output.v }}`, []string{"d"}},
		{"root container index", `{{ index . "tasks" "e" "output" }}`, []string{"e"}},
		{"sorted and deduped", `{{ and .tasks.z.output.a .tasks.a.output.b .tasks.z.output.c }}`, []string{"a", "z"}},
		{"no template", `true`, nil},
		{"empty", ``, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dsl.ReferencedUpstream(tc.text)
			if err != nil {
				t.Fatalf("ReferencedUpstream(%q): %v", tc.text, err)
			}
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ReferencedUpstream(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}

	t.Run("parse error is returned", func(t *testing.T) {
		if _, err := dsl.ReferencedUpstream(`{{ .tasks.a.output.x `); err == nil {
			t.Fatal("ReferencedUpstream of an unterminated action returned no error")
		}
	})
}

// TestValidate_LiteralBracesInAWithStringNeedTheEscape: a with: string
// containing {{ is a DSL template, so a jq filter that needs the characters
// {{ literally writes {{ "{{" }} — and the unescaped spelling is refused at
// validate, naming the key, never discovered at run time.
func TestValidate_LiteralBracesInAWithStringNeedTheEscape(t *testing.T) {
	doc := func(filter string) []byte {
		return []byte(`name: braces
version: "0.1.0"
tasks:
  - id: q
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      filter: '` + filter + `'
`)
	}
	_, err := dsl.Compile(doc(`"{{name}}"`))
	if err == nil {
		t.Fatal(`filter '"{{name}}"' compiled; an unescaped {{ is a template and must be refused`)
	}
	for _, want := range []string{`task "q": with.filter: template parse error`, `function "name" not defined`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if _, err := dsl.Compile(doc(`"{{ "{{" }}name}}"`)); err != nil {
		t.Fatalf(`the escaped filter '"{{ "{{" }}name}}"' was refused: %v`, err)
	}
}
