package dsl_test

import (
	"reflect"
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
