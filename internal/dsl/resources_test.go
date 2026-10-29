package dsl_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// Rule 6d: a resource request is refused at validate, with its path, when it
// does not read as a limit in the grammar the Orker reads it with.
func TestCompile_RefusesAResourceRequestTheOrkerCannotRead(t *testing.T) {
	document := func(cpu, memory string) string {
		return `
apiVersion: orkstack/v1
kind: Orkflow
name: sized
tasks:
  - id: build
    uses: shell
    with: {command: "make"}
    resources:
      cpu: "` + cpu + `"
      memory: "` + memory + `"
`
	}
	for _, tc := range []struct {
		cpu, memory string
		field       string // empty: accepted
	}{
		{"2", "4Gi", ""},
		{"500m", "512Mi", ""},
		{"0.25", "1000000", ""},
		{"banana", "4Gi", "tasks[0].resources.cpu"},
		{"8 cores", "4Gi", "tasks[0].resources.cpu"},
		{"0", "4Gi", "tasks[0].resources.cpu"},
		{"-1", "4Gi", "tasks[0].resources.cpu"},
		{"2", "4GB", "tasks[0].resources.memory"},
		{"2", "0", "tasks[0].resources.memory"},
		{"2", "lots", "tasks[0].resources.memory"},
	} {
		_, err := dsl.Compile([]byte(document(tc.cpu, tc.memory)))
		if tc.field == "" {
			if err != nil {
				t.Errorf("cpu %q memory %q refused: %v", tc.cpu, tc.memory, err)
			}
			continue
		}
		var verr *dsl.ValidationError
		if !errors.As(err, &verr) || verr.Field != tc.field {
			t.Errorf("cpu %q memory %q = %v; want a ValidationError on %s", tc.cpu, tc.memory, err, tc.field)
			continue
		}
		if !strings.Contains(err.Error(), `task "build"`) {
			t.Errorf("the refusal does not name the task: %v", err)
		}
	}
}
