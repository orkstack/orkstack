package dsl_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// The `sh` helper is documented (docs/DSL.md, "Templating") and implemented by
// the Orker's renderer; a document that uses it must therefore compile, which
// means the DSL's parse-time stubs have to know the name.
func TestCompile_AcceptsTheShHelper(t *testing.T) {
	source := `
apiVersion: orkstack/v1
kind: Orkflow
name: quoted
inputs:
  name:
    type: string
tasks:
  - id: greet
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo {{ sh .inputs.name }}"]
`
	if _, err := dsl.Compile([]byte(source)); err != nil {
		t.Fatalf("a document using the sh helper must compile: %v", err)
	}
	if _, err := dsl.Compile([]byte(strings.Replace(source, "sh .inputs.name", "quote .inputs.name", 1))); err == nil {
		t.Fatal("an unknown helper must still be refused")
	}
}
