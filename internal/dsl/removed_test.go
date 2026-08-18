// This file, and removed.go beside it, are the ONLY two in internal/dsl that
// may spell the removed key. They exist to reject it, and a rule forbidding
// the compiler to name what it rejects would forbid the rejection.
// proof/M10.1/naming-sweep.sh exempts exactly this pair and holds every other
// file in the package to the ban.
package dsl_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// TestCompile_QueueFieldIsRejectedAndNamesPool is M10.1's DSL canary.
//
// `queue:` named a Temporal task queue in a document whose author has no
// business knowing Temporal exists. It is gone, and it is NOT aliased: a
// silent alias would copy a queue name into `pool:` and leave the tree passing
// a naming gate while still wrong. So the document fails, and the failure says
// what to write instead.
func TestCompile_QueueFieldIsRejectedAndNamesPool(t *testing.T) {
	const doc = `
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    queue: gpu-orkers
    with: {command: "echo 1"}
`
	wf, err := dsl.Compile([]byte(doc))
	if err == nil {
		t.Fatalf("Compile accepted a document with the removed key; got orkflow %v", wf)
	}
	message := err.Error()
	for _, want := range []string{"settle", "pool:"} {
		if !strings.Contains(message, want) {
			t.Errorf("error %q does not name %q", message, want)
		}
	}
	// The rejection must not double as an alias.
	if wf != nil {
		t.Errorf("Compile returned an orkflow alongside the error: %v", wf)
	}
}

// TestCompile_RejectsTheRemovedKeyEvenWhenEmpty stops the obvious escape: a
// document that writes the key with no value is still a document written
// against the old contract, and yaml decodes it to the zero value.
func TestCompile_RejectsTheRemovedKeyEvenWhenEmpty(t *testing.T) {
	const doc = `
name: nightly-close
tasks:
  - id: settle
    plugin: shell
    queue: ""
    with: {command: "echo 1"}
`
	if _, err := dsl.Compile([]byte(doc)); err == nil {
		t.Fatal("Compile accepted the removed key with an empty value")
	} else if !strings.Contains(err.Error(), "pool:") {
		t.Errorf("error %q does not name pool:", err.Error())
	}
}
