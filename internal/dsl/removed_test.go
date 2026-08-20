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

// TestCompile_RejectsTheRemovedKeyHoweverItIsSpelled stops the obvious escape:
// a document that writes the key with no value is still a document written
// against the old contract. Every YAML spelling of a valueless key is here,
// because the first version of this test fed only a valued key: the compiler
// decoded the key into a *string, yaml.v3 leaves that pointer nil for an
// explicitly null value, and `queue:` and `queue: null` compiled in silence
// while `queue: ""` was refused. The rejection reads the key's PRESENCE now,
// and this table is what says so.
func TestCompile_RejectsTheRemovedKeyHoweverItIsSpelled(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"a queue name", "ork-tasks"},
		{"an empty string", `""`},
		{"no value at all", ""},
		{"an explicit null", "null"},
		{"a null tilde", "~"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := "name: nightly-close\ntasks:\n  - id: settle\n    plugin: shell\n" +
				"    queue: " + tc.value + "\n    with: {command: \"echo 1\"}\n"
			wf, err := dsl.Compile([]byte(doc))
			if err == nil {
				t.Fatalf("Compile accepted the removed key written as `queue: %s`; got %v", tc.value, wf)
			}
			for _, want := range []string{"settle", "pool:"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err.Error(), want)
				}
			}
		})
	}
}
