package dsl

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// handBuiltArtifactChild is a definition that never met the YAML decoder, as
// a request arriving at internal/api would carry it: fit names the artifact
// input data literally, report names nothing, and train forwards data.
func handBuiltArtifactChild(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: child
inputs:
  data:
    type: artifact
    required: true
tasks:
  - id: fit
    uses: shell
    with:
      op: exec
      command:
        - fit
        - '{{ artifact "_inputs/data" }}'
  - id: report
    uses: shell
    needs:
      - fit
    with:
      op: exec
      command:
        - report
  - id: train
    uses: orkflow:train@1.0.0
    needs:
      - fit
    artifacts:
      data: _inputs/data
      weights: fit/model.bin
`))
	must.OKf(t, err, "Compile")
	return wf
}

// A hand-built definition meets the compiled rules (55 and 56): a delivery
// set its templates do not name, a binding on a container task or bindings
// out of order are refused on the message itself.
func TestValidate_ArtifactFieldsOnTheCompiledMessage(t *testing.T) {
	byID := func(wf *orkstackv1.Orkflow, id string) *orkstackv1.Task {
		for _, task := range wf.GetTasks() {
			if task.GetId() == id {
				return task
			}
		}
		t.Fatalf("no task %q", id)
		return nil
	}
	cases := []struct {
		name string
		edit func(*orkstackv1.Orkflow)
		want string
	}{
		{"a delivery set its templates do not name", func(wf *orkstackv1.Orkflow) {
			byID(wf, "report").InputArtifacts = []string{"data"}
		}, `task "report": input_artifacts [data] is not the set its templates name literally ([])`},
		{"a delivery set missing a named input", func(wf *orkstackv1.Orkflow) {
			byID(wf, "fit").InputArtifacts = nil
		}, `task "fit": input_artifacts [] is not the set its templates name literally ([data])`},
		{"a binding on a container task", func(wf *orkstackv1.Orkflow) {
			byID(wf, "report").ChildArtifacts = []*orkstackv1.ChildArtifact{{Input: "x", TaskId: "fit", Name: "a"}}
		}, "only a composed task (uses: orkflow:…) takes it"},
		{"bindings out of order", func(wf *orkstackv1.Orkflow) {
			b := byID(wf, "train").ChildArtifacts
			b[0], b[1] = b[1], b[0]
		}, "child_artifacts are sorted by input, each input once"},
		{"a binding with no name", func(wf *orkstackv1.Orkflow) {
			byID(wf, "train").ChildArtifacts[1].Name = ""
		}, `artifacts.weights: "fit/" must be "<task>/<name>"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wf := handBuiltArtifactChild(t)
			tc.edit(wf)
			err := validate(wf, Options{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	must.OKf(t, validate(handBuiltArtifactChild(t), Options{}), "the compiled definition validates")
}
