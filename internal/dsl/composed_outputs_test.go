package dsl_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// outputsDoc is an Orkflow with a container producer, an awaited task, a
// failure handler and one artifact input, and the given outputs: block.
func outputsDoc(outputs string) string {
	return `
apiVersion: orkstack/v1
kind: Orkflow
name: train
version: 1.0.0
inputs:
  dataset:
    type: artifact
    required: true
tasks:
  - id: fit
    uses: shell
    with:
      op: exec
      command:
        - fit
        - '{{ artifact "_inputs/dataset" }}'
  - id: wait
    await:
      timeout_seconds: 60
  - id: cleanup
    uses: shell
    needs:
      - fit
    join: any_failed
    with:
      op: exec
      command:
        - cleanup
outputs:
` + outputs
}

func TestCompile_ArtifactOutputs(t *testing.T) {
	wf, err := dsl.Compile([]byte(outputsDoc("  accuracy: '{{ .tasks.fit.output.accuracy }}'\n" +
		"  model:\n    artifact: fit/model.bin\n  data:\n    artifact: _inputs/dataset\n  channel: stable\n")))
	must.OKf(t, err, "Compile")
	if got := wf.GetOutputs(); len(got) != 2 || got["accuracy"] != "{{ .tasks.fit.output.accuracy }}" || got["channel"] != "stable" {
		t.Fatalf("outputs = %v", got)
	}
	arts := wf.GetOutputArtifacts()
	if len(arts) != 2 || arts["model"].GetTaskId() != "fit" || arts["model"].GetName() != "model.bin" ||
		arts["data"].GetTaskId() != "_inputs" || arts["data"].GetName() != "dataset" {
		t.Fatalf("output_artifacts = %v", arts)
	}
	if !dsl.OutputsDeclared(wf) {
		t.Fatal("OutputsDeclared = false with outputs declared")
	}
	only, err := dsl.Compile([]byte(outputsDoc("  model:\n    artifact: fit/model.bin\n")))
	must.OKf(t, err, "Compile artifact-only outputs")
	if len(only.GetOutputs()) != 0 || !dsl.OutputsDeclared(only) {
		t.Fatalf("artifact-only outputs: values %v, declared %v", only.GetOutputs(), dsl.OutputsDeclared(only))
	}
}

func TestCompile_ArtifactOutputsStrictYAML(t *testing.T) {
	cases := []struct {
		name, outputs, field, want string
	}{
		{"unknown key", "  model:\n    artefact: fit/model.bin\n", "outputs.model",
			`outputs.model: an output is a template string or {artifact: <task>/<name>}; "artefact" is not a key of one`},
		{"two keys", "  model:\n    artifact: fit/model.bin\n    name: x\n", "outputs.model",
			`outputs.model: an output is a template string or {artifact: <task>/<name>}; "name" is not a key of one`},
		{"not a string", "  model:\n    artifact: 3\n", "outputs.model",
			`outputs.model.artifact must be a string "<task>/<name>"`},
		{"a list", "  model:\n    - fit/model.bin\n", "outputs.model",
			`outputs.model: an output is a template string or {artifact: <task>/<name>}`},
		{"no slash", "  model:\n    artifact: fit\n", "outputs.model",
			`outputs.model: "fit" must be "<task>/<name>"`},
		{"template", "  model:\n    artifact: '{{ .inputs.x }}/y'\n", "outputs.model",
			`outputs.model: a binding is a literal "<task>/<name>", never a template`},
		{"unknown task", "  model:\n    artifact: ghost/x\n", "outputs.model",
			`outputs.model: "ghost/x" names unknown task "ghost"`},
		{"awaited", "  model:\n    artifact: wait/x\n", "outputs.model",
			`outputs.model: "wait" is an awaited task and produces no artifacts`},
		{"undeclared input", "  model:\n    artifact: _inputs/other\n", "outputs.model",
			`outputs.model: "_inputs/other" names no artifact input of this Orkflow (declared: dataset)`},
		{"a failure handler", "  model:\n    artifact: cleanup/x\n", "outputs.model",
			`outputs.model: task "cleanup" has join: any_failed, so it is always skipped when the run succeeds`},
		{"name grammar", "  Model:\n    artifact: fit/x\n", "outputs.Model",
			`outputs.Model: an output name is a template field name`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(outputsDoc(tc.outputs)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				t.Fatalf("not anchored at %s: %#v", tc.field, err)
			}
		})
	}
	t.Run("at most sixteen", func(t *testing.T) {
		var b strings.Builder
		for i := range 17 {
			b.WriteString("  out_" + string(rune('a'+i)) + ":\n    artifact: fit/f\n")
		}
		_, err := dsl.Compile([]byte(outputsDoc(b.String())))
		if err == nil || !strings.Contains(err.Error(), "declares 17 artifact outputs; at most 16") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestCompat_ArtifactRetypedIsBreaking(t *testing.T) {
	compile := func(inputs, outputs string) *orkstackv1.Orkflow {
		t.Helper()
		wf, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: train
version: 1.0.0
inputs:
` + inputs + `
tasks:
  - id: fit
    uses: shell
    with:
      op: exec
      command:
        - fit
outputs:
` + outputs))
		must.OKf(t, err, "Compile")
		return wf
	}
	value := compile("  data:\n    type: string\n", "  model: '{{ .tasks.fit.output.m }}'\n")
	artifact := compile("  data:\n    type: artifact\n    required: true\n", "  model:\n    artifact: fit/model.bin\n")
	untyped := compile("  data: {}\n", "  model:\n    artifact: fit/model.bin\n")
	findings, verdict := dsl.CompareInterfaces(value, artifact)
	if verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
		t.Fatalf("value → artifact is %v: %v", verdict, findings)
	}
	var retyped []string
	for _, f := range findings {
		if f.GetChange() == orkstackv1.InterfaceChange_INTERFACE_CHANGE_RETYPED {
			retyped = append(retyped, f.GetField())
		}
	}
	if !slices.Equal(retyped, []string{"inputs.data", "outputs.model"}) {
		t.Fatalf("retyped = %v, want the input and the output", retyped)
	}
	if _, verdict := dsl.CompareInterfaces(artifact, untyped); verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
		t.Fatalf("artifact → untyped is %v, want BREAKING", verdict)
	}
	added := compile("  data:\n    type: artifact\n    required: true\n",
		"  model:\n    artifact: fit/model.bin\n  log:\n    artifact: fit/train.log\n")
	if _, verdict := dsl.CompareInterfaces(artifact, added); verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE {
		t.Fatalf("adding an artifact output is %v, want COMPATIBLE", verdict)
	}
	if _, verdict := dsl.CompareInterfaces(added, artifact); verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
		t.Fatalf("removing an artifact output is %v, want BREAKING", verdict)
	}
}
