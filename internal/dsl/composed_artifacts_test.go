package dsl_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// artifactChild is a child Orkflow with one artifact input, named literally
// by fit and forwarded by package, and never named by report.
const artifactChild = `
apiVersion: orkstack/v1
kind: Orkflow
name: train
version: 1.0.0
inputs:
  epochs:
    type: number
    default: 3
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
        - '{{ .inputs.epochs }}'
  - id: report
    uses: shell
    needs:
      - fit
    with:
      op: exec
      command:
        - report
  - id: package
    uses: orkflow:package-model@2.0.0
    needs:
      - fit
    artifacts:
      weights: fit/model.bin
      dataset: _inputs/dataset
`

// artifactParent binds the child's dataset input to an artifact of build.
const artifactParent = `
apiVersion: orkstack/v1
kind: Orkflow
name: release-model
version: 4.1.0
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command:
        - build
  - id: train
    uses: orkflow:train@1.0.0
    needs:
      - build
    with:
      epochs: 5
    artifacts:
      dataset: build/dataset.tar.gz
`

func taskByID(t *testing.T, wf *orkstackv1.Orkflow, id string) *orkstackv1.Task {
	t.Helper()
	for _, task := range wf.GetTasks() {
		if task.GetId() == id {
			return task
		}
	}
	t.Fatalf("no task %q", id)
	return nil
}

func TestCompile_ComposedTaskArtifactBindings(t *testing.T) {
	wf, err := dsl.Compile([]byte(artifactParent))
	must.OKf(t, err, "Compile parent")
	got := taskByID(t, wf, "train").GetChildArtifacts()
	if len(got) != 1 || got[0].GetInput() != "dataset" || got[0].GetTaskId() != "build" || got[0].GetName() != "dataset.tar.gz" {
		t.Fatalf("child_artifacts = %v", got)
	}

	child, err := dsl.Compile([]byte(artifactChild))
	must.OKf(t, err, "Compile child")
	var bindings []string
	for _, b := range taskByID(t, child, "package").GetChildArtifacts() {
		bindings = append(bindings, b.GetInput()+"="+b.GetTaskId()+"/"+b.GetName())
	}
	// Sorted by input, whatever the document's order.
	if want := []string{"dataset=_inputs/dataset", "weights=fit/model.bin"}; !slices.Equal(bindings, want) {
		t.Fatalf("bindings = %v, want %v", bindings, want)
	}
}

func TestCompile_DeliverySetFromLiteralHelperCalls(t *testing.T) {
	child, err := dsl.Compile([]byte(artifactChild))
	must.OKf(t, err, "Compile child")
	if got := taskByID(t, child, "fit").GetInputArtifacts(); !slices.Equal(got, []string{"dataset"}) {
		t.Fatalf("fit input_artifacts = %v, want [dataset]", got)
	}
	if got := taskByID(t, child, "report").GetInputArtifacts(); len(got) != 0 {
		t.Fatalf("report input_artifacts = %v, want none: it names no _inputs", got)
	}
	// A composed task forwards the reference; the engine resolves it, and
	// nothing is staged for it.
	if got := taskByID(t, child, "package").GetInputArtifacts(); len(got) != 0 {
		t.Fatalf("package input_artifacts = %v, want none", got)
	}
}

func TestCompile_ArtifactInputRules(t *testing.T) {
	doc := func(inputs, with string) string {
		return `
apiVersion: orkstack/v1
kind: Orkflow
name: child
inputs:
` + inputs + `
tasks:
  - id: fit
    uses: shell
    with:
      op: exec
      command:
        - fit
` + with
	}
	named := "        - '{{ artifact \"_inputs/data\" }}'\n"
	cases := []struct {
		name, inputs, with, field, want string
	}{
		{"not required", "  data:\n    type: artifact\n", named, "inputs.data",
			`inputs.data: an artifact input is required: write required: true`},
		{"required false", "  data:\n    type: artifact\n    required: false\n", named, "inputs.data",
			`inputs.data: an artifact input is required: write required: true`},
		{"default", "  data:\n    type: artifact\n    required: true\n    default: x\n", named, "inputs.data",
			`inputs.data: an artifact input takes no default`},
		{"name grammar", "  Data:\n    type: artifact\n    required: true\n", "        - '{{ artifact \"_inputs/Data\" }}'\n", "inputs.Data",
			`inputs.Data: an artifact input's name is ^[a-z][a-z0-9_]*$`},
		{"value read", "  data:\n    type: artifact\n    required: true\n", "        - '{{ .inputs.data }}'\n", "tasks[0].with.command[1]",
			`input "data" is an artifact; read it as a file with {{ artifact "_inputs/data" }}`},
		{"undeclared", "  data:\n    type: artifact\n    required: true\n", "        - '{{ artifact \"_inputs/other\" }}'\n", "tasks[0].with.command[1]",
			`"_inputs/other" names no artifact input of this Orkflow (declared: data)`},
		{"value input", "  data:\n    type: string\n", "        - '{{ artifact \"_inputs/data\" }}'\n", "tasks[0].with.command[1]",
			`"_inputs/data" names no artifact input of this Orkflow (declared: none)`},
		{"computed argument", "  data:\n    type: artifact\n    required: true\n  which:\n    type: string\n",
			"        - '{{ artifact .inputs.which }}'\n", "tasks[0].with.command[1]",
			`an Orkflow with artifact inputs names every artifact literally`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(doc(tc.inputs, tc.with)))
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
			b.WriteString("  in_" + string(rune('a'+i)) + ":\n    type: artifact\n    required: true\n")
		}
		_, err := dsl.Compile([]byte(doc(b.String(), "        - '{{ artifact \"_inputs/in_a\" }}'\n")))
		if err == nil || !strings.Contains(err.Error(), "declares 17 artifact inputs; at most 16") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("a dynamic argument stays legal without artifact inputs", func(t *testing.T) {
		_, err := dsl.Compile([]byte(doc("  which:\n    type: string\n", "        - '{{ artifact .inputs.which }}'\n")))
		must.OKf(t, err, "Compile")
	})
}

func TestCompile_ArtifactBindingRules(t *testing.T) {
	doc := func(childArtifacts string) string {
		return `
apiVersion: orkstack/v1
kind: Orkflow
name: parent
inputs:
  given:
    type: artifact
    required: true
  plain:
    type: string
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command:
        - build
  - id: other
    uses: shell
    with:
      op: exec
      command:
        - other
  - id: wait
    await:
      timeout_seconds: 60
  - id: train
    uses: orkflow:train@1.0.0
    needs:
      - build
      - wait
    artifacts:
` + childArtifacts
	}
	cases := []struct {
		name, line, field, want string
	}{
		{"no slash", "      dataset: build\n", "tasks[3].artifacts.dataset",
			`artifacts.dataset: "build" must be "<task>/<name>"`},
		{"template", "      dataset: '{{ .inputs.plain }}/x'\n", "tasks[3].artifacts.dataset",
			`artifacts.dataset: a binding is a literal "<task>/<name>", never a template`},
		{"outside the closure", "      dataset: other/x.tar\n", "tasks[3].artifacts.dataset",
			`task "other" is not in the needs closure of "train"`},
		{"unknown task", "      dataset: ghost/x.tar\n", "tasks[3].artifacts.dataset",
			`names unknown task "ghost"`},
		{"awaited producer", "      dataset: wait/x.tar\n", "tasks[3].artifacts.dataset",
			`"wait" is an awaited task and produces no artifacts`},
		{"value input", "      dataset: _inputs/plain\n", "tasks[3].artifacts.dataset",
			`"_inputs/plain" names no artifact input of this Orkflow (declared: given)`},
		{"input name grammar", "      Data-set: build/x.tar\n", "tasks[3].artifacts.Data-set",
			`artifacts.Data-set: an artifact input's name is ^[a-z][a-z0-9_]*$`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(doc(tc.line)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				t.Fatalf("not anchored at %s: %#v", tc.field, err)
			}
		})
	}

	t.Run("an _inputs binding forwards", func(t *testing.T) {
		wf, err := dsl.Compile([]byte(doc("      dataset: _inputs/given\n")))
		must.OKf(t, err, "Compile")
		got := taskByID(t, wf, "train").GetChildArtifacts()
		if len(got) != 1 || got[0].GetTaskId() != "_inputs" || got[0].GetName() != "given" {
			t.Fatalf("child_artifacts = %v", got)
		}
	})

	t.Run("only a composed task binds artifacts", func(t *testing.T) {
		_, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: parent
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command:
        - build
  - id: use
    uses: shell
    needs:
      - build
    artifacts:
      dataset: build/x.tar
    with:
      op: exec
      command:
        - use
`))
		if err == nil || !strings.Contains(err.Error(), `task "use": artifacts: binds a child Orkflow's artifact inputs, so only a composed task (uses: orkflow:…) takes it`) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestCanonical_ArtifactFieldsAbsentKeepDigest(t *testing.T) {
	wf, err := dsl.Compile([]byte(artifactParent))
	must.OKf(t, err, "Compile")
	with, err := dsl.DefinitionDigest(wf)
	must.OKf(t, err, "digest")
	bare := proto.Clone(wf).(*orkstackv1.Orkflow)
	taskByID(t, bare, "train").ChildArtifacts = nil
	without, err := dsl.DefinitionDigest(bare)
	must.OKf(t, err, "digest")
	if with == without {
		t.Fatal("a binding does not change the digest; it must, since it changes what runs")
	}
	// A definition with no artifact field emits neither key, so its digest is
	// the one it had before the fields existed: empty lists read as absent.
	empty := proto.Clone(bare).(*orkstackv1.Orkflow)
	taskByID(t, empty, "train").ChildArtifacts = []*orkstackv1.ChildArtifact{}
	taskByID(t, empty, "build").InputArtifacts = []string{}
	emptyDigest, err := dsl.DefinitionDigest(empty)
	must.OKf(t, err, "digest")
	if emptyDigest != without {
		t.Fatalf("empty lists changed the digest: %s != %s", emptyDigest, without)
	}

	child, err := dsl.Compile([]byte(artifactChild))
	must.OKf(t, err, "Compile child")
	delivered, err := dsl.DefinitionDigest(child)
	must.OKf(t, err, "digest")
	undelivered := proto.Clone(child).(*orkstackv1.Orkflow)
	taskByID(t, undelivered, "fit").InputArtifacts = nil
	other, err := dsl.DefinitionDigest(undelivered)
	must.OKf(t, err, "digest")
	if delivered == other {
		t.Fatal("a delivery set does not change the digest; it must, since it changes what is staged")
	}
}

// FuzzParseArtifactBinding states what a binding parse claims: an accepted
// value is exactly "<task>/<name>" split at its first "/", both halves
// non-empty and free of NUL, and never a template.
func FuzzParseArtifactBinding(f *testing.F) {
	for _, seed := range []string{"build/dataset.tar.gz", "_inputs/dataset", "build", "/x", "x/", "a/b/c",
		"{{ .inputs.x }}/y", "a\x00/b", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		taskID, name, err := dsl.ParseArtifactBinding(value)
		if err != nil {
			if taskID != "" || name != "" {
				t.Fatalf("refused %q but returned %q, %q", value, taskID, name)
			}
			return
		}
		if taskID+"/"+name != value || strings.Contains(taskID, "/") {
			t.Fatalf("%q split as %q / %q", value, taskID, name)
		}
		if taskID == "" || name == "" || strings.ContainsRune(value, 0) || strings.Contains(value, "{{") {
			t.Fatalf("accepted %q", value)
		}
	})
}
