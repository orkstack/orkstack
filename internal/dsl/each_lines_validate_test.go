package dsl

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// linesOrkflow is an Orkflow whose task "convert" fans out with each.
func linesOrkflow(each, with string) string {
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n" +
		"  - id: list\n    uses: busybox:1\n" +
		"  - id: wait\n    await:\n      timeout_seconds: 60\n" +
		"  - id: nap\n    sleep:\n      seconds: 5\n" +
		"  - id: fan\n    uses: busybox:1\n    needs: [list]\n    each:\n      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 2\n" +
		"  - id: convert\n    uses: busybox:1\n    needs: [list, wait, nap]\n    each:\n" + each
	if with != "" {
		doc += "    with:\n" + with
	}
	return doc
}

func TestValidateEach_OverArtifact(t *testing.T) {
	cases := []struct {
		name, each, with, refusal string
	}{
		{name: "jsonl", each: "      over_artifact: list/files.jsonl\n      format: jsonl\n      max_items: 10\n",
			with: "      src: '{{ .each.item.path }}'\n"},
		{name: "lines", each: "      over_artifact: list/files.txt\n      format: lines\n      max_items: 10\n      as: file\n"},
		{name: "over and over_artifact", each: "      over: '{{ .tasks.list.output.x }}'\n      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			refusal: "both set"},
		{name: "format missing", each: "      over_artifact: list/a.jsonl\n      max_items: 1\n", refusal: "each.format is required"},
		{name: "format unknown", each: "      over_artifact: list/a.jsonl\n      format: csv\n      max_items: 1\n", refusal: "not jsonl or lines"},
		{name: "format with over", each: "      over: '{{ .tasks.list.output.x }}'\n      format: jsonl\n      max_items: 1\n",
			refusal: "read only with each.over_artifact"},
		{name: "producer not a direct need", each: "      over_artifact: other/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			refusal: "unknown task"},
		{name: "producer an await", each: "      over_artifact: wait/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			refusal: "awaited task"},
		{name: "producer a sleep", each: "      over_artifact: nap/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			refusal: "sleeping"},
		{name: "producer a fan-out", each: "      over_artifact: fan/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			refusal: "not in its needs:"},
		{name: "an input this Orkflow does not declare", each: "      over_artifact: _inputs/a\n      format: jsonl\n      max_items: 1\n",
			refusal: "names no artifact input of this Orkflow (declared: none)"},
		{name: "path in name", each: "      over_artifact: list/a/b\n      format: jsonl\n      max_items: 1\n",
			refusal: "holds a \"/\""},
		{name: "template reads the source", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			with: "      f: '{{ artifact \"list/a.jsonl\" }}'\n", refusal: "the fan-out's own source"},
		{name: "template reads another artifact", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n",
			with: "      f: '{{ artifact \"list/b.json\" }}'\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := linesOrkflow(tc.each, tc.with)
			_, err := Compile([]byte(doc))
			if tc.refusal == "" {
				if err != nil {
					t.Fatalf("Compile: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("Compile = %v; want a refusal containing %q", err, tc.refusal)
			}
		})
	}
}

func TestValidateEach_OverArtifactNeedsTheProducerDirectly(t *testing.T) {
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n" +
		"  - id: list\n    uses: busybox:1\n" +
		"  - id: mid\n    uses: busybox:1\n    needs: [list]\n" +
		"  - id: convert\n    uses: busybox:1\n    needs: [mid]\n    each:\n      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n"
	_, err := Compile([]byte(doc))
	if err == nil || !strings.Contains(err.Error(), "not in its needs:") {
		t.Fatalf("Compile = %v; want the direct-needs refusal", err)
	}
}

func TestValidateEach_Batch(t *testing.T) {
	cases := []struct {
		name, each, with, refusal string
	}{
		{name: "batch", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 500\n      max_items: 4\n",
			with: "      label: '{{ .each.index }} of {{ .each.count }}'\n"},
		{name: "batch at max", each: "      over_artifact: list/a.txt\n      format: lines\n      batch: 10000\n      max_items: 1\n"},
		{name: "batch zero", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 0\n      max_items: 1\n", refusal: "out of range"},
		{name: "batch over max", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 10001\n      max_items: 1\n", refusal: "out of range"},
		{name: "batch with over", each: "      over: '{{ .tasks.list.output.x }}'\n      batch: 3\n      max_items: 1\n",
			refusal: "batch an array in the task that writes it"},
		{name: "as with batch", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 3\n      as: row\n      max_items: 1\n",
			refusal: "each.as is refused with batch"},
		{name: "item key with batch", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 3\n      max_items: 1\n",
			with: "      v: '{{ .each.item }}'\n", refusal: "only .each.index and .each.count"},
		{name: "bare each with batch", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 3\n      max_items: 1\n",
			with: "      v: '{{ .each }}'\n", refusal: "only .each.index and .each.count"},
		{name: "item key with batch 1", each: "      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 1\n      max_items: 1\n",
			with: "      v: '{{ .each.item }}'\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(linesOrkflow(tc.each, tc.with)))
			if tc.refusal == "" {
				if err != nil {
					t.Fatalf("Compile: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("Compile = %v; want a refusal containing %q", err, tc.refusal)
			}
		})
	}
}

func TestValidateEach_LinesOnTheCompiledMessage(t *testing.T) {
	wf, err := Compile([]byte(linesOrkflow("      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 3\n      max_items: 1\n", "")))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*orkstackv1.EachSpec)
		refusal string
	}{
		{"batch 1 hand-built", func(e *orkstackv1.EachSpec) { e.Batch = 1 }, "compiled as no batch"},
		{"source disagrees", func(e *orkstackv1.EachSpec) { e.Source.Artifact.Name = "b.jsonl" }, "does not match each.over_artifact"},
		{"as with batch", func(e *orkstackv1.EachSpec) { e.As = "row" }, "each.as is refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone := proto.Clone(wf).(*orkstackv1.Orkflow)
			tc.mutate(clone.GetTasks()[4].GetEach())
			if err := validate(clone, Options{}); err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("Validate = %v; want %q", err, tc.refusal)
			}
		})
	}
}

func TestCompileEach_BatchOneNormalises(t *testing.T) {
	one, err := Compile([]byte(linesOrkflow("      over_artifact: list/a.jsonl\n      format: jsonl\n      batch: 1\n      max_items: 1\n", "")))
	if err != nil {
		t.Fatal(err)
	}
	none, err := Compile([]byte(linesOrkflow("      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n", "")))
	if err != nil {
		t.Fatal(err)
	}
	// The minted version hashes the document's text, which differs; the
	// definition must not.
	one.Version, none.Version = "", ""
	a, err := CanonicalOrkflow(one)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalOrkflow(none)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("batch: 1 and no batch compile to different definitions:\n%s\n%s", a, b)
	}
	if got := one.GetTasks()[4].GetEach().GetBatch(); got != 0 {
		t.Fatalf("batch: 1 compiled as %d; want 0", got)
	}
}

// TestCompileEach_DigestUnchangedWithoutLines: a fan-out over an array
// canonicalises exactly as it did before the lines keys existed.
func TestCompileEach_DigestUnchangedWithoutLines(t *testing.T) {
	wf, err := Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  l:\n    type: array\ntasks:\n" +
		"  - id: a\n    uses: busybox:1\n    each:\n      over: '{{ .inputs.l }}'\n      max_items: 2\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := CanonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"overArtifact", "format", "batch", "artifact"} {
		if strings.Contains(string(got), `"`+key+`"`) {
			t.Fatalf("an array fan-out's canonical form holds %q:\n%s", key, got)
		}
	}
	const want = `"each":{"as":"item","maxItems":2,"maxParallel":0,"over":"{{ .inputs.l }}","source":{"input":"l","path":[],"taskId":""}}`
	if !strings.Contains(string(got), want) {
		t.Fatalf("canonical each changed:\n%s\nwant it to contain\n%s", got, want)
	}
}

func TestEachSourceLabel(t *testing.T) {
	for _, tc := range []struct {
		spec *orkstackv1.EachSpec
		want string
	}{
		{&orkstackv1.EachSpec{Over: "{{ .inputs.l }}"}, "{{ .inputs.l }}"},
		{&orkstackv1.EachSpec{OverArtifact: "list/files.jsonl"}, "the lines of list/files.jsonl"},
		{&orkstackv1.EachSpec{OverArtifact: "list/urls.jsonl", Batch: 500}, "batches of 500 lines of list/urls.jsonl"},
	} {
		if got := EachSourceLabel(tc.spec); got != tc.want {
			t.Errorf("EachSourceLabel(%v) = %q; want %q", tc.spec, got, tc.want)
		}
	}
}

// inputsFanOut is an Orkflow with one artifact input, rows, and one value
// input, n, whose only task fans out over source.
func inputsFanOut(source, with string) string {
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n" +
		"  rows:\n    type: artifact\n    required: true\n  label:\n    type: string\n" +
		"tasks:\n  - id: convert\n    uses: busybox:1\n    each:\n" +
		"      over_artifact: " + source + "\n      format: jsonl\n      max_items: 4\n"
	if with != "" {
		doc += "    with:\n" + with
	}
	return doc
}

// TestValidateEach_OverAnArtifactInput: a fan-out runs over the lines of an
// artifact input this Orkflow declares, with no needs: of its own; a name
// that is no artifact input, and a template that reads the source, are
// refused, and the source is never in the fan-out's delivery set.
func TestValidateEach_OverAnArtifactInput(t *testing.T) {
	for _, tc := range []struct{ name, source, with, refusal string }{
		{"a declared artifact input", "_inputs/rows", "      v: '{{ .each.item.n }}'\n", ""},
		{"an undeclared name", "_inputs/other", "", "names no artifact input of this Orkflow (declared: rows)"},
		{"a value input", "_inputs/label", "", "names no artifact input of this Orkflow (declared: rows)"},
		{"another reserved producer", "_runs/rows", "", "reserved"},
		{"a template reads the source", "_inputs/rows", "      f: '{{ artifact \"_inputs/rows\" }}'\n", "the fan-out's own source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := Compile([]byte(inputsFanOut(tc.source, tc.with)))
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("Compile = %v; want a refusal containing %q", err, tc.refusal)
				}
				return
			}
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			got := wf.GetTasks()[0]
			art := got.GetEach().GetSource().GetArtifact()
			if art.GetProducer() != InputsProducer || art.GetName() != "rows" || len(got.GetInputArtifacts()) != 0 {
				t.Fatalf("source = %v, input_artifacts = %v; want _inputs/rows and none delivered", art, got.GetInputArtifacts())
			}
		})
	}
}

// TestValidateEach_OverAComposedTasksArtifact: a composed task is a producer
// of the artifacts its child returns, needed directly as any other.
func TestValidateEach_OverAComposedTasksArtifact(t *testing.T) {
	doc := func(needs string) string {
		return "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n" +
			"  - id: train\n    uses: orkflow:train@1.0.0\n" +
			"  - id: convert\n    uses: busybox:1\n" + needs +
			"    each:\n      over_artifact: train/rows\n      format: jsonl\n      max_items: 4\n"
	}
	if _, err := Compile([]byte(doc("    needs: [train]\n"))); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := Compile([]byte(doc(""))); err == nil || !strings.Contains(err.Error(), "not in its needs:") {
		t.Fatalf("Compile without the need = %v", err)
	}
}
