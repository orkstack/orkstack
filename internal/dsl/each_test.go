package dsl_test

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func TestParseEachOver(t *testing.T) {
	accepted := []struct {
		over string
		want *orkstackv1.EachSource
	}{
		{`{{ .inputs.parts }}`, &orkstackv1.EachSource{Input: "parts"}},
		{`{{ .inputs.cfg.parts.all }}`, &orkstackv1.EachSource{Input: "cfg", Path: []string{"parts", "all"}}},
		{`{{ .tasks.list.output.parts }}`, &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}}},
		{`{{ .tasks.list.output.a.b }}`, &orkstackv1.EachSource{TaskId: "list", Path: []string{"a", "b"}}},
		{`{{ index .inputs "my-parts" }}`, &orkstackv1.EachSource{Input: "my-parts"}},
		{`{{ index .inputs "cfg" "a.b" "c-d" }}`, &orkstackv1.EachSource{Input: "cfg", Path: []string{"a.b", "c-d"}}},
		{`{{ index .tasks "list-partitions" "output" "partitions" }}`, &orkstackv1.EachSource{TaskId: "list-partitions", Path: []string{"partitions"}}},
		{`  {{- .tasks.list.output.parts -}}  `, &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}}},
	}
	for _, tc := range accepted {
		got, err := dsl.ParseEachOver(tc.over)
		if err != nil {
			t.Fatalf("ParseEachOver(%q) = %v", tc.over, err)
		}
		if !proto.Equal(got, tc.want) {
			t.Fatalf("ParseEachOver(%q) = %v, want %v", tc.over, got, tc.want)
		}
	}
}

// The guardrail nearest a line — the DSL becoming a language: one reference,
// four shapes, nothing else.
func TestParseEachOver_RefusesEverythingButOneReference(t *testing.T) {
	refused := map[string]string{
		"text around the action":    `parts: {{ .inputs.parts }}`,
		"two actions":               `{{ .inputs.a }}{{ .inputs.b }}`,
		"a pipeline":                `{{ .inputs.parts | len }}`,
		"len":                       `{{ len .inputs.parts }}`,
		"printf":                    `{{ printf "%v" .inputs.parts }}`,
		"the root variable":         `{{ $ }}`,
		"a variable":                `{{ $.inputs.parts }}`,
		"a declaration":             `{{ $x := .inputs.parts }}`,
		"a non-literal index arg":   `{{ index .inputs .inputs.key }}`,
		".tasks.x":                  `{{ .tasks.x }}`,
		".tasks.x.output (no key)":  `{{ .tasks.x.output }}`,
		".tasks.x.artifacts":        `{{ .tasks.x.artifacts.y }}`,
		".inputs alone":             `{{ .inputs }}`,
		".each.y":                   `{{ .each.y }}`,
		"an empty string":           ``,
		"a control structure":       `{{ if .inputs.a }}{{ .inputs.a }}{{ end }}`,
		"index of another root":     `{{ index .each "y" }}`,
		"index with no root":        `{{ index }}`,
		"index .tasks without keys": `{{ index .tasks "x" "output" }}`,
		"index .tasks not output":   `{{ index .tasks "x" "values" "k" }}`,
		"plain text":                `parts`,
		"a chain":                   `{{ (.inputs).parts }}`,
	}
	for name, over := range refused {
		t.Run(name, func(t *testing.T) {
			got, err := dsl.ParseEachOver(over)
			if err == nil {
				t.Fatalf("ParseEachOver(%q) = %v, want a refusal", over, got)
			}
			if !strings.Contains(err.Error(), "accepted:") {
				t.Fatalf("the refusal %q does not show the accepted shapes", err)
			}
		})
	}
}

func eachDoc(task string) []byte {
	return []byte(`
name: fan
inputs:
  parts: {type: array}
  cfg: {type: object}
  label: {type: string}
  untyped: {default: 1}
tasks:
  - id: list
    uses: shell
    with: {op: exec}
` + task)
}

func TestCompile_EachCompiles(t *testing.T) {
	wf, err := dsl.CompileFile("../../examples/each-fan-out.ork.yaml")
	if err != nil {
		t.Fatalf("compile the example: %v", err)
	}
	var convert *orkstackv1.Task
	for _, task := range wf.GetTasks() {
		if task.GetId() == "convert" {
			convert = task
		}
	}
	want := &orkstackv1.EachSpec{
		Over: "{{ .tasks.list.output.result }}", As: "part", MaxItems: 50, MaxParallel: 2,
		Source: &orkstackv1.EachSource{TaskId: "list", Path: []string{"result"}},
	}
	if !proto.Equal(convert.GetEach(), want) {
		t.Fatalf("EachSpec = %v, want %v", convert.GetEach(), want)
	}

	// `as` normalised: absent and `as: item` are one message and one digest.
	absent, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5}
    with: {op: exec, x: "{{ .each.item }}"}
`))
	if err != nil {
		t.Fatalf("compile without as: %v", err)
	}
	explicit, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5, as: item}
    with: {op: exec, x: "{{ .each.item }}"}
`))
	if err != nil {
		t.Fatalf("compile with as: item: %v", err)
	}
	if got := absent.GetTasks()[1].GetEach().GetAs(); got != dsl.DefaultEachAs {
		t.Fatalf("as = %q, want %q", got, dsl.DefaultEachAs)
	}
	// The source differs by two words, so the derived version does too; the
	// definition is what must agree.
	absent.Version = explicit.GetVersion()
	if !proto.Equal(absent.GetTasks()[1], explicit.GetTasks()[1]) {
		t.Fatalf("as absent and as: item compile to two messages")
	}
	a, _ := dsl.DefinitionDigest(absent)
	b, _ := dsl.DefinitionDigest(explicit)
	if a != b {
		t.Fatalf("as absent and as: item digest differently: %s vs %s", a, b)
	}
}

func compileFanOut(t *testing.T, each string) error {
	t.Helper()
	_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    needs: [list]
    each: ` + each + `
    with: {op: exec}
`))
	return err
}

func fieldOf(err error) string {
	var verr *dsl.ValidationError
	if errors.As(err, &verr) {
		return verr.Field
	}
	return ""
}

func TestCompile_EachMaxItemsRequiredAndBounded(t *testing.T) {
	over := `{over: "{{ .inputs.parts }}"`
	err := compileFanOut(t, over+`}`)
	if err == nil || !strings.Contains(err.Error(), "each.max_items is required") || fieldOf(err) != "tasks[1].each.max_items" {
		t.Fatalf("absent max_items = %v (field %q)", err, fieldOf(err))
	}
	for _, n := range []string{"0", "-1", "1001"} {
		if err := compileFanOut(t, over+`, max_items: `+n+`}`); err == nil || fieldOf(err) != "tasks[1].each.max_items" {
			t.Fatalf("max_items %s = %v (field %q)", n, err, fieldOf(err))
		}
	}
	for _, n := range []string{"1", "1000"} {
		if err := compileFanOut(t, over+`, max_items: `+n+`}`); err != nil {
			t.Fatalf("max_items %s = %v", n, err)
		}
	}
	_, err = dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 600}
    with: {op: exec}
  - id: render
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 500}
    with: {op: exec}
`))
	if err == nil || fieldOf(err) != "tasks[2].each.max_items" ||
		!strings.Contains(err.Error(), "convert 600, render 500") || !strings.Contains(err.Error(), "1100 items") {
		t.Fatalf("600 + 500 = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_EachAsRules(t *testing.T) {
	for _, as := range []string{"My", "my-part", "9x", "index", "count", strings.Repeat("a", 65)} {
		err := compileFanOut(t, `{over: "{{ .inputs.parts }}", max_items: 3, as: "`+as+`"}`)
		if err == nil || fieldOf(err) != "tasks[1].each.as" {
			t.Fatalf("as %q = %v (field %q)", as, err, fieldOf(err))
		}
	}
	for _, as := range []string{"part", "my_part", "p2", strings.Repeat("a", 64)} {
		if err := compileFanOut(t, `{over: "{{ .inputs.parts }}", max_items: 3, as: "`+as+`"}`); err != nil {
			t.Fatalf("as %q = %v", as, err)
		}
	}
}

func TestCompile_EachMaxParallelBounds(t *testing.T) {
	for _, p := range []string{"-1", "4"} {
		err := compileFanOut(t, `{over: "{{ .inputs.parts }}", max_items: 3, max_parallel: `+p+`}`)
		if err == nil || fieldOf(err) != "tasks[1].each.max_parallel" {
			t.Fatalf("max_parallel %s = %v (field %q)", p, err, fieldOf(err))
		}
	}
	for _, p := range []string{"0", "1", "3"} {
		if err := compileFanOut(t, `{over: "{{ .inputs.parts }}", max_items: 3, max_parallel: `+p+`}`); err != nil {
			t.Fatalf("max_parallel %s = %v", p, err)
		}
	}
}

func TestCompile_EachRefusesPin(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: fan
inputs: {parts: {type: array}}
pins: {build: {}}
tasks:
  - id: convert
    uses: shell
    pin: build
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err == nil || fieldOf(err) != "tasks[0].pin" || !strings.Contains(err.Error(), "cannot join a pin") {
		t.Fatalf("each + pin = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_EachInputSourceTypeRules(t *testing.T) {
	ok := []string{`{{ .inputs.parts }}`, `{{ .inputs.cfg.parts }}`}
	for _, over := range ok {
		if err := compileFanOut(t, `{over: "`+over+`", max_items: 3}`); err != nil {
			t.Fatalf("%s = %v", over, err)
		}
	}
	refused := []string{`{{ .inputs.label }}`, `{{ .inputs.untyped }}`, `{{ .inputs.parts.x }}`, `{{ .inputs.cfg }}`, `{{ .inputs.ghost }}`}
	for _, over := range refused {
		err := compileFanOut(t, `{over: "`+over+`", max_items: 3}`)
		if err == nil || fieldOf(err) != "tasks[1].each.over" {
			t.Fatalf("%s = %v (field %q)", over, err, fieldOf(err))
		}
	}
	err := compileFanOut(t, `{over: "{{ .inputs.label }}", max_items: 3}`)
	if !strings.Contains(err.Error(), "a fan-out needs an array") {
		t.Fatalf("string input = %v", err)
	}
}

func TestCompile_EachTaskSourceMustBeInTheNeedsClosure(t *testing.T) {
	_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err == nil || fieldOf(err) != "tasks[1].each.over" || !strings.Contains(err.Error(), "not in its needs closure") {
		t.Fatalf("source outside the closure = %v (field %q)", err, fieldOf(err))
	}
	// Transitively in the closure is enough.
	if _, err := dsl.Compile(eachDoc(`
  - id: mid
    uses: shell
    needs: [list]
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [mid]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 3}
    with: {op: exec}
`)); err != nil {
		t.Fatalf("source in the transitive closure = %v", err)
	}
}

func TestCompile_EachTaskSourceMustNotBeAFanOut(t *testing.T) {
	_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
  - id: again
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.convert.output.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err == nil || fieldOf(err) != "tasks[2].each.over" || !strings.Contains(err.Error(), "no nesting") {
		t.Fatalf("a fan-out over a fan-out = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_FanOutMayNotNeedAFanOut(t *testing.T) {
	_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
  - id: again
    uses: shell
    needs: [list, convert]
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err == nil || fieldOf(err) != "tasks[2].needs[1]" {
		t.Fatalf("a fan-out needing a fan-out = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_TemplatesCannotReadAFanOut(t *testing.T) {
	refs := []string{
		`{{ .tasks.convert.output.x }}`,
		`{{ index .tasks "convert" "output" "x" }}`,
		`{{ artifact "convert/x" }}`,
	}
	for _, ref := range refs {
		for _, field := range []string{"with: {op: exec, v: '" + ref + "'}", "condition: '" + ref + "'\n    with: {op: exec}"} {
			_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [convert]
    ` + field + `
`))
			if err == nil || !strings.Contains(err.Error(), "is a fan-out; its items are read as files under /orkstack/input/upstream/convert/<item>/") {
				t.Fatalf("%s in %q = %v", ref, field, err)
			}
			if !strings.HasPrefix(fieldOf(err), "tasks[2].") {
				t.Fatalf("%s: field = %q, want anchored at tasks[2]", ref, fieldOf(err))
			}
		}
	}
	// Reading the items as files compiles.
	if _, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec, command: [cat, /orkstack/input/upstream/convert/00000/values.json]}
`)); err != nil {
		t.Fatalf("a direct dependent that reads files = %v", err)
	}
}

func TestCompile_EachRootOnlyInWithOfAFanOut(t *testing.T) {
	fanOut := func(with string) error {
		_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 3, as: part}
    with: {op: exec, v: '` + with + `'}
`))
		return err
	}
	for _, ok := range []string{`{{ .each.part }}`, `{{ .each.part.uri }}`, `{{ .each.index }}`, `{{ .each.count }}`,
		`{{ index .each "part" }}`, `{{ index .each "part" "uri" }}`, `{{ sh .each.part.name }}`} {
		if err := fanOut(ok); err != nil {
			t.Fatalf("%s = %v", ok, err)
		}
	}
	for _, bad := range []string{`{{ .each.other }}`, `{{ .each }}`, `{{ index .each "item" }}`} {
		err := fanOut(bad)
		if err == nil || !strings.Contains(err.Error(), ".each.part, .each.index and .each.count") {
			t.Fatalf("%s = %v", bad, err)
		}
	}
	// .each in a condition, even a fan-out's own.
	_, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    condition: '{{ .each.index }}'
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err == nil || !strings.Contains(err.Error(), "before the items exist") || fieldOf(err) != "tasks[1].condition" {
		t.Fatalf(".each in a condition = %v (field %q)", err, fieldOf(err))
	}
	// .each in a task without each:.
	_, err = dsl.Compile(eachDoc(`
  - id: plain
    uses: shell
    with: {op: exec, v: '{{ .each.item }}'}
`))
	if err == nil || !strings.Contains(err.Error(), "declares each:") {
		t.Fatalf(".each without each: = %v", err)
	}
}

func TestCompile_AwaitRefusesEach(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: fan
inputs: {parts: {type: array}}
tasks:
  - id: gate
    await: {timeout_seconds: 60}
    each: {over: "{{ .inputs.parts }}", max_items: 3}
`))
	if err == nil || fieldOf(err) != "tasks[0].each" || !strings.Contains(err.Error(), "an await is one resolution, not N") {
		t.Fatalf("each on an await (YAML) = %v (field %q)", err, fieldOf(err))
	}
}

func TestEachItemsDeclared(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		{Id: "a"},
		{Id: "b", Each: &orkstackv1.EachSpec{MaxItems: 10}},
		{Id: "c", Each: &orkstackv1.EachSpec{MaxItems: 32}},
	}}
	if got := dsl.EachItemsDeclared(wf); got != 42 {
		t.Fatalf("EachItemsDeclared = %d, want 42", got)
	}
	if got := dsl.EachItemsDeclared(&orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a"}}}); got != 0 {
		t.Fatalf("no fan-out = %d, want 0", got)
	}
}
