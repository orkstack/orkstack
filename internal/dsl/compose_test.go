package dsl_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

func TestParseOrkflowUses(t *testing.T) {
	hex64 := strings.Repeat("ab", 32)
	long := strings.Repeat("v", 200)
	accepted := []struct {
		uses, name, version string
	}{
		{"orkflow:release-block@2.1.0", "release-block", "2.1.0"},
		{"orkflow:x@" + hex64, "x", hex64},
		{"orkflow:x@" + long, "x", long},
	}
	for _, tc := range accepted {
		name, version, ok, err := dsl.ParseOrkflowUses(tc.uses)
		if err != nil || !ok || name != tc.name || version != tc.version {
			t.Errorf("ParseOrkflowUses(%q) = %q, %q, %v, %v", tc.uses, name, version, ok, err)
		}
	}
	for _, uses := range []string{"shell", "ghcr.io/acme/x:1", "orkflow.example:5000/app"} {
		_, _, ok, err := dsl.ParseOrkflowUses(uses)
		if ok || err != nil {
			t.Errorf("ParseOrkflowUses(%q) = ok %v, err %v; want not a composition", uses, ok, err)
		}
	}
	refused := []struct {
		uses, want string
	}{
		{"orkflow:release-block", "orkctl orkflow versions release-block"},
		{"orkflow:@1", "names no orkflow"},
		{"orkflow:Release@1", "not a slug"},
		{"orkflow:x@1/2", "must match"},
		{"orkflow:x@a@b", "must match"},
		{"orkflow:x@-1", "must match"},
		{"orkflow:x@" + long + "v", "exceeds the 200-byte limit"},
		{"orkflow:{{ .inputs.n }}@1", "cannot be a template"},
	}
	for _, tc := range refused {
		_, _, ok, err := dsl.ParseOrkflowUses(tc.uses)
		if !ok || err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ParseOrkflowUses(%q) = ok %v, err %v; want an error naming %q", tc.uses, ok, err, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), "orkflow:") {
			t.Errorf("ParseOrkflowUses(%q): %v does not name the shape", tc.uses, err)
		}
	}
}

func TestParseOrkflowUses_RequiresOnePinnedVersion(t *testing.T) {
	for _, uses := range []string{"orkflow:release-block", "orkflow:release-block@"} {
		_, _, ok, err := dsl.ParseOrkflowUses(uses)
		var missing *dsl.ComposeVersionRequiredError
		if !ok || !errors.As(err, &missing) || missing.Name != "release-block" {
			t.Fatalf("ParseOrkflowUses(%q) = %v, %v; want *ComposeVersionRequiredError", uses, ok, err)
		}
	}
	_, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
namespace: team-a
name: caller
tasks:
  - id: release
    uses: orkflow:release-block
`))
	var missing *dsl.ComposeVersionRequiredError
	if !errors.As(err, &missing) {
		t.Fatalf("Compile = %v; want *ComposeVersionRequiredError", err)
	}
	if missing.TaskIndex != 0 || missing.TaskID != "release" || missing.Namespace != "team-a" {
		t.Fatalf("error = %+v", missing)
	}
	want := `task "release": uses: orkflow:release-block needs a version — a composed task pins one immutable version and never follows a pointer; list them with orkctl orkflow versions release-block`
	if err.Error() != want {
		t.Fatalf("error = %q\nwant  %q", err.Error(), want)
	}
	var verr *dsl.ValidationError
	if !errors.As(err, &verr) || verr.Field != "tasks[0].uses" {
		t.Fatalf("not anchored at tasks[0].uses: %#v", err)
	}
}

const composedCaller = `
apiVersion: orkstack/v1
kind: Orkflow
name: composed-release
version: "0.1.0"
inputs:
  image: { type: string, default: "ghcr.io/acme/app:1.4" }
outputs:
  digest: '{{ .tasks.release.output.digest }}'
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@0.1.0
    needs: [build]
    with:
      image: '{{ .tasks.build.output.value }}'
      count: 3
  - id: announce
    uses: shell
    needs: [release]
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

func TestCompile_ComposedTaskCompiles(t *testing.T) {
	wf, err := dsl.Compile([]byte(composedCaller))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	release := wf.GetTasks()[1]
	ref := release.GetOrkflow()
	if ref.GetName() != "release-block" || ref.GetVersion() != "0.1.0" || ref.GetDefinitionDigest() != "" {
		t.Fatalf("orkflow = %v", ref)
	}
	if release.GetTimeoutSeconds() != 0 || release.GetImage() != nil {
		t.Fatalf("timeout %d, image %v", release.GetTimeoutSeconds(), release.GetImage())
	}
	if got := release.GetWith().AsMap()["image"]; got != "{{ .tasks.build.output.value }}" {
		t.Fatalf("with.image = %v", got)
	}
	if got := wf.GetOutputs()["digest"]; got != "{{ .tasks.release.output.digest }}" {
		t.Fatalf("outputs = %v", wf.GetOutputs())
	}
	if release.GetUses() != "orkflow:release-block@0.1.0" {
		t.Fatalf("uses = %q", release.GetUses())
	}
}

func TestCompile_ComposedTaskTakesNoExecutionKeys(t *testing.T) {
	keys := map[string]string{
		"command":                           `command: ["x"]`,
		"env":                               `env: {A: b}`,
		"secrets":                           `secrets: [{name: db, source: "env:DB"}]`,
		"resources":                         `resources: {cpu: "1"}`,
		"network":                           `network: none`,
		"pool":                              `pool: gpu`,
		"pin":                               `pin: build`,
		"retry":                             `retry: {maximum_attempts: 2}`,
		"timeout_seconds":                   `timeout_seconds: 60`,
		"cancellation_grace_period_seconds": `cancellation_grace_period_seconds: 5`,
		"run_as_orker_user":                 `run_as_orker_user: true`,
		"each":                              `each: {over: "{{ .inputs.xs }}", max_items: 2}`,
		"await":                             `await: {timeout_seconds: 60}`,
	}
	if len(keys) != 13 {
		t.Fatalf("%d keys, want the thirteen", len(keys))
	}
	for key, line := range keys {
		t.Run(key, func(t *testing.T) {
			_, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: caller
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    ` + line + `
`))
			want := `task "release": a composed task takes only id, needs, condition, join, uses and with; "` + key +
				`" does not apply (it runs orkflow release-block@2.1.0 as a child run, whose own tasks declare where and how they run)`
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v\nwant %s", err, want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0]."+key {
				t.Fatalf("not anchored at tasks[0].%s: %#v", key, err)
			}
		})
	}
	t.Run("run_as_orker_user false is accepted", func(t *testing.T) {
		if _, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: caller
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    run_as_orker_user: false
`)); err != nil {
			t.Fatalf("refused: %v", err)
		}
	})
}

func compileErr(t *testing.T, doc string) error {
	t.Helper()
	_, err := dsl.Compile([]byte(doc))
	return err
}

func TestCompile_EngineRenderedTemplatesRefuseOrkerHelpers(t *testing.T) {
	base := func(withLine, outputsLine string) string {
		return `
apiVersion: orkstack/v1
kind: Orkflow
name: caller
inputs:
  t: { type: string }
  num: { type: string }
` + outputsLine + `
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
` + withLine
	}
	cases := []struct {
		name, expr, want string
	}{
		{"artifact", `{{ artifact "build/x.txt" }}`, "the artifact helper resolves on an Orker"},
		{"env", `{{ env "HOME" }}`, "the env helper"},
		{"sh", `{{ sh .inputs.num }}`, "the sh helper"},
		{"artifacts", `{{ .tasks.build.artifacts }}`, ".tasks.build.artifacts is not a value"},
		{"each", `{{ .each.x }}`, ".each is not available here"},
		{"dynamic index", `{{ index .tasks .inputs.t "output" "v" }}`, "names every task it reads literally"},
	}
	for _, tc := range cases {
		t.Run("with/"+tc.name, func(t *testing.T) {
			err := compileErr(t, base("    with:\n      image: '"+tc.expr+"'\n", ""))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if tc.name == "artifact" && !strings.Contains(err.Error(),
				`task "release": with.image: the artifact helper resolves on an Orker; a composed task's with: becomes the child run's input, which carries values, not files`) {
				t.Fatalf("artifact sentence = %v", err)
			}
		})
		t.Run("outputs/"+tc.name, func(t *testing.T) {
			err := compileErr(t, base("", "outputs:\n  digest: '"+tc.expr+"'\n"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || !strings.HasPrefix(verr.Field, "outputs.digest") {
				t.Fatalf("not anchored at outputs.digest: %#v", err)
			}
		})
	}
	t.Run("a container task keeps its helpers", func(t *testing.T) {
		if err := compileErr(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: caller
inputs:
  num: { type: string }
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: use
    uses: shell
    needs: [build]
    with:
      op: exec
      command: ["sh", "-c", "cat {{ artifact \"build/x.txt\" }} {{ sh .inputs.num }} {{ env \"HOME\" }} {{ .tasks.build.artifacts }}"]
`); err != nil {
			t.Fatalf("refused: %v", err)
		}
	})
}

func TestCompile_OutputsRules(t *testing.T) {
	doc := func(outputs string) string {
		return `
apiVersion: orkstack/v1
kind: Orkflow
name: block
inputs:
  image: { type: string }
  xs: { type: array }
outputs:
` + outputs + `
tasks:
  - id: a
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: b
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: fan
    uses: shell
    each: { over: "{{ .inputs.xs }}", max_items: 3 }
    with: { op: eval, command: ["true"] }
`
	}
	for _, key := range []string{"Digest", "d-1", strings.Repeat("d", 129)} {
		err := compileErr(t, doc("  "+key+": x\n"))
		var verr *dsl.ValidationError
		if err == nil || !errors.As(err, &verr) || verr.Field != "outputs."+key ||
			!strings.Contains(err.Error(), "an output name is a template field name") {
			t.Errorf("key %q: error = %v", key, err)
		}
	}
	if err := compileErr(t, doc("  first: '{{ .tasks.a.output.v }}'\n  second: '{{ .tasks.b.output.v }}'\n  img: '{{ .inputs.image }}'\n  const: fixed\n")); err != nil {
		t.Errorf("outputs reading any task refused: %v", err)
	}
	if err := compileErr(t, doc("  x: '{{ .tasks.fan.output.v }}'\n")); err == nil || !strings.Contains(err.Error(), "is a fan-out") {
		t.Errorf("reading a fan-out: %v", err)
	}
	if err := compileErr(t, doc("  x: '{{ .inputs.ghost }}'\n")); err == nil || !strings.Contains(err.Error(), "undeclared input") {
		t.Errorf("undeclared input: %v", err)
	}
	wf, err := dsl.Compile([]byte(strings.Replace(doc(""), "outputs:\n", "outputs: {}\n", 1)))
	if err != nil {
		t.Fatalf("outputs: {}: %v", err)
	}
	if wf.GetOutputs() != nil {
		t.Fatalf("outputs: {} compiled to %v, want none", wf.GetOutputs())
	}
}

func TestCompile_FanOutCannotReadAComposedTask(t *testing.T) {
	err := compileErr(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: caller
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
  - id: fan
    uses: shell
    needs: [release]
    each: { over: "{{ .tasks.release.output.items }}", max_items: 3 }
    with: { op: eval, command: ["true"] }
`)
	want := `task "fan": each.over: "release" is a composed task; its values are the child's declared outputs, which are strings and never an array`
	var verr *dsl.ValidationError
	if err == nil || err.Error() != want || !errors.As(err, &verr) || verr.Field != "tasks[1].each.over" {
		t.Fatalf("error = %v", err)
	}
}

func TestRenderValues(t *testing.T) {
	inputs := map[string]any{"image": "ghcr.io/acme/app:1", "n": json.Number("12345678901234567890")}
	outputs := map[string]map[string]any{"build": {"value": "sha256:abc", "null": nil}}
	value := map[string]any{
		"image":           "{{ .tasks.build.output.value }}",
		"big":             "{{ .inputs.n }}",
		"count":           json.Number("3"),
		"flag":            true,
		"none":            nil,
		"{{ .inputs.x }}": "key never templated",
		"list":            []any{"{{ .inputs.image }}", 2.5},
	}
	got, err := dsl.RenderValues(`task "release": with`, value, inputs, outputs)
	if err != nil {
		t.Fatalf("RenderValues: %v", err)
	}
	want := map[string]any{
		"image":           "sha256:abc",
		"big":             "12345678901234567890",
		"count":           json.Number("3"),
		"flag":            true,
		"none":            nil,
		"{{ .inputs.x }}": "key never templated",
		"list":            []any{"ghcr.io/acme/app:1", 2.5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rendered %#v\nwant %#v", got, want)
	}

	_, err = dsl.RenderValues("outputs", map[string]any{"digest": "{{ .tasks.build.output.missing }}"}, inputs, outputs)
	if err == nil || !strings.Contains(err.Error(), "outputs.digest") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing key: %v", err)
	}
	_, err = dsl.RenderValues("outputs", map[string]any{"d": "{{ .tasks.build.output.null }}"}, inputs, outputs)
	if err == nil || !strings.Contains(err.Error(), "<no value>") {
		t.Fatalf("no value: %v", err)
	}
	_, err = dsl.RenderValues("outputs", map[string]any{"d": "{{ sh .inputs.image }}"}, inputs, outputs)
	if err == nil || !strings.Contains(err.Error(), `function "sh" not defined`) {
		t.Fatalf("a helper was available: %v", err)
	}
}

func TestReferencedUpstreamIn(t *testing.T) {
	got, err := dsl.ReferencedUpstreamIn(map[string]any{
		"a": "{{ .tasks.build.output.v }}",
		"b": []any{`{{ index .tasks "sign-it" "output" "x" }}`, "{{ $.tasks.build.output.w }}"},
		"c": "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"build", "sign-it"}) {
		t.Fatalf("got %v", got)
	}
	_, err = dsl.ReferencedUpstreamIn(map[string]any{"a": `{{ index .tasks .inputs.t "output" }}`})
	if err == nil || !strings.Contains(err.Error(), "literally") {
		t.Fatalf("a dynamic index was skipped: %v", err)
	}
	outs, err := dsl.ReferencedUpstreamIn(map[string]string{"digest": "{{ .tasks.sign.output.value }}"})
	if err != nil || !reflect.DeepEqual(outs, []string{"sign"}) {
		t.Fatalf("outputs map: %v, %v", outs, err)
	}
}

func TestOutputReferences(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: caller
inputs:
  k: { type: string }
outputs:
  z: '{{ .tasks.release.output.digest }}'
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
  - id: use
    uses: shell
    needs: [release]
    condition: '{{ eq .tasks.release.output.ok "true" }}'
    with:
      op: exec
      body: '{{ index .tasks "release" "output" "url" }} {{ index .tasks.release.output .inputs.k }}'
      command: ["echo", "{{ .tasks.release.output.digets }}"]
`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := dsl.OutputReferences(wf)
	if err != nil {
		t.Fatal(err)
	}
	want := []dsl.OutputReference{
		{Field: "tasks[1].condition", TaskID: "release", Key: "ok"},
		{Field: "tasks[1].with.body", TaskID: "release", Key: "url"},
		{Field: "tasks[1].with.command[1]", TaskID: "release", Key: "digets"},
		{Field: "outputs.z", TaskID: "release", Key: "digest"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
}
