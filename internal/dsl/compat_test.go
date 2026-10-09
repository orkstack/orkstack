package dsl_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

var updateCompat = flag.Bool("update", false, "rewrite testdata goldens")

const (
	compatible = orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE
	breaking   = orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING
	unknown    = orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN
)

// releaseBlock210 and releaseBlock300 are the shared compatibility fixture: 3.0.0
// adds a required region and a defaulted timeout, drops output url, adds
// output sbom, and rewrites every task.
const releaseBlock210 = `
name: release-block
version: "2.1.0"
inputs:
  image:
    type: string
    required: true
  notes:
    type: string
    default: ""
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.publish.output.value }}'
tasks:
  - id: sign
    uses: shell
    with:
      op: eval
      command: ["sh", "-c", "printf sha256:%s {{ sh .inputs.image }}"]
  - id: publish
    uses: shell
    needs: [sign]
    with:
      op: eval
      command: ["sh", "-c", "printf https://r/%s {{ sh .tasks.sign.output.value }}"]
`

const releaseBlock300 = `
name: release-block
version: "3.0.0"
inputs:
  image:
    type: string
    required: true
  notes:
    type: string
    default: ""
  region:
    type: string
    required: true
  timeout:
    type: number
    default: 300
outputs:
  digest: '{{ .tasks.attest.output.value }}'
  sbom: '{{ .tasks.sbom.output.value }}'
tasks:
  - id: attest
    uses: shell
    with:
      op: eval
      command: ["sh", "-c", "printf sha256:%s {{ sh .inputs.image }} {{ sh .inputs.region }}"]
  - id: sbom
    uses: shell
    needs: [attest]
    with:
      op: eval
      command: ["sh", "-c", "printf sbom-%s {{ sh .tasks.attest.output.value }}"]
`

func compile(t *testing.T, source string) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.Compile([]byte(source))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return wf
}

// decl builds an input declaration from a Go map.
func decl(t *testing.T, m map[string]any) *structpb.Value {
	t.Helper()
	v, err := structpb.NewValue(m)
	if err != nil {
		t.Fatalf("structpb: %v", err)
	}
	return v
}

// iface builds an Orkflow with only a declared interface.
func iface(t *testing.T, inputs map[string]any, outputs ...string) *orkstackv1.Orkflow {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "x", Version: "1"}
	if inputs != nil {
		s, err := structpb.NewStruct(inputs)
		if err != nil {
			t.Fatalf("structpb: %v", err)
		}
		wf.Inputs = s
	}
	if len(outputs) > 0 {
		wf.Outputs = map[string]string{}
		for _, o := range outputs {
			wf.Outputs[o] = "{{ .inputs.a }}"
		}
	}
	return wf
}

func TestReadInput(t *testing.T) {
	cases := []struct {
		name     string
		decl     *structpb.Value
		wantType string
		required bool
		hasDef   bool
	}{
		{"all three keys, required with default is optional", decl(t, map[string]any{
			"type": "string", "required": true, "default": "x"}), "string", false, true},
		{"required, no default", decl(t, map[string]any{"type": "number", "required": true}), "number", true, false},
		{"required not a boolean", decl(t, map[string]any{"required": "yes"}), "", false, false},
		{"type not a string", decl(t, map[string]any{"type": 5.0}), "", false, false},
		{"default null is present", decl(t, map[string]any{"required": true, "default": nil}), "", false, true},
		{"non-object declaration", structpb.NewStringValue("string"), "", false, false},
		{"nil", nil, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dsl.ReadInput(tc.decl)
			if got.Type != tc.wantType || got.Required != tc.required || (got.Default != nil) != tc.hasDef {
				t.Fatalf("ReadInput = %+v, want type %q required %v default %v",
					got, tc.wantType, tc.required, tc.hasDef)
			}
		})
	}
	if d := dsl.ReadInput(decl(t, map[string]any{"default": nil})).Default; d == nil || d.GetNullValue() != structpb.NullValue_NULL_VALUE {
		t.Fatalf("default: null should read as a present JSON null, got %v", d)
	}
}

type wantFinding struct {
	field   string
	change  orkstackv1.InterfaceChange
	verdict orkstackv1.InterfaceVerdict
	from    string
	to      string
}

func findingsOf(fs []*orkstackv1.InterfaceFinding) []wantFinding {
	out := make([]wantFinding, 0, len(fs))
	for _, f := range fs {
		out = append(out, wantFinding{f.GetField(), f.GetChange(), f.GetVerdict(), f.GetFrom(), f.GetTo()})
	}
	return out
}

func TestCompareInterfaces_ClassificationTable(t *testing.T) {
	c := func(s string) orkstackv1.InterfaceChange {
		return orkstackv1.InterfaceChange(orkstackv1.InterfaceChange_value["INTERFACE_CHANGE_"+s])
	}
	str := map[string]any{"type": "string"}
	reqStr := map[string]any{"type": "string", "required": true}
	type tcase struct {
		name     string
		from, to *orkstackv1.Orkflow
		want     []wantFinding // input findings only (outputs: both declare "o", O5)
	}
	o5 := wantFinding{"outputs.o", c("UNCHANGED"), compatible, "declared", "declared"}
	cases := []tcase{
		{"I1 added required", iface(t, map[string]any{}, "o"), iface(t, map[string]any{"a": reqStr}, "o"),
			[]wantFinding{{"inputs.a", c("ADDED"), breaking, "", "string, required"}}},
		{"I2 added optional", iface(t, map[string]any{}, "o"), iface(t, map[string]any{"a": map[string]any{"type": "number", "default": 300.0}}, "o"),
			[]wantFinding{{"inputs.a", c("ADDED"), compatible, "", "number, optional, default 300"}}},
		{"I3 removed", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{}, "o"),
			[]wantFinding{{"inputs.a", c("REMOVED"), breaking, "string, optional", ""}}},
		{"I4 retyped", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{"a": map[string]any{"type": "number"}}, "o"),
			[]wantFinding{{"inputs.a", c("RETYPED"), breaking, "string, optional", "number, optional"}}},
		{"I5 now typed", iface(t, map[string]any{"a": map[string]any{}}, "o"), iface(t, map[string]any{"a": str}, "o"),
			[]wantFinding{{"inputs.a", c("RETYPED"), breaking, "untyped, optional", "string, optional"}}},
		{"I6 untyped", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{"a": map[string]any{}}, "o"),
			[]wantFinding{{"inputs.a", c("UNTYPED"), compatible, "string, optional", "untyped, optional"}}},
		{"I7 now required", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{"a": reqStr}, "o"),
			[]wantFinding{{"inputs.a", c("NOW_REQUIRED"), breaking, "string, optional", "string, required"}}},
		{"I8 no longer required", iface(t, map[string]any{"a": reqStr}, "o"), iface(t, map[string]any{"a": str}, "o"),
			[]wantFinding{{"inputs.a", c("NO_LONGER_REQUIRED"), compatible, "string, required", "string, optional"}}},
		{"I9 default added", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{"a": map[string]any{"type": "string", "default": "x"}}, "o"),
			[]wantFinding{{"inputs.a", c("DEFAULT_ADDED"), compatible, "string, optional", `string, optional, default "x"`}}},
		{"I10 default removed", iface(t, map[string]any{"a": map[string]any{"type": "string", "default": "x"}}, "o"), iface(t, map[string]any{"a": str}, "o"),
			[]wantFinding{{"inputs.a", c("DEFAULT_REMOVED"), breaking, `string, optional, default "x"`, "string, optional"}}},
		{"I11 default changed", iface(t, map[string]any{"a": map[string]any{"type": "string", "default": "x"}}, "o"), iface(t, map[string]any{"a": map[string]any{"type": "string", "default": "y"}}, "o"),
			[]wantFinding{{"inputs.a", c("DEFAULT_CHANGED"), compatible, `string, optional, default "x"`, `string, optional, default "y"`}}},
		{"I12 unchanged", iface(t, map[string]any{"a": reqStr}, "o"), iface(t, map[string]any{"a": reqStr}, "o"),
			[]wantFinding{{"inputs.a", c("UNCHANGED"), compatible, "string, required", "string, required"}}},
		{"I10 skipped under T.Required", iface(t, map[string]any{"a": map[string]any{"type": "string", "default": "x"}}, "o"), iface(t, map[string]any{"a": reqStr}, "o"),
			[]wantFinding{{"inputs.a", c("NOW_REQUIRED"), breaking, `string, optional, default "x"`, "string, required"}}},
		{"retyped and now required: type then required", iface(t, map[string]any{"a": str}, "o"), iface(t, map[string]any{"a": map[string]any{"type": "number", "required": true}}, "o"),
			[]wantFinding{
				{"inputs.a", c("RETYPED"), breaking, "string, optional", "number, required"},
				{"inputs.a", c("NOW_REQUIRED"), breaking, "string, optional", "number, required"},
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := dsl.CompareInterfaces(tc.from, tc.to)
			want := append(append([]wantFinding{}, tc.want...), o5)
			if g := findingsOf(got); !equalFindings(g, want) {
				t.Fatalf("findings\n got  %+v\n want %+v", g, want)
			}
		})
	}

	outputCases := []struct {
		name     string
		from, to *orkstackv1.Orkflow
		want     []wantFinding
	}{
		{"O1 neither declares", iface(t, nil), iface(t, nil),
			[]wantFinding{{"outputs", c("OUTPUTS_UNDECLARED"), unknown, "none", "none"}}},
		{"O2 only to declares", iface(t, nil), iface(t, nil, "b", "a"),
			[]wantFinding{{"outputs", c("OUTPUTS_DECLARED"), unknown, "none", "2 outputs"}}},
		{"O3 removed (to declares none)", iface(t, nil, "a"), iface(t, nil),
			[]wantFinding{{"outputs.a", c("REMOVED"), breaking, "declared", ""}}},
		{"O3 O4 O5", iface(t, nil, "keep", "gone"), iface(t, nil, "keep", "new"),
			[]wantFinding{
				{"outputs.gone", c("REMOVED"), breaking, "declared", ""},
				{"outputs.keep", c("UNCHANGED"), compatible, "declared", "declared"},
				{"outputs.new", c("ADDED"), compatible, "", "declared"},
			}},
	}
	for _, tc := range outputCases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := dsl.CompareInterfaces(tc.from, tc.to)
			if g := findingsOf(got); !equalFindings(g, tc.want) {
				t.Fatalf("findings\n got  %+v\n want %+v", g, tc.want)
			}
		})
	}
	// O2's message names the sorted outputs; O3's to-declares-none tail.
	got, _ := dsl.CompareInterfaces(iface(t, nil), iface(t, nil, "b", "a"))
	if !strings.Contains(got[0].GetMessage(), "(a, b)") {
		t.Fatalf("O2 message = %q", got[0].GetMessage())
	}
	got, _ = dsl.CompareInterfaces(iface(t, nil, "a"), iface(t, nil))
	if !strings.HasSuffix(got[0].GetMessage(), "(this version declares no outputs, so a run's output reverts to every leaf task's values)") {
		t.Fatalf("O3 message = %q", got[0].GetMessage())
	}
}

func equalFindings(a, b []wantFinding) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCompareInterfaces_ImplementationChangesAreNotFindings(t *testing.T) {
	base := compile(t, releaseBlock210)
	rewritten := compile(t, `
name: release-block
version: "2.2.0"
inputs:
  image:
    type: string
    required: true
  notes:
    type: string
    default: ""
outputs:
  digest: '{{ .tasks.other.output.digest }}'
  url: '{{ .tasks.other.output.where }}'
pins:
  build: {}
tasks:
  - id: prepare
    uses: shell
    pool: gpu
    pin: build
    timeout_seconds: 300
    retry:
      maximum_attempts: 5
    with:
      op: exec
      command: ["sh", "-c", "echo prepare"]
  - id: other
    uses: shell
    needs: [prepare]
    pool: gpu
    pin: build
    condition: '{{ eq .inputs.notes "" }}'
    with:
      op: eval
      command: ["sh", "-c", "echo {{ sh .inputs.image }}"]
`)
	self, selfVerdict := dsl.CompareInterfaces(base, base)
	got, verdict := dsl.CompareInterfaces(base, rewritten)
	if !equalFindings(findingsOf(got), findingsOf(self)) || verdict != selfVerdict {
		t.Fatalf("implementation changes produced findings:\n got  %+v\n self %+v", findingsOf(got), findingsOf(self))
	}
	if verdict != compatible {
		t.Fatalf("verdict = %v, want COMPATIBLE", verdict)
	}
}

func TestCompareInterfaces_OutputsUndeclaredIsNeverCompatible(t *testing.T) {
	in := map[string]any{"a": map[string]any{"type": "string"}}
	got, verdict := dsl.CompareInterfaces(iface(t, in), iface(t, in))
	if verdict != unknown {
		t.Fatalf("no outputs: verdict = %v, want UNKNOWN", verdict)
	}
	if last := got[len(got)-1]; last.GetChange() != orkstackv1.InterfaceChange_INTERFACE_CHANGE_OUTPUTS_UNDECLARED {
		t.Fatalf("O1 missing: %+v", findingsOf(got))
	}
	if _, verdict := dsl.CompareInterfaces(iface(t, in, "o"), iface(t, in, "o")); verdict != compatible {
		t.Fatalf("identical declared outputs: verdict = %v, want COMPATIBLE", verdict)
	}
}

func TestInterfaceVerdictOf(t *testing.T) {
	f := func(v orkstackv1.InterfaceVerdict) *orkstackv1.InterfaceFinding {
		return &orkstackv1.InterfaceFinding{Verdict: v}
	}
	cases := []struct {
		in   []*orkstackv1.InterfaceFinding
		want orkstackv1.InterfaceVerdict
	}{
		{nil, compatible},
		{[]*orkstackv1.InterfaceFinding{f(compatible)}, compatible},
		{[]*orkstackv1.InterfaceFinding{f(compatible), f(unknown)}, unknown},
		{[]*orkstackv1.InterfaceFinding{f(unknown), f(breaking), f(compatible)}, breaking},
		{[]*orkstackv1.InterfaceFinding{f(breaking), f(unknown)}, breaking},
	}
	for i, tc := range cases {
		if got := dsl.InterfaceVerdictOf(tc.in); got != tc.want {
			t.Errorf("case %d: got %v, want %v", i, got, tc.want)
		}
	}
	if got := dsl.BreakingFindings([]*orkstackv1.InterfaceFinding{f(compatible), f(breaking), f(unknown)}); len(got) != 1 {
		t.Fatalf("BreakingFindings = %d, want 1", len(got))
	}
}

func TestCompareInterfaces_OrderIsDeterministic(t *testing.T) {
	from, to := compile(t, releaseBlock210), compile(t, releaseBlock300)
	first, _ := dsl.CompareInterfaces(from, to)
	for range 100 {
		got, _ := dsl.CompareInterfaces(proto.Clone(from).(*orkstackv1.Orkflow), proto.Clone(to).(*orkstackv1.Orkflow))
		if !equalFindings(findingsOf(got), findingsOf(first)) {
			t.Fatalf("order changed between runs")
		}
	}
	var fields []string
	for _, f := range first {
		fields = append(fields, f.GetField())
	}
	want := []string{"inputs.image", "inputs.notes", "inputs.region", "inputs.timeout",
		"outputs.digest", "outputs.sbom", "outputs.url"}
	if strings.Join(fields, " ") != strings.Join(want, " ") {
		t.Fatalf("order = %v, want %v", fields, want)
	}
}

func TestCompareInterfaces_DefaultsRenderAsStableJSON(t *testing.T) {
	from := iface(t, map[string]any{"a": map[string]any{}}, "o")
	to := iface(t, map[string]any{"a": map[string]any{"default": map[string]any{
		"z": "<b>", "a": 300.0, "m": []any{1.5, true, nil}}}}, "o")
	got, _ := dsl.CompareInterfaces(from, to)
	want := `input "a" gains default {"a":300,"m":[1.5,true,null],"z":"<b>"}`
	if got[0].GetMessage() != want {
		t.Fatalf("message = %q\nwant      %q", got[0].GetMessage(), want)
	}
}

func TestCompareInterfaces_NilIsEmpty(t *testing.T) {
	got, verdict := dsl.CompareInterfaces(nil, nil)
	if verdict != unknown || len(got) != 1 || got[0].GetField() != "outputs" {
		t.Fatalf("nil, nil = %+v %v", findingsOf(got), verdict)
	}
	got, verdict = dsl.CompareInterfaces(nil, iface(t, map[string]any{"a": map[string]any{"required": true}}, "o"))
	if verdict != breaking || len(got) != 2 {
		t.Fatalf("nil, x = %+v %v", findingsOf(got), verdict)
	}
	got, verdict = dsl.CompareInterfaces(iface(t, map[string]any{"a": map[string]any{}}, "o"), nil)
	if verdict != breaking || len(got) != 2 {
		t.Fatalf("x, nil = %+v %v", findingsOf(got), verdict)
	}
}

func TestCompareInterfaces_MessagesGolden(t *testing.T) {
	got, verdict := dsl.CompareInterfaces(compile(t, releaseBlock210), compile(t, releaseBlock300))
	if verdict != breaking || len(got) != 7 || len(dsl.BreakingFindings(got)) != 2 {
		t.Fatalf("fixture: verdict %v, %d findings, %d breaking; want BREAKING, 7, 2",
			verdict, len(got), len(dsl.BreakingFindings(got)))
	}
	type row struct {
		Field   string `json:"field"`
		Change  string `json:"change"`
		Verdict string `json:"verdict"`
		From    string `json:"from"`
		To      string `json:"to"`
		Message string `json:"message"`
	}
	rows := make([]row, 0, len(got))
	for _, f := range got {
		rows = append(rows, row{f.GetField(),
			strings.TrimPrefix(f.GetChange().String(), "INTERFACE_CHANGE_"),
			strings.TrimPrefix(f.GetVerdict().String(), "INTERFACE_VERDICT_"),
			f.GetFrom(), f.GetTo(), f.GetMessage()})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rows); err != nil {
		t.Fatal(err)
	}
	encoded := buf.Bytes()
	path := filepath.Join("testdata", "compat", "release-block-2.1.0-3.0.0.golden")
	if *updateCompat {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if string(want) != string(encoded) {
		t.Fatalf("golden mismatch\n got:\n%s\nwant:\n%s", encoded, want)
	}
}

func TestCountNoun(t *testing.T) {
	if got := dsl.CountNoun(1, "breaking change", "breaking changes"); got != "1 breaking change" {
		t.Fatal(got)
	}
	if got := dsl.CountNoun(2, "output", "outputs"); got != "2 outputs" {
		t.Fatal(got)
	}
}
