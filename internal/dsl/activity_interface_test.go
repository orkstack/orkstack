package dsl

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// firstPartyLabel reads the org.orkstack.activity.interface LABEL line of one
// first-party image's Dockerfile, so the label the test parses is the label
// the image ships — never a copy that can drift from it.
func firstPartyLabel(t *testing.T, image string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "images", image, "Dockerfile"))
	if err != nil {
		t.Fatalf("read %s Dockerfile: %v", image, err)
	}
	match := regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(InterfaceLabel) + `='([^']*)'$`).FindSubmatch(raw)
	if match == nil {
		t.Fatalf("images/%s/Dockerfile has no single-line LABEL %s='…'", image, InterfaceLabel)
	}
	return string(match[1])
}

// readmeInterface is the label text the image's README reproduces under its
// "Interface" heading, so the README and the Dockerfile cannot drift apart.
func readmeInterface(t *testing.T, image string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "images", image, "README.md"))
	if err != nil {
		t.Fatalf("read %s README: %v", image, err)
	}
	match := regexp.MustCompile(`(?s)## Interface\n.*?LABEL ` + regexp.QuoteMeta(InterfaceLabel) + `='([^']*)'`).FindSubmatch(raw)
	if match == nil {
		t.Fatalf("images/%s/README.md has no Interface section reproducing the label", image)
	}
	return string(match[1])
}

func mustParse(t *testing.T, raw string) *orkstackv1.ActivityInterface {
	t.Helper()
	iface, err := ParseInterfaceLabel(raw)
	if err != nil {
		t.Fatalf("parse label: %v\n%s", err, raw)
	}
	return iface
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func field(typ string, required bool) *orkstackv1.FieldSchema {
	return &orkstackv1.FieldSchema{Type: typ, Required: required}
}

// fieldDefault is an optional field with a documentary default.
func fieldDefault(typ string, value any) *orkstackv1.FieldSchema {
	v, err := structpb.NewValue(value)
	if err != nil {
		panic(err)
	}
	return &orkstackv1.FieldSchema{Type: typ, Default: v}
}

func TestParseInterfaceLabel_AcceptsEveryFirstPartyLabel(t *testing.T) {
	type want struct {
		fields    map[string]*orkstackv1.FieldSchema
		ops       map[string]map[string]*orkstackv1.FieldSchema
		defaultOp string
		values    map[string]string
	}
	str, arr, num, obj, boolean := "string", "array", "number", "object", "boolean"
	cases := map[string]want{
		// result is deliberately undeclared: its JSON type is whatever the
		// filter returns, and the vocabulary has no "any". The declared values
		// are the three an `as:` run reports.
		"activity-json": {
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"query": {
					"filter": field(str, true), "src": field(str, false), "slurp": fieldDefault(boolean, false),
					"srcs": field(arr, false), "gather": field(str, false), "nullInput": fieldDefault(boolean, false),
					"vars": field(obj, false), "as": field(str, false),
				},
				"merge": {"srcs": field(arr, true), "as": field(str, false)},
			},
			values: map[string]string{"path": str, "sizeBytes": num, "sha256": str},
		},
		"activity-git": {
			fields: map[string]*orkstackv1.FieldSchema{
				"url": field(str, true), "ref": field(str, false), "tokenSecret": field(str, false),
				"tokenUser": fieldDefault(str, "x-access-token"), "caFile": field(str, false),
			},
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"clone":   {"depth": fieldDefault(num, 1.0), "as": fieldDefault(str, "src.tar.gz"), "dest": field(str, false)},
				"resolve": {},
			},
			values: map[string]string{"commit": str, "ref": str, "path": str, "sizeBytes": num, "sha256": str},
		},
		"activity-template": {
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"render": {
					"src": field(str, true), "data": field(str, false), "values": field(obj, false),
					"datasources": field(obj, false), "leftDelim": field(str, false), "rightDelim": field(str, false),
					"as": field(str, false),
				},
			},
			values: map[string]string{"path": str, "sizeBytes": num, "sha256": str},
		},
		"activity-file": {
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"copy":     {"src": field(str, true), "as": field(str, false)},
				"checksum": {"src": field(str, true)},
				"gzip":     {"src": field(str, true), "as": field(str, false)},
				"gunzip":   {"src": field(str, true), "as": field(str, false)},
				"tar":      {"srcs": field(arr, true), "as": field(str, false)},
				"untar":    {"src": field(str, true), "member": field(str, true), "as": field(str, false)},
				"concat":   {"srcs": field(arr, true), "as": field(str, true)},
			},
			values: map[string]string{"path": str, "sizeBytes": num, "sha256": str},
		},
		"activity-http": {
			fields: map[string]*orkstackv1.FieldSchema{
				"url": field(str, true), "headers": field(obj, false), "body": field("string|object|array", false),
				"timeoutSeconds": field(num, false), "captureBodyAs": field(str, false),
			},
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"get": {}, "post": {}, "put": {}, "patch": {}, "delete": {}, "head": {},
			},
			defaultOp: "get",
			values: map[string]string{"status": num, "headers": obj, "durationMs": num,
				"sizeBytes": num, "json": obj},
		},
		"activity-shell": {
			ops: map[string]map[string]*orkstackv1.FieldSchema{
				"exec":   {"command": field(arr, true)},
				"script": {"script": field(str, true), "interpreter": field(str, false), "command": field(arr, false)},
				"eval":   {"command": field(arr, true)},
			},
			defaultOp: "exec",
			values:    map[string]string{"exitCode": num, "scriptPath": str, "value": str},
		},
	}
	for image, w := range cases {
		t.Run(image, func(t *testing.T) {
			label := firstPartyLabel(t, image)
			if readme := readmeInterface(t, image); readme != label {
				t.Fatalf("README Interface section and Dockerfile LABEL differ:\nREADME:     %s\nDockerfile: %s", readme, label)
			}
			iface := mustParse(t, label)
			if iface.GetSchemaVersion() != 1 {
				t.Fatalf("schemaVersion = %d", iface.GetSchemaVersion())
			}
			if iface.GetDefaultOp() != w.defaultOp {
				t.Fatalf("defaultOp = %q, want %q", iface.GetDefaultOp(), w.defaultOp)
			}
			checkFields := func(where string, got, want map[string]*orkstackv1.FieldSchema) {
				t.Helper()
				if !reflect.DeepEqual(keysOf(got), keysOf(want)) {
					t.Fatalf("%s keys = %v, want %v", where, keysOf(got), keysOf(want))
				}
				for k, f := range want {
					if !proto.Equal(got[k], f) {
						t.Fatalf("%s.%s = %v, want %v", where, k, got[k], f)
					}
				}
			}
			checkFields("fields", iface.GetFields(), w.fields)
			if !reflect.DeepEqual(keysOf(iface.GetOps()), keysOf(w.ops)) {
				t.Fatalf("ops = %v, want %v", keysOf(iface.GetOps()), keysOf(w.ops))
			}
			for op, fields := range w.ops {
				checkFields("ops."+op+".fields", iface.GetOps()[op].GetFields(), fields)
			}
			gotValues := map[string]string{}
			for k, f := range iface.GetValues() {
				gotValues[k] = f.GetType()
			}
			if !reflect.DeepEqual(gotValues, w.values) {
				t.Fatalf("values = %v, want %v (every key the README's Output documents)", gotValues, w.values)
			}
			// Round trip: the parsed message re-parses to itself.
			again := mustParse(t, mustMarshalInterface(t, iface))
			if !proto.Equal(again, iface) {
				t.Fatalf("round trip changed the interface:\n%v\n%v", iface, again)
			}
		})
	}
}

func TestParseInterfaceLabel_RefusesEveryWordOutsideTheVocabulary(t *testing.T) {
	for _, tc := range []struct{ name, label, word string }{
		{"description on a field", `{"schemaVersion":1,"fields":{"a":{"type":"string","description":"x"}}}`, "description"},
		{"pattern", `{"schemaVersion":1,"fields":{"a":{"type":"string","pattern":"^x$"}}}`, "pattern"},
		{"enum", `{"schemaVersion":1,"fields":{"a":{"type":"string","enum":["x"]}}}`, "enum"},
		{"items", `{"schemaVersion":1,"fields":{"a":{"type":"array","items":{"type":"string"}}}}`, "items"},
		{"properties", `{"schemaVersion":1,"fields":{"a":{"type":"object","properties":{}}}}`, "properties"},
		{"oneOf", `{"schemaVersion":1,"fields":{"a":{"oneOf":[]}}}`, "oneOf"},
		{"unknown top-level key", `{"schemaVersion":1,"required":["a"]}`, "required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInterfaceLabel(tc.label)
			if err == nil {
				t.Fatalf("label with %q parsed; a word outside the vocabulary must never be dropped silently", tc.word)
			}
			if !strings.HasPrefix(err.Error(), "label is not the JSON encoding of an ActivityInterface: ") ||
				!strings.Contains(err.Error(), tc.word) {
				t.Fatalf("error %q does not name %q", err, tc.word)
			}
		})
	}
}

func TestParseInterfaceLabel_ValidatesShape(t *testing.T) {
	manyOps := make([]string, 0, 65)
	for i := range 65 {
		manyOps = append(manyOps, fmt.Sprintf(`"op%d":{}`, i))
	}
	manyFields := make([]string, 0, 257)
	for i := range 257 {
		manyFields = append(manyFields, fmt.Sprintf(`"k%d":{"type":"string"}`, i))
	}
	longKey := strings.Repeat("k", 129)
	for _, tc := range []struct{ name, label, want string }{
		{"over-size label", `{"schemaVersion":1,"fields":{"a":{"type":"string"}}}` + strings.Repeat(" ", MaxInterfaceLabelBytes),
			fmt.Sprintf("label is %d bytes, limit is 65536", len(`{"schemaVersion":1,"fields":{"a":{"type":"string"}}}`)+MaxInterfaceLabelBytes)},
		{"schemaVersion 2", `{"schemaVersion":2}`, "schemaVersion 2 is not supported (this engine reads 1)"},
		{"65 ops", `{"schemaVersion":1,"ops":{` + strings.Join(manyOps, ",") + `}}`, "declares 65 ops, limit is 64"},
		{"an op named Copy", `{"schemaVersion":1,"ops":{"Copy":{}}}`, `op "Copy" is not a lowercase op name (^[a-z][a-z0-9_-]{0,63}$)`},
		{"257 fields", `{"schemaVersion":1,"fields":{` + strings.Join(manyFields, ",") + `}}`, "fields declares 257 keys, limit is 256"},
		{"a key of 129 bytes", `{"schemaVersion":1,"values":{"` + longKey + `":{"type":"string"}}}`, `values: key "` + longKey + `" is not a valid with: key`},
		{"type integer", `{"schemaVersion":1,"ops":{"copy":{"fields":{"n":{"type":"integer"}}}}}`, `ops.copy.fields.n: type "integer" is not one of string, number, boolean, object, array, or several of them joined by |`},
		{"required with a default", `{"schemaVersion":1,"fields":{"a":{"type":"string","required":true,"default":"x"}}}`, "fields.a: required and default contradict; a defaulted key is never required"},
		{"default null", `{"schemaVersion":1,"fields":{"a":{"type":"string","default":null}}}`, "fields.a: null is not a default; omit the field"},
		{"default string on a number", `{"schemaVersion":1,"fields":{"n":{"type":"number","default":"3"}}}`, "fields.n: default is a string but type is number"},
		{"fields.op with ops", `{"schemaVersion":1,"fields":{"op":{"type":"string"}},"ops":{"copy":{}}}`, "fields.op is declared explicitly but ops is non-empty; op is implied by ops"},
		{"defaultOp naming no op", `{"schemaVersion":1,"ops":{"copy":{},"tar":{}},"defaultOp":"gzip"}`, `defaultOp "gzip" is not one of the declared ops (copy, tar)`},
		{"defaultOp with empty ops", `{"schemaVersion":1,"defaultOp":"copy"}`, `defaultOp "copy" is set but ops is empty`},
		{"src shared and under copy", `{"schemaVersion":1,"fields":{"src":{"type":"string"}},"ops":{"copy":{"fields":{"src":{"type":"string"}}}}}`, `key "src" is declared both in fields and under op "copy"; declare it once`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInterfaceLabel(tc.label)
			if err == nil {
				t.Fatalf("label parsed, want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Fatalf("error = %q\nwant    %q", err, tc.want)
			}
		})
	}
}

// Interfaces the checker tests use. They are the first-party labels, so the
// checks are exercised against the vocabulary a real image ships.
func fileInterface(t *testing.T) *orkstackv1.ActivityInterface {
	return mustParse(t, firstPartyLabel(t, "activity-file"))
}

func httpInterface(t *testing.T) *orkstackv1.ActivityInterface {
	return mustParse(t, firstPartyLabel(t, "activity-http"))
}

const testPinned = "registry.test/orkstack/activity-file@sha256:" + "ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"

func taskWith(t *testing.T, with map[string]any) *orkstackv1.Task {
	t.Helper()
	task := &orkstackv1.Task{Id: "t", Uses: "file", Image: &orkstackv1.ImageRef{Pinned: testPinned}}
	if with != nil {
		s, err := structpb.NewStruct(with)
		if err != nil {
			t.Fatalf("with: %v", err)
		}
		task.With = s
	}
	return task
}

type wantIssue struct {
	severity orkstackv1.ValidationSeverity
	field    string
	message  string
}

const (
	sevError   = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR
	sevWarning = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING
)

func assertIssues(t *testing.T, got []*orkstackv1.ValidationIssue, want []wantIssue) {
	t.Helper()
	render := func() string {
		var b strings.Builder
		for _, g := range got {
			fmt.Fprintf(&b, "  %v %s: %s\n", g.GetSeverity(), g.GetField(), g.GetMessage())
		}
		return b.String()
	}
	if len(got) != len(want) {
		t.Fatalf("got %d issues, want %d:\n%s", len(got), len(want), render())
	}
	for i := range want {
		if got[i].GetSeverity() != want[i].severity || got[i].GetField() != want[i].field || got[i].GetMessage() != want[i].message {
			t.Fatalf("issue %d = %v %s: %s\nwant      %v %s: %s", i,
				got[i].GetSeverity(), got[i].GetField(), got[i].GetMessage(),
				want[i].severity, want[i].field, want[i].message)
		}
	}
}

const fileOps = "checksum, concat, copy, gunzip, gzip, tar, untar"

func TestCheckWith_MissingRequiredKeyIsAnError(t *testing.T) {
	want := []wantIssue{{sevError, "tasks[0].with.src",
		`with.src is required by the interface of ` + testPinned + ` for op "copy" and is not supplied`}}
	assertIssues(t, CheckWith(0, taskWith(t, map[string]any{"op": "copy"}), fileInterface(t)), want)
	assertIssues(t, CheckWith(0, taskWith(t, map[string]any{"op": "copy", "src": nil}), fileInterface(t)), want)
}

func TestCheckWith_UnknownKeyIsAWarningNeverAnError(t *testing.T) {
	got := CheckWith(0, taskWith(t, map[string]any{"op": "copy", "src": "a", "srcs": []any{"b"}}), fileInterface(t))
	assertIssues(t, got, []wantIssue{{sevWarning, "tasks[0].with.srcs",
		`with.srcs is not declared by the interface of ` + testPinned + ` for op "copy" (declared: as, op, src); ` +
			`the image may still accept it — check for a typo or a key renamed in a newer image`}})
}

func TestCheckWith_OpSelection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		iface func(*testing.T) *orkstackv1.ActivityInterface
		with  map[string]any
		want  []wantIssue
	}{
		{"no op with file", fileInterface, map[string]any{"src": "a"}, []wantIssue{{sevError, "tasks[0].with.op",
			`with.op is required: ` + testPinned + ` selects its operation with op: and declares no default (declared ops: ` + fileOps + `)`}}},
		{"no op with http applies get and requires url", httpInterface, map[string]any{}, []wantIssue{{sevError, "tasks[0].with.url",
			`with.url is required by the interface of ` + testPinned + ` for op "get" and is not supplied`}}},
		{"op gzpi", fileInterface, map[string]any{"op": "gzpi", "src": "a"}, []wantIssue{{sevError, "tasks[0].with.op",
			`with.op "gzpi" is not an operation of ` + testPinned + ` (declared ops: ` + fileOps + `)`}}},
		{"op 3", fileInterface, map[string]any{"op": 3, "src": "a"}, []wantIssue{{sevError, "tasks[0].with.op",
			`with.op must be a string naming one of ` + testPinned + `'s operations (declared ops: ` + fileOps + `)`}}},
		{"templated op with srcs", fileInterface, map[string]any{"op": "{{ .inputs.op }}", "srcs": []any{"a"}}, nil},
		{"templated op with http and no url", httpInterface, map[string]any{"op": "{{ .inputs.op }}"}, []wantIssue{{sevError, "tasks[0].with.url",
			`with.url is required by the interface of ` + testPinned + ` and is not supplied`}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertIssues(t, CheckWith(0, taskWith(t, tc.with), tc.iface(t)), tc.want)
		})
	}
}

func TestCheckWith_LiteralTypeMismatchIsAWarningAndTemplatesAreNotTyped(t *testing.T) {
	for _, tc := range []struct {
		name string
		with map[string]any
		want []wantIssue
	}{
		{"a quoted number", map[string]any{"url": "u", "timeoutSeconds": "30"}, []wantIssue{{sevWarning, "tasks[0].with.timeoutSeconds",
			`with.timeoutSeconds is declared number by the interface of ` + testPinned + `; this document supplies a string — the image may coerce it, or refuse it at run time`}}},
		{"a templated number", map[string]any{"url": "u", "timeoutSeconds": "{{ .inputs.t }}"}, nil},
		{"a string for an object", map[string]any{"url": "u", "headers": "x"}, []wantIssue{{sevWarning, "tasks[0].with.headers",
			`with.headers is declared object by the interface of ` + testPinned + `; this document supplies a string — the image may coerce it, or refuse it at run time`}}},
		{"an object carrying a template", map[string]any{"url": "u", "headers": map[string]any{"a": "{{ .x }}"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertIssues(t, CheckWith(0, taskWith(t, tc.with), httpInterface(t)), tc.want)
		})
	}
}

// TestCheckWith_TransformImageDocuments checks documents against the labels
// the json, git and template images ship, so validate/apply catch their
// mistakes with no engine change.
func TestCheckWith_TransformImageDocuments(t *testing.T) {
	label := func(image string) func(*testing.T) *orkstackv1.ActivityInterface {
		return func(t *testing.T) *orkstackv1.ActivityInterface { return mustParse(t, firstPartyLabel(t, image)) }
	}
	jsonIface, gitIface, templateIface := label("activity-json"), label("activity-git"), label("activity-template")
	const jsonQuery = `declared: as, filter, gather, nullInput, op, slurp, src, srcs, vars`
	for _, tc := range []struct {
		name  string
		iface func(*testing.T) *orkstackv1.ActivityInterface
		with  map[string]any
		want  []wantIssue
	}{
		{"json without filter", jsonIface, map[string]any{"op": "query", "nullInput": true}, []wantIssue{{sevError, "tasks[0].with.filter",
			`with.filter is required by the interface of ` + testPinned + ` for op "query" and is not supplied`}}},
		{"json op transform", jsonIface, map[string]any{"op": "transform", "filter": "."}, []wantIssue{{sevError, "tasks[0].with.op",
			`with.op "transform" is not an operation of ` + testPinned + ` (declared ops: merge, query)`}}},
		{"json key filtre", jsonIface, map[string]any{"op": "query", "filter": ".", "nullInput": true, "filtre": "."}, []wantIssue{{sevWarning, "tasks[0].with.filtre",
			`with.filtre is not declared by the interface of ` + testPinned + ` for op "query" (` + jsonQuery + `); ` +
				`the image may still accept it — check for a typo or a key renamed in a newer image`}}},
		{"json vars templated as a string", jsonIface, map[string]any{"op": "query", "filter": ".", "nullInput": true, "vars": "{{ .inputs.v }}"}, nil},
		{"git op push", gitIface, map[string]any{"op": "push", "url": "https://h/r.git"}, []wantIssue{{sevError, "tasks[0].with.op",
			`with.op "push" is not an operation of ` + testPinned + ` (declared ops: clone, resolve)`}}},
		{"git without url", gitIface, map[string]any{"op": "resolve", "ref": "main"}, []wantIssue{{sevError, "tasks[0].with.url",
			`with.url is required by the interface of ` + testPinned + ` for op "resolve" and is not supplied`}}},
		{"git depth as a literal string", gitIface, map[string]any{"op": "clone", "url": "https://h/r.git", "depth": "1"}, []wantIssue{{sevWarning, "tasks[0].with.depth",
			`with.depth is declared number by the interface of ` + testPinned + `; this document supplies a string — the image may coerce it, or refuse it at run time`}}},
		{"template without src", templateIface, map[string]any{"op": "render"}, []wantIssue{{sevError, "tasks[0].with.src",
			`with.src is required by the interface of ` + testPinned + ` for op "render" and is not supplied`}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertIssues(t, CheckWith(0, taskWith(t, tc.with), tc.iface(t)), tc.want)
		})
	}
}

func TestCheckWith_NoOpsMeansEveryKeyIsShared(t *testing.T) {
	iface := mustParse(t, `{"schemaVersion":1,"fields":{"target":{"type":"string","required":true}}}`)
	got := CheckWith(2, taskWith(t, map[string]any{"target": "prod", "op": "apply"}), iface)
	assertIssues(t, got, []wantIssue{{sevWarning, "tasks[2].with.op",
		`with.op is not declared by the interface of ` + testPinned + ` (declared: target); ` +
			`the image may still accept it — check for a typo or a key renamed in a newer image`}})
}

func TestCheckWith_IssueOrderIsStable(t *testing.T) {
	iface := mustParse(t, `{"schemaVersion":1,"fields":{"b":{"type":"number","required":true},"a":{"type":"number","required":true},"n":{"type":"number"},"m":{"type":"boolean"}},"ops":{"go":{"fields":{"c":{"type":"string","required":true}}}}}`)
	with := map[string]any{"op": "nosuch", "n": "1", "m": "yes", "zz": 1, "yy": 2}
	want := []wantIssue{
		{sevError, "tasks[0].with.op", `with.op "nosuch" is not an operation of ` + testPinned + ` (declared ops: go)`},
		{sevError, "tasks[0].with.a", `with.a is required by the interface of ` + testPinned + ` and is not supplied`},
		{sevError, "tasks[0].with.b", `with.b is required by the interface of ` + testPinned + ` and is not supplied`},
		{sevWarning, "tasks[0].with.yy", `with.yy is not declared by the interface of ` + testPinned + ` (declared: a, b, c, m, n, op); the image may still accept it — check for a typo or a key renamed in a newer image`},
		{sevWarning, "tasks[0].with.zz", `with.zz is not declared by the interface of ` + testPinned + ` (declared: a, b, c, m, n, op); the image may still accept it — check for a typo or a key renamed in a newer image`},
		{sevWarning, "tasks[0].with.m", `with.m is declared boolean by the interface of ` + testPinned + `; this document supplies a string — the image may coerce it, or refuse it at run time`},
		{sevWarning, "tasks[0].with.n", `with.n is declared number by the interface of ` + testPinned + `; this document supplies a string — the image may coerce it, or refuse it at run time`},
	}
	for range 100 {
		assertIssues(t, CheckWith(0, taskWith(t, with), iface), want)
	}
}

func TestCheckOrkflowInterfaces_SkipsUnresolvedAndReportsAMalformedLabel(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		{Id: "a", Uses: "unresolved"},
		taskWith(t, map[string]any{"op": "copy"}),
		{Id: "c", Uses: "broken", Image: &orkstackv1.ImageRef{Pinned: testPinned}},
		{Id: "d", Uses: "plain", Image: &orkstackv1.ImageRef{Pinned: testPinned}},
	}}
	resolutions := Resolutions{
		"file":   {Interface: fileInterface(t)},
		"broken": {InterfaceError: "schemaVersion 2 is not supported (this engine reads 1)"},
		"plain":  {},
	}
	assertIssues(t, CheckOrkflowInterfaces(wf, resolutions), []wantIssue{
		{sevError, "tasks[1].with.src", `with.src is required by the interface of ` + testPinned + ` for op "copy" and is not supplied`},
		{sevWarning, "tasks[2].uses", testPinned + ` carries an org.orkstack.activity.interface label that could not be read ` +
			`(schemaVersion 2 is not supported (this engine reads 1)); with: was not checked against it`},
	})
	if got := CheckOrkflowInterfaces(wf, nil); len(got) != 0 {
		t.Fatalf("no resolutions produced %d issues, want none", len(got))
	}
}

func mustMarshalInterface(t *testing.T, iface *orkstackv1.ActivityInterface) string {
	t.Helper()
	raw, err := protojson.Marshal(iface)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

func TestParseInterfaceLabel_AcceptsAUnionType(t *testing.T) {
	iface, err := ParseInterfaceLabel(`{"schemaVersion":1,"fields":{"body":{"type":"string|object|array"},"a":{"type":"string|number","default":"x"},"b":{"type":"string|number","default":3}}}`)
	if err != nil {
		t.Fatalf("a union type was refused: %v", err)
	}
	if got := iface.GetFields()["body"].GetType(); got != "string|object|array" {
		t.Fatalf("type = %q, want it as written", got)
	}
	_, err = ParseInterfaceLabel(`{"schemaVersion":1,"fields":{"a":{"type":"string|number","default":true}}}`)
	if err == nil || err.Error() != "fields.a: default is a boolean but type is string|number" {
		t.Fatalf("a default outside the union = %v", err)
	}
}

func TestParseInterfaceLabel_RefusesABadUnion(t *testing.T) {
	for typ, want := range map[string]string{
		"string|text":    `fields.a: type "string|text" is not one of string, number, boolean, object, array, or several of them joined by |`,
		"string||object": `fields.a: type "string||object" is not one of string, number, boolean, object, array, or several of them joined by |`,
		"|string":        `fields.a: type "|string" is not one of string, number, boolean, object, array, or several of them joined by |`,
		"string|string":  `fields.a: type "string|string" names "string" twice`,
	} {
		_, err := ParseInterfaceLabel(`{"schemaVersion":1,"fields":{"a":{"type":"` + typ + `"}}}`)
		if err == nil || err.Error() != want {
			t.Errorf("%s: err = %v, want %q", typ, err, want)
		}
	}
}

func TestCheckWith_AUnionAcceptsEachOfItsTypes(t *testing.T) {
	for _, body := range []any{"raw", map[string]any{"status": "ok"}, []any{1.0, "two"}} {
		if got := CheckWith(0, taskWith(t, map[string]any{"op": "post", "url": "https://x", "body": body}), httpInterface(t)); len(got) != 0 {
			t.Errorf("body %#v: %v", body, got)
		}
	}
	got := CheckWith(0, taskWith(t, map[string]any{"op": "post", "url": "https://x", "body": 12.0}), httpInterface(t))
	assertIssues(t, got, []wantIssue{{sevWarning, "tasks[0].with.body",
		`with.body is declared string|object|array by the interface of ` + testPinned +
			`; this document supplies a number — the image may coerce it, or refuse it at run time`}})
}
