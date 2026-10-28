package dsl

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// httpInterfaceDocument is images/activity-http's interface, authored.
const httpInterfaceDocument = `# The http activity's with: contract.
apiVersion: orkstack/v1
kind: ActivityInterface
fields:
  url:
    type: string
    required: true
  headers:
    type: object
  body:
    type: string|object|array
  timeoutSeconds:
    type: number
  captureBodyAs:
    type: string
ops:
  get: {}
  post: {}
  put: {}
  patch: {}
  delete: {}
  head: {}
defaultOp: get
values:
  status:
    type: number
  headers:
    type: object
  durationMs:
    type: number
  sizeBytes:
    type: number
  json:
    type: object
`

// httpLabelGolden is the label PackInterfaceLabel writes for it, byte for
// byte: protojson's field order, map keys sorted, no whitespace.
const httpLabelGolden = `{"schemaVersion":1,"fields":{"body":{"type":"string|object|array"},"captureBodyAs":{"type":"string"},` +
	`"headers":{"type":"object"},"timeoutSeconds":{"type":"number"},"url":{"type":"string","required":true}},` +
	`"ops":{"delete":{},"get":{},"head":{},"patch":{},"post":{},"put":{}},"defaultOp":"get",` +
	`"values":{"durationMs":{"type":"number"},"headers":{"type":"object"},"json":{"type":"object"},` +
	`"sizeBytes":{"type":"number"},"status":{"type":"number"}}}`

func TestParseInterfaceDocument_IsTheLabelsInterface(t *testing.T) {
	got, err := ParseInterfaceDocument([]byte(httpInterfaceDocument))
	if err != nil {
		t.Fatalf("ParseInterfaceDocument: %v", err)
	}
	want := mustParse(t, firstPartyLabel(t, "activity-http"))
	if !proto.Equal(got, want) {
		t.Fatalf("the document parses to\n%v\nthe label is\n%v", got, want)
	}
}

// interfaceDirs are the directories this repository builds an image from
// beside an interface.ork.yaml: every first-party image and the examples.
func interfaceDirs(t *testing.T) []string {
	t.Helper()
	var dirs []string
	for _, pattern := range []string{"images/activity-*/Dockerfile", "examples/custom-image*/Dockerfile"} {
		matches, err := filepath.Glob(filepath.Join("..", "..", pattern))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range matches {
			dirs = append(dirs, filepath.Dir(match))
		}
	}
	if len(dirs) != 8 {
		t.Fatalf("image directories = %v, want the six first-party images and the two examples", dirs)
	}
	return dirs
}

// TestInterfaceDocuments_AreTheirImagesLabels: each image's interface.ork.yaml
// is the source of the LABEL its Dockerfile carries — the same declaration,
// and the very bytes PackInterfaceLabel writes for it (genschemas
// regenerates the line).
func TestInterfaceDocuments_AreTheirImagesLabels(t *testing.T) {
	label := regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(InterfaceLabel) + `='([^']*)'$`)
	for _, dir := range interfaceDirs(t) {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(dir, "interface.ork.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			doc, err := ParseInterfaceDocument(source)
			if err != nil {
				t.Fatalf("interface.ork.yaml: %v", err)
			}
			dockerfile, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
			if err != nil {
				t.Fatal(err)
			}
			m := label.FindSubmatch(dockerfile)
			if m == nil {
				t.Fatal("the Dockerfile carries no single-line interface LABEL")
			}
			if carried := mustParse(t, string(m[1])); !proto.Equal(doc, carried) {
				t.Fatalf("interface.ork.yaml declares\n%v\nthe Dockerfile's LABEL\n%v", doc, carried)
			}
			packed, err := PackInterfaceLabel(doc)
			if err != nil {
				t.Fatal(err)
			}
			if string(m[1]) != packed {
				t.Fatalf("the Dockerfile's LABEL is not the packed interface.ork.yaml: run go run ./internal/schemas/cmd/genschemas\n"+
					"carried: %s\npacked:  %s", m[1], packed)
			}
		})
	}
}

func TestParseInterfaceDocument_Accepts(t *testing.T) {
	const header = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	str := func(s string) *structpb.Value { return structpb.NewStringValue(s) }
	for _, tc := range []struct {
		name   string
		source string
		want   *orkstackv1.ActivityInterface
	}{
		{"the header alone declares nothing", header, &orkstackv1.ActivityInterface{SchemaVersion: 1}},
		{"comments and quoted scalars", "# leading comment\n" + header +
			"fields:\n  \"as\":   # the output name\n    type: 'string'\n    default: \"report.txt\"\n",
			&orkstackv1.ActivityInterface{SchemaVersion: 1, Fields: map[string]*orkstackv1.FieldSchema{
				"as": {Type: "string", Default: str("report.txt")}}}},
		{"an op written with no body reads no keys", header + "ops:\n  resolve:\n  clone: {}\n",
			&orkstackv1.ActivityInterface{SchemaVersion: 1, Ops: map[string]*orkstackv1.OpSchema{
				"resolve": {}, "clone": {}}}},
		{"defaults of every type", header + "fields:\n" +
			"  n:\n    type: number\n    default: 1.5\n" +
			"  b:\n    type: boolean\n    default: false\n" +
			"  a:\n    type: array\n    default:\n      - x\n" +
			"  o:\n    type: object\n    default: {}\n",
			&orkstackv1.ActivityInterface{SchemaVersion: 1, Fields: map[string]*orkstackv1.FieldSchema{
				"n": {Type: "number", Default: structpb.NewNumberValue(1.5)},
				"b": {Type: "boolean", Default: structpb.NewBoolValue(false)},
				"a": {Type: "array", Default: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{str("x")}})},
				"o": {Type: "object", Default: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{}})},
			}}},
		{"a dotted, camelCase with: key", header + "fields:\n  tls.caFile:\n    type: string\n",
			&orkstackv1.ActivityInterface{SchemaVersion: 1, Fields: map[string]*orkstackv1.FieldSchema{
				"tls.caFile": {Type: "string"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInterfaceDocument([]byte(tc.source))
			if err != nil {
				t.Fatalf("ParseInterfaceDocument: %v\n%s", err, tc.source)
			}
			if !proto.Equal(got, tc.want) {
				t.Fatalf("got\n%v\nwant\n%v", got, tc.want)
			}
		})
	}
}

func TestParseInterfaceDocument_Refuses(t *testing.T) {
	const header = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	for _, tc := range []struct {
		name   string
		source string
		want   string // a substring of the error
	}{
		{"an empty document", "", `the document has no apiVersion or kind: an activity interface begins with the two lines "apiVersion: orkstack/v1" and "kind: ActivityInterface"`},
		{"only a comment", "# nothing\n", `the document has no apiVersion or kind`},
		{"a list", "- a\n", `an activity interface is a YAML mapping that begins with "apiVersion: orkstack/v1" and "kind: ActivityInterface", got a list`},
		{"no apiVersion", "kind: ActivityInterface\n", `apiVersion is required: add the line "apiVersion: orkstack/v1" above "kind: ActivityInterface"`},
		{"no kind", "apiVersion: orkstack/v1\n", `kind is required: add the line "kind: ActivityInterface" below "apiVersion: orkstack/v1"`},
		{"another apiVersion", "apiVersion: orkstack/v2\nkind: ActivityInterface\n", `apiVersion "orkstack/v2" is not supported: this engine reads apiVersion: orkstack/v1`},
		{"an Orkflow", "apiVersion: orkstack/v1\nkind: Orkflow\n", `kind: Orkflow is an Orkflow, not an activity interface: apply it with orkctl apply; an activity interface says kind: ActivityInterface`},
		{"an automation document", "apiVersion: orkstack/v1\nkind: Automation\n", `kind: Automation is an automation document, not an activity interface: apply it with orkctl apply; an activity interface says kind: ActivityInterface`},
		{"an unknown kind", "apiVersion: orkstack/v1\nkind: Interface\n", `kind "Interface" is not an activity interface: an activity interface says kind: ActivityInterface`},
		{"a kind that is not a string", "apiVersion: orkstack/v1\nkind: 1\n", `kind must be the string ActivityInterface, got the number 1`},
		{"schemaVersion written", header + "schemaVersion: 1\n", `schemaVersion is not written in an activity interface: apiVersion names the document's format, and orkctl activity interface pack writes the label's schemaVersion`},
		{"an unknown key", header + "field:\n  a:\n    type: string\n", `field field not found`},
		{"a snake_case key", header + "default_op: get\nops:\n  get: {}\n", `field default_op not found`},
		{"an unknown field key", header + "fields:\n  a:\n    type: string\n    pattern: x\n", `field pattern not found`},
		{"a duplicate key", header + "fields:\n  a:\n    type: string\n  a:\n    type: number\n", `already defined`},
		{"required written as a YAML 1.1 boolean", header + "fields:\n  a:\n    type: string\n    required: yes\n", `fields.a.required`},
		{"required that is not a boolean", header + "fields:\n  a:\n    type: string\n    required: \"true\"\n", `fields.a.required: required is true or false, got "true"`},
		{"a YAML 1.1 boolean default", header + "fields:\n  a:\n    type: string\n    default: yes\n", `fields.a.default`},
		{"an octal-looking default", header + "fields:\n  a:\n    type: number\n    default: 0755\n", `fields.a.default`},
		{"an anchor", header + "fields:\n  a: &shared\n    type: string\n", `the anchor &shared is not allowed`},
		{"an alias", header + "fields:\n  a: &shared\n    type: string\n  b: *shared\n", `not allowed`},
		{"a merge key", header + "fields:\n  a:\n    <<: {type: string}\n", `merge key`},
		{"a tag", header + "fields:\n  a:\n    type: !!str string\n", `the tag !!str is not allowed`},
		{"a second document", header + "---\n" + header, `one activity interface per file: a second YAML document (---) is not allowed`},
		{"a type outside the vocabulary", header + "fields:\n  a:\n    type: text\n", `fields.a: type "text" is not one of string, number, boolean, object, array`},
		{"a field with no type", header + "fields:\n  a:\n    required: true\n", `fields.a: type "" is not one of`},
		{"required and default", header + "fields:\n  a:\n    type: string\n    required: true\n    default: x\n", `fields.a: required and default contradict`},
		{"a null default", header + "fields:\n  a:\n    type: string\n    default: ~\n", `fields.a: null is not a default`},
		{"a default of the wrong type", header + "fields:\n  a:\n    type: number\n    default: x\n", `fields.a: default is a string but type is number`},
		{"an op name that is not lowercase", header + "ops:\n  Get: {}\n", `op "Get" is not a lowercase op name`},
		{"defaultOp naming no op", header + "ops:\n  get: {}\ndefaultOp: post\n", `defaultOp "post" is not one of the declared ops (get)`},
		{"a key both shared and under an op", header + "fields:\n  url:\n    type: string\nops:\n  get:\n    fields:\n      url:\n        type: string\n", `key "url" is declared both in fields and under op "get"`},
		{"an invalid with: key", header + "fields:\n  \"a b\":\n    type: string\n", `fields: key "a b" is not a valid with: key`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iface, err := ParseInterfaceDocument([]byte(tc.source))
			if err == nil {
				t.Fatalf("ParseInterfaceDocument accepted\n%s\nas %v", tc.source, iface)
			}
			if iface != nil {
				t.Fatalf("ParseInterfaceDocument refused with %v but returned %v", err, iface)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error\n  %v\nwant it to contain\n  %s", err, tc.want)
			}
		})
	}
}

func TestParseInterfaceDocument_RefusesAnOversizedSource(t *testing.T) {
	source := "apiVersion: orkstack/v1\nkind: ActivityInterface\n# " + strings.Repeat("x", MaxInterfaceDocumentBytes) + "\n"
	_, err := ParseInterfaceDocument([]byte(source))
	if err == nil || !strings.Contains(err.Error(), "limit is") {
		t.Fatalf("an oversized document: %v", err)
	}
}

// TestParseInterfaceDocument_HeaderErrorsNameTheirField: a header refusal is
// a ValidationError on the line it names, like an Orkflow's.
func TestParseInterfaceDocument_HeaderErrorsNameTheirField(t *testing.T) {
	_, err := ParseInterfaceDocument([]byte("apiVersion: orkstack/v1\nkind: Orkflow\n"))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Field != "kind" {
		t.Fatalf("got %v, want a ValidationError on kind", err)
	}
}

func TestPackInterfaceLabel_IsCanonical(t *testing.T) {
	iface, err := ParseInterfaceDocument([]byte(httpInterfaceDocument))
	if err != nil {
		t.Fatal(err)
	}
	got, err := PackInterfaceLabel(iface)
	if err != nil {
		t.Fatalf("PackInterfaceLabel: %v", err)
	}
	if got != httpLabelGolden {
		t.Fatalf("label\n  %s\nwant\n  %s", got, httpLabelGolden)
	}
}

func TestPackInterfaceLabel_Golden(t *testing.T) {
	for _, tc := range []struct {
		name  string
		iface *orkstackv1.ActivityInterface
		want  string
	}{
		{"nothing declared", &orkstackv1.ActivityInterface{}, `{"schemaVersion":1}`},
		{"a whole-number default is written without a fraction",
			&orkstackv1.ActivityInterface{Fields: map[string]*orkstackv1.FieldSchema{
				"depth": {Type: "number", Default: structpb.NewNumberValue(1)}}},
			`{"schemaVersion":1,"fields":{"depth":{"type":"number","default":1}}}`},
		{"a single quote is escaped so the value fits a Dockerfile's single quotes",
			&orkstackv1.ActivityInterface{Fields: map[string]*orkstackv1.FieldSchema{
				"greeting": {Type: "string", Default: structpb.NewStringValue("it's <ok> & done")}}},
			`{"schemaVersion":1,"fields":{"greeting":{"type":"string","default":"it\u0027s <ok> & done"}}}`},
		{"uppercase keys sort before lowercase ones",
			&orkstackv1.ActivityInterface{Values: map[string]*orkstackv1.FieldSchema{
				"b": {Type: "string"}, "A": {Type: "string"}, "a": {Type: "string"}}},
			`{"schemaVersion":1,"values":{"A":{"type":"string"},"a":{"type":"string"},"b":{"type":"string"}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PackInterfaceLabel(tc.iface)
			if err != nil {
				t.Fatalf("PackInterfaceLabel: %v", err)
			}
			if got != tc.want {
				t.Fatalf("label\n  %s\nwant\n  %s", got, tc.want)
			}
			if strings.Contains(got, "'") {
				t.Fatalf("label %s holds a single quote", got)
			}
			back, err := ParseInterfaceLabel(got)
			if err != nil {
				t.Fatalf("the packed label does not parse: %v", err)
			}
			want := proto.Clone(tc.iface).(*orkstackv1.ActivityInterface)
			want.SchemaVersion = 1
			if !proto.Equal(back, want) {
				t.Fatalf("round trip\n%v\nwant\n%v", back, want)
			}
		})
	}
}

// TestPackInterfaceLabel_LeavesItsInputAlone: packing writes the lowest
// schemaVersion into a copy, never into the caller's message.
func TestPackInterfaceLabel_LeavesItsInputAlone(t *testing.T) {
	iface := &orkstackv1.ActivityInterface{}
	if _, err := PackInterfaceLabel(iface); err != nil {
		t.Fatal(err)
	}
	if iface.GetSchemaVersion() != 0 {
		t.Fatalf("PackInterfaceLabel set the caller's schemaVersion to %d", iface.GetSchemaVersion())
	}
}

// TestPackInterfaceLabel_RoundTripsEveryFirstPartyLabel: packing what a
// first-party label declares yields a label that declares the same thing.
func TestPackInterfaceLabel_RoundTripsEveryFirstPartyLabel(t *testing.T) {
	for _, image := range []string{"activity-file", "activity-git", "activity-http", "activity-json", "activity-shell", "activity-template"} {
		t.Run(image, func(t *testing.T) {
			iface := mustParse(t, firstPartyLabel(t, image))
			packed, err := PackInterfaceLabel(iface)
			if err != nil {
				t.Fatalf("PackInterfaceLabel: %v", err)
			}
			if back := mustParse(t, packed); !proto.Equal(back, iface) {
				t.Fatalf("round trip\n%v\nwant\n%v", back, iface)
			}
		})
	}
}

func TestPackInterfaceLabel_Refuses(t *testing.T) {
	wide := map[string]*orkstackv1.FieldSchema{}
	for i := 0; i < MaxInterfaceFields; i++ {
		wide[strings.Repeat("k", 100)+string(rune('a'+i%26))+strings.Repeat("z", i/26)] = &orkstackv1.FieldSchema{Type: "string|number|boolean|object|array"}
	}
	ops := map[string]*orkstackv1.OpSchema{}
	for i := 0; i < 8; i++ {
		ops[string(rune('a'+i))] = &orkstackv1.OpSchema{Fields: wide}
	}
	for _, tc := range []struct {
		name  string
		iface *orkstackv1.ActivityInterface
		want  string
	}{
		{"an invalid type", &orkstackv1.ActivityInterface{Fields: map[string]*orkstackv1.FieldSchema{"a": {Type: "text"}}},
			`fields.a: type "text" is not one of`},
		{"a label past the bound", &orkstackv1.ActivityInterface{Ops: ops},
			`label is`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PackInterfaceLabel(tc.iface)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("PackInterfaceLabel = %d bytes, %v; want an error containing %q", len(got), err, tc.want)
			}
		})
	}
}
