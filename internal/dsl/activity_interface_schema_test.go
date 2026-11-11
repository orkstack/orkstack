package dsl

import (
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// schemaKeys reads an object schema's property names, sorted.
func schemaKeys(t *testing.T, schema map[string]any) []string {
	t.Helper()
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema %v has no properties", schema)
	}
	var keys []string
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// yamlKeys reads a document struct's yaml keys, sorted.
func yamlKeys(v any) []string {
	var keys []string
	typ := reflect.TypeOf(v)
	for i := 0; i < typ.NumField(); i++ {
		keys = append(keys, strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0])
	}
	sort.Strings(keys)
	return keys
}

// TestActivityInterfaceSchema_HasEveryDocumentKey: the schema's objects hold
// exactly the keys the parser decodes, each closed to others, so the editor
// refuses what ParseInterfaceDocument refuses.
func TestActivityInterfaceSchema_HasEveryDocumentKey(t *testing.T) {
	root, err := ActivityInterfaceSchema()
	must.OK(t, err)
	defs := root["definitions"].(map[string]any)
	for _, tc := range []struct {
		name   string
		schema map[string]any
		doc    any
	}{
		{"the document", root, interfaceDocument{}},
		{"an op", defs["activityinterface.op"].(map[string]any), interfaceOpDocument{}},
		{"a field", defs["activityinterface.field"].(map[string]any), interfaceFieldDocument{}},
	} {
		if got, want := schemaKeys(t, tc.schema), yamlKeys(tc.doc); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: schema keys %v, parser keys %v", tc.name, got, want)
		}
		if tc.schema["additionalProperties"] != false {
			t.Errorf("%s: the schema admits keys the parser refuses", tc.name)
		}
	}
	if !reflect.DeepEqual(root["required"], []any{"apiVersion", "kind"}) {
		t.Errorf("required = %v, want apiVersion and kind", root["required"])
	}
	properties := root["properties"].(map[string]any)
	if got := properties["kind"].(map[string]any)["const"]; got != KindActivityInterface {
		t.Errorf("kind const = %v", got)
	}
	if got := properties["apiVersion"].(map[string]any)["const"]; got != APIVersion {
		t.Errorf("apiVersion const = %v", got)
	}
}

// TestActivityInterfaceSchema_BoundsAreTheParsers: every pattern and bound the
// schema states is the variable the parser checks.
func TestActivityInterfaceSchema_BoundsAreTheParsers(t *testing.T) {
	root, err := ActivityInterfaceSchema()
	must.OK(t, err)
	defs := root["definitions"].(map[string]any)
	properties := root["properties"].(map[string]any)
	fields := defs["activityinterface.fields"].(map[string]any)
	if got := fields["propertyNames"].(map[string]any)["pattern"]; got != interfaceKeyPattern.String() {
		t.Errorf("fields key pattern = %v, want %s", got, interfaceKeyPattern)
	}
	// An empty `fields:`, `values:` or `ops:` is null in YAML, and the
	// parser reads it as no keys; the schema must not flag it.
	if got := fields["type"]; !reflect.DeepEqual(got, []any{"object", "null"}) {
		t.Errorf("fields type = %v, want object or null", got)
	}
	if got := properties["ops"].(map[string]any)["type"]; !reflect.DeepEqual(got, []any{"object", "null"}) {
		t.Errorf("ops type = %v, want object or null", got)
	}
	if got := fields["maxProperties"]; got != MaxInterfaceFields {
		t.Errorf("fields maxProperties = %v, want %d", got, MaxInterfaceFields)
	}
	ops := properties["ops"].(map[string]any)
	if got := ops["propertyNames"].(map[string]any)["pattern"]; got != opNamePattern.String() {
		t.Errorf("ops key pattern = %v, want %s", got, opNamePattern)
	}
	if got := ops["maxProperties"]; got != MaxInterfaceOps {
		t.Errorf("ops maxProperties = %v, want %d", got, MaxInterfaceOps)
	}
	if got := properties["defaultOp"].(map[string]any)["pattern"]; got != opNamePattern.String() {
		t.Errorf("defaultOp pattern = %v, want %s", got, opNamePattern)
	}
	typ := defs["activityinterface.field"].(map[string]any)["properties"].(map[string]any)["type"].(map[string]any)
	pattern := typ["pattern"].(string)
	for _, word := range typeWords {
		if !strings.Contains(pattern, word) {
			t.Errorf("the type pattern %s does not name %s", pattern, word)
		}
	}
}

// stringVerdict evaluates the string keywords a generated schema uses for a
// description — type, minLength, maxLength (in characters, as JSON Schema
// counts) and pattern — against one value.
func stringVerdict(t *testing.T, schema map[string]any, value string) bool {
	t.Helper()
	if schema["type"] != "string" {
		t.Fatalf("schema %v is not a string schema", schema)
	}
	n := utf8.RuneCountInString(value)
	if limit, ok := schema["minLength"].(int); ok && n < limit {
		return false
	}
	if limit, ok := schema["maxLength"].(int); ok && n > limit {
		return false
	}
	if pattern, ok := schema["pattern"].(string); ok && !regexp.MustCompile(pattern).MatchString(value) {
		return false
	}
	return true
}

// TestActivityInterfaceSchema_JudgesLengthAndLineBreaksAsThePackerDoes: for
// each place a description stands, the generated schema and the packer
// agree on the rules the schema models — not empty, a length in
// characters, no line break but a block scalar's last — so the editor flags
// those refusals and none the packer would accept. The schema does not
// model the character rules (control, format, noncharacters) or the icon's
// SVG allowlist; those are the parser's alone.
func TestActivityInterfaceSchema_JudgesLengthAndLineBreaksAsThePackerDoes(t *testing.T) {
	root, err := ActivityInterfaceSchema()
	must.OK(t, err)
	defs := root["definitions"].(map[string]any)
	description := func(def map[string]any) map[string]any {
		return def["properties"].(map[string]any)["description"].(map[string]any)
	}
	places := []struct {
		name     string
		schema   map[string]any
		document func(quoted string) string
		limit    int
	}{
		{"the activity", unwrapDescribed(description(root)),
			func(q string) string { return "description: " + q + "\n" }, MaxActivityDescriptionChars},
		{"a key", unwrapDescribed(description(defs["activityinterface.field"].(map[string]any))),
			func(q string) string { return "fields:\n  a:\n    type: string\n    description: " + q + "\n" }, MaxDescriptionChars},
		{"an op", unwrapDescribed(description(defs["activityinterface.op"].(map[string]any))),
			func(q string) string { return "ops:\n  a:\n    description: " + q + "\n" }, MaxDescriptionChars},
	}
	for _, place := range places {
		for _, value := range []string{
			"Copies one file.",
			"Two `code` spans and ünïcödé.",
			"A folded block scalar ends with a line break.\n",
			"one\ntwo",
			"one\r\ntwo",
			"one\rtwo",
			strings.Repeat("日", place.limit),
			strings.Repeat("a", place.limit+1),
			"",
		} {
			quoted, err := stablejson.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			_, parseErr := ParseActivityDocument([]byte("apiVersion: orkstack/v1\nkind: ActivityInterface\n" + place.document(string(quoted))))
			if schema := stringVerdict(t, place.schema, value); schema != (parseErr == nil) {
				t.Errorf("%s: for %q the schema says %v, the packer %v", place.name, truncateText(value), schema, parseErr)
			}
		}
	}
}

// unwrapDescribed is a property's own schema, under withDescription's allOf
// when it has one.
func unwrapDescribed(schema map[string]any) map[string]any {
	if all, ok := schema["allOf"].([]any); ok && len(all) == 1 {
		return all[0].(map[string]any)
	}
	return schema
}
