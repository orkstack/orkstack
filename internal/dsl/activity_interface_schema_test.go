package dsl

import (
	"reflect"
	"sort"
	"strings"
	"testing"
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
