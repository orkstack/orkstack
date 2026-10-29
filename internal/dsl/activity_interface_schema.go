package dsl

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// The JSON Schema of an activity interface document
// (docs/schemas/activityinterface.v1.json).
//
// Every object's keys are reflected from the struct ParseInterfaceDocument
// decodes into, and every pattern and bound is the variable the parser
// checks; what the structs cannot say — each key's schema and description —
// is the table below, keyed by "<struct>.<yaml key>". A key added to a struct
// without an entry, or an entry left for a removed key, fails the build of
// the schema rather than drifting from the parser.

// interfaceTypePattern is a field's type: one word of the vocabulary, or
// several joined by "|".
var interfaceTypePattern = func() string {
	words := strings.Join(typeWords, "|")
	return "^(" + words + `)(\|(` + words + "))*$"
}()

// interfaceSchemaKeys is each document key's schema.
func interfaceSchemaKeys() map[string]map[string]any {
	fields := map[string]any{"$ref": "#/definitions/activityinterface.fields"}
	return map[string]map[string]any{
		"interfaceDocument.apiVersion": {"const": APIVersion,
			"description": "The document format's version: exactly `orkstack/v1`."},
		"interfaceDocument.kind": {"const": KindActivityInterface,
			"description": "Exactly `ActivityInterface`: an image's `with:` contract, packed into its `" + InterfaceLabel + "` label at build time."},
		"interfaceDocument.fields": withDescription(fields,
			"The top-level `with:` keys every op reads (for an image with no `op`, the whole `with:` block)."),
		"interfaceDocument.ops": {
			"type":                 []any{"object", "null"},
			"maxProperties":        MaxInterfaceOps,
			"propertyNames":        map[string]any{"type": "string", "pattern": opNamePattern.String()},
			"additionalProperties": map[string]any{"anyOf": []any{map[string]any{"$ref": "#/definitions/activityinterface.op"}, map[string]any{"type": "null"}}},
			"description":          "The image follows the `op` convention: each op's name, and the keys it reads beside `fields`. `op` is then implied and must not appear in `fields`.",
		},
		"interfaceDocument.defaultOp": {"type": "string", "pattern": opNamePattern.String(),
			"description": "The op the image runs when `op` is absent; with `ops` and no `defaultOp`, `op` is required."},
		"interfaceDocument.values": withDescription(fields,
			"Documentary: the keys the image writes to `values.json`."),
		"interfaceOpDocument.fields": withDescription(fields,
			"The keys this op reads in addition to `fields`; a key is declared once, shared or under an op."),
		"interfaceFieldDocument.type": {"type": "string", "pattern": interfaceTypePattern,
			"description": "One of string, number, boolean, object, array, or several joined by `|`."},
		"interfaceFieldDocument.required": {"type": "boolean",
			"description": "The image refuses to run without this key. A key with a default is never required."},
		"interfaceFieldDocument.default": {
			"description": "Documentary: what the image assumes when the key is absent. Never written into `with:`; not null, and of the declared type."},
	}
}

// interfaceObjectSchema reflects one document struct into a closed object.
func interfaceObjectSchema(doc any, keys map[string]map[string]any, used map[string]bool) (map[string]any, error) {
	typ := reflect.TypeOf(doc)
	properties := map[string]any{}
	for i := 0; i < typ.NumField(); i++ {
		key := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]
		entry := typ.Name() + "." + key
		schema, ok := keys[entry]
		if !ok {
			return nil, fmt.Errorf("dsl schema: the interface key %s has no schema", entry)
		}
		used[entry] = true
		properties[key] = schema
	}
	return map[string]any{"type": "object", "additionalProperties": false, "properties": properties}, nil
}

// ActivityInterfaceSchema is the JSON Schema of an activity interface
// document, as a JSON tree.
func ActivityInterfaceSchema() (map[string]any, error) {
	keys := interfaceSchemaKeys()
	used := map[string]bool{}
	root, err := interfaceObjectSchema(interfaceDocument{}, keys, used)
	if err != nil {
		return nil, err
	}
	op, err := interfaceObjectSchema(interfaceOpDocument{}, keys, used)
	if err != nil {
		return nil, err
	}
	field, err := interfaceObjectSchema(interfaceFieldDocument{}, keys, used)
	if err != nil {
		return nil, err
	}
	var stale []string
	for entry := range keys {
		if !used[entry] {
			stale = append(stale, entry)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		return nil, fmt.Errorf("dsl schema: interface schema entries name no parser key: %s", strings.Join(stale, ", "))
	}
	field["required"] = []any{"type"}
	field["description"] = "One `with:` key: `type`, `required` and a documentary `default` — the vocabulary an Orkflow's `inputs:` has, and not one word more."
	op["description"] = "One op: the keys it reads beside `fields`."
	root["required"] = []any{"apiVersion", "kind"}
	root["$schema"] = SchemaDialect
	root["$comment"] = SchemaComment
	root["title"] = "OrkStack activity interface (" + APIVersion + ")"
	root["description"] = "An image's `with:` contract (kind: ActivityInterface, docs/ACTIVITY_IMAGES.md): " +
		"`orkctl activity interface pack` packs it into the image's `" + InterfaceLabel + "` label, which the engine reads."
	root["definitions"] = map[string]any{
		"activityinterface.fields": map[string]any{
			// null as well: the parser reads `fields:` with nothing under it
			// as no keys, and the editor must not flag what pack accepts.
			"type":                 []any{"object", "null"},
			"maxProperties":        MaxInterfaceFields,
			"propertyNames":        map[string]any{"type": "string", "pattern": interfaceKeyPattern.String(), "maxLength": MaxInterfaceKeyBytes},
			"additionalProperties": map[string]any{"$ref": "#/definitions/activityinterface.field"},
		},
		"activityinterface.field": field,
		"activityinterface.op":    op,
	}
	return root, nil
}
