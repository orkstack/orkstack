package dsl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The JSON Schema of a service interface document
// (docs/schemas/serviceinterface.v1.json), reflected from the structs
// ParseServiceInterfaceDocument decodes into, as the activity interface's is:
// a key added to a struct without an entry below, or an entry left for a
// removed key, fails the schema's build.

func serviceInterfaceSchemaKeys() map[string]map[string]any {
	flag := func(text string) map[string]any { return map[string]any{"type": "boolean", "description": text} }
	return map[string]map[string]any{
		"serviceInterfaceDocument.apiVersion": {"const": APIVersion,
			"description": "The document format's version: exactly `orkstack/v1`."},
		"serviceInterfaceDocument.kind": {"const": KindServiceInterface,
			"description": "Exactly `ServiceInterface`: a service image's interface, packed into its `" + eventsources.InterfaceLabel + "` label at build time."},
		"serviceInterfaceDocument.profile": {"const": eventsources.ProfileEventSource,
			"description": "The profile the image implements: `event-source`."},
		"serviceInterfaceDocument.with": {
			"type":                 []any{"object", "null"},
			"maxProperties":        MaxInterfaceFields,
			"propertyNames":        map[string]any{"type": "string", "pattern": interfaceKeyPattern.String(), "maxLength": MaxInterfaceKeyBytes},
			"additionalProperties": map[string]any{"$ref": "#/definitions/serviceinterface.field"},
			"description":          "The keys of the image's `with:` block, each with its own `type`, `required` and `default`, as an activity interface's `fields:`.",
		},
		"serviceInterfaceDocument.event_source": {"$ref": "#/definitions/serviceinterface.event_source"},
		"serviceFieldDocument.type": {"type": "string", "pattern": interfaceTypePattern,
			"description": "One of string, number, boolean, object, array, or several joined by `|`."},
		"serviceFieldDocument.required": {"type": "boolean",
			"description": "A source without this key is refused. A key with a default is never required."},
		"serviceFieldDocument.default": {
			"description": "What the image assumes when the key is absent; not null, and of the declared type."},
		"eventSourceDeclarationDocument.delivery": {"enum": []any{"at-least-once", "at-most-once"},
			"description": "The image's delivery class — absent means `at-least-once`."},
		"eventSourceDeclarationDocument.max_concurrency": {"type": "integer", "minimum": 1, "maximum": eventsources.MaxConcurrency,
			"description": "The most messages one container handles at once — absent means 1."},
		"eventSourceDeclarationDocument.ordering":       flag("Messages can carry a group the image delivers in order."),
		"eventSourceDeclarationDocument.dead_letter":    flag("The broker stops delivering a message after a maximum; an at-least-once image must not say false."),
		"eventSourceDeclarationDocument.extends_leases": flag("The image keeps a message leased until its verdict; an at-least-once image must not say false."),
	}
}

// ServiceInterfaceSchema is the JSON Schema of a service interface
// document, as a JSON tree.
func ServiceInterfaceSchema() (map[string]any, error) {
	keys := serviceInterfaceSchemaKeys()
	used := map[string]bool{}
	root, err := interfaceObjectSchema(serviceInterfaceDocument{}, keys, used)
	if err != nil {
		return nil, err
	}
	field, err := interfaceObjectSchema(serviceFieldDocument{}, keys, used)
	if err != nil {
		return nil, err
	}
	decl, err := interfaceObjectSchema(eventSourceDeclarationDocument{}, keys, used)
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
		return nil, fmt.Errorf("dsl schema: service interface schema entries name no parser key: %s", strings.Join(stale, ", "))
	}
	field["required"] = []any{"type"}
	field["description"] = "One `with:` key: `type`, `required` and `default`."
	decl["description"] = "What an image of the event-source profile declares about how it delivers."
	root["required"] = []any{"apiVersion", "kind", "profile"}
	root["$schema"] = SchemaDialect
	root["$comment"] = SchemaComment
	root["title"] = "OrkStack service interface (" + APIVersion + ")"
	root["description"] = "A service image's interface (kind: ServiceInterface, docs/ORKER_SERVICE_CONTRACT.md): " +
		"`orkctl activity interface pack` packs it into the image's `" + eventsources.InterfaceLabel + "` label, which the engine reads."
	root["definitions"] = map[string]any{
		"serviceinterface.field":        field,
		"serviceinterface.event_source": decl,
	}
	return root, nil
}
