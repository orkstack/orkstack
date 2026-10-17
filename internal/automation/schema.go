package automation

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The JSON Schema of an automation document (docs/schemas/automation.v1.json),
// generated from this parser's key lists and settings rows. Each settings
// row's value schema is derived from the orkstack.v1.Namespace field the row
// names; settingWords and settingMinimums carry what the descriptor cannot
// say. The schedule, trigger and webhook value schemas are this file's,
// held to the parser by TestAutomationSchema_TypesAgreeWithTheParser.

// automationDescriptions is each key's hover text, copied from
// docs/AUTOMATION.md's tables: the Meaning or Value column, then " — absent
// means " and the Absent column where it says something
// (TestAutomationSchema_DescriptionsAreAutomationText holds each part there).
var automationDescriptions = map[string]string{
	"apiVersion": "Exactly `orkstack/v1`.",
	"kind":       "Exactly `Automation`; an Orkflow says `kind: Orkflow`, and `orkctl apply` dispatches on it.",
	"namespace": "The one namespace the document describes, a slug. Required and authoritative: never inferred from " +
		"`-n`, `ORK_NAMESPACE` or a saved default, so a document that prunes can never land in the wrong namespace by accident of a shell.",
	"settings":  "The namespace's settings, as an overlay.",
	"schedules": "`name: entry` mappings.",
	"triggers":  "`name: entry` mappings.",
	"webhooks":  "`name: entry` mappings.",

	"settings.description":       "string",
	"settings.labels":            "mapping of strings",
	"settings.default_pool":      "string",
	"settings.allowed_pools":     "list of strings",
	"settings.run_retention":     "Go duration, or `inherit`",
	"settings.telemetry":         "mapping: `sink`, `task_logs` (`inherit`|`enabled`|`disabled`), `attributes`",
	"settings.max_running":       "integer ≥ 0",
	"settings.allowed_secrets":   "list of strings",
	"settings.require_signed_by": "string",
	"settings.budget":            "mapping: `cpu_seconds`, `gib_seconds` (integers ≥ 0), `period` (Go duration), `enforce` (boolean) — the `--budget-*` flags",

	"schedules.*.orkflow":          "slug, **required**, fixed at creation",
	"schedules.*.version":          "string — absent means the current pointer at each fire",
	"schedules.*.description":      "string, ≤ 256 bytes, no control characters",
	"schedules.*.cron":             "string, or list of strings — absent means none",
	"schedules.*.interval":         "Go duration — absent means unset",
	"schedules.*.timezone":         "IANA name — absent means UTC",
	"schedules.*.jitter":           "Go duration — absent means none",
	"schedules.*.start_at":         "RFC 3339 — absent means unbounded",
	"schedules.*.end_at":           "RFC 3339 — absent means unbounded",
	"schedules.*.overlap":          "`skip`|`buffer-one`|`buffer-all`|`cancel-other`|`allow-all` — absent means `skip`",
	"schedules.*.catchup_window":   "Go duration, ≥ 10s — absent means the cluster's default",
	"schedules.*.pause_on_failure": "boolean — absent means `false`",
	"schedules.*.input":            "a YAML mapping, converted to JSON by the strict mapping (`docs/JSON.md`) — absent means none",

	"triggers.*.orkflow":     "slug, **required**, fixed at creation",
	"triggers.*.version":     "string — absent means the current pointer at each invoke",
	"triggers.*.description": "string",

	"webhooks.*.url":        "absolute `http(s)` URL, **required**",
	"webhooks.*.events":     "list of event names — absent means every run transition",
	"webhooks.*.orkflow":    "slug — absent means every Orkflow",
	"webhooks.*.task":       "slug; needs a `TASK_*` event — absent means every task",
	"webhooks.*.secret_env": "environment-variable name — absent means a generated signing secret, disclosed once",
}

// settingWords are the document words of an enum field, keyed by the field's
// full name: the descriptor cannot say them.
var settingWords = map[protoreflect.FullName][]string{
	"orkstack.v1.TelemetryPolicy.task_logs": sortedTaskLogWords(),
}

// settingMinimums are the parsers' lower bounds, keyed by full name.
var settingMinimums = map[protoreflect.FullName]int{
	"orkstack.v1.Namespace.max_running": 0,
	"orkstack.v1.Budget.cpu_seconds":    0,
	"orkstack.v1.Budget.gib_seconds":    0,
}

// settingSubKeys are the parsers' sub-key lists of the settings that are
// messages, which their fields' names must equal.
var settingSubKeys = map[protoreflect.FullName][]string{
	"orkstack.v1.TelemetryPolicy": telemetrySubKeys,
	"orkstack.v1.Budget":          budgetSubKeys,
}

func sortedTaskLogWords() []string {
	words := make([]string, 0, len(taskLogWords))
	for word := range taskLogWords {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

// namespaceField is the descriptor of the Namespace field a settings row sets.
func namespaceField(name protoreflect.Name) protoreflect.FieldDescriptor {
	return (&orkstackv1.Namespace{}).ProtoReflect().Descriptor().Fields().ByName(name)
}

// settingSchema is a settings value's schema, derived from its field.
func settingSchema(fd protoreflect.FieldDescriptor) (map[string]any, error) {
	if fd.IsMap() {
		value, err := scalarSchema(fd.MapValue())
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "object", "additionalProperties": value}, nil
	}
	item, err := scalarSchema(fd)
	if err != nil {
		return nil, err
	}
	if fd.IsList() {
		return map[string]any{"type": "array", "items": item}, nil
	}
	return item, nil
}

// scalarSchema is one value's schema: a scalar, an enum, a duration or a
// message of its own.
func scalarSchema(fd protoreflect.FieldDescriptor) (map[string]any, error) {
	var out map[string]any
	switch fd.Kind() {
	case protoreflect.StringKind:
		out = map[string]any{"type": "string"}
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		out = map[string]any{"type": "integer"}
	case protoreflect.BoolKind:
		out = map[string]any{"type": "boolean"}
	case protoreflect.EnumKind:
		words, ok := settingWords[fd.FullName()]
		if !ok {
			return nil, fmt.Errorf("automation schema: the enum field %s has no document words", fd.FullName())
		}
		out = map[string]any{"enum": append([]string(nil), words...)}
	case protoreflect.MessageKind:
		message := fd.Message()
		if message.FullName() == "google.protobuf.Duration" {
			// The one well-known type a document writes, as text.
			return map[string]any{"type": "string",
				"description": "A Go duration such as 168h, which `orkctl apply` checks."}, nil
		}
		properties := map[string]any{}
		for i := 0; i < message.Fields().Len(); i++ {
			field := message.Fields().Get(i)
			schema, err := settingSchema(field)
			if err != nil {
				return nil, err
			}
			properties[string(field.Name())] = schema
		}
		return map[string]any{"type": "object", "additionalProperties": false, "properties": properties}, nil
	default:
		return nil, fmt.Errorf("automation schema: %s has no document type", fd.FullName())
	}
	if minimum, ok := settingMinimums[fd.FullName()]; ok {
		out["minimum"] = minimum
	}
	return out, nil
}

func describe(path string, schema map[string]any) map[string]any {
	text, ok := automationDescriptions[path]
	if !ok {
		return schema
	}
	out := make(map[string]any, len(schema)+1)
	for k, v := range schema {
		out[k] = v
	}
	if _, isRef := out["$ref"]; isRef {
		return map[string]any{"allOf": []any{schema}, "description": text}
	}
	out["description"] = text
	return out
}

// entrySchemas are the value schemas of the schedule, trigger and webhook
// keys. versionScalar reads a number too; the schema says string, steering
// toward a quoted version (docs/DSL.md "Editor support", class V).
func entrySchemas() map[string]map[string]any {
	slug := map[string]any{"type": "string", "pattern": dslSlugPattern, "maxLength": dsl.MaxSlug}
	str := map[string]any{"type": "string"}
	events := make([]string, 0, len(webhooks.KnownEvents()))
	for _, event := range webhooks.KnownEvents() {
		events = append(events, string(event))
	}
	return map[string]map[string]any{
		"schedule.orkflow":          slug,
		"schedule.version":          str,
		"schedule.description":      {"type": "string", "maxLength": scheduleTextMaxBytes},
		"schedule.cron":             {"anyOf": []any{map[string]any{"type": "string", "maxLength": cronMaxBytes}, map[string]any{"type": "array", "items": map[string]any{"type": "string", "maxLength": cronMaxBytes}}}},
		"schedule.interval":         str,
		"schedule.timezone":         str,
		"schedule.jitter":           str,
		"schedule.start_at":         {"type": "string", "format": "date-time"},
		"schedule.end_at":           {"type": "string", "format": "date-time"},
		"schedule.overlap":          {"enum": overlapSlugs()},
		"schedule.catchup_window":   str,
		"schedule.pause_on_failure": {"type": "boolean"},
		"schedule.input":            {"type": "object"},
		"trigger.orkflow":           slug,
		"trigger.version":           str,
		"trigger.description":       str,
		"webhook.url":               str,
		"webhook.events":            {"type": "array", "items": map[string]any{"enum": events}},
		"webhook.orkflow":           slug,
		"webhook.task":              slug,
		"webhook.secret_env":        {"type": "string", "pattern": secretEnvPattern.String(), "maxLength": secretEnvMaxBytes},
	}
}

// dslSlugPattern is the slug rule every name in the document is held to.
var dslSlugPattern = dsl.SlugPattern()

// entryRequired are each entry's required keys.
var entryRequired = map[string][]string{
	"schedule": {"orkflow"},
	"trigger":  {"orkflow"},
	"webhook":  {"url"},
}

// Schema is the JSON Schema of an automation document, as a JSON tree.
func Schema() (map[string]any, error) {
	definitions := map[string]any{}
	settings := map[string]any{}
	for _, row := range namespaceSettings {
		fd := namespaceField(row.field)
		if fd == nil {
			return nil, fmt.Errorf("automation schema: settings.%s names no Namespace field %q", row.key, row.field)
		}
		schema, err := settingSchema(fd)
		if err != nil {
			return nil, err
		}
		settings[row.key] = describe("settings."+row.key, schema)
	}
	definitions["automation.settings"] = map[string]any{"type": "object", "additionalProperties": false, "properties": settings}
	entries := entrySchemas()
	for kind, keys := range map[string][]string{"schedule": scheduleKeys, "trigger": triggerKeys, "webhook": webhookKeys} {
		properties := map[string]any{}
		for _, key := range keys {
			schema, ok := entries[kind+"."+key]
			if !ok {
				return nil, fmt.Errorf("automation schema: %s key %s has no schema", kind, key)
			}
			properties[key] = describe(kind+"s.*."+key, schema)
		}
		required := make([]any, 0, len(entryRequired[kind]))
		for _, key := range entryRequired[kind] {
			required = append(required, key)
		}
		definitions["automation."+kind] = map[string]any{
			"type": "object", "additionalProperties": false, "properties": properties, "required": required,
		}
	}
	section := func(kind string) map[string]any {
		return map[string]any{
			"type": "object", "additionalProperties": map[string]any{"$ref": "#/definitions/automation." + kind},
			"propertyNames": map[string]any{"type": "string", "pattern": dslSlugPattern, "maxLength": dsl.MaxSlug},
		}
	}
	properties := map[string]any{
		"apiVersion": describe("apiVersion", map[string]any{"const": dsl.APIVersion}),
		"kind":       describe("kind", map[string]any{"const": dsl.KindAutomation}),
		"namespace":  describe("namespace", map[string]any{"type": "string", "pattern": dslSlugPattern, "maxLength": dsl.MaxSlug}),
		"settings":   describe("settings", map[string]any{"$ref": "#/definitions/automation.settings"}),
		"schedules":  describe("schedules", section("schedule")),
		"triggers":   describe("triggers", section("trigger")),
		"webhooks":   describe("webhooks", section("webhook")),
	}
	for _, key := range topLevelKeys {
		if _, ok := properties[key]; !ok {
			return nil, fmt.Errorf("automation schema: top-level key %s has no schema", key)
		}
	}
	return map[string]any{
		"$schema":  dsl.SchemaDialect,
		"$comment": dsl.SchemaComment,
		"title":    "OrkStack automation document (" + dsl.APIVersion + ")",
		"description": "One namespace's settings, schedules, triggers and webhooks, declared by name (docs/AUTOMATION.md). " +
			"`orkctl apply --dry-run` plans it against what the namespace holds; a schema checks only its shape.",
		"type": "object", "additionalProperties": false, "properties": properties,
		"required":    []any{"apiVersion", "kind", "namespace"},
		"definitions": definitions,
	}, nil
}
