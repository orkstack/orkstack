package automation

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/eventsources"
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
	"kind":       "Exactly `Automation`; an Orkflow says `kind: Orkflow`, and `orkctl automation apply` refuses any other kind, as `orkctl orkflow create` refuses this one.",
	"namespace": "The one namespace the document describes, a slug. Required and authoritative: never inferred from " +
		"`-n`, `ORK_NAMESPACE` or a saved default, so a document that prunes can never land in the wrong namespace by accident of a shell.",
	"settings":      "The namespace's settings, as an overlay.",
	"schedules":     "`name: entry` mappings.",
	"triggers":      "`name: entry` mappings.",
	"webhooks":      "`name: entry` mappings.",
	"event_sources": "`name: entry` mappings.",

	"settings.description":       "string",
	"settings.labels":            "mapping of strings",
	"settings.default_pool":      "string",
	"settings.allowed_pools":     "list of strings",
	"settings.run_retention":     "Go duration, or `inherit`",
	"settings.telemetry":         "mapping: `sink`, `task_logs` (`inherit`|`enabled`|`disabled`), `attributes`",
	"settings.max_running":       "integer ≥ 0",
	"settings.fair_share_weight": "number from 0.001 to 1000, or 0 for the default weight of 1",
	"settings.allowed_secrets":   "list of strings",
	"settings.require_signed_by": "string",
	"settings.budget":            "mapping: `cpu_seconds`, `gib_seconds` (integers ≥ 0), `period` (Go duration), `enforce` (boolean) — the `--budget-*` flags",

	"schedules.*.orkflow":          "slug of this namespace's Orkflow, **required**, fixed at creation",
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

	"triggers.*.orkflow":     "slug of this namespace's Orkflow, **required**, fixed at creation",
	"triggers.*.version":     "string — absent means the current pointer at each invoke",
	"triggers.*.description": "string",

	"webhooks.*.url":        "absolute `http(s)` URL, **required**",
	"webhooks.*.events":     "list of event names — absent means every run transition",
	"webhooks.*.orkflow":    "slug — absent means every Orkflow",
	"webhooks.*.task":       "slug; needs a `TASK_*` event — absent means every task",
	"webhooks.*.secret_env": "environment-variable name — absent means a generated signing secret, disclosed once",

	"event_sources.*.orkflow":        "slug of this namespace's Orkflow, **required**, fixed at creation",
	"event_sources.*.version":        "string — absent means the current pointer, for each message",
	"event_sources.*.description":    "string, at most 256 bytes",
	"event_sources.*.uses":           "a source image's registry reference, **required**",
	"event_sources.*.with":           "the image's settings, a YAML mapping converted by the strict mapping, checked against the image's declared interface — absent means `{}`",
	"event_sources.*.secrets":        "list of `name`, `source`, as a task's — absent means none",
	"event_sources.*.identities":     "refused until task identity is available",
	"event_sources.*.credentials":    "refused until task identity is available",
	"event_sources.*.env":            "mapping of strings; configuration, never a secret — absent means none",
	"event_sources.*.pool":           "the pool whose source hosts run the instances — absent means the namespace's default",
	"event_sources.*.instances":      "1 to `event_sources.max_instances` — absent means `1`",
	"event_sources.*.resources":      "`cpu`, `memory`, per instance — absent means the pool's most",
	"event_sources.*.delivery":       "`at-least-once`|`at-most-once`, the image's declared class — absent means `at-least-once`",
	"event_sources.*.concurrency":    "messages an instance handles at once, 1 to 64 and at most the image's maximum — absent means `1`",
	"event_sources.*.rate":           "messages a second an instance, 1 to `event_sources.max_rate` — absent means `10`",
	"event_sources.*.format":         "`bytes`|`json` — absent means `bytes`",
	"event_sources.*.max_bytes":      "the body bound, at most the deployment's — absent means the deployment's",
	"event_sources.*.reject_breaker": "distinct new messages rejected in a row that pause the source; 0 disables — absent means the deployment's",
	"event_sources.*.match":          "list of tests, all of which must hold — absent means every message",
	"event_sources.*.input":          "constants for the Orkflow's inputs, a YAML mapping converted by the strict mapping — absent means none",
	"event_sources.*.map":            "`input: {pointer|attribute|field: …}` — absent means none",
	"event_sources.*.artifacts":      "`input: body|metadata` — absent means none",
}

// settingWords are the document words of an enum field, keyed by the field's
// full name: the descriptor cannot say them.
var settingWords = map[protoreflect.FullName][]string{
	"orkstack.v1.TelemetryPolicy.task_logs": sortedTaskLogWords(),
}

// settingMinimums are the parsers' lower bounds, keyed by full name.
var settingMinimums = map[protoreflect.FullName]int{
	"orkstack.v1.Namespace.max_running":       0,
	"orkstack.v1.Namespace.fair_share_weight": 0,
	"orkstack.v1.Budget.cpu_seconds":          0,
	"orkstack.v1.Budget.gib_seconds":          0,
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
	case protoreflect.DoubleKind:
		out = map[string]any{"type": "number"}
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
				"description": "A Go duration such as 168h, which `orkctl automation apply` checks."}, nil
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

		"event_source.orkflow":        slug,
		"event_source.version":        str,
		"event_source.description":    {"type": "string", "maxLength": eventsources.MaxDescriptionBytes},
		"event_source.uses":           {"type": "string", "minLength": 1, "maxLength": eventsources.MaxUsesBytes},
		"event_source.with":           {"type": "object"},
		"event_source.secrets":        {"type": "array", "maxItems": eventsources.MaxSecrets, "items": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"name", "source"}, "properties": map[string]any{"name": str, "source": str}}},
		"event_source.identities":     {"type": "array", "maxItems": 0},
		"event_source.credentials":    {"type": "array", "maxItems": 0},
		"event_source.env":            {"type": "object", "additionalProperties": str},
		"event_source.pool":           slug,
		"event_source.instances":      {"type": "integer", "minimum": 1},
		"event_source.resources":      {"type": "object", "additionalProperties": false, "properties": map[string]any{"cpu": str, "memory": str}},
		"event_source.delivery":       {"enum": []any{"at-least-once", "at-most-once"}},
		"event_source.concurrency":    {"type": "integer", "minimum": 1, "maximum": eventsources.MaxConcurrency},
		"event_source.rate":           {"type": "integer", "minimum": 1},
		"event_source.format":         {"enum": []any{"bytes", "json"}},
		"event_source.max_bytes":      {"type": "integer", "minimum": 1},
		"event_source.reject_breaker": {"type": "integer", "minimum": 0},
		"event_source.match":          {"type": "array", "maxItems": eventsources.MaxMatchTests, "items": map[string]any{"type": "object"}},
		"event_source.input":          {"type": "object"},
		"event_source.map":            {"type": "object", "maxProperties": eventsources.MaxMapEntries, "additionalProperties": map[string]any{"type": "object"}},
		"event_source.artifacts":      {"type": "object", "additionalProperties": map[string]any{"enum": []any{"body", "metadata"}}},
	}
}

// dslSlugPattern is the slug rule every name in the document is held to.
var dslSlugPattern = dsl.SlugPattern()

// entryRequired are each entry's required keys.
var entryRequired = map[string][]string{
	"schedule":     {"orkflow"},
	"trigger":      {"orkflow"},
	"webhook":      {"url"},
	"event_source": {"orkflow", "uses"},
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
	for kind, keys := range map[string][]string{"schedule": scheduleKeys, "trigger": triggerKeys, "webhook": webhookKeys, "event_source": eventSourceKeys} {
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
		"apiVersion":    describe("apiVersion", map[string]any{"const": dsl.APIVersion}),
		"kind":          describe("kind", map[string]any{"const": dsl.KindAutomation}),
		"namespace":     describe("namespace", map[string]any{"type": "string", "pattern": dslSlugPattern, "maxLength": dsl.MaxSlug}),
		"settings":      describe("settings", map[string]any{"$ref": "#/definitions/automation.settings"}),
		"schedules":     describe("schedules", section("schedule")),
		"triggers":      describe("triggers", section("trigger")),
		"webhooks":      describe("webhooks", section("webhook")),
		"event_sources": describe("event_sources", section("event_source")),
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
		"description": "One namespace's settings, schedules, triggers, webhooks and event sources, declared by name (docs/AUTOMATION.md). " +
			"`orkctl automation apply --dry-run` plans it against what the namespace holds; a schema checks only its shape.",
		"type": "object", "additionalProperties": false, "properties": properties,
		"required":    []any{"apiVersion", "kind", "namespace"},
		"definitions": definitions,
	}, nil
}

// EventSourceSchema is the JSON Schema of a kind: EventSource document, the
// file orkctl event-source create|update|evaluate --file reads: the header,
// an optional namespace and name, and an event_sources entry's keys, each
// with the entry's own schema.
func EventSourceSchema() (map[string]any, error) {
	entries := entrySchemas()
	slugName := map[string]any{"type": "string", "pattern": dslSlugPattern, "maxLength": dsl.MaxSlug}
	properties := map[string]any{
		"apiVersion": map[string]any{"const": dsl.APIVersion, "description": "always `" + dsl.APIVersion + "`"},
		"kind":       map[string]any{"const": dsl.KindEventSource, "description": "always `" + dsl.KindEventSource + "`"},
		"namespace":  withDescription(slugName, "the source's namespace — absent means the command's -n"),
		"name":       withDescription(slugName, "the source's name, which a command naming one must agree with — absent means the command's"),
	}
	for _, key := range eventSourceKeys {
		schema, ok := entries["event_source."+key]
		if !ok {
			return nil, fmt.Errorf("event source schema: key %s has no schema", key)
		}
		properties[key] = describe("event_sources.*."+key, schema)
	}
	required := []any{"apiVersion", "kind"}
	for _, key := range entryRequired["event_source"] {
		required = append(required, key)
	}
	return map[string]any{
		"$schema":  dsl.SchemaDialect,
		"$comment": dsl.SchemaComment,
		"title":    "OrkStack event source (" + dsl.APIVersion + ")",
		"description": "One event source, for orkctl event-source create, update and evaluate --file (docs/EVENT_SOURCES.md). " +
			"Its keys are an automation document's event_sources entry's; a schema checks only its shape.",
		"type": "object", "additionalProperties": false, "properties": properties,
		"required":    required,
		"definitions": map[string]any{},
	}, nil
}

func withDescription(schema map[string]any, text string) map[string]any {
	out := make(map[string]any, len(schema)+1)
	for k, v := range schema {
		out[k] = v
	}
	out["description"] = text
	return out
}
