package automation

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The automation schema, held to this parser and to docs/AUTOMATION.md.

func automationSchemaTree(t *testing.T) map[string]any {
	t.Helper()
	tree, err := Schema()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func definition(tree map[string]any, name string) map[string]any {
	return tree["definitions"].(map[string]any)[name].(map[string]any)
}

func propertiesOf(schema map[string]any) map[string]any {
	return schema["properties"].(map[string]any)
}

func schemaKeysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// schemaKeys is every key path the schema declares: "settings.budget",
// "settings.budget.cpu_seconds", "schedules.*.cron", ….
func schemaKeys(tree map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for key, schema := range propertiesOf(tree) {
		out[key] = schema.(map[string]any)
	}
	// settings is a reference to its definition.
	out["settings"] = definition(tree, "automation.settings")
	for key, schema := range propertiesOf(definition(tree, "automation.settings")) {
		s := schema.(map[string]any)
		out["settings."+key] = s
		if props, ok := s["properties"].(map[string]any); ok {
			for sub, subSchema := range props {
				out["settings."+key+"."+sub] = subSchema.(map[string]any)
			}
		}
	}
	for _, kind := range []string{"schedule", "trigger", "webhook"} {
		for key, schema := range propertiesOf(definition(tree, "automation."+kind)) {
			out[kind+"s.*."+key] = schema.(map[string]any)
		}
	}
	return out
}

func TestAutomationSchema_HasEveryKey(t *testing.T) {
	tree := automationSchemaTree(t)
	if got := schemaKeysOf(propertiesOf(tree)); !reflect.DeepEqual(got, sortedCopy(topLevelKeys)) {
		t.Errorf("top-level keys %v, want %v", got, sortedCopy(topLevelKeys))
	}
	var rows []string
	for _, row := range namespaceSettings {
		rows = append(rows, row.key)
	}
	if got := schemaKeysOf(propertiesOf(definition(tree, "automation.settings"))); !reflect.DeepEqual(got, sortedCopy(rows)) {
		t.Errorf("settings keys %v, want the rows %v", got, sortedCopy(rows))
	}
	for kind, keys := range map[string][]string{"schedule": scheduleKeys, "trigger": triggerKeys, "webhook": webhookKeys} {
		if got := schemaKeysOf(propertiesOf(definition(tree, "automation."+kind))); !reflect.DeepEqual(got, sortedCopy(keys)) {
			t.Errorf("%s keys %v, want %v", kind, got, sortedCopy(keys))
		}
	}
	keys := schemaKeys(tree)
	for path := range automationDescriptions {
		if _, ok := keys[path]; !ok {
			t.Errorf("description %s names no key", path)
		}
	}
}

func enumOf(schema map[string]any) []string {
	var out []string
	for _, word := range schema["enum"].([]any) {
		out = append(out, word.(string))
	}
	return out
}

func TestAutomationSchema_WordsAreTheParsersWords(t *testing.T) {
	keys := schemaKeys(automationSchemaTree(t))
	if got := enumOf(keys["schedules.*.overlap"]); !reflect.DeepEqual(got, overlapSlugs()) {
		t.Errorf("overlap %v, want %v", got, overlapSlugs())
	}
	var events []string
	for _, event := range webhooks.KnownEvents() {
		events = append(events, string(event))
	}
	if got := enumOf(keys["webhooks.*.events"]["items"].(map[string]any)); !reflect.DeepEqual(got, events) {
		t.Errorf("events %v, want %v", got, events)
	}
	var words []string
	for word := range taskLogWords {
		words = append(words, word)
	}
	if got := enumOf(keys["settings.telemetry.task_logs"]); !reflect.DeepEqual(got, sortedCopy(words)) {
		t.Errorf("task_logs %v, want %v", got, sortedCopy(words))
	}
	for _, word := range words {
		mustParse(t, header+"settings:\n  telemetry:\n    task_logs: "+word+"\n")
	}
}

// reachableFields is every field a settings row reaches, by full name.
func reachableFields() map[protoreflect.FullName]protoreflect.FieldDescriptor {
	out := map[protoreflect.FullName]protoreflect.FieldDescriptor{}
	var walk func(fd protoreflect.FieldDescriptor)
	walk = func(fd protoreflect.FieldDescriptor) {
		out[fd.FullName()] = fd
		if fd.Kind() == protoreflect.MessageKind && !fd.IsMap() && fd.Message().FullName() != "google.protobuf.Duration" {
			for i := 0; i < fd.Message().Fields().Len(); i++ {
				walk(fd.Message().Fields().Get(i))
			}
		}
	}
	for _, row := range namespaceSettings {
		walk(namespaceField(row.field))
	}
	return out
}

func withoutDescription(schema map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range schema {
		if k != "description" {
			out[k] = v
		}
	}
	return out
}

func TestAutomationSchema_SettingsComeFromTheNamespaceDescriptor(t *testing.T) {
	tree := automationSchemaTree(t)
	settings := propertiesOf(definition(tree, "automation.settings"))
	for _, row := range namespaceSettings {
		want, err := settingSchema(namespaceField(row.field))
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(want)
		var decoded map[string]any
		_ = json.Unmarshal(encoded, &decoded)
		if got := withoutDescription(settings[row.key].(map[string]any)); !reflect.DeepEqual(got, withoutDescription(decoded)) {
			t.Errorf("settings.%s = %v, want settingSchema of %s: %v", row.key, got, row.field, decoded)
		}
	}
	reachable := reachableFields()
	for message, subKeys := range settingSubKeys {
		var names []string
		for name, fd := range reachable {
			if fd.Parent().FullName() == message {
				names = append(names, string(fd.Name()))
				_ = name
			}
		}
		if !reflect.DeepEqual(sortedCopy(names), sortedCopy(subKeys)) {
			t.Errorf("%s fields %v, the parser's sub-keys %v", message, sortedCopy(names), sortedCopy(subKeys))
		}
	}
	for name, fd := range reachable {
		if fd.Kind() == protoreflect.EnumKind {
			if _, ok := settingWords[name]; !ok {
				t.Errorf("enum field %s has no document words", name)
			}
		}
	}
	for name := range settingWords {
		if _, ok := reachable[name]; !ok {
			t.Errorf("settingWords names %s, which no row reaches", name)
		}
	}
	for name := range settingMinimums {
		if _, ok := reachable[name]; !ok {
			t.Errorf("settingMinimums names %s, which no row reaches", name)
		}
	}
	// The descriptor is orkstack.v1.Namespace's.
	if namespaceField("max_running").Parent().FullName() != (&orkstackv1.Namespace{}).ProtoReflect().Descriptor().FullName() {
		t.Error("settings rows are not read from orkstack.v1.Namespace")
	}
}

// jsonTypes is what a schema admits by type: its type, its enum or const
// members' types, or its anyOf branches' — nil when it says nothing.
func jsonTypes(schema map[string]any) []string {
	typeOf := func(v any) string {
		switch v.(type) {
		case string:
			return "string"
		case bool:
			return "boolean"
		case float64:
			return "number"
		}
		return "object"
	}
	switch {
	case schema["type"] != nil:
		switch typ := schema["type"].(type) {
		case string:
			return []string{typ}
		case []any:
			var out []string
			for _, w := range typ {
				out = append(out, w.(string))
			}
			return out
		}
	case schema["enum"] != nil:
		var out []string
		for _, v := range schema["enum"].([]any) {
			out = append(out, typeOf(v))
		}
		return out
	case schema["const"] != nil:
		return []string{typeOf(schema["const"])}
	case schema["anyOf"] != nil:
		var out []string
		for _, branch := range schema["anyOf"].([]any) {
			out = append(out, jsonTypes(branch.(map[string]any))...)
		}
		return out
	}
	return nil
}

// TestAutomationSchema_TypesAgreeWithTheParser: the schema never shows an
// error the parser would not give. The parser walks nodes and refuses a
// number or a boolean where it reads a string, so its strings stay strings;
// the one exception is a version, which versionScalar reads as a number too
// and the schema types as a string, steering toward a quoted version.
func TestAutomationSchema_TypesAgreeWithTheParser(t *testing.T) {
	keys := schemaKeys(automationSchemaTree(t))
	values := []struct {
		text, typ string
	}{{`"x"`, "string"}, {"1", "integer"}, {"true", "boolean"}, {"[x]", "array"}, {"{a: b}", "object"}, {"~", "null"}}
	excepted := map[string]bool{"schedules.*.version": true, "triggers.*.version": true}
	used := map[string]bool{}
	for path, schema := range keys {
		var doc string
		switch {
		case path == "apiVersion" || path == "kind" || path == "namespace":
			continue // the header; TestParse_HeaderIsRequiredAndExact
		case path == "settings" || path == "schedules" || path == "triggers" || path == "webhooks":
			doc = header + path + ": @V@\n"
		case strings.HasPrefix(path, "settings."):
			parts := strings.Split(strings.TrimPrefix(path, "settings."), ".")
			if len(parts) == 1 {
				doc = header + "settings:\n  " + parts[0] + ": @V@\n"
			} else {
				doc = header + "settings:\n  " + parts[0] + ":\n    " + parts[1] + ": @V@\n"
			}
		default:
			kind := strings.Split(path, ".")[0]
			key := strings.Split(path, ".")[2]
			base := map[string]map[string]string{
				"schedules": {"orkflow": "x", "cron": "'@daily'"},
				"triggers":  {"orkflow": "x"},
				"webhooks":  {"url": "https://hooks.example.test/x"},
			}[kind]
			lines := []string{kind + ":", "  e:"}
			for _, k := range sortedCopy(mapKeysOf(base)) {
				if k != key {
					lines = append(lines, "    "+k+": "+base[k])
				}
			}
			lines = append(lines, "    "+key+": @V@")
			doc = header + strings.Join(lines, "\n") + "\n"
		}
		admitted := jsonTypes(schema)
		if admitted == nil {
			t.Errorf("%s: the schema types nothing", path)
			continue
		}
		excludesOne := false
		field := strings.ReplaceAll(path, "*", "e")
		for _, value := range values {
			if slices.Contains(admitted, value.typ) || (value.typ == "integer" && slices.Contains(admitted, "number")) {
				continue
			}
			excludesOne = true
			_, issues := Parse([]byte(strings.ReplaceAll(doc, "@V@", value.text)))
			if errorOn(issues, field) {
				continue
			}
			if excepted[path] && value.typ == "integer" {
				used[path] = true
				continue
			}
			t.Errorf("%s = %s: the schema refuses it and the parser reports no error on it: %v", path, value.text, issues)
		}
		if !excludesOne {
			t.Errorf("%s: the schema excludes none of the probe values; the key is untyped", path)
		}
	}
	for path := range excepted {
		if !used[path] {
			t.Errorf("%s: the version exception excuses nothing; the parser tightened, so remove it", path)
		}
	}
}

func mapKeysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// errorOn reports an ERROR on field, on one of its ancestors, or under it.
func errorOn(issues []*orkstackv1.ValidationIssue, field string) bool {
	for _, issue := range issues {
		if issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			continue
		}
		f := issue.GetField()
		if f == field || strings.HasPrefix(field, f+".") || strings.HasPrefix(f, field+".") || strings.HasPrefix(f, field+"[") {
			return true
		}
	}
	return false
}

func normalizedDoc(text string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(text, "#", " ")), " ")
}

func TestAutomationSchema_DescriptionsAreAutomationText(t *testing.T) {
	raw, err := os.ReadFile("../../docs/AUTOMATION.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := normalizedDoc(strings.ReplaceAll(string(raw), `\|`, "|"))
	for path, text := range automationDescriptions {
		for _, part := range strings.Split(text, " — absent means ") {
			if !strings.Contains(doc, normalizedDoc(part)) {
				t.Errorf("%s: %q is not docs/AUTOMATION.md text", path, part)
			}
		}
	}
}
