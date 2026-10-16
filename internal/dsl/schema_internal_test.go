package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The Orkflow schema's tables, held to the compiler and to docs/DSL.md
// (schema.go, "What generated means here").

// orkflowSchemaTree is OrkflowSchema(nil) as the file holds it.
func orkflowSchemaTree(t *testing.T) map[string]any {
	t.Helper()
	tree, err := OrkflowSchema(nil)
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

// deref follows a $ref, and a description's allOf wrapper around one.
func deref(tree, schema map[string]any) map[string]any {
	for {
		if ref, ok := schema["$ref"].(string); ok {
			schema = tree["definitions"].(map[string]any)[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
			continue
		}
		if all, ok := schema["allOf"].([]any); ok && len(all) == 1 && schema["type"] == nil {
			if inner, ok := all[0].(map[string]any); ok && inner["$ref"] != nil {
				schema = inner
				continue
			}
		}
		return schema
	}
}

// schemaAt resolves a path ("tasks[].retry.maximum_attempts", "pins.*").
func schemaAt(t *testing.T, tree map[string]any, path string) map[string]any {
	t.Helper()
	schema := tree
	rest := path
	for rest != "" {
		schema = deref(tree, schema)
		switch {
		case strings.HasPrefix(rest, "[]"):
			schema = schema["items"].(map[string]any)
			rest = strings.TrimPrefix(rest[2:], ".")
		case strings.HasPrefix(rest, "*"):
			schema = schema["additionalProperties"].(map[string]any)
			rest = strings.TrimPrefix(rest[1:], ".")
		default:
			end := strings.IndexAny(rest, ".[")
			name := rest
			if end >= 0 {
				name = rest[:end]
				rest = strings.TrimPrefix(rest[end:], ".")
			} else {
				rest = ""
			}
			properties, ok := schema["properties"].(map[string]any)
			if !ok || properties[name] == nil {
				t.Fatalf("%s: no property %s", path, name)
			}
			schema = properties[name].(map[string]any)
		}
	}
	return deref(tree, schema)
}

// TestOrkflowSchema_HasEveryDocumentKey: every yaml tag reachable from
// document is a schema property, every property a tag, and every annotation,
// data-position and description key names a reflected path.
func TestOrkflowSchema_HasEveryDocumentKey(t *testing.T) {
	b, _ := buildOrkflowSchema()
	if b.err != nil {
		t.Fatal(b.err)
	}
	tree := orkflowSchemaTree(t)
	for path := range b.paths {
		schemaAt(t, tree, path)
	}
	// Each document struct's tags are exactly its definition's properties.
	for typ, name := range documentStructs {
		var tags []string
		for i := 0; i < typ.NumField(); i++ {
			if tag := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]; tag != "" && tag != "-" {
				tags = append(tags, tag)
			}
		}
		var props []string
		for key := range tree["definitions"].(map[string]any)[name].(map[string]any)["properties"].(map[string]any) {
			props = append(props, key)
		}
		sort.Strings(tags)
		sort.Strings(props)
		if !reflect.DeepEqual(tags, props) {
			t.Errorf("%s: tags %v, properties %v", name, tags, props)
		}
	}
	for path := range orkflowAnnotations() {
		if !b.paths[path] {
			t.Errorf("annotation %s names no document key", path)
		}
	}
	for path := range orkflowDataPositions() {
		if !b.paths[path] {
			t.Errorf("data position %s names no document key", path)
		}
	}
	for path := range orkflowDescriptions {
		if !b.paths[path] {
			t.Errorf("description %s names no document key", path)
		}
	}
}

// slot is one key's context: a document with @V@ where the key's value goes,
// the field path an error on it names, the value that compiles, and the
// document without the key.
type slot struct {
	doc, field, baseline, absent string
}

const slotHeader = "apiVersion: orkstack/v1\nkind: Orkflow\n"

// task is a minimal plain task followed by a second task b.
func taskDoc(lines string) string {
	return slotHeader + "name: x\ntasks:\n  - id: a\n    uses: busybox:1\n" + lines + "  - id: b\n    uses: busybox:1\n"
}

func taskDocWith(root, lines string) string {
	return slotHeader + "name: x\n" + root + "tasks:\n  - id: a\n    uses: busybox:1\n" + lines + "  - id: b\n    uses: busybox:1\n"
}

func awaitDoc(await string) string {
	return slotHeader + "name: x\ntasks:\n  - id: a\n    await: " + await + "\n"
}

// slots are the contexts of every key the type test covers.
func slots() map[string]slot {
	rootDoc := func(key, rest string) slot {
		lines := map[string]string{"apiVersion": "apiVersion: orkstack/v1", "kind": "kind: Orkflow", "name": "name: x",
			"tasks": "tasks:\n  - id: a\n    uses: busybox:1"}
		order := []string{"apiVersion", "kind", "name", "tasks"}
		build := func(value string, drop bool) string {
			var out []string
			for _, k := range order {
				if k == key {
					if !drop {
						out = append(out, key+": "+value)
					}
					continue
				}
				out = append(out, lines[k])
			}
			if !slices.Contains(order, key) && !drop {
				out = append(out, key+": "+value)
			}
			return strings.Join(out, "\n") + "\n" + rest
		}
		absent := build("", true)
		if key == "pins" {
			// A declared pin must be referenced.
			lines["tasks"] += "\n    pin: p"
		}
		return slot{doc: build("@V@", false), field: key, absent: absent}
	}
	out := map[string]slot{}
	for key, baseline := range map[string]string{
		"apiVersion": "orkstack/v1", "kind": "Orkflow", "name": "x", "namespace": "ns", "version": `"1"`,
		"description": "d", "labels": "{l: v}", "inputs": "{i: {type: string}}", "outputs": "{r: x}",
		"pins": "{p: {}}", "tasks": "[{id: a, uses: 'busybox:1'}]",
	} {
		s := rootDoc(key, "")
		s.baseline = baseline
		out[key] = s
	}
	out["labels.*"] = slot{doc: slotHeader + "name: x\nlabels: {l: @V@}\ntasks: [{id: a, uses: 'busybox:1'}]\n", field: "labels.l", baseline: "v"}
	out["outputs.*"] = slot{doc: slotHeader + "name: x\noutputs: {r: @V@}\ntasks: [{id: a, uses: 'busybox:1'}]\n", field: "outputs.r", baseline: "x"}
	out["pins.*"] = slot{doc: slotHeader + "name: x\npins: {p: @V@}\ntasks: [{id: a, uses: 'busybox:1', pin: p}]\n", field: "pins.p", baseline: "{}"}
	out["pins.*.timeout_seconds"] = slot{doc: slotHeader + "name: x\npins: {p: {timeout_seconds: @V@}}\ntasks: [{id: a, uses: 'busybox:1', pin: p}]\n",
		field: "pins.p.timeout_seconds", baseline: "60", absent: slotHeader + "name: x\npins: {p: {}}\ntasks: [{id: a, uses: 'busybox:1', pin: p}]\n"}
	out["tasks[]"] = slot{doc: slotHeader + "name: x\ntasks: [@V@]\n", field: "tasks[0]", baseline: "{id: a, uses: 'busybox:1'}"}
	out["inputs.*"] = slot{doc: slotHeader + "name: x\ninputs: {i: @V@}\ntasks: [{id: a, uses: 'busybox:1'}]\n", field: "inputs.i", baseline: "{type: string}"}
	for key, baseline := range map[string]string{"type": "string", "required": "true", "default": "x"} {
		out["inputs.*."+key] = slot{doc: slotHeader + "name: x\ninputs: {i: {" + key + ": @V@}}\ntasks: [{id: a, uses: 'busybox:1'}]\n",
			field: "inputs.i." + key, baseline: baseline, absent: slotHeader + "name: x\ninputs: {i: {}}\ntasks: [{id: a, uses: 'busybox:1'}]\n"}
	}

	plainTask := map[string]string{
		"needs": "[b]", "condition": "'{{ true }}'", "timeout_seconds": "10", "retry": "{maximum_attempts: 2}",
		"pool": "p", "command": "[x]", "env": "{A: b}", "secrets": "[{name: s, source: 'env:S'}]",
		"resources": "{cpu: '1'}", "network": "none", "cancellation_grace_period_seconds": "5", "run_as_orker_user": "true",
		"cache": "{enabled: true}", "with": "{}", "description_unused": "",
	}
	delete(plainTask, "description_unused")
	for key, baseline := range plainTask {
		out["tasks[]."+key] = slot{doc: taskDoc("    " + key + ": @V@\n"), field: "tasks[0]." + key, baseline: baseline, absent: taskDoc("")}
	}
	out["tasks[].id"] = slot{doc: slotHeader + "name: x\ntasks:\n  - id: @V@\n    uses: busybox:1\n", field: "tasks[0].id", baseline: "a",
		absent: slotHeader + "name: x\ntasks:\n  - uses: busybox:1\n"}
	out["tasks[].uses"] = slot{doc: slotHeader + "name: x\ntasks:\n  - id: a\n    uses: @V@\n", field: "tasks[0].uses", baseline: "'busybox:1'",
		absent: slotHeader + "name: x\ntasks:\n  - id: a\n"}
	out["tasks[].join"] = slot{doc: taskDoc("    needs: [b]\n    join: @V@\n"), field: "tasks[0].join", baseline: "all_done", absent: taskDoc("    needs: [b]\n")}
	out["tasks[].pin"] = slot{doc: taskDocWith("pins: {p: {}}\n", "    pin: @V@\n"), field: "tasks[0].pin", baseline: "p", absent: taskDoc("")}
	out["tasks[].each"] = slot{doc: taskDocWith("inputs: {l: {type: array}}\n", "    each: @V@\n"), field: "tasks[0].each",
		baseline: "{over: '{{ .inputs.l }}', max_items: 2}", absent: taskDocWith("inputs: {l: {type: array}}\n", "")}
	out["tasks[].await"] = slot{doc: awaitDoc("@V@"), field: "tasks[0].await", baseline: "{timeout_seconds: 60}"}
	out["tasks[].needs[]"] = slot{doc: taskDoc("    needs: [@V@]\n"), field: "tasks[0].needs[0]", baseline: "b"}
	out["tasks[].command[]"] = slot{doc: taskDoc("    command: [@V@]\n"), field: "tasks[0].command[0]", baseline: "x"}
	out["tasks[].env.*"] = slot{doc: taskDoc("    env: {A: @V@}\n"), field: "tasks[0].env.A", baseline: "b"}
	out["tasks[].secrets[]"] = slot{doc: taskDoc("    secrets: [@V@]\n"), field: "tasks[0].secrets[0]", baseline: "{name: s, source: 'env:S'}"}
	for key, baseline := range map[string]string{"name": "s", "source": "'env:S'"} {
		other := map[string]string{"name": "source: 'env:S'", "source": "name: s"}[key]
		out["tasks[].secrets[]."+key] = slot{doc: taskDoc("    secrets: [{" + other + ", " + key + ": @V@}]\n"), field: "tasks[0].secrets[0]." + key,
			baseline: baseline, absent: taskDoc("    secrets: [{" + other + "}]\n")}
	}
	for key, baseline := range map[string]string{"maximum_attempts": "2", "initial_interval_seconds": "1", "backoff_coefficient": "2.0", "maximum_interval_seconds": "10"} {
		out["tasks[].retry."+key] = slot{doc: taskDoc("    retry: {" + key + ": @V@}\n"), field: "tasks[0].retry." + key, baseline: baseline, absent: taskDoc("    retry: {}\n")}
	}
	for key, baseline := range map[string]string{"cpu": "'1'", "memory": "1Gi"} {
		out["tasks[].resources."+key] = slot{doc: taskDoc("    resources: {" + key + ": @V@}\n"), field: "tasks[0].resources." + key, baseline: baseline, absent: taskDoc("    resources: {}\n")}
	}
	for key, baseline := range map[string]string{"timeout_seconds": "60", "callback": "true", "schema": "{f: {type: string}}"} {
		base := map[string]string{"timeout_seconds": "", "callback": "timeout_seconds: 60, ", "schema": "timeout_seconds: 60, "}[key]
		out["tasks[].await."+key] = slot{doc: awaitDoc("{" + base + key + ": @V@}"), field: "tasks[0].await." + key, baseline: baseline,
			absent: awaitDoc("{" + strings.TrimSuffix(base, ", ") + "}")}
	}
	out["tasks[].await.schema.*"] = slot{doc: awaitDoc("{timeout_seconds: 60, schema: {f: @V@}}"), field: "tasks[0].await.schema.f", baseline: "{type: string}"}
	for key, baseline := range map[string]string{"type": "string", "required": "true"} {
		base := map[string]string{"type": "", "required": "type: string, "}[key]
		out["tasks[].await.schema.*."+key] = slot{doc: awaitDoc("{timeout_seconds: 60, schema: {f: {" + base + key + ": @V@}}}"),
			field: "tasks[0].await.schema.f." + key, baseline: baseline,
			absent: awaitDoc("{timeout_seconds: 60, schema: {f: {" + strings.TrimSuffix(base, ", ") + "}}}")}
	}
	eachBase := map[string]string{"over": "max_items: 2", "max_items": "over: '{{ .inputs.l }}'", "as": "over: '{{ .inputs.l }}', max_items: 2", "max_parallel": "over: '{{ .inputs.l }}', max_items: 2"}
	for key, baseline := range map[string]string{"over": "'{{ .inputs.l }}'", "max_items": "2", "as": "part", "max_parallel": "1"} {
		out["tasks[].each."+key] = slot{doc: taskDocWith("inputs: {l: {type: array}}\n", "    each: {"+eachBase[key]+", "+key+": @V@}\n"),
			field: "tasks[0].each." + key, baseline: baseline, absent: taskDocWith("inputs: {l: {type: array}}\n", "    each: {"+eachBase[key]+"}\n")}
	}
	cacheBase := map[string]string{"enabled": "", "ttl_seconds": "enabled: true, ", "scope": "enabled: true, "}
	for key, baseline := range map[string]string{"enabled": "true", "ttl_seconds": "60", "scope": "namespace"} {
		out["tasks[].cache."+key] = slot{doc: taskDoc("    cache: {" + cacheBase[key] + key + ": @V@}\n"), field: "tasks[0].cache." + key,
			baseline: baseline, absent: taskDoc("    cache: {" + strings.TrimSuffix(cacheBase[key], ", ") + "}\n")}
	}
	return out
}

// probeValue is one value of the type test, as a YAML 1.2 reader reads it.
type probeValue struct {
	text string
	json any
}

var probeValues = []probeValue{
	{`"x"`, "x"}, {"1", 1.0}, {"true", true}, {"[x]", []any{"x"}}, {"{a: b}", map[string]any{"a": "b"}},
	{"~", nil}, {"1.5", 1.5}, {"yes", "yes"},
}

// admits applies a schema's type, enum and const to a value.
func admits(schema map[string]any, value any) bool {
	if c, ok := schema["const"]; ok {
		return reflect.DeepEqual(c, value)
	}
	if enum, ok := schema["enum"].([]any); ok {
		return slices.ContainsFunc(enum, func(e any) bool { return reflect.DeepEqual(e, value) })
	}
	typ, ok := schema["type"]
	if !ok {
		return true
	}
	var types []string
	switch tv := typ.(type) {
	case string:
		types = []string{tv}
	case []any:
		for _, w := range tv {
			types = append(types, w.(string))
		}
	}
	var kind string
	switch v := value.(type) {
	case nil:
		kind = "null"
	case bool:
		kind = "boolean"
	case string:
		kind = "string"
	case []any:
		kind = "array"
	case map[string]any:
		kind = "object"
	case float64:
		if v == float64(int64(v)) && slices.Contains(types, "integer") {
			return true
		}
		kind = "number"
	}
	return slices.Contains(types, kind)
}

// deliberateClass names the class of docs/DSL.md "Editor support" that
// excuses a value the schema refuses and the compiler accepts, or "".
func deliberateClass(path string, schema map[string]any, value probeValue) string {
	switch {
	case strings.HasPrefix(path, "inputs.*"):
		return "D"
	case value.json == nil:
		return "N"
	case path == "version" && (value.text == "1" || value.text == "true" || value.text == "1.5"):
		return "V"
	case schema["type"] == "integer" && value.text == "1.5":
		return "I"
	case schema["type"] == "boolean" && value.text == "yes":
		return "B"
	}
	return ""
}

// refusedOn reports whether err is about the slot's key: a field at or above
// its path, or a decode error naming the value's line.
func refusedOn(err error, field string, line int) bool {
	var verr *ValidationError
	if errors.As(err, &verr) && verr.Field != "" &&
		(verr.Field == field || strings.HasPrefix(field, verr.Field+".") || strings.HasPrefix(field, verr.Field+"[")) {
		return true
	}
	return strings.Contains(err.Error(), fmt.Sprintf("line %d:", line))
}

// TestOrkflowSchema_TypesAgreeWithTheCompiler: the schema never refuses a
// document the compiler accepts, except in the deliberate classes V, N, I, B
// and D — and each class excuses at least one pair the compiler accepted, so
// a class goes stale when the compiler tightens.
func TestOrkflowSchema_TypesAgreeWithTheCompiler(t *testing.T) {
	b, _ := buildOrkflowSchema()
	tree := orkflowSchemaTree(t)
	contexts := slots()
	excused := map[string]int{}
	for path := range b.paths {
		ctx, ok := contexts[path]
		if !ok {
			t.Errorf("%s: no context for the type test", path)
			continue
		}
		baseline := strings.ReplaceAll(ctx.doc, "@V@", ctx.baseline)
		if _, err := Compile([]byte(baseline)); err != nil {
			t.Errorf("%s: the baseline %q does not compile: %v", path, ctx.baseline, err)
			continue
		}
		line := 0
		for i, l := range strings.Split(ctx.doc, "\n") {
			if strings.Contains(l, "@V@") {
				line = i + 1
			}
		}
		schema := schemaAt(t, tree, path)
		excludesOne := false
		for _, value := range probeValues {
			if admits(schema, value.json) {
				continue
			}
			excludesOne = true
			_, err := Compile([]byte(strings.ReplaceAll(ctx.doc, "@V@", value.text)))
			if err != nil {
				// A null reads as the key unwritten (class N), so a refusal of
				// the missing key is the compiler agreeing.
				if !refusedOn(err, ctx.field, line) && deliberateClass(path, schema, value) != "N" {
					t.Errorf("%s = %s: the schema refuses it and the compiler refuses something else: %v", path, value.text, err)
				}
				continue
			}
			class := deliberateClass(path, schema, value)
			if class == "" {
				t.Errorf("%s = %s: the schema refuses it, the compiler accepts it, and no deliberate class excuses it", path, value.text)
				continue
			}
			excused[class]++
		}
		if !excludesOne && path != "inputs.*.default" {
			t.Errorf("%s: the schema excludes none of the probe values; the key is untyped", path)
		}
	}
	for _, class := range []string{"V", "N", "I", "B", "D"} {
		if excused[class] == 0 {
			t.Errorf("class %s excuses nothing the compiler accepts: the compiler tightened, so delete the class", class)
		}
	}
	t.Logf("deliberately stricter pairs by class: %v", excused)
}

// TestOrkflowSchema_RequiredKeysAreRequiredByTheCompiler: a required key's
// absence fails Compile; a key not required compiles absent. uses is
// required by the task kind (the else branch), not by an annotation.
func TestOrkflowSchema_RequiredKeysAreRequiredByTheCompiler(t *testing.T) {
	annotations := orkflowAnnotations()
	tree := orkflowSchemaTree(t)
	for path, ctx := range slots() {
		if ctx.absent == "" {
			continue
		}
		required := requiredInSchema(t, tree, path) || path == "tasks[].uses"
		_, err := Compile([]byte(ctx.absent))
		switch {
		case required && err == nil:
			t.Errorf("%s is required by the schema, and a document without it compiles", path)
		case !required && err != nil:
			t.Errorf("%s is optional in the schema, and a document without it fails: %v", path, err)
		}
	}
	for _, path := range []string{"apiVersion", "kind", "name", "tasks"} {
		if !annotations[path].required {
			t.Errorf("%s is not required", path)
		}
	}
	task := tree["definitions"].(map[string]any)["orkflow.task"].(map[string]any)
	if got := task["else"].(map[string]any)["required"]; !reflect.DeepEqual(got, []any{"uses"}) {
		t.Errorf("a task that is not awaited requires %v, want uses", got)
	}
}

// requiredInSchema reports whether the schema requires path's last key.
func requiredInSchema(t *testing.T, tree map[string]any, path string) bool {
	t.Helper()
	parent, key := "", path
	if i := strings.LastIndex(path, "."); i >= 0 {
		parent, key = path[:i], path[i+1:]
	}
	schema := tree
	if parent != "" {
		schema = schemaAt(t, tree, parent)
	}
	list, _ := deref(tree, schema)["required"].([]any)
	return slices.Contains(list, any(key))
}

func enumAt(t *testing.T, tree map[string]any, path string) []string {
	t.Helper()
	var out []string
	for _, word := range schemaAt(t, tree, path)["enum"].([]any) {
		out = append(out, word.(string))
	}
	return out
}

// TestOrkflowSchema_WordsAreTheCompilersWords: every enum is the variable the
// compiler checks against, each word compiles and a word outside fails —
// except an inputs: type, which the compiler compares as written (class D).
func TestOrkflowSchema_WordsAreTheCompilersWords(t *testing.T) {
	tree := orkflowSchemaTree(t)
	contexts := slots()
	for path, want := range map[string][]string{
		"tasks[].join":                joinWordsSorted(),
		"tasks[].network":             networkWords,
		"tasks[].cache.scope":         cacheScopeWords,
		"tasks[].await.schema.*.type": typeWords,
		"inputs.*.type":               typeWords,
	} {
		if got := enumAt(t, tree, path); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: enum %v, want %v", path, got, want)
		}
		ctx := contexts[path]
		for _, word := range want {
			if _, err := Compile([]byte(strings.ReplaceAll(ctx.doc, "@V@", word))); err != nil {
				t.Errorf("%s = %s does not compile: %v", path, word, err)
			}
		}
		_, err := Compile([]byte(strings.ReplaceAll(ctx.doc, "@V@", "nonword")))
		if path == "inputs.*.type" {
			if err != nil {
				t.Errorf("inputs.*.type = nonword fails: the compiler tightened; remove class D (docs/DSL.md, \"Editor support\"): %v", err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s = nonword compiles", path)
		}
	}
}

func TestOrkflowSchema_TaskKindsComeFromTheRefusalTables(t *testing.T) {
	tree := orkflowSchemaTree(t)
	task := tree["definitions"].(map[string]any)["orkflow.task"].(map[string]any)
	keys := func(clause map[string]any) []string {
		var out []string
		for key, v := range clause["properties"].(map[string]any) {
			if v != false {
				t.Errorf("%s is not forbidden", key)
			}
			out = append(out, key)
		}
		sort.Strings(out)
		return out
	}
	sorted := func(in []string) []string { out := append([]string(nil), in...); sort.Strings(out); return out }
	if got := keys(task["then"].(map[string]any)); !reflect.DeepEqual(got, sorted(awaitForbiddenKeys())) {
		t.Errorf("an awaited task forbids %v, want %v", got, sorted(awaitForbiddenKeys()))
	}
	composed := task["allOf"].([]any)[0].(map[string]any)
	if got := keys(composed["then"].(map[string]any)); !reflect.DeepEqual(got, sorted(composeForbiddenKeys())) {
		t.Errorf("a composed task forbids %v, want %v", got, sorted(composeForbiddenKeys()))
	}
}

// normalizedDoc is text with # removed and whitespace collapsed, so a
// multi-line YAML comment matches as one sentence.
func normalizedDoc(text string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(text, "#", " ")), " ")
}

func TestOrkflowSchema_DescriptionsAreDSLText(t *testing.T) {
	raw, err := os.ReadFile("../../docs/DSL.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := normalizedDoc(string(raw))
	for path, text := range orkflowDescriptions {
		if !strings.Contains(doc, normalizedDoc(text)) {
			t.Errorf("%s: %q is not docs/DSL.md text", path, text)
		}
	}
	b, _ := buildOrkflowSchema()
	for path := range b.paths {
		if strings.Count(path, ".") == 0 && !strings.Contains(path, "[") && orkflowDescriptions[path] == "" {
			t.Errorf("top-level key %s has no description", path)
		}
		if strings.HasPrefix(path, "tasks[].") && strings.Count(path, ".") == 1 && !strings.Contains(strings.TrimPrefix(path, "tasks[]"), "[") &&
			orkflowDescriptions[path] == "" {
			t.Errorf("task key %s has no description", path)
		}
	}
}
