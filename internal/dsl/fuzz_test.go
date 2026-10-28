package dsl_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// Reference grammar, from the OCI distribution specification's reference
// rules. The oracle is written from that grammar, not from ParseReference.
var (
	domainComponent = `(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])`
	registryHost    = regexp.MustCompile(`^(?:` + domainComponent + `(?:\.` + domainComponent + `)*|\[[0-9a-fA-F:.]+\])(?::[0-9]{1,5})?$`)
	pathComponent   = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
	tagPattern      = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`)
)

// FuzzParseReference: an image reference an Orkflow names reaches the registry
// as a URL and the container runtime as an argument, so what ParseReference
// accepts must be exactly a registry host, lower-case path components, a tag
// and a digest in the distribution grammar, and the pinned form it builds
// from them can never begin like a runtime option or hold a space.
func FuzzParseReference(f *testing.F) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	for _, seed := range []string{
		"alpine", "alpine:3.20", "library/alpine", "docker.io/library/alpine:3.20", "ghcr.io/acme/migrate@" + digest,
		"localhost:5001/orkstack/shell:test-abc", "registry.example:5000/a/b/c:v1@" + digest, "[::1]:5001/x:1", "  alpine  ", "",
		"alpine:", "alpine@sha256:abc", "alpine@" + digest + "@" + digest, "a/b:c:d", "Alpine", "ALPINE:3", "a//b", "/a", "a/", "a b",
		"-x.y/repo", "--entrypoint=a.b/c", "--cap-add=ALL.example/x:1", "-:1/x", "a.b/-c", "a.b/c..d", "a.b/c__d", "a.b/c___d",
		"host.example/path?query=1", "host.example/a#frag", "host.example/a%2f..%2fb", "host.example:99999999/x", "host.example:/x",
		"a.b/c:" + strings.Repeat("t", 128), "a.b/c:" + strings.Repeat("t", 129), "a.b/c:-x", "a.b/c:.x", "a\nb", "a.b/c\x00",
		"\u00e9.example/x", "x.example/\u00e9", "[::1/x", "[]:5001/x", "1.2.3.4:5000/x", "a.b./x", ".a.b/x", "a_b.example/x",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, reference string) {
		parsed, err := dsl.ParseReference(reference)
		if err != nil {
			if err.Error() == "" {
				t.Fatalf("ParseReference(%q) refused without a reason", reference)
			}
			return
		}
		if parsed.Original != reference {
			t.Fatalf("ParseReference(%q).Original = %q", reference, parsed.Original)
		}
		if !registryHost.MatchString(parsed.Registry) {
			t.Fatalf("ParseReference(%q) accepted the registry %q, which is not a host[:port]", reference, parsed.Registry)
		}
		if parsed.Repository == "" {
			t.Fatalf("ParseReference(%q) accepted no repository", reference)
		}
		for _, component := range strings.Split(parsed.Repository, "/") {
			if !pathComponent.MatchString(component) {
				t.Fatalf("ParseReference(%q) accepted the repository %q, whose component %q is not in the grammar", reference, parsed.Repository, component)
			}
		}
		if parsed.Tag != "" && !tagPattern.MatchString(parsed.Tag) {
			t.Fatalf("ParseReference(%q) accepted the tag %q", reference, parsed.Tag)
		}
		if parsed.Digest != "" && !dsl.ValidDigest(parsed.Digest) {
			t.Fatalf("ParseReference(%q) accepted the digest %q", reference, parsed.Digest)
		}
		if parsed.Tag == "" && parsed.Digest == "" {
			t.Fatalf("ParseReference(%q) names neither a tag nor a digest", reference)
		}

		// The pin: registry/repository@digest, one token that starts like a
		// name and holds nothing a shell, a URL or an option parser would split.
		pinned := parsed.Pinned("sha256:" + strings.Repeat("0", 64))
		first := pinned[0]
		if !(first >= 'a' && first <= 'z' || first >= 'A' && first <= 'Z' || first >= '0' && first <= '9' || first == '[') {
			t.Fatalf("ParseReference(%q) pins %q, which starts like an option or a path", reference, pinned)
		}
		for i := 0; i < len(pinned); i++ {
			if c := pinned[i]; c <= ' ' || c >= 0x7f || strings.IndexByte("?#%\\\"'`$;&|<>(){}*!", c) >= 0 {
				t.Fatalf("ParseReference(%q) pins %q, which holds %q", reference, pinned, c)
			}
		}

		// What it parsed, written out, parses to itself.
		written := parsed.Registry + "/" + parsed.Repository
		if parsed.Tag != "" {
			written += ":" + parsed.Tag
		}
		if parsed.Digest != "" {
			written += "@" + parsed.Digest
		}
		again, err := dsl.ParseReference(written)
		if err != nil {
			t.Fatalf("ParseReference(%q) = %+v, but its own spelling %q is refused: %v", reference, parsed, written, err)
		}
		parsed.Original, again.Original = "", ""
		if parsed != again {
			t.Fatalf("ParseReference(%q) = %+v, its spelling %q parses to %+v", reference, parsed, written, again)
		}
	})
}

// FuzzParseOrkflowUses: `uses: orkflow:<name>@<version>` accepts a slug and a
// stored-version string and nothing else, and what it accepts spells itself.
func FuzzParseOrkflowUses(f *testing.F) {
	for _, seed := range []string{
		"orkflow:build@1.2.0", "orkflow:build", "orkflow:build@", "orkflow:@1", "orkflow:", "shell", "orkflow:Build@1", "orkflow:build@-1",
		"orkflow:build@1@2", "orkflow:{{ .inputs.x }}@1", "orkflow:build@{{ .inputs.v }}", "orkflow:a-b@2026-09-08.1", "orkflow:build@" + strings.Repeat("1", 201),
		"orkflow:" + strings.Repeat("a", 201) + "@1", "orkflow:build@1 ", " orkflow:build@1", "orkflow:build@\u00e9", "orkflow:bu\x00ild@1",
	} {
		f.Add(seed)
	}
	version := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	slug := regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	f.Fuzz(func(t *testing.T, uses string) {
		name, ver, ok, err := dsl.ParseOrkflowUses(uses)
		if ok != strings.HasPrefix(uses, dsl.OrkflowUsesPrefix) {
			t.Fatalf("ParseOrkflowUses(%q) ok=%v", uses, ok)
		}
		if err != nil {
			if name != "" || ver != "" || !ok {
				t.Fatalf("ParseOrkflowUses(%q) = %q, %q, %v, %v", uses, name, ver, ok, err)
			}
			return
		}
		if !ok {
			if name != "" || ver != "" {
				t.Fatalf("ParseOrkflowUses(%q) is not a reference but returned %q@%q", uses, name, ver)
			}
			return
		}
		if len(name) > 200 || !slug.MatchString(name) || len(ver) > 200 || !version.MatchString(ver) {
			t.Fatalf("ParseOrkflowUses(%q) accepted %q@%q", uses, name, ver)
		}
		if written := dsl.OrkflowUsesPrefix + name + "@" + ver; written != uses {
			t.Fatalf("ParseOrkflowUses(%q) = %q@%q, which spells %q", uses, name, ver, written)
		}
	})
}

// FuzzParseEachOver: `each.over` is one reference to an array in four
// shapes, never executed. What it accepts names exactly one root, and written
// as the index form, parses back to the same source.
func FuzzParseEachOver(f *testing.F) {
	for _, seed := range []string{
		"{{ .inputs.items }}", "{{ .inputs.cfg.list }}", "{{ .tasks.plan.output.items }}", "{{ .tasks.plan.output.a.b }}",
		`{{ index .inputs "items" }}`, `{{ index .tasks "plan-it" "output" "my-key" }}`, "{{- .inputs.items -}}", "  {{ .inputs.items }}  ",
		"", "items", "{{ .inputs }}", "{{ .tasks.plan }}", "{{ .tasks.plan.output }}", "{{ .env.X }}", "{{ .inputs.a | len }}", "{{ range .inputs.a }}x{{ end }}",
		"{{ .inputs.a }}{{ .inputs.b }}", "x {{ .inputs.a }}", "{{ $x := .inputs.a }}", `{{ index .inputs 1 }}`, `{{ index .inputs "a" 1 }}`,
		`{{ index "x" "a" }}`, "{{ . }}", "{{ define \"x\" }}{{ end }}{{ .inputs.a }}", "{{ call .inputs.a }}", "{{ .inputs.a.b.c.d.e.f.g.h }}",
		`{{ index .inputs "a\"b" }}`, `{{ index .inputs "\u00e9" }}`, "{{", "{{ }}",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, over string) {
		source, err := dsl.ParseEachOver(over)
		if err != nil {
			if source != nil || !strings.Contains(err.Error(), "each.over") {
				t.Fatalf("ParseEachOver(%q) = %v, %v; a refusal names each.over", over, source, err)
			}
			return
		}
		if (source.GetInput() == "") == (source.GetTaskId() == "") {
			t.Fatalf("ParseEachOver(%q) = %v: exactly one of input and task_id is set", over, source)
		}
		if source.GetTaskId() != "" && len(source.GetPath()) == 0 {
			t.Fatalf("ParseEachOver(%q) = %v: a task source holds at least one key", over, source)
		}
		var b strings.Builder
		if source.GetInput() != "" {
			b.WriteString("{{ index .inputs " + strconv.Quote(source.GetInput()))
		} else {
			b.WriteString("{{ index .tasks " + strconv.Quote(source.GetTaskId()) + ` "output"`)
		}
		for _, key := range source.GetPath() {
			b.WriteString(" " + strconv.Quote(key))
		}
		b.WriteString(" }}")
		again, err := dsl.ParseEachOver(b.String())
		if err != nil || !proto.Equal(source, again) {
			t.Fatalf("ParseEachOver(%q) = %v, but its index spelling %s reads as %v (%v)", over, source, b.String(), again, err)
		}
	})
}

// FuzzValidateAwaitValues feeds a callback body to the validation the doors
// run, against a schema with every type, and checks the verdict and the words
// of the refusal against the documented rules, read with a different decoder:
// the first failing rule wins, sorted by name, and a value's kind is the kind
// its JSON text spells.
func FuzzValidateAwaitValues(f *testing.F) {
	for _, seed := range []string{
		`{"name":"a","count":1,"flag":true,"obj":{},"list":[]}`, `{"name":"a"}`, `{}`, `{"name":null}`, `{"name":1}`, `{"name":"a","count":"1"}`,
		`{"name":"a","flag":1}`, `{"name":"a","obj":[]}`, `{"name":"a","list":{}}`, `[]`, `"x"`, `12`, `true`, `null`, ``, ` `, `{`, `{}{}`,
		`{"name":"a","name":1}`, "{\"name\":\"\xff\"}", `{"name":"a","count":1e999}`, `{"name":"a","count":-0.0}`, "\u00a0{\"name\":\"a\"}",
		`{"name":"` + strings.Repeat("x", 100) + `"}`,
	} {
		f.Add([]byte(seed), int64(0), uint8(1))
		f.Add([]byte(seed), int64(64), uint8(0))
	}
	f.Fuzz(func(t *testing.T, raw []byte, maxBytes int64, required uint8) {
		types := map[string]string{"name": "string", "count": "number", "flag": "boolean", "obj": "object", "list": "array"}
		order := []string{"count", "flag", "list", "name", "obj"}
		schema := map[string]any{}
		for i, key := range order {
			schema[key] = map[string]any{"type": types[key], "required": required>>uint(i)&1 == 1}
		}
		fields, err := structpb.NewStruct(schema)
		if err != nil {
			t.Fatal(err)
		}
		spec := &orkstackv1.AwaitSpec{TimeoutSeconds: 60, Schema: fields}
		got := dsl.ValidateAwaitValues(spec, raw, maxBytes)
		want := awaitVerdict(raw, maxBytes, order, types, required)
		switch {
		case got == nil && want == "":
		case got == nil || want == "" || got.Error() != want:
			t.Fatalf("ValidateAwaitValues(%q, max %d, required %05b) = %v, the rules say %q", raw, maxBytes, required, got, want)
		}
		if bare := dsl.ValidateAwaitValues(nil, raw, maxBytes); (bare == nil) != (want == "" || strings.Contains(want, "values.")) {
			t.Fatalf("with no schema ValidateAwaitValues(%q) = %v; the schema-free rules say %q", raw, bare, want)
		}
	})
}

// awaitVerdict is the documented rule list, answered from the JSON text.
func awaitVerdict(raw []byte, maxBytes int64, order []string, types map[string]string, required uint8) string {
	if maxBytes > 0 && int64(len(raw)) > maxBytes {
		return fmt.Sprintf("values: the values are %d bytes, over the %d-byte limit", len(raw), maxBytes)
	}
	if !utf8.Valid(raw) {
		return "values: the values are not valid UTF-8"
	}
	if !json.Valid(raw) {
		return "values: the values are not one valid JSON value"
	}
	kindOf := func(text []byte) string {
		switch trimmed := strings.TrimLeft(string(text), " \t\r\n"); trimmed[0] {
		case '{':
			return "an object"
		case '[':
			return "an array"
		case '"':
			return "a string"
		case 't', 'f':
			return "a boolean"
		case 'n':
			return "null"
		default:
			return "a number"
		}
	}
	if kind := kindOf(raw); kind != "an object" {
		return "values: the values must be a JSON object, not " + kind
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return "values: the values are not one valid JSON value"
	}
	names := append([]string(nil), order...)
	sort.Strings(names)
	for i, name := range order {
		_ = i
		_ = name
	}
	for _, name := range names {
		index := sort.SearchStrings(order, name)
		value, present := object[name]
		if !present {
			if required>>uint(index)&1 == 1 {
				return "values." + name + ": is required by the await's schema"
			}
			continue
		}
		want := map[string]string{"string": "a string", "number": "a number", "boolean": "a boolean", "object": "an object", "array": "an array"}[types[name]]
		if got := kindOf(value); got != want {
			article := "a " + types[name]
			if types[name] == "object" || types[name] == "array" {
				article = "an " + types[name]
			}
			return "values." + name + ": must be " + article + ", not " + got
		}
	}
	return ""
}

// FuzzCompile_Orkflow reads arbitrary documents with the examples as seeds.
// A document compiles or is refused with a reason, never panics, and one that
// compiles has a definition digest in its one shape that depends only on the
// message: compiling again, and a trip through the wire format, keep it.
func FuzzCompile_Orkflow(f *testing.F) {
	examples, err := filepath.Glob("../../examples/*.ork.yaml")
	if err != nil || len(examples) == 0 {
		f.Fatalf("no example Orkflows found: %v", err)
	}
	for _, path := range examples {
		source, err := os.ReadFile(path)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(source)
	}
	f.Add([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ntasks: []\n"))
	f.Add([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ntasks:\n  - id: t\n    uses: shell\n    with: {op: exec, command: [echo, \"{{ sh .inputs.x }}\"]}\n"))
	digestShape := regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	f.Fuzz(func(t *testing.T, source []byte) {
		wf, err := dsl.Compile(source)
		if err != nil {
			if err.Error() == "" {
				t.Fatalf("Compile refused %q without a reason", source)
			}
			return
		}
		digest, err := dsl.DefinitionDigest(wf)
		if err != nil || !digestShape.MatchString(digest) {
			t.Fatalf("Compile accepted %q, whose digest is %q, %v", source, digest, err)
		}
		again, err := dsl.Compile(source)
		if err != nil {
			t.Fatalf("Compile accepted %q once and refused it again: %v", source, err)
		}
		if second, _ := dsl.DefinitionDigest(again); second != digest {
			t.Fatalf("Compile(%q) digests differ between runs: %s then %s", source, digest, second)
		}
		wire, err := proto.Marshal(wf)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		back := &orkstackv1.Orkflow{}
		if err := proto.Unmarshal(wire, back); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if viaWire, _ := dsl.DefinitionDigest(back); viaWire != digest {
			t.Fatalf("Compile(%q): the digest %s changed to %s across the wire format", source, digest, viaWire)
		}
	})
}

// FuzzParseInterfaceLabel: a label is config-blob text an untrusted image
// supplies. It is refused with a reason or yields an interface that holds
// every bound the parser states and whose types are the closed vocabulary.
func FuzzParseInterfaceLabel(f *testing.F) {
	for _, seed := range []string{
		`{"schemaVersion":1}`, `{"schemaVersion":1,"fields":{"url":{"type":"string","required":true}}}`,
		`{"schemaVersion":1,"ops":{"get":{"fields":{"url":{"type":"string"}}}},"defaultOp":"get"}`,
		`{"schemaVersion":1,"fields":{"n":{"type":"number|string","default":1}}}`, `{"schemaVersion":2}`, `{}`, ``, `[]`, `null`,
		`{"schemaVersion":1,"fields":{"n":{"type":"nope"}}}`, `{"schemaVersion":1,"fields":{"n":{"type":"string|string"}}}`,
		`{"schemaVersion":1,"fields":{"n":{"type":"string","required":true,"default":"x"}}}`, `{"schemaVersion":1,"unknown":1}`,
		`{"schemaVersion":1,"fields":{"op":{"type":"string"}},"ops":{"a":{}}}`,
	} {
		f.Add(seed)
	}
	pattern := regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	vocabulary := map[string]bool{"string": true, "number": true, "boolean": true, "object": true, "array": true}
	f.Fuzz(func(t *testing.T, raw string) {
		iface, err := dsl.ParseInterfaceLabel(raw)
		if err != nil {
			if iface != nil || err.Error() == "" {
				t.Fatalf("ParseInterfaceLabel refused %q as %v with %v", raw, err, iface)
			}
			return
		}
		if len(raw) > dsl.MaxInterfaceLabelBytes || iface.GetSchemaVersion() != 1 || len(iface.GetOps()) > dsl.MaxInterfaceOps {
			t.Fatalf("ParseInterfaceLabel accepted %q beyond its bounds", raw)
		}
		check := func(where string, fields map[string]*orkstackv1.FieldSchema) {
			if len(fields) > dsl.MaxInterfaceFields {
				t.Fatalf("%s holds %d keys", where, len(fields))
			}
			for key, field := range fields {
				if !pattern.MatchString(key) {
					t.Fatalf("%s accepted the key %q", where, key)
				}
				seen := map[string]bool{}
				for _, word := range strings.Split(field.GetType(), "|") {
					if !vocabulary[word] || seen[word] {
						t.Fatalf("%s.%s accepted the type %q", where, key, field.GetType())
					}
					seen[word] = true
				}
				if field.GetRequired() && field.GetDefault() != nil {
					t.Fatalf("%s.%s is required and defaulted", where, key)
				}
			}
		}
		check("fields", iface.GetFields())
		check("values", iface.GetValues())
		for name, op := range iface.GetOps() {
			check("ops."+name, op.GetFields())
			for key := range op.GetFields() {
				if _, shared := iface.GetFields()[key]; shared {
					t.Fatalf("key %q is declared in fields and under op %q", key, name)
				}
			}
		}
		if def := iface.GetDefaultOp(); def != "" {
			if _, ok := iface.GetOps()[def]; !ok {
				t.Fatalf("defaultOp %q is not a declared op", def)
			}
		}
	})
}
