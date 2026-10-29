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
		`{"name":"\ud800"}`, `{"name":"\udc00x"}`, `{"name":"\ud800A"}`, `{"\ud800":1}`, `{"name":"😀"}`, `{"name":"\\ud800"}`,
		`{"name":"\uD800"}`, `{"name":"\ud800𐀀"}`, `{"name":"a\\\ud800"}`, `{"name":"\\\\ud800"}`,
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
	if escape := halfOfAPair(raw); escape != "" {
		return "values: the values hold a string that cannot be read as text: the escape " + escape + " is half of a surrogate pair, not a character"
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

var (
	jsonStringLiteral = regexp.MustCompile(`(?s)"(?:[^"\\]|\\.)*"`)
	jsonStringToken   = regexp.MustCompile(`(?s)\\u[0-9a-fA-F]{4}|\\.|[^\\]`)
)

// halfOfAPair returns the first unicode escape in a valid JSON text's strings
// that is half of a surrogate pair, as spelled, or "" when every string is
// text. It reads the strings as sequences of escapes and characters, which is
// not how CheckJSONText scans them.
func halfOfAPair(valid []byte) string {
	unitOf := func(token string) (rune, bool) {
		if len(token) != 6 || token[:2] != `\u` {
			return 0, false
		}
		unit, err := strconv.ParseUint(token[2:], 16, 32)
		return rune(unit), err == nil
	}
	for _, literal := range jsonStringLiteral.FindAll(valid, -1) {
		tokens := jsonStringToken.FindAllString(string(literal[1:len(literal)-1]), -1)
		for i := 0; i < len(tokens); i++ {
			unit, isEscape := unitOf(tokens[i])
			switch {
			case !isEscape:
			case unit >= 0xDC00 && unit < 0xE000:
				return tokens[i]
			case unit >= 0xD800 && unit < 0xDC00:
				if next, ok := unitOf(func() string {
					if i+1 < len(tokens) {
						return tokens[i+1]
					}
					return ""
				}()); !ok || next < 0xDC00 || next >= 0xE000 {
					return tokens[i]
				}
				i++
			}
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

// FuzzParseInterfaceDocument: an authored activity interface is refused with
// a reason, or yields an interface whose packed label the engine reads back
// as the same interface — canonical bytes, no single quote, within the
// label's bound — so what `orkctl activity interface pack` writes is never a
// label the engine would read differently or refuse.
func FuzzParseInterfaceDocument(f *testing.F) {
	const header = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	for _, seed := range []string{
		header,
		header + "fields:\n  url:\n    type: string\n    required: true\n  body:\n    type: string|object|array\n",
		header + "ops:\n  get: {}\n  post:\n    fields:\n      body:\n        type: object\ndefaultOp: get\n",
		header + "fields:\n  as:\n    type: string\n    default: \"it's here\"\n  n:\n    type: number\n    default: 1.5\n",
		header + "values:\n  path:\n    type: string\n",
		header + "fields:\n  a:\n    type: string\n    default: yes\n",
		header + "fields:\n  a: &x\n    type: string\n  b: *x\n",
		header + "schemaVersion: 1\n",
		header + "---\n" + header,
		"apiVersion: orkstack/v1\nkind: Orkflow\n",
		"- a\n", "", "fields: {}\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		iface, err := dsl.ParseInterfaceDocument([]byte(source))
		if err != nil {
			if iface != nil || err.Error() == "" {
				t.Fatalf("ParseInterfaceDocument refused %q as %v with %v", source, err, iface)
			}
			return
		}
		if len(source) > dsl.MaxInterfaceDocumentBytes || iface.GetSchemaVersion() != 1 {
			t.Fatalf("ParseInterfaceDocument accepted %q beyond its bounds", source)
		}
		label, err := dsl.PackInterfaceLabel(iface)
		if err != nil {
			if !strings.Contains(err.Error(), "limit is") {
				t.Fatalf("PackInterfaceLabel refused an accepted document %q: %v", source, err)
			}
			return
		}
		if len(label) > dsl.MaxInterfaceLabelBytes || strings.Contains(label, "'") {
			t.Fatalf("PackInterfaceLabel(%q) = %q", source, label)
		}
		if again, err := dsl.PackInterfaceLabel(iface); err != nil || again != label {
			t.Fatalf("PackInterfaceLabel is not deterministic: %q, then %q (%v)", label, again, err)
		}
		back, err := dsl.ParseInterfaceLabel(label)
		if err != nil {
			t.Fatalf("the engine refuses the packed label %q of %q: %v", label, source, err)
		}
		if !proto.Equal(back, iface) {
			t.Fatalf("the packed label of %q reads back as\n%v\nnot\n%v", source, back, iface)
		}
	})
}

// FuzzRenderValues_OnlyAPrintedNilIsRefused: a template prints what its
// pieces spell, and refuses exactly one thing, a nil value it prints. Text,
// data and a function's result that spell the nil marker, whole or in pieces
// that only meet in the output, are just text; a nil value that is tested,
// not printed, is just a nil. The oracle is the concatenation of the pieces.
func FuzzRenderValues_OnlyAPrintedNilIsRefused(f *testing.F) {
	for _, seed := range []struct{ before, middle, after, v1, v2 string }{
		{"", "", "", "<no", " value>"},
		{"<no ", "", "", "value>", ""},
		{"see <no ", "> here", "", "value", ""},
		{"", "", "", "<no value>", "<no value>"},
		{"<no value>", "", "<no value>", "x", "y"},
		{"", " ", "", "<no\x01value>", "<no value>"},
		{"a", "b", "c", "d", "e"},
		{"", "", "", "", ""},
		{"", "", "", "\xff", "é"},
	} {
		for mode := uint8(0); mode < 8; mode++ {
			f.Add(seed.before, seed.middle, seed.after, seed.v1, seed.v2, mode)
		}
	}
	f.Fuzz(func(t *testing.T, before, middle, after, v1, v2 string, mode uint8) {
		if strings.Contains(before+middle+after, "{") {
			return // template text, the author's: no brace that could join an action's
		}
		type variant struct {
			tail    string // appended after the second value
			printed string // what it prints when it prints
			nilHit  bool   // it prints a nil value
		}
		variants := []variant{
			{"", "", false},
			{`{{ printf "%s%s" .inputs.v1 .inputs.v2 }}`, v1 + v2, false},
			{`{{ print .inputs.v1 .inputs.v2 }}`, v1 + v2, false},
			{`{{ .inputs.n }}`, "", true},
			{`{{ if .inputs.n }}x{{ else }}` + "y" + `{{ end }}`, "y", false},
			{`{{ index .inputs.m "gone" }}`, "", true},
			{`{{ range .inputs.l }}[{{ . }}]{{ end }}`, "[" + v1 + "][" + v2 + "]", false},
			{`{{ range .inputs.ln }}[{{ . }}]{{ end }}`, "", true},
		}
		chosen := variants[int(mode)%len(variants)]
		inputs := map[string]any{
			"v1": v1, "v2": v2, "n": nil, "m": map[string]any{},
			"l": []any{v1, v2}, "ln": []any{v1, nil},
		}
		text := before + "{{ .inputs.v1 }}" + middle + "{{ .inputs.v2 }}" + after + chosen.tail
		got, err := dsl.RenderValues("with", map[string]any{"v": text}, inputs, nil)
		if chosen.nilHit {
			if err == nil || !strings.Contains(err.Error(), "template resolved to <no value>") {
				t.Fatalf("%q printed a nil value and rendered %v, %v", text, got, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%q over %q and %q: %v", text, v1, v2, err)
		}
		if want := before + v1 + middle + v2 + after + chosen.printed; got.(map[string]any)["v"] != want {
			t.Fatalf("%q rendered %q, want %q", text, got.(map[string]any)["v"], want)
		}
	})
}

// FuzzCheckJSONText: for a text that is JSON, CheckJSONText refuses exactly
// what Go's decoder would change on the way through, text that is not UTF-8
// and half of a surrogate pair, names the escape it refuses, and nothing the
// decoder keeps whole is refused. The oracle reads the strings as escapes and
// characters; CheckJSONText scans their bytes.
func FuzzCheckJSONText(f *testing.F) {
	for _, seed := range []string{
		`{}`, `"a"`, `"😀"`, `"\ud800"`, `"\udc00"`, `"\ud800A"`, `"\ud800x"`, `"\\ud800"`, `"\\\ud800"`, `"\ud800𐀀"`,
		`"\uD800"`, `"􏿿"`, `"\uDC00\uD800"`, `{"\ud800":"\udc00"}`, `["\u0000","\u001f","�"]`, `"\ud800`, `"\ud80`, `"\u"`, `"\ud800\u`,
		"\"\xff\"", "\"\xed\xa0\x80\"", "\"é\"", `[1,"\ud83d",2]`, `"\ud83d😀"`, "", `x`,
		`"\ufffd"`, `"\uFFFD"`, `"\udbff\udfff"`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		err := dsl.CheckJSONText(data)
		if !utf8.Valid(data) {
			if err == nil {
				t.Fatalf("CheckJSONText accepted %q, which is not UTF-8", data)
			}
			return
		}
		if !json.Valid(data) {
			return // not JSON: the decoder's to refuse
		}
		if escape := halfOfAPair(data); escape == "" {
			if err != nil {
				t.Fatalf("CheckJSONText refused %q, whose strings are all text: %v", data, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), escape) {
			t.Fatalf("CheckJSONText(%q) = %v, want the half pair %s named", data, err, escape)
		}
		if err == nil {
			// What it lets through, Go's decoder reads back whole: a
			// replacement character appears only where the text spells one,
			// as the escape \ufffd in either case or as the character itself.
			var decoded any
			if json.Unmarshal(data, &decoded) != nil {
				return
			}
			if holdsReplacement(decoded) && !strings.Contains(strings.ToLower(string(data)), `\ufffd`) && !strings.Contains(string(data), string(utf8.RuneError)) {
				t.Fatalf("CheckJSONText accepted %q, which Go reads with a replacement character it does not spell", data)
			}
		}
	})
}

func holdsReplacement(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.ContainsRune(typed, utf8.RuneError)
	case map[string]any:
		for k, v := range typed {
			if strings.ContainsRune(k, utf8.RuneError) || holdsReplacement(v) {
				return true
			}
		}
	case []any:
		for _, v := range typed {
			if holdsReplacement(v) {
				return true
			}
		}
	}
	return false
}
