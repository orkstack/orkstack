package dsl

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const testDocsLabel = `{"schemaVersion":1,"description":"Copies files.","fields":{"verbose":"Prints each file."},` +
	`"ops":{"copy":{"description":"Copies one file.","fields":{"src":"The file to copy."}}},"values":{"path":"The copy."}}`

// docsInterface is the interface testDocsLabel describes.
var docsInterface = &orkstackv1.ActivityInterface{
	SchemaVersion: 1,
	Fields:        map[string]*orkstackv1.FieldSchema{"verbose": {Type: "boolean"}},
	Ops:           map[string]*orkstackv1.OpSchema{"copy": {Fields: map[string]*orkstackv1.FieldSchema{"src": {Type: "string"}}}},
	Values:        map[string]*orkstackv1.FieldSchema{"path": {Type: "string"}},
}

func TestParseDocsLabel_ReadsValidDocs(t *testing.T) {
	docs, err := ParseDocsLabel(testDocsLabel)
	must.OK(t, err)
	must.OK(t, CheckDocsAgainstInterface(docs, docsInterface))
	want := &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "Copies files.",
		Fields: map[string]string{"verbose": "Prints each file."},
		Ops:    map[string]*orkstackv1.OpDocs{"copy": {Description: "Copies one file.", Fields: map[string]string{"src": "The file to copy."}}},
		Values: map[string]string{"path": "The copy."}}
	if !proto.Equal(docs, want) {
		t.Fatalf("docs = %v, want %v", docs, want)
	}
	if docs, err := ParseDocsLabel(""); docs != nil || err != nil {
		t.Fatalf("ParseDocsLabel(\"\") = %v, %v; want nil, nil", docs, err)
	}
}

func TestParseDocsLabel_RefusesEveryRuleItStates(t *testing.T) {
	docs := func(body string) string { return `{"schemaVersion":1,` + body + `}` }
	long := func(n int) string { return strings.Repeat("a", n) }
	// entries is n comma-joined copies of format, each with its index.
	entries := func(n int, format string) string {
		var out []string
		for i := range n {
			out = append(out, fmt.Sprintf(format, i))
		}
		return strings.Join(out, ",")
	}
	// esc is a JSON \u escape, so the source holds no raw control.
	esc := func(code string) string { return `\` + "u" + code }
	for _, tc := range []struct{ name, label, want string }{
		{"too long", docs(`"description":"` + long(MaxDocsLabelBytes) + `"`), "limit is 65536"},
		{"not JSON", `description: x`, "is not the JSON encoding of an ActivityDocs"},
		{"a word outside the vocabulary", docs(`"title":"x"`), "is not the JSON encoding of an ActivityDocs"},
		{"a key's text as an object", docs(`"fields":{"a":{"description":"x"}}`), "is not the JSON encoding of an ActivityDocs"},
		{"another version", `{"schemaVersion":2}`, "schemaVersion 2 is not supported"},
		{"a long activity description", docs(`"description":"` + long(MaxActivityDescriptionChars+1) + `"`),
			"description is 1001 characters, limit is 1000"},
		{"a long key description", docs(`"fields":{"a":"` + long(MaxDescriptionChars+1) + `"}`), "fields.a is 501 characters, limit is 500"},
		{"a long value description", docs(`"values":{"a":"` + long(MaxDescriptionChars+1) + `"}`), "values.a is 501 characters"},
		{"a long op description", docs(`"ops":{"x":{"description":"` + long(MaxDescriptionChars+1) + `"}}`),
			"ops.x.description is 501 characters"},
		{"a blank key description", docs(`"values":{"a":"   "}`), "values.a is empty"},
		{"a blank op description", docs(`"ops":{"x":{"description":" "}}`), "ops.x.description is empty"},
		{"a line break", docs(`"description":"one\ntwo"`), "U+000A"},
		{"a tab", docs(`"description":"a\tb"`), "U+0009"},
		{"a carriage return", docs(`"fields":{"a":"a\rb"}`), "U+000D"},
		{"an escape", docs(`"fields":{"a":"` + esc("001b") + `[31m"}`), "U+001B"},
		{"a C1 control", docs(`"fields":{"a":"a` + esc("0085") + `b"}`), "U+0085"},
		{"a line separator", docs(`"fields":{"a":"a` + esc("2028") + `b"}`), "U+2028"},
		{"a bidi override", docs(`"description":"a` + esc("202e") + `b"`), "U+202E"},
		{"a bidi isolate", docs(`"description":"a` + esc("2066") + `b"`), "U+2066"},
		{"a bidi mark", docs(`"description":"a` + esc("200f") + `b"`), "U+200F"},
		{"a zero-width space", docs(`"description":"a` + esc("200b") + `b"`), "U+200B"},
		{"a soft hyphen", docs(`"description":"a` + esc("00ad") + `b"`), "U+00AD"},
		{"a byte order mark", docs(`"description":"a` + esc("feff") + `b"`), "U+FEFF"},
		{"a tag character", docs(`"description":"a` + esc("db40") + esc("dc41") + `b"`), "U+E0041"},
		{"a noncharacter", docs(`"description":"a` + esc("fdd0") + `b"`), "U+FDD0"},
		{"a noncharacter at a plane's end", docs(`"description":"a` + esc("fffe") + `b"`), "U+FFFE"},
		{"the last noncharacter of the plane", docs(`"description":"a` + esc("ffff") + `b"`), "U+FFFF"},
		{"the last of the noncharacter block", docs(`"description":"a` + esc("fdef") + `b"`), "U+FDEF"},
		{"a noncharacter past the plane", docs(`"description":"a` + esc("d83f") + esc("dffe") + `b"`), "U+1FFFE"},
		{"a paragraph separator", docs(`"fields":{"a":"a` + esc("2029") + `b"}`), "U+2029"},
		{"a bad key", docs(`"fields":{"a b":"x"}`), `fields: key "a b" is not a valid with: key`},
		{"a bad value key", docs(`"values":{"-a":"x"}`), `values: key "-a" is not a valid with: key`},
		{"a bad op", docs(`"ops":{"Copy":{"description":"x"}}`), `op "Copy" is not a lowercase op name`},
		{"a bad op key", docs(`"ops":{"copy":{"fields":{"a/b":"x"}}}`), `ops.copy.fields: key "a/b" is not a valid with: key`},
		{"too many ops", docs(`"ops":{` + entries(MaxInterfaceOps+1, `"op%d":{}`) + `}`), "65 ops"},
		{"too many keys", docs(`"values":{` + entries(MaxInterfaceFields+1, `"k%d":"x"`) + `}`), "257 keys"},
		{"one byte past the label bound", labelOfSize(t, MaxDocsLabelBytes+1), "limit is 65536"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDocsLabel(tc.label)
			if err == nil {
				t.Fatalf("ParseDocsLabel accepted %q as %v", tc.label, got)
			}
			if got != nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseDocsLabel = %v, %v; want an error containing %q", got, err, tc.want)
			}
		})
	}
}

func TestParseDocsLabel_KeepsPlainText(t *testing.T) {
	// Markup and backticks are text a client shows; every script is text;
	// a limit counts characters, not bytes.
	text := "Two <b>{{ x }}</b> `code` — ünïcödé, 日本語, עברית. " + strings.Repeat("日", MaxActivityDescriptionChars-50)
	docs, err := ParseDocsLabel(`{"schemaVersion":1,"description":"` + text + `"}`)
	must.OK(t, err)
	if want := text; docs.GetDescription() != want {
		t.Fatalf("description = %q, want %q", docs.GetDescription(), want)
	}
}

func TestCheckDocsAgainstInterface(t *testing.T) {
	for _, tc := range []struct{ name, label, want string }{
		{"every op and key declared", testDocsLabel, ""},
		{"an undeclared shared key", `{"schemaVersion":1,"fields":{"quiet":"x"}}`, `the shared with: key "quiet"`},
		{"an op key described as shared", `{"schemaVersion":1,"fields":{"src":"x"}}`, `the shared with: key "src"`},
		{"an undeclared op", `{"schemaVersion":1,"ops":{"move":{"description":"x"}}}`, `the op "move"`},
		{"an undeclared op key", `{"schemaVersion":1,"ops":{"copy":{"fields":{"dest":"x"}}}}`, `the with: key "dest" of op "copy"`},
		{"the implied op key", `{"schemaVersion":1,"fields":{"op":"x"}}`, `the shared with: key "op"`},
		{"an undeclared value", `{"schemaVersion":1,"values":{"size":"x"}}`, `the values.json key "size"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := ParseDocsLabel(tc.label)
			must.OK(t, err)
			err = CheckDocsAgainstInterface(docs, docsInterface)
			switch {
			case tc.want == "" && err != nil:
				t.Fatal(err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("CheckDocsAgainstInterface = %v, want an error naming %s", err, tc.want)
			case tc.want != "" && !strings.Contains(err.Error(), "inherits every label it does not set"):
				t.Fatalf("the error %q does not say why a derived image meets it", err)
			}
		})
	}
}

// TestCheckDocsAgainstInterface_WithoutAnInterfaceChecksNothing: no
// interface label, or one this engine cannot read, leaves docs readable —
// the labels fail independently.
func TestCheckDocsAgainstInterface_WithoutAnInterfaceChecksNothing(t *testing.T) {
	docs, err := ParseDocsLabel(`{"schemaVersion":1,"fields":{"anything":"x"},"ops":{"any":{"description":"x"}}}`)
	must.OK(t, err)
	if err := CheckDocsAgainstInterface(docs, nil); err != nil {
		t.Fatalf("CheckDocsAgainstInterface(docs, nil) = %v, want nil", err)
	}
}

func TestPackDocsLabel_RoundTripsCanonically(t *testing.T) {
	docs := &orkstackv1.ActivityDocs{Description: `It's "quoted".`, Values: map[string]string{"path": "The copy."}}
	label, err := PackDocsLabel(docs, docsInterface)
	must.OK(t, err)
	if want := `{"schemaVersion":1,"description":"It\u0027s \"quoted\".","values":{"path":"The copy."}}`; label != want {
		t.Fatalf("label = %s, want %s", label, want)
	}
	read, err := ParseDocsLabel(label)
	must.OK(t, err)
	docs.SchemaVersion = 1
	if !proto.Equal(read, docs) {
		t.Fatalf("read back %v, want %v", read, docs)
	}
	if label, err := PackDocsLabel(nil, docsInterface); label != "" || err != nil {
		t.Fatalf("PackDocsLabel(nil) = %q, %v; want the empty value", label, err)
	}
	if _, err := PackDocsLabel(&orkstackv1.ActivityDocs{Values: map[string]string{"size": "x"}}, docsInterface); err == nil {
		t.Fatal("PackDocsLabel packed docs of a value the interface does not declare")
	}
	if _, err := PackDocsLabel(&orkstackv1.ActivityDocs{Description: "one\ntwo"}, nil); err == nil {
		t.Fatal("PackDocsLabel packed a description the engine refuses")
	}
}

// labelOfSize is a valid docs label of exactly n bytes: 256 described
// values, the last one's text sized to fill.
func labelOfSize(t *testing.T, n int) string {
	t.Helper()
	build := func(last int) string {
		var entries []string
		for i := range MaxInterfaceFields {
			text := strings.Repeat("a", 245)
			if i == MaxInterfaceFields-1 {
				text = strings.Repeat("a", last)
			}
			entries = append(entries, fmt.Sprintf(`"k%03d":"%s"`, i, text))
		}
		return `{"schemaVersion":1,"values":{` + strings.Join(entries, ",") + `}}`
	}
	last := 245 + n - len(build(245))
	if last < 1 || last > MaxDescriptionChars {
		t.Fatalf("cannot build a label of %d bytes", n)
	}
	return build(last)
}

// TestParseDocsLabel_ReadsEveryBoundExactly: 64 ops, 256 keys and a label
// of exactly 65536 bytes are read.
func TestParseDocsLabel_ReadsEveryBoundExactly(t *testing.T) {
	var ops, keys []string
	for i := range MaxInterfaceOps {
		ops = append(ops, fmt.Sprintf(`"op%d":{"description":"x"}`, i))
	}
	for i := range MaxInterfaceFields {
		keys = append(keys, fmt.Sprintf(`"k%d":"x"`, i))
	}
	for name, label := range map[string]string{
		"64 ops":      `{"schemaVersion":1,"ops":{` + strings.Join(ops, ",") + `}}`,
		"256 keys":    `{"schemaVersion":1,"fields":{` + strings.Join(keys, ",") + `}}`,
		"65536 bytes": labelOfSize(t, MaxDocsLabelBytes),
	} {
		if _, err := ParseDocsLabel(label); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
