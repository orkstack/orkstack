package automation

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/proto"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const sourceFileHeader = "apiVersion: orkstack/v1\nkind: EventSource\n"

func TestParseEventSourceFile(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string // "" means it parses
		wantNS  string
		name2   string
	}{
		{name: "minimal", source: sourceFileHeader + "orkflow: f\nuses: ghcr.io/acme/s:1\n"},
		{name: "namespace and name", source: sourceFileHeader + "namespace: payments\nname: orders\norkflow: f\nuses: r\n", wantNS: "payments", name2: "orders"},
		{name: "header after comments", source: "# orders\n" + sourceFileHeader + "orkflow: f\nuses: r\n"},
		{name: "no header", source: "orkflow: f\nuses: r\n", wantErr: "no apiVersion or kind"},
		{name: "no kind", source: "apiVersion: orkstack/v1\norkflow: f\nuses: r\n", wantErr: `add the line "kind: EventSource"`},
		{name: "no apiVersion", source: "kind: EventSource\norkflow: f\nuses: r\n", wantErr: `add the line "apiVersion: orkstack/v1"`},
		{name: "other apiVersion", source: "apiVersion: orkstack/v2\nkind: EventSource\norkflow: f\nuses: r\n", wantErr: `apiVersion "orkstack/v2" is not supported`},
		{name: "kind not a string", source: "apiVersion: orkstack/v1\nkind: 1\norkflow: f\nuses: r\n", wantErr: "kind must be the string EventSource"},
		{name: "an Orkflow", source: "apiVersion: orkstack/v1\nkind: Orkflow\nname: f\n", wantErr: "is an Orkflow, not an event source"},
		{name: "an automation document", source: "apiVersion: orkstack/v1\nkind: Automation\nnamespace: a\n", wantErr: "is an automation document"},
		{name: "an activity interface", source: "apiVersion: orkstack/v1\nkind: ActivityInterface\n", wantErr: "is an activity interface"},
		{name: "unknown kind", source: "apiVersion: orkstack/v1\nkind: Source\n", wantErr: `kind "Source" is not an event source`},
		{name: "unknown key", source: sourceFileHeader + "orkflow: f\nuses: r\nqueue: x\n", wantErr: `unknown key "queue"`},
		{name: "key twice", source: sourceFileHeader + "orkflow: f\nuses: r\nuses: s\n", wantErr: "uses is written twice"},
		{name: "kind twice", source: sourceFileHeader + "kind: EventSource\norkflow: f\nuses: r\n", wantErr: "kind is written twice"},
		{name: "alias", source: sourceFileHeader + "uses: &u r\norkflow: f\ndescription: *u\n", wantErr: "anchors, aliases and merge keys"},
		{name: "second document", source: sourceFileHeader + "orkflow: f\nuses: r\n---\norkflow: g\n", wantErr: "event source document"},
		{name: "bad name", source: sourceFileHeader + "name: Orders\norkflow: f\nuses: r\n", wantErr: "name is the event source's name"},
		{name: "bad namespace", source: sourceFileHeader + "namespace: \"\"\norkflow: f\nuses: r\n", wantErr: "namespace is a namespace's name"},
		{name: "yes is refused by the strict mapping", source: sourceFileHeader + "orkflow: f\nuses: r\nwith:\n  tls: yes\n", wantErr: "with.tls: yes is a boolean to YAML 1.1 readers"},
		{name: "tagged header key", source: "!!str apiVersion: orkstack/v1\nkind: EventSource\norkflow: f\nuses: r\n", wantErr: "a top-level key must be a plain string"},
		{name: "custom-tagged key", source: sourceFileHeader + "!x orkflow: f\nuses: r\n", wantErr: "a top-level key must be a plain string"},
		{name: "header behind a merge key", source: "<<: {apiVersion: orkstack/v1, kind: EventSource}\norkflow: f\nuses: r\n", wantErr: "merge key"},
		{name: "apiVersion not a string", source: "apiVersion: 1\nkind: EventSource\norkflow: f\nuses: r\n", wantErr: "apiVersion must be the string orkstack/v1, got the number 1"},
		{name: "kind tagged int", source: "apiVersion: orkstack/v1\nkind: !!int 1\norkflow: f\nuses: r\n", wantErr: "kind must be the string EventSource"},
		{name: "apiVersion twice", source: sourceFileHeader + "apiVersion: orkstack/v2\norkflow: f\nuses: r\n", wantErr: "apiVersion is written twice"},
		{name: "empty name", source: sourceFileHeader + "name: \"\"\norkflow: f\nuses: r\n", wantErr: "name is the event source's name"},
		{name: "number name", source: sourceFileHeader + "name: 5\norkflow: f\nuses: r\n", wantErr: "name is the event source's name"},
		{name: "null name", source: sourceFileHeader + "name:\norkflow: f\nuses: r\n", wantErr: "name is the event source's name"},
		{name: "over-long name", source: sourceFileHeader + "name: a" + strings.Repeat("b", 200) + "\norkflow: f\nuses: r\n", wantErr: "name is the event source's name"},
		{name: "bad namespace slug", source: sourceFileHeader + "namespace: Bad_Name\norkflow: f\nuses: r\n", wantErr: "namespace is a namespace's name"},
		{name: "null value", source: sourceFileHeader + "orkflow: f\nuses: r\nrate:\n", wantErr: "rate"},
		{name: "not a mapping", source: "- a\n", wantErr: "is a mapping that begins with"},
		{name: "empty", source: "", wantErr: "the document is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseEventSourceFile([]byte(tt.source))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Namespace != tt.wantNS || got.HasNamespace != (tt.wantNS != "") || got.Name != tt.name2 || got.Source.GetName() != tt.name2 {
				t.Fatalf("got namespace %q (%v), name %q, source name %q", got.Namespace, got.HasNamespace, got.Name, got.Source.GetName())
			}
			if got.Source.GetOrkflowName() != "f" {
				t.Fatalf("orkflow = %q, want f", got.Source.GetOrkflowName())
			}
		})
	}
}

// TestParseEventSourceFile_SameAsAnEntry holds the document to the automation
// entry it mirrors: the same keys read to the same source.
func TestParseEventSourceFile_SameAsAnEntry(t *testing.T) {
	entry := "orkflow: handle-order\nversion: \"1.4.0\"\nuses: ghcr.io/acme/source:1\nwith:\n  stream: ORDERS\n  tls: true\ninstances: 2\nformat: json\n" +
		"match:\n  - pointer: /kind\n    equals: created\nmap:\n  order_id:\n    pointer: /id\n"
	file, err := ParseEventSourceFile([]byte(sourceFileHeader + "name: orders\n" + entry))
	if err != nil {
		t.Fatal(err)
	}
	doc, issues := Parse([]byte("apiVersion: orkstack/v1\nkind: Automation\nnamespace: a\nevent_sources:\n  orders:\n" + indent(entry, "    ")))
	if HasErrors(issues) {
		t.Fatalf("automation document: %v", issues)
	}
	want := proto.Clone(doc.EventSources["orders"].Source)
	got := proto.Clone(file.Source)
	want.ProtoReflect().Clear(want.ProtoReflect().Descriptor().Fields().ByName("namespace"))
	got.ProtoReflect().Clear(got.ProtoReflect().Descriptor().Fields().ByName("namespace"))
	if !proto.Equal(got, want) {
		t.Fatalf("the document and the entry read differently:\n%v\n%v", got, want)
	}
}

func indent(text, prefix string) string {
	lines := strings.SplitAfter(strings.TrimSuffix(text, "\n"), "\n")
	return prefix + strings.Join(lines, prefix) + "\n"
}

func FuzzParseEventSourceFile(f *testing.F) {
	f.Add(sourceFileHeader + "orkflow: f\nuses: ghcr.io/acme/s:1\n")
	f.Add(sourceFileHeader + "namespace: payments\nname: orders\norkflow: f\nuses: r\nformat: json\nmatch:\n  - pointer: /x\n    in: [1, \"1\", true]\n")
	f.Add("orkflow: f\nuses: r\n")
	f.Add("apiVersion: orkstack/v1\nkind: Orkflow\n")
	f.Add(sourceFileHeader + "uses: &u r\norkflow: f\ndescription: *u\n")
	f.Add("<<: {apiVersion: orkstack/v1, kind: EventSource}\norkflow: f\nuses: r\n")
	f.Add("!!str apiVersion: orkstack/v1\nkind: EventSource\norkflow: f\nuses: r\n")
	f.Add(sourceFileHeader + "kind: EventSource\norkflow: f\nuses: r\n")
	f.Add("apiVersion: orkstack/v1\nkind: !!int 1\norkflow: f\nuses: r\n")
	f.Add(sourceFileHeader + "orkflow: f\nuses: r\n---\norkflow: g\n")
	f.Add("description: EventSource orkstack/v1\norkflow: f\nuses: r\n")
	f.Fuzz(func(t *testing.T, source string) {
		got, err := ParseEventSourceFile([]byte(source))
		again, errAgain := ParseEventSourceFile([]byte(source))
		if (err == nil) != (errAgain == nil) {
			t.Fatalf("parse is not deterministic: %v then %v", err, errAgain)
		}
		if err != nil {
			return
		}
		if got.Source == nil || !proto.Equal(got.Source, again.Source) {
			t.Fatalf("parse is not deterministic: %v then %v", got.Source, again.Source)
		}
		var root yaml.Node
		if err := yaml.Unmarshal([]byte(source), &root); err != nil || len(root.Content) == 0 {
			t.Fatalf("a document YAML does not read parsed: %q", source)
		}
		body := root.Content[0]
		if alias := findAlias(body); alias != nil {
			t.Fatalf("a document with an anchor, alias or merge key parsed: %q", source)
		}
		header := map[string]string{}
		for i := 0; i+1 < len(body.Content); i += 2 {
			key := body.Content[i]
			if !dsl.PlainTopLevelKey(key) {
				t.Fatalf("a document with a key that is not a plain string parsed: %q", source)
			}
			if key.Value == "apiVersion" || key.Value == "kind" {
				if _, twice := header[key.Value]; twice {
					t.Fatalf("a document with %s twice parsed: %q", key.Value, source)
				}
				header[key.Value] = body.Content[i+1].Value
			}
		}
		if header["apiVersion"] != dsl.APIVersion || header["kind"] != dsl.KindEventSource {
			t.Fatalf("a document whose header is %v parsed: %q", header, source)
		}
		if got.Source.GetNamespace() != "" {
			t.Fatalf("the parsed source carries namespace %q; the namespace is the caller's to set", got.Source.GetNamespace())
		}
		if got.Name != got.Source.GetName() || (got.Name != "" && !eventsources.ValidName(got.Name)) {
			t.Fatalf("name %q (source %q) is not a valid name", got.Name, got.Source.GetName())
		}
		if got.HasNamespace && dsl.ValidSlug("namespace", got.Namespace) != nil {
			t.Fatalf("namespace %q is not a valid name", got.Namespace)
		}
	})
}
