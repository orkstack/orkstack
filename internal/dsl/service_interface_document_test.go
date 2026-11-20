package dsl

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const serviceHeader = "apiVersion: orkstack/v1\nkind: ServiceInterface\n"

const serviceDoc = serviceHeader + `profile: event-source
with:
  stream:
    type: string
    required: true
  batch:
    type: number
    default: 10
event_source:
  delivery: at-least-once
  max_concurrency: 4
  ordering: true
  dead_letter: true
  extends_leases: true
`

func TestServiceInterfaceDocument_Table(t *testing.T) {
	tests := []struct {
		name, source, wantErr string
	}{
		{name: "full", source: serviceDoc},
		{name: "minimal", source: serviceHeader + "profile: event-source\n"},
		{name: "at-most-once without dead letters", source: serviceHeader + "profile: event-source\nevent_source:\n  delivery: at-most-once\n  dead_letter: false\n  extends_leases: false\n"},
		{name: "no header", source: "profile: event-source\n", wantErr: "no apiVersion or kind"},
		{name: "no kind", source: "apiVersion: orkstack/v1\nprofile: event-source\n", wantErr: `add the line "kind: ServiceInterface"`},
		{name: "no apiVersion", source: "kind: ServiceInterface\nprofile: event-source\n", wantErr: `add the line "apiVersion: orkstack/v1"`},
		{name: "other apiVersion", source: "apiVersion: orkstack/v2\nkind: ServiceInterface\n", wantErr: `apiVersion "orkstack/v2" is not supported`},
		{name: "an activity interface", source: "apiVersion: orkstack/v1\nkind: ActivityInterface\n", wantErr: "is an activity interface, not a service interface"},
		{name: "an Orkflow", source: "apiVersion: orkstack/v1\nkind: Orkflow\n", wantErr: "kind: Orkflow is not a service interface"},
		{name: "an event source", source: "apiVersion: orkstack/v1\nkind: EventSource\n", wantErr: "kind: EventSource is not a service interface"},
		{name: "unknown kind", source: "apiVersion: orkstack/v1\nkind: Service\n", wantErr: `kind "Service" is not a service interface`},
		{name: "tagged key", source: "!!str apiVersion: orkstack/v1\nkind: ServiceInterface\n", wantErr: "a top-level key must be a plain string"},
		{name: "unknown key", source: serviceHeader + "profile: event-source\nfields: {}\n", wantErr: "fields"},
		{name: "schemaVersion refused", source: serviceHeader + "schemaVersion: 1\nprofile: event-source\n", wantErr: "schemaVersion is not written"},
		{name: "no profile", source: serviceHeader + "with: {}\n", wantErr: "profile is required"},
		{name: "other profile", source: serviceHeader + "profile: http-server\n", wantErr: "profile"},
		{name: "event_source under another profile", source: serviceHeader + "profile: http-server\nevent_source: {}\n", wantErr: "event_source: is the event-source profile's block"},
		{name: "anchor", source: serviceHeader + "profile: &p event-source\n", wantErr: "anchor"},
		{name: "alias", source: serviceHeader + "profile: event-source\nwith:\n  a: &x {type: string}\n  b: *x\n", wantErr: "anchor &x"},
		{name: "merge key", source: serviceHeader + "profile: event-source\nwith:\n  a:\n    <<: {type: string}\n", wantErr: "merge"},
		{name: "JSON-Schema grammar", source: serviceHeader + "profile: event-source\nwith:\n  required: [stream]\n  properties:\n    stream: {type: string}\n", wantErr: "names each key with its own declaration"},
		{name: "properties level", source: serviceHeader + "profile: event-source\nwith:\n  properties:\n    stream: {type: string}\n", wantErr: "no properties: level"},
		{name: "default through the strict mapping", source: serviceHeader + "profile: event-source\nwith:\n  tls:\n    type: boolean\n    default: yes\n", wantErr: "yes"},
		{name: "required yes refused", source: serviceHeader + "profile: event-source\nwith:\n  s:\n    type: string\n    required: yes\n", wantErr: "yes"},
		{name: "required with default", source: serviceHeader + "profile: event-source\nwith:\n  s:\n    type: string\n    required: true\n    default: x\n", wantErr: "a key with a default is not required"},
		{name: "bad type", source: serviceHeader + "profile: event-source\nwith:\n  s:\n    type: text\n", wantErr: "type is string"},
		{name: "description refused", source: serviceHeader + "profile: event-source\nwith:\n  s:\n    type: string\n    description: x\n", wantErr: "description"},
		{name: "max_concurrency 0", source: serviceHeader + "profile: event-source\nevent_source:\n  max_concurrency: 0\n", wantErr: "max_concurrency is a whole number from 1 to 64"},
		{name: "max_concurrency 65", source: serviceHeader + "profile: event-source\nevent_source:\n  max_concurrency: 65\n", wantErr: "max_concurrency is a whole number from 1 to 64"},
		{name: "max_concurrency fraction", source: serviceHeader + "profile: event-source\nevent_source:\n  max_concurrency: 1.5\n", wantErr: "max_concurrency"},
		{name: "max_concurrency string", source: serviceHeader + "profile: event-source\nevent_source:\n  max_concurrency: \"4\"\n", wantErr: "max_concurrency"},
		{name: "bad delivery", source: serviceHeader + "profile: event-source\nevent_source:\n  delivery: exactly-once\n", wantErr: "delivery is at-least-once or at-most-once"},
		{name: "ordering yes", source: serviceHeader + "profile: event-source\nevent_source:\n  ordering: yes\n", wantErr: "yes"},
		{name: "at-least-once without dead letters", source: serviceHeader + "profile: event-source\nevent_source:\n  dead_letter: false\n", wantErr: "declares dead_letter"},
		{name: "at-least-once without lease extension", source: serviceHeader + "profile: event-source\nevent_source:\n  extends_leases: false\n", wantErr: "declares extends_leases"},
		{name: "second document", source: serviceHeader + "profile: event-source\n---\nkind: x\n", wantErr: "service interface"},
		{name: "empty", source: "", wantErr: "no apiVersion or kind"},
		{name: "bad with key", source: serviceHeader + "profile: event-source\nwith:\n  \"a b\":\n    type: string\n", wantErr: "must match"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseServiceInterfaceDocument([]byte(tt.source))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.GetSchemaVersion() != 1 || got.GetProfile() != eventsources.ProfileEventSource {
				t.Fatalf("got %v", got)
			}
		})
	}
}

func TestServiceInterfaceDocument_Reads(t *testing.T) {
	got, err := ParseServiceInterfaceDocument([]byte(serviceDoc))
	if err != nil {
		t.Fatal(err)
	}
	decl := got.GetEventSource()
	if decl.GetDelivery() != orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE || decl.GetMaxConcurrency() != 4 ||
		!decl.GetOrdering() || !decl.GetDeadLetter() || !decl.GetExtendsLeases() {
		t.Fatalf("event_source = %v", decl)
	}
	if !got.GetWith()["stream"].GetRequired() || got.GetWith()["batch"].GetDefault().GetNumberValue() != 10 {
		t.Fatalf("with = %v", got.GetWith())
	}
}

// TestPackServiceInterface_Canonical: one document packs to one label, a
// single quote is escaped for a Dockerfile LABEL, and the label reads back
// through the engine's own reader to the same interface.
func TestPackServiceInterface_Canonical(t *testing.T) {
	doc := strings.Replace(serviceDoc, "default: 10", "default: \"it's\"\n    type: string", 1)
	doc = strings.Replace(doc, "    type: number\n", "", 1)
	iface, err := ParseServiceInterfaceDocument([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	first, err := PackServiceInterfaceLabel(iface)
	if err != nil {
		t.Fatal(err)
	}
	again, err := PackServiceInterfaceLabel(proto.Clone(iface).(*orkstackv1.ServiceInterface))
	if err != nil || again != first {
		t.Fatalf("pack is not canonical: %q then %q (%v)", first, again, err)
	}
	if strings.Contains(first, "'") || !strings.Contains(first, `\u0027`) || strings.ContainsAny(first, " \n") {
		t.Fatalf("label %q is not a one-line, single-quote-free value", first)
	}
	back, err := eventsources.ParseInterfaceLabel(first)
	if err != nil {
		t.Fatalf("the engine refuses the packed label: %v", err)
	}
	if !proto.Equal(back, iface) {
		t.Fatalf("round trip:\n%v\n%v", back, iface)
	}
	if _, err := PackServiceInterfaceLabel(nil); err == nil {
		t.Fatal("packing nothing succeeded")
	}
	big := &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: eventsources.ProfileEventSource, With: map[string]*orkstackv1.FieldSchema{}}
	for i := 0; i < 4000; i++ {
		big.With[strings.Repeat("k", 20)+string(rune('a'+i%26))+strings.Repeat("x", i%7)+itoa(i)] = &orkstackv1.FieldSchema{Type: "string"}
	}
	if _, err := PackServiceInterfaceLabel(big); err == nil || !strings.Contains(err.Error(), "limit is") {
		t.Fatalf("a label past 64 KiB packed: %v", err)
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var out []byte
	for ; i > 0; i /= 10 {
		out = append([]byte{digits[i%10]}, out...)
	}
	return string(out)
}

func FuzzServiceInterfaceDocument(f *testing.F) {
	f.Add(serviceDoc)
	f.Add(serviceHeader + "profile: event-source\n")
	f.Add(serviceHeader + "profile: event-source\nwith:\n  required: [stream]\n")
	f.Add(serviceHeader + "profile: event-source\nevent_source:\n  delivery: at-most-once\n  max_concurrency: 64\n")
	f.Add("apiVersion: orkstack/v1\nkind: ActivityInterface\n")
	f.Fuzz(func(t *testing.T, source string) {
		iface, err := ParseServiceInterfaceDocument([]byte(source))
		if err != nil {
			return
		}
		label, err := PackServiceInterfaceLabel(iface)
		if err != nil {
			if strings.Contains(err.Error(), "limit is") {
				return
			}
			t.Fatalf("a document that parses does not pack: %v", err)
		}
		back, err := eventsources.ParseInterfaceLabel(label)
		if err != nil {
			t.Fatalf("the engine refuses a packed label: %v\n%s", err, label)
		}
		if !proto.Equal(back, iface) {
			t.Fatalf("the label does not round-trip:\n%v\n%v", back, iface)
		}
	})
}
