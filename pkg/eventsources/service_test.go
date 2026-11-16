package eventsources

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func packed(t *testing.T, iface *orkstackv1.ServiceInterface) string {
	t.Helper()
	b, err := protojson.Marshal(iface)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func sourceLabels(t *testing.T, iface *orkstackv1.ServiceInterface) map[string]string {
	labels := map[string]string{ContractLabel: "1", ProfileLabel: "event-source/1"}
	if iface != nil {
		labels[InterfaceLabel] = packed(t, iface)
	}
	return labels
}

func TestReadServiceLabels(t *testing.T) {
	iface := &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: ProfileEventSource,
		With: map[string]*orkstackv1.FieldSchema{"stream": {Type: "string", Required: true}}}
	tests := []struct {
		name   string
		labels map[string]string
		reason string
		detail string
	}{
		{"declared", sourceLabels(t, iface), "", ""},
		{"no interface", sourceLabels(t, nil), "", ""},
		{"several versions", map[string]string{ContractLabel: "2, 1", ProfileLabel: "event-source/1,3"}, "", ""},
		{"no labels at all", map[string]string{}, ReasonImageUnsupported, ReasonServiceContractUnknown},
		{"another contract", map[string]string{ContractLabel: "2", ProfileLabel: "event-source/1"}, ReasonImageUnsupported, ReasonServiceContractUnknown},
		{"another profile", map[string]string{ContractLabel: "1", ProfileLabel: "provider/1"}, ReasonImageUnsupported, ReasonServiceProfileUnknown},
		{"another profile version", map[string]string{ContractLabel: "1", ProfileLabel: "event-source/2"}, ReasonImageUnsupported, ReasonServiceProfileUnknown},
		{"an interface that does not parse", map[string]string{ContractLabel: "1", ProfileLabel: "event-source/1", InterfaceLabel: "{"}, ReasonImageUnsupported, "interface"},
		{"an interface of another profile", map[string]string{ContractLabel: "1", ProfileLabel: "event-source/1",
			InterfaceLabel: packed(t, &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: "provider"})}, ReasonImageUnsupported, "profile"},
		{"at-least-once without dead letters", map[string]string{ContractLabel: "1", ProfileLabel: "event-source/1",
			InterfaceLabel: packed(t, &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: ProfileEventSource,
				EventSource: &orkstackv1.EventSourceDeclaration{DeadLetter: ptr(false)}})}, ReasonImageUnsupported, "dead_letter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadServiceLabels(tt.labels)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("ReadServiceLabels = %v", err)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) || e.Reason != tt.reason || !strings.Contains(e.Message, tt.detail) {
				t.Fatalf("ReadServiceLabels = %v, want %s naming %q", err, tt.reason, tt.detail)
			}
		})
	}
}

func TestCheckImage(t *testing.T) {
	iface := &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: ProfileEventSource,
		With: map[string]*orkstackv1.FieldSchema{
			"stream":  {Type: "string", Required: true},
			"servers": {Type: "array"},
			"tls":     {Type: "boolean|object"},
		},
		EventSource: &orkstackv1.EventSourceDeclaration{MaxConcurrency: 4}}
	image, err := ReadServiceLabels(sourceLabels(t, iface))
	if err != nil {
		t.Fatal(err)
	}
	with := func(m map[string]any) *structpb.Struct {
		s, err := structpb.NewStruct(m)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	tests := []struct {
		name   string
		src    *orkstackv1.EventSource
		reason string
		field  string
	}{
		{"fits", &orkstackv1.EventSource{With: with(map[string]any{"stream": "S", "tls": true}), Concurrency: 4}, "", ""},
		{"required missing", &orkstackv1.EventSource{With: with(map[string]any{})}, ReasonConfigInvalid, "with.stream"},
		{"undeclared key", &orkstackv1.EventSource{With: with(map[string]any{"stream": "S", "strem": 1})}, ReasonConfigInvalid, "with.strem"},
		{"wrong type", &orkstackv1.EventSource{With: with(map[string]any{"stream": 1})}, ReasonConfigInvalid, "with.stream"},
		{"over the image's lanes", &orkstackv1.EventSource{With: with(map[string]any{"stream": "S"}), Concurrency: 5}, ReasonConfigInvalid, "concurrency"},
		{"another class", &orkstackv1.EventSource{With: with(map[string]any{"stream": "S"}),
			Delivery: orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE}, ReasonDeliveryMismatch, "delivery"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckImage(image, tt.src)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("CheckImage = %v", err)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) || e.Reason != tt.reason || e.Field != tt.field {
				t.Fatalf("CheckImage = %v, want %s at %s", err, tt.reason, tt.field)
			}
		})
	}

	// An image that declares no interface takes any with: and runs one lane.
	bare, err := ReadServiceLabels(sourceLabels(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckImage(bare, &orkstackv1.EventSource{With: with(map[string]any{"anything": 1})}); err != nil {
		t.Fatalf("no interface: %v", err)
	}
	if err := CheckImage(bare, &orkstackv1.EventSource{Concurrency: 2}); err == nil {
		t.Fatal("an image that declares no maximum serves one lane")
	}
}

func TestFirstPartyNames(t *testing.T) {
	for _, name := range []string{"nats-jetstream", "nats", "sqs"} {
		if !IsFirstPartyName(name) {
			t.Errorf("%s is a first-party name", name)
		}
	}
	for _, ref := range []string{"ghcr.io/acme/nats", "nats:2", "docker.io/library/sqs"} {
		if IsFirstPartyName(ref) {
			t.Errorf("%s is a reference", ref)
		}
	}
}
