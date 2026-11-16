package eventsources

import (
	json "encoding/json/v2"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var declared = map[string]Input{
	"order_id": {Type: "string", Required: true},
	"region":   {Type: "string", HasDefault: true},
	"count":    {Type: "number"},
	"detail":   {Type: "object"},
	"order":    {Type: "artifact", Required: true},
	"untyped":  {},
}

func TestMapping_AgainstDeclaredInputs(t *testing.T) {
	jsonFmt := orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON
	ok := func() *orkstackv1.EventSource {
		return &orkstackv1.EventSource{
			Format:    jsonFmt,
			Map:       map[string]*orkstackv1.MapRule{"order_id": {Pointer: "/id"}},
			Artifacts: map[string]orkstackv1.MessagePart{"order": orkstackv1.MessagePart_MESSAGE_PART_BODY},
		}
	}
	tests := []struct {
		name  string
		edit  func(*orkstackv1.EventSource)
		field string
	}{
		{"valid", func(*orkstackv1.EventSource) {}, ""},
		{"undeclared map name", func(s *orkstackv1.EventSource) { s.Map["nope"] = &orkstackv1.MapRule{Pointer: "/x"} }, "map.nope"},
		{"undeclared constant", func(s *orkstackv1.EventSource) { s.Input = payload(t, map[string]any{"nope": 1}) }, "input.nope"},
		{"name under two keys", func(s *orkstackv1.EventSource) { s.Input = payload(t, map[string]any{"order_id": "x"}) }, "map.order_id"},
		{"artifact input mapped as a value", func(s *orkstackv1.EventSource) { s.Map["order"] = &orkstackv1.MapRule{Pointer: "/x"} }, "map.order"},
		{"value input bound as an artifact", func(s *orkstackv1.EventSource) {
			s.Artifacts["region"] = orkstackv1.MessagePart_MESSAGE_PART_BODY
		}, "artifacts.region"},
		{"artifact input left unbound", func(s *orkstackv1.EventSource) { s.Artifacts = nil }, "artifacts.order"},
		{"attribute to a number", func(s *orkstackv1.EventSource) { s.Map["count"] = &orkstackv1.MapRule{Attribute: "n"} }, "map.count"},
		{"field to an untyped input", func(s *orkstackv1.EventSource) {
			s.Map["untyped"] = &orkstackv1.MapRule{Field: orkstackv1.MessageField_MESSAGE_FIELD_SUBJECT}
		}, "map.untyped"},
		{"pointer on a bytes source", func(s *orkstackv1.EventSource) { s.Format = orkstackv1.MessageFormat_MESSAGE_FORMAT_BYTES }, "map.order_id"},
		{"constant of the wrong type", func(s *orkstackv1.EventSource) { s.Input = payload(t, map[string]any{"count": "two"}) }, "input.count"},
		{"required input uncovered", func(s *orkstackv1.EventSource) { delete(s.Map, "order_id") }, "map.order_id"},
		{"two selectors", func(s *orkstackv1.EventSource) { s.Map["order_id"].Attribute = "x" }, "map.order_id"},
		{"no selector", func(s *orkstackv1.EventSource) { s.Map["order_id"] = &orkstackv1.MapRule{} }, "map.order_id"},
		{"bad pointer", func(s *orkstackv1.EventSource) { s.Map["order_id"].Pointer = "id" }, "map.order_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := ok()
			tt.edit(src)
			err := CheckMapping(src, declared)
			if tt.field == "" {
				if err != nil {
					t.Fatalf("CheckMapping = %v", err)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) || e.Reason != ReasonMappingInvalid || e.Field != tt.field {
				t.Fatalf("CheckMapping = %#v, want %s at %s", err, ReasonMappingInvalid, tt.field)
			}
		})
	}
}

func TestExtract_SelectsWithoutTypes(t *testing.T) {
	src := &orkstackv1.EventSource{
		Format: orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Map: map[string]*orkstackv1.MapRule{
			"a": {Pointer: "/a"},
			"b": {Pointer: "/missing"},
			"c": {Attribute: "Region"},
			"d": {Field: orkstackv1.MessageField_MESSAGE_FIELD_ID},
		},
	}
	got, err := Extract(src, Message{ID: "m-1", Attributes: map[string][]string{"Region": {"eu", "us"}}, Body: []byte(`{"a":[1,2]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(got["a"]) != `[1,2]` || string(got["c"]) != `"eu"` || string(got["d"]) != `"m-1"` {
		t.Fatalf("extracted %q", got)
	}
	if _, present := got["b"]; present {
		t.Fatal("a selector that selected nothing yields nothing")
	}
}

func TestMap_FieldGroupIsTheBrokersNotAnAttribute(t *testing.T) {
	src := &orkstackv1.EventSource{Map: map[string]*orkstackv1.MapRule{
		"g": {Field: orkstackv1.MessageField_MESSAGE_FIELD_GROUP},
	}}
	got, err := Extract(src, Message{Group: "broker-group", Attributes: map[string][]string{"group": {"publisher"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(got["g"]) != `"broker-group"` {
		t.Fatalf("field group = %s, want the broker's", got["g"])
	}
}

func TestEvaluate_JudgesTypes(t *testing.T) {
	src := &orkstackv1.EventSource{
		Format: orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Map: map[string]*orkstackv1.MapRule{
			"order_id": {Pointer: "/id"},
			"region":   {Pointer: "/region"},
			"count":    {Pointer: "/n"},
		},
		Input: &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{"detail": structpb.NewStructValue(&structpb.Struct{})}}},
	}
	inputs := map[string]Input{
		"order_id": {Type: "string", Required: true},
		"region":   {Type: "string", HasDefault: true},
		"count":    {Type: "number"},
		"detail":   {Type: "object"},
	}
	tests := []struct {
		body   string
		reason string
		input  string
	}{
		{`{"id":"o-1","region":"eu","n":2}`, "", `{"count":2,"detail":{},"order_id":"o-1","region":"eu"}`},
		{`{"id":"o-1","region":null}`, "", `{"detail":{},"order_id":"o-1"}`},
		{`{"region":"eu"}`, ReasonMessageFieldMissing, ""},
		{`{"id":null}`, ReasonMessageFieldMissing, ""},
		{`{"id":7}`, ReasonMessageFieldType, ""},
		{`{"id":"o","n":"2"}`, ReasonMessageFieldType, ""},
		{`{"id":"` + strings.Repeat("x", MaxSelectedBytes) + `"}`, ReasonMessageInputTooLarge, ""},
	}
	for _, tt := range tests {
		res := Evaluate(src, Message{Body: []byte(tt.body)}, Judge{Inputs: inputs})
		if tt.reason != "" {
			if res.Reason != tt.reason {
				t.Errorf("body %.40s: reason %q (%s), want %q", tt.body, res.Reason, res.Detail, tt.reason)
			}
			continue
		}
		if res.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_ACCEPTED {
			t.Errorf("body %s: %v %s %s", tt.body, res.Outcome, res.Reason, res.Detail)
			continue
		}
		got, _ := json.Marshal(res.Input, json.Deterministic(true))
		if string(got) != tt.input {
			t.Errorf("body %s: input %s, want %s", tt.body, got, tt.input)
		}
	}
}

func TestEvaluate_MessageBounds(t *testing.T) {
	src := &orkstackv1.EventSource{MaxBytes: 4}
	tests := []struct {
		msg    Message
		reason string
	}{
		{Message{Body: []byte("12345")}, ReasonMessageTooLarge},
		{Message{ID: strings.Repeat("i", 257)}, ReasonMessageIdentityInvalid},
		{Message{ID: "a\x07b"}, ReasonMessageIdentityInvalid},
		{Message{Subject: strings.Repeat("s", 1025)}, ReasonMessageTextInvalid},
		{Message{Subject: "\xff"}, ReasonMessageTextInvalid},
		{Message{Attributes: map[string][]string{strings.Repeat("k", 257): {"v"}}}, ReasonMessageTextInvalid},
		{Message{Attributes: map[string][]string{"k": {strings.Repeat("v", 64<<10)}}}, ReasonMessageAttributesTooLarge},
	}
	for i, tt := range tests {
		if res := Evaluate(src, tt.msg, Judge{}); res.Reason != tt.reason {
			t.Errorf("case %d: reason %q, want %q", i, res.Reason, tt.reason)
		}
	}
}

func payload(t *testing.T, m map[string]any) *orkstackv1.Payload {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: s}
}
