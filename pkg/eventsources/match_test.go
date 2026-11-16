package eventsources

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestMatch_EqualityByType(t *testing.T) {
	tests := []struct {
		name     string
		selected string
		literal  *structpb.Value
		want     bool
	}{
		{"same integer", `1`, structpb.NewNumberValue(1), true},
		{"1.0 is 1", `1.0`, structpb.NewNumberValue(1), true},
		{"1e0 is 1", `1e0`, structpb.NewNumberValue(1), true},
		{"10e-1 is 1", `10e-1`, structpb.NewNumberValue(1), true},
		{"0.10 is 0.1", `0.10`, structpb.NewNumberValue(0.1), true},
		{"-0 is 0", `-0`, structpb.NewNumberValue(0), true},
		{"-0.0e9 is 0", `-0.0e9`, structpb.NewNumberValue(0), true},
		{"sign differs", `-1`, structpb.NewNumberValue(1), false},
		{"beyond a float, exactly", `9007199254740993`, structpb.NewNumberValue(9007199254740992), false},
		{"huge exponent is cheap and unequal", `1e999999999999999999999`, structpb.NewNumberValue(1), false},
		{"string is not number", `"1"`, structpb.NewNumberValue(1), false},
		{"number is not string", `1`, structpb.NewStringValue("1"), false},
		{"string by code point", `"café"`, structpb.NewStringValue("café"), true},
		{"no normalisation", `"café"`, structpb.NewStringValue("café"), false},
		{"no case folding", `"PAID"`, structpb.NewStringValue("paid"), false},
		{"true", `true`, structpb.NewBoolValue(true), true},
		{"false is not true", `false`, structpb.NewBoolValue(true), false},
		{"null", `null`, structpb.NewNullValue(), true},
		{"null is not false", `null`, structpb.NewBoolValue(false), false},
		{"object equals nothing", `{}`, structpb.NewStringValue("{}"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalJSON([]byte(tt.selected), tt.literal); got != tt.want {
				t.Fatalf("equalJSON(%s, %v) = %v, want %v", tt.selected, tt.literal, got, tt.want)
			}
		})
	}
}

func TestMatch_Evaluate(t *testing.T) {
	src := &orkstackv1.EventSource{
		Format: orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Match: []*orkstackv1.MatchRule{
			{Attribute: "Ce-Type", Equals: structpb.NewStringValue("order.created")},
			{Pointer: "/status", In: []*structpb.Value{structpb.NewStringValue("PAID"), structpb.NewStringValue("SHIPPED")}},
			{Pointer: "/test", Exists: ptr(false)},
			{Field: orkstackv1.MessageField_MESSAGE_FIELD_SUBJECT, Equals: structpb.NewStringValue("orders.eu")},
		},
	}
	msg := Message{
		Subject:    "orders.eu",
		Attributes: map[string][]string{"Ce-Type": {"order.created", "ignored"}},
		Body:       []byte(`{"status":"PAID"}`),
	}
	res := Evaluate(src, msg, Judge{})
	if res.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_ACCEPTED {
		t.Fatalf("outcome %v (%s %s), want accepted", res.Outcome, res.Reason, res.Detail)
	}
	if len(res.Tests) != 4 {
		t.Fatalf("want every test reported, got %d", len(res.Tests))
	}

	msg.Body = []byte(`{"status":"NEW","test":true}`)
	res = Evaluate(src, msg, Judge{})
	if res.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_FILTERED || res.Reason != ReasonMessageNotMatched {
		t.Fatalf("outcome %v %s, want filtered", res.Outcome, res.Reason)
	}
	if !strings.Contains(res.Detail, "match[1]") {
		t.Fatalf("detail %q does not name the first test that did not hold", res.Detail)
	}
	if res.Tests[1].Held || res.Tests[2].Held || !res.Tests[0].Held {
		t.Fatalf("tests %+v", res.Tests)
	}
}

func TestEvaluate_ReportsEachTest(t *testing.T) {
	src := &orkstackv1.EventSource{
		Match: []*orkstackv1.MatchRule{
			{Attribute: "a", Exists: ptr(true)},
			{Attribute: "b", Exists: ptr(true)},
		},
	}
	res := Evaluate(src, Message{Attributes: map[string][]string{"b": {"x"}}}, Judge{})
	if len(res.Tests) != 2 || res.Tests[0].Held || !res.Tests[1].Held {
		t.Fatalf("tests %+v", res.Tests)
	}
	if res.Tests[0].Selector != "attribute:a" || res.Tests[1].Selected != `"x"` {
		t.Fatalf("tests %+v", res.Tests)
	}
}

func TestMatch_NotJSONIsARejectionNotAMiss(t *testing.T) {
	src := &orkstackv1.EventSource{
		Format: orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Match:  []*orkstackv1.MatchRule{{Pointer: "/a", Exists: ptr(false)}},
	}
	res := Evaluate(src, Message{Body: []byte(`{"a":`)}, Judge{})
	if res.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_REJECTED || res.Reason != ReasonMessageNotJSON {
		t.Fatalf("outcome %v %s", res.Outcome, res.Reason)
	}
	if len(res.Tests) != 0 {
		t.Fatal("no test runs on a body that does not parse")
	}
}

func ptr[T any](v T) *T { return &v }
