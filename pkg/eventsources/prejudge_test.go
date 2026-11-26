package eventsources

import (
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestPrejudge(t *testing.T) {
	src := validSource()
	for _, tc := range []struct {
		name, body, reason string
		filtered           bool
	}{
		{name: "matches", body: `{"status":"PAID","id":"o-1"}`},
		{name: "filtered", body: `{"status":"OPEN","id":"o-1"}`, filtered: true},
		{name: "not JSON is a rejection, not a miss", body: `{"status":`, reason: ReasonMessageNotJSON},
		{name: "too large", body: `{"status":"PAID","pad":"` + strings.Repeat("x", 2<<20) + `"}`, reason: ReasonMessageTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Prejudge(src, Message{ID: "m-1", Body: []byte(tc.body)}, 0)
			switch {
			case tc.reason != "":
				if got.Rejection == nil || got.Rejection.Reason != tc.reason || !IsPreRejection(tc.reason) {
					t.Fatalf("%+v; want rejection %s", got, tc.reason)
				}
			case tc.filtered:
				if !got.Filtered || got.Failing == "" || got.Rejection != nil {
					t.Fatalf("%+v; want filtered", got)
				}
			default:
				if got.Filtered || got.Rejection != nil || string(got.Values["order_id"]) != `"o-1"` {
					t.Fatalf("%+v", got)
				}
			}
		})
	}
	if got := Prejudge(src, Message{ID: strings.Repeat("i", 300), Body: []byte(`{}`)}, 0); got.Rejection == nil ||
		got.Rejection.Reason != ReasonMessageIdentityInvalid {
		t.Fatalf("an identity over its bound: %+v", got)
	}
}

// TestCheckEnvelope: the bounds of a message whose body is not at hand are
// the ones of a whole message.
func TestCheckEnvelope(t *testing.T) {
	src := validSource()
	src.MaxBytes = 100
	for name, c := range map[string]struct {
		msg    Message
		bytes  int64
		source int64
		most   int64
		reason string
	}{
		"within":                {Message{ID: "m-1"}, 100, 100, 0, ""},
		"over the source's":     {Message{ID: "m-1"}, 101, 100, 0, ReasonMessageTooLarge},
		"over the deployment's": {Message{ID: "m-1"}, 51, 0, 50, ReasonMessageTooLarge},
		"over the default":      {Message{ID: "m-1"}, DefaultMaxMessageBytes + 1, 0, 0, ReasonMessageTooLarge},
		"an identity":           {Message{ID: "a\nb"}, 1, 100, 0, ReasonMessageIdentityInvalid},
		"a subject":             {Message{ID: "m-1", Subject: "\xff"}, 1, 100, 0, ReasonMessageTextInvalid},
		"attributes":            {Message{ID: "m-1", Attributes: map[string][]string{"a": {strings.Repeat("x", MaxAttributesBytes)}}}, 1, 100, 0, ReasonMessageAttributesTooLarge},
	} {
		src.MaxBytes = c.source
		got := ""
		if err := CheckEnvelope(src, c.msg, c.bytes, c.most); err != nil {
			got = err.Reason
		}
		if got != c.reason {
			t.Errorf("%s: %q; want %q", name, got, c.reason)
		}
	}
}

// TestJudgeExtracted: what an Orker extracted is typed as a whole message's
// values are, and nothing it adds is read.
func TestJudgeExtracted(t *testing.T) {
	src := validSource()
	inputs := map[string]Input{"order_id": {Type: "string", Required: true}}
	for name, c := range map[string]struct {
		extracted map[string]*structpb.Value
		judge     Judge
		reason    string
		want      string
	}{
		"a value":              {map[string]*structpb.Value{"order_id": structpb.NewStringValue("o-1")}, Judge{Inputs: inputs}, "", `"o-1"`},
		"a name not mapped":    {map[string]*structpb.Value{"order_id": structpb.NewStringValue("o-1"), "mode": structpb.NewStringValue("x")}, Judge{Inputs: inputs}, "", `"o-1"`},
		"none":                 {nil, Judge{Inputs: inputs}, ReasonMessageFieldMissing, ""},
		"null":                 {map[string]*structpb.Value{"order_id": structpb.NewNullValue()}, Judge{Inputs: inputs}, ReasonMessageFieldMissing, ""},
		"another type":         {map[string]*structpb.Value{"order_id": structpb.NewNumberValue(4)}, Judge{Inputs: inputs}, ReasonMessageFieldType, ""},
		"no kind":              {map[string]*structpb.Value{"order_id": {}}, Judge{Inputs: inputs}, ReasonMessageUnprocessable, ""},
		"nil":                  {map[string]*structpb.Value{"order_id": nil}, Judge{Inputs: inputs}, ReasonMessageUnprocessable, ""},
		"not a number":         {map[string]*structpb.Value{"order_id": structpb.NewNumberValue(math.NaN())}, Judge{}, ReasonMessageUnprocessable, ""},
		"a number beyond JSON": {map[string]*structpb.Value{"order_id": structpb.NewNumberValue(math.Inf(1))}, Judge{}, ReasonMessageUnprocessable, ""},
		"over the input bound": {map[string]*structpb.Value{"order_id": structpb.NewStringValue("o-1")}, Judge{Inputs: inputs, MaxInputBytes: 4}, ReasonMessageInputTooLarge, ""},
	} {
		input, err := JudgeExtracted(src, c.extracted, c.judge)
		if c.reason != "" {
			if err == nil || err.Reason != c.reason {
				t.Errorf("%s: %v; want %s", name, err, c.reason)
			}
			continue
		}
		if err != nil || string(input["order_id"]) != c.want {
			t.Errorf("%s: %s, %v; want %s", name, input["order_id"], err, c.want)
		}
		if _, ok := input["mode"]; ok && src.GetMap()["mode"] == nil && src.GetInput().GetData().GetFields()["mode"] == nil {
			t.Errorf("%s: a name the source does not map was read", name)
		}
	}
}

// FuzzPrejudgeAgreesWithEvaluate: the Orker's half and the engine's
// evaluation, by one package, never disagree on what they both decide.
func FuzzPrejudgeAgreesWithEvaluate(f *testing.F) {
	for _, seed := range []string{`{"status":"PAID","id":"o-1"}`, `{"status":"OPEN"}`, `{`, `[]`, `{"status":"PAID","id":{"a":1}}`} {
		f.Add(seed, "m-1")
	}
	src := validSource()
	inputs := map[string]Input{"order_id": {Type: "string"}}
	f.Fuzz(func(t *testing.T, body, id string) {
		msg := Message{ID: id, Body: []byte(body)}
		pre := Prejudge(src, msg, 0)
		ev := Evaluate(src, msg, Judge{Inputs: inputs})
		switch {
		case pre.Rejection != nil:
			if !IsPreRejection(pre.Rejection.Reason) {
				t.Fatalf("an Orker rejects for %s, which the engine does not take from one", pre.Rejection.Reason)
			}
			if ev.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_REJECTED || ev.Reason != pre.Rejection.Reason {
				t.Fatalf("prejudge rejected %s; evaluate said %v %s", pre.Rejection.Reason, ev.Outcome, ev.Reason)
			}
		case pre.Filtered:
			if ev.Outcome != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_FILTERED {
				t.Fatalf("prejudge filtered; evaluate said %v", ev.Outcome)
			}
		default:
			if ev.Outcome == orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_FILTERED {
				t.Fatal("prejudge passed a message evaluate filtered")
			}
		}
	})
}
