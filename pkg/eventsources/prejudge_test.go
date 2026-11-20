package eventsources

import (
	"strings"
	"testing"

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
				if got.Rejection == nil || got.Rejection.Reason != tc.reason {
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
