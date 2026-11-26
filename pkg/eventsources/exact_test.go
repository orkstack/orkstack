package eventsources

import (
	"encoding/json/jsontext"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestHoldsExactly: a value a run's input holds is one whose every number reads
// back, from the double it becomes, as the number the message wrote.
func TestHoldsExactly(t *testing.T) {
	for raw, want := range map[string]bool{
		`"text"`: true, `null`: true, `true`: true, `[]`: true, `{}`: true,
		`0`: true, `-0`: true, `0.0`: true, `1`: true, `0.1`: true, `1.50`: true, `1e2`: true, `-12.5e-3`: true,
		`9007199254740992`: true, `9007199254740993`: false, `123456789012345678901234567890`: false, `1e308`: true, `1e400`: false,
		`1e-400`: false, `0.30000000000000004`: true, `0.1000000000000000055511151231257827`: false, `1e999999999999999999999`: false,
		`[1,{"a":[2,9007199254740993]}]`: false, `{"a":{"b":0.1}}`: true, `{"9007199254740993":"x"}`: true,
		// What is not one JSON value is nothing an input holds.
		``: false, `[1`: false, `"\ud800"`: false, `1 x`: false,
	} {
		if got := holdsExactly(jsontext.Value(raw)); got != want {
			t.Errorf("holdsExactly(%s) = %v, want %v", raw, got, want)
		}
	}
}

// TestPrejudge_ANumberNoInputHolds: a mapped number a run's input would change
// rejects its message, in the Orker's half and in an evaluation alike.
func TestPrejudge_ANumberNoInputHolds(t *testing.T) {
	src := validSource()
	for body, reason := range map[string]string{
		`{"status":"PAID","id":9007199254740993}`: ReasonMessageFieldType,
		`{"status":"PAID","id":9007199254740992}`: "",
		// A number nothing maps is the body's own business.
		`{"status":"PAID","id":"o-1","other":9007199254740993}`: "",
	} {
		pre := Prejudge(src, Message{ID: "m-1", Body: []byte(body)}, 0)
		ev := Evaluate(src, Message{ID: "m-1", Body: []byte(body)}, Judge{})
		switch {
		case reason == "":
			if pre.Rejection != nil || ev.Outcome == orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_REJECTED {
				t.Errorf("%s: %+v, %v %s", body, pre.Rejection, ev.Outcome, ev.Reason)
			}
		case pre.Rejection == nil || pre.Rejection.Reason != reason || ev.Reason != reason:
			t.Errorf("%s: %+v, %v %s; want %s", body, pre.Rejection, ev.Outcome, ev.Reason, reason)
		}
	}
}
