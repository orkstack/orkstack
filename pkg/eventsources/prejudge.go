package eventsources

import (
	"encoding/json/jsontext"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Prejudgement is what the Orker makes of one message before it offers it
// (01 §4.3): the same bounds, parse, filter and extraction as Evaluate, by
// the same code, and no typing, which is the engine's.
type Prejudgement struct {
	// Rejection is a pre-rejection: the message can never be accepted. It
	// is not acted on until the engine has read the message's claim.
	Rejection *MessageError
	// Filtered is a message the source's match: does not select; Failing
	// names the first test that did not hold.
	Filtered bool
	Failing  string
	// Values are what each map: selector selected, as JSON.
	Values map[string]jsontext.Value
	// Tests are the match: tests and how each went.
	Tests []TestResult
}

// Prejudge runs bounds, parse, match and extraction over msg for src, read
// from a judging spec. maxMessageBytes is the deployment's body bound for a
// source that sets none.
func Prejudge(src *orkstackv1.EventSource, msg Message, maxMessageBytes int64) Prejudgement {
	j := Judge{MaxMessageBytes: maxMessageBytes}
	if err := checkMessage(src, msg, j); err != nil {
		return Prejudgement{Rejection: err}
	}
	sel, err := readBody(src, msg)
	if err != nil {
		return Prejudgement{Rejection: err}
	}
	out := Prejudgement{Tests: runTests(src.GetMatch(), msg, sel)}
	if failed, ok := firstFailing(out.Tests); ok {
		out.Filtered, out.Failing = true, describeTest(failed)
		return out
	}
	values, err := extract(src, msg, sel)
	if err != nil {
		out.Rejection = err
		return out
	}
	out.Values = values
	return out
}
