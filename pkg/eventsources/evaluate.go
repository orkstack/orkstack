package eventsources

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The fixed bounds of a message (docs/EVENT_SOURCES.md, "Bounds").
const (
	MaxIdentityBytes      = 256
	MaxSubjectBytes       = 1 << 10
	MaxAttributeNameBytes = 256
	MaxAttributesBytes    = 64 << 10
	// DefaultMaxInputBytes is event_sources.max_input_bytes' default.
	DefaultMaxInputBytes = 16 << 10
	// DefaultMaxMessageBytes is event_sources.max_message_bytes' default.
	DefaultMaxMessageBytes = 1 << 20
)

// Message is one message as its image reported it.
type Message struct {
	ID          string
	Subject     string
	Group       string
	PublishedAt time.Time
	// Attributes are the headers or attributes, each with its values in
	// order.
	Attributes map[string][]string
	Body       []byte
}

// Judge is what the engine judges a message against beside the source: the
// Orkflow's declared inputs of the version resolved for it, and the
// deployment's bounds. With Inputs nil, nothing is typed: the mapped values
// are what the selectors extracted.
type Judge struct {
	Inputs map[string]Input
	// MaxInputBytes bounds the mapped run input; 0 is DefaultMaxInputBytes.
	MaxInputBytes int
	// MaxMessageBytes is the deployment's body bound, for a source that
	// sets none; 0 is DefaultMaxMessageBytes.
	MaxMessageBytes int64
}

// Result is what a message would come to.
type Result struct {
	Outcome orkstackv1.EvaluationOutcome
	Reason  string
	Detail  string
	Tests   []TestResult
	// Input is the run input it would start with: the constants, then the
	// mapped values that fit.
	Input map[string]jsontext.Value
	// Artifacts are the artifact inputs bound to its parts.
	Artifacts []string
}

func rejected(err *MessageError) Result {
	return Result{Outcome: orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_REJECTED, Reason: err.Reason, Detail: err.Error()}
}

// Evaluate runs a source's bounds, parse, filter and mapping over one
// message and says what would happen, with the package the Orker uses. It
// is pure: it starts nothing and writes nothing.
func Evaluate(src *orkstackv1.EventSource, msg Message, j Judge) Result {
	if err := checkMessage(src, msg, j); err != nil {
		return rejected(err)
	}
	sel, err := readBody(src, msg)
	if err != nil {
		return rejected(err)
	}
	res := Result{Tests: runTests(src.GetMatch(), msg, sel)}
	if failed, ok := firstFailing(res.Tests); ok {
		res.Outcome = orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_FILTERED
		res.Reason = ReasonMessageNotMatched
		res.Detail = describeTest(failed)
		return res
	}
	extracted, err := extract(src, msg, sel)
	if err != nil {
		out := rejected(err)
		out.Tests = res.Tests
		return out
	}
	input, err := judge(src, extracted, j)
	if err != nil {
		out := rejected(err)
		out.Tests = res.Tests
		return out
	}
	res.Outcome = orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_ACCEPTED
	res.Input = input
	res.Artifacts = sortedKeys(src.GetArtifacts())
	return res
}

// checkMessage applies the bounds checked before anything is parsed.
func checkMessage(src *orkstackv1.EventSource, msg Message, j Judge) *MessageError {
	return CheckEnvelope(src, msg, int64(len(msg.Body)), j.MaxMessageBytes)
}

// MaxBodyBytes is src's bound on a message's body. maxMessageBytes is the
// deployment's, for a source that sets none; 0 is DefaultMaxMessageBytes.
func MaxBodyBytes(src *orkstackv1.EventSource, maxMessageBytes int64) int64 {
	switch {
	case src.GetMaxBytes() > 0:
		return src.GetMaxBytes()
	case maxMessageBytes > 0:
		return maxMessageBytes
	}
	return DefaultMaxMessageBytes
}

// CheckEnvelope holds a message to the bounds checked before anything is
// parsed, where its body is bodyBytes long and need not be at hand.
// maxMessageBytes is the deployment's body bound for a source that sets
// none; 0 is DefaultMaxMessageBytes.
func CheckEnvelope(src *orkstackv1.EventSource, msg Message, bodyBytes, maxMessageBytes int64) *MessageError {
	limit := MaxBodyBytes(src, maxMessageBytes)
	if bodyBytes > limit {
		return &MessageError{Reason: ReasonMessageTooLarge, Offset: -1,
			Detail: fmt.Sprintf("the body is %d bytes; the source's bound is %d", bodyBytes, limit)}
	}
	if len(msg.ID) > MaxIdentityBytes || hasControl(msg.ID) || !utf8.ValidString(msg.ID) {
		return &MessageError{Reason: ReasonMessageIdentityInvalid, Offset: -1,
			Detail: fmt.Sprintf("a message's identity is at most %d bytes of text with no control character", MaxIdentityBytes)}
	}
	if len(msg.Subject) > MaxSubjectBytes || !validText(msg.Subject) {
		return &MessageError{Reason: ReasonMessageTextInvalid, Offset: -1,
			Detail: fmt.Sprintf("a subject is valid UTF-8 of at most %d bytes", MaxSubjectBytes)}
	}
	if len(msg.Group) > MaxSubjectBytes || !validText(msg.Group) {
		return &MessageError{Reason: ReasonMessageTextInvalid, Offset: -1,
			Detail: fmt.Sprintf("a group is valid UTF-8 of at most %d bytes", MaxSubjectBytes)}
	}
	total := 0
	for name, values := range msg.Attributes {
		if len(name) > MaxAttributeNameBytes || !validText(name) {
			return &MessageError{Reason: ReasonMessageTextInvalid, Offset: -1,
				Detail: fmt.Sprintf("an attribute name is valid UTF-8 of at most %d bytes", MaxAttributeNameBytes)}
		}
		total += len(name)
		for _, v := range values {
			total += len(v)
		}
	}
	if total > MaxAttributesBytes {
		return &MessageError{Reason: ReasonMessageAttributesTooLarge, Offset: -1,
			Detail: fmt.Sprintf("the attributes total %d bytes, over %d", total, MaxAttributesBytes)}
	}
	return nil
}

func validText(s string) bool { return utf8.ValidString(s) }

func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return true
		}
	}
	return false
}

// readBody reads a JSON source's body once, selecting every pointer its
// tests and its mapping name. A source of format bytes reads nothing.
func readBody(src *orkstackv1.EventSource, msg Message) (Selected, *MessageError) {
	if src.GetFormat() != orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON {
		return Selected{}, nil
	}
	var pointers []string
	seen := map[string]bool{}
	add := func(p string) {
		if p != "" && !seen[p] {
			seen[p] = true
			pointers = append(pointers, p)
		}
	}
	for _, rule := range src.GetMatch() {
		add(rule.GetPointer())
	}
	for _, name := range sortedKeys(src.GetMap()) {
		add(src.GetMap()[name].GetPointer())
	}
	return ReadJSON(msg.Body, pointers)
}

// JudgeExtracted is the engine's half of a message an Orker prejudged: it
// types what the selectors extracted against the version's declared inputs
// and builds the run input. A name the source does not map is not read.
func JudgeExtracted(src *orkstackv1.EventSource, extracted map[string]*structpb.Value, j Judge) (map[string]jsontext.Value, *MessageError) {
	values := make(map[string]jsontext.Value, len(extracted))
	for name := range src.GetMap() {
		v, ok := extracted[name]
		if !ok {
			continue
		}
		// What JSON cannot write (no kind, a number that is none) no selector
		// extracts.
		raw, err := json.Marshal(v)
		if err != nil || v.GetKind() == nil {
			return nil, &MessageError{Reason: ReasonMessageUnprocessable, Offset: -1, Detail: fmt.Sprintf("the value of input %q is not JSON", name)}
		}
		values[name] = raw
	}
	return judge(src, values, j)
}

// preRejections are the reasons an Orker can find by itself (Prejudge): the
// bounds, the parse and the extraction, a mapped number a run's input would
// change among them. Whether a value is of its input's type is the engine's
// alone to say.
var preRejections = map[string]bool{
	ReasonMessageTooLarge: true, ReasonMessageAttributesTooLarge: true, ReasonMessageIdentityInvalid: true,
	ReasonMessageTextInvalid: true, ReasonMessageNotJSON: true, ReasonMessageJSONTooDeep: true,
	ReasonMessageJSONTooLarge: true, ReasonMessageInputTooLarge: true, ReasonMessageUnprocessable: true, ReasonMessageFieldType: true,
}

// IsPreRejection reports a reason an Orker may reject a message with before
// the engine judges it.
func IsPreRejection(reason string) bool { return preRejections[reason] }

// judge types what was extracted against the declared inputs and builds the
// run input: a value that is absent or null leaves out an input that has a
// default or is not required, and rejects the message otherwise; a value of
// another JSON type rejects it.
func judge(src *orkstackv1.EventSource, extracted map[string]jsontext.Value, j Judge) (map[string]jsontext.Value, *MessageError) {
	input := map[string]jsontext.Value{}
	for name, v := range src.GetInput().GetData().GetFields() {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, &MessageError{Reason: ReasonMessageUnprocessable, Offset: -1, Detail: fmt.Sprintf("constant %q does not encode", name)}
		}
		input[name] = raw
	}
	for _, name := range sortedKeys(src.GetMap()) {
		raw, present := extracted[name]
		if present && string(raw) == "null" {
			present = false
		}
		decl, declared := j.Inputs[name]
		if !present {
			if j.Inputs != nil && declared && decl.Required && !decl.HasDefault {
				return nil, &MessageError{Reason: ReasonMessageFieldMissing, Offset: -1,
					Detail: fmt.Sprintf("required input %q: its selector selected nothing", name)}
			}
			continue
		}
		if j.Inputs != nil && declared && !rawHasType(raw, decl.Type) {
			return nil, &MessageError{Reason: ReasonMessageFieldType, Offset: -1,
				Detail: fmt.Sprintf("input %q is declared %s", name, typeWord(decl.Type))}
		}
		input[name] = raw
	}
	limit := j.MaxInputBytes
	if limit <= 0 {
		limit = DefaultMaxInputBytes
	}
	encoded, err := json.Marshal(input, json.Deterministic(true))
	if err != nil {
		return nil, &MessageError{Reason: ReasonMessageUnprocessable, Offset: -1, Detail: "the mapped input does not encode"}
	}
	if len(encoded) > limit {
		return nil, &MessageError{Reason: ReasonMessageInputTooLarge, Offset: -1,
			Detail: fmt.Sprintf("the mapped input is %d bytes, over %d", len(encoded), limit)}
	}
	return input, nil
}
