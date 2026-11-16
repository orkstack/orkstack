package eventsources

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Error is a source refused for what it says: one reason, the field it is
// about, and OrkStack's sentence.
type Error struct {
	Reason  string
	Field   string
	Message string
}

func (e *Error) Error() string {
	if e.Field == "" {
		return e.Reason + ": " + e.Message
	}
	return fmt.Sprintf("%s: %s: %s", e.Reason, e.Field, e.Message)
}

func mappingError(field, format string, args ...any) *Error {
	return &Error{Reason: ReasonMappingInvalid, Field: field, Message: fmt.Sprintf(format, args...)}
}

// Input is one declared input of an Orkflow as a source reads it: its type
// word ("" for untyped, "artifact" for an artifact input), whether a run
// must be given it, and whether it has a default.
type Input struct {
	Type       string
	Required   bool
	HasDefault bool
}

// TypeArtifact is the type word of an artifact input.
const TypeArtifact = "artifact"

// CheckMapping holds a source's input, map and artifacts to the Orkflow's
// declared inputs (docs/EVENT_SOURCES.md, "Mapping"): every name is
// declared and appears under one key only; a map or input name is a value
// input and an artifacts name an artifact input; every artifact input is
// bound; attribute and field selectors fill only string inputs; a pointer
// needs format JSON; a constant has the declared type; every required value
// input without a default is covered.
func CheckMapping(src *orkstackv1.EventSource, inputs map[string]Input) error {
	seen := map[string]string{}
	constants := src.GetInput().GetData().GetFields()
	for _, name := range sortedKeys(constants) {
		field := "input." + name
		decl, ok := inputs[name]
		switch {
		case !ok:
			return mappingError(field, "the Orkflow declares no input %q", name)
		case decl.Type == TypeArtifact:
			return mappingError(field, "input %q is an artifact input; bind it under artifacts", name)
		case !valueHasType(constants[name], decl.Type):
			return mappingError(field, "input %q is declared %s", name, decl.Type)
		}
		seen[name] = "input"
	}
	for _, name := range sortedKeys(src.GetMap()) {
		field := "map." + name
		rule := src.GetMap()[name]
		decl, ok := inputs[name]
		if !ok {
			return mappingError(field, "the Orkflow declares no input %q", name)
		}
		if other, dup := seen[name]; dup {
			return mappingError(field, "input %q is filled under %s too; name it under one key only", name, other)
		}
		if decl.Type == TypeArtifact {
			return mappingError(field, "input %q is an artifact input; bind it under artifacts", name)
		}
		if err := checkSelector(field, rule.GetAttribute(), rule.GetPointer(), rule.GetField(), src.GetFormat()); err != nil {
			return err
		}
		if rule.GetPointer() == "" && decl.Type != "string" {
			return mappingError(field, "an attribute or a field gives a string, and input %q is declared %s", name, typeWord(decl.Type))
		}
		seen[name] = "map"
	}
	for _, name := range sortedKeys(src.GetArtifacts()) {
		field := "artifacts." + name
		decl, ok := inputs[name]
		switch {
		case !ok:
			return mappingError(field, "the Orkflow declares no input %q", name)
		case decl.Type != TypeArtifact:
			return mappingError(field, "input %q is not an artifact input", name)
		case seen[name] != "":
			return mappingError(field, "input %q is filled under %s too", name, seen[name])
		}
		switch src.GetArtifacts()[name] {
		case orkstackv1.MessagePart_MESSAGE_PART_BODY, orkstackv1.MessagePart_MESSAGE_PART_METADATA:
		default:
			return mappingError(field, "a part is body or metadata")
		}
		seen[name] = "artifacts"
	}
	for _, name := range sortedKeys(inputs) {
		decl := inputs[name]
		if seen[name] != "" {
			continue
		}
		if decl.Type == TypeArtifact {
			return mappingError("artifacts."+name, "artifact input %q is not bound to a part of the message", name)
		}
		if decl.Required && !decl.HasDefault {
			return mappingError("map."+name, "required input %q is neither mapped nor given a constant", name)
		}
	}
	return nil
}

func typeWord(t string) string {
	if t == "" {
		return "untyped"
	}
	return t
}

// checkSelector holds a selector to having exactly one of its three
// readers, and a pointer to a JSON source and to RFC 6901.
func checkSelector(field, attribute, pointer string, msgField orkstackv1.MessageField, format orkstackv1.MessageFormat) *Error {
	n := 0
	if attribute != "" {
		n++
	}
	if pointer != "" {
		n++
	}
	if msgField != orkstackv1.MessageField_MESSAGE_FIELD_UNSPECIFIED {
		n++
	}
	if n != 1 {
		return &Error{Reason: ReasonMappingInvalid, Field: field, Message: "a selector is exactly one of pointer, attribute and field"}
	}
	if pointer != "" {
		if format != orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON {
			return &Error{Reason: ReasonMappingInvalid, Field: field, Message: "pointer reads the body, which needs format: json"}
		}
		if _, err := ParsePointer(pointer); err != nil {
			return &Error{Reason: ReasonMappingInvalid, Field: field, Message: err.Error()}
		}
	}
	if attribute != "" && (len(attribute) > MaxAttributeNameBytes || !validText(attribute)) {
		return &Error{Reason: ReasonMappingInvalid, Field: field, Message: fmt.Sprintf("an attribute name is valid UTF-8 of at most %d bytes", MaxAttributeNameBytes)}
	}
	if _, known := orkstackv1.MessageField_name[int32(msgField)]; !known {
		return &Error{Reason: ReasonMappingInvalid, Field: field, Message: "field is subject, id, published_at or group"}
	}
	return nil
}

// valueHasType reports whether a constant is of a declared type; an untyped
// input takes anything.
func valueHasType(v *structpb.Value, t string) bool {
	switch t {
	case "":
		return true
	case "string":
		_, ok := v.GetKind().(*structpb.Value_StringValue)
		return ok
	case "number":
		_, ok := v.GetKind().(*structpb.Value_NumberValue)
		return ok
	case "boolean":
		_, ok := v.GetKind().(*structpb.Value_BoolValue)
		return ok
	case "object":
		_, ok := v.GetKind().(*structpb.Value_StructValue)
		return ok
	case "array":
		_, ok := v.GetKind().(*structpb.Value_ListValue)
		return ok
	}
	return true
}

// rawHasType reports whether a JSON value's text is of a declared type.
func rawHasType(raw []byte, t string) bool {
	if len(raw) == 0 {
		return false
	}
	c := raw[0]
	switch t {
	case "":
		return true
	case "string":
		return c == '"'
	case "number":
		return c == '-' || (c >= '0' && c <= '9')
	case "boolean":
		return c == 't' || c == 'f'
	case "object":
		return c == '{'
	case "array":
		return c == '['
	}
	return true
}

// Extract is what each map selector selects in msg, as JSON text: what the
// Orker does for every message, with no knowledge of the Orkflow. A selector
// that selected nothing has no entry. It reads the body only for a JSON
// source, and a body that does not parse is the message's rejection.
func Extract(src *orkstackv1.EventSource, msg Message) (map[string]jsontext.Value, *MessageError) {
	sel, err := readBody(src, msg)
	if err != nil {
		return nil, err
	}
	return extract(src, msg, sel)
}

func extract(src *orkstackv1.EventSource, msg Message, sel Selected) (map[string]jsontext.Value, *MessageError) {
	out := map[string]jsontext.Value{}
	for _, name := range sortedKeys(src.GetMap()) {
		rule := src.GetMap()[name]
		if rule.GetPointer() != "" && sel.TooLarge(rule.GetPointer()) {
			return nil, &MessageError{Reason: ReasonMessageInputTooLarge, Offset: -1,
				Detail: fmt.Sprintf("the value mapped to input %q is over %d bytes", name, MaxSelectedBytes)}
		}
		if raw, ok := selectValue(rule.GetAttribute(), rule.GetPointer(), rule.GetField(), msg, sel); ok {
			if len(raw) > MaxSelectedBytes {
				return nil, &MessageError{Reason: ReasonMessageInputTooLarge, Offset: -1,
					Detail: fmt.Sprintf("the value mapped to input %q is over %d bytes", name, MaxSelectedBytes)}
			}
			out[name] = raw
		}
	}
	return out, nil
}

// selectValue is what one selector selects, as JSON text.
func selectValue(attribute, pointer string, field orkstackv1.MessageField, msg Message, sel Selected) ([]byte, bool) {
	switch {
	case attribute != "":
		values := msg.Attributes[attribute]
		if len(values) == 0 {
			return nil, false
		}
		return jsonString(values[0]), true
	case field != orkstackv1.MessageField_MESSAGE_FIELD_UNSPECIFIED:
		var s string
		switch field {
		case orkstackv1.MessageField_MESSAGE_FIELD_SUBJECT:
			s = msg.Subject
		case orkstackv1.MessageField_MESSAGE_FIELD_ID:
			s = msg.ID
		case orkstackv1.MessageField_MESSAGE_FIELD_GROUP:
			s = msg.Group
		case orkstackv1.MessageField_MESSAGE_FIELD_PUBLISHED_AT:
			if !msg.PublishedAt.IsZero() {
				s = msg.PublishedAt.UTC().Format(time.RFC3339Nano)
			}
		}
		if s == "" {
			return nil, false
		}
		return jsonString(s), true
	default:
		return sel.Value(pointer)
	}
}

func jsonString(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		// A Go string always encodes; invalid UTF-8 is replaced, never refused.
		return []byte(`""`)
	}
	return b
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
