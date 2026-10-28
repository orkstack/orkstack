package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/enumname"
)

// This file owns OrkStack's JSON dialect. protojson writes enum values under
// their full proto name ("STATUS_SUCCEEDED"), which is noise in every API
// sample, every jq filter and every UI table: the enum's own type already says
// "status". MarshalJSON drops that prefix; UnmarshalJSON accepts both spellings
// so generated clients (which send the canonical name) and hand-written ones
// (which may send the short form) both work.
//
// The rewrite is descriptor-driven, not textual: it walks the message schema
// alongside the JSON tree and only touches values that sit in an enum-typed
// field. A user payload whose *string* value happens to be "STATUS_SUCCEEDED"
// is left untouched, and field order is preserved so output stays byte-stable
// apart from the enum values themselves.

// MarshalJSON encodes a protobuf message as JSON with enum values written
// without their type prefix: "SUCCEEDED", never "STATUS_SUCCEEDED".
func MarshalJSON(message proto.Message) ([]byte, error) {
	return MarshalJSONOptions(protojson.MarshalOptions{}, message)
}

// MarshalJSONOptions is MarshalJSON with caller-chosen protojson options.
func MarshalJSONOptions(options protojson.MarshalOptions, message proto.Message) ([]byte, error) {
	encoded, err := options.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode json: %w", err)
	}
	rewritten, err := rewriteEnums(encoded, message.ProtoReflect().Descriptor(), enumname.Short)
	if err != nil {
		return nil, fmt.Errorf("encode json: %w", err)
	}
	return rewritten, nil
}

// UnmarshalJSON decodes JSON into message, accepting enum values in the short
// form ("SUCCEEDED"), the proto form ("STATUS_SUCCEEDED") or lower case.
func UnmarshalJSON(data []byte, message proto.Message) error {
	return UnmarshalJSONOptions(protojson.UnmarshalOptions{}, data, message)
}

// UnmarshalJSONOptions is UnmarshalJSON with caller-chosen protojson options.
func UnmarshalJSONOptions(options protojson.UnmarshalOptions, data []byte, message proto.Message) error {
	// A rewrite failure means the input is not well-formed JSON. Hand the
	// original bytes to protojson so the caller sees protojson's diagnostics
	// rather than ours.
	if rewritten, err := rewriteEnums(data, message.ProtoReflect().Descriptor(), enumname.Proto); err == nil {
		data = rewritten
	}
	return options.Unmarshal(data, message)
}

// renameEnumValue maps one JSON enum value name to another.
type renameEnumValue func(enum protoreflect.EnumDescriptor, name string) string

// rewriteEnums re-encodes a JSON document, renaming every value that lands in
// an enum-typed field of md. Everything else, including key order, survives.
//
// Survives means unchanged in meaning: a document that Go's decoder would
// quietly change on the way through (a string with bytes that are not UTF-8,
// or with half of a surrogate pair, both of which it turns into U+FFFD) is
// refused here, so the caller hands its original bytes to protojson, which
// refuses them too. A run's input or a caller's name is never silently
// altered by the dialect.
func rewriteEnums(data []byte, md protoreflect.MessageDescriptor, rename renameEnumValue) ([]byte, error) {
	if err := dsl.CheckJSONText(data); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	node, err := decodeJSON(decoder)
	if err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	// The decoder reads one value and would leave whatever follows it unread;
	// the rewrite would then drop it and protojson, handed the rewrite, accept
	// "{}0" as "{}". Anything after the value is for protojson to refuse.
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode json: data after the JSON value")
	}
	rewriteMessage(node, md, rename)
	var buf bytes.Buffer
	if err := encodeJSON(&buf, node); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// rewriteMessage renames enum values inside one JSON object, guided by md.
func rewriteMessage(node any, md protoreflect.MessageDescriptor, rename renameEnumValue) {
	object, ok := node.(*jsonObject)
	if !ok || md == nil {
		return
	}
	// Well-known types (Struct, Value, Timestamp, Any, ...) have bespoke JSON
	// forms that do not mirror their fields, so walking them by descriptor
	// would misread user data.
	if strings.HasPrefix(string(md.FullName()), "google.protobuf.") {
		return
	}
	fields := md.Fields()
	for i := range object.fields {
		field := fields.ByJSONName(object.fields[i].name)
		if field == nil {
			field = fields.ByTextName(object.fields[i].name)
		}
		if field == nil {
			continue
		}
		object.fields[i].value = rewriteField(object.fields[i].value, field, rename)
	}
}

// rewriteField dispatches on the field's cardinality.
func rewriteField(node any, field protoreflect.FieldDescriptor, rename renameEnumValue) any {
	switch {
	case field.IsMap():
		entries, ok := node.(*jsonObject)
		if !ok {
			return node
		}
		value := field.MapValue()
		for i := range entries.fields {
			entries.fields[i].value = rewriteSingular(entries.fields[i].value, value, rename)
		}
		return node
	case field.IsList():
		items, ok := node.([]any)
		if !ok {
			return node
		}
		for i := range items {
			items[i] = rewriteSingular(items[i], field, rename)
		}
		return node
	default:
		return rewriteSingular(node, field, rename)
	}
}

// rewriteSingular renames one enum value or descends into one nested message.
func rewriteSingular(node any, field protoreflect.FieldDescriptor, rename renameEnumValue) any {
	switch field.Kind() {
	case protoreflect.EnumKind:
		name, ok := node.(string)
		if !ok {
			return node
		}
		return rename(field.Enum(), name)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		rewriteMessage(node, field.Message(), rename)
		return node
	default:
		return node
	}
}

// ---------------------------------------------------------------------------
// Order-preserving JSON tree
// ---------------------------------------------------------------------------

// jsonObject is a JSON object that remembers the order its keys arrived in.
// encoding/json's map[string]any sorts keys on the way out, which would
// scramble protojson's field-number ordering for every message we touch.
type jsonObject struct {
	fields []jsonField
}

type jsonField struct {
	name  string
	value any
}

// decodeJSON reads one JSON value, preserving object key order and number text.
func decodeJSON(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil // nil, bool, string or json.Number
	}
	switch delim {
	case '{':
		object := &jsonObject{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("object key %v is not a string", key)
			}
			value, err := decodeJSON(decoder)
			if err != nil {
				return nil, err
			}
			object.fields = append(object.fields, jsonField{name: name, value: value})
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		items := []any{}
		for decoder.More() {
			item, err := decodeJSON(decoder)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unexpected json delimiter %q", delim)
	}
}

// encodeJSON writes a decoded tree back out compactly.
func encodeJSON(buf *bytes.Buffer, node any) error {
	switch value := node.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		buf.WriteString(strconv.FormatBool(value))
	case json.Number:
		buf.WriteString(value.String())
	case string:
		return encodeJSONString(buf, value)
	case []any:
		buf.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case *jsonObject:
		buf.WriteByte('{')
		for i, field := range value.fields {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeJSONString(buf, field.name); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := encodeJSON(buf, field.value); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("encode json: unsupported value %T", node)
	}
	return nil
}

// encodeJSONString writes a quoted string without HTML escaping, matching
// protojson (encoding/json would turn "<" into "\u003c").
func encodeJSONString(buf *bytes.Buffer, value string) error {
	var quoted bytes.Buffer
	encoder := json.NewEncoder(&quoted)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	buf.Write(bytes.TrimRight(quoted.Bytes(), "\n"))
	return nil
}
