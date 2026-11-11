// Package stablejson writes the JSON bytes OrkStack records wherever bytes are
// hashed, compared, stored or read by a container: a definition digest, a memo
// key, a plan digest, an evidence manifest, a workspace's context files, a
// stored record. Those bytes were first written by encoding/json; the module
// now uses encoding/json/v2 only, and these options make v2 write exactly what
// v1 wrote, so no digest moves and no stored object reads differently:
//
//   - object keys of a map sorted, as v1 sorted them;
//   - a nil slice or map written as null, not [] or {};
//   - <, > and & escaped as <, > and & (Options) or not
//     (Unescaped), and U+2028 and U+2029 always escaped, as v1 always did;
//   - text that is not valid UTF-8 written with U+FFFD in its place, as v1
//     wrote it, rather than refused;
//   - a time.Duration written as an integer of nanoseconds.
//
// Reading keeps encoding/json/v2's own defaults everywhere: a duplicate key is
// refused, a field name matches exactly, text that is not valid UTF-8 is
// refused (docs/JSON.md). Explain words such a refusal, naming the key and
// where it is.
package stablejson

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"strconv"
	"time"
)

// DurationAsNano reads and writes a time.Duration as a JSON integer of
// nanoseconds, as encoding/json did; encoding/json/v2 has no representation
// for one of its own. Options includes it; a decode of a document holding one
// passes it.
var DurationAsNano = json.JoinOptions(
	json.WithMarshalers(json.MarshalToFunc(func(enc *jsontext.Encoder, d time.Duration) error {
		return enc.WriteToken(jsontext.Int(int64(d)))
	})),
	json.WithUnmarshalers(json.UnmarshalFromFunc(func(dec *jsontext.Decoder, d *time.Duration) error {
		var nanos int64
		if err := json.UnmarshalDecode(dec, &nanos); err != nil {
			return err
		}
		*d = time.Duration(nanos)
		return nil
	})),
)

// Options marshals as encoding/json.Marshal did.
var Options = json.JoinOptions(
	DurationAsNano,
	json.Deterministic(true),
	json.FormatNilSliceAsNull(true),
	json.FormatNilMapAsNull(true),
	jsontext.EscapeForHTML(true),
	jsontext.EscapeForJS(true),
	jsontext.AllowInvalidUTF8(true),
)

// Unescaped marshals as an encoding/json.Encoder with SetEscapeHTML(false)
// did: Options without the HTML escapes.
var Unescaped = json.JoinOptions(Options, jsontext.EscapeForHTML(false))

// Marshal is encoding/json.Marshal.
func Marshal(v any) ([]byte, error) {
	return json.Marshal(v, Options)
}

// MarshalIndent is encoding/json.MarshalIndent(v, "", indent).
func MarshalIndent(v any, indent string) ([]byte, error) {
	return json.Marshal(v, Options, jsontext.WithIndent(indent))
}

// Encode is an encoding/json.Encoder's Encode: v marshalled with opts
// (Options or Unescaped, and an indent if wanted) and a line feed after it.
func Encode(w io.Writer, v any, opts ...json.Options) error {
	encoded, err := json.Marshal(v, opts...)
	if err != nil {
		return err
	}
	_, err = w.Write(append(encoded, '\n'))
	return err
}

// Compact is encoding/json.Compact: src with its insignificant whitespace
// removed, appended to dst, strings and numbers exactly as written. It is a
// formatter, not a reader, and accepts what v1's did: text that is not one
// JSON value is refused, a duplicate key is not — a document whose meaning
// matters is decoded, and refused there, before it is compacted.
func Compact(dst *bytes.Buffer, src []byte) error {
	value := jsontext.Value(bytes.Clone(src))
	if err := value.Compact(); err != nil {
		return err
	}
	dst.Write(value)
	return nil
}

// Indent is encoding/json.Indent(dst, src, "", indent), which also kept the
// whitespace src ends with.
func Indent(dst *bytes.Buffer, src []byte, indent string) error {
	value := jsontext.Value(bytes.Clone(src))
	if err := value.Indent(jsontext.WithIndent(indent)); err != nil {
		return err
	}
	dst.Write(value)
	dst.Write(src[len(bytes.TrimRight(src, " \t\r\n")):])
	return nil
}

// RawJSON is an encoded JSON value carried as it is, exactly as
// encoding/json.RawMessage was. It differs from jsontext.Value, which v2
// treats the same way, in one respect that matters: it has MarshalJSON and
// UnmarshalJSON methods, so an encoder that is still encoding/json — the
// Temporal SDK's payload converter, which writes every Go-struct workflow and
// activity argument — writes it as the JSON it holds, as it always has, and
// not as base64. A struct that crosses Temporal holds a RawJSON, never a
// jsontext.Value, or the histories recorded before the move stop decoding.
type RawJSON []byte

// MarshalJSON returns m, or null when m is nil.
func (m RawJSON) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// IsZero reports whether m holds no bytes, which is what encoding/json's
// omitempty omitted for a RawMessage. A RawJSON field is tagged omitzero, never
// omitempty: encoding/json/v2's omitempty looks at the encoded value, so it
// would also drop a RawJSON that holds {}, [], "" or null.
func (m RawJSON) IsZero() bool { return len(m) == 0 }

// UnmarshalJSON sets *m to a copy of data.
func (m *RawJSON) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("stablejson.RawJSON: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

// Number is a JSON number kept as the text it was written as, as
// encoding/json.Number was: a decode with UseNumber reads every number it
// meets inside an any as a Number, and a Number marshals as that text.
type Number string

// String returns the number's text.
func (n Number) String() string { return string(n) }

// Float64 returns the number as a float64.
func (n Number) Float64() (float64, error) { return strconv.ParseFloat(string(n), 64) }

// Int64 returns the number as an int64.
func (n Number) Int64() (int64, error) { return strconv.ParseInt(string(n), 10, 64) }

// MarshalJSON writes the number's text, refusing text that is not a JSON
// number.
func (n Number) MarshalJSON() ([]byte, error) {
	if n == "" {
		return []byte("0"), nil // v1 wrote an empty Number as 0
	}
	if !isNumber(string(n)) {
		return nil, errors.New("stablejson.Number: invalid number literal " + strconv.Quote(string(n)))
	}
	return []byte(n), nil
}

// UseNumber is the option that makes a decode into an any read every JSON
// number, however deep, as a Number, as encoding/json.Decoder.UseNumber did.
var UseNumber = json.WithUnmarshalers(json.UnmarshalFromFunc(func(dec *jsontext.Decoder, v *any) error {
	if dec.PeekKind() != '0' {
		return errors.ErrUnsupported // not a number: decode it as usual
	}
	token, err := dec.ReadToken()
	if err != nil {
		return err
	}
	*v = Number(token.String())
	return nil
}))

// isNumber reports whether s is one JSON number (RFC 8259 §6).
func isNumber(s string) bool {
	value := jsontext.Value(s)
	return len(bytes.TrimSpace(value)) == len(value) && value.IsValid() && value.Kind() == '0'
}
