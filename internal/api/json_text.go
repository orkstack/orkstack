package api

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/orkstack/orkstack/internal/dsl"
)

// jsonWhitespace is the four bytes JSON allows between tokens (RFC 8259 §2).
// A body wrapped in any other Unicode space (U+00A0, U+0085) is not JSON.
const jsonWhitespace = " \t\r\n"

// decodeJSONValue reads raw as exactly one JSON value into v, for a plain-HTTP
// body that is a Go struct and not a proto message. It refuses what a bare
// json.Decoder would quietly change or ignore: text that is not UTF-8 and a
// lone surrogate escape (both would become U+FFFD in a field the caller
// chose) and anything after the value.
func decodeJSONValue(raw []byte, v any, disallowUnknownFields bool) error {
	if err := dsl.CheckJSONText(raw); err != nil {
		return err
	}
	if !json.Valid(raw) {
		return errors.New("not one valid JSON value")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if disallowUnknownFields {
		decoder.DisallowUnknownFields()
	}
	return decoder.Decode(v)
}
