package api

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// jsonWhitespace is the four bytes JSON allows between tokens (RFC 8259 §2).
// A body wrapped in any other Unicode space (U+00A0, U+0085) is not JSON.
const jsonWhitespace = " \t\r\n"

// decodeJSONValue reads raw as exactly one JSON value into v, for a plain-HTTP
// body that is a Go struct and not a proto message. It refuses what a decoder
// could read more than one way: text that is not UTF-8 and a lone surrogate
// escape (both would become U+FFFD in a field the caller chose), an object
// that writes a key twice, and anything after the value. A field name
// matches exactly, case included; with disallowUnknownFields a key no field
// takes is refused, naming it.
func decodeJSONValue(raw []byte, v any, disallowUnknownFields bool) error {
	if err := dsl.CheckJSONText(raw); err != nil {
		return err
	}
	var opts []json.Options
	if disallowUnknownFields {
		opts = append(opts, json.RejectUnknownMembers(true))
	}
	err := json.Unmarshal(raw, v, opts...)
	var syntactic *jsontext.SyntacticError
	if errors.As(err, &syntactic) {
		return fmt.Errorf("not one valid JSON value: %w", stablejson.Explained(err))
	}
	if err != nil {
		return stablejson.Explained(err)
	}
	if !disallowUnknownFields {
		return stablejson.RefuseCaseOnlyMatch(raw, v)
	}
	return nil
}
