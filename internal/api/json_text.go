package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// jsonWhitespace is the four bytes JSON allows between tokens (RFC 8259 §2).
// A body wrapped in any other Unicode space (U+00A0, U+0085) is not JSON.
const jsonWhitespace = " \t\r\n"

// checkJSONText reports why Go's JSON decoder could not read data without
// changing a string in it: data is not valid UTF-8, or a string holds a
// unicode escape of a surrogate that is not half of a pair. Go's decoder
// replaces both with U+FFFD, so a value a caller chose (a run's input, a
// signing secret) would be stored as something they did not send. It looks
// at nothing else: a document that is not JSON at all is for the decoder to
// refuse.
func checkJSONText(data []byte) error {
	if !utf8.Valid(data) {
		return errors.New("not valid UTF-8")
	}
	inString := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if !inString {
			inString = c == '"'
			continue
		}
		switch c {
		case '"':
			inString = false
		case '\\':
			i++
			if i >= len(data) || data[i] != 'u' {
				continue
			}
			unit, ok := hexUnit(data, i+1)
			if !ok {
				return nil // malformed: the decoder says so
			}
			i += 4
			switch {
			case unit >= 0xDC00 && unit < 0xE000:
				return halfPair(data[i-5 : i+1])
			case unit >= 0xD800 && unit < 0xDC00:
				low, ok := pairedUnit(data, i+1)
				if !ok || low < 0xDC00 || low >= 0xE000 {
					return halfPair(data[i-5 : i+1])
				}
				i += 6
			}
		}
	}
	return nil
}

func halfPair(escape []byte) error {
	return fmt.Errorf("the escape %s is half of a surrogate pair, not a character", escape)
}

// hexUnit reads the four hex digits at data[at:] as a UTF-16 code unit.
func hexUnit(data []byte, at int) (rune, bool) {
	if at+4 > len(data) {
		return 0, false
	}
	var unit rune
	for _, c := range data[at : at+4] {
		switch {
		case c >= '0' && c <= '9':
			unit = unit<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			unit = unit<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			unit = unit<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return unit, true
}

// pairedUnit reads a unicode escape that follows at data[at:].
func pairedUnit(data []byte, at int) (rune, bool) {
	if at+2 > len(data) || data[at] != '\\' || data[at+1] != 'u' {
		return 0, false
	}
	return hexUnit(data, at+2)
}

// decodeJSONValue reads raw as exactly one JSON value into v, for a plain-HTTP
// body that is a Go struct and not a proto message. It refuses what a bare
// json.Decoder would quietly change or ignore: text that is not UTF-8 and a
// lone surrogate escape (both would become U+FFFD in a field the caller
// chose) and anything after the value.
func decodeJSONValue(raw []byte, v any, disallowUnknownFields bool) error {
	if err := checkJSONText(raw); err != nil {
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
