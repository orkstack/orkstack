package stablejson

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// DecodeRecorded reads one JSON value into an any exactly as
// encoding/json.Unmarshal did: the last of two values for one key wins,
// replacing the first; a string that is not valid UTF-8 or holds half of a
// surrogate pair reads with U+FFFD in its place; a number is a float64, and
// one too large for a float64 is refused, as is anything after the value.
//
// It exists for workflow code alone, which must decide from bytes an earlier
// build recorded exactly as that build did, or a replay diverges (AGENTS.md
// §7.2): a container's inline values.json, an await's resolution values. Every
// other reader uses encoding/json/v2's defaults and refuses what this
// tolerates.
func DecodeRecorded(data []byte) (any, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	value, err := decodeRecorded(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, errors.New("data after the JSON value")
	}
	return value, nil
}

func decodeRecorded(dec *jsontext.Decoder) (any, error) {
	token, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch token.Kind() {
	case 'n':
		return nil, nil
	case 't', 'f':
		return token.Bool(), nil
	case '"':
		return token.String(), nil
	case '0':
		number, err := strconv.ParseFloat(token.String(), 64)
		if err != nil {
			return nil, fmt.Errorf("the number %s does not fit a float64", token.String())
		}
		return number, nil
	case '{':
		object := map[string]any{}
		for dec.PeekKind() != '}' {
			key, err := dec.ReadToken()
			if err != nil {
				return nil, err
			}
			name := key.String() // a token is valid only until the next read
			value, err := decodeRecorded(dec)
			if err != nil {
				return nil, err
			}
			object[name] = value // the last wins, replacing, as encoding/json did
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		list := []any{}
		for dec.PeekKind() != ']' {
			value, err := decodeRecorded(dec)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return list, nil
	default:
		return nil, fmt.Errorf("unexpected JSON token %v", token)
	}
}
