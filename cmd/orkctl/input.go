package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// buildInputPayload turns --input-json and repeated --input key=value pairs
// into a run Payload. --input entries are applied after --input-json, so an
// explicit pair wins over the same key inside the JSON object.
func buildInputPayload(pairs []string, inputJSON string) (*orkstackv1.Payload, error) {
	values, err := decodeInputObject(inputJSON)
	if err != nil {
		return nil, err
	}

	for _, pair := range pairs {
		key, raw, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("invalid --input %q: expected key=value", pair)
		}
		if key == "" {
			return nil, fmt.Errorf("invalid --input %q: key must not be empty", pair)
		}
		value, err := parseInputValue(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid --input %q: %w", pair, err)
		}
		values[key] = value
	}

	if len(values) == 0 {
		return nil, nil
	}
	data, err := structpb.NewStruct(values)
	if err != nil {
		return nil, fmt.Errorf("build run input: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// decodeInputObject reads --input-json: one JSON object, or nothing at all
// when the flag is blank. It refuses what a decoder could read more than one
// way (text that is not UTF-8, half of a surrogate pair, a key written twice)
// and JSON null, which is not an object and would otherwise read as an empty
// one.
func decodeInputObject(text string) (map[string]any, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	if err := dsl.CheckJSONText([]byte(trimmed)); err != nil {
		return nil, fmt.Errorf("invalid --input-json: %w", err)
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(trimmed), &object); err != nil {
		return nil, fmt.Errorf("invalid --input-json: %w", stablejson.Explained(err))
	}
	if object == nil {
		return nil, errors.New("invalid --input-json: want a JSON object, not null")
	}
	return object, nil
}

// parseInputValue types a --input value: valid JSON keeps its JSON type, and
// anything else is preserved verbatim as a string, including text whose JSON
// reading would change it (see decodeInputObject). JSON that writes a key
// twice is refused, naming the key: it is plainly meant as JSON, and which of
// the two values it means is not something to guess.
func parseInputValue(raw string) (any, error) {
	if dsl.CheckJSONText([]byte(raw)) == nil {
		var decoded any
		err := json.Unmarshal([]byte(raw), &decoded)
		if err == nil {
			return decoded, nil
		}
		if errors.Is(err, jsontext.ErrDuplicateName) {
			return nil, stablejson.Explained(err)
		}
	}
	return raw, nil
}
