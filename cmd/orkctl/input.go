package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// buildInputPayload turns --input-json and repeated --input key=value pairs
// into a run Payload. --input entries are applied after --input-json, so an
// explicit pair wins over the same key inside the JSON object.
func buildInputPayload(pairs []string, inputJSON string) (*orkstackv1.Payload, error) {
	values := map[string]any{}

	if trimmed := strings.TrimSpace(inputJSON); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &values); err != nil {
			return nil, fmt.Errorf("invalid --input-json: %w", err)
		}
	}

	for _, pair := range pairs {
		key, raw, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("invalid --input %q: expected key=value", pair)
		}
		if key == "" {
			return nil, fmt.Errorf("invalid --input %q: key must not be empty", pair)
		}
		values[key] = parseInputValue(raw)
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

// parseInputValue types a --input value: valid JSON keeps its JSON type, and
// anything else is preserved verbatim as a string.
func parseInputValue(raw string) any {
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
		return decoded
	}
	return raw
}

// parseStatus maps a --status filter onto the Status enum. It accepts the
// short form (succeeded) and the wire form (STATUS_SUCCEEDED), case
// insensitively; an empty filter means "any status".
func parseStatus(raw string) (orkstackv1.Status, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return orkstackv1.Status_STATUS_UNSPECIFIED, nil
	}
	name := strings.ToUpper(trimmed)
	if !strings.HasPrefix(name, "STATUS_") {
		name = "STATUS_" + name
	}
	value, ok := orkstackv1.Status_value[name]
	if !ok || value == int32(orkstackv1.Status_STATUS_UNSPECIFIED) {
		return orkstackv1.Status_STATUS_UNSPECIFIED, fmt.Errorf(
			"invalid --status %q: want one of pending, running, succeeded, failed, canceled, skipped", raw)
	}
	return orkstackv1.Status(value), nil
}
