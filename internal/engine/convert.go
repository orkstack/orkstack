package engine

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// payloadMap converts a proto payload into the plain map the workflow uses for
// condition evaluation and for the run's aggregated output.
//
// It returns a non-nil map so a missing key is an error rather than the zero
// value an index into a nil map yields.
func payloadMap(p *orkstackv1.Payload) map[string]any {
	if p.GetData() == nil {
		return map[string]any{}
	}
	return p.GetData().AsMap()
}

// payloadOf converts a plain map back into proto form.
func payloadOf(m map[string]any) (*orkstackv1.Payload, error) {
	if len(m) == 0 {
		return &orkstackv1.Payload{}, nil
	}
	data, err := structpb.NewStruct(m)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// decodeValues turns a task's committed values.json bytes into the map
// downstream conditions read as .tasks.<id>.output.
//
// The bytes are the workload's exact bytes and are never re-serialized: this
// decode is for the ENGINE's own reading, and what travels onward is always
// the reference plus, at 4 KiB or less, the original bytes (§3.4, §14.9).
func decodeValues(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return map[string]any{}
	}
	return decoded
}
