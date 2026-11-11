package engine

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
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
//
// It runs in workflow code on bytes a history recorded, so it reads them as
// the build that recorded them did (stablejson.DecodeRecorded): a values.json
// an earlier Orker committed with a key written twice, which the Orker now
// refuses, still decides a replayed condition as it did then.
func decodeValues(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	decoded, err := stablejson.DecodeRecorded(raw)
	object, ok := decoded.(map[string]any)
	if err != nil || !ok {
		return map[string]any{}
	}
	return object
}
