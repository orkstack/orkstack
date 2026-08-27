package engine_test

import (
	"encoding/json"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// valuesJSON renders a test's intended task output as the bytes an Orker would
// actually return.
//
// It exists because ExecuteTaskResult no longer carries a structured Output.
// It carries values_json: the workload's EXACT bytes, inline only at 4 KiB or
// less. That field is `bytes` rather than a Struct-backed Payload because a
// Struct round trip is lossy in four measured ways — key order is not
// preserved, number formatting is normalized, unicode escapes are decoded, and
// an integer outside float64's exact range is SILENTLY CORRUPTED (§14.9). A
// field that promises byte identity cannot be a Struct.
//
// So a test that wants to say "this task produced {\"stdout\": \"a\"}" has to
// produce the bytes, and this is the one place that happens.
func valuesJSON(values map[string]any) []byte {
	encoded, err := json.Marshal(values)
	if err != nil {
		// A test that cannot encode its own fixture is a broken test, and a
		// panic here names it immediately. This helper is never reached from
		// production code.
		panic("valuesJSON: " + err.Error())
	}
	return encoded
}

// decodeValuesJSON is the inverse, for asserting on what a task returned.
func decodeValuesJSON(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return map[string]any{}
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode values.json %q: %v", raw, err)
	}
	return decoded
}

// succeeded builds the result a healthy Orker returns for one task.
//
// It sets values_ref as well as the inline copy, because those two travel
// together on every real success: values_ref is authoritative and the inline
// bytes exist only for the engine's read model. A fixture that set one without
// the other would let a bug through that production could not produce.
func succeeded(taskID string, values map[string]any) *orkstackv1.ExecuteTaskResult {
	raw := valuesJSON(values)
	return &orkstackv1.ExecuteTaskResult{
		TaskId:        taskID,
		ExitCode:      0,
		ValuesJson:    raw,
		ValuesInlined: true,
		ValuesRef: &orkstackv1.ArtifactRef{
			Name:        "values.json",
			Uri:         "mem://artifacts/values/" + taskID,
			Sha256:      "0000000000000000000000000000000000000000000000000000000000000000",
			SizeBytes:   int64(len(raw)),
			ContentType: "application/json",
		},
		Attempt: 1,
	}
}
