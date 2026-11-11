package task

import (
	"math"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestPayloadSize_PinnedForTextAnEncoderCouldRespell pins the bound for an
// input whose strings protojson escapes (HTML characters, U+2028, control
// characters, astral text), recorded before the module moved to
// encoding/json/v2. Submit refuses an input over the limit by this number, so
// it must not move with the JSON library.
func TestPayloadSize_PinnedForTextAnEncoderCouldRespell(t *testing.T) {
	with, err := structpb.NewStruct(map[string]any{
		"html": "<script>a && b</script>", "lines": "a b c", "ctrl": "\x01\t\"\\/",
		"astral": "😀é", "nums": []any{1e21, 0.1, math.Copysign(0, -1)}, "nested": map[string]any{"b": nil, "a": []any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := &orkstackv1.ExecuteTaskInput{
		RunId: "ork-1", TaskId: "t", Command: []string{"sh", "-c", "echo '<b>' && true"},
		Env: map[string]string{"Z": "&", "a": " "}, With: with,
	}
	size, err := PayloadSize(in)
	if err != nil {
		t.Fatal(err)
	}
	const pinned = 281
	if size != pinned {
		t.Fatalf("PayloadSize = %d, pinned %d", size, pinned)
	}
}
