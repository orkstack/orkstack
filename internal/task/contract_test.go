package task

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// inputWithBlob builds an ExecuteTaskInput whose `with:` block carries one
// string of the given content, repeated to length.
func inputWithBlob(t *testing.T, unit string, length int) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	blob := strings.Repeat(unit, length/len(unit)+1)[:length]
	with, err := structpb.NewStruct(map[string]any{"blob": blob})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.ExecuteTaskInput{TaskId: "t1", RunId: "r1", With: with}
}

// temporalPayloadBytes is what Temporal will actually put in history, measured
// through the same default data converter the SDK uses.
func temporalPayloadBytes(t *testing.T, in *orkstackv1.ExecuteTaskInput) int {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(in)
	if err != nil {
		t.Fatal(err)
	}
	return len(payload.GetData())
}

// TestCheckInputSize_MeasuresWhatTemporalActuallySends is the regression for a
// gate that measured the wrong encoding.
//
// CheckInputSize bounded proto.Marshal — the BINARY encoding — while Temporal's
// default data converter encodes a top-level protobuf message with protojson.
// The repository states this in its own words at internal/engine/plan.go:24-27
// and is the whole reason RunPlan is a plain Go struct. Binary and protojson
// agree closely for plain ASCII, which is why the bug hid; every quote or
// newline in a `with:` value becomes a two-character JSON escape, so the real
// payload can be nearly twice what the gate measured. AGENTS.md §2.2 calls
// 256 KiB a HARD maximum, so a check that can pass something Temporal rejects
// is worse than no check: it produces exactly the "Temporal-side rejection
// nobody can attribute" this function claims to prevent.
func TestCheckInputSize_MeasuresWhatTemporalActuallySends(t *testing.T) {
	tests := []struct {
		name string
		unit string
		// expandsUnderJSON records whether this content is escaped by JSON, so
		// the test states the mechanism rather than only the outcome.
		expandsUnderJSON bool
	}{
		{"plain ascii hides the bug", "a", false},
		{"quotes double under json escaping", `"`, true},
		{"newlines double under json escaping", "\n", true},
		{"backslashes double under json escaping", `\`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Sized just under the limit by BINARY measure, which is what the
			// broken gate consulted.
			in := inputWithBlob(t, tc.unit, MaxActivityInputBytes-2000)
			binary := len(mustMarshal(t, in))
			payload := temporalPayloadBytes(t, in)

			if binary > MaxActivityInputBytes {
				t.Fatalf("test setup: binary size %d already over the limit", binary)
			}
			if tc.expandsUnderJSON && payload <= MaxActivityInputBytes {
				t.Fatalf("test setup: %q did not expand past the limit (binary=%d payload=%d)",
					tc.unit, binary, payload)
			}

			err := CheckInputSize(in)
			if tc.expandsUnderJSON {
				if err == nil {
					t.Fatalf("CheckInputSize accepted an input Temporal will reject: "+
						"binary=%d payload=%d limit=%d; the gate measured the wrong encoding",
						binary, payload, MaxActivityInputBytes)
				}
				return
			}
			if err != nil {
				t.Fatalf("CheckInputSize refused an input that fits: "+
					"binary=%d payload=%d limit=%d: %v", binary, payload, MaxActivityInputBytes, err)
			}
		})
	}
}

// TestCheckInputSize_HonoursThePlanLimit keeps the deployment-configurable
// bound working, measured in the same encoding.
func TestCheckInputSize_HonoursThePlanLimit(t *testing.T) {
	in := inputWithBlob(t, "a", 4096)
	// The bound the check itself uses: deterministic, and never below what
	// Temporal will carry.
	bound, err := PayloadSize(in)
	if err != nil {
		t.Fatal(err)
	}
	if raw := int64(temporalPayloadBytes(t, in)); bound < raw {
		t.Fatalf("the bound %d is below this build's actual payload %d; it would let "+
			"through something Temporal rejects", bound, raw)
	}

	if err := CheckInputSize(in, bound); err != nil {
		t.Fatalf("an input exactly at the plan limit was refused: %v", err)
	}
	err = CheckInputSize(in, bound-1)
	if err == nil {
		t.Fatal("an input over the plan limit was accepted")
	}
	if !strings.Contains(err.Error(), ReasonInputTooLarge) {
		t.Errorf("error %q does not carry the %s reason code", err, ReasonInputTooLarge)
	}
	// A zero or absent plan limit means the hard ceiling, not "no limit".
	if err := CheckInputSize(in, 0); err != nil {
		t.Fatalf("a zero plan limit must fall back to the ceiling, not refuse: %v", err)
	}
}

func mustMarshal(t *testing.T, in *orkstackv1.ExecuteTaskInput) []byte {
	t.Helper()
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
