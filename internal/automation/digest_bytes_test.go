package automation

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestDigest_PinnedWithTextAnEncoderCouldRespell pins a plan digest whose
// changes hold HTML characters, U+2028 and non-ASCII text. The value was
// recorded with encoding/json before the module moved to encoding/json/v2: a
// digest a reviewer pinned then still applies now.
func TestDigest_PinnedWithTextAnEncoderCouldRespell(t *testing.T) {
	steps := []*orkstackv1.AutomationStep{
		{Action: actionUpdate, Kind: kindSchedule, Name: "nightly", Id: "sch-1", Changes: []*orkstackv1.AutomationFieldChange{
			{Field: "input", Before: `{"q":"<a>"}`, After: "{\"q\":\"a && b é\"}"},
		}},
		{Action: actionCreate, Kind: kindTrigger, Name: "deploy"},
	}
	const pinned = "sha256:a9f6f748a646f7600477f52c4279b213f315dd06a7a5297909d245a7ba558092"
	if got := Digest("payments", steps); got != pinned {
		t.Fatalf("digest = %q, pinned %q", got, pinned)
	}
}
