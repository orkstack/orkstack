package main

import (
	"strings"
	"testing"
)

// TestBuildInputPayload_RefusesAKeyWrittenTwice: --input-json, and an --input
// value written as JSON, that write a key twice are refused naming the key and
// the object holding it; encoding/json kept the last value, so the run would
// have started with an input the caller may not have meant.
func TestBuildInputPayload_RefusesAKeyWrittenTwice(t *testing.T) {
	for _, tc := range []struct {
		pairs     []string
		inputJSON string
		want      string
	}{
		{nil, `{"a":1,"a":2}`, `invalid --input-json: the key "a" appears twice in the top-level object`},
		{[]string{`cfg={"x":{"y":1,"y":2}}`}, ``, `invalid --input "cfg={\"x\":{\"y\":1,\"y\":2}}": the key "y" appears twice in the object at /x`},
	} {
		payload, err := buildInputPayload(tc.pairs, tc.inputJSON)
		if err == nil || payload != nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("buildInputPayload(%q, %q) = %v, %v; want %q", tc.pairs, tc.inputJSON, payload, err, tc.want)
		}
	}
	// Text that is not JSON at all is still a string.
	payload, err := buildInputPayload([]string{`note={not json`}, "")
	if err != nil || payload.GetData().GetFields()["note"].GetStringValue() != "{not json" {
		t.Fatalf("buildInputPayload(note={not json) = %v, %v", payload, err)
	}
}
