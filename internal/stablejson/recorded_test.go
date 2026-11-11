package stablejson

import (
	"encoding/json/v2"
	"reflect"
	"testing"
)

// TestDecodeRecorded_ReadsAsEncodingJSONDid pins what encoding/json.Unmarshal
// into an any gave for the inputs encoding/json/v2 refuses, recorded with
// encoding/json itself (and differential-fuzzed against it over 2.4 million
// inputs before the move): a replayed workflow decides from these.
func TestDecodeRecorded_ReadsAsEncodingJSONDid(t *testing.T) {
	for _, tc := range []struct {
		doc  string
		want any
	}{
		{`{"a":1,"a":2}`, map[string]any{"a": 2.0}},
		{`{"b":{"c":1,"c":[3]}}`, map[string]any{"b": map[string]any{"c": []any{3.0}}}},
		{"{\"a\":\"\xff\"}", map[string]any{"a": "�"}},
		{"{\"\xff\":1}", map[string]any{"�": 1.0}},
		{`{"a":"\ud800"}`, map[string]any{"a": "�"}},
		{`{"a":"\udc00x𐀀"}`, map[string]any{"a": "�x\U00010000"}},
		{` {"k":-0,"n":null,"l":[true,"x"]} `, map[string]any{"k": 0.0, "n": nil, "l": []any{true, "x"}}},
		{`null`, nil},
	} {
		got, err := DecodeRecorded([]byte(tc.doc))
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("DecodeRecorded(%q) = %#v, %v; want %#v", tc.doc, got, err, tc.want)
		}
	}
	for _, bad := range []string{``, `{`, `{"a":1e999}`, `{} x`, `{"a":1,}`, `01`} {
		if got, err := DecodeRecorded([]byte(bad)); err == nil {
			t.Errorf("DecodeRecorded(%q) = %#v; encoding/json refused it", bad, got)
		}
	}
}

// FuzzDecodeRecorded: whatever encoding/json/v2 reads into an any,
// DecodeRecorded reads to the same value; it tolerates only more.
func FuzzDecodeRecorded(f *testing.F) {
	for _, seed := range []string{`{"a":1,"a":2}`, `{"b":{"c":1,"c":[3]}}`, "\"\xff\"", `"\ud800"`, `{"a":1e999}`, `[1,{"x":null}]`, `null`, `{} x`, ` {"k":-0} `} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		recorded, err := DecodeRecorded(data)
		var strict any
		if json.Unmarshal(data, &strict) != nil {
			return
		}
		if err != nil || !reflect.DeepEqual(recorded, strict) {
			t.Fatalf("%q: DecodeRecorded = %#v, %v; encoding/json/v2 = %#v", data, recorded, err, strict)
		}
	})
}
