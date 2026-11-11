package engine

import (
	"reflect"
	"testing"
)

// TestDecodeValues_ReadsRecordedBytesAsTheyWereRead: decodeValues runs in
// workflow code on a history's recorded values_json. A values.json an earlier
// Orker committed with a key written twice or a byte that is not UTF-8 must
// decide a replayed condition exactly as it did when it was recorded.
func TestDecodeValues_ReadsRecordedBytesAsTheyWereRead(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want map[string]any
	}{
		{`{"ok":false,"ok":true}`, map[string]any{"ok": true}},
		{"{\"name\":\"caf\xe9\"}", map[string]any{"name": "caf�"}},
		{`{"rows":{"n":1,"n":[2]}}`, map[string]any{"rows": map[string]any{"n": []any{2.0}}}},
		{`[1]`, map[string]any{}},
		{`{`, map[string]any{}},
		{``, map[string]any{}},
	} {
		if got := decodeValues([]byte(tc.raw)); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("decodeValues(%q) = %#v, want %#v", tc.raw, got, tc.want)
		}
	}
}
