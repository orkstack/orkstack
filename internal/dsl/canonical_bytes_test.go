package dsl

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

// trickyValues holds what an encoder could spell more than one way: HTML
// characters, U+2028 and U+2029, keys that sort differently by byte and by
// UTF-16 unit, numbers at the edge of the exponent rules, and empty and
// nested containers. (A Struct cannot hold text that is not valid UTF-8.)
func trickyValues(t *testing.T) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(map[string]any{
		"html":   "<script>a && b</script>",
		"lines":  "a b c",
		"ctrl":   "\x01\b\f\t\n\"\\/\x7f",
		"�":      "replacement key",
		"😀":      "astral key",
		"Z":      "upper",
		"z":      "lower",
		"é":      "accent",
		"nums":   []any{1e21, 1e20, 0.1, math.Copysign(0, -1), 5.0, 1.5e-7, 9007199254740993.0, 123456789.125},
		"empty":  map[string]any{},
		"list":   []any{},
		"null":   nil,
		"nested": map[string]any{"b": true, "a": []any{map[string]any{"y": 1, "x": "<"}}},
	})
	if err != nil {
		t.Fatalf("struct: %v", err)
	}
	return s
}

// The canonical form's bytes, recorded with encoding/json before the module
// moved to encoding/json/v2. A definition digest is a contract (§4.4): if one
// of these moves, the encoder changed, not the document.
const (
	goldenTrickyDefinitionDigest = "sha256:ffe0a39ec083c11637e1509302ac619f34ec8326d06974271ca468177610f0e5"
	goldenTrickyContentHash      = "b55f57448ad474b543e3402806fc841ec0dae8b27ccc105cbc32d02399c8bc7b"
	goldenTrickyInputLine        = "{\"Z\":\"upper\",\"ctrl\":\"\\u0001\\b\\f\\t\\n\\\"\\\\/\x7f\",\"empty\":{},\"html\":\"<script>a && b</script>\",\"lines\":\"a\\u2028b\\u2029c\",\"list\":[],\"nested\":{\"a\":[{\"x\":\"<\",\"y\":1}],\"b\":true},\"null\":null,\"nums\":[1000000000000000000000,100000000000000000000,0.1,0,5,0.00000015,9007199254740992,123456789.125],\"z\":\"lower\",\"é\":\"accent\",\"�\":\"replacement key\",\"😀\":\"astral key\"}"
	goldenTrickyInputJSONSHA256  = "da32d0eaff2b13edd64674a8af37f1118b399dbe45d5e153b81bdb89215f9c55"
)

func TestCanonicalBytes_AreWhatEncodingJSONWrote(t *testing.T) {
	wf := goldenOrkflow(t)
	wf.Tasks[0].With = trickyValues(t)
	wf.Tasks[0].Env = map[string]string{"é": "<1>", "Z": "&", "z": " ", "😀": "x", "�": "y"}
	wf.Labels = map[string]string{"b": "<", "a": " "}

	digest, err := DefinitionDigest(wf)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := ContentHash([]byte("source: <yaml>\n"), wf)
	if err != nil {
		t.Fatal(err)
	}
	line, err := CanonicalInputLine(trickyValues(t))
	if err != nil {
		t.Fatal(err)
	}
	indented, err := CanonicalInputJSON(trickyValues(t))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(indented))
	for _, c := range []struct{ name, got, want string }{
		{"DefinitionDigest", digest, goldenTrickyDefinitionDigest},
		{"ContentHash", hash, goldenTrickyContentHash},
		{"CanonicalInputLine", line, goldenTrickyInputLine},
		{"sha256(CanonicalInputJSON)", hex.EncodeToString(sum[:]), goldenTrickyInputJSONSHA256},
	} {
		if c.got != c.want {
			t.Errorf("%s moved.\n got: %q\nwant: %q", c.name, c.got, c.want)
		}
	}
}
