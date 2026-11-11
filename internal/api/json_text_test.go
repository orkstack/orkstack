package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDecodeJSONValue_RefusesWhatADecoderCouldReadTwoWays: the plain-HTTP
// doors read a body as encoding/json/v2 does, and every refusal names the key
// and where it is.
func TestDecodeJSONValue_RefusesWhatADecoderCouldReadTwoWays(t *testing.T) {
	type body struct {
		URL    string   `json:"url"`
		Events []string `json:"events"`
	}
	for _, tc := range []struct {
		name     string
		raw      string
		disallow bool
		want     string // "" accepts
	}{
		{"one object", `{"url":"https://x","events":["RUN_FAILED"]}`, false, ""},
		{"a key no field takes is ignored", `{"url":"https://x","note":"x"}`, false, ""},
		{"a key written twice", `{"url":"https://a","url":"https://b"}`, false,
			`not one valid JSON value: the key "url" appears twice in the top-level object; a JSON object holds each key once`},
		{"a key that matches a field only by case", `{"URL":"https://x"}`, false,
			`the top-level object has the key "URL", which is not one it takes: did you mean "url"? (keys match exactly, case included)`},
		{"an unknown key where none is allowed", `{"url":"x","Events":[]}`, true,
			`the top-level object has the key "Events", which is not one it takes (keys match exactly, case included)`},
		{"data after the value", `{"url":"x"} {}`, false, `not one valid JSON value: invalid character '{' after top-level value`},
		{"half of a surrogate pair", `{"url":"\ud800"}`, false, `half of a surrogate pair`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got body
			err := decodeJSONValue([]byte(tc.raw), &got, tc.disallow)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestTriggerInput_RefusesAKeyWrittenTwiceNamingIt: the trigger door refused a
// repeated key before, through protojson, at a byte offset; it now names the
// key and the object holding it.
func TestTriggerInput_RefusesAKeyWrittenTwiceNamingIt(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/triggers/t", strings.NewReader(`{"env":{"region":"eu","region":"us"}}`))
	request.Header.Set("Content-Type", "application/json")
	_, err := triggerInput(httptest.NewRecorder(), request)
	want := `request body is not valid JSON: the key "region" appears twice in the object at /env`
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("triggerInput = %v, want %q", err, want)
	}
}
