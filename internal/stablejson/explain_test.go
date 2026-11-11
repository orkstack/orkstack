package stablejson

import (
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"
)

func TestExplain_NamesTheKeyAndWhereItIs(t *testing.T) {
	type door struct {
		URL  string `json:"url"`
		Name string `json:"name"`
	}
	for _, tc := range []struct {
		doc  string
		into any
		opts []json.Options
		want string
	}{
		{`{"a":1,"a":2}`, new(any), nil, `the key "a" appears twice in the top-level object; a JSON object holds each key once`},
		{`{"x":[{"b":1,"b":2}]}`, new(any), nil, `the key "b" appears twice in the object at /x/0; a JSON object holds each key once`},
		{`{"a/b~c":{"k":1,"k":2}}`, new(any), nil, `the key "k" appears twice in the object at /a~1b~0c; a JSON object holds each key once`},
		{`{"url":"x","url":"y"}`, new(door), nil, `the key "url" appears twice in the top-level object; a JSON object holds each key once`},
		{"{\"a\":\"\xff\"}", new(any), nil, `the string at /a is not valid UTF-8`},
		{`{"a":"\ud800"}`, new(any), nil, `the string at /a holds half of a surrogate pair, not a character`},
		{`{"URL":"x"}`, new(door), []json.Options{json.RejectUnknownMembers(true)}, `the top-level object has the key "URL", which is not one it takes (keys match exactly, case included)`},
		{`{"a":1} x`, new(any), nil, `invalid character 'x' after top-level value after offset 8`},
	} {
		err := json.Unmarshal([]byte(tc.doc), tc.into, tc.opts...)
		if err == nil {
			t.Fatalf("%q decoded", tc.doc)
		}
		if got := Explain(err); got != tc.want {
			t.Errorf("Explain(%q)\n got: %s\nwant: %s", tc.doc, got, tc.want)
		}
	}
	if Explain(nil) != "" {
		t.Error("Explain(nil) is not empty")
	}
}

// FuzzExplain: every refusal of a document is explained by a non-empty
// sentence that never begins with a package prefix, and a duplicate key is
// always named.
func FuzzExplain(f *testing.F) {
	for _, seed := range []string{`{"a":1,"a":2}`, `{"x":[{"b":1,"b":2}]}`, "\"\xff\"", `"\ud800"`, `{`, ``, `{"a":1}x`, `{"~0/~1":{"":1,"":2}}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, doc []byte) {
		var v any
		err := json.Unmarshal(doc, &v)
		if err == nil {
			return
		}
		got := Explain(err)
		if got == "" || strings.HasPrefix(got, "json") {
			t.Fatalf("Explain(%v) = %q", err, got)
		}
		if strings.Contains(err.Error(), "duplicate object member name") && !strings.Contains(got, "appears twice") {
			t.Fatalf("Explain(%v) = %q does not name the duplicate", err, got)
		}
	})
}

func TestExplained_WordsTheErrorAndUnwrapsToIt(t *testing.T) {
	var v any
	err := json.Unmarshal([]byte(`{"a":1,"a":2}`), &v)
	wrapped := Explained(err)
	if wrapped.Error() != Explain(err) || !errors.Is(wrapped, err) {
		t.Fatalf("Explained = %v", wrapped)
	}
	if Explained(nil) != nil {
		t.Fatal("Explained(nil) is not nil")
	}
}
