package stablejson

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"testing"
	"time"
)

// v1Golden is what encoding/json wrote for each corpus value, recorded with
// encoding/json itself before the module moved to v2: json.Marshal,
// json.MarshalIndent(v, "", "  "), and an Encoder with SetEscapeHTML(false).
// A digest, a memo key or a stored record written with this package keeps
// these bytes.
var v1Golden = []struct{ marshal, indent, unescaped string }{
	{"null", "null", "null\n"},
	{"\"plain\"", "\"plain\"", "\"plain\"\n"},
	{"\"a\\u003cb\\u003e\\u0026c\\u2028d\\u2029e\x7f\\u0001\\b\\f\\t\\\"\\\\/é😀\"", "\"a\\u003cb\\u003e\\u0026c\\u2028d\\u2029e\x7f\\u0001\\b\\f\\t\\\"\\\\/é😀\"", "\"a<b>&c\\u2028d\\u2029e\x7f\\u0001\\b\\f\\t\\\"\\\\/é😀\"\n"},
	{"\"bad � utf-8\"", "\"bad � utf-8\"", "\"bad � utf-8\"\n"},
	{"null", "null", "null\n"},
	{"[]", "[]", "[]\n"},
	{"null", "null", "null\n"},
	{"{}", "{}", "{}\n"},
	{"{\"B\":{\"y\":\"\\u003c\",\"z\":1.5},\"a\":[true,null,\"x\"],\"b\":1,\"�\":1,\"😀\":2}", "{\n  \"B\": {\n    \"y\": \"\\u003c\",\n    \"z\": 1.5\n  },\n  \"a\": [\n    true,\n    null,\n    \"x\"\n  ],\n  \"b\": 1,\n  \"�\": 1,\n  \"😀\": 2\n}", "{\"B\":{\"y\":\"<\",\"z\":1.5},\"a\":[true,null,\"x\"],\"b\":1,\"�\":1,\"😀\":2}\n"},
	{"\"Ynl0ZXM=\"", "\"Ynl0ZXM=\"", "\"Ynl0ZXM=\"\n"},
	{"null", "null", "null\n"},
	{"1e+21", "1e+21", "1e+21\n"},
	{"100000000000000000000", "100000000000000000000", "100000000000000000000\n"},
	{"1e-7", "1e-7", "1e-7\n"},
	{"0.1", "0.1", "0.1\n"},
	{"-0", "-0", "-0\n"},
	{"123456789.125", "123456789.125", "123456789.125\n"},
	{"0.1", "0.1", "0.1\n"},
	{"-9007199254740993", "-9007199254740993", "-9007199254740993\n"},
	{"18446744073709551615", "18446744073709551615", "18446744073709551615\n"},
	{"{\"name\":\"\",\"tags\":null,\"labels\":null,\"bytes\":null,\"when\":\"0001-01-01T00:00:00Z\",\"inner\":{\"y\":\"\"},\"raw\":null,\"num\":0,\"any\":null,\"ratio\":0,\"Untaged\":\"\"}", "{\n  \"name\": \"\",\n  \"tags\": null,\n  \"labels\": null,\n  \"bytes\": null,\n  \"when\": \"0001-01-01T00:00:00Z\",\n  \"inner\": {\n    \"y\": \"\"\n  },\n  \"raw\": null,\n  \"num\": 0,\n  \"any\": null,\n  \"ratio\": 0,\n  \"Untaged\": \"\"\n}", "{\"name\":\"\",\"tags\":null,\"labels\":null,\"bytes\":null,\"when\":\"0001-01-01T00:00:00Z\",\"inner\":{\"y\":\"\"},\"raw\":null,\"num\":0,\"any\":null,\"ratio\":0,\"Untaged\":\"\"}\n"},
	{"{\"name\":\"n\\u003c\",\"count\":3,\"flag\":true,\"tags\":[\"t\"],\"opt\":[\"o\"],\"labels\":{\"a\":\"2\",\"z\":\"1\"},\"bytes\":\"AAEC\",\"when\":\"2026-11-08T01:02:03.000000004Z\",\"inner\":{\"x\":1,\"y\":\"y\"},\"ptr\":{\"y\":\"\"},\"raw\":{\"k\":[1,\"\\u003c\"]},\"num\":1.50,\"any\":{\"q\":[1,\"2\"]},\"ratio\":0.5,\"Untaged\":\"u\"}", "{\n  \"name\": \"n\\u003c\",\n  \"count\": 3,\n  \"flag\": true,\n  \"tags\": [\n    \"t\"\n  ],\n  \"opt\": [\n    \"o\"\n  ],\n  \"labels\": {\n    \"a\": \"2\",\n    \"z\": \"1\"\n  },\n  \"bytes\": \"AAEC\",\n  \"when\": \"2026-11-08T01:02:03.000000004Z\",\n  \"inner\": {\n    \"x\": 1,\n    \"y\": \"y\"\n  },\n  \"ptr\": {\n    \"y\": \"\"\n  },\n  \"raw\": {\n    \"k\": [\n      1,\n      \"\\u003c\"\n    ]\n  },\n  \"num\": 1.50,\n  \"any\": {\n    \"q\": [\n      1,\n      \"2\"\n    ]\n  },\n  \"ratio\": 0.5,\n  \"Untaged\": \"u\"\n}", "{\"name\":\"n<\",\"count\":3,\"flag\":true,\"tags\":[\"t\"],\"opt\":[\"o\"],\"labels\":{\"a\":\"2\",\"z\":\"1\"},\"bytes\":\"AAEC\",\"when\":\"2026-11-08T01:02:03.000000004Z\",\"inner\":{\"x\":1,\"y\":\"y\"},\"ptr\":{\"y\":\"\"},\"raw\":{\"k\":[1,\"<\"]},\"num\":1.50,\"any\":{\"q\":[1,\"2\"]},\"ratio\":0.5,\"Untaged\":\"u\"}\n"},
	{"{\"r\":null}", "{\n  \"r\": null\n}", "{\"r\":null}\n"},
}

func TestMarshal_WritesWhatEncodingJSONWrote(t *testing.T) {
	values := corpus()
	if len(values) != len(v1Golden) {
		t.Fatalf("corpus has %d values, golden %d", len(values), len(v1Golden))
	}
	for i, v := range values {
		want := v1Golden[i]
		if got, err := Marshal(v); err != nil || string(got) != want.marshal {
			t.Errorf("%d: Marshal = %q, %v; want %q", i, got, err, want.marshal)
		}
		if got, err := MarshalIndent(v, "  "); err != nil || string(got) != want.indent {
			t.Errorf("%d: MarshalIndent = %q, %v; want %q", i, got, err, want.indent)
		}
		var buf bytes.Buffer
		if err := Encode(&buf, v, Unescaped); err != nil || buf.String() != want.unescaped {
			t.Errorf("%d: Encode(Unescaped) = %q, %v; want %q", i, buf.String(), err, want.unescaped)
		}
	}
}

func TestCompactAndIndent_KeepStringsAndNumbersAsWritten(t *testing.T) {
	src := []byte("{ \"a\" : \"< \\u00e9\\/\" , \"n\": 1.50, \"z\":[ ] } \n")
	var compact bytes.Buffer
	if err := Compact(&compact, src); err != nil {
		t.Fatal(err)
	}
	if want := "{\"a\":\"< \\u00e9\\/\",\"n\":1.50,\"z\":[]}"; compact.String() != want {
		t.Errorf("Compact = %q, want %q", compact.String(), want)
	}
	var indented bytes.Buffer
	if err := Indent(&indented, src, "  "); err != nil {
		t.Fatal(err)
	}
	// encoding/json.Indent kept the whitespace src ends with.
	if want := "{\n  \"a\": \"< \\u00e9\\/\",\n  \"n\": 1.50,\n  \"z\": []\n} \n"; indented.String() != want {
		t.Errorf("Indent = %q, want %q", indented.String(), want)
	}
}

func TestCompact_AcceptsWhatEncodingJSONCompactAccepted(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"x":{"b":1, "b":2}}`, `{"x":{"b":1,"b":2}}`}, // a formatter: the reader refuses it
		{"\"\xff\"", "\"\xff\""},
		{`"\ud800"`, `"\ud800"`},
	} {
		var buf bytes.Buffer
		if err := Compact(&buf, []byte(tc.in)); err != nil || buf.String() != tc.want {
			t.Errorf("Compact(%q) = %q, %v; want %q", tc.in, buf.String(), err, tc.want)
		}
	}
	for _, bad := range []string{``, ` `, `{"a":1}x`, `1 2`, `{"a":1} {"b":2}`} {
		var buf bytes.Buffer
		if err := Compact(&buf, []byte(bad)); err == nil {
			t.Errorf("Compact(%q) succeeded", bad)
		}
	}
}

func TestRawJSON_IsCarriedAsTheJSONItHolds(t *testing.T) {
	type carrier struct {
		R RawJSON `json:"r"`
		N RawJSON `json:"n"`
	}
	encoded, err := json.Marshal(carrier{R: RawJSON(`{"k":[1,"x"]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"r":{"k":[1,"x"]},"n":null}`; string(encoded) != want {
		t.Fatalf("Marshal = %s, want %s", encoded, want)
	}
	var back carrier
	if err := json.Unmarshal([]byte(`{"r": { "a" : 1 } }`), &back); err != nil {
		t.Fatal(err)
	}
	if want := `{ "a" : 1 }`; string(back.R) != want {
		t.Fatalf("Unmarshal kept %q, want the bytes as written %q", back.R, want)
	}
	// A jsontext.Value, by contrast, has no methods: an encoder that is still
	// encoding/json would write it as base64. RawJSON must keep both methods.
	var _ interface {
		MarshalJSON() ([]byte, error)
	} = RawJSON(nil)
	var _ interface{ UnmarshalJSON([]byte) error } = new(RawJSON)
}

func TestUseNumber_ReadsEveryNumberAsItsText(t *testing.T) {
	var v any
	if err := json.Unmarshal([]byte(`{"a":9007199254740993,"b":[1.50,{"c":-0e3}],"d":"7"}`), &v, UseNumber); err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["a"] != Number("9007199254740993") {
		t.Errorf("a = %#v", m["a"])
	}
	b := m["b"].([]any)
	if b[0] != Number("1.50") || b[1].(map[string]any)["c"] != Number("-0e3") {
		t.Errorf("b = %#v", b)
	}
	if m["d"] != "7" {
		t.Errorf("d = %#v, want the string", m["d"])
	}
	out, err := Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"a":9007199254740993,"b":[1.50,{"c":-0e3}],"d":"7"}`; string(out) != want {
		t.Errorf("re-encoded %s, want %s", out, want)
	}
}

func TestNumber_RefusesTextThatIsNotANumber(t *testing.T) {
	for _, bad := range []Number{"1.", "01", "x", "1 ", "NaN"} {
		if _, err := Marshal(bad); err == nil {
			t.Errorf("Marshal(%q) succeeded", bad)
		}
	}
	if got, err := Marshal(Number("")); err != nil || string(got) != "0" {
		t.Errorf("Marshal(\"\") = %s, %v; encoding/json wrote 0", got, err)
	}
}

// FuzzUseNumber: a document v2 accepts decodes with UseNumber to a value that
// re-encodes with every number exactly as written. UseNumber accepts every
// document the plain decode accepts, and also a number too large for a
// float64 (1e700), as encoding/json's UseNumber did.
func FuzzUseNumber(f *testing.F) {
	for _, seed := range []string{
		`{}`, `[]`, `0`, `-0.0e+00`, `{"a":9007199254740993,"b":[1.50,{"c":-0e3}]}`,
		`{"a":1,"a":2}`, "\"\xff\"", `"\ud800"`, `[1,]`, `{"a":"< "}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var plain, numbered any
		errPlain := json.Unmarshal(data, &plain)
		errNumbered := json.Unmarshal(data, &numbered, UseNumber)
		if errPlain == nil && errNumbered != nil {
			t.Fatalf("UseNumber refused what the plain decode accepts: %v", errNumbered)
		}
		if errNumbered != nil {
			return
		}
		out, err := json.Marshal(numbered, json.Deterministic(true))
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if got, want := numberTexts(out), numberTexts(data); !slices.Equal(got, want) {
			t.Fatalf("numbers %q of %s are not %q of %s", got, out, want, data)
		}
	})
}

// numberTexts is the sorted number literals of a document.
func numberTexts(data []byte) []string {
	dec := jsontext.NewDecoder(bytes.NewReader(data))
	var texts []string
	for {
		token, err := dec.ReadToken()
		if err != nil {
			break
		}
		if token.Kind() == '0' {
			texts = append(texts, token.String())
		}
	}
	slices.Sort(texts)
	return texts
}

// FuzzRawJSON: whatever JSON value a RawJSON is decoded from, it holds those
// bytes exactly and marshals as the same value.
func FuzzRawJSON(f *testing.F) {
	for _, seed := range []string{`null`, `{ "a" : 1 }`, `[1, 2]`, `"x"`, `1.50`, ` 1 `} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if !jsontext.Value(data).IsValid() {
			return
		}
		var raw RawJSON
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("Unmarshal of a valid value: %v", err)
		}
		trimmed := bytes.Trim(data, " \t\r\n")
		if !bytes.Equal(raw, trimmed) {
			t.Fatalf("held %q, want %q", raw, trimmed)
		}
		out, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		want := jsontext.Value(bytes.Clone(trimmed))
		_ = want.Compact()
		if !bytes.Equal(out, want) {
			t.Fatalf("marshalled %q, want %q", out, want)
		}
	})
}

func TestDurationAsNano_IsIntegerNanoseconds(t *testing.T) {
	type attempt struct {
		Backoff time.Duration `json:"backoff,omitzero"`
		Took    time.Duration `json:"took"`
	}
	encoded, err := Marshal(attempt{Took: 1500 * time.Millisecond})
	if err != nil || string(encoded) != `{"took":1500000000}` {
		t.Fatalf("Marshal = %s, %v; encoding/json wrote {\"took\":1500000000}", encoded, err)
	}
	var back attempt
	if err := json.Unmarshal([]byte(`{"backoff":100,"took":-7}`), &back, DurationAsNano); err != nil {
		t.Fatal(err)
	}
	if back.Backoff != 100 || back.Took != -7 {
		t.Fatalf("decoded %+v", back)
	}
	if err := json.Unmarshal([]byte(`{"took":"1s"}`), &back, DurationAsNano); err == nil {
		t.Fatal("a string decoded as a duration")
	}
}

func TestRawJSON_OmitzeroOmitsWhatOmitemptyOmittedBefore(t *testing.T) {
	type carrier struct {
		Empty  RawJSON `json:"empty,omitzero"`
		Nil    RawJSON `json:"nil,omitzero"`
		Object RawJSON `json:"object,omitzero"`
		Null   RawJSON `json:"null,omitzero"`
	}
	// encoding/json with omitempty: {"object":{},"null":null}.
	encoded, err := Marshal(carrier{Empty: RawJSON{}, Object: RawJSON(`{}`), Null: RawJSON(`null`)})
	if err != nil || string(encoded) != `{"object":{},"null":null}` {
		t.Fatalf("Marshal = %s, %v", encoded, err)
	}
}
