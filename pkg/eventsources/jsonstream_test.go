package eventsources

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestReadJSON_Validity(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		reason string
	}{
		{"object", `{"a":1,"b":[true,false,null,"x"]}`, ""},
		{"scalar", `  42  `, ""},
		{"nested empty", `{"a":{},"b":[]}`, ""},
		{"unicode escapes", `{"a":"é😀"}`, ""},
		{"empty", ``, ReasonMessageNotJSON},
		{"second value", `{} {}`, ReasonMessageNotJSON},
		{"trailing comma", `[1,]`, ReasonMessageNotJSON},
		{"duplicate key", `{"a":1,"a":2}`, ReasonMessageNotJSON},
		{"duplicate key by escape", `{"a":1,"a":2}`, ReasonMessageNotJSON},
		{"same key in sibling objects", `[{"a":1},{"a":2}]`, ""},
		{"same key at two depths", `{"a":{"a":1}}`, ""},
		{"lone surrogate", `"\ud800"`, ReasonMessageNotJSON},
		{"reversed surrogates", `"\udc00\ud800"`, ReasonMessageNotJSON},
		{"invalid utf8", "\"\xff\"", ReasonMessageNotJSON},
		{"control char", "\"a\x01\"", ReasonMessageNotJSON},
		{"leading zero", `01`, ReasonMessageNotJSON},
		{"bare minus", `-`, ReasonMessageNotJSON},
		{"exponent no digits", `1e`, ReasonMessageNotJSON},
		{"fraction no digits", `1.`, ReasonMessageNotJSON},
		{"single quotes", `'a'`, ReasonMessageNotJSON},
		{"nan", `NaN`, ReasonMessageNotJSON},
		{"unclosed", `{"a":1`, ReasonMessageNotJSON},
		{"bad escape", `"\x"`, ReasonMessageNotJSON},
		{"too deep", strings.Repeat("[", 65) + strings.Repeat("]", 65), ReasonMessageJSONTooDeep},
		{"deep enough", strings.Repeat("[", 64) + strings.Repeat("]", 64), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadJSON([]byte(tt.body), nil)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("ReadJSON(%q) = %v, want valid", tt.body, err)
				}
				return
			}
			if err == nil || err.Reason != tt.reason {
				t.Fatalf("ReadJSON(%q) = %v, want %s", tt.body, err, tt.reason)
			}
		})
	}
}

func TestReadJSON_NeverQuotesTheBody(t *testing.T) {
	_, err := ReadJSON([]byte(`{"secret-key":1,"secret-key":2}`), nil)
	if err == nil {
		t.Fatal("want a duplicate-key refusal")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("refusal %q quotes the body", err)
	}
	if err.Offset <= 0 {
		t.Fatalf("refusal names no offset: %+v", err)
	}
}

func TestReadJSON_Selects(t *testing.T) {
	body := []byte(`{"detail":{"status":"PAID","order_id":"o-1","n":1.50,"list":[10,{"x":"y"}]},"a/b":1,"m~n":2,"":3}`)
	got, err := ReadJSON(body, []string{"/detail/status", "/detail/n", "/detail/list/1/x", "/detail/missing", "/a~1b", "/m~0n", "/", "", "/detail/list/01"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"/detail/status":   `"PAID"`,
		"/detail/n":        `1.50`,
		"/detail/list/1/x": `"y"`,
		"/a~1b":            `1`,
		"/m~0n":            `2`,
		"/":                `3`,
		"":                 string(body),
	}
	for p, w := range want {
		v, ok := got.Value(p)
		if !ok || string(v) != w {
			t.Errorf("Value(%q) = %q, %v; want %q", p, v, ok, w)
		}
	}
	for _, p := range []string{"/detail/missing", "/detail/list/01"} {
		if v, ok := got.Value(p); ok {
			t.Errorf("Value(%q) = %q, want nothing selected", p, v)
		}
	}
}

func TestReadJSON_SelectedValueOverBound(t *testing.T) {
	big := `"` + strings.Repeat("a", MaxSelectedBytes) + `"`
	got, err := ReadJSON([]byte(`{"big":`+big+`,"small":1}`), []string{"/big", "/small"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.TooLarge("/big") {
		t.Fatal("a selected value over its bound must be reported too large")
	}
	if _, ok := got.Value("/big"); ok {
		t.Fatal("a value over its bound must not be returned")
	}
	if v, ok := got.Value("/small"); !ok || string(v) != "1" {
		t.Fatalf("Value(/small) = %q, %v", v, ok)
	}
}

func TestReadJSON_KeyBudget(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("{")
	key := strings.Repeat("k", 1000)
	for i := 0; i < 1100; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"` + key + strconv.Itoa(i) + `":0`)
	}
	b.WriteString("}")
	_, err := ReadJSON(b.Bytes(), nil)
	if err == nil || err.Reason != ReasonMessageJSONTooLarge {
		t.Fatalf("want %s, got %v", ReasonMessageJSONTooLarge, err)
	}
}

// TestJSONStream_BoundedMemory reads a body of a million small objects and
// holds what it allocates to a bound that does not grow with the body.
func TestJSONStream_BoundedMemory(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("[")
	for i := 0; i < 1_000_000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"a":1,"bb":"x","c":[true]}`)
	}
	b.WriteString("]")
	body := b.Bytes()
	pointers := []string{"/0/a", "/999999/bb"}
	allocs := testing.AllocsPerRun(2, func() {
		if _, err := ReadJSON(body, pointers); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 200 {
		t.Fatalf("ReadJSON allocated %.0f times over %d bytes; want a fixed bound", allocs, len(body))
	}
}
