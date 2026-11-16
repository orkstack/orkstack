package eventsources

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"math/big"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FuzzJSONStream holds the streaming reader to a tree-building reference:
// whatever it accepts the reference accepts; the whole document it selects
// is the body itself; and what it selects is the value the reference finds
// at that pointer.
func FuzzJSONStream(f *testing.F) {
	for _, seed := range []string{`{"a":1,"b":[true,{"c":"x"}]}`, `[]`, `"é"`, `{"a":1,"a":2}`, `1.5e3`, `{"":{"/":2}}`, `[[[[]]]]`, "\"\xff\""} {
		f.Add([]byte(seed), "/b/1/c")
	}
	f.Fuzz(func(t *testing.T, body []byte, pointer string) {
		if len(body) > 1<<16 {
			return
		}
		sel, err := ReadJSON(body, []string{"", pointer})
		if err != nil {
			return
		}
		if !jsontext.Value(body).IsValid() {
			t.Fatalf("ReadJSON accepted %q, which encoding/json refuses", body)
		}
		whole, ok := sel.Value("")
		if !ok && !sel.TooLarge("") {
			t.Fatal("the whole document selects nothing")
		}
		if ok && !bytes.Equal(whole, bytes.TrimSpace(body)) {
			t.Fatalf("the whole document selects %q of %q", whole, body)
		}
		got, ok := sel.Value(pointer)
		if !ok {
			return
		}
		var tree any
		if err := json.Unmarshal(body, &tree); err != nil {
			t.Fatalf("reference cannot decode an accepted body: %v", err)
		}
		want, found := lookup(tree, pointer)
		if !found {
			t.Fatalf("selected %q at %q where the reference finds nothing", got, pointer)
		}
		var gotValue any
		if err := json.Unmarshal(got, &gotValue); err != nil {
			t.Fatalf("selected text %q does not decode: %v", got, err)
		}
		a, _ := json.Marshal(gotValue, json.Deterministic(true))
		b, _ := json.Marshal(want, json.Deterministic(true))
		if !bytes.Equal(a, b) {
			t.Fatalf("selected %s at %q; the reference finds %s", a, pointer, b)
		}
	})
}

func lookup(tree any, pointer string) (any, bool) {
	tokens, err := ParsePointer(pointer)
	if err != nil {
		return nil, false
	}
	cur := tree
	for _, tok := range tokens {
		switch v := cur.(type) {
		case map[string]any:
			next, ok := v[tok]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i := arrayIndex(tok)
			if i < 0 || i >= len(v) {
				return nil, false
			}
			cur = v[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// FuzzMatchEquality holds number equality to an arbitrary-precision
// reference, for numbers whose exponents the reference can expand.
func FuzzMatchEquality(f *testing.F) {
	for _, p := range [][2]string{{"1", "1.0"}, {"-0", "0"}, {"1e2", "100"}, {"0.10", "1e-1"}, {"12", "1.2e1"}, {"3", "-3"}} {
		f.Add(p[0], p[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		if len(a) > 64 || len(b) > 64 {
			return
		}
		if _, err := ReadJSON([]byte(a), nil); err != nil || !isNumberText(a) {
			return
		}
		if _, err := ReadJSON([]byte(b), nil); err != nil || !isNumberText(b) {
			return
		}
		ra, okA := new(big.Rat).SetString(a)
		rb, okB := new(big.Rat).SetString(b)
		if !okA || !okB || exponentTooLarge(a) || exponentTooLarge(b) {
			return
		}
		if got, want := equalNumbers(a, b), ra.Cmp(rb) == 0; got != want {
			t.Fatalf("equalNumbers(%s, %s) = %v, want %v", a, b, got, want)
		}
	})
}

func isNumberText(s string) bool {
	return s != "" && (s[0] == '-' || (s[0] >= '0' && s[0] <= '9'))
}

func exponentTooLarge(s string) bool {
	i := strings.IndexAny(s, "eE")
	return i >= 0 && len(strings.TrimLeft(s[i+1:], "+-0")) > 3
}

// FuzzJSONPointer: a pointer either parses or is refused, never panics; a
// parsed pointer's tokens rebuild it.
func FuzzJSONPointer(f *testing.F) {
	for _, seed := range []string{"", "/", "/a/b", "/a~1b/~0", "a", "/~", "/~2"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, p string) {
		tokens, err := ParsePointer(p)
		if err != nil {
			return
		}
		var b strings.Builder
		for _, tok := range tokens {
			b.WriteString("/" + strings.ReplaceAll(strings.ReplaceAll(tok, "~", "~0"), "/", "~1"))
		}
		if b.String() != p {
			t.Fatalf("pointer %q rebuilds as %q", p, b.String())
		}
	})
}

// FuzzServiceInterface: a packed interface never panics the reader, and one
// that parses round-trips.
func FuzzServiceInterface(f *testing.F) {
	for _, seed := range []string{`{"schemaVersion":1,"profile":"event-source"}`,
		`{"schemaVersion":1,"profile":"event-source","with":{"s":{"type":"string","required":true}},"eventSource":{"delivery":"DELIVERY_CLASS_AT_MOST_ONCE","maxConcurrency":4}}`,
		`{`, `{"schemaVersion":2}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		iface, err := ParseInterfaceLabel(raw)
		if err != nil {
			return
		}
		again, err := protojson.Marshal(iface)
		if err != nil {
			t.Fatal(err)
		}
		back, err := ParseInterfaceLabel(string(again))
		if err != nil {
			t.Fatalf("a parsed interface does not parse again: %v", err)
		}
		if !protoEqual(iface, back) {
			t.Fatal("an interface does not round-trip")
		}
	})
}

// FuzzMessageMetadata: any message passes through Evaluate without a
// panic, and a refusal names one MESSAGE_* reason.
func FuzzMessageMetadata(f *testing.F) {
	f.Add("m-1", "orders.eu", "Ce-Type", "x", []byte(`{"a":1}`))
	f.Add("a\x07", "\xff", "", "", []byte(`{`))
	f.Fuzz(func(t *testing.T, id, subject, attr, value string, body []byte) {
		src := validSource()
		res := Evaluate(src, Message{ID: id, Subject: subject, Attributes: map[string][]string{attr: {value}}, Body: body}, Judge{})
		if res.Reason != "" && !strings.HasPrefix(res.Reason, "MESSAGE_") {
			t.Fatalf("reason %q", res.Reason)
		}
	})
}

func protoEqual(a, b interface{ ProtoReflect() protoreflect.Message }) bool {
	return proto.Equal(a, b)
}
