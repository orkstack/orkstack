package dsl_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/orkstack/orkstack/internal/dsl"
)

// The strict YAML-to-JSON mapping (docs/JSON.md, "Data written in an
// authored document").

// authored parses "v: <text>" and converts the value at path "v".
func authored(t testing.TB, text string) (any, error) {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte("v: "+text+"\n"), &doc); err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return dsl.AuthoredJSON(doc.Content[0].Content[1], "v")
}

func authoredDoc(t testing.TB, source string) (any, error) {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(source), &doc); err != nil {
		t.Fatalf("parse %q: %v", source, err)
	}
	return dsl.AuthoredJSON(doc.Content[0], "with")
}

func TestStrictJSON_AcceptsWhatEveryReaderAgreesOn(t *testing.T) {
	for _, tc := range []struct {
		text string
		want any
	}{
		{"null", nil}, {"~", nil}, {"", nil},
		{"true", true}, {"False", false},
		{"0", 0.0}, {"-0", 0.0}, {"+12", 12.0}, {"42", 42.0},
		{"0x1F", 31.0},
		{"1.5", 1.5}, {".5", 0.5}, {"1.", 1.0}, {"1.0", 1.0}, {"01.5", 1.5}, {"1.5e+3", 1500.0}, {"1.5E-3", 0.0015},
		{"9007199254740992", 9007199254740992.0},
		{"1.2.3", "1.2.3"}, {"10.0.0.1", "10.0.0.1"}, {"v1", "v1"}, {"3d", "3d"}, {"inf", "inf"},
		{"nUll", "nUll"}, {"_1", "_1"}, {"https://x/y", "https://x/y"},
		{"'yes'", "yes"}, {`"0755"`, "0755"}, {"|\n  block", "block\n"},
	} {
		got, err := authored(t, tc.text)
		if err != nil {
			t.Errorf("%q: %v", tc.text, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q = %#v (%T), want %#v (%T)", tc.text, got, got, tc.want, tc.want)
		}
		if f, ok := got.(float64); ok && math.Signbit(f) {
			t.Errorf("%q is -0; an integer form is its integer", tc.text)
		}
	}
	mapping, err := authoredDoc(t, "a: b\n")
	if err != nil || !reflect.DeepEqual(mapping, map[string]any{"a": "b"}) {
		t.Errorf("a: b = %#v, %v", mapping, err)
	}
}

const (
	r1   = "%s is a boolean to YAML 1.1 readers (yes, no, on, off, y and n, in any casing) and a string to YAML 1.2 readers; write true or false for a boolean, or quote it ('%s') for the string"
	r2   = "%s is the number %s to YAML 1.1 readers and %s to YAML 1.2 readers; write the number without a leading zero, or quote it ('%s') for the string"
	r3   = "%s is the number %s to YAML 1.1 readers and a string to YAML 1.2 readers; write %s for the number, or quote it ('%s') for the string"
	r4   = "%s is the number %s to YAML 1.2 readers and a string to YAML 1.1 readers; write %s for the number, or quote it ('%s') for the string"
	r5   = "%s is a timestamp to some YAML readers, and JSON has no timestamp; quote it ('%s') for the string"
	r6   = "%s is not a number JSON can hold; quote it ('%s') for the string"
	r7   = "%s is out of range for a JSON number; quote it ('%s') for the string"
	r8   = "%s is beyond ±2^53, so a JSON number cannot hold it exactly; quote it ('%s') for the string"
	r9   = "%s was the number %s to earlier OrkStack releases and is a string to YAML 1.2 readers; write %s for the number, or quote it ('%s') for the string"
	rR19 = "%s is not kept exactly by a JSON number: it would become %s; write %s for that number, or quote it ('%s') for the string"
)

func requireRefusal(t *testing.T, text, path string, line int, reason string) {
	t.Helper()
	_, err := authored(t, text)
	var refused *dsl.AuthoredDataError
	if !errors.As(err, &refused) {
		t.Errorf("%q: err = %v, want a refusal", text, err)
		return
	}
	if refused.Path != path || refused.Line != line || refused.Reason != reason {
		t.Errorf("%q: refused at %s line %d: %q\nwant at %s line %d: %q", text, refused.Path, refused.Line, refused.Reason, path, line, reason)
	}
}

func TestStrictJSON_RefusesWhatReadersDisagreeOn(t *testing.T) {
	for _, text := range []string{"yes", "No", "on", "OFF", "y", "n", "yEs"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r1, text, text))
	}
	for _, text := range []string{"2026-01-02", "2026-1-2", "2026-01-02T03:04:05Z", "2026-01-02 03:04:05"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r5, text, text))
	}
	for _, text := range []string{".inf", "-.Inf", ".NaN"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r6, text, text))
	}
	requireRefusal(t, "1e400", "v", 1, fmt.Sprintf(r7, "1e400", "1e400"))
	for _, text := range []string{"9007199254740993", "-9007199254740993", "0x20000000000001"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r8, text, text))
	}
	requireRefusal(t, "0755", "v", 1, fmt.Sprintf(r2, "0755", "493", "755", "0755"))
	requireRefusal(t, "-017", "v", 1, fmt.Sprintf(r2, "-017", "-15", "-17", "-017"))
	for text, y11 := range map[string]string{"1_000": "1000", "0b101": "5", "1:30": "90", "12:30": "750", "-0x1F": "-31", "+0x1F": "31", "1:30.5": "90.5"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r3, text, y11, y11, text))
	}
	for text, core := range map[string]string{"0o17": "15", "1e3": "1000", "1.5e3": "1500", "-.5": "-0.5", "08": "8"} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(r4, text, core, core, text))
	}
	requireRefusal(t, "0X1F", "v", 1, fmt.Sprintf(r9, "0X1F", "31", "31", "0X1F"))

	// The path and line are the value's, nested.
	_, err := authoredDoc(t, "flags:\n  - quiet: true\n    verbose: on\n")
	var refused *dsl.AuthoredDataError
	if !errors.As(err, &refused) || refused.Path != "with.flags[0].verbose" || refused.Line != 3 {
		t.Errorf("nested refusal = %v, want with.flags[0].verbose at line 3", err)
	}
	// A quote with a single quote in it is double-quoted.
	requireRefusal(t, "1:30", "v", 1, fmt.Sprintf(r3, "1:30", "90", "90", "1:30"))
}

func TestStrictJSON_RefusesANumberJSONWouldChange(t *testing.T) {
	for text, becomes := range map[string]string{
		"9007199254740993.0":  "9007199254740992",
		"0.30000000000000001": "0.3",
		"1.00000000000000001": "1",
		"123456789.123456789": "123456789.12345679",
		"1.0e-400":            "0",
		"4.9e-324":            strconv.FormatFloat(5e-324, 'f', -1, 64),
	} {
		requireRefusal(t, text, "v", 1, fmt.Sprintf(rR19, text, becomes, becomes, text))
	}
	for _, text := range []string{"0.1", "1.10", "1.0e+23", "1.0e+21", "-0.0", "5.0e-324", "1.7976931348623157e+308"} {
		got, err := authored(t, text)
		if err != nil {
			t.Errorf("%q: %v", text, err)
			continue
		}
		want, _ := strconv.ParseFloat(text, 64)
		if got != want {
			t.Errorf("%q = %v, want %v", text, got, want)
		}
	}
	if got, _ := authored(t, "-0.0"); !math.Signbit(got.(float64)) {
		t.Error("-0.0 lost its sign, which the previous decode kept")
	}
}

// plainNumber matches the numeric plain texts the property below draws from.
var plainNumber = regexp.MustCompile(`^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$|^0x[0-9a-fA-F]+$`)

// FuzzStrictJSON_AnAcceptedNumberKeepsItsValue: for any plain numeric text the
// mapping accepts, the text's exact value equals the exact value of
// encoding/json's encoding of the result — an oracle independent of the
// FormatFloat call the rule uses.
func FuzzStrictJSON_AnAcceptedNumberKeepsItsValue(f *testing.F) {
	for _, seed := range []string{"0", "-0", "42", "0x1F", "1.5", ".5", "1.", "0.1", "1.10", "1.0e+23", "9007199254740992",
		"9007199254740993.0", "0.30000000000000001", "1e-400", "5.0e-324", "1.7976931348623157e+308", "123456789.123456789"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		if !plainNumber.MatchString(text) {
			return
		}
		got, err := authored(t, text)
		if err != nil {
			return
		}
		number, ok := got.(float64)
		if !ok {
			t.Fatalf("%q accepted as %T", text, got)
		}
		encoded, err := json.Marshal(number)
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		written, ok := new(big.Rat).SetString(string(encoded))
		if !ok {
			t.Fatalf("encoding/json wrote %s", encoded)
		}
		if authoredValue := ratOf(text); authoredValue == nil || authoredValue.Cmp(written) != 0 {
			t.Fatalf("%q was accepted but JSON writes %s", text, encoded)
		}
	})
}

func ratOf(text string) *big.Rat {
	t := strings.TrimPrefix(text, "+")
	negative := strings.HasPrefix(t, "-")
	t = strings.TrimPrefix(t, "-")
	var r *big.Rat
	if strings.HasPrefix(t, "0x") {
		n, ok := new(big.Int).SetString(t[2:], 16)
		if !ok {
			return nil
		}
		r = new(big.Rat).SetInt(n)
	} else {
		if strings.HasPrefix(t, ".") {
			t = "0" + t
		}
		t = strings.Replace(strings.Replace(t, ".e", ".0e", 1), ".E", ".0E", 1)
		t = strings.TrimSuffix(t, ".")
		var ok bool
		if r, ok = new(big.Rat).SetString(t); !ok {
			return nil
		}
	}
	if negative {
		r.Neg(r)
	}
	return r
}

func TestStrictJSON_RefusesTagsAnchorsAliasesAndMergeKeys(t *testing.T) {
	for _, tc := range []struct{ source, path, reason string }{
		{"a: !!str 1\n", "with.a", "the tag !!str is not allowed in authored data; write the value plainly, or quote it for a string"},
		{"a: !custom x\n", "with.a", "the tag !custom is not allowed in authored data; write the value plainly, or quote it for a string"},
		{"a: &base {k: 1}\n", "with.a", "the anchor &base is not allowed in authored data; write the value out where it is used"},
		{"a: &base 1\nb: *base\n", "with.a", "the anchor &base is not allowed in authored data; write the value out where it is used"},
		{"<<: {k: 1}\n", "with.<<", "a merge key (<<) is not allowed in authored data; write the keys out"},
		{"a: <<\n", "with.a", "a merge key (<<) is not allowed in authored data; write the keys out"},
	} {
		_, err := authoredDoc(t, tc.source)
		var refused *dsl.AuthoredDataError
		if !errors.As(err, &refused) || refused.Path != tc.path || refused.Reason != tc.reason {
			t.Errorf("%q = %v, want %s: %q", tc.source, err, tc.path, tc.reason)
		}
	}
	// An alias of an anchor defined outside the data position, used inside it.
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte("description: &d x\nwith:\n  k: *d\n"), &doc); err != nil {
		t.Fatal(err)
	}
	_, err := dsl.AuthoredJSON(doc.Content[0].Content[3], "with")
	var refused *dsl.AuthoredDataError
	if !errors.As(err, &refused) || refused.Path != "with.k" ||
		refused.Reason != "the alias *d is not allowed in authored data; write the value out where it is used" {
		t.Errorf("alias = %v", err)
	}
}

func TestStrictJSON_KeysAreTheirText(t *testing.T) {
	got, err := authoredDoc(t, "200: a\n-1: b\ntrue: c\nfalse: d\n\"yes\": e\n'0755': f\n")
	want := map[string]any{"200": "a", "-1": "b", "true": "c", "false": "d", "yes": "e", "0755": "f"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %#v, %v; want %#v", got, err, want)
	}
	for _, tc := range []struct{ source, path, reason string }{
		{"~: a\n", "with.~", "the key ~ is null, and a JSON object key is a string; quote it ('~') for the key '~'"},
		{"yes: a\n", "with.yes", "the key " + fmt.Sprintf(r1, "yes", "yes")},
		{"True: a\n", "with.True", "the key True is the boolean true to YAML readers, and a JSON object key is a string; write true, or quote it ('True') for the key 'True'"},
		{"1.0: a\n", "with.1.0", "the key 1.0 is the number 1 to YAML readers, and a JSON object key is a string; write 1, or quote it ('1.0') for the key '1.0'"},
		{"0755: a\n", "with.0755", "the key " + fmt.Sprintf(r2, "0755", "493", "755", "0755")},
		{"2026-01-02: a\n", "with.2026-01-02", "the key " + fmt.Sprintf(r5, "2026-01-02", "2026-01-02")},
		{"[a]: b\n", "with", "a key must be a string, got a list"},
		{"1: a\n\"1\": b\n", "with.1", `the key "1" is written twice (first at line 1)`},
	} {
		_, err := authoredDoc(t, tc.source)
		var refused *dsl.AuthoredDataError
		if !errors.As(err, &refused) || refused.Path != tc.path || refused.Reason != tc.reason {
			t.Errorf("%q = %v\nwant %s: %q", tc.source, err, tc.path, tc.reason)
		}
	}
	// Keys in different mappings never collide.
	if _, err := authoredDoc(t, "l: [{a: 1}, {a: 2}]\nm: {a: 1}\nn2: {a: 2}\n"); err != nil {
		t.Errorf("sibling mappings collided: %v", err)
	}
}

// agreementSeeds are the D3 table's strings and every example of the rule.
var agreementSeeds = []string{
	"yes", "on", "0755", "1_000", "0b101", "1:30", "2026-01-02", ".inf", "9007199254740993", "0o17", "1e3", "0X1F",
	"null", "~", "", "true", "False", "0", "-0", "+12", "42", "0x1F", "1.5", ".5", "1.", "1.0", "01.5", "1.5e+3",
	"1.5E-3", "9007199254740992", "1.2.3", "10.0.0.1", "v1", "3d", "inf", "nUll", "_1", "https://x/y", "No", "OFF",
	"y", "n", "yEs", "2026-1-2", "-.Inf", ".NaN", "1e400", "-9007199254740993", "0x20000000000001",
	"9007199254740993.0", "0.30000000000000001", "1.0e-400", "4.9e-324", "-017", "12:30", "-0x1F", "+0x1F",
	"1:30.5", "1.5e3", "-.5", "08", "-0.0", "1.0e+23", "0.1", "1.10", "123456789012345678901234567890",
}

// requireAgreement holds AuthoredJSON to the previous decode on one
// document: equal JSON, or a refusal — never a different value.
func requireAgreement(t *testing.T, source string) {
	t.Helper()
	old, oldErr := legacyDecode(source)
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(source), &doc); err != nil || len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return
	}
	got, err := dsl.AuthoredJSON(doc.Content[0], "with")
	if err != nil {
		return // a refusal is always allowed
	}
	if oldErr != nil {
		t.Fatalf("%q: the previous decode refused it (%v) and the strict mapping accepted %#v", source, oldErr, got)
	}
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(got)
	if string(a) != string(b) {
		t.Fatalf("%q: the previous decode gave %s, the strict mapping %s", source, a, b)
	}
	if !sameSigns(old, got) {
		t.Fatalf("%q: a zero changed sign", source)
	}
}

func sameSigns(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Signbit(x) == math.Signbit(y)
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range x {
			if !sameSigns(v, y[k]) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameSigns(x[i], y[i]) {
				return false
			}
		}
	}
	return true
}

func TestStrictJSON_AgreesWithThePreviousDecodeOrRefuses(t *testing.T) {
	for _, text := range agreementSeeds {
		requireAgreement(t, "v: "+text+"\n")
		if text != "" {
			requireAgreement(t, text+": v\n")
		}
		requireAgreement(t, "l: ["+text+"]\nm:\n  k: "+text+"\n")
	}
	requireAgreement(t, "a: 1\na: 2\n")
	requireAgreement(t, "1: a\n\"1\": b\n")
	requireAgreement(t, "a: &x 1\nb: *x\n")
	requireAgreement(t, "base: &b {k: 1}\nother:\n  <<: *b\n  j: 2\n")
}

func FuzzStrictJSON_AgreesWithThePreviousDecodeOrRefuses(f *testing.F) {
	for i, text := range agreementSeeds {
		f.Add(text, agreementSeeds[(i+7)%len(agreementSeeds)], uint8(i))
	}
	f.Fuzz(func(t *testing.T, a, b string, shape uint8) {
		var source string
		switch shape % 5 {
		case 0:
			source = "v: " + a + "\nw: " + b + "\n"
		case 1:
			source = a + ": " + b + "\n"
		case 2:
			source = "l:\n  - " + a + "\n  - " + b + "\n"
		case 3:
			source = "m:\n  " + a + ": 1\n  " + b + ": 2\n"
		case 4:
			source = "n:\n  k:\n    " + a + ": " + b + "\n"
		}
		requireAgreement(t, source)
	})
}

// position compiles a minimal Orkflow whose one data position is written as
// value, and returns the error's field and text.
func positionDocument(position, value string) string {
	switch position {
	case "inputs":
		return "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs: " + value + "\ntasks:\n  - id: a\n    uses: shell\n"
	case "with":
		return "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ndescription: &b x\ntasks:\n  - id: a\n    uses: shell\n    with: " + value + "\n"
	default: // await.schema
		return "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ndescription: &b x\ntasks:\n  - id: a\n    await:\n      timeout_seconds: 60\n      schema: " + value + "\n"
	}
}

func TestCompile_DataPositionsUseTheStrictMapping(t *testing.T) {
	for _, tc := range []struct{ source, field, prefix string }{
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  retries:\n    type: boolean\n    default: yes\ntasks:\n  - id: a\n    uses: shell\n",
			"inputs.retries.default", "line 7: yes is a boolean to YAML 1.1 readers"},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: a\n    uses: shell\n  - id: b\n    uses: shell\n    with:\n      flags:\n        verbose: on\n",
			"tasks[1].with.flags.verbose", "line 11: on is a boolean to YAML 1.1 readers"},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: a\n    uses: shell\n  - id: b\n    uses: shell\n  - id: c\n    await:\n      timeout_seconds: 60\n      schema:\n        ok: {type: boolean, required: yes}\n",
			"tasks[2].await.schema.ok.required", "line 13: yes is a boolean to YAML 1.1 readers"},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: a\n    uses: orkflow:block@1.0.0\n    with:\n      when: 2026-01-02\n",
			"tasks[0].with.when", "line 8: 2026-01-02 is a timestamp to some YAML readers"},
	} {
		_, err := dsl.Compile([]byte(tc.source))
		if err == nil || fieldOf(err) != tc.field || !strings.HasPrefix(err.Error(), tc.prefix) {
			t.Errorf("%s: err = %v (field %q), want %s starting %q", tc.field, err, fieldOf(err), tc.field, tc.prefix)
		}
	}
}

func TestCompile_DataPositionsAreReadAsWritten(t *testing.T) {
	type row struct {
		value, field, message string // message empty: accepted
	}
	accepted := map[string][]string{
		"inputs":       {"{a: {type: number}}", "{l: {type: array, default: [1, {b: c}]}}", "{}"},
		"with":         {"{a: 1}", "{l: [1, {b: c}]}", "{}"},
		"await.schema": {"{a: {type: number}}", "{l: {type: boolean, required: true}}", "{}"},
	}
	for position, values := range accepted {
		for _, value := range values {
			if _, err := dsl.Compile([]byte(positionDocument(position, value))); err != nil {
				t.Errorf("%s: %s refused: %v", position, value, err)
			}
		}
	}
	paths := map[string]string{"inputs": "inputs", "with": "tasks[0].with", "await.schema": "tasks[0].await.schema"}
	lines := map[string]int{"inputs": 4, "with": 8, "await.schema": 9}
	for position, path := range paths {
		line := lines[position]
		for _, tc := range []row{
			{"[1]", path, fmt.Sprintf("line %d: %s must be a mapping of key: value, got a list", line, position)},
			{"yes", path, fmt.Sprintf(`line %d: %s must be a mapping of key: value, got "yes"`, line, position)},
			{"1", path, fmt.Sprintf("line %d: %s must be a mapping of key: value, got the number 1", line, position)},
			{"!!str x", path, fmt.Sprintf("line %d: the tag !!str is not allowed in authored data; write the value plainly, or quote it for a string", line)},
			{"!!null", path, fmt.Sprintf("line %d: the tag !!null is not allowed in authored data; write the value plainly, or quote it for a string", line)},
			{"!!map {k: 1}", path, fmt.Sprintf("line %d: the tag !!map is not allowed in authored data; write the value plainly, or quote it for a string", line)},
			{"&a {k: 1}", path, fmt.Sprintf("line %d: the anchor &a is not allowed in authored data; write the value out where it is used", line)},
		} {
			if position == "inputs" && strings.HasPrefix(tc.value, "&") {
				continue // no anchor is defined above inputs: in its document
			}
			_, err := dsl.Compile([]byte(positionDocument(position, tc.value)))
			if err == nil || fieldOf(err) != tc.field || err.Error() != tc.message {
				t.Errorf("%s: %s = %v (field %q)\nwant %s: %q", position, tc.value, err, fieldOf(err), tc.field, tc.message)
			}
		}
		if position == "inputs" {
			continue
		}
		for _, tc := range []row{
			{"*b", path, fmt.Sprintf("line %d: the alias *b is not allowed in authored data; write the value out where it is used", line)},
			{"{k: *b}", path + ".k", fmt.Sprintf("line %d: the alias *b is not allowed in authored data; write the value out where it is used", line)},
		} {
			_, err := dsl.Compile([]byte(positionDocument(position, tc.value)))
			if err == nil || fieldOf(err) != tc.field || err.Error() != tc.message {
				t.Errorf("%s: %s = %v (field %q)\nwant %s: %q", position, tc.value, err, fieldOf(err), tc.field, tc.message)
			}
		}
	}
}

func TestCompile_DataPositionsRefuseADuplicateKey(t *testing.T) {
	for _, tc := range []struct{ source, field, message string }{
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  a: {type: string}\n  a: {type: number}\ntasks:\n  - id: t\n    uses: shell\n",
			"inputs.a", `line 6: the key "a" is written twice (first at line 5)`},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with:\n      a: 1\n      a: 2\n",
			"tasks[0].with.a", `line 9: the key "a" is written twice (first at line 8)`},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n      schema:\n        a: {type: string}\n        a: {type: number}\n",
			"tasks[0].await.schema.a", `line 10: the key "a" is written twice (first at line 9)`},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with:\n      m:\n        p:\n          a: 1\n          a: 2\n",
			"tasks[0].with.m.p.a", `line 11: the key "a" is written twice (first at line 10)`},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with: {l: [{a: 1, a: 2}]}\n",
			"tasks[0].with.l[0].a", `line 7: the key "a" is written twice (first at line 7)`},
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with:\n      1: a\n      \"1\": b\n",
			"tasks[0].with.1", `line 9: the key "1" is written twice (first at line 8)`},
	} {
		_, err := dsl.Compile([]byte(tc.source))
		if err == nil || fieldOf(err) != tc.field || err.Error() != tc.message {
			t.Errorf("err = %v (field %q)\nwant %s: %q", err, fieldOf(err), tc.field, tc.message)
		}
	}
	for _, ok := range []string{
		"apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with: {l: [{a: 1}, {a: 2}], m: {a: 1}, o: {a: 2}}\n",
	} {
		if _, err := dsl.Compile([]byte(ok)); err != nil {
			t.Errorf("sibling mappings refused: %v", err)
		}
	}
	_, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n  - id: t\n    uses: shell\n    with: {a: 1}\n    with: {b: 2}\n"))
	if err == nil || !strings.Contains(err.Error(), `mapping key "with" already defined at line 7`) {
		t.Errorf("a second with: = %v, want yaml.v3's own refusal", err)
	}
}

// FuzzAuthoredNode_RoundTrips: what an export writes reads back to the same
// value.
func FuzzAuthoredNode_RoundTrips(f *testing.F) {
	for _, seed := range agreementSeeds {
		f.Add(seed, 0.0)
	}
	for _, n := range []float64{0, math.Copysign(0, -1), 1 << 53, 1<<53 + 2, 1e21, 1e-7, 0.1, 1.5, -2.25e-9, 1.7976931348623157e+308} {
		f.Add("x", n)
	}
	f.Fuzz(func(t *testing.T, s string, n float64) {
		if math.IsNaN(n) || math.IsInf(n, 0) || !utf8Valid(s) {
			return
		}
		value := map[string]any{"s": s, "n": n, s + "k": []any{s, n, nil, true, map[string]any{}}, "e": []any{}}
		out, err := yaml.Marshal(dsl.AuthoredNode(value))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(out, &doc); err != nil {
			t.Fatalf("parse %q: %v", out, err)
		}
		got, err := dsl.AuthoredJSON(doc.Content[0], "input")
		if err != nil {
			t.Fatalf("%q: %v", out, err)
		}
		a, _ := json.Marshal(value)
		b, _ := json.Marshal(got)
		if string(a) != string(b) || !sameSigns(value, got) {
			t.Fatalf("round trip: %s → %q → %s", a, out, b)
		}
	})
}

func utf8Valid(s string) bool { return strings.ToValidUTF8(s, "\uFFFD") == s }
