package main

import (
	"encoding/json"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// What the CLI sends is what the user typed. A copied command must reproduce
// exactly what was submitted (docs/JSON.md), so each flag parser below is
// held to reading its text the one way its help says, and to refusing with a
// message instead of failing.

// FuzzBuildInputPayload: --input-json is one JSON object, each --input is
// key=value whose value keeps its JSON type when it is valid JSON and is a
// string otherwise, and a pair wins over the same key in the object. Nothing
// a shell can pass makes it panic, and an empty result is no payload.
func FuzzBuildInputPayload(f *testing.F) {
	for _, seed := range []string{``, `{}`, `{"a":1}`, `{"a":1,"b":{"c":[1,2]}}`, ` {"a":"x"} `, `null`, `[]`, `"x"`, `12`, `{`, `{"a":1}{"b":2}`, "{\"a\":\"\xff\"}", `{"a":1e999}`} {
		f.Add(seed, "k=v", "n=1", "")
		f.Add(seed, "a=2", "b={\"x\":1}", "z=")
		f.Add(seed, "=v", "novalue", "k=true")
		f.Add(seed, "t=\"s\"", "u=[1,", "w=null")
	}
	f.Fuzz(func(t *testing.T, inputJSON, p1, p2, p3 string) {
		var pairs []string
		for _, p := range []string{p1, p2, p3} {
			if p != "" {
				pairs = append(pairs, p)
			}
		}
		payload, err := buildInputPayload(pairs, inputJSON)
		if err != nil {
			if payload != nil || err.Error() == "" {
				t.Fatalf("buildInputPayload(%q, %q) = %v, %v", pairs, inputJSON, payload, err)
			}
			return
		}
		want := map[string]any{}
		if trimmed := strings.TrimSpace(inputJSON); trimmed != "" {
			var object map[string]any
			if json.Unmarshal([]byte(trimmed), &object) != nil || object == nil {
				t.Fatalf("buildInputPayload(%q, %q) accepted an --input-json that is not a JSON object", pairs, inputJSON)
			}
			want = object
		}
		for _, pair := range pairs {
			key, raw, found := strings.Cut(pair, "=")
			if !found || key == "" {
				t.Fatalf("buildInputPayload(%q) accepted the malformed pair %q", pairs, pair)
			}
			var decoded any
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
				decoded = raw
			}
			want[key] = decoded
		}
		if len(want) == 0 {
			if payload != nil {
				t.Fatalf("buildInputPayload(%q, %q) = %v, want no payload", pairs, inputJSON, payload)
			}
			return
		}
		if payload == nil || !reflect.DeepEqual(payload.GetData().AsMap(), want) {
			t.Fatalf("buildInputPayload(%q, %q) = %v, want %v", pairs, inputJSON, payload, want)
		}
	})
}

// FuzzAgainInput: the new run's input is the source run's, then --input-json,
// then --input, and every key it did not override survives.
func FuzzAgainInput(f *testing.F) {
	for _, seed := range []string{``, `{}`, `{"a":2}`, `null`, `[]`, `{"new":true}`} {
		f.Add(seed, "a=3", "keep")
	}
	f.Fuzz(func(t *testing.T, rawJSON, pair, sourceKey string) {
		source, err := structpb.NewStruct(map[string]any{"a": 1.0, "b": "kept"})
		if err != nil {
			t.Fatal(err)
		}
		if sourceKey != "" && sourceKey != "a" && sourceKey != "b" {
			source.Fields[sourceKey] = structpb.NewStringValue("s")
		}
		var pairs []string
		if pair != "" {
			pairs = []string{pair}
		}
		payload, err := againInput(&orkstackv1.Payload{Data: source}, pairs, rawJSON)
		if err != nil {
			if payload != nil || err.Error() == "" {
				t.Fatalf("againInput(%q, %q) = %v, %v", pairs, rawJSON, payload, err)
			}
			return
		}
		got := payload.GetData().AsMap()
		for key, value := range source.AsMap() {
			if _, overridden := got[key]; !overridden {
				t.Fatalf("againInput(%q, %q) lost the source key %q = %v", pairs, rawJSON, key, value)
			}
		}
	})
}

// FuzzShellQuote: the commands orkctl prints for a person to paste (the next
// page, the equivalent --filter) quote every argument so that a shell reads
// back exactly the bytes. Proven against a real shell.
func FuzzShellQuote(f *testing.F) {
	for _, seed := range []string{
		"", "plain", "it's", "a b", "$(id)", "`id`", "a;b", "a\nb", "-n", "'", "''", "\\", "*", "~", "!", "${HOME}", "name~zz", "status=FAILED,CANCELED",
		"labels.team=pay ments", "\xff", "tab\there", "#", "a=b", "x'y'z",
	} {
		f.Add(seed)
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		f.Fatalf("shell quoting is proven against a real shell: %v", err)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if strings.IndexByte(value, 0) >= 0 {
			return // a NUL byte cannot be part of a command line
		}
		for name, quote := range map[string]func(string) string{"shellQuote": shellQuote, "shellArg": shellArg} {
			quoted := quote(value)
			script := `set -- ` + quoted + `; [ "$#" -eq 1 ] || exit 91; printf '%s' "$1"`
			out, err := exec.Command(shell, "-c", script).CombinedOutput()
			if err != nil {
				t.Fatalf("%s(%q) = %s, which the shell rejected: %v: %s", name, value, quoted, err, out)
			}
			if string(out) != value {
				t.Fatalf("%s(%q) = %s, which the shell read as %q", name, value, quoted, out)
			}
		}
	})
}

// FuzzParseKeyValuePairs: key=value splits at the first "=", an empty key or a
// missing "=" is refused naming the flag, and a later pair of the same key
// wins.
func FuzzParseKeyValuePairs(f *testing.F) {
	for _, seed := range []string{"team=payments", "a=b=c", "=v", "k", "k=", "", "a=b\nc"} {
		f.Add(seed, "team=x")
	}
	f.Fuzz(func(t *testing.T, first, second string) {
		got, err := parseKeyValuePairs("label", []string{first, second})
		want := map[string]string{}
		malformed := false
		for _, pair := range []string{first, second} {
			key, value, found := strings.Cut(pair, "=")
			if !found || key == "" {
				malformed = true
				break
			}
			want[key] = value
		}
		if malformed || err != nil {
			if malformed != (err != nil) || err != nil && !strings.Contains(err.Error(), "--label") {
				t.Fatalf("parseKeyValuePairs(%q, %q) = %v, %v; malformed pair: %v", first, second, got, err, malformed)
			}
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("parseKeyValuePairs(%q, %q) = %v, want %v", first, second, got, want)
		}
	})
}

// FuzzParseSince: a window is a positive duration looking backwards or an
// instant, never both and never neither, and anything else is refused with the
// two forms spelled out.
func FuzzParseSince(f *testing.F) {
	for _, seed := range []string{"6h", "90m", "168h", "0", "0s", "-1h", "2026-05-01", "2026-05-01T09:00:00Z", "2026-05-01T09:00:00+02:00", "", "1d", "9999-12-31T23:59:59-23:59", "1e3h", "9223372036854775807ns"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		lookback, at, err := parseSince(raw)
		if err != nil {
			if lookback != 0 || !at.IsZero() || !strings.Contains(err.Error(), "--since") {
				t.Fatalf("parseSince(%q) = %v, %v, %v", raw, lookback, at, err)
			}
			return
		}
		if (lookback > 0) == !at.IsZero() {
			t.Fatalf("parseSince(%q) = %v, %v: exactly one of a window and an instant", raw, lookback, at)
		}
		if lookback < 0 || at.Location() != time.UTC && !at.IsZero() {
			t.Fatalf("parseSince(%q) = %v, %v", raw, lookback, at)
		}
		if !at.IsZero() && !timestamppb.New(at).IsValid() {
			t.Fatalf("parseSince(%q) = %v, an instant the wire cannot carry", raw, at)
		}
	})
}

// FuzzParseInstantFlags: --until and the run list's RFC 3339 bounds read an
// instant, and the instant they return is one a request can carry: UTC, inside
// the protobuf timestamp range, and the one the text names.
func FuzzParseInstantFlags(f *testing.F) {
	for _, seed := range []string{
		"2026-05-01", "2026-05-01T09:00:00Z", "2026-05-01T09:00:00.123456789Z", "2026-05-01T09:00:00+02:00", "2026-05-01T09:00:00,5Z", "0001-01-01T00:00:00Z",
		"0001-01-01T00:00:00+01:00", "0000-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "9999-12-31T23:59:59-23:59", "9999-12-31T23:59:59.999999999Z", "6h", "", "now",
		"2026-02-30", "2026-05-01T24:00:00Z",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if at, err := parseUntil(raw); err == nil {
			if at.Location() != time.UTC || !timestamppb.New(at).IsValid() {
				t.Fatalf("parseUntil(%q) = %v, an instant the wire cannot carry", raw, at)
			}
		} else if !strings.Contains(err.Error(), "--until") {
			t.Fatalf("parseUntil(%q) refused as %q, which does not name the flag", raw, err)
		}
		bound, err := parseRFC3339Bound("--created-after", raw)
		switch {
		case err != nil:
			if bound != nil || !strings.Contains(err.Error(), "--created-after") {
				t.Fatalf("parseRFC3339Bound(%q) = %v, %v", raw, bound, err)
			}
		case raw == "":
			if bound != nil {
				t.Fatalf("parseRFC3339Bound(\"\") = %v, want no bound", bound)
			}
		case !bound.IsValid():
			t.Fatalf("parseRFC3339Bound(%q) = %v, an instant the wire cannot carry", raw, bound)
		}
	})
}

// FuzzParseRunRetentionFlag: blank and "inherit" set nothing, anything else is
// a positive duration or a refusal that names the flag.
func FuzzParseRunRetentionFlag(f *testing.F) {
	for _, seed := range []string{"", "inherit", "168h", "24h30m", "0", "-1h", "1.5h", "forever", "9223372036854775807ns", "100000h"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		d, err := parseRunRetentionFlag(raw)
		switch {
		case err != nil:
			if d != nil || !strings.Contains(err.Error(), "--run-retention") {
				t.Fatalf("parseRunRetentionFlag(%q) = %v, %v", raw, d, err)
			}
		case raw == "" || raw == "inherit":
			if d != nil {
				t.Fatalf("parseRunRetentionFlag(%q) = %v, want inherit", raw, d)
			}
		default:
			if d == nil || d.AsDuration() <= 0 {
				t.Fatalf("parseRunRetentionFlag(%q) = %v, want a positive duration", raw, d)
			}
		}
	})
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c - 'A' + 'a'
		}
	}
	return string(b)
}

// FuzzParseGroupBy: --group-by is a comma-separated list of dimensions, each
// read in ASCII case with its spaces trimmed, none named twice, in the order
// written; the flake index also refuses the two it reports as a group's
// dominant cause. Whatever else is refused is refused naming every spelling.
func FuzzParseGroupBy(f *testing.F) {
	for _, seed := range []string{
		"orkflow", "task,pool", "Task", " TASK , Pool ", "code,category", "task,task", "task,Task", "", ",", "task,", ",task", "colour",
		"taſk", "ımage", "phaſe,orker", "namespace", "unspecified", "orkflow,task,code,category,image,pool,phase,orker",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		known := map[string]bool{}
		for _, spelling := range groupBySpellings() {
			known[spelling] = true
		}
		for _, parse := range []struct {
			name    string
			fn      func(string) ([]orkstackv1.FailureGroupBy, error)
			refused map[string]bool
		}{
			{"parseGroupBy", parseGroupBy, nil},
			{"parseFlakeGroupBy", parseFlakeGroupBy, map[string]bool{"code": true, "category": true}},
		} {
			var want []string
			wantOK, seen := true, map[string]bool{}
			for _, part := range strings.Split(raw, ",") {
				word := lowerASCII(strings.TrimSpace(part))
				if !known[word] || seen[word] || parse.refused[word] {
					wantOK = false
					break
				}
				seen[word] = true
				want = append(want, word)
			}
			got, err := parse.fn(raw)
			if (err == nil) != wantOK {
				t.Fatalf("%s(%q) = %v, %v; the rules say accepted: %v", parse.name, raw, got, err, wantOK)
			}
			if err != nil {
				if !strings.Contains(err.Error(), "--group-by") {
					t.Fatalf("%s(%q) refused as %q, which does not name the flag", parse.name, raw, err)
				}
				continue
			}
			spelled := make([]string, len(got))
			for i, dim := range got {
				spelled[i] = groupBySpelling(dim)
			}
			if !reflect.DeepEqual(spelled, want) {
				t.Fatalf("%s(%q) = %v, want %v", parse.name, raw, spelled, want)
			}
		}
	})
}

// FuzzParseFailureCategory: --category is one failure category, in ASCII case,
// with or without the enum's prefix once, and nothing else; the refusal
// names every category.
func FuzzParseFailureCategory(f *testing.F) {
	for _, seed := range []string{
		"activity_failed", "INFRASTRUCTURE_ERROR", "failure_category_contract_violation", "Timed_Out", "cancelled", "canceled", "unspecified",
		"failure_category_unspecified", "FAILURE_CATEGORY_FAILURE_CATEGORY_TIMED_OUT", " timed_out ", "", "infraſtructure_error", "tımed_out",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		known := map[string]orkstackv1.FailureCategory{}
		for _, spelling := range categorySpellings() {
			known[spelling] = orkstackv1.FailureCategory(orkstackv1.FailureCategory_value[categoryPrefix+strings.ToUpper(spelling)])
		}
		word := strings.TrimPrefix(lowerASCII(strings.TrimSpace(raw)), strings.ToLower(categoryPrefix))
		want, wantOK := known[word]
		got, err := parseFailureCategory(raw)
		if (err == nil) != wantOK || err == nil && got != want {
			t.Fatalf("parseFailureCategory(%q) = %v, %v; the rules say %v, accepted: %v", raw, got, err, want, wantOK)
		}
		if err != nil {
			for spelling := range known {
				if !strings.Contains(err.Error(), spelling) {
					t.Fatalf("parseFailureCategory(%q) refused as %q, which does not name %s", raw, err, spelling)
				}
			}
		}
	})
}

// FuzzBuildAssumedOutputs: a planned task's assumed output is a JSON object
// given as TASK=JSON or in a file keyed by task id, a pair wins over the file,
// and text Go's decoder would change on the way through is refused, so a
// replacement character reaches the plan only where the text spells one.
func FuzzBuildAssumedOutputs(f *testing.F) {
	for _, seed := range []string{
		`flaky={"exitCode":1}`, `flaky={"a":{"b":[1,"x",null]}}`, `flaky=[1]`, `flaky`, `=1`, `flaky=null`, `flaky={"a":"x\ud800y"}`, "flaky={\"a\":\"p\xffq\"}",
		`flaky={"a":"😀"}`, `flaky={"a":"�"}`, `a=b={}`, `flaky={}{}`, `flaky={"a":1e999}`,
	} {
		f.Add(seed, `{"flaky":{"exitCode":2},"warmup":{"ok":true}}`)
		f.Add(seed, ``)
	}
	f.Add(``, `{"flaky":{"a":"x\ud800y"}}`)
	f.Add(``, `{"flaky":3}`)
	f.Add(``, `null`)
	f.Add(``, `{"flaky":null}`)
	spellsReplacement := func(text string) bool {
		return strings.Contains(text, "�") || strings.Contains(strings.ToLower(text), `�`)
	}
	f.Fuzz(func(t *testing.T, pair, file string) {
		var pairs []string
		if pair != "" {
			pairs = []string{pair}
		}
		path := ""
		if file != "" {
			path = writePlanFile(t, "outputs.json", file)
		}
		got, err := buildAssumedOutputs(pairs, path)
		if err != nil {
			if got != nil || err.Error() == "" {
				t.Fatalf("buildAssumedOutputs(%q, %q) = %v, %v", pairs, file, got, err)
			}
			return
		}
		for id, payload := range got {
			if id == "" || payload.GetData() == nil {
				t.Fatalf("buildAssumedOutputs(%q, %q) holds %q: %v", pairs, file, id, payload)
			}
			if !utf8.ValidString(id) {
				t.Fatalf("buildAssumedOutputs(%q, %q) holds the task id %q, which is not text", pairs, file, id)
			}
			encoded, err := payload.GetData().MarshalJSON()
			if err != nil {
				t.Fatalf("the assumed output of %q does not encode: %v", id, err)
			}
			if strings.Contains(string(encoded), "�") && !spellsReplacement(pair) && !spellsReplacement(file) {
				t.Fatalf("buildAssumedOutputs(%q, %q) put a replacement character in %s without the text spelling one", pairs, file, encoded)
			}
		}
		if id, _, found := strings.Cut(pair, "="); pair != "" && found && got[id] == nil {
			t.Fatalf("buildAssumedOutputs(%q, %q) lost the pair for %q: %v", pairs, file, id, got)
		}
	})
}
