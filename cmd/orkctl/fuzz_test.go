package main

import (
	"encoding/json"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

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
