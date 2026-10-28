package main

import (
	"strings"
	"testing"
)

// A spelling is read in ASCII case only. Unicode upper-casing maps U+017F
// (long s) to S and U+0131 (dotless i) to I, so a word that spells no
// dimension or category would read as one.
func TestFlagSpellings_AreReadInASCIICaseOnly(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"a dimension with a long s", "taſk"},
		{"a dimension with a dotless i", "ımage"},
		{"a later dimension with a long s", "orkflow,phaſe"},
	} {
		t.Run("group-by "+tc.name, func(t *testing.T) {
			if dims, err := parseGroupBy(tc.raw); err == nil {
				t.Errorf("parseGroupBy(%q) = %v, want it refused", tc.raw, dims)
			}
			if dims, err := parseFlakeGroupBy(tc.raw); err == nil {
				t.Errorf("parseFlakeGroupBy(%q) = %v, want it refused", tc.raw, dims)
			}
		})
	}
	for _, raw := range []string{"infraſtructure_error", "tımed_out", "FAILURE_CATEGORY_tımed_out"} {
		if category, err := parseFailureCategory(raw); err == nil {
			t.Errorf("parseFailureCategory(%q) = %v, want it refused", raw, category)
		}
	}
	for _, raw := range []string{"Task", "TASK,Orkflow", " pool "} {
		if _, err := parseGroupBy(raw); err != nil {
			t.Errorf("parseGroupBy(%q): %v", raw, err)
		}
	}
	if !groupsByNamespace(" Namespace ") || groupsByNamespace("namespaſe") || groupsByNamespace("namespace,task") {
		t.Error("--group-by namespace is the ASCII word namespace, in any ASCII case, alone")
	}
}

// --output and --outputs-file are read the way --input-json is: text Go's
// decoder would change on the way through is refused, not replaced.
func TestBuildAssumedOutputs_RefusesTextTheDecoderWouldChange(t *testing.T) {
	for _, tc := range []struct{ name, pair string }{
		{"a lone surrogate escape", `flaky={"a":"x\ud800y"}`},
		{"a low surrogate escape", `flaky={"a":"\udc00"}`},
		{"bytes that are not UTF-8", "flaky={\"a\":\"p\xffq\"}"},
	} {
		t.Run("--output "+tc.name, func(t *testing.T) {
			if got, err := buildAssumedOutputs([]string{tc.pair}, ""); err == nil {
				t.Errorf("buildAssumedOutputs(%q) = %v, want it refused", tc.pair, got)
			}
		})
		t.Run("--outputs-file "+tc.name, func(t *testing.T) {
			_, body, _ := strings.Cut(tc.pair, "=")
			file := writePlanFile(t, "outputs.json", `{"flaky":`+body+`}`)
			if got, err := buildAssumedOutputs(nil, file); err == nil {
				t.Errorf("buildAssumedOutputs from %q = %v, want it refused", body, got)
			}
		})
	}
	// The pair refuses an empty task id; the file's key is held to the same.
	if got, err := buildAssumedOutputs(nil, writePlanFile(t, "empty-id.json", `{"":{}}`)); err == nil {
		t.Errorf("an --outputs-file keyed by an empty task id = %v, want it refused as the pair is", got)
	}
	got, err := buildAssumedOutputs([]string{`flaky={"a":"😀"}`}, "")
	if err != nil || got["flaky"].GetData().GetFields()["a"].GetStringValue() != "\U0001F600" {
		t.Errorf("a surrogate pair is a character: %v, %v", got, err)
	}
}

// A time zone offset can push a valid local time past either end of the range
// a request carries (0001-01-01 to 9999-12-31 UTC), where it would fail to
// encode with no word about the flag. Each time flag refuses it by name.
func TestInstantFlags_RefuseAnInstantTheWireCannotCarry(t *testing.T) {
	for _, raw := range []string{"0000-01-01T00:00:00Z", "0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-23:59"} {
		if _, _, err := parseSince(raw); err == nil || !strings.Contains(err.Error(), "--since") {
			t.Errorf("parseSince(%q) = %v, want a refusal naming --since", raw, err)
		}
		if _, err := parseUntil(raw); err == nil || !strings.Contains(err.Error(), "--until") {
			t.Errorf("parseUntil(%q) = %v, want a refusal naming --until", raw, err)
		}
		if bound, err := parseRFC3339Bound("--created-after", raw); err == nil || !strings.Contains(err.Error(), "--created-after") {
			t.Errorf("parseRFC3339Bound(%q) = %v, %v, want a refusal naming the flag", raw, bound, err)
		}
	}
	for _, raw := range []string{"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z", "2026-05-01T09:00:00+02:00"} {
		if _, err := parseUntil(raw); err != nil {
			t.Errorf("parseUntil(%q) = %v, want the instant", raw, err)
		}
	}
}
