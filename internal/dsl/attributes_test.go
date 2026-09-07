package dsl_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// TestValidateAttributeMap_TableDriven pins ValidateAttributeMap's rules
// directly, independent of any YAML document: the entry-count bound, the key
// shape, the value size bound, and the value control-character rule.
func TestValidateAttributeMap_TableDriven(t *testing.T) {
	cases := []struct {
		name    string
		m       map[string]string
		max     int
		wantErr string // substring, or "" for no error
	}{
		{
			name: "empty map is fine",
			m:    nil,
			max:  32,
		},
		{
			name: "at the entry limit is fine",
			m:    repeatKeys(32),
			max:  32,
		},
		{
			name:    "over the entry limit is refused",
			m:       repeatKeys(33),
			max:     32,
			wantErr: "33",
		},
		{
			name:    "uppercase key is refused",
			m:       map[string]string{"Team": "payments"},
			max:     32,
			wantErr: "not",
		},
		{
			name:    "key starting with a digit is refused",
			m:       map[string]string{"1team": "payments"},
			max:     32,
			wantErr: "not",
		},
		{
			name: "key with dots, dashes and underscores is fine",
			m:    map[string]string{"team.sub-unit_1": "payments"},
			max:  32,
		},
		{
			name:    "value over 256 bytes is refused",
			m:       map[string]string{"team": strings.Repeat("x", 257)},
			max:     32,
			wantErr: "256",
		},
		{
			name: "value at exactly 256 bytes is fine",
			m:    map[string]string{"team": strings.Repeat("x", 256)},
			max:  32,
		},
		{
			name:    "value with an embedded newline is refused",
			m:       map[string]string{"team": "payments\ndrop table"},
			max:     32,
			wantErr: "control character",
		},
		{
			name:    "value with an embedded escape/DEL byte is refused",
			m:       map[string]string{"team": "payments\x1b[31m"},
			max:     32,
			wantErr: "control character",
		},
		{
			name: "value with ordinary punctuation is fine",
			m:    map[string]string{"team": "payments, eng-42 (on call)"},
			max:  32,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := dsl.ValidateAttributeMap("telemetry attribute", tc.m, tc.max)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateAttributeMap: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateAttributeMap: want an error naming %q, got none", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

func repeatKeys(n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("k%d", i)] = "v"
	}
	return m
}

// TestCompile_AcceptsFreeFormLabels is the reversal this test file records:
// `labels:` in a *.ork.yaml document is NOT held to ValidateAttributeMap (it
// no longer shares the namespace telemetry attribute shape) — a document
// with ordinary, widely-used label keys like "Team" and
// "app.kubernetes.io/name", and with far more than the old 32-entry bound,
// compiles without complaint. The export boundary that turns labels into
// telemetry attributes (internal/engine.telemetryFor) is what bounds and
// sanitizes them, not the compiler.
func TestCompile_AcceptsFreeFormLabels(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
name: free-form-labels
labels:
  Team: payments
  app.kubernetes.io/name: checkout
tasks:
  - id: a
    uses: shell
    with: {command: "echo 1"}
`))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got, want := wf.GetLabels()["Team"], "payments"; got != want {
		t.Errorf(`labels["Team"] = %q, want %q`, got, want)
	}
	if got, want := wf.GetLabels()["app.kubernetes.io/name"], "checkout"; got != want {
		t.Errorf(`labels["app.kubernetes.io/name"] = %q, want %q`, got, want)
	}
}

// TestCompile_Accepts40Labels checks that the old 32-entry MaxLabels bound is
// gone from the compiler: a document with 40 labels — over the removed bound
// — compiles cleanly.
func TestCompile_Accepts40Labels(t *testing.T) {
	wf, err := dsl.Compile([]byte(labelsDocument(40)))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got, want := len(wf.GetLabels()), 40; got != want {
		t.Fatalf("compiled %d labels, want %d", got, want)
	}
}

func labelsDocument(n int) string {
	var b strings.Builder
	b.WriteString("name: label-bounds\nlabels:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  k%d: \"v\"\n", i)
	}
	b.WriteString("tasks:\n  - id: a\n    uses: shell\n    with: {command: \"echo 1\"}\n")
	return b.String()
}

// TestSanitizeAttributeKey pins the fold: lowercase, every byte outside
// [a-z0-9_.-] becomes '_', a non-letter first character gets an "l_" prefix,
// and the result is capped at 64 bytes.
func TestSanitizeAttributeKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
	}{
		{name: "already valid", key: "team", want: "team"},
		{name: "uppercase folds to lowercase", key: "Team", want: "team"},
		{name: "kubernetes-style label passes dots and slashes", key: "app.kubernetes.io/name", want: "app.kubernetes.io_name"},
		{name: "spaces become underscores", key: "cost center", want: "cost_center"},
		{name: "leading digit gets an l_ prefix", key: "1team", want: "l_1team"},
		{name: "empty key gets an l_ prefix", key: "", want: "l_"},
		{name: "long key is truncated to 64 bytes", key: strings.Repeat("a", 100), want: strings.Repeat("a", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dsl.SanitizeAttributeKey(tc.key); got != tc.want {
				t.Errorf("SanitizeAttributeKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestSanitizeAttributeValue pins truncation to 256 bytes and control
// character stripping.
func TestSanitizeAttributeValue(t *testing.T) {
	if got, want := dsl.SanitizeAttributeValue("payments"), "payments"; got != want {
		t.Errorf("SanitizeAttributeValue(%q) = %q, want %q", "payments", got, want)
	}
	long := strings.Repeat("x", 300)
	if got, want := dsl.SanitizeAttributeValue(long), strings.Repeat("x", 256); got != want {
		t.Errorf("SanitizeAttributeValue(300 x's) truncated to %d bytes, want %d", len(got), len(want))
	}
	if got, want := dsl.SanitizeAttributeValue("payments\ndrop table"), "paymentsdrop table"; got != want {
		t.Errorf("SanitizeAttributeValue(%q) = %q, want %q", "payments\ndrop table", got, want)
	}
	if got, want := dsl.SanitizeAttributeValue("payments\x1b[31m\x7f"), "payments[31m"; got != want {
		t.Errorf("SanitizeAttributeValue with escape/DEL bytes = %q, want %q", got, want)
	}
}

// TestReservedAttributePrefixes_MatchesUI pins dsl.ReservedAttributePrefixes
// against ui/src/lib/namespaceRules.ts's own RESERVED_ATTRIBUTE_PREFIXES
// literal: the UI's client-side check and the engine's own validation must
// refuse exactly the same prefixes, or a namespace admin sees the UI accept
// an attribute the API then refuses (or the reverse).
func TestReservedAttributePrefixes_MatchesUI(t *testing.T) {
	got := uiReservedAttributePrefixes(t)
	want := append([]string(nil), dsl.ReservedAttributePrefixes...)
	if len(got) != len(want) {
		t.Fatalf("ui/src/lib/namespaceRules.ts RESERVED_ATTRIBUTE_PREFIXES = %v (%d entries), "+
			"dsl.ReservedAttributePrefixes = %v (%d entries)", got, len(got), want, len(want))
	}
	seen := make(map[string]bool, len(want))
	for _, p := range want {
		seen[p] = true
	}
	for _, p := range got {
		if !seen[p] {
			t.Errorf("ui/src/lib/namespaceRules.ts declares prefix %q, which dsl.ReservedAttributePrefixes does not", p)
		}
	}
}

// uiReservedAttributePrefixes extracts the string literals inside
// ui/src/lib/namespaceRules.ts's `RESERVED_ATTRIBUTE_PREFIXES = [...]`
// declaration with a small regexp, rather than parsing TypeScript: the test
// only needs the set of quoted strings between the brackets.
func uiReservedAttributePrefixes(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "..", "ui", "src", "lib", "namespaceRules.ts")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := reservedAttributePrefixesDecl.FindSubmatch(data)
	if m == nil {
		t.Fatalf("%s: RESERVED_ATTRIBUTE_PREFIXES declaration not found", path)
	}
	var out []string
	for _, lit := range reservedAttributePrefixLiteral.FindAllStringSubmatch(string(m[1]), -1) {
		out = append(out, lit[1])
	}
	return out
}

var (
	reservedAttributePrefixesDecl  = regexp.MustCompile(`(?s)RESERVED_ATTRIBUTE_PREFIXES\s*=\s*\[(.*?)\]`)
	reservedAttributePrefixLiteral = regexp.MustCompile(`'([^']*)'`)
)
