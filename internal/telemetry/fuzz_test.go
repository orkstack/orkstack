package telemetry

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

// FuzzParseSinks: ORK_TELEMETRY_SINKS is an operator's document naming where
// telemetry goes and which secrets sign it. An accepted table holds only
// slug-named sinks, every header of which has exactly one of a literal and a
// source read from the environment or a file, in sorted order for any log
// line; a refusal names the setting. A misspelled key is refused, never
// dropped.
func FuzzParseSinks(f *testing.F) {
	for _, seed := range []string{
		"", "  \n", "sinks: {}", "sinks:\n  default:\n    endpoint: http://otel-collector:4318\n",
		"sinks:\n  datadog:\n    endpoint: https://otlp.example\n    headers:\n      DD-API-KEY: { source: \"env:DD_API_KEY\" }\n      X-Scope-OrgID: { value: tenant-a }\n    timeout: 10s\n",
		"sinks:\n  a:\n    headers:\n      H: { value: v, source: \"env:X\" }\n", "sinks:\n  a:\n    headers:\n      H: {}\n",
		"sinks:\n  a:\n    headers:\n      H: { source: \"vault:x\" }\n", "sinks:\n  a:\n    headers:\n      H: { source: \"x\" }\n",
		"sinks:\n  A:\n    endpoint: x\n", "sinks:\n  1a:\n    endpoint: x\n", "sinks:\n  a:\n    endpoit: x\n", "sinks:\n  a: {}\n  a: {}\n",
		"sinks:\n  " + strings.Repeat("a", 64) + ": {}\n", "sinks:\n  " + strings.Repeat("a", 65) + ": {}\n", "---\nsinks: {a: {}}\n---\nsinks: {b: {}}\n",
		"sinks: [", "sinks: 1", "- sinks", "sinks:\n  a:\n    timeout: soon\n", "sinks:\n  a:\n    insecure: yes\n", "sinks:\n  \"a\\u0000\": {}\n",
		"sinks:\n  a: &x {}\n  b: *x\n", "sinks:\n  <<: {a: {}}\n",
	} {
		f.Add([]byte(seed))
	}
	slug := regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	f.Fuzz(func(t *testing.T, raw []byte) {
		table, err := ParseSinks(raw)
		if err != nil {
			if !strings.Contains(err.Error(), SinksVar) || table.Len() != 0 {
				t.Fatalf("ParseSinks(%q) refused as %q with %d sinks", raw, err, table.Len())
			}
			return
		}
		names := table.Names()
		if len(names) != table.Len() || !sort.StringsAreSorted(names) {
			t.Fatalf("ParseSinks(%q) lists %v for %d sinks", raw, names, table.Len())
		}
		for _, name := range names {
			if !slug.MatchString(name) || !table.Has(name) {
				t.Fatalf("ParseSinks(%q) accepted the sink name %q", raw, name)
			}
			for header, source := range table.sinks[name].Headers {
				if (source.Value != "") == (source.Source != "") {
					t.Fatalf("ParseSinks(%q) accepted header %q of %q with value %q and source %q", raw, header, name, source.Value, source.Source)
				}
				if source.Source != "" {
					if scheme, _, _ := strings.Cut(source.Source, ":"); scheme != "env" && scheme != "file" || !strings.Contains(source.Source, ":") {
						t.Fatalf("ParseSinks(%q) accepted the source %q", raw, source.Source)
					}
				}
			}
		}
	})
}
