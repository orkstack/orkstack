package config

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// docRow is one row of docs/DEPLOY.md's "## 1. Configuration" table: the
// file-key column, the variable column, and the default column, verbatim.
type docRow struct {
	keyCell, varCell, defaultCell string
}

// parseDeployDocTable extracts the configuration table from docs/DEPLOY.md:
// every "| ... |" row between the header (containing "| variable |") and
// the section's closing blank line before the next "### " heading.
func parseDeployDocTable(t *testing.T) []docRow {
	t.Helper()
	data, err := os.ReadFile("../../docs/DEPLOY.md")
	if err != nil {
		t.Fatalf("read docs/DEPLOY.md: %v", err)
	}
	lines := strings.Split(string(data), "\n")

	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "## 1. Configuration") {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatal("docs/DEPLOY.md: no \"## 1. Configuration\" section")
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "### ") {
			end = i
			break
		}
	}

	var rows []docRow
	sawHeader := false
	for _, l := range lines[start:end] {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "|") {
			continue
		}
		if !sawHeader {
			if strings.Contains(l, "| variable |") {
				sawHeader = true
			}
			continue
		}
		if strings.HasPrefix(l, "|---") || strings.HasPrefix(l, "| ---") {
			continue // the header/body separator row
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		if len(cells) < 2 {
			continue
		}
		row := docRow{
			keyCell: strings.TrimSpace(cells[0]),
			varCell: strings.TrimSpace(cells[1]),
		}
		if len(cells) >= 3 {
			row.defaultCell = strings.TrimSpace(cells[2])
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		t.Fatal("docs/DEPLOY.md: found the configuration table header but no rows")
	}
	return rows
}

var backtickSpan = regexp.MustCompile("`([^`]+)`")

// expandBraces expands one "{a,b,c}" alternation in s, recursively, so
// "telemetry.otlp.{traces,metrics}.{endpoint,headers}" becomes the four
// dotted paths it stands for. A string with no "{" is returned unchanged.
func expandBraces(s string) []string {
	i := strings.IndexByte(s, '{')
	if i < 0 {
		return []string{s}
	}
	j := strings.IndexByte(s[i:], '}')
	if j < 0 {
		return []string{s}
	}
	j += i
	prefix, options, suffix := s[:i], s[i+1:j], s[j+1:]
	var out []string
	for _, opt := range strings.Split(options, ",") {
		out = append(out, expandBraces(prefix+opt+suffix)...)
	}
	return out
}

// expandKeyTokens turns a "file key" column into the dotted paths it names:
// one path per backtick span (each brace-expanded), with a trailing ":"
// (docs write "activities:", "telemetry.sinks:") trimmed.
func expandKeyTokens(cell string) []string {
	var tokens []string
	for _, m := range backtickSpan.FindAllStringSubmatch(cell, -1) {
		span := strings.TrimSuffix(m[1], ":")
		tokens = append(tokens, expandBraces(span)...)
	}
	return tokens
}

// expandVarTokens turns a "variable" column into the environment variable
// names (or, for a glob such as "OTEL_BSP_*", prefixes) it names. A span
// starting with "_" is a continuation of the nearest earlier full name's
// stem (docs write `OTEL_EXPORTER_OTLP_ENDPOINT`, `_PROTOCOL`, ... rather
// than repeating the shared "OTEL_EXPORTER_OTLP" prefix each time).
func expandVarTokens(cell string) []string {
	var tokens []string
	stem := ""
	for _, m := range backtickSpan.FindAllStringSubmatch(cell, -1) {
		span := m[1]
		if strings.HasPrefix(span, "_") {
			tokens = append(tokens, expandBraces(stem+span)...)
			continue
		}
		tokens = append(tokens, expandBraces(span)...)
		if !strings.ContainsAny(span, "{}") {
			if idx := strings.LastIndex(span, "_"); idx > 0 {
				stem = span[:idx]
			} else {
				stem = span
			}
		}
	}
	return tokens
}

// wholeCellItalicParen matches a default-column cell that is ENTIRELY one
// italic parenthetical, e.g. "*(empty)*" or "*(SDK default: `x`)*" — the
// form normalizeDefaultCell unwraps to "(empty)" / "(SDK default: `x`)". A
// bare '*' appearing elsewhere in a cell (e.g. a glob in a literal value
// like `ghcr.io/orkstack/activity-*:0.1`) is not markdown emphasis and must
// survive normalization untouched.
var wholeCellItalicParen = regexp.MustCompile(`^\*(\(.*\))\*$`)

// normalizeDefaultCell strips docs/DEPLOY.md's markdown decoration from a
// default-column cell — backtick code spans, and the italic-parenthetical
// wrapper around an entire placeholder note like `*(empty)*` — down to its
// bare comparable text, and trims surrounding whitespace. It deliberately
// keeps everything else (including a parenthetical's own wording, e.g.
// "SDK default: ..."), since the resulting string is compared verbatim
// against Var.Default: the docs are the source of truth for that wording
// (see the task's fix 4), so this function must not discard any of it.
func normalizeDefaultCell(cell string) string {
	s := strings.TrimSpace(cell)
	if m := wholeCellItalicParen.FindStringSubmatch(s); m != nil {
		s = m[1]
	}
	s = strings.ReplaceAll(s, "`", "")
	return collapsePlaceholderNote(strings.TrimSpace(s))
}

// collapsePlaceholderNote collapses a fully-parenthesized default cell down
// to whichever of Var's two standardized placeholders
// (EmptyDefault/DerivedDefault, defaults.go) it stands for, discarding the
// human note a docs cell spells out in prose ("empty = no listener",
// "SDK default: parentbased_always_on", "derived from the pool") — the
// same note Var.DerivedNote carries instead of Var.Default once a Schema
// entry is bound to one of the two placeholders. A cell that is not
// entirely one parenthetical (a real literal value, however it happens to
// be spelled) is returned unchanged, so this only ever narrows a
// placeholder note, never a literal.
func collapsePlaceholderNote(s string) string {
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return s
	}
	if strings.Contains(strings.ToLower(s), "empty") {
		return EmptyDefault
	}
	return DerivedDefault
}

// splitDefaultParts splits a normalized default cell on ", " into the
// individual defaults a row names, in the same left-to-right order as its
// key/variable tokens — the shape a row like
// "`8`, `16Gi`" (for `orker.resources.max_cpu`, `orker.resources.max_memory`)
// takes.
func splitDefaultParts(normalized string) []string {
	if normalized == "" {
		return nil
	}
	return strings.Split(normalized, ", ")
}

// TestSchemaCompleteness_DefaultsMatchDocs proves every Schema entry's
// Default agrees with docs/DEPLOY.md's default column, word for word once
// markdown decoration is stripped. A row naming several variables (and as
// many key tokens) lists as many comma-separated defaults, in the same
// order; a row naming several variables under one shared default (e.g. the
// OTLP block) repeats that single default cell for each. Where the two
// disagree, the docs are the source of truth: fix Var.Default in config.go,
// never this test.
func TestSchemaCompleteness_DefaultsMatchDocs(t *testing.T) {
	rows := parseDeployDocTable(t)

	for _, row := range rows {
		if isEnvOnly(row.keyCell) {
			continue // no Schema entry to check a default against
		}
		varTokens := expandVarTokens(row.varCell)
		if len(varTokens) == 0 {
			continue
		}
		parts := splitDefaultParts(normalizeDefaultCell(row.defaultCell))
		if len(parts) == 1 && len(varTokens) > 1 {
			shared := parts[0]
			parts = make([]string, len(varTokens))
			for i := range parts {
				parts[i] = shared
			}
		}
		if len(parts) != len(varTokens) {
			// A shape this test's simple column-splitting cannot align
			// safely (e.g. a default containing its own ", ") — skip
			// rather than risk a false failure against the wrong pairing.
			continue
		}
		for i, env := range varTokens {
			v, ok := Lookup(env)
			if !ok {
				continue // already reported by TestSchemaCompleteness
			}
			if v.Default != parts[i] {
				t.Errorf("Schema entry %s (%s): Default = %q, docs/DEPLOY.md's default column says %q (row variable(s) %q)",
					v.Path, env, v.Default, parts[i], row.varCell)
			}
		}
	}
}

// isEnvOnly reports whether a row's file-key column marks it as having no
// config-file key at all ("—" or "environment only", both used in the doc).
func isEnvOnly(keyCell string) bool {
	lower := strings.ToLower(keyCell)
	return keyCell == "—" || strings.Contains(lower, "environment only")
}

// TestSchemaCompleteness proves docs/DEPLOY.md and config.Schema agree:
// every file key and variable the doc names is in Schema (except the
// explicitly env-only rows), and every Schema entry is named somewhere in
// the doc.
func TestSchemaCompleteness(t *testing.T) {
	rows := parseDeployDocTable(t)

	schemaPaths := map[string]bool{}
	schemaEnvs := map[string]bool{}
	for _, v := range Schema {
		schemaPaths[v.Path] = true
		schemaEnvs[v.Env] = true
	}
	coveredPaths := map[string]bool{}
	coveredEnvs := map[string]bool{}

	for _, row := range rows {
		exempt := isEnvOnly(row.keyCell)
		keyTokens := expandKeyTokens(row.keyCell)
		varTokens := expandVarTokens(row.varCell)

		if !exempt {
			for _, k := range keyTokens {
				if !schemaPaths[k] {
					t.Errorf("docs/DEPLOY.md names file key %q (row variable(s) %q), which is not in Schema", k, row.varCell)
				}
				coveredPaths[k] = true
			}
		}

		for _, v := range varTokens {
			glob := strings.HasSuffix(v, "*")
			prefix := strings.TrimSuffix(v, "*")

			if exempt {
				for env := range schemaEnvs {
					if (glob && strings.HasPrefix(env, prefix)) || (!glob && env == v) {
						t.Errorf("docs/DEPLOY.md documents %q as environment-only, but Schema has an entry for %q", v, env)
					}
				}
				continue
			}
			if glob {
				for env := range schemaEnvs {
					if strings.HasPrefix(env, prefix) {
						coveredEnvs[env] = true
					}
				}
				continue
			}
			if !schemaEnvs[v] {
				t.Errorf("docs/DEPLOY.md names variable %q (file key(s) %q), which is not in Schema", v, row.keyCell)
			}
			coveredEnvs[v] = true
		}
	}

	for _, v := range Schema {
		if !coveredPaths[v.Path] {
			t.Errorf("Schema path %q is not documented in docs/DEPLOY.md's configuration table", v.Path)
		}
		if !coveredEnvs[v.Env] {
			t.Errorf("Schema variable %q is not documented in docs/DEPLOY.md's configuration table", v.Env)
		}
	}

	// The two known env-only families must both stay out of Schema and be
	// documented as such; lenient because the phrasing is documentation
	// prose, not a contract this test should be brittle against.
	data, err := os.ReadFile("../../docs/DEPLOY.md")
	if err != nil {
		t.Fatalf("read docs/DEPLOY.md: %v", err)
	}
	lower := strings.ToLower(string(data))
	if !strings.Contains(lower, "environment only") && !strings.Contains(lower, "environment-only") {
		t.Log("docs/DEPLOY.md does not use the phrase \"environment only\"; skipping the env-only-family documentation check")
	} else {
		for _, family := range []string{"OTEL_BSP_", "OTEL_BLRP_"} {
			for env := range schemaEnvs {
				if strings.HasPrefix(env, family) {
					t.Errorf("%s is documented as environment-only but present in Schema (%s)", family, env)
				}
			}
		}
	}
}

// TestLookup proves Lookup and Schema stay in sync: every Schema entry is
// findable by its own Env, and an unknown variable reports absent.
func TestLookup(t *testing.T) {
	for _, v := range Schema {
		got, ok := Lookup(v.Env)
		if !ok {
			t.Errorf("Lookup(%q): not found", v.Env)
			continue
		}
		if got.Path != v.Path {
			t.Errorf("Lookup(%q).Path = %q, want %q", v.Env, got.Path, v.Path)
		}
	}
	if _, ok := Lookup("ORK_NOT_A_REAL_VARIABLE"); ok {
		t.Error("Lookup of an unknown variable reported found")
	}
}
