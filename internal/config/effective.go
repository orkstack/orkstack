package config

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// redacted is what Effective prints in place of a secret's actual value.
const redacted = "***"

// Effective renders the effective configuration for binary: for every
// Schema entry binary reads, the value that would win under this
// document's precedence (environment, then file, then the documented
// default) and where it came from, as YAML-ish text — one line per key,
// grouped by dotted path. A secret leaf's value is always redacted,
// regardless of which origin supplied it, since `--check` output is meant
// to be pasted into a bug report.
//
// lookup reads the same environment Apply would have populated by the time
// --check runs (os.LookupEnv in production), so a value the file set but
// the environment overrides reports "env", exactly like Apply's precedence.
func (d *Document) Effective(binary Binary, lookup func(string) (string, bool)) string {
	var b strings.Builder
	for _, v := range Schema {
		if !v.usesBinary(binary) {
			continue
		}

		value, org := d.resolve(v, lookup)
		switch {
		case v.Kind == KindSecret || v.Secret:
			value = redacted
		case v.Kind == KindDocument && v.Path == "telemetry.sinks":
			value = redactSinksDocument(value)
		}
		value = strings.ReplaceAll(value, "\n", "\\n")
		value = quoteIfNeeded(value)

		fmt.Fprintf(&b, "%s: %s  # %s origin=%s\n", v.Path, value, v.Env, org)
	}
	return b.String()
}

// resolve applies Apply's exact precedence to report where a value would
// come from: environment first, then this document's file value, then the
// entry's documented default.
func (d *Document) resolve(v Var, lookup func(string) (string, bool)) (value string, org origin) {
	if val, ok := lookup(v.Env); ok {
		return val, originEnv
	}
	if rv, ok := d.values[v.Env]; ok {
		return rv.value, originFile
	}
	return v.Default, originDefault
}

// redactSinksDocument redacts the telemetry.sinks document's raw YAML text
// (the same text ORK_TELEMETRY_SINKS carries) before Effective prints it: a
// named sink's own headers may carry secret values just like the top-level
// telemetry.otlp.headers a KindMap leaf redacts wholesale, but a document
// leaf is never itself KindSecret, so nothing upstream of this function
// would otherwise catch them. It walks the parsed YAML node — never a
// regex, so it cannot be fooled by a header value that happens to look like
// YAML syntax — and replaces (1) every value under a "headers" mapping and
// (2) every {file: ...} leaf anywhere else in the document with "***".
// Text that fails to parse as YAML (should not happen; renderDocument only
// ever wrote valid YAML) or is not a mapping is returned unchanged, since
// there is then nothing shaped like a secret to find.
func redactSinksDocument(text string) string {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(text), &root); err != nil || len(root.Content) == 0 {
		return text
	}
	redactSecretNodes(root.Content[0])
	out, err := yaml.Marshal(&root)
	if err != nil {
		return text
	}
	return string(out)
}

// redactSecretNodes walks n in place, blanking every value under a
// "headers" mapping key and every {file: ...} leaf it finds elsewhere.
func redactSecretNodes(n *yaml.Node) {
	if n == nil {
		return
	}
	switch n.Kind {
	case yaml.MappingNode:
		if isFileLeaf(n) {
			redactToStars(n)
			return
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			if key.Value == "headers" && val.Kind == yaml.MappingNode {
				redactHeaderValues(val)
				continue
			}
			redactSecretNodes(val)
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			redactSecretNodes(c)
		}
	}
}

// redactHeaderValues blanks every entry's value directly under a "headers"
// mapping, whatever shape that value has (a literal string, {file: ...}, or
// a named sink's own {source: "file:..."} — telemetry.sinksDocument's
// convention): a header exists to carry credentials, so nothing under it is
// ever safe to print.
func redactHeaderValues(headers *yaml.Node) {
	for i := 0; i+1 < len(headers.Content); i += 2 {
		redactToStars(headers.Content[i+1])
	}
}

// isFileLeaf reports whether n is the one-key mapping {file: ...}.
func isFileLeaf(n *yaml.Node) bool {
	return len(n.Content) == 2 && n.Content[0].Kind == yaml.ScalarNode && n.Content[0].Value == "file"
}

// redactToStars turns n into the scalar "***" in place, discarding whatever
// shape it had.
func redactToStars(n *yaml.Node) {
	n.Kind = yaml.ScalarNode
	n.Tag = "!!str"
	n.Value = redacted
	n.Content = nil
}

// quoteIfNeeded wraps value in strconv.Quote when printing it bare would be
// ambiguous or hard to read in the "key: value  # ..." line Effective
// writes: an empty value, one containing '#' (which Effective's own line
// comment would otherwise swallow), one containing ": " (which looks like
// another key), one with leading or trailing whitespace, or one containing
// a literal newline.
func quoteIfNeeded(value string) string {
	if value == "" {
		return `""`
	}
	if strings.ContainsRune(value, '#') ||
		strings.Contains(value, ": ") ||
		strings.TrimSpace(value) != value ||
		strings.ContainsRune(value, '\n') {
		return strconv.Quote(value)
	}
	return value
}
