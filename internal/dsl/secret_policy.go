package dsl

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file is the one rule both halves of the secret policy share: which
// SecretRef.source values a list of `scheme:pattern` entries permits. The
// tenant half is Namespace.allowed_secrets, checked by the engine at
// validate, apply and submit; the operator half is ORK_ORKER_SECRET_ALLOW,
// checked by the Orker at dispatch. Both call CheckSecretSource, and the
// registry and the Orker's boot validation both call ValidSecretPatterns,
// so there is no second reading of the grammar anywhere.
//
// The grammar is deliberately not an expression language (AGENTS.md §13): a
// literal scheme, a literal locator, and "*" as the one wildcard, which
// never crosses "/". "?", "[…]", "{a,b}", "**", "\" and anchors are ordinary
// characters. A "file:" source with a "..", "." or empty segment is refused
// rather than normalized, because normalization is how a name escapes a
// pattern it plainly sits outside of. The policy bounds NAMES only; nothing
// here sees a secret value.

// MaxSecretPatterns bounds Namespace.allowed_secrets and
// ORK_ORKER_SECRET_ALLOW alike. 32 is generous for one tenant's prefixes and
// keeps a namespace document and a refusal sentence readable.
const MaxSecretPatterns = 32

// MaxSecretPatternBytes bounds ONE entry — scheme, separator and pattern
// together — and is checked as a whole, before either half is looked at.
const MaxSecretPatternBytes = 256

// MaxSecretSchemeBytes bounds the scheme half of an entry.
const MaxSecretSchemeBytes = 16

// ValidSecretPattern reports why one `scheme:pattern` entry cannot be stored
// or configured, or nil. The rule, stated once and checked in this order:
//
//  1. the entry is at most MaxSecretPatternBytes bytes;
//  2. it contains ":" and is split at the FIRST one; both halves are
//     non-empty;
//  3. the scheme is at most MaxSecretSchemeBytes bytes and matches
//     ^[a-z][a-z0-9]*$ — literal, so a "*" in the scheme is refused and a
//     pattern can never cross the separator;
//  4. the pattern is valid UTF-8 with no control character (U+0000–U+001F,
//     U+007F), no whitespace, and no "," (the list separator
//     ORK_ORKER_SECRET_ALLOW uses);
//  5. "*" is the only metacharacter; "?", "[", "]", "{", "}", "\" and
//     every other character are literal;
//  6. for scheme "file" the pattern begins with "/", has no empty segment
//     ("//"), no "." segment and no ".." segment, and does not end in "/".
//     A file pattern names paths, and a path that normalization could
//     rewrite is refused rather than rewritten.
func ValidSecretPattern(entry string) error {
	if len(entry) > MaxSecretPatternBytes {
		return fmt.Errorf("secret pattern %q is %d bytes; an entry is at most %d bytes",
			entry, len(entry), MaxSecretPatternBytes)
	}
	scheme, pattern, found := strings.Cut(entry, ":")
	if !found {
		return fmt.Errorf("secret pattern %q must be scheme:pattern, with a \":\" after the scheme (e.g. \"env:SANDBOX_*\")", entry)
	}
	if scheme == "" {
		return fmt.Errorf("secret pattern %q has an empty scheme before the \":\"", entry)
	}
	if pattern == "" {
		return fmt.Errorf("secret pattern %q has an empty pattern after the \":\"", entry)
	}
	if len(scheme) > MaxSecretSchemeBytes {
		return fmt.Errorf("secret pattern %q: scheme %q is %d bytes; a scheme is at most %d bytes",
			entry, scheme, len(scheme), MaxSecretSchemeBytes)
	}
	if !validSecretScheme(scheme) {
		return fmt.Errorf("secret pattern %q: scheme %q must match ^[a-z][a-z0-9]*$ (a scheme is literal)",
			entry, scheme)
	}
	if !utf8.ValidString(pattern) {
		return fmt.Errorf("secret pattern %q is not valid UTF-8", entry)
	}
	for _, r := range pattern {
		switch {
		case r < 0x20 || r == 0x7f:
			return fmt.Errorf("secret pattern %q contains a control character", entry)
		case unicode.IsSpace(r):
			return fmt.Errorf("secret pattern %q contains whitespace", entry)
		case r == ',':
			return fmt.Errorf("secret pattern %q contains \",\", the list separator", entry)
		}
	}
	if scheme == "file" {
		if !strings.HasPrefix(pattern, "/") {
			return fmt.Errorf("secret pattern %q: a \"file:\" pattern must begin with \"/\"", entry)
		}
		if strings.HasSuffix(pattern, "/") {
			return fmt.Errorf("secret pattern %q: a \"file:\" pattern must not end in \"/\"", entry)
		}
		if why := badPathSegment(pattern); why != "" {
			return fmt.Errorf("secret pattern %q: a \"file:\" pattern has %s, which is refused rather than normalized",
				entry, SecretRefusalPhrase(why))
		}
	}
	return nil
}

// ValidSecretPatterns applies ValidSecretPattern to every entry, refuses
// more than MaxSecretPatterns entries and any byte-equal duplicate, and
// names the offending entry and its index in the error.
func ValidSecretPatterns(entries []string) error {
	if len(entries) > MaxSecretPatterns {
		return fmt.Errorf("%d secret patterns; at most %d are allowed", len(entries), MaxSecretPatterns)
	}
	seen := make(map[string]int, len(entries))
	for i, entry := range entries {
		if err := ValidSecretPattern(entry); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if first, dup := seen[entry]; dup {
			return fmt.Errorf("entry %d: secret pattern %q duplicates entry %d", i, entry, first)
		}
		seen[entry] = i
	}
	return nil
}

// CheckSecretSource reports whether one SecretRef.source is PERMITTED by a
// list of patterns. nil is permission. An EMPTY list permits everything
// without looking at the source: it is "no restriction", never "restriction
// to nothing", the reading Namespace.allowed_pools already has.
//
// source is split at its first ":" exactly as orker.SplitSecretSource
// splits it; a source with no ":" has an empty scheme and matches no
// pattern. A pattern matches when its scheme equals the source's scheme
// byte for byte and its pattern matches the source's locator, where "*"
// matches zero or more characters other than "/" and every other character
// matches itself. Matching is case-sensitive.
//
// A "file" source whose locator is not absolute, or has an empty, "." or
// ".." segment, is refused BEFORE matching, with an error that says so:
// ".." is never normalized away, because
// file:/etc/orkstack/secrets/sandbox/../payments/db would otherwise sit
// inside a pattern it plainly escapes.
//
// The returned error is a *SecretSourceRefusal. Callers prefix their own
// coordinates; they never rephrase it.
func CheckSecretSource(patterns []string, source string) error {
	if len(patterns) == 0 {
		return nil
	}
	refuse := func(why string) error {
		return &SecretSourceRefusal{Source: source, Patterns: append([]string(nil), patterns...), Why: why}
	}
	scheme, locator, found := strings.Cut(source, ":")
	if !found {
		return refuse("")
	}
	if scheme == "file" {
		if !strings.HasPrefix(locator, "/") {
			return refuse("relative")
		}
		if why := badPathSegment(locator); why != "" {
			return refuse(why)
		}
	}
	for _, entry := range patterns {
		pScheme, pattern, ok := strings.Cut(entry, ":")
		if !ok || pScheme != scheme {
			continue
		}
		if matchSecretLocator(pattern, locator) {
			return nil
		}
	}
	return refuse("")
}

// SecretSourceRefusal is why a source was not permitted.
type SecretSourceRefusal struct {
	Source   string
	Patterns []string
	// Why is "" for a plain non-match, or the segment rule that refused a
	// file source before matching: "..", ".", "empty" or "relative".
	Why string
}

// Error renders exactly one of:
//
//	"env:PROD_DB_PASSWORD" matches none of env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*
//	"file:/etc/orkstack/secrets/sandbox/../payments/db" has a ".." segment, which is refused rather than normalized
//	"file:/etc/orkstack/secrets/sandbox/./db" has a "." segment, which is refused rather than normalized
//	"file:/etc/orkstack/secrets/sandbox//db" has an empty segment, which is refused rather than normalized
//	"file:etc/orkstack/secrets/db" is not an absolute path
//
// (patterns joined with ", " in the order given).
func (r *SecretSourceRefusal) Error() string {
	switch r.Why {
	case "":
		return fmt.Sprintf("%q matches none of %s", r.Source, strings.Join(r.Patterns, ", "))
	case "relative":
		return fmt.Sprintf("%q is not an absolute path", r.Source)
	default:
		return fmt.Sprintf("%q has %s, which is refused rather than normalized", r.Source, SecretRefusalPhrase(r.Why))
	}
}

// SecretRefusalPhrase names what a Why value refused, as the noun phrase a
// caller's sentence embeds: `a ".." segment`, `a "." segment`, `an empty
// segment` or `a relative path`; "" for a plain non-match.
func SecretRefusalPhrase(why string) string {
	switch why {
	case "..":
		return `a ".." segment`
	case ".":
		return `a "." segment`
	case "empty":
		return "an empty segment"
	case "relative":
		return "a relative path"
	}
	return ""
}

// badPathSegment returns the first segment rule an absolute path breaks:
// "empty" ("//" or a trailing "/"), "." or "..", or "" when it breaks none.
// It never normalizes.
func badPathSegment(path string) string {
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		switch seg {
		case "":
			return "empty"
		case ".":
			return "."
		case "..":
			return ".."
		}
	}
	return ""
}

func validSecretScheme(scheme string) bool {
	if scheme == "" {
		return false
	}
	for i := 0; i < len(scheme); i++ {
		c := scheme[i]
		switch {
		case c >= 'a' && c <= 'z':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}

// matchSecretLocator matches a pattern against a locator: "*" matches zero
// or more characters other than "/", every other byte matches itself. It is
// hand-written, not path.Match or filepath.Match, which would give "?",
// "[…]" and "\" meanings this contract says they do not have. Because "*"
// never crosses "/", the two sides must have the same number of "/"
// segments, and each pattern segment matches its locator segment alone.
func matchSecretLocator(pattern, locator string) bool {
	pSegs := strings.Split(pattern, "/")
	lSegs := strings.Split(locator, "/")
	if len(pSegs) != len(lSegs) {
		return false
	}
	for i := range pSegs {
		if !matchSegment(pSegs[i], lSegs[i]) {
			return false
		}
	}
	return true
}

// matchSegment matches one "/"-free pattern segment: split on "*", the
// first piece is a prefix, the last a suffix, and the middle pieces are
// found in order, leftmost first — which is optimal when "*" may consume
// any character, as it may inside one segment.
func matchSegment(pattern, s string) bool {
	pieces := strings.Split(pattern, "*")
	if len(pieces) == 1 {
		return pattern == s
	}
	first, last := pieces[0], pieces[len(pieces)-1]
	if len(s) < len(first)+len(last) || !strings.HasPrefix(s, first) || !strings.HasSuffix(s, last) {
		return false
	}
	rest := s[len(first) : len(s)-len(last)]
	for _, piece := range pieces[1 : len(pieces)-1] {
		idx := strings.Index(rest, piece)
		if idx < 0 {
			return false
		}
		rest = rest[idx+len(piece):]
	}
	return true
}

// SecretPolicyVector is one row of the shared table every boundary tests
// against (AGENTS.md §7: one rule, restated at each boundary over shared
// test vectors).
type SecretPolicyVector struct {
	Pattern   string // "" means an empty list
	Source    string
	Permitted bool
	Why       string // expected SecretSourceRefusal.Why when refused
}

// SecretPolicyVectors returns the shared table. It is exported so
// pkg/artifacts, internal/orker and cmd/orkctl test against the same rows
// rather than their own paraphrase of the rule.
func SecretPolicyVectors() []SecretPolicyVector {
	const sandbox = "file:/etc/orkstack/secrets/sandbox/*"
	return []SecretPolicyVector{
		{Pattern: "env:SANDBOX_*", Source: "env:SANDBOX_DB", Permitted: true},
		{Pattern: "env:SANDBOX_*", Source: "env:SANDBOX_", Permitted: true},
		{Pattern: "env:SANDBOX_*", Source: "env:PROD_DB_PASSWORD"},
		{Pattern: "env:SANDBOX_*", Source: "ENV:SANDBOX_DB"},
		{Pattern: "env:*", Source: "file:/etc/x"},
		{Pattern: "env:SANDBOX_*", Source: "SANDBOX_DB"},
		{Pattern: sandbox, Source: "file:/etc/orkstack/secrets/sandbox/db", Permitted: true},
		{Pattern: sandbox, Source: "file:/etc/orkstack/secrets/sandbox/sub/db"},
		{Pattern: sandbox, Source: "file:/etc/orkstack/secrets/sandbox/../payments/db", Why: ".."},
		{Pattern: sandbox, Source: "file:/etc/orkstack/secrets/sandbox//db", Why: "empty"},
		{Pattern: sandbox, Source: "file:/etc/orkstack/secrets/sandbox/./db", Why: "."},
		{Pattern: sandbox, Source: "file:etc/orkstack/secrets/sandbox/db", Why: "relative"},
		{Pattern: "file:/etc/*/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/db", Permitted: true},
		{Pattern: "env:DB?", Source: "env:DBX"},
		{Pattern: "env:DB?", Source: "env:DB?", Permitted: true},
		{Pattern: "env:A..B", Source: "env:A..B", Permitted: true},
		{Pattern: "", Source: "file:/a/../b", Permitted: true},
		{Pattern: "", Source: "NO_SCHEME", Permitted: true},
		{Pattern: "env:DB[a-z]", Source: "env:DBa"},
		{Pattern: `env:DB\*`, Source: "env:DBX"},
		{Pattern: "file:/etc/*", Source: "file:/etc/a/b"},
		{Pattern: "file:/etc/*/*", Source: "file:/etc/a/b", Permitted: true},
	}
}
