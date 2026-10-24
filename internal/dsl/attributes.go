package dsl

import (
	"fmt"
	"regexp"
	"strings"
)

// attributeKeyPattern is the shape of a key in any bounded string-map this
// package's ValidateAttributeMap checks — today, a namespace's telemetry
// attributes (internal/api and pkg/artifacts hold TelemetryPolicy.attributes
// to it). It is deliberately NOT the shape of an orkflow's `labels:`: labels
// are free-form (docs/DSL.md), and the export boundary that turns them into
// telemetry attributes (internal/engine.telemetryFor) sanitizes them into
// this same alphabet itself, with SanitizeAttributeKey below, rather than
// refusing a document for a key its author never meant as an identifier.
var attributeKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// maxAttributeValueBytes bounds a single value in any map ValidateAttributeMap
// checks. It matches the WorkspaceLimits order of magnitude for a single
// scalar: small enough that many of them together stay far under the 256 KiB
// ExecuteTaskInput wire cap (AGENTS.md §2.2), generous enough for a real
// attribute value.
const maxAttributeValueBytes = 256

// ValidateAttributeMap holds a string-to-string map to the one rule this
// package uses for a namespace's telemetry attributes: at most max entries,
// each key ^[a-z][a-z0-9_.-]{0,63}$, each value at most 256 bytes and free of
// control characters.
//
// It is exported so a boundary outside this package validates the SAME rule
// rather than a second pattern that quietly drifts from it — internal/api's
// namespace write path and pkg/artifacts's namespace store both hold
// TelemetryPolicy.attributes to it. Two boundaries checking two patterns is
// exactly how the "one rule, two answers" class of defect this package's
// checkSlug comment already describes for slugs would recur for attribute
// maps.
//
// Values are refused for a control character (a byte under 0x20, or 0x7f)
// because a namespace's telemetry attributes are operator/admin input that
// gets rendered verbatim in terminals (`orkctl namespace get`) and the UI's
// namespace page: an embedded escape sequence or a newline splitting a
// table row is a rendering hazard this package can close at the one place
// every attribute map is validated, rather than trusting every renderer to
// close it again.
//
// kind names the map in error messages ("telemetry attribute").
func ValidateAttributeMap(kind string, m map[string]string, max int) error {
	if len(m) > max {
		return fmt.Errorf("%d %ss exceed the %d-entry limit", len(m), kind, max)
	}
	for key, value := range m {
		if !attributeKeyPattern.MatchString(key) {
			return fmt.Errorf("%s key %q is not %s", kind, key, attributeKeyPattern.String())
		}
		if len(value) > maxAttributeValueBytes {
			return fmt.Errorf("%s %q value of %d bytes exceeds the %d-byte limit",
				kind, key, len(value), maxAttributeValueBytes)
		}
		if i := indexControlByte(value); i >= 0 {
			return fmt.Errorf("%s %q value contains a control character (byte 0x%02x at index %d)",
				kind, key, value[i], i)
		}
	}
	return nil
}

// indexControlByte returns the index of the first control character in s —
// a byte under 0x20 (space), or 0x7f (DEL) — or -1 when s has none. Checked
// byte-wise rather than by decoding runes: a control character is a specific
// set of byte values regardless of the UTF-8 sequence around it, and every
// byte of a multi-byte rune is >= 0x80, so this never misidentifies one.
func indexControlByte(s string) int {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return i
		}
	}
	return -1
}

// ReservedAttributePrefixes are the namespaces the platform's own
// instrumentation stamps: ork. for OrkStack's own attributes
// (internal/engine.telemetryFor's ork.ns.<key> and ork.label.<key>), service.
// and host. for the resource attributes every OTLP exporter adds. A
// namespace admin's telemetry.attributes may not shadow any of them.
//
// Exported so internal/api and pkg/artifacts share the one literal instead of
// each keeping its own copy that can drift from the other — the same reason
// ValidateAttributeMap itself is exported. ui/src/lib/namespaceRules.ts keeps
// a mirror of this same set for the UI's own client-side check;
// TestReservedAttributePrefixes_MatchesUI pins the two together.
var ReservedAttributePrefixes = []string{"ork.", "service.", "host."}

// ValidateNoReservedPrefix refuses any key in m that starts with a reserved
// prefix (ReservedAttributePrefixes), naming the key and the prefix it
// collides with. kind names the map in the error message, matching
// ValidateAttributeMap's convention.
func ValidateNoReservedPrefix(kind string, m map[string]string) error {
	for key := range m {
		for _, prefix := range ReservedAttributePrefixes {
			if strings.HasPrefix(key, prefix) {
				return fmt.Errorf("%s key %q uses prefix %q, which is the platform's own", kind, key, prefix)
			}
		}
	}
	return nil
}

// sanitizedKeyPattern is the alphabet SanitizeAttributeKey folds a label key
// into: the same shape attributeKeyPattern requires of a telemetry
// attribute, minus the length bound (SanitizeAttributeKey enforces its own,
// in bytes, since truncation must happen before the "first char is a
// letter" fixup can be reasoned about).
var sanitizedKeyByte = regexp.MustCompile(`[^a-z0-9_.-]`)

// maxSanitizedKeyBytes bounds a key SanitizeAttributeKey produces, matching
// attributeKeyPattern's own 64-byte shape so a sanitized label key is always
// itself a valid telemetry attribute key.
const maxSanitizedKeyBytes = 64

// SanitizeAttributeKey folds an arbitrary orkflow label key into the
// telemetry attribute key alphabet (`^[a-z][a-z0-9_.-]{0,63}$`), rather than
// refusing the label outright: labels are free-form (docs/DSL.md) and an
// author who wrote `Team` or `app.kubernetes.io/name` — both ordinary,
// widely-used label keys — did not name a telemetry attribute and should not
// have their orkflow refused for one.
//
// The fold is: lowercase; every byte outside the attribute alphabet
// (`[a-z0-9_.-]`, checked after lowercasing) becomes `_`; if the result does
// not start with a letter, prefix `l_`; then truncate to 64 bytes. Applying
// the length bound last means a prefix survives truncation rather than being
// cut away by it.
//
// Exported and used by internal/engine.telemetryFor, the one place a label
// becomes a telemetry attribute — kept here, beside ValidateAttributeMap, so
// the alphabet the sanitizer targets and the alphabet the validator checks
// can never drift apart.
func SanitizeAttributeKey(key string) string {
	lower := strings.ToLower(key)
	folded := sanitizedKeyByte.ReplaceAllString(lower, "_")
	if folded == "" || folded[0] < 'a' || folded[0] > 'z' {
		folded = "l_" + folded
	}
	if len(folded) > maxSanitizedKeyBytes {
		folded = folded[:maxSanitizedKeyBytes]
	}
	return folded
}

// SanitizeAttributeValue truncates v to 256 bytes (maxAttributeValueBytes,
// matching ValidateAttributeMap's own bound) and strips control characters
// (bytes under 0x20, and 0x7f) — the same hazard ValidateAttributeMap
// refuses outright, closed here by removal instead of refusal because a
// label's value, like its key, is free-form input that only becomes a
// telemetry attribute at the export boundary and should not fail a submit
// for it.
//
// Truncation happens before stripping so the byte budget is spent on the
// value's own bytes rather than on characters that will not survive anyway.
func SanitizeAttributeValue(v string) string {
	if len(v) > maxAttributeValueBytes {
		v = v[:maxAttributeValueBytes]
	}
	if indexControlByte(v) < 0 {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		if c := v[i]; c < 0x20 || c == 0x7f {
			continue
		}
		b.WriteByte(v[i])
	}
	return b.String()
}
