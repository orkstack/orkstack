// Package enumname is the one spelling of protobuf enum values OrkStack
// shows people: the value's short name, without the enum's type prefix
// (STATUS_SUCCEEDED reads SUCCEEDED). docs/JSON.md states the dialect.
//
// The engine's JSON codec writes and reads enum values through it, and
// internal/listfilter reads filter values through it, so a value copied
// from `orkctl --json` output is exactly what a filter accepts.
package enumname

import (
	"strings"
	"unicode"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Short drops the enum's type prefix: STATUS_SUCCEEDED -> SUCCEEDED. A name
// the enum does not declare is returned unchanged, and so is one whose
// short spelling would itself be a declared value (it would be ambiguous).
func Short(enum protoreflect.EnumDescriptor, name string) string {
	values := enum.Values()
	if values.ByName(protoreflect.Name(name)) == nil {
		return name // an unmapped number, or already short
	}
	trimmed := strings.TrimPrefix(name, Prefix(enum))
	if trimmed == name || trimmed == "" {
		return name
	}
	if values.ByName(protoreflect.Name(trimmed)) != nil {
		return name // the short spelling is itself a declared value: ambiguous
	}
	return trimmed
}

// Proto restores the enum's type prefix: SUCCEEDED -> STATUS_SUCCEEDED.
// Input is case-insensitive (ASCII letters only: every declared name is
// ASCII, and a letter such as U+017F that Unicode upper-cases to an ASCII one
// is not that letter), the prefix is optional and "-" reads as "_", so
// failed, STATUS_FAILED and parent-run all resolve. A declared name passes
// through untouched; a name that resolves to nothing is returned unchanged,
// for the caller to refuse.
func Proto(enum protoreflect.EnumDescriptor, name string) string {
	values := enum.Values()
	if values.ByName(protoreflect.Name(name)) != nil {
		return name
	}
	prefix := Prefix(enum)
	upper := UpperASCII(name)
	snake := strings.ReplaceAll(upper, "-", "_")
	for _, candidate := range []string{prefix + name, upper, prefix + upper, snake, prefix + snake} {
		if values.ByName(protoreflect.Name(candidate)) != nil {
			return candidate
		}
	}
	return name // unknown: the caller reports it
}

// UpperASCII upper-cases the ASCII letters of s and leaves every other byte
// as it is, which strings.ToUpper does not: it maps U+017F to S and U+0131 to
// I, and so would let a spelling no declared name has resolve to one.
func UpperASCII(s string) string {
	upper := []byte(s)
	for i, c := range upper {
		if c >= 'a' && c <= 'z' {
			upper[i] = c - 'a' + 'A'
		}
	}
	return string(upper)
}

// Prefix is the SCREAMING_SNAKE form of the enum's name plus "_", which is
// the prefix protobuf style gives every value of that enum: Status ->
// STATUS_, EventType -> EVENT_TYPE_.
func Prefix(enum protoreflect.EnumDescriptor) string {
	name := []rune(string(enum.Name()))
	var prefix strings.Builder
	for i, r := range name {
		if i > 0 && unicode.IsUpper(r) {
			startsWord := !unicode.IsUpper(name[i-1])
			endsAcronym := i+1 < len(name) && unicode.IsLower(name[i+1])
			if startsWord || endsAcronym {
				prefix.WriteRune('_')
			}
		}
		prefix.WriteRune(unicode.ToUpper(r))
	}
	prefix.WriteRune('_')
	return prefix.String()
}
