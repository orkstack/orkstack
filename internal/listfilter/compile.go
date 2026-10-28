package listfilter

import (
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/enumname"
	"github.com/orkstack/orkstack/internal/task"
)

// Limits (docs/UI_CLI_PARITY.md "Filtering lists").
const (
	MaxFilters    = 16
	MaxInValues   = 64
	MaxValueBytes = 1024
)

// Error is a refused filter set. Message is the exact text the API answers
// with; Index is the offending filter's position in the request, or -1 when
// the set as a whole is refused.
type Error struct {
	Index   int
	Message string
}

func (e *Error) Error() string { return e.Message }

func refuse(i int, format string, args ...any) *Error {
	return &Error{Index: i, Message: fmt.Sprintf("filters[%d]: ", i) + fmt.Sprintf(format, args...)}
}

// Compile validates filters against the allowlist and canonicalizes them.
// Every refusal is an *Error carrying the exact message.
func (s *Spec) Compile(filters []*orkstackv1.FieldFilter) (Set, error) {
	if len(filters) > MaxFilters {
		return Set{}, &Error{Index: -1, Message: fmt.Sprintf("at most %d filters; got %d", MaxFilters, len(filters))}
	}
	compiled := make([]filter, 0, len(filters))
	for i, f := range filters {
		c, err := s.compileOne(i, f)
		if err != nil {
			return Set{}, err
		}
		compiled = append(compiled, c)
	}
	for i, c := range compiled {
		if c.field.Requires == "" {
			continue
		}
		satisfied := slices.ContainsFunc(compiled, func(other filter) bool {
			return other.path == c.field.Requires && other.op == orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS
		})
		if !satisfied {
			return Set{}, refuse(i, "%s needs an equals (=) filter on %s", c.path, c.field.Requires)
		}
	}
	set := canonicalize(s, compiled)
	for _, f := range s.Fields {
		if f.Kind != KindTime {
			continue
		}
		from, before := set.Interval(f.Path)
		if !from.IsZero() && !before.IsZero() && !before.After(from) {
			return Set{}, &Error{Index: -1, Message: fmt.Sprintf("%s >= %s and < %s select no instant",
				f.Path, formatTime(from), formatTime(before))}
		}
	}
	return set, nil
}

// compileOne validates one filter (§4.4) and canonicalizes its values.
func (s *Spec) compileOne(i int, f *orkstackv1.FieldFilter) (filter, error) {
	field, key, keyMissing, ok := s.field(f.GetField())
	if !ok {
		return filter{}, refuse(i, "%q is not a filterable field of %s; filterable: %s",
			f.GetField(), s.Resource.Name(), strings.Join(s.paths(), ", "))
	}
	if keyMissing {
		return filter{}, refuse(i, "name a key: %s.<key>", field.Path)
	}
	path := f.GetField()
	op := f.GetOp()
	if op == orkstackv1.FilterOperator_FILTER_OPERATOR_UNSPECIFIED {
		return filter{}, refuse(i, "%s: no operator", path)
	}
	if !field.takes(op) {
		takes := make([]string, len(field.Operators))
		for n, o := range field.Operators {
			takes[n] = OperatorLabel(o)
		}
		return filter{}, refuse(i, "%s does not take %s; it takes %s", path, OperatorLabel(op), strings.Join(takes, ", "))
	}
	values := f.GetValues()
	if op == orkstackv1.FilterOperator_FILTER_OPERATOR_IN {
		if len(values) < 1 || len(values) > MaxInValues {
			return filter{}, refuse(i, "%s takes 1 to %d values", OperatorLabel(op), MaxInValues)
		}
		if slices.Contains(values, "") {
			return filter{}, refuse(i, "%s cannot hold an empty value", OperatorLabel(op))
		}
	} else if len(values) != 1 {
		return filter{}, refuse(i, "%s takes exactly one value", OperatorLabel(op))
	}
	for _, v := range values {
		if len(v) > MaxValueBytes || !utf8.ValidString(v) {
			return filter{}, refuse(i, "a value is at most %d bytes of UTF-8", MaxValueBytes)
		}
	}
	c := filter{field: field, path: path, key: key, op: op}
	switch op {
	case orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS, orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:
		if values[0] == "" {
			return filter{}, refuse(i, "%s needs a non-empty value", OperatorLabel(op))
		}
	}
	for _, v := range values {
		canonical, err := s.canonicalValue(i, field, op, v)
		if err != nil {
			return filter{}, err
		}
		c.values = append(c.values, canonical)
	}
	if op == orkstackv1.FilterOperator_FILTER_OPERATOR_IN {
		slices.Sort(c.values)
		c.values = slices.Compact(c.values)
		if len(c.values) == 1 {
			c.op = orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS
		}
	}
	if field.Kind == KindTime {
		t, _ := time.Parse(time.RFC3339Nano, c.values[0])
		c.instant = t
	}
	return c, nil
}

// wholeValue reports whether op compares a whole value (equals, not equals,
// in) rather than a fragment of one.
func wholeValue(op orkstackv1.FilterOperator) bool {
	switch op {
	case orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_IN:
		return true
	}
	return false
}

// canonicalValue checks one value against what its field declares it must
// spell, and returns its canonical spelling.
func (s *Spec) canonicalValue(i int, field *Field, op orkstackv1.FilterOperator, v string) (string, error) {
	switch field.Kind {
	case KindEnum:
		value := field.enum.Values().ByName(protoreflect.Name(enumname.Proto(field.enum, v)))
		if value != nil {
			short := enumname.Short(field.enum, string(value.Name()))
			if slices.Contains(field.AllowedValues, short) {
				return short, nil
			}
		}
		return "", refuse(i, "%q is not %s %s this list can select; %s is one of %s",
			v, article(field.Path), field.Path, field.Path, strings.Join(field.AllowedValues, ", "))
	case KindBool:
		if v != "true" && v != "false" {
			return "", refuse(i, "%s is true or false, not %q", field.Path, v)
		}
		return v, nil
	case KindTime:
		t, err := parseInstant(v)
		if err != nil {
			return "", refuse(i, "%q is not an RFC 3339 instant with a zone (2026-10-09T00:00:00Z)", v)
		}
		// The canonical spelling is UTC, and a year outside 0000-9999 has no
		// RFC 3339 spelling that reads back (a zone offset can carry
		// 9999-12-31T23:59:59-23:59 into year 10000): refused here, rather
		// than compiled to a text the next request cannot send.
		if year := t.UTC().Year(); year < 0 || year > 9999 {
			return "", refuse(i, "%q is outside %s to %s, the instants a filter can name", v, firstInstant, lastInstant)
		}
		return formatTime(t), nil
	case KindText:
		if !wholeValue(op) {
			return v, nil
		}
		switch field.ValueRule {
		case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_NAME:
			if v != "" {
				if err := dsl.ValidSlug(field.Path, v); err != nil {
					return "", refuse(i, "%s", err.Error())
				}
			}
		case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_ORKER_ID:
			if err := task.ValidOrkerID(v); err != nil {
				return "", refuse(i, "%s: %s", field.Path, err.Error())
			}
		}
	}
	return v, nil
}

// parseInstant reads an RFC 3339 instant that names its zone, truncated to
// the microsecond every comparison uses.
func parseInstant(v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, err
	}
	return t.Truncate(time.Microsecond), nil
}

// firstInstant and lastInstant bound the instants a time filter can name:
// the years RFC 3339 spells with four digits, in the canonical (UTC,
// microsecond) spelling.
const (
	firstInstant = "0000-01-01T00:00:00Z"
	lastInstant  = "9999-12-31T23:59:59.999999Z"
)

// formatTime is an instant's canonical spelling.
func formatTime(t time.Time) string {
	return t.Truncate(time.Microsecond).UTC().Format(time.RFC3339Nano)
}

// paths are the declared paths as a refusal lists them: a labels map as
// labels.KEY.
func (s *Spec) paths() []string {
	out := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		out[i] = f.display()
	}
	return out
}

// article is the indefinite article a field's name takes in a sentence:
// "a status", "an event".
func article(word string) string {
	if word != "" && strings.ContainsRune("aeiou", rune(word[0])) {
		return "an"
	}
	return "a"
}
