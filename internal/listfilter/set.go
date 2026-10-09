package listfilter

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
)

// filter is one compiled, canonical filter.
type filter struct {
	field   *Field
	path    string // as the request spelled it: "labels.team", "origin.kind"
	key     string // the label key, for a labels path
	op      orkstackv1.FilterOperator
	values  []string  // canonical spellings
	instant time.Time // a time filter's bound
	text    string    // the canonical CLI text
}

// Set is a compiled, canonical filter set: every filter must hold. The zero
// Set holds no filter and matches every row.
type Set struct {
	spec    *Spec
	filters []filter // sorted bytewise by text, without duplicates
}

// canonicalize applies §4.6: several lower bounds on one field collapse to
// the latest and several upper bounds to the earliest, then every filter is
// formatted in the CLI grammar, sorted by that text and deduplicated.
func canonicalize(spec *Spec, compiled []filter) Set {
	var out []filter
	bounds := map[string]int{} // path+op -> index in out
	for _, c := range compiled {
		if c.field.Kind == KindTime {
			key := c.path + "\x00" + c.op.String()
			if at, seen := bounds[key]; seen {
				keep := out[at]
				if (c.op == orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER && c.instant.After(keep.instant)) ||
					(c.op == orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE && c.instant.Before(keep.instant)) {
					out[at] = c
				}
				continue
			}
			bounds[key] = len(out)
		}
		out = append(out, c)
	}
	for i := range out {
		out[i].text = Format(out[i].proto())
	}
	slices.SortFunc(out, func(a, b filter) int { return strings.Compare(a.text, b.text) })
	out = slices.CompactFunc(out, func(a, b filter) bool { return a.text == b.text })
	return Set{spec: spec, filters: out}
}

func (f filter) proto() *orkstackv1.FieldFilter {
	return &orkstackv1.FieldFilter{Field: f.path, Op: f.op, Values: slices.Clone(f.values)}
}

// Empty reports whether the set holds no filter.
func (s Set) Empty() bool { return len(s.filters) == 0 }

// Len is the number of canonical filters.
func (s Set) Len() int { return len(s.filters) }

// Key is the lowercase hex SHA-256 of the canonical texts joined by "\n",
// and "" for an empty set. A page token binds it.
func (s Set) Key() string {
	if s.Empty() {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join(s.Texts(), "\n")))
	return hex.EncodeToString(sum[:])
}

// Texts are the canonical filters in the CLI grammar, in canonical order.
func (s Set) Texts() []string {
	out := make([]string, len(s.filters))
	for i, f := range s.filters {
		out[i] = f.text
	}
	return out
}

// Proto is the canonical set as wire filters.
func (s Set) Proto() []*orkstackv1.FieldFilter {
	out := make([]*orkstackv1.FieldFilter, len(s.filters))
	for i, f := range s.filters {
		out[i] = f.proto()
	}
	return out
}

// Equality is the only door to a pushdown of one value. It returns (v, true)
// only when the set holds exactly one filter on path, that filter is equals
// (an in of one value already is), and v is non-empty. In every other case
// the caller reads its whole source and Match decides.
func (s Set) Equality(path string) (string, bool) {
	var found *filter
	for i := range s.filters {
		if s.filters[i].path != path {
			continue
		}
		if found != nil {
			return "", false
		}
		found = &s.filters[i]
	}
	if found == nil || found.op != orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS || found.values[0] == "" {
		return "", false
	}
	return found.values[0], true
}

// Interval is the collapsed bounds on a time path: atOrAfter inclusive,
// before exclusive, each zero when unbounded. Like Equality it only narrows
// a read; Match still decides every row.
func (s Set) Interval(path string) (atOrAfter, before time.Time) {
	for _, f := range s.filters {
		if f.path != path {
			continue
		}
		switch f.op {
		case orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER:
			atOrAfter = f.instant
		case orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE:
			before = f.instant
		}
	}
	return atOrAfter, before
}

// Restrict is the subset of filters on the named declared paths (a labels
// map by its declared path, "labels").
func (s Set) Restrict(paths ...string) Set {
	out := Set{spec: s.spec}
	for _, f := range s.filters {
		if slices.Contains(paths, f.field.Path) {
			out.filters = append(out.filters, f)
		}
	}
	return out
}

// Match reports whether every filter holds for row, a message of the
// spec's resource. A row whose unavailable field is set holds every filter
// on a field declared kept_when_unavailable: a filter never hides a row the
// deployment failed to ask.
func (s Set) Match(row proto.Message) bool {
	if s.Empty() {
		return true
	}
	m := row.ProtoReflect()
	if m.Descriptor().FullName() != s.spec.Resource.FullName() {
		return false
	}
	unavailable := s.spec.Unavailable != nil && m.Get(s.spec.Unavailable).String() != ""
	for _, f := range s.filters {
		if unavailable && f.field.KeptWhenUnavailable {
			continue
		}
		if !f.holds(m) {
			return false
		}
	}
	return true
}

// holds evaluates one filter on m.
func (f filter) holds(m protoreflect.Message) bool {
	value, set := f.read(m)
	if f.field.Kind == KindTime {
		if !set {
			return false // an unset time never matches
		}
		ts := value.Message()
		fields := ts.Descriptor().Fields() // google.protobuf.Timestamp: 1 seconds, 2 nanos
		instant := time.Unix(ts.Get(fields.ByNumber(1)).Int(), ts.Get(fields.ByNumber(2)).Int()).Truncate(time.Microsecond)
		if f.op == orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER {
			return !instant.Before(f.instant)
		}
		return instant.Before(f.instant)
	}
	var text string
	switch f.field.Kind {
	case KindText:
		text = value.String()
	case KindLabels:
		got := value.Map().Get(protoreflect.ValueOfString(f.key).MapKey())
		if got.IsValid() {
			text = got.String()
		}
	case KindEnum:
		number := value.Enum()
		if v := f.field.enum.Values().ByNumber(number); v != nil {
			text = enumname.Short(f.field.enum, string(v.Name()))
		}
		if number == 0 {
			// UNSPECIFIED is never named by a filter: = and in never match
			// it, != always does.
			return f.op == orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS
		}
	case KindBool:
		if value.Bool() {
			text = "true"
		} else {
			text = "false"
		}
	}
	switch f.op {
	case orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS:
		return text == f.values[0]
	case orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:
		return text != f.values[0]
	case orkstackv1.FilterOperator_FILTER_OPERATOR_IN:
		return slices.Contains(f.values, text)
	case orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS:
		return strings.Contains(text, f.values[0])
	case orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:
		return strings.HasPrefix(text, f.values[0])
	}
	return false
}

// read follows the field's chain from m. A nested message that is unset
// reads as its zero value; set reports whether the leaf field is set (only
// a timestamp needs to know).
func (f filter) read(m protoreflect.Message) (protoreflect.Value, bool) {
	chain := f.field.chain
	for _, fd := range chain[:len(chain)-1] {
		m = m.Get(fd).Message()
	}
	leaf := chain[len(chain)-1]
	return m.Get(leaf), m.Has(leaf)
}
