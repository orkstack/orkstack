package listfilter

import (
	"errors"
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The CLI grammar of one filter (docs/UI_CLI_PARITY.md "Filtering lists"):
//
//	FILTER   = FIELD OPERATOR [VALUES]
//	FIELD    = 1*( ESCAPED / any character except \ = ! ~ ^ < > )
//	OPERATOR = ">=" / "!=" / "=" / "~" / "^" / "<"
//	VALUES   = ITEM *( "," ITEM )      -- more than one ITEM only after "="
//	ITEM     = 1*( ESCAPED / any character except \ and , )
//	ESCAPED  = "\" any-character
//
// Parsing is exact: nothing is trimmed. Parse checks syntax only; what a
// field may take is Compile's, so the browser and the CLI share one
// validator, the server's. ui/src/lib/listFilter.ts implements the same
// grammar, and testdata/vectors.json holds both to the same cases.

// operatorText is each operator's CLI spelling.
var operatorText = map[orkstackv1.FilterOperator]string{
	orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS:      "=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:  "!=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_IN:          "=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS:    "~",
	orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:      "^",
	orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER: ">=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE:      "<",
}

// operatorLabels are the fixed display names both clients use.
var operatorLabels = map[orkstackv1.FilterOperator]string{
	orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS:      "equals (=)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:  "not equals (!=)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_IN:          "in (=a,b)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS:    "contains (~)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:      "prefix (^)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER: "at or after (>=)",
	orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE:      "before (<)",
}

// OperatorLabel is op's display name: "contains (~)".
func OperatorLabel(op orkstackv1.FilterOperator) string {
	if label, ok := operatorLabels[op]; ok {
		return label
	}
	return fmt.Sprintf("operator %d", int32(op))
}

// isFieldEnd reports whether c, unescaped, ends a field.
func isFieldEnd(c byte) bool {
	switch c {
	case '=', '!', '~', '^', '<', '>':
		return true
	}
	return false
}

// Parse reads one filter in the CLI grammar.
func Parse(text string) (*orkstackv1.FieldFilter, error) {
	if text == "" {
		return nil, errors.New("empty filter")
	}
	var field strings.Builder
	i := 0
	for ; i < len(text); i++ {
		c := text[i]
		if c == '\\' {
			if i+1 == len(text) {
				return nil, fmt.Errorf("a trailing \\ escapes nothing in filter %q", text)
			}
			i++
			field.WriteByte(text[i])
			continue
		}
		if isFieldEnd(c) {
			break
		}
		field.WriteByte(c)
	}
	if i == len(text) {
		return nil, fmt.Errorf("no operator in filter %q; use = != ~ ^ >= or <", text)
	}
	if i == 0 {
		return nil, fmt.Errorf("empty field in filter %q", text)
	}
	var op orkstackv1.FilterOperator
	switch text[i] {
	case '=':
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS
		i++
	case '~':
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS
		i++
	case '^':
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX
		i++
	case '<':
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE
		i++
	case '!':
		if i+1 == len(text) || text[i+1] != '=' {
			return nil, fmt.Errorf(`no operator "!"; use != (not equals)`)
		}
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS
		i += 2
	case '>':
		if i+1 == len(text) || text[i+1] != '=' {
			return nil, fmt.Errorf(`no operator ">"; use >= (at or after) or < (before)`)
		}
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER
		i += 2
	}
	rest := text[i:]
	if strings.HasPrefix(rest, "=") {
		return nil, fmt.Errorf(`no operator "<=", "==" or ">=="; escape a value's leading = as \=`)
	}
	values, err := splitValues(rest, text)
	if err != nil {
		return nil, err
	}
	if len(values) > 1 {
		if op != orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS {
			return nil, errors.New(`a list of values needs =; escape a literal comma as \,`)
		}
		for _, v := range values {
			if v == "" {
				return nil, fmt.Errorf(`an empty value in the list of filter %q; escape a literal comma as \,`, text)
			}
		}
		op = orkstackv1.FilterOperator_FILTER_OPERATOR_IN
	}
	return &orkstackv1.FieldFilter{Field: field.String(), Op: op, Values: values}, nil
}

// splitValues unescapes rest and splits it on its unescaped commas.
func splitValues(rest, text string) ([]string, error) {
	var values []string
	var item strings.Builder
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		switch c {
		case '\\':
			if i+1 == len(rest) {
				return nil, fmt.Errorf("a trailing \\ escapes nothing in filter %q", text)
			}
			i++
			item.WriteByte(rest[i])
		case ',':
			values = append(values, item.String())
			item.Reset()
		default:
			item.WriteByte(c)
		}
	}
	return append(values, item.String()), nil
}

// Format writes f in the CLI grammar; Parse(Format(f)) equals f for every
// filter Parse can produce.
func Format(f *orkstackv1.FieldFilter) string {
	var b strings.Builder
	for i := 0; i < len(f.GetField()); i++ {
		c := f.GetField()[i]
		if c == '\\' || isFieldEnd(c) {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteString(operatorText[f.GetOp()])
	for n, v := range f.GetValues() {
		if n > 0 {
			b.WriteByte(',')
		}
		for i := 0; i < len(v); i++ {
			c := v[i]
			if c == '\\' || c == ',' || (i == 0 && c == '=') {
				b.WriteByte('\\')
			}
			b.WriteByte(c)
		}
	}
	return b.String()
}
