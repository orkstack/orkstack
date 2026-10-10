package listfilter

import (
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Help is the allowlist as `orkctl … --filter help` prints it and as every
// list command's --help ends: one row per field, with the operators it
// takes in the CLI grammar and what its values must spell.
//
// It takes no command name: the table depends only on the resource's
// allowlist, so every command listing one resource prints the same text and
// the caller (cmd/orkctl/filter.go) places it under its own usage.
func (s *Spec) Help() string {
	type row struct{ field, ops, values string }
	rows := []row{{"FIELD", "OPERATORS", "VALUES"}}
	for _, f := range s.Fields {
		ops := make([]string, len(f.Operators))
		for i, op := range f.Operators {
			ops[i] = operatorHelp[op]
		}
		values := valuesHelp(f)
		if f.Requires != "" {
			values += "; needs " + f.Requires + "="
		}
		if f.KeptWhenUnavailable {
			values += "; an unreachable row is kept"
		}
		rows = append(rows, row{f.display(), strings.Join(ops, " "), values})
	}
	fieldWidth, opsWidth := 0, 0
	for _, r := range rows {
		fieldWidth = max(fieldWidth, len(r.field))
		opsWidth = max(opsWidth, len(r.ops))
	}
	var b strings.Builder
	b.WriteString("Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", fieldWidth, r.field, opsWidth, r.ops, r.values)
	}
	b.WriteString("Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.\n")
	b.WriteString("A label is labels.KEY. \\ escapes the next character; a comma inside a value is \\,.\n")
	return b.String()
}

// operatorHelp is each operator as the help table shows it.
var operatorHelp = map[orkstackv1.FilterOperator]string{
	orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS:      "=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:  "!=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_IN:          "=a,b",
	orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS:    "~",
	orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:      "^",
	orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER: ">=",
	orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE:      "<",
}

func valuesHelp(f *Field) string {
	switch f.Kind {
	case KindEnum:
		return strings.Join(f.AllowedValues, " ")
	case KindBool:
		return "true false"
	case KindTime:
		return "RFC 3339 instant with a zone (2026-10-09T00:00:00Z)"
	}
	switch f.ValueRule {
	case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_NAME:
		return "a name (a-z, 0-9, -; at most 200 bytes) or empty"
	case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_ORKER_ID:
		return "an Orker id"
	}
	return "text"
}
