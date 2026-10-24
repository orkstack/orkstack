package dsl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The declared interface of an Orkflow version is its inputs: and its
// outputs:, and nothing else. This file classifies how one version's declared
// interface changes in another (docs/UI_CLI_PARITY.md "Interface compatibility"): every
// declared input and output gets a finding with its own verdict. Tasks,
// images, with:, pools, timeouts and output TEMPLATES are implementation and
// are never findings — DiffOrkflowVersions reports them.
//
// The classification reads exactly three words of an input declaration —
// type, required, default — and an output's NAME. It is deliberately not a
// schema language (AGENTS.md §13).

// InputDeclaration is one entry of Orkflow.inputs as the platform reads it.
// It is exactly the reading composed-orkflows rule 32 applies to a child's
// inputs, and the one engine.ApplyInputDefaults applies to defaults, so the
// three cannot disagree about what "required" means.
type InputDeclaration struct {
	// Type is the declaration's `type` when it is a JSON string, else "".
	// "" reads as "untyped". docs/DSL.md "Inputs" documents the five words
	// string | number | boolean | object | array; any other string is
	// compared as written and is not refused here (validation is not this
	// function's job).
	Type string
	// Required is `required: true` AND no `default` key. A declaration
	// carrying both is not required (docs/DSL.md "Inputs" and validation rule
	// 32: "required: true and no default").
	Required bool
	// Default is the `default` value when the key is present — a JSON null
	// included, because ApplyInputDefaults copies whatever the key holds —
	// and nil when absent.
	Default *structpb.Value
}

// ReadInput reads one entry of Orkflow.inputs. A declaration that is not a
// JSON object reads as the zero InputDeclaration (untyped, optional, no
// default) — what ApplyInputDefaults makes of it. A `required` that is not a
// JSON boolean is false. Pure.
func ReadInput(decl *structpb.Value) InputDeclaration {
	fields := decl.GetStructValue().GetFields()
	if fields == nil {
		return InputDeclaration{}
	}
	var out InputDeclaration
	if t, ok := fields["type"]; ok {
		if s, isString := t.GetKind().(*structpb.Value_StringValue); isString {
			out.Type = s.StringValue
		}
	}
	if d, ok := fields["default"]; ok {
		if d == nil {
			d = structpb.NewNullValue()
		}
		out.Default = d
	}
	if r, ok := fields["required"]; ok {
		if b, isBool := r.GetKind().(*structpb.Value_BoolValue); isBool && b.BoolValue {
			out.Required = out.Default == nil
		}
	}
	return out
}

// CountNoun renders "1 output" / "3 outputs": one helper for every count
// sentence of the interface comparison, its refusal and its warnings.
func CountNoun(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %s", n, plural)
}

// renderJSON renders a default value as stable JSON: map keys sorted, numbers
// as Go formats a float64, HTML characters unescaped, no trailing newline.
// Never protojson, whose whitespace is deliberately unstable.
func renderJSON(value *structpb.Value) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value.AsInterface()); err != nil {
		// AsInterface yields only JSON-encodable values; a NaN or Inf number
		// is the one exception, and it renders as Go formats it.
		return fmt.Sprint(value.AsInterface())
	}
	return strings.TrimRight(buf.String(), "\n")
}

// renderInput renders one input declaration for a finding's from/to.
func renderInput(d InputDeclaration) string {
	t := d.Type
	if t == "" {
		t = "untyped"
	}
	out := t + ", optional"
	if d.Required {
		out = t + ", required"
	}
	if d.Default != nil {
		out += ", default " + renderJSON(d.Default)
	}
	return out
}

func compatVerdict() orkstackv1.InterfaceVerdict {
	return orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE
}

func breakingVerdict() orkstackv1.InterfaceVerdict {
	return orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING
}

func unknownVerdict() orkstackv1.InterfaceVerdict {
	return orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN
}

// CompareInterfaces classifies how from's declared interface changes in to
// (docs/UI_CLI_PARITY.md "Interface compatibility"). Pure, total, deterministic: a nil
// message reads as an Orkflow with no inputs and no outputs. Findings are
// every input, names sorted bytewise, each in the table's order (type, then
// required, then default); then the output side — O1 or O2 alone, or every
// output name sorted bytewise. The returned verdict is
// InterfaceVerdictOf(findings).
func CompareInterfaces(from, to *orkstackv1.Orkflow) ([]*orkstackv1.InterfaceFinding, orkstackv1.InterfaceVerdict) {
	var findings []*orkstackv1.InterfaceFinding
	add := func(field string, change orkstackv1.InterfaceChange, verdict orkstackv1.InterfaceVerdict,
		fromText, toText, message string) {
		findings = append(findings, &orkstackv1.InterfaceFinding{
			Field: field, Change: change, Verdict: verdict,
			From: fromText, To: toText, Message: message,
		})
	}

	fromInputs := from.GetInputs().GetFields()
	toInputs := to.GetInputs().GetFields()
	for _, name := range unionKeys(fromInputs, toInputs) {
		field := "inputs." + name
		fDecl, inFrom := fromInputs[name]
		tDecl, inTo := toInputs[name]
		F, T := ReadInput(fDecl), ReadInput(tDecl)
		q := quote(name)
		switch {
		case !inFrom: // I1, I2
			if T.Required {
				add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_ADDED, breakingVerdict(),
					"", renderInput(T), fmt.Sprintf(
						"input %s is added and required with no default: a run that does not "+
							"supply it no longer satisfies the declaration", q))
			} else {
				msg := fmt.Sprintf("input %s is added and optional", q)
				if T.Default != nil {
					msg += " with default " + renderJSON(T.Default)
				}
				add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_ADDED, compatVerdict(),
					"", renderInput(T), msg)
			}
			continue
		case !inTo: // I3
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_REMOVED, breakingVerdict(),
				renderInput(F), "", fmt.Sprintf(
					"input %s is removed: a value a caller supplies for it is no longer declared, "+
						"and no template of this version can read it", q))
			continue
		}
		fromText, toText := renderInput(F), renderInput(T)
		before := len(findings)
		// Type.
		switch {
		case F.Type != "" && T.Type != "" && F.Type != T.Type: // I4
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_RETYPED, breakingVerdict(),
				fromText, toText, fmt.Sprintf(
					"input %s is retyped from %s to %s: a value of the old type no longer "+
						"matches the declaration", q, F.Type, T.Type))
		case F.Type == "" && T.Type != "": // I5
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_RETYPED, breakingVerdict(),
				fromText, toText, fmt.Sprintf(
					"input %s is now typed %s (it was untyped): a value of any other type no "+
						"longer matches the declaration", q, T.Type))
		case F.Type != "" && T.Type == "": // I6
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_UNTYPED, compatVerdict(),
				fromText, toText, fmt.Sprintf("input %s is no longer typed (it was %s)", q, F.Type))
		}
		// Required.
		switch {
		case !F.Required && T.Required: // I7
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_NOW_REQUIRED, breakingVerdict(),
				fromText, toText, fmt.Sprintf(
					"input %s is now required with no default: a run that omits it no longer "+
						"satisfies the declaration", q))
		case F.Required && !T.Required: // I8
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_NO_LONGER_REQUIRED, compatVerdict(),
				fromText, toText, fmt.Sprintf("input %s is no longer required", q))
		}
		// Default.
		switch {
		case F.Default == nil && T.Default != nil: // I9
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_DEFAULT_ADDED, compatVerdict(),
				fromText, toText, fmt.Sprintf("input %s gains default %s", q, renderJSON(T.Default)))
		case F.Default != nil && T.Default == nil && !T.Required: // I10
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_DEFAULT_REMOVED, breakingVerdict(),
				fromText, toText, fmt.Sprintf(
					"input %s loses its default %s: a run that omits it no longer receives a value",
					q, renderJSON(F.Default)))
		case F.Default != nil && T.Default != nil && !proto.Equal(F.Default, T.Default): // I11
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_DEFAULT_CHANGED, compatVerdict(),
				fromText, toText, fmt.Sprintf(
					"input %s changes its default from %s to %s: a run that omits it receives "+
						"the new value", q, renderJSON(F.Default), renderJSON(T.Default)))
		}
		if len(findings) == before { // I12
			add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_UNCHANGED, compatVerdict(),
				fromText, toText, fmt.Sprintf("input %s is unchanged", q))
		}
	}

	fromOutputs := from.GetOutputs()
	toOutputs := to.GetOutputs()
	fromDeclares, toDeclares := len(fromOutputs) > 0, len(toOutputs) > 0
	switch {
	case !fromDeclares && !toDeclares: // O1
		add("outputs", orkstackv1.InterfaceChange_INTERFACE_CHANGE_OUTPUTS_UNDECLARED, unknownVerdict(),
			"none", "none",
			"neither version declares outputs: a run's output is every leaf task's values, an "+
				"accident of graph shape, so whether it changed cannot be stated")
	case !fromDeclares && toDeclares: // O2
		names := unionKeys(toOutputs, nil)
		add("outputs", orkstackv1.InterfaceChange_INTERFACE_CHANGE_OUTPUTS_DECLARED, unknownVerdict(),
			"none", CountNoun(len(toOutputs), "output", "outputs"), fmt.Sprintf(
				"outputs are declared for the first time (%s): a run's output changes from every "+
					"leaf task's values to the declared outputs, and an Orkflow composing this one "+
					"receives them instead of no values", strings.Join(names, ", ")))
	default:
		for _, name := range unionKeys(fromOutputs, toOutputs) {
			field := "outputs." + name
			_, inFrom := fromOutputs[name]
			_, inTo := toOutputs[name]
			q := quote(name)
			switch {
			case inFrom && !inTo: // O3
				msg := fmt.Sprintf(
					"output %s is removed: an Orkflow composing this one that reads "+
						".tasks.<id>.output.%s is refused at its next apply against this version, "+
						"and a reader of the run's output no longer finds it", q, name)
				if !toDeclares {
					msg += " (this version declares no outputs, so a run's output reverts to " +
						"every leaf task's values)"
				}
				add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_REMOVED, breakingVerdict(),
					"declared", "", msg)
			case !inFrom && inTo: // O4
				add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_ADDED, compatVerdict(),
					"", "declared", fmt.Sprintf("output %s is added", q))
			default: // O5
				add(field, orkstackv1.InterfaceChange_INTERFACE_CHANGE_UNCHANGED, compatVerdict(),
					"declared", "declared", fmt.Sprintf("output %s is unchanged", q))
			}
		}
	}
	return findings, InterfaceVerdictOf(findings)
}

// InterfaceVerdictOf is BREAKING when any finding is, else UNKNOWN when any
// finding is, else COMPATIBLE. It never returns UNSPECIFIED. CompareInterfaces
// never hands it an empty list — the output side always yields at least one
// finding — and an empty list, from any other caller, is COMPATIBLE.
func InterfaceVerdictOf(findings []*orkstackv1.InterfaceFinding) orkstackv1.InterfaceVerdict {
	verdict := compatVerdict()
	for _, f := range findings {
		switch f.GetVerdict() {
		case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING:
			return breakingVerdict()
		case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN:
			verdict = unknownVerdict()
		}
	}
	return verdict
}

// BreakingFindings returns the BREAKING findings, in order.
func BreakingFindings(findings []*orkstackv1.InterfaceFinding) []*orkstackv1.InterfaceFinding {
	var out []*orkstackv1.InterfaceFinding
	for _, f := range findings {
		if f.GetVerdict() == orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
			out = append(out, f)
		}
	}
	return out
}

// quote is Go's strconv.Quote of a name, the %q every finding message uses.
func quote(name string) string { return fmt.Sprintf("%q", name) }

// unionKeys returns the keys of either map, sorted bytewise.
func unionKeys[V any](a, b map[string]V) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, m := range []map[string]V{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}
