package main

import (
	"fmt"
	"io"
	"strings"
	"unicode"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// originKindPrefix is the proto enum prefix no user-facing spelling carries.
const originKindPrefix = "RUN_ORIGIN_KIND_"

// principalCaption is said beside every principal this CLI prints, in the
// words the audit trail uses: the value is the gateway's assertion, recorded
// and never checked.
const principalCaption = "(asserted by the gateway; not authenticated)"

// assertedName is an asserted principal as this CLI prints it, with every
// control character (the tab an HTTP header may carry included), format
// character (a bidi override, a zero-width joiner) and line or paragraph
// separator as a space, so a gateway's value can neither break a table's
// columns, reorder the caption beside it, nor reach the terminal as a control
// sequence.
func assertedName(principal string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return ' '
		}
		return r
	}, principal)
}

// unrecordedOrigin is what a run recorded before origins existed renders as.
// It is never "submit": nothing supports the claim that a person submitted it.
const unrecordedOrigin = "unrecorded"

// originKindSlug is the user-facing spelling of a RunOriginKind: the proto
// value's name after RUN_ORIGIN_KIND_, lowercased, "_" as "-". It is the same
// rule pkg/artifacts.OriginKindSlug and ui/src/lib/origin.ts apply; this copy
// exists because the CLI does not import the storage client for one rule.
// ok is false for UNSPECIFIED and for a number the enum does not declare.
func originKindSlug(kind orkstackv1.RunOriginKind) (string, bool) {
	if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
		return "", false
	}
	name, ok := orkstackv1.RunOriginKind_name[int32(kind)]
	if !ok {
		return "", false
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, originKindPrefix)), "_", "-"), true
}

// originSummary renders the origin's kind and id in one short phrase:
// "trigger trg-…", "submit", "run-again ork-…", or "" when there is none.
func originSummary(origin *orkstackv1.RunOrigin) string {
	slug, ok := originKindSlug(origin.GetKind())
	if !ok {
		return ""
	}
	if origin.GetId() == "" {
		return slug
	}
	return slug + " " + origin.GetId()
}

// originColumn is the run list's ORIGIN cell: the summary, or a dash for a
// run with no recorded origin — the house rendering of an absent value in a
// table column; the full word is in `run get`.
func originColumn(origin *orkstackv1.RunOrigin) string {
	return orDash(terminalSafe(originSummary(origin)))
}

// writeOriginLines writes the Origin, Root run, Reference and Principal lines
// of a run detail view into a tab-aligned table. Origin is always written; the
// others only when they carry a value. Every value is written terminalSafe:
// a reference and a description are the caller's text, and none of them may
// reach the terminal as a control or end its line.
func writeOriginLines(table io.Writer, origin *orkstackv1.RunOrigin) {
	summary := originSummary(origin)
	if summary == "" {
		fmt.Fprintf(table, "Origin:\t%s (started before origins were recorded)\n", unrecordedOrigin)
		return
	}
	if description := origin.GetDescription(); description != "" {
		summary += " (" + description + ")"
	}
	fmt.Fprintf(table, "Origin:\t%s\n", terminalSafe(summary))
	if root := origin.GetRootRunId(); root != "" {
		fmt.Fprintf(table, "Root run:\t%s\n", terminalSafe(root))
	}
	if reference := origin.GetReference(); reference != "" {
		fmt.Fprintf(table, "Reference:\t%s\n", terminalSafe(reference))
	}
	if principal := origin.GetPrincipal(); principal != "" {
		fmt.Fprintf(table, "Principal:\t%s  %s\n", assertedName(principal), principalCaption)
	}
}
