package main

import (
	"bytes"
	"strings"
	"testing"
	"unicode"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func canceledRun(by string) *orkstackv1.Run {
	at := timestamppb.New(fixedNow)
	return &orkstackv1.Run{
		Id: "ork-1", OrkflowName: "deploy", OrkflowVersion: "1.0.0",
		Status:     orkstackv1.Status_STATUS_CANCELED,
		Error:      &orkstackv1.Error{Code: "RUN_CANCELED", Message: "run canceled"},
		CreatedAt:  at,
		FinishedAt: at,
		Origin:     &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT},
		CanceledBy: by,
	}
}

// unsafePrincipals are gateway header values that would break a detail
// line if printed as they are: a tab splits the table's columns, an escape
// reaches the terminal, a bidi override reorders the caption, and a line or
// paragraph separator ends the line before the caption. Each renders with
// every such character as a space.
var unsafePrincipals = []struct {
	name, principal, shown string
}{
	{"plain", "dana@acme", "dana@acme"},
	{"a tab and an escape", "dana\t@acme\x1b[2J", "dana @acme [2J"},
	{"a bidi override", "dana\u202e@acme", "dana @acme"},
	{"a newline", "dana\n@acme", "dana @acme"},
	{"a line separator", "dana\u2028@acme", "dana @acme"},
	{"a zero-width joiner", "dana\u200d@acme", "dana @acme"},
}

// detailLine is the one line of a detail view that opens with label, or the
// test fails when there is not exactly one.
func detailLine(t *testing.T, out, label string) string {
	t.Helper()
	var found []string
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(line, label) {
			found = append(found, line)
		}
	}
	if len(found) != 1 {
		t.Fatalf("output has %d %q lines, want 1:\n%s", len(found), label, out)
	}
	return found[0]
}

// requirePrintable fails when line carries a control, format or separator
// character a terminal would act on.
func requirePrintable(t *testing.T, line string) {
	t.Helper()
	for _, r := range line {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			t.Fatalf("line carries %U: %q", r, line)
		}
	}
}

// `run get` says who canceled the run on one line, the gateway's control,
// format and separator characters as spaces, with the caption every
// principal this CLI prints carries after it; and says nothing at all when
// nobody is recorded.
func TestRenderRun_SaysWhoCanceledTheRunAsAsserted(t *testing.T) {
	for _, tt := range unsafePrincipals {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := renderRun(&out, canceledRun(tt.principal)); err != nil {
				t.Fatalf("renderRun: %v", err)
			}
			line := detailLine(t, out.String(), "Canceled by:")
			requirePrintable(t, line)
			want := "Canceled by: " + tt.shown + " " + principalCaption
			if got := collapseSpaces(line); got != want {
				t.Fatalf("Canceled by line = %q, want %q", got, want)
			}
		})
	}

	var out bytes.Buffer
	if err := renderRun(&out, canceledRun("")); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	if strings.Contains(out.String(), "Canceled by") {
		t.Fatalf("a run nobody is recorded as canceling shows a Canceled by line:\n%s", out.String())
	}
}

// `run get` prints an origin's principal the same way: one line, the
// gateway's control, format and separator characters as spaces, the caption
// after it.
func TestRenderRun_PrintsTheOriginPrincipalAsAsserted(t *testing.T) {
	for _, tt := range unsafePrincipals {
		t.Run(tt.name, func(t *testing.T) {
			run := canceledRun("")
			run.Origin.Principal = tt.principal
			var out bytes.Buffer
			if err := renderRun(&out, run); err != nil {
				t.Fatalf("renderRun: %v", err)
			}
			line := detailLine(t, out.String(), "Principal:")
			requirePrintable(t, line)
			want := "Principal: " + tt.shown + " " + principalCaption
			if got := collapseSpaces(line); got != want {
				t.Fatalf("Principal line = %q, want %q", got, want)
			}
		})
	}
}

// `run list` carries the name in the STATUS cell, marked asserted, and a
// gateway's control characters cannot split the row's columns.
func TestRenderRunList_SaysWhoCanceledEachRun(t *testing.T) {
	tests := []struct {
		name string
		by   string
		want string
	}{
		{"named", "dana@acme", "ork-1 deploy 1.0.0 submit CANCELED by dana@acme (asserted) 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z"},
		{"nobody", "", "ork-1 deploy 1.0.0 submit CANCELED 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z"},
		{"a tab in the header", "dana\t@acme\x1b[2J", "ork-1 deploy 1.0.0 submit CANCELED by dana @acme [2J (asserted) 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z"},
		{"a bidi override", "dana\u202e@acme", "ork-1 deploy 1.0.0 submit CANCELED by dana @acme (asserted) 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := renderRunList(&out, []*orkstackv1.Run{canceledRun(tt.by)}); err != nil {
				t.Fatalf("renderRunList: %v", err)
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != 2 {
				t.Fatalf("run list = %q, want a header and one row", out.String())
			}
			if got := collapseSpaces(lines[1]); got != tt.want {
				t.Fatalf("row = %q, want %q", got, tt.want)
			}
			if strings.ContainsAny(lines[1], "\t\x1b\u202e") {
				t.Fatalf("row carries a control character: %q", lines[1])
			}
		})
	}
}

func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
