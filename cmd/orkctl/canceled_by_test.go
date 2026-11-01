package main

import (
	"bytes"
	"strings"
	"testing"

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

// `run get` says who canceled the run with the caption every principal this
// CLI prints carries, and says nothing at all when nobody is recorded.
func TestRenderRun_SaysWhoCanceledTheRunAsAsserted(t *testing.T) {
	var out bytes.Buffer
	if err := renderRun(&out, canceledRun("dana@acme")); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	want := "Canceled by: dana@acme (asserted by the gateway; not authenticated)"
	if got := collapseSpaces(out.String()); !strings.Contains(got, want) {
		t.Fatalf("run get output lacks %q:\n%s", want, out.String())
	}

	out.Reset()
	if err := renderRun(&out, canceledRun("")); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	if strings.Contains(out.String(), "Canceled by") {
		t.Fatalf("a run nobody is recorded as canceling shows a Canceled by line:\n%s", out.String())
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
