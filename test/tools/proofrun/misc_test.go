package main

import (
	"strings"
	"testing"
)

func TestWriteGates(t *testing.T) {
	sel := &selection{Integration: map[string]*pkgSelection{pEngine: {Evidence: map[string]string{"TestX": "named"}}},
		Specs: []selectedSpec{{File: "ui/e2e/admin.spec.ts"}, {File: "ui/e2e/budgets.spec.ts"}}}
	sel.Gates.UICheck, sel.Gates.UIUnit = true, true
	var b strings.Builder
	writeGates(&b, sel)
	want := "integration=yes\nui_check=yes\nui_unit=yes\nui_e2e=yes\nactivity_fixture=no\nobservability=no\nspecs=e2e/admin.spec.ts e2e/budgets.spec.ts\n"
	if b.String() != want {
		t.Errorf("got\n%s\nwant\n%s", b.String(), want)
	}
	b.Reset()
	writeGates(&b, &selection{Integration: map[string]*pkgSelection{pEngine: {Evidence: map[string]string{}}}, AllSpecs: "ui/e2e/harness.ts changed"})
	if !strings.Contains(b.String(), "integration=no\n") || !strings.Contains(b.String(), "specs=ALL\n") || !strings.Contains(b.String(), "ui_e2e=yes\n") {
		t.Errorf("got\n%s", b.String())
	}
}

func TestTouching(t *testing.T) {
	scope, err := parseScope(strings.NewReader("internal/telemetry/**.go\tobservability,go\ninternal/**\tgo\ndocs/**\tnone\n"), "t")
	if err != nil {
		t.Fatal(err)
	}
	paths := []changedPath{{Path: "docs/x.md"}, {Path: "internal/telemetry/sink.go"}, {Path: "internal/engine/a.go"}}
	if got := touching(scope, paths, actObservability); strings.Join(got, ",") != "internal/telemetry/sink.go" {
		t.Errorf("got %v", got)
	}
	if got := touching(scope, paths[:1], actObservability); len(got) != 0 {
		t.Errorf("got %v", got)
	}
}
