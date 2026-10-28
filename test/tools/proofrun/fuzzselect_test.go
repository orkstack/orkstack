package main

import (
	"reflect"
	"strings"
	"testing"
)

// A native fuzz target is a test (AGENTS.md §11.1): its seeds run in every
// unit gate, and the unit ledger lists it beside the TestXxx functions. So the
// feature proof's evidence selector must treat one as it treats a test: a diff
// that changes only a fuzz target, or only a helper it reaches, selects it,
// --tests accepts its name, and a deleted one is removed rather than selected.

// dslFuzzFile is a native fuzz target with a helper of its own, in the miniature
// repository's dsl package (select_test.go's baseTree).
const dslFuzzFile = `package dsl

import "testing"

func parseText(s string) int { return Parse() + len(s) }

func FuzzParse(f *testing.F) {
	f.Add("a")
	f.Fuzz(func(t *testing.T, s string) { _ = parseText(s) })
}
`

func TestSelect_AChangedFuzzTargetIsUnitEvidence(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/dsl/fuzz_test.go",
		strings.Replace(dslFuzzFile, `f.Add("a")`, `f.Add("b")`, 1)}}, nil, nil)
	requireErrors(t, sel)
	if got := unitNames(sel); !reflect.DeepEqual(got, []string{"internal/dsl.FuzzParse"}) {
		t.Errorf("unit evidence = %v, want the changed fuzz target", got)
	}
	if len(sel.UnitEvidence) == 1 && sel.UnitEvidence[0].Why != "changed" {
		t.Errorf("why = %q", sel.UnitEvidence[0].Why)
	}
}

func TestSelect_AHelperOnlyAFuzzTargetReachesSelectsIt(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/dsl/fuzz_test.go",
		strings.Replace(dslFuzzFile, "Parse() + len(s)", "Parse() + 2*len(s)", 1)}}, nil, nil)
	requireErrors(t, sel)
	if got := unitNames(sel); !reflect.DeepEqual(got, []string{"internal/dsl.FuzzParse"}) {
		t.Errorf("unit evidence = %v, want the fuzz target that reaches the changed helper", got)
	}
	if len(sel.UnitEvidence) == 1 && !strings.HasPrefix(sel.UnitEvidence[0].Why, "reaches changed") {
		t.Errorf("why = %q", sel.UnitEvidence[0].Why)
	}
}

func TestSelect_TestsNamesAFuzzTarget(t *testing.T) {
	prod := change{"internal/dsl/parse.go", "package dsl\n\nfunc Parse() int { return 2 }\n"}
	for _, spec := range []string{"internal/dsl:FuzzParse", "./internal/dsl:Fuzz.*", modulePath + "/internal/dsl:FuzzParse"} {
		sel := fakeSelect(t, []change{prod}, []string{spec}, nil)
		requireErrors(t, sel)
		if got := unitNames(sel); !reflect.DeepEqual(got, []string{"internal/dsl.FuzzParse"}) {
			t.Errorf("--tests %s: unit evidence = %v", spec, got)
		}
		if len(sel.UnitEvidence) == 1 && sel.UnitEvidence[0].Why != "named by --tests "+spec {
			t.Errorf("--tests %s: why = %q", spec, sel.UnitEvidence[0].Why)
		}
	}
	// A name that is no fuzz target of the package is still refused.
	sel := fakeSelect(t, []change{prod}, []string{"internal/dsl:FuzzNope"}, nil)
	requireErrors(t, sel, `--tests "internal/dsl:FuzzNope" matches no test declared in internal/dsl`)
}

func TestSelect_ADeletedFuzzTargetIsRemovedNotSelected(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/dsl/fuzz_test.go", ""}}, nil, nil)
	if !reflect.DeepEqual(sel.Removed, []string{pDSL + ".FuzzParse"}) {
		t.Errorf("removed = %v", sel.Removed)
	}
	if got := unitNames(sel); len(got) != 0 {
		t.Errorf("unit evidence = %v: a deleted target is no evidence", got)
	}
}
